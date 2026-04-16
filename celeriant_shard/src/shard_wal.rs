use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use glommio::sync::Semaphore;
use tracing::{debug, info, trace, warn};

use celeriant_disk::files::rwlock_timeout::write_with_timeout;
use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::validated_node_status::{self, ValidatedNodeStatus, set_node_status_and_metric};
use celeriant_rotating_log::errors::ready_up_error::ReadyUpError;
use celeriant_rotating_log::errors::scan_error::ScanError;
use celeriant_wire::disk::disk_format_error::DiskFormatError;
use celeriant_wire::disk::metablock_bytes;
use celeriant_wire::disk::serialised_datablock::SerialisedDatablock;
use celeriant_wire::disk::versioned_block::{deserialise_metablock, deserialise_segment_summary};
use crate::shard_wal_sync::summary_path;
use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::mem_snapshot_aggregate::{AggregateStatus, MemSnapshotAggregate};
use celeriant_memcache::metablock_position::MetablockPosition;
use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use crate::schema_validator::CompiledValidator;

type MemCache = ShardMemCache<CompiledValidator>;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{DeleteRequest, AggregateDetailsRequest, ListAggregateTypesRequest, ListAggregatesRequest, ListOrgsRequest, ReadRequest, SingleAggregateWrite, TrimStartRequest, WriteRequest};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::{AggregateListItem, AggregateTypeListItem, AggregateDetailsResponse, FollowerRejection, ListAggregateTypesResponse, ListAggregatesResponse, ListOrgsResponse, OrgListItem, ReadResponse, ReplicationBatchResponse, ReplicationResult, SuccessResponse};
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_rotating_log::reverse_metablock_scanner::ReverseMetablockScanner;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::aggregate_client_key::AggregateClientKey;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::aggregate_type_key::AggregateTypeKey;
use celeriant_wal::schema_key::SchemaKey;
use celeriant_wal::constants::{FIRST_EVENT_BATCH_INDEX, FIXED_BLOCK_SIZE_BYTES, GENESIS_HASH};
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use celeriant_wal::segment_summary::SegmentSummaryPayload;
use celeriant_wal::datablocks::datablock_kind::DatablockKind;
use celeriant_wal::datablocks::datablock_schema_registration::DatablockSchemaRegistration;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_event_batch::{EventTypesKind, MetablockEventBatch};
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_wal::metablocks::metablock_schema_registration::MetablockSchemaRegistration;
use celeriant_wal::metablocks::metablock_soft_delete::MetablockSoftDelete;
use celeriant_wal::metablocks::metablock_soft_trim::MetablockSoftTrim;
use celeriant_watch::aggregate_reader::AggregateReader;
use celeriant_watch::aggregate_watchers::AggregateWatchers;

use crate::amortisation::coordinator::Coordinator;
use crate::bloom::bloom_filter_cache::BloomFilterCache;
use crate::bloom::event_type_filter::extract_unique_event_types;
use crate::collect_from_disk::{EventBatchFromLogSegmentFile, fetch_datablocks_for_metablocks};
use crate::error::apply_batch_error::ApplyBatchError;
use crate::error::follower_replication_write_error::FollowerReplicationWriteError;
use crate::error::replication_error::ReplicationError;
use crate::error::s3_catchup_error::S3CatchupError;
use crate::error::shard_cache_load_error::ShardCacheLoadError;
use crate::error::shard_delete_error::ShardDeleteError;
use crate::error::shard_error::ShardError;
use crate::error::shard_exists_error::ShardAggregateDetailsError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::error::shard_listing_error::ShardListingError;
use crate::error::shard_read_error::ShardReadError;
use crate::error::shard_schema_error::ShardSchemaError;
use crate::error::shard_trim_error::ShardTrimError;
use crate::error::shard_write_error::ShardWriteError;
use crate::in_memory_filtering;
use crate::internal_shard_config::InternalShardConfig;
use crate::loading_coordinator::LoadingCoordinator;
use crate::replication_client::ReplicationClient;
use crate::s3_downloader::S3Downloader;
use crate::shard_wal_compact::{CompactionResult, compact_segment};
use crate::error::compaction_error::CompactionError;
use crate::shard_wal_replicate::{capture_replication_snapshot, commit_replication_with_rollback};
use crate::shard_wal_s3_catchup::{self, S3CatchupResult, catchup_from_s3};
use crate::shard_wal_sync::{capture_fsync_snapshot, commit_fsync_with_rollback};

/// Compile a schema datablock and insert into the cache.
/// Shared by pre_warm_cache, ensure_schema_cached, and follower replication.
pub(crate) fn compile_and_cache_schema(cache: &mut MemCache, schema_key: &SchemaKey, datablock: &Datablock) {
    if let DatablockKind::SchemaRegistration(ref schema_data) = datablock.datablock_kind {
        let cached = match crate::schema_validator::CompiledValidator::compile(schema_data.schema_type, &schema_data.schema) {
            Ok(validator) => celeriant_memcache::cached_schema::CachedSchema::Validated(validator),
            Err(e) => celeriant_memcache::cached_schema::CachedSchema::CompilationFailed(e),
        };
        cache.schema_cache_insert(schema_key.clone(), cached);
    }
}

/// Write-ahead log for a single shard.
///
/// Handles the complete lifecycle of reads and writes:
/// - Validation (idempotency, optimistic concurrency)
/// - Event batch construction
/// - Queue management
/// - Durability (fsync coordination)
/// - Cache population
/// - Watch notifications
///
/// Not thread-safe—designed for single-threaded per-core access.
pub struct ShardWal<R: ReplicationClient + 'static, D: S3Downloader + 'static> {
    /// Trait implementation to download replicated data stored on S3 for catchup
    s3_downloader: Rc<D>,

    /// No async in shard_mem_cache and no interior mutability
    shard_mem_cache: Rc<RefCell<MemCache>>,

    /// Uses interior mutability with glommio RwLocks for async access to log files
    log_segments_cache: Rc<LogSegmentsCache>,

    /// Uses interior mutability with glommio RwLocks to select an fsync leader
    fsync_coordinator: Rc<Coordinator<ShardFsyncError>>,

    /// Same pattern as fsync_coordinator, single leader, batched replication over tcp
    replication_coordinator: Rc<Coordinator<ReplicationError>>,

    /// Registry of watchers for aggregates in this shard, uses local channel broadcasting
    pub watched_aggregates: Rc<AggregateWatchers>,

    /// Cache for bloom filter construction to avoid repeated allocations, uses interior mutability
    bloom_filter_cache: Rc<BloomFilterCache>,

    // Are we the leader? Follower? Single node mode? Note this can change at runtime.
    pub node_status: Rc<Cell<ValidatedNodeStatus>>,

    config: InternalShardConfig,

    /// Serializes concurrent aggregate snapshot loading from disk
    aggregate_loading: LoadingCoordinator<AggregateKey>,

    /// Serializes concurrent client event index loading from disk
    aggregate_client_loading: LoadingCoordinator<AggregateClientKey>,

    /// Serializes concurrent schema loading from disk
    schema_loading: LoadingCoordinator<SchemaKey>,

    /// Limits concurrent list operations to bound unbudgeted per-request memory
    list_semaphore: Rc<Semaphore>,

    /// Limits concurrent cache-miss disk scans to prevent NVMe read saturation
    /// starving the fsync write path (cold start read amplification)
    cache_load_semaphore: Rc<Semaphore>,

    /// Client for replicating data to followers or S3.
    /// Interior mutability — FollowerConnection manages its own split locks.
    pub replication_client: Rc<R>,

    /// Leader's client-facing address, set when this node is a follower.
    /// Included in write-rejection errors so clients can redirect.
    pub leader_client_address: RefCell<Option<String>>,

    /// Peer's node_id from S3 membership. Used during S3 catchup to ignore
    /// stale fallback batches from previous cluster generations.
    pub peer_node_id: Cell<Option<u128>>,

    /// Monotonic timestamp of the most recent replication rollback. Used by
    /// the write path to apply a cooldown window (ReplicationBackpressure error).
    /// Happens if the network is slow or s3/minio having issues
    pub last_rollback_at: Rc<Cell<Option<Instant>>>,

    /// Cached metrics label to avoid per-request String allocation
    metrics_shard_label: [(&'static str, String); 1],
}

impl<R: ReplicationClient + 'static, D: S3Downloader + 'static> AggregateReader for ShardWal<R, D> {
    fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
        Rc::clone(&self.watched_aggregates)
    }
}

/// Read the segment summary from a closed log segment's sidecar `.summary` file.
/// Returns `None` for legacy segments that don't have a summary, or if the file is corrupt.
/// Opens and closes the file each time — no LRU involvement, OS page cache handles repeats.
pub(crate) async fn read_segment_summary(
    shard_dir: &std::path::Path,
    log_id: u64,
) -> Option<SegmentSummaryPayload> {
    let path = summary_path(shard_dir, log_id);
    let file = match glommio::io::BufferedFile::open(&path).await {
        Ok(f) => f,
        Err(e) if e.raw_os_error() == Some(2) => return None, // ENOENT
        Err(e) => {
            tracing::warn!(log_id, error = %e, "Failed to open segment summary sidecar");
            return None;
        }
    };

    let file_size = match file.file_size().await {
        Ok(s) if s > 0 => s as usize,
        Ok(_) => return None,
        Err(e) => {
            tracing::warn!(log_id, error = %e, "Failed to read segment summary file size");
            return None;
        }
    };

    let buf = match file.read_at(0, file_size).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(log_id, error = %e, "Failed to read segment summary sidecar");
            return None;
        }
    };

    match deserialise_segment_summary(&buf) {
        Ok(block) => Some(block.payload),
        Err(_) => None,
    }
}

impl<R: ReplicationClient + 'static, D: S3Downloader + 'static> ShardWal<R, D> {
    pub async fn process_client_request(&self, request: ClientRequest) -> Result<ClientResponse, ShardError> {
        let shard_label = &self.metrics_shard_label;
        let start = Instant::now();

        let (op_name, result) = match request {
            ClientRequest::Write(r) => ("write", self.write(r).await.map(ClientResponse::Write).map_err(ShardError::Write)),
            ClientRequest::Read(r) => ("read", self.read(&r).await.map(ClientResponse::Read).map_err(ShardError::Read)),
            ClientRequest::Delete(r) => ("delete", self.delete(r).await.map(ClientResponse::Delete).map_err(ShardError::Delete)),
            ClientRequest::TrimStart(r) => ("trim", self.trim_start(r).await.map(ClientResponse::TrimStart).map_err(ShardError::TrimStart)),
            ClientRequest::AggregateDetails(r) => ("exists", self.exists(&r).await.map(ClientResponse::AggregateDetails).map_err(ShardError::AggregateDetails)),
            ClientRequest::Watch(_) => return Err(ShardError::WatchRequestInvalid),
            ClientRequest::ListOrgs(r) => ("list_orgs", self.list_orgs(r).await.map(ClientResponse::ListOrgs).map_err(ShardError::ListOrgs)),
            ClientRequest::ListAggregateTypes(r) => ("list_aggregate_types", self.list_aggregate_types(r).await.map(ClientResponse::ListAggregateTypes).map_err(ShardError::ListAggregateTypes)),
            ClientRequest::ListAggregates(r) => ("list_aggregates", self.list_aggregates(r).await.map(ClientResponse::ListAggregates).map_err(ShardError::ListAggregates)),
            ClientRequest::RegisterSchema(r) => ("register_schema", self.register_schema(r).await.map(ClientResponse::RegisterSchema).map_err(ShardError::RegisterSchema)),
        };

        let duration = start.elapsed().as_secs_f64();

        match &result {
            Ok(_) => {
                let counter_name = match op_name {
                    "write" => "celeriant_writes_total",
                    "read" => "celeriant_reads_total",
                    "delete" => "celeriant_deletes_total",
                    "trim" => "celeriant_trims_total",
                    _ => "celeriant_reads_total",
                };
                metrics::counter!(counter_name, shard_label).increment(1);

                let duration_name = match op_name {
                    "write" => Some("celeriant_write_duration_seconds"),
                    "read" => Some("celeriant_read_duration_seconds"),
                    _ => None,
                };
                if let Some(name) = duration_name {
                    metrics::histogram!(name, shard_label).record(duration);
                }
            }
            Err(e) => {
                let error_counter = match op_name {
                    "write" => "celeriant_write_errors_total",
                    "read" => "celeriant_read_errors_total",
                    _ => "celeriant_write_errors_total",
                };
                let error_label = [
                    ("shard_id", shard_label[0].1.clone()),
                    ("error_code", e.error_code().to_owned()),
                ];
                metrics::counter!(error_counter, error_label.as_slice()).increment(1);
            }
        }

        result
    }

    /// Open or create a shard WAL.
    ///
    /// If the shard directory exists with log files, reopens from the latest.
    /// Otherwise creates a new shard with an empty log file.
    pub async fn open(config: InternalShardConfig, node_status: ValidatedNodeStatus, replication_client: R, s3_downloader: D) -> Result<Self, ReadyUpError> {
        let shard_mem_cache = MemCache::new(
            config.recent_write_cache_bytes,
            config.aggregate_snapshots_cache_bytes,
            config.aggregate_client_snapshots_cache_bytes,
            config.list_wal_index_cache_bytes,
            config.schema_cache_bytes,
            config.pending_replication_high_water_bytes,
        );

        let log_segments_cache = LogSegmentsCache::ready_up(
            config.shard_dir.clone(),
            config.shard_log_preallocate_bytes,
            config.max_open_files as usize,
            config.shard_id,
        )
        .await?;

        let list_semaphore = Rc::new(Semaphore::new(config.list_max_concurrent));
        let cache_load_semaphore = Rc::new(Semaphore::new(config.read_max_concurrent));

        metrics::gauge!("celeriant_replication_queue_high_water_bytes").set(config.pending_replication_high_water_bytes as f64);

        let metrics_shard_label = [("shard_id", config.shard_id.to_string())];

        let shard_mem_cache = Rc::new(RefCell::new(shard_mem_cache));
        let log_segments_cache = Rc::new(log_segments_cache);

        Self::pre_warm_cache(&log_segments_cache, &shard_mem_cache, &config).await?;

        let recovered_wal_index = log_segments_cache.active().metadata.borrow().write.wal_index;
        metrics::gauge!("celeriant_wal_index", &metrics_shard_label).set(recovered_wal_index as f64);

        Ok(Self {
            s3_downloader: Rc::new(s3_downloader),
            shard_mem_cache,
            log_segments_cache,
            fsync_coordinator: Rc::new(Coordinator::new()),
            replication_coordinator: Rc::new(Coordinator::new()),
            watched_aggregates: Rc::new(AggregateWatchers::new()),
            bloom_filter_cache: Rc::new(BloomFilterCache::new()),
            node_status: Rc::new(Cell::new(node_status)),
            config,
            aggregate_loading: LoadingCoordinator::new(),
            aggregate_client_loading: LoadingCoordinator::new(),
            schema_loading: LoadingCoordinator::new(),
            list_semaphore,
            cache_load_semaphore,
            replication_client: Rc::new(replication_client),
            leader_client_address: RefCell::new(None),
            peer_node_id: Cell::new(None),
            last_rollback_at: Rc::new(Cell::new(None)),
            metrics_shard_label,
        })
    }

    /// Pre-warm aggregate and client caches by reverse-scanning the WAL.
    /// SoftDelete/SoftTrim metablocks carry full aggregate state, so each
    /// metablock kind can populate the cache immediately without continuing the scan.
    async fn pre_warm_cache(
        log_segments_cache: &Rc<LogSegmentsCache>,
        shard_mem_cache: &Rc<RefCell<MemCache>>,
        config: &InternalShardConfig,
    ) -> Result<(), ReadyUpError> {
        let warmup_start = Instant::now();
        let warmup_deadline = config.cache_warmup_max_duration;
        let mut warmup_agg_count = 0u64;
        let mut warmup_client_count = 0u64;
        let mut agg_cache_full = false;
        let mut client_cache_full = false;
        let mut timed_out = false;

        let starting_log_id = log_segments_cache.active_log_id();
        let mut active_segment_metablocks: Vec<Metablock> = Vec::new();

        let mut scanner = ReverseMetablockScanner::new(
            log_segments_cache,
            starting_log_id,
            None,
            config.read_max_chunk_size,
        );

        let mut deferred_schema_blocks: Vec<(u64, SchemaKey, Metablock)> = Vec::new();

        scanner
            .scan::<(), ReadyUpError>(|log_id, metablock_absolute_pos, metablock_bytes| {
                if agg_cache_full && client_cache_full {
                    return Ok(Some(()));
                }
                if warmup_start.elapsed() >= warmup_deadline {
                    timed_out = true;
                    return Ok(Some(()));
                }

                if metablock_bytes::is_metablock_kind_soft_delete(metablock_bytes) {
                    let metablock = deserialise_metablock(metablock_bytes)
                        .map_err(|e| ReadyUpError::UnableToAccessDirectory {
                            directory: format!("corrupt soft-delete metablock at log {log_id} pos {metablock_absolute_pos}"),
                            source: std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:?}")),
                        })?;
                    if log_id == starting_log_id {
                        active_segment_metablocks.push(metablock.clone());
                    }
                    if let MetablockKind::SoftDelete(soft_delete) = metablock.wal_metablock_type {
                        let mut cache = shard_mem_cache.borrow_mut();
                        if !cache.is_aggregate_snapshot_full_or_contains(&soft_delete.aggregate_key, CachePath::Write) {
                            cache.put_aggregate_into_cache_as_deleted(
                                soft_delete.aggregate_key,
                                log_id,
                                metablock_absolute_pos,
                                soft_delete.event_index,
                                soft_delete.event_batch_index,
                                soft_delete.allow_recreate,
                                soft_delete.allow_index_continuation,
                                CachePath::Write,
                            );
                            warmup_agg_count += 1;
                            agg_cache_full = cache.is_aggregate_snapshot_cache_full(CachePath::Write);
                        }
                    }
                    return Ok(None);
                }

                if metablock_bytes::is_metablock_kind_soft_trim(metablock_bytes) {
                    let metablock = deserialise_metablock(metablock_bytes)
                        .map_err(|e| ReadyUpError::UnableToAccessDirectory {
                            directory: format!("corrupt soft-trim metablock at log {log_id} pos {metablock_absolute_pos}"),
                            source: std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:?}")),
                        })?;
                    if log_id == starting_log_id {
                        active_segment_metablocks.push(metablock.clone());
                    }
                    if let MetablockKind::SoftTrim(soft_trim) = metablock.wal_metablock_type {
                        let mut cache = shard_mem_cache.borrow_mut();
                        if !cache.is_aggregate_snapshot_full_or_contains(&soft_trim.aggregate_key, CachePath::Write) {
                            let snapshot = MemSnapshotAggregate::found(
                                log_id,
                                metablock_absolute_pos,
                                soft_trim.event_index,
                                soft_trim.event_batch_index,
                                soft_trim.keep_from_event_batch_index,
                            );
                            cache.put_aggregate_snapshot_only(soft_trim.aggregate_key, snapshot, false, CachePath::Write);
                            warmup_agg_count += 1;
                            agg_cache_full = cache.is_aggregate_snapshot_cache_full(CachePath::Write);
                        }
                    }
                    return Ok(None);
                }

                if metablock_bytes::is_metablock_kind_schema_registration(metablock_bytes) {
                    if let Ok(metablock) = deserialise_metablock(metablock_bytes) {
                        if log_id == starting_log_id {
                            active_segment_metablocks.push(metablock.clone());
                        }
                        if let MetablockKind::SchemaRegistration(ref schema_reg) = metablock.wal_metablock_type {
                            let mut cache = shard_mem_cache.borrow_mut();
                            if !cache.is_schema_cache_full() && !cache.schema_cache_contains(&schema_reg.schema_key) {
                                cache.schema_cache_insert(schema_reg.schema_key.clone(), celeriant_memcache::cached_schema::CachedSchema::NotYetLoaded);
                                deferred_schema_blocks.push((log_id, schema_reg.schema_key.clone(), metablock.clone()));
                            }
                        }
                    }
                    return Ok(None);
                }

                if !metablock_bytes::is_metablock_kind_event_batch_metadata(metablock_bytes) {
                    return Ok(None);
                }

                if log_id == starting_log_id {
                    if let Ok(metablock) = deserialise_metablock(metablock_bytes) {
                        active_segment_metablocks.push(metablock);
                    }
                }

                let aggregate_key = metablock_bytes::read_event_batch_aggregate_key(metablock_bytes);
                let mut cache = shard_mem_cache.borrow_mut();
                if !cache.is_aggregate_snapshot_full_or_contains(&aggregate_key, CachePath::Write) {
                    let snapshot = MemSnapshotAggregate::found(
                        log_id,
                        metablock_absolute_pos,
                        metablock_bytes::read_event_batch_max_event_index(metablock_bytes),
                        metablock_bytes::read_event_batch_event_batch_index(metablock_bytes),
                        metablock_bytes::read_event_batch_min_event_batch_index(metablock_bytes),
                    );
                    let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
                    let last_client_event_index = metablock_bytes::read_event_batch_max_client_event_index(metablock_bytes);
                    cache.put_aggregate_into_cache(aggregate_key, snapshot, client_id, last_client_event_index, false, CachePath::Write);
                    warmup_agg_count += 1;
                    agg_cache_full = cache.is_aggregate_snapshot_cache_full(CachePath::Write);
                    client_cache_full = cache.is_aggregate_client_cache_full();
                } else {
                    let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
                    let client_key = AggregateClientKey::new(aggregate_key, client_id);
                    if !cache.is_aggregate_client_cache_full_or_contains(&client_key) {
                        let last_client_event_index = metablock_bytes::read_event_batch_max_client_event_index(metablock_bytes);
                        cache.put_aggregate_client_into_cache(client_key, last_client_event_index, false);
                        warmup_client_count += 1;
                        client_cache_full = cache.is_aggregate_client_cache_full();
                    }
                }

                Ok(None)
            })
            .await
            .map_err(|e| match e {
                ScanError::Visitor(ready_up) => ready_up,
                ScanError::OpenLogSegment(e) => ReadyUpError::ActiveFileError(e),
                ScanError::Io { log_id, source } => ReadyUpError::UnableToAccessDirectory {
                    directory: format!("log segment {log_id}"),
                    source: std::io::Error::new(std::io::ErrorKind::Other, source),
                },
                ScanError::LockTimeout(e) => ReadyUpError::UnableToAccessDirectory {
                    directory: "lock timeout during warmup".to_string(),
                    source: std::io::Error::new(std::io::ErrorKind::TimedOut, e.to_string()),
                },
                ScanError::NoFileHandle { log_id } => ReadyUpError::UnableToAccessDirectory {
                    directory: format!("log segment {log_id}"),
                    source: std::io::Error::new(std::io::ErrorKind::NotFound, "no file handle"),
                },
            })?;

        // Fetch deferred schema datablocks (inline or from disk) and compile into cache
        if !deferred_schema_blocks.is_empty() {
            let mut batches: Vec<crate::collect_from_disk::EventBatchFromLogSegmentFile> = deferred_schema_blocks.iter()
                .map(|(log_id, _, metablock)| crate::collect_from_disk::EventBatchFromLogSegmentFile {
                    log_id: *log_id,
                    metablock: metablock.clone(),
                    datablock: None,
                })
                .collect();

            crate::collect_from_disk::fetch_datablocks_for_metablocks(&mut batches, config.read_max_chunk_size, log_segments_cache)
                .await
                .map_err(|e| ReadyUpError::UnableToAccessDirectory {
                    directory: format!("schema datablock fetch: {e:?}"),
                    source: std::io::Error::new(std::io::ErrorKind::Other, format!("{e:?}")),
                })?;

            let mut cache = shard_mem_cache.borrow_mut();
            for (batch, (_, schema_key, _)) in batches.into_iter().zip(deferred_schema_blocks.iter()) {
                if let Some(ref datablock) = batch.datablock {
                    compile_and_cache_schema(&mut cache, schema_key, datablock);
                }
            }
        }

        // Replay active segment metablocks in forward (write) order for correct summary state.
        // The reverse scan collected them newest-first; reversing gives chronological order.
        {
            let mut cache = shard_mem_cache.borrow_mut();
            for metablock in active_segment_metablocks.into_iter().rev() {
                cache.update_segment_summary(&metablock);
            }
        }

        info!(
            shard_id = config.shard_id,
            aggregates = warmup_agg_count,
            clients = warmup_client_count,
            timed_out,
            duration_ms = warmup_start.elapsed().as_millis() as u64,
            "Cache warmup complete"
        );

        Ok(())
    }

    /// List all unique organizations that have data in this shard.
    ///
    /// Reads segment summaries newest-to-oldest, falling back to reverse metablock
    /// scan for legacy segments without summaries. Pagination breaks between segments:
    /// each segment is fully processed before checking the page limit.
    pub async fn list_orgs(&self, request: ListOrgsRequest) -> Result<ListOrgsResponse, ShardListingError> {
        let _permit = self.list_semaphore.acquire_permit(1).await
            .map_err(|_| ShardListingError::ListSemaphoreClosed)?;

        let start_time = Instant::now();
        let max_duration = self.config.list_max_duration;
        let page_size = self.config.list_page_size;
        let active_log_id = self.log_segments_cache.active_log_id();

        let mut seen: HashSet<u128> = HashSet::with_capacity(page_size);
        let mut results: Vec<OrgListItem> = Vec::with_capacity(page_size);

        // cursor: None = first page, Some(log_id) = resume from this closed segment downward
        let start_log_id = match request.cursor {
            None => {
                let orgs = { self.shard_mem_cache.borrow().peek_segment_summary_orgs().clone() };
                for org_id in orgs {
                    if seen.insert(org_id) {
                        results.push(OrgListItem { org_id });
                    }
                }
                active_log_id.saturating_sub(1)
            }
            Some(log_id) => log_id,
        };

        if start_log_id == 0 {
            return Ok(ListOrgsResponse { correlation_id: request.correlation_id, orgs: results, next_cursor: None });
        }

        for log_id in (1..=start_log_id).rev() {
            if results.len() >= page_size || start_time.elapsed() >= max_duration {
                return Ok(ListOrgsResponse {
                    correlation_id: request.correlation_id, orgs: results,
                    next_cursor: Some(log_id),
                });
            }

            match read_segment_summary(self.log_segments_cache.shard_dir(), log_id).await {
                Some(payload) => {
                    for org_id in payload.orgs {
                        if seen.insert(org_id) {
                            results.push(OrgListItem { org_id });
                        }
                    }
                }
                None => {
                    self.list_orgs_legacy_segment(log_id, &mut seen, &mut results).await
                        .map_err(ShardListingError::ReadFromDiskError)?;
                }
            }
        }

        Ok(ListOrgsResponse { correlation_id: request.correlation_id, orgs: results, next_cursor: None })
    }

    async fn list_orgs_legacy_segment(
        &self,
        target_log_id: u64,
        seen: &mut HashSet<u128>,
        results: &mut Vec<OrgListItem>,
    ) -> Result<(), ScanError<()>> {
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache, target_log_id, None, self.config.read_max_chunk_size,
        );
        scanner.scan::<bool, ()>(|log_id, _pos, bytes| {
            if log_id != target_log_id { return Ok(Some(true)); }
            if !metablock_bytes::is_metablock_kind_event_batch_metadata(bytes) { return Ok(None); }
            let org_id = metablock_bytes::read_event_batch_org_id(bytes);
            if seen.insert(org_id) {
                results.push(OrgListItem { org_id });
            }
            Ok(None)
        }).await?;
        Ok(())
    }

    /// List aggregate types, optionally filtered by org_id.
    ///
    /// Reads segment summaries newest-to-oldest, falling back to reverse metablock
    /// scan for legacy segments without summaries.
    pub async fn list_aggregate_types(&self, request: ListAggregateTypesRequest) -> Result<ListAggregateTypesResponse, ShardListingError> {
        let _permit = self.list_semaphore.acquire_permit(1).await
            .map_err(|_| ShardListingError::ListSemaphoreClosed)?;

        let filter_org_id = request.org_id;

        let start_time = Instant::now();
        let max_duration = self.config.list_max_duration;
        let page_size = self.config.list_page_size;
        let active_log_id = self.log_segments_cache.active_log_id();

        let mut seen: HashSet<AggregateTypeKey> = HashSet::with_capacity(page_size);
        let mut results: Vec<AggregateTypeListItem> = Vec::with_capacity(page_size);

        let start_log_id = match request.cursor {
            None => {
                let types = { self.shard_mem_cache.borrow().peek_segment_summary_types().clone() };
                for atk in types {
                    if let Some(filter) = filter_org_id {
                        if atk.org_id != filter { continue; }
                    }
                    if seen.insert(atk.clone()) {
                        results.push(AggregateTypeListItem { org_id: atk.org_id, aggregate_type_id: atk.aggregate_type_id });
                    }
                }
                active_log_id.saturating_sub(1)
            }
            Some(log_id) => log_id,
        };

        if start_log_id == 0 {
            return Ok(ListAggregateTypesResponse { correlation_id: request.correlation_id, aggregate_types: results, next_cursor: None });
        }

        for log_id in (1..=start_log_id).rev() {
            if results.len() >= page_size || start_time.elapsed() >= max_duration {
                return Ok(ListAggregateTypesResponse {
                    correlation_id: request.correlation_id, aggregate_types: results,
                    next_cursor: Some(log_id),
                });
            }

            match read_segment_summary(self.log_segments_cache.shard_dir(), log_id).await {
                Some(payload) => {
                    for atk in payload.aggregate_types {
                        if let Some(filter) = filter_org_id {
                            if atk.org_id != filter { continue; }
                        }
                        if seen.insert(atk.clone()) {
                            results.push(AggregateTypeListItem { org_id: atk.org_id, aggregate_type_id: atk.aggregate_type_id });
                        }
                    }
                }
                None => {
                    self.list_types_legacy_segment(log_id, filter_org_id, &mut seen, &mut results).await
                        .map_err(ShardListingError::ReadFromDiskError)?;
                }
            }
        }

        Ok(ListAggregateTypesResponse { correlation_id: request.correlation_id, aggregate_types: results, next_cursor: None })
    }

    async fn list_types_legacy_segment(
        &self,
        target_log_id: u64,
        filter_org_id: Option<u128>,
        seen: &mut HashSet<AggregateTypeKey>,
        results: &mut Vec<AggregateTypeListItem>,
    ) -> Result<(), ScanError<()>> {
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache, target_log_id, None, self.config.read_max_chunk_size,
        );
        scanner.scan::<bool, ()>(|log_id, _pos, bytes| {
            if log_id != target_log_id { return Ok(Some(true)); }
            if !metablock_bytes::is_metablock_kind_event_batch_metadata(bytes) { return Ok(None); }
            let org_id = metablock_bytes::read_event_batch_org_id(bytes);
            if let Some(filter) = filter_org_id {
                if org_id != filter { return Ok(None); }
            }
            let aggregate_type_id = metablock_bytes::read_event_batch_aggregate_type_id(bytes);
            if seen.insert(AggregateTypeKey::new(org_id, aggregate_type_id)) {
                results.push(AggregateTypeListItem { org_id, aggregate_type_id });
            }
            Ok(None)
        }).await?;
        Ok(())
    }

    /// List aggregates, optionally filtered by org_id and/or aggregate_type_id.
    ///
    /// Reads segment summaries newest-to-oldest with delete barrier semantics.
    /// Falls back to reverse metablock scan for legacy segments without summaries.
    pub async fn list_aggregates(&self, request: ListAggregatesRequest) -> Result<ListAggregatesResponse, ShardListingError> {
        let _permit = self.list_semaphore.acquire_permit(1).await
            .map_err(|_| ShardListingError::ListSemaphoreClosed)?;
        let start_time = Instant::now();
        let max_duration = self.config.list_max_duration;
        let page_size = self.config.list_page_size;
        let active_log_id = self.log_segments_cache.active_log_id();
        let filter_org_id = request.org_id;
        let filter_aggregate_type_id = request.aggregate_type_id;

        struct AccumulatedStats {
            is_deleted: bool,
            event_batch_count: u64,
            min_event_batch_index: u64,
            max_event_batch_index: u64,
            max_server_timestamp: u64,
            compressed_size: u64,
            uncompressed_size: u64,
        }

        fn build_response(
            correlation_id: Option<u128>,
            result_order: Vec<AggregateKey>,
            seen: &HashMap<AggregateKey, AccumulatedStats>,
            next_cursor: Option<u64>,
        ) -> ListAggregatesResponse {
            let aggregates = result_order.into_iter()
                .filter_map(|key| {
                    seen.get(&key).map(|stats| AggregateListItem {
                        org_id: key.org_id,
                        aggregate_type_id: key.aggregate_type_id,
                        aggregate_id: key.aggregate_id,
                        is_deleted: stats.is_deleted,
                        event_batch_count: stats.event_batch_count,
                        min_event_timestamp: 0,
                        max_event_timestamp: 0,
                        min_event_batch_index: if stats.min_event_batch_index == u64::MAX { 0 } else { stats.min_event_batch_index },
                        max_event_batch_index: stats.max_event_batch_index,
                        min_event_index: 0,
                        max_event_index: 0,
                        min_server_timestamp: 0,
                        max_server_timestamp: stats.max_server_timestamp,
                        compressed_size: stats.compressed_size,
                        uncompressed_size: stats.uncompressed_size,
                    })
                })
                .collect();
            ListAggregatesResponse { correlation_id, aggregates, next_cursor }
        }

        fn process_aggregate_entry(
            org_id: u128, aggregate_type_id: u128, aggregate_id: u128,
            is_deleted: bool, event_batch_count: u64,
            min_event_batch_index: u64, last_event_batch_index: u64,
            last_server_timestamp: u64, compressed_size: u64, uncompressed_size: u64,
            filter_org_id: Option<u128>, filter_aggregate_type_id: Option<u128>,
            seen: &mut HashMap<AggregateKey, AccumulatedStats>,
            result_order: &mut Vec<AggregateKey>,
            deleted_barrier: &mut HashSet<AggregateKey>,
            unique_count: &mut usize,
        ) {
            if let Some(f) = filter_org_id { if org_id != f { return; } }
            if let Some(f) = filter_aggregate_type_id { if aggregate_type_id != f { return; } }

            let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
            if deleted_barrier.contains(&key) { return; }

            if is_deleted {
                deleted_barrier.insert(key.clone());
                if !seen.contains_key(&key) {
                    seen.insert(key.clone(), AccumulatedStats {
                        is_deleted: true, event_batch_count: 0,
                        min_event_batch_index: u64::MAX, max_event_batch_index: 0,
                        max_server_timestamp: 0, compressed_size: 0, uncompressed_size: 0,
                    });
                    result_order.push(key);
                    *unique_count += 1;
                }
                return;
            }

            if let Some(stats) = seen.get_mut(&key) {
                if !stats.is_deleted {
                    stats.event_batch_count += event_batch_count;
                    stats.min_event_batch_index = stats.min_event_batch_index.min(min_event_batch_index);
                    stats.max_event_batch_index = stats.max_event_batch_index.max(last_event_batch_index);
                    stats.max_server_timestamp = stats.max_server_timestamp.max(last_server_timestamp);
                    stats.compressed_size += compressed_size;
                    stats.uncompressed_size += uncompressed_size;
                }
            } else {
                seen.insert(key.clone(), AccumulatedStats {
                    is_deleted: false,
                    event_batch_count,
                    min_event_batch_index,
                    max_event_batch_index: last_event_batch_index,
                    max_server_timestamp: last_server_timestamp,
                    compressed_size,
                    uncompressed_size,
                });
                result_order.push(key);
                *unique_count += 1;
            }
        }

        let mut seen: HashMap<AggregateKey, AccumulatedStats> = HashMap::with_capacity(page_size);
        let mut result_order: Vec<AggregateKey> = Vec::with_capacity(page_size);
        let mut deleted_barrier: HashSet<AggregateKey> = HashSet::new();
        let mut unique_count = 0usize;
        macro_rules! process {
            ($org:expr, $atype:expr, $aid:expr, $del:expr, $ebc:expr,
             $min_ebi:expr, $last_ebi:expr, $last_ts:expr, $csz:expr, $usz:expr) => {
                process_aggregate_entry(
                    $org, $atype, $aid, $del, $ebc, $min_ebi, $last_ebi, $last_ts, $csz, $usz,
                    filter_org_id, filter_aggregate_type_id,
                    &mut seen, &mut result_order, &mut deleted_barrier, &mut unique_count,
                )
            }
        }

        // cursor: None = first page, Some(log_id) = resume from this closed segment downward
        let start_log_id = match request.cursor {
            None => {
                let summary = { self.shard_mem_cache.borrow().peek_segment_summary().clone() };
                for (key, entry) in &summary {
                    process!(key.org_id, key.aggregate_type_id, key.aggregate_id,
                        entry.is_deleted, entry.event_batch_count,
                        entry.min_event_batch_index, entry.last_event_batch_index,
                        entry.last_server_timestamp, entry.compressed_size, entry.uncompressed_size);
                }
                active_log_id.saturating_sub(1)
            }
            Some(log_id) => log_id,
        };

        if start_log_id == 0 {
            return Ok(build_response(request.correlation_id, result_order, &seen, None));
        }

        for log_id in (1..=start_log_id).rev() {
            // Check page limit between segments (not within)
            if unique_count >= page_size || start_time.elapsed() >= max_duration {
                return Ok(build_response(request.correlation_id, result_order, &seen, Some(log_id)));
            }

            match read_segment_summary(self.log_segments_cache.shard_dir(), log_id).await {
                Some(payload) => {
                    for entry in &payload.aggregates {
                        process!(entry.org_id, entry.aggregate_type_id, entry.aggregate_id,
                            entry.is_deleted, entry.event_batch_count,
                            entry.min_event_batch_index, entry.last_event_batch_index,
                            entry.last_server_timestamp, entry.compressed_size, entry.uncompressed_size);
                    }
                }
                None => {
                    // Legacy fallback: scan this single segment's metablocks
                    let mut scanner = ReverseMetablockScanner::new(
                        &self.log_segments_cache, log_id, None, self.config.read_max_chunk_size,
                    );
                    scanner.scan::<bool, ()>(|scan_log_id, _pos, bytes| {
                        if scan_log_id != log_id { return Ok(Some(true)); }

                        if metablock_bytes::is_metablock_kind_soft_delete(bytes) {
                            let ak = metablock_bytes::read_soft_delete_aggregate_key(bytes);
                            process!(ak.org_id, ak.aggregate_type_id, ak.aggregate_id,
                                        true, 0, u64::MAX, 0, 0, 0, 0);
                            return Ok(None);
                        }

                        if !metablock_bytes::is_metablock_kind_event_batch_metadata(bytes) {
                            return Ok(None);
                        }

                        let ak = metablock_bytes::read_event_batch_aggregate_key(bytes);
                        let ebi = metablock_bytes::read_event_batch_event_batch_index(bytes);
                        let ts = metablock_bytes::read_server_timestamp(bytes);
                        let csz = metablock_bytes::read_compressed_size(bytes);
                        let usz = metablock_bytes::read_uncompressed_size(bytes);
                        process!(ak.org_id, ak.aggregate_type_id, ak.aggregate_id,
                                    false, 1, ebi, ebi, ts, csz, usz);
                        Ok(None)
                    }).await.map_err(ShardListingError::ReadFromDiskError)?;
                }
            }
        }

        Ok(build_response(request.correlation_id, result_order, &seen, None))
    }
    
    pub async fn exists(&self, exists_request: &AggregateDetailsRequest) -> Result<AggregateDetailsResponse, ShardAggregateDetailsError> {
        self.aggregate_exists_and_cache(&exists_request.aggregate_key, CachePath::Read).await?;

        let snapshot = self
            .shard_mem_cache
            .borrow_mut()
            .get_aggregate_snapshot(&exists_request.aggregate_key, CachePath::Read);

        let snapshot = match snapshot {
            Some(s) if s.status == AggregateStatus::NotFound => {
                return Err(ShardAggregateDetailsError::AggregateNotExists);
            }
            Some(s) => s,
            None => return Err(ShardAggregateDetailsError::AggregateNotExists),
        };

        let is_deleted = snapshot.status == AggregateStatus::Deleted;

        // Read last metablock from disk for server_timestamp, client_id, user_id
        let (last_server_timestamp, last_client_id, last_user_id) =
            self.read_metablock_details(snapshot.log_id, snapshot.metablock_absolute_pos).await?;

        Ok(AggregateDetailsResponse {
            correlation_id: exists_request.correlation_id,
            min_event_batch_index: snapshot.min_event_batch_index,
            max_event_batch_index: snapshot.event_batch_index,
            max_event_index: snapshot.event_index,
            is_deleted,
            allow_recreate: snapshot.allow_recreate,
            allow_index_continuation: snapshot.allow_index_continuation,
            last_server_timestamp,
            last_client_id,
            last_user_id,
        })
    }

    /// Read a single metablock from disk and extract client_id, user_id, server_timestamp.
    async fn read_metablock_details(
        &self,
        log_id: u64,
        metablock_absolute_pos: u64,
    ) -> Result<(u64, u128, Option<u128>), ShardAggregateDetailsError> {
        let log_segment = self.log_segments_cache.get(log_id).await
            .map_err(|e| ShardAggregateDetailsError::MetablockReadError(format!("{:?}", e)))?;

        let guard = log_segment.lock_reader("exists_metablock_read").await
            .map_err(|e| ShardAggregateDetailsError::MetablockReadError(format!("{:?}", e)))?;

        let dma_file = guard.as_ref()
            .ok_or_else(|| ShardAggregateDetailsError::MetablockReadError("no file handle".into()))?;

        let buf = dma_file.read_at(metablock_absolute_pos, FIXED_BLOCK_SIZE_BYTES).await
            .map_err(|e| ShardAggregateDetailsError::MetablockReadError(format!("{:?}", e)))?;

        let (chunks, _) = (*buf).as_chunks::<FIXED_BLOCK_SIZE_BYTES>();
        let block = chunks.first()
            .ok_or_else(|| ShardAggregateDetailsError::MetablockReadError("empty read".into()))?;

        let metablock = deserialise_metablock(block)
            .map_err(|e| ShardAggregateDetailsError::MetablockReadError(format!("{:?}", e)))?;

        let (client_id, user_id) = match &metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(eb) => (eb.client_id, eb.user_id),
            MetablockKind::SoftDelete(sd) => (sd.client_id, sd.user_id),
            other @ (MetablockKind::SoftTrim(_) | MetablockKind::SchemaRegistration(_)) => {
                return Err(ShardAggregateDetailsError::MetablockReadError(
                    format!("unexpected metablock kind: {:?}", std::mem::discriminant(other)),
                ))
            }
        };

        Ok((metablock.server_timestamp, client_id, user_id))
    }

    pub async fn trim_start(&self, trim_request: TrimStartRequest) -> Result<SuccessResponse, ShardTrimError> {
        
        let lease_index = match self.node_status.get().effective_node_status() {
            NodeStatus::Leader { lease_index } => lease_index,
            NodeStatus::Standalone => 0,
            _ => return Err(ShardTrimError::ShardCannotAcceptWrites { leader_address: self.leader_client_address.borrow().clone() }),
        };

        let aggregate_key = &trim_request.aggregate_key;

        // Ensure aggregate exists
        if !self.aggregate_exists_and_cache(aggregate_key, CachePath::Write).await? {
            return Err(ShardTrimError::AggregateNotExists);
        }

        let current_indexes = self.shard_mem_cache.borrow_mut().get_write_event_indexes(aggregate_key);

        // Validate trim index is within valid range
        if trim_request.keep_from_event_batch_index <= current_indexes.min_event_batch_index {
            // Already trimmed to this point or beyond, nothing to do
            return Ok(SuccessResponse {
                correlation_id: trim_request.correlation_id,
            });
        }

        if trim_request.keep_from_event_batch_index > current_indexes.event_batch_index {
            return Err(ShardTrimError::TrimIndexOutOfRange {
                requested: trim_request.keep_from_event_batch_index,
                max_event_batch_index: current_indexes.event_batch_index,
            });
        }

        let server_timestamp = self.config.timestamp_config.now();

        let metablock_soft_trim = MetablockSoftTrim {
            aggregate_key: aggregate_key.clone(),
            keep_from_event_batch_index: trim_request.keep_from_event_batch_index,
            event_batch_index: current_indexes.event_batch_index,
            event_index: current_indexes.event_index,
            client_id: trim_request.client_id,
            user_id: trim_request.user_id,
        };

        let metablock = Metablock {
            wal_index: 0,
            server_timestamp,
            lease_index,
            node_id: self.config.node_id,
            compressed_size: 0,
            uncompressed_size: 0,
            datablock_version: 0,
            datablock_compression_type: 0,
            datablock: DatablockStorageKind::None,
            wal_metablock_type: MetablockKind::SoftTrim(metablock_soft_trim),
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
        };

        let shard_log_queue_item = ShardLogQueueItem::new(None, None, metablock);

        // Add to queue
        {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            shard_mem_cache.add_pending_trim_to_queue(
                aggregate_key,
                trim_request.keep_from_event_batch_index,
                shard_log_queue_item,
            );
        }

        // Wait for durable write
        self.sync_durable().await?;

        // Same deal for replication, if we are the leader, 
        // wait on durable replication, also batched
        self.replicate_durable().await?;

        Ok(SuccessResponse {
            correlation_id: trim_request.correlation_id,
        })
    }
    
    pub async fn delete(&self, delete_request: DeleteRequest) -> Result<SuccessResponse, ShardDeleteError> {
        
        let lease_index = match self.node_status.get().effective_node_status() {
            NodeStatus::Leader { lease_index } => lease_index,
            NodeStatus::Standalone => 0,
            _ => return Err(ShardDeleteError::ShardCannotAcceptWrites { leader_address: self.leader_client_address.borrow().clone() }),
        };

        // Make sure we have at least one aggregate to write
        if delete_request.deletes.is_empty() {
            return Err(ShardDeleteError::EmptyDeleteList);
        }

        let mut prepared_deletes = Vec::with_capacity(delete_request.deletes.len());
        for (aggregate_key, single_delete) in &delete_request.deletes {
            // Ensure aggregate snapshot is in memcache, loading from disk if necessary
            if !self.aggregate_exists_and_cache(aggregate_key, CachePath::Write).await? {
                return Err(ShardDeleteError::AggregateNotExists);
            }

            let aggregate_current_indexes = self.shard_mem_cache.borrow_mut().get_write_event_indexes(aggregate_key);

            // Validate optimistic concurrency
            if let Some(expected) = single_delete.expected_event_batch_index {
                if expected != aggregate_current_indexes.event_batch_index {
                    return Err(ShardDeleteError::OptimisticConcurrencyViolation {
                        expected_event_batch_index: expected,
                        current_event_batch_index: aggregate_current_indexes.event_batch_index,
                    });
                }
            }

            let server_timestamp = self.config.timestamp_config.now();

            let metablock_soft_delete = MetablockSoftDelete {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: aggregate_current_indexes.event_batch_index,
                event_index: aggregate_current_indexes.event_index,
                client_id: delete_request.client_id,
                user_id: delete_request.user_id,
                allow_recreate: single_delete.allow_recreate,
                allow_index_continuation: single_delete.allow_index_continuation,
            };
            let metablock = Metablock {
                wal_index: 0,
                server_timestamp,
                lease_index,
                node_id: self.config.node_id,
                compressed_size: 0,
                uncompressed_size: 0,
                datablock_version: 0,
                datablock_compression_type: 0,
                datablock: DatablockStorageKind::None,
                wal_metablock_type: MetablockKind::SoftDelete(metablock_soft_delete.clone()),
                previous_tip_hash: GENESIS_HASH,
                datablock_position: 0,
            };

            let shard_log_queue_item = ShardLogQueueItem::new(None, None, metablock);
            prepared_deletes.push((aggregate_key.clone(), metablock_soft_delete, shard_log_queue_item));
        }

        // Phase 2: Append all prepared deletes to queue - cannot fail
        {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            for (aggregate_key, soft_delete, shard_log_queue_item) in prepared_deletes {
                shard_mem_cache.add_pending_delete_to_queue(
                    &aggregate_key,
                    soft_delete.event_index,
                    soft_delete.event_batch_index,
                    soft_delete.allow_recreate,
                    soft_delete.allow_index_continuation,
                    shard_log_queue_item,
                );
            }
        }

        // Now we wait on disk write before ack to client
        self.sync_durable().await?;

        // Same deal for replication, if we are the leader, 
        // wait on durable replication, also batched
        self.replicate_durable().await?;

        Ok(SuccessResponse {
            correlation_id: delete_request.correlation_id,
        })
    }

    /// Write events to an aggregate.
    ///
    /// # Flow
    /// 1. Validate request (idempotency, optimistic concurrency)
    /// 2. Build datablock and metablock
    /// 3. Add to pending queue, assigning indexes (not yet visible for reads)
    /// 4. Wait for durability (based on config)
    /// 5. Return response with assigned indexes
    ///
    /// # Arguments
    /// * `lease_index` - Current lease for write authorization
    /// * `request` - Write request with events to append
    pub async fn write(&self, write_request: WriteRequest) -> Result<SuccessResponse, ShardWriteError> {
        
        let status = self.node_status.get();
        let lease_index = match status.effective_node_status() {
            NodeStatus::Leader { lease_index } => lease_index,
            NodeStatus::Standalone => 0,
            other => {
                debug!(effective = ?other, raw = ?status.raw(), expires_at_ms = status.lease_expires_at_ms(), now_ms = validated_node_status::unix_epoch_now_ms(), "Write rejected: not Leader/Standalone");
                return Err(ShardWriteError::ShardCannotAcceptWrites { leader_address: self.leader_client_address.borrow().clone() });
            }
        };

        if self.shard_mem_cache.borrow().is_replication_queue_pressured() {
            return Err(ShardWriteError::ReplicationBackpressure);
        }

        if let Some(t) = self.last_rollback_at.get() {
            if t.elapsed() < self.config.replication_rollback_cooldown {
                return Err(ShardWriteError::ReplicationBackpressure);
            }
        }

        // Make sure we have at least one aggregate to write
        if write_request.writes.is_empty() {
            return Err(ShardWriteError::EmptyEventsList);
        }

        // Phase 1: Validation and preparation - all checks that can fail happen here
        // No mutations to shard_mem_cache until all validations pass
        let total_events: usize = write_request.writes.values().map(|w| w.events.len()).sum();
        let total_payload_bytes: usize = write_request.writes.values()
            .flat_map(|w| w.events.iter())
            .map(|e| e.event_value.len())
            .sum();
        let mut prepared_writes = Vec::with_capacity(write_request.writes.len());

        for (aggregate_key, single_write) in write_request.writes {
            // Validate events list is not empty
            if single_write.events.is_empty() {
                return Err(ShardWriteError::EmptyEventsList);
            }

            // Validate that no event uses the sentinel 0 event type
            if let Some(ev) = single_write.events.iter().find(|e| e.event_type_major == 0) {
                return Err(ShardWriteError::ZeroEventType {
                    client_event_index: ev.client_event_index,
                });
            }

            // Ensure aggregate snapshot is in memcache, loading from disk if necessary
            // Returns true for Found, false for NotFound/Deleted
            let aggregate_exists = self.aggregate_exists_and_cache(&aggregate_key, CachePath::Write).await
                .map_err(ShardWriteError::AggregateExistsAndCacheError)?;
            
            if !aggregate_exists {
                // Check if it was deleted with allow_recreate = false
                let (is_loaded, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(&aggregate_key, CachePath::Write);
                if is_loaded && status == AggregateStatus::Deleted {
                    let indexes = self.shard_mem_cache.borrow_mut().get_write_event_indexes(&aggregate_key);
                    if !indexes.allow_recreate {
                        return Err(ShardWriteError::AggregateRecreateNotAllowed);
                    }
                    // allow_recreate is true, so we can proceed with create
                } else if !single_write.allow_create {
                    return Err(ShardWriteError::AggregateNotExists);
                }
            }

            let aggregate_client_key = AggregateClientKey::new(aggregate_key.clone(), write_request.client_id);

            // Prep work done outside of validate_and_prepare_write as it's async
            if single_write.enforce_client_idempotency {
                self.cache_aggregate_client(&aggregate_key, &aggregate_client_key).await
                    .map_err(ShardWriteError::CacheAggregateClientError)?;
            }

            // Pre-warm schema cache for validation
            self.pre_warm_schema_cache(&single_write.events, &aggregate_key).await
                .map_err(ShardWriteError::CacheAggregateClientError)?;

            // Validate and prepare - reads from memcache but does not mutate
            let prepared = self.validate_and_prepare_write(
                lease_index,
                &aggregate_key,
                write_request.client_id,
                write_request.user_id,
                single_write,
            )?;

            prepared_writes.push(prepared);
        }

        // Phase 2: Append all prepared writes to queue - cannot fail
        tracing::debug!(
            shard_id = self.config.shard_id,
            client_id = write_request.client_id,
            aggregate_count = prepared_writes.len(),
            total_events,
            total_payload_bytes,
            "Write request accepted",
        );
        self.append_prepared_writes_to_queue(prepared_writes);

        // Wait on disk write, it's batched for performance
        let fsync_start = std::time::Instant::now();
        self.sync_durable().await?;
        let fsync_ms = fsync_start.elapsed().as_millis() as u64;

        // Same deal for replication, if we are the leader,
        // wait on durable replication, also batched
        let repl_start = std::time::Instant::now();
        self.replicate_durable().await?;
        let repl_ms = repl_start.elapsed().as_millis() as u64;

        let total_ms = fsync_ms + repl_ms;
        if total_ms > 1000 {
            warn!(
                shard_id = self.config.shard_id,
                fsync_ms,
                repl_ms,
                total_ms,
                "Slow write: fsync + replication exceeded 1s",
            );
        }

        let shard_label = &self.metrics_shard_label;
        metrics::counter!("celeriant_write_events_total", shard_label).increment(total_events as u64);
        metrics::counter!("celeriant_write_bytes_total", shard_label).increment(total_payload_bytes as u64);

        Ok(SuccessResponse {
            correlation_id: write_request.correlation_id,
        })
    }

    /// Read event batches from an aggregate.
    /// Reads from cache when possible, falls back to disk for older batches.
    pub async fn read(&self, request: &ReadRequest) -> Result<ReadResponse, ShardReadError> {
        let aggregate_key = &request.aggregate_key;
        let filters = &request.filters;
        let max_bytes = self.config.max_response_size as u64;

        // 1. Ensure aggregate exists and is cached
        if !self.aggregate_exists_and_cache(aggregate_key, CachePath::Read).await? {
            let visible_wal_index = self.log_segments_cache.get_latest_read_cursor().wal_index;
            debug!(
                shard_id = self.config.shard_id,
                aggregate_key = %aggregate_key,
                visible_wal_index,
                "Read: aggregate not found after disk scan"
            );
            return Err(ShardReadError::AggregateNotExists);
        }

        let last_known = self.shard_mem_cache.borrow_mut().get_aggregate_last_metablock_pos(aggregate_key, CachePath::Read);

        // 2. Validate requested range is available (not trimmed)
        if filters.from_event_batch_index < last_known.min_event_batch_index {
            return Err(ShardReadError::UnavailableBatchIndex {
                minimum_available: last_known.min_event_batch_index,
                requested: filters.from_event_batch_index,
            });
        }

        // 3. Collect metablocks with size-bounded accumulation (NO datablocks yet)
        let mut collection = self.collect_metablocks_bounded(aggregate_key, filters, max_bytes, last_known).await?;

        // 4. Fetch datablocks only for kept metablocks
        fetch_datablocks_for_metablocks(&mut collection.kept_metablocks, self.config.read_max_chunk_size, &self.log_segments_cache).await?;

        // 5. Deserialize and apply event-level filters
        let event_batches = self.build_filtered_response(collection.kept_metablocks, filters);

        let read_bytes: u64 = event_batches.iter().map(|b| b.events.iter().map(|e| e.event_value.len() as u64).sum::<u64>()).sum();
        let shard_label = &self.metrics_shard_label;
        metrics::counter!("celeriant_read_bytes_total", shard_label).increment(read_bytes);

        Ok(ReadResponse {
            correlation_id: request.correlation_id,
            event_batches,
            next_event_batch_index: collection.next_event_batch_index,
        })
    }

    /// Close the shard WAL, flushing any pending writes.
    pub async fn close(&self) {
        self.log_segments_cache.close().await
    }

    /// Scan sealed segments oldest-first and compact the first eligible one.
    ///
    /// A segment is eligible if it is sealed (not the active segment) and fully
    /// replicated (`!is_pending_advance()`). Compaction is skipped if the reclaimable
    /// fraction is below `compaction_min_reclaimable_ratio`.
    ///
    /// Returns `Some(CompactionResult)` if a segment was compacted, `None` if no
    /// eligible segment was found or all eligible segments were below the threshold.
    pub async fn compact_oldest_eligible_segment(&self) -> Result<Option<CompactionResult>, CompactionError> {
        let active_log_id = self.log_segments_cache.active_log_id();

        // No sealed segments when active is the first log.
        if active_log_id <= 1 {
            return Ok(None);
        }

        let min_ratio = self.config.compaction_min_reclaimable_ratio;
        let temp_dir = &self.config.compaction_temp_dir;
        let mut segments_checked: u32 = 0;

        for log_id in 1..active_log_id {
            let result = compact_segment(
                log_id,
                &self.log_segments_cache,
                &self.shard_mem_cache,
                min_ratio,
                temp_dir,
            )
            .await;

            match result {
                // Segment compacted — return immediately (one per cycle).
                Ok(Some(r)) => return Ok(Some(r)),
                // Below threshold or pending advance — yield then try next segment.
                Ok(None) => {
                    segments_checked += 1;
                    glommio::yield_if_needed().await;
                    continue;
                }
                // Non-existent segment (e.g. gap in log_ids) — skip gracefully.
                Err(CompactionError::OpenSegment(_)) => continue,
                // Other error — surface to caller for logging.
                Err(e) => return Err(e),
            }
        }

        debug!(
            shard_id = self.config.shard_id,
            segments_checked,
            sealed_segments = active_log_id - 1,
            "Compaction: no eligible segments"
        );

        Ok(None)
    }

    /// Get the watched aggregates registry for this shard.
    pub fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
        self.watched_aggregates.clone()
    }

    async fn cache_aggregate_client(&self, aggregate_key: &AggregateKey, aggregate_client_key: &AggregateClientKey) -> Result<(), ShardCacheLoadError> {
        // If we are cached already
        if let (true, _exists) = self
            .shard_mem_cache
            .borrow_mut()
            .aggregate_client_load_status(aggregate_key, aggregate_client_key)
        {
            return Ok(());
        }

        // Take an exclusive lock on this aggregate client to deduplicate thundering herd
        let aggregate_lock = self.aggregate_client_loading.acquire(aggregate_client_key);
        let _ = write_with_timeout(&aggregate_lock, "cache_aggregate_client").await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        // We have exclusive access now, check if another concurrent task has already done the work
        if let (true, _exists) = self
            .shard_mem_cache
            .borrow_mut()
            .aggregate_client_load_status(aggregate_key, aggregate_client_key)
        {
            return Ok(());
        }

        // Limit concurrent disk scans across different aggregates (NVMe starvation)
        let _cache_permit = self.cache_load_semaphore.acquire_permit(1).await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        let last_known_metablock = self.shard_mem_cache.borrow_mut().get_aggregate_last_metablock_pos(aggregate_key, CachePath::Write);

        // Begin the search from the last_known_metablock, moving backwards
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            last_known_metablock.log_id,
            Some(last_known_metablock.metablock_absolute_pos.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64)), //Include SELF
            self.config.read_max_chunk_size,
        )
        .with_bloom_filter(aggregate_key);

        let find_result = scanner
            .scan::<bool, ()>(|_log_id, _metablock_absolute_pos, metablock_bytes| {
                if !metablock_bytes::is_matches_aggregate_key(metablock_bytes, aggregate_key) {
                    return Ok(None);
                }

                let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
                let low_priority = client_id != aggregate_client_key.client_id;
                let target_aggregate_client_key = AggregateClientKey::new(aggregate_key.clone(), client_id);

                let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

                // Not the aggregate we are searching for. Can we eager cache it? If not, skip it.
                if low_priority && shard_mem_cache.is_aggregate_client_cache_full_or_contains(&target_aggregate_client_key) {
                    return Ok(None);
                }

                let last_client_event_index = metablock_bytes::read_event_batch_max_client_event_index(metablock_bytes);

                shard_mem_cache.put_aggregate_client_into_cache(target_aggregate_client_key, last_client_event_index, low_priority);

                if low_priority {
                    Ok(None) //Haven't found aggregate client yet
                } else {
                    Ok(Some(true)) //Done searching
                }
            })
            .await
            .map_err(ShardCacheLoadError::FileScanningError)?;

        let found = find_result.unwrap_or(false);
        if !found {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            shard_mem_cache.put_aggregate_into_cache_as_not_found(aggregate_key.clone(), CachePath::Write);
        }

        Ok(())
    }

    /// Pre-warm schema cache for all unique SchemaKeys in the write request.
    /// Called before validate_and_prepare_write() so all lookups are cache hits.
    async fn pre_warm_schema_cache(&self, events: &[celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent], aggregate_key: &AggregateKey) -> Result<(), ShardCacheLoadError> {
        use celeriant_memcache::cached_schema::UniqueSchemaKeys;

        let mut unique_keys = UniqueSchemaKeys::new();
        for event in events {
            if event.iv.is_some() { continue; }

            let schema_key = SchemaKey::new(
                aggregate_key.org_id,
                aggregate_key.aggregate_type_id,
                event.event_type_major,
                event.event_type_minor,
            );
            unique_keys.try_insert(schema_key);
        }

        for schema_key in unique_keys.iter() {
            self.ensure_schema_cached(schema_key).await?;
        }

        Ok(())
    }

    /// Load a single schema key into cache via reverse WAL scan if not already cached.
    async fn ensure_schema_cached(&self, schema_key: &SchemaKey) -> Result<(), ShardCacheLoadError> {
        if self.shard_mem_cache.borrow_mut().schema_cache_contains(schema_key) {
            return Ok(());
        }

        let schema_lock = self.schema_loading.acquire(schema_key);
        let _ = write_with_timeout(&schema_lock, "ensure_schema_cached").await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        if self.shard_mem_cache.borrow_mut().schema_cache_contains(schema_key) {
            return Ok(());
        }

        // Limit concurrent disk scans across different aggregates (NVMe starvation)
        let _cache_permit = self.cache_load_semaphore.acquire_permit(1).await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        let starting_log_id = self.log_segments_cache.active_log_id();
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            starting_log_id,
            None,
            self.config.read_max_chunk_size,
        )
        .with_bloom_filter_hash(schema_key.hash_bytes());

        let found_metablock = scanner
            .scan::<(u64, Metablock), ()>(|log_id, _metablock_absolute_pos, metablock_bytes| {
                if !metablock_bytes::is_schema_registration_for_key(metablock_bytes, schema_key) {
                    return Ok(None);
                }

                let metablock = deserialise_metablock(metablock_bytes)
                    .map_err(|_| ())?;

                if let MetablockKind::SchemaRegistration(_) = metablock.wal_metablock_type {
                    return Ok(Some((log_id, metablock)));
                }

                Ok(None)
            })
            .await
            .map_err(ShardCacheLoadError::FileScanningError)?;

        match found_metablock {
            Some((log_id, metablock)) => {
                let mut batch = [crate::collect_from_disk::EventBatchFromLogSegmentFile {
                    log_id,
                    metablock,
                    datablock: None,
                }];

                crate::collect_from_disk::fetch_datablocks_for_metablocks(&mut batch, self.config.read_max_chunk_size, &self.log_segments_cache)
                    .await
                    .map_err(|e| ShardCacheLoadError::DatablockReadError(format!("{e:?}")))?;

                let [batch] = batch;
                let mut cache = self.shard_mem_cache.borrow_mut();
                if let Some(ref datablock) = batch.datablock {
                    compile_and_cache_schema(&mut cache, schema_key, datablock);
                }
            }
            None => {
                self.shard_mem_cache.borrow_mut().no_schema_cache_insert(schema_key.clone());
            }
        }

        Ok(())
    }

    async fn aggregate_exists_and_cache(&self, searching_for_aggregate_key: &AggregateKey, cache_path: CachePath) -> Result<bool, ShardCacheLoadError> {
        // If we are cached already
        if let (true, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key, cache_path) {
            trace!(
                shard_id = self.config.shard_id,
                aggregate_key = %searching_for_aggregate_key,
                ?cache_path,
                found = (status == AggregateStatus::Found),
                "Cache hit — no disk scan needed"
            );
            return Ok(status == AggregateStatus::Found);
        }

        // Take an exclusive lock on this aggregate to deduplicate thundering herd
        let aggregate_lock = self.aggregate_loading.acquire(searching_for_aggregate_key);
        let _ = write_with_timeout(&aggregate_lock, "move_aggregate_to_memcache").await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        // We have exclusive access now, check if another concurrent task has already done the work
        if let (true, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key, cache_path) {
            return Ok(status == AggregateStatus::Found);
        }

        // Limit concurrent disk scans across different aggregates (NVMe starvation)
        let _cache_permit = self.cache_load_semaphore.acquire_permit(1).await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        let (starting_log_id, start_from_postion) = match cache_path {
            CachePath::Read => {
                let read_cursor = self.log_segments_cache.get_latest_read_cursor();
                (read_cursor.log_id, Some(read_cursor.metablocks_position))
            },
            CachePath::Write => (self.log_segments_cache.active_log_id(), None),
        };

        // Begin the search from the active log, moving backwards
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            starting_log_id,
            start_from_postion,
            self.config.read_max_chunk_size,
        )
        .with_bloom_filter(searching_for_aggregate_key);

        let find_result = scanner
            .scan::<bool, ()>(|log_id, metablock_absolute_pos, metablock_bytes| {
                // Check for soft delete first - if this aggregate was deleted, we're done
                if metablock_bytes::is_soft_delete_for_aggregate(metablock_bytes, searching_for_aggregate_key) {
                    // Deserialize to get the deletion options
                    let metablock = deserialise_metablock(metablock_bytes)
                        .map_err(|_| ())?;
                    
                    if let MetablockKind::SoftDelete(soft_delete) = metablock.wal_metablock_type {
                        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
                        shard_mem_cache.put_aggregate_into_cache_as_deleted(
                            searching_for_aggregate_key.clone(),
                            log_id,
                            metablock_absolute_pos,
                            soft_delete.event_index,
                            soft_delete.event_batch_index,
                            soft_delete.allow_recreate,
                            soft_delete.allow_index_continuation,
                            cache_path,
                        );
                    }
                    return Ok(Some(false)); // Found but deleted
                }

                // SoftTrim carries full aggregate state — cache and stop scanning
                if metablock_bytes::is_soft_trim_for_aggregate(metablock_bytes, searching_for_aggregate_key) {
                    let metablock = deserialise_metablock(metablock_bytes)
                        .map_err(|_| ())?;
                    if let MetablockKind::SoftTrim(soft_trim) = metablock.wal_metablock_type {
                        let snapshot = MemSnapshotAggregate::found(
                            log_id,
                            metablock_absolute_pos,
                            soft_trim.event_index,
                            soft_trim.event_batch_index,
                            soft_trim.keep_from_event_batch_index,
                        );
                        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
                        shard_mem_cache.put_aggregate_snapshot_only(
                            searching_for_aggregate_key.clone(),
                            snapshot,
                            false,
                            cache_path,
                        );
                    }
                    return Ok(Some(true));
                }

                if !metablock_bytes::is_metablock_kind_event_batch_metadata(metablock_bytes) {
                    return Ok(None);
                }

                let current_aggregate_key = metablock_bytes::read_event_batch_aggregate_key(metablock_bytes);
                let low_priority = *searching_for_aggregate_key != current_aggregate_key;

                let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

                // Not the aggregate we are searching for. Can we eager cache it? If not, skip it.
                if low_priority && shard_mem_cache.is_aggregate_snapshot_full_or_contains(&current_aggregate_key, cache_path) {
                    return Ok(None);
                }

                let min_event_batch_index = metablock_bytes::read_event_batch_min_event_batch_index(metablock_bytes);

                let snapshot = MemSnapshotAggregate::found(
                    log_id,
                    metablock_absolute_pos,
                    metablock_bytes::read_event_batch_max_event_index(metablock_bytes),
                    metablock_bytes::read_event_batch_event_batch_index(metablock_bytes),
                    min_event_batch_index,
                );

                let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
                let last_client_event_index = metablock_bytes::read_event_batch_max_client_event_index(metablock_bytes);

                shard_mem_cache.put_aggregate_into_cache(current_aggregate_key, snapshot, client_id, last_client_event_index, low_priority, cache_path);

                if low_priority {
                    Ok(None) //Haven't found aggregate yet
                } else {
                    Ok(Some(true)) //Done searching
                }
            })
            .await
            .map_err(ShardCacheLoadError::FileScanningError)?;

        let found = find_result.unwrap_or(false);
        trace!(
            shard_id = self.config.shard_id,
            aggregate_key = %searching_for_aggregate_key,
            ?cache_path,
            found,
            "Disk scan complete"
        );
        if find_result.is_none() {
            // Never found any metablock for this aggregate
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            shard_mem_cache.put_aggregate_into_cache_as_not_found(searching_for_aggregate_key.clone(), cache_path);
        }

        Ok(found)
    }

    /// Validate a write request and prepare all data for appending.
    /// This performs read-only access to shard_mem_cache and can fail.
    fn validate_and_prepare_write(
        &self,
        lease_index: u64,
        aggregate_key: &AggregateKey,
        client_id: u128,
        user_id: Option<u128>,
        mut write_request: SingleAggregateWrite,
    ) -> Result<PreparedWrite, ShardWriteError> {
        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

        let aggregate_current_indexes = shard_mem_cache.get_write_event_indexes(aggregate_key);

        // There is a soft delete entry in the queue that hasn't been committed yet
        if aggregate_current_indexes.pending_delete_or_deleted && !aggregate_current_indexes.allow_recreate {
            return Err(ShardWriteError::AggregateRecreateNotAllowed);
        }

        // Validate optimistic concurrency (only for existing aggregates, not recreates)
        if let Some(expected) = write_request.expected_event_batch_index {
            if expected != aggregate_current_indexes.event_batch_index {
                debug!(
                    shard_id = self.config.shard_id,
                    aggregate_key = %aggregate_key,
                    expected_event_batch_index = expected,
                    current_event_batch_index = aggregate_current_indexes.event_batch_index,
                    "Write rejected: optimistic concurrency violation"
                );
                return Err(ShardWriteError::OptimisticConcurrencyViolation {
                    expected_event_batch_index: expected,
                    current_event_batch_index: aggregate_current_indexes.event_batch_index,
                });
            }
        }

        // Validate client idempotency
        if write_request.enforce_client_idempotency {
            if let Some(last_client_event_index) = shard_mem_cache.get_client_event_index(aggregate_key, client_id) {
                let attempted_client_event_index = write_request.events.iter().map(|e| e.client_event_index).min().unwrap_or(0);
                if attempted_client_event_index <= last_client_event_index {
                    debug!(
                        shard_id = self.config.shard_id,
                        aggregate_key = %aggregate_key,
                        client_id = %celeriant_wal::format_uuid(client_id),
                        last_client_event_index,
                        attempted_client_event_index,
                        "Write rejected: client idempotency violation"
                    );
                    return Err(ShardWriteError::ClientIdempotencyViolation {
                        last_client_event_index,
                        attempted_client_event_index,
                    });
                }
            }
        }

        // Schema validation
        for event in &write_request.events {
            if event.iv.is_some() { continue; }

            let schema_key = SchemaKey::new(
                aggregate_key.org_id,
                aggregate_key.aggregate_type_id,
                event.event_type_major,
                event.event_type_minor,
            );

            match shard_mem_cache.schema_cache_get(&schema_key) {
                Some(celeriant_memcache::cached_schema::CachedSchema::Validated(validator)) => {
                    validator.validate(&event.event_value).map_err(|e| {
                        ShardWriteError::SchemaValidationFailed {
                            event_type_major: event.event_type_major,
                            event_type_minor: event.event_type_minor,
                            client_event_index: event.client_event_index,
                            validation_error: e,
                        }
                    })?;
                }
                Some(celeriant_memcache::cached_schema::CachedSchema::CompilationFailed(err)) => {
                    return Err(ShardWriteError::SchemaCompilationFailed {
                        event_type_major: event.event_type_major,
                        event_type_minor: event.event_type_minor,
                        client_event_index: event.client_event_index,
                        compilation_error: err.clone(),
                    });
                }
                _ => {}
            }
        }

        // Drop the borrow before doing potentially expensive serialization
        drop(shard_mem_cache);

        // Determine starting indexes based on whether this is a recreate with index continuation
        let is_recreate = aggregate_current_indexes.pending_delete_or_deleted 
            || aggregate_current_indexes.event_batch_index == 0;
        
        let (mut event_index, event_batch_index, mut min_event_batch_index) = if is_recreate && aggregate_current_indexes.allow_index_continuation {
            // Continue from pre-deletion indexes
            (
                aggregate_current_indexes.event_index,
                aggregate_current_indexes.event_batch_index.saturating_add(1),
                aggregate_current_indexes.min_event_batch_index,
            )
        } else if is_recreate {
            // Fresh start
            (0, FIRST_EVENT_BATCH_INDEX, FIRST_EVENT_BATCH_INDEX)
        } else {
            // Normal append to existing aggregate
            (
                aggregate_current_indexes.event_index,
                aggregate_current_indexes.event_batch_index.saturating_add(1),
                aggregate_current_indexes.min_event_batch_index,
            )
        };

        if min_event_batch_index == 0 {
            min_event_batch_index = FIRST_EVENT_BATCH_INDEX;
        }

        // Prepare event data
        let mut events_in_batch = std::mem::take(&mut write_request.events);

        for e in events_in_batch.iter_mut() {
            event_index = event_index.saturating_add(1);
            e.event_index = event_index;
        }

        let event_type_extraction = extract_unique_event_types(&events_in_batch);
        let event_types_data = if event_type_extraction.needs_bloom {
            let bloom_bytes = self.bloom_filter_cache.create_bloom_bytes(&events_in_batch);
            EventTypesKind::Bloom(bloom_bytes)
        } else {
            EventTypesKind::Direct(event_type_extraction.event_types)
        };

        let datablock_aggregate_event_batch = DatablockAggregateEventBatch {
            event_batch_index,
            events: events_in_batch,
        };

        let metablock_event_batch = MetablockEventBatch::from_batch_item(
            client_id,
            user_id,
            aggregate_key.clone(),
            min_event_batch_index,
            &datablock_aggregate_event_batch,
            event_types_data,
        );
        let latest_client_event_index = metablock_event_batch.max_client_event_index;

        let datablock = Datablock {
            datablock_kind: DatablockKind::EventBatchItem(datablock_aggregate_event_batch),
        };

        // Serialize datablock - this can fail
        let serialized_datablock = SerialisedDatablock::new(&datablock, CompressionType::from_tuple(write_request.compression_type_id, write_request.compression_level))
            .map_err(ShardWriteError::FailedToSerialiseDatablocks)?;

        let server_timestamp = self.config.timestamp_config.now();

        let metablock = Metablock {
            wal_index: 0,
            server_timestamp,
            lease_index,
            node_id: self.config.node_id,
            uncompressed_size: serialized_datablock.uncompressed_size,
            compressed_size: serialized_datablock.compressed_size,
            datablock_version: serialized_datablock.datablock_version,
            datablock_compression_type: serialized_datablock.compression_type,
            datablock: serialized_datablock.storage_kind,
            wal_metablock_type: MetablockKind::EventBatchMetadata(metablock_event_batch),
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
        };

        let shard_log_queue_item = ShardLogQueueItem::new(Some(datablock), serialized_datablock.external_data, metablock);

        Ok(PreparedWrite {
            aggregate_key: aggregate_key.clone(),
            client_id,
            event_index,
            event_batch_index,
            latest_client_event_index,
            shard_log_queue_item,
            min_event_batch_index,
        })
    }

    /// Append all prepared writes to the pending queue.
    /// This mutates shard_mem_cache but cannot fail.
    fn append_prepared_writes_to_queue(&self, prepared_writes: Vec<PreparedWrite>) {
        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

        for prepared in prepared_writes {
            shard_mem_cache.add_to_pending_append_queue(
                &prepared.aggregate_key,
                prepared.event_index,
                prepared.event_batch_index,
                prepared.min_event_batch_index,
                prepared.client_id,
                prepared.latest_client_event_index,
                prepared.shard_log_queue_item,
            );
        }
    }

    /// Perform sync with potential delay for amortisation.
    /// Uses two-phase sync to fix the race condition where clearing the orchestrator
    /// before taking the snapshot can cause a subsequent leader to find an empty queue.
    async fn sync_durable(&self) -> Result<(), ShardFsyncError> {
        let rotating_log_cache = self.log_segments_cache.clone();
        let shard_mem_cache = self.shard_mem_cache.clone();
        let watched_aggregates = self.watched_aggregates.clone();
        let shard_id = self.config.shard_id;

        // Node status goes into fsync because we need to know if we should advance read position (standalone or follower mode)
        // We already pass lease status checks so can use raw()
        let node_status = self.node_status.get().raw();

        let mc_capture = shard_mem_cache.clone();
        self.fsync_coordinator
            .request_sync_two_phase(
                Some(self.config.fsync_delay),
                move || async move { capture_fsync_snapshot(&mc_capture) },
                move |captured| commit_fsync_with_rollback(node_status, rotating_log_cache, shard_mem_cache, watched_aggregates, captured, shard_id),
            )
            .await
    }

    async fn replicate_durable(&self) -> Result<(), ReplicationError> {
        let replication_client = self.replication_client.clone();
        let fsync_coordinator = self.fsync_coordinator.clone();
        let rotating_log_cache = self.log_segments_cache.clone();
        let shard_mem_cache = self.shard_mem_cache.clone();
        let watched_aggregates = self.watched_aggregates.clone();
        let node_status = self.node_status.clone();
        let max_catchup_gap_bytes = self.config.max_catchup_gap_bytes;
        let max_request_size = self.config.max_request_size;
        let read_max_chunk_size = self.config.read_max_chunk_size;
        let shard_id = self.config.shard_id;
        let last_rollback_at = self.last_rollback_at.clone();

        if !self.node_status.get().raw().is_leader() {
            return Ok(());
        }

        let follower_reachable = replication_client.is_follower_reachable();
        let delay = if follower_reachable {
            self.config.replication_delay
        } else {
            self.config.s3_replication_delay
        };

        let mc_capture = shard_mem_cache.clone();
        self.replication_coordinator
            .request_sync_two_phase(
                Some(delay),
                move || async move { capture_replication_snapshot(&mc_capture) },
                move |captured| commit_replication_with_rollback(replication_client, fsync_coordinator, rotating_log_cache, shard_mem_cache, watched_aggregates, node_status, last_rollback_at, captured, max_catchup_gap_bytes, max_request_size, read_max_chunk_size, shard_id),
            )
            .await
    }
    
    pub async fn handle_replication_batch(
        &self, request: celeriant_msg::request::requests::ReplicationBatchRequest
    ) -> Result<ReplicationBatchResponse, FollowerReplicationWriteError> {
        let follower_timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before Unix epoch")
            .as_millis() as u64;

        let response = |result| ReplicationBatchResponse {
            correlation_id: request.correlation_id,
            follower_timestamp_ms,
            result,
        };

        let leader_lease_index = match self.node_status.get().effective_node_status() {
            NodeStatus::Follower { leader_lease_index } => leader_lease_index,
            _ => return Ok(response(ReplicationResult::Rejected(FollowerRejection::NotAFollower))),
        };

        if follower_timestamp_ms.saturating_sub(request.leader_timestamp_ms) > self.config.max_clock_drift_ms {
            return Ok(response(ReplicationResult::Rejected(FollowerRejection::TimeDriftTooHigh {
                leader_ms: request.leader_timestamp_ms,
                follower_ms: follower_timestamp_ms,
                max_allowed_ms: self.config.max_clock_drift_ms,
            })));
        }

        if request.batches.is_empty() {
            return Ok(response(ReplicationResult::Rejected(FollowerRejection::EmptyBatch)));
        }

        let batch_lease_index = request.batches[0].metablock.lease_index;
        if batch_lease_index < leader_lease_index {
            return Ok(response(ReplicationResult::Rejected(FollowerRejection::StaleLease {
                follower_lease_index: leader_lease_index,
                received_lease_index: batch_lease_index,
            })));
        }

        match shard_wal_s3_catchup::apply_external_batch(
            &self.log_segments_cache, &self.shard_mem_cache, &request.batches,
        ) {
            Ok(()) => {}
            Err(ApplyBatchError::WalIndexMismatch { current, .. }) => {
                return Ok(response(ReplicationResult::Rejected(FollowerRejection::WalIndexMismatch {
                    max_follower_wal_index: current,
                })));
            }
            Err(ApplyBatchError::TipHashMismatch { current, current_wal_index, batch, batch_wal_index }) => {
                return Ok(response(ReplicationResult::Rejected(FollowerRejection::TipHashMismatch {
                    follower: current,
                    follower_wal_index: current_wal_index,
                    leader: batch,
                    leader_wal_index: batch_wal_index,
                })));
            }
            Err(ApplyBatchError::MissingDatablock) => {
                return Ok(response(ReplicationResult::Rejected(FollowerRejection::MissingDatablock)));
            }
            Err(ApplyBatchError::SerialiseDatablocks(e)) => {
                return Err(FollowerReplicationWriteError::FailedToSerialiseDatablocks(e));
            }
            Err(ApplyBatchError::BlockBecameInline) => {
                return Err(FollowerReplicationWriteError::BlockBecameInline);
            }
            Err(ApplyBatchError::BatchWalIndexGap { index, expected, actual }) => {
                return Err(FollowerReplicationWriteError::BatchWalIndexGap { index, expected, actual });
            }
        }

        // Track the first WAL index of this batch so we can upload it to S3 on promotion.
        // Covers the case where the leader rolled back this batch but we (the follower) kept it.
        {
            let active = self.log_segments_cache.active();
            active.metadata.borrow_mut().last_received_replication_wal_index =
                request.batches[0].metablock.wal_index;
        }

        self.sync_durable().await
            .map_err(FollowerReplicationWriteError::ShardFSyncError)?;

        let shard_label = &self.metrics_shard_label;
        let applied_bytes: u64 = request.batches.iter().map(|b| b.metablock.uncompressed_size).sum();
        metrics::counter!("celeriant_replication_applied_events_total", shard_label).increment(request.batches.len() as u64);
        metrics::counter!("celeriant_replication_applied_bytes_total", shard_label).increment(applied_bytes);

        Ok(response(ReplicationResult::Success {
            last_follower_metablock: None,
        }))
    }

    /// Upload the last TCP-replicated batch to S3 on promotion to leader.
    ///
    /// When this node was a follower, the leader may have rolled back its last batch
    /// after failing to get our ACK (partition). We kept the batch. On promotion,
    /// upload it to S3 so the old leader can catch up without a gap.
    pub async fn upload_s3_promotion_batch(&self) -> Result<(), crate::error::replication_to_s3_error::ReplicateToS3Error> {
        let (start_wal_index, current_wal_index) = {
            let active = self.log_segments_cache.active();
            let metadata = active.metadata.borrow();
            (metadata.last_received_replication_wal_index, metadata.write.wal_index)
        };

        if start_wal_index == 0 || start_wal_index > current_wal_index {
            return Ok(());
        }

        let shard_id = self.config.shard_id;
        let read_max_chunk_size = self.config.read_max_chunk_size;

        // Scan backwards from WAL tip to collect entries from start_wal_index onward
        let current_log_id = self.log_segments_cache.active_log_id();
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache, current_log_id, None, read_max_chunk_size,
        );

        let mut items: Vec<EventBatchFromLogSegmentFile> = vec![];
        let _scan_result = scanner
            .scan(|log_id, _pos, bytes| {
                let wal_index = metablock_bytes::read_wal_index(bytes);
                if wal_index < start_wal_index {
                    return Ok(Some(()));
                }
                let metablock = deserialise_metablock(bytes)?;
                items.push(EventBatchFromLogSegmentFile { log_id, metablock, datablock: None });
                Ok::<Option<()>, DiskFormatError>(None)
            })
            .await
            .map_err(|e| crate::error::replication_to_s3_error::ReplicateToS3Error::SerializationFailed(
                format!("Failed to scan WAL for promotion batch: {e:?}"),
            ))?;

        if items.is_empty() {
            return Ok(());
        }

        items.reverse();

        fetch_datablocks_for_metablocks(&mut items, read_max_chunk_size, &self.log_segments_cache)
            .await
            .map_err(|e| crate::error::replication_to_s3_error::ReplicateToS3Error::SerializationFailed(
                format!("Failed to fetch datablocks for promotion batch: {e:?}"),
            ))?;

        let batch_items: Vec<celeriant_msg::request::requests::ReplicationBatchItem> = items
            .into_iter()
            .map(|e| celeriant_msg::request::requests::ReplicationBatchItem {
                metablock: e.metablock,
                datablock: e.datablock,
            })
            .collect();

        let batch_count = batch_items.len();
        info!(shard_id, batch_count, start_wal_index, current_wal_index, "Uploading promotion batch to S3");

        self.replication_client.replicate_to_s3(batch_items).await?;

        // Clear the field now that S3 has the data
        {
            let active = self.log_segments_cache.active();
            active.metadata.borrow_mut().last_received_replication_wal_index = 0;
        }

        Ok(())
    }
}

/// Intermediate struct for validated and prepared write data
struct PreparedWrite {
    aggregate_key: AggregateKey,
    client_id: u128,
    event_index: u64,
    event_batch_index: u64,
    min_event_batch_index: u64,
    latest_client_event_index: u64,
    shard_log_queue_item: ShardLogQueueItem,
}

/// Result of size-bounded metablock collection
struct MetablockCollection {
    /// Metablocks that fit within max_bytes, sorted by batch index ascending
    kept_metablocks: Vec<EventBatchFromLogSegmentFile>,
    /// If we hit the size limit, this is the next batch index to continue from
    next_event_batch_index: Option<u64>,
}

impl<R: ReplicationClient + 'static, D: S3Downloader + 'static> ShardWal<R, D> {
    fn get_batch_index(metablock: &Metablock) -> u64 {
        match &metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(m) => m.event_batch_index,
            _ => 0,
        }
    }

    async fn collect_metablocks_bounded(
        &self,
        aggregate_key: &AggregateKey,
        filters: &ReadFilters,
        max_bytes: u64,
        last_known: MetablockPosition,
    ) -> Result<MetablockCollection, ScanError<DiskFormatError>> {
        let mut kept: Vec<EventBatchFromLogSegmentFile> = Vec::new();
        let mut cumulative_size: u64 = 0;
        let mut evicted_batch_index: Option<u64> = None;

        // Try cache first (iterates forward from from_event_batch_index)
        self.collect_from_cache_bounded(
            aggregate_key,
            filters,
            max_bytes,
            &mut kept,
            &mut cumulative_size,
            &mut evicted_batch_index,
        );

        // Check if we need disk (cache doesn't cover from_event_batch_index)
        let cache_min_batch = kept.first().map(|k| Self::get_batch_index(&k.metablock));
        let need_disk = cache_min_batch.map(|min| min > filters.from_event_batch_index).unwrap_or(true);

        if need_disk && last_known.metablock_absolute_pos > 0 {
            let disk_to = cache_min_batch.map(|min| min.saturating_sub(1));

            self.collect_from_disk_bounded(
                aggregate_key,
                filters.from_event_batch_index,
                disk_to.or(filters.to_event_batch_index),
                last_known,
                filters,
                max_bytes,
                &mut kept,
                &mut cumulative_size,
                &mut evicted_batch_index,
            )
            .await?;
        }

        Ok(MetablockCollection {
            kept_metablocks: kept,
            next_event_batch_index: evicted_batch_index,
        })
    }

    fn collect_from_cache_bounded(
        &self,
        aggregate_key: &AggregateKey,
        filters: &ReadFilters,
        max_bytes: u64,
        kept: &mut Vec<EventBatchFromLogSegmentFile>,
        cumulative_size: &mut u64,
        evicted_batch_index: &mut Option<u64>,
    ) {
        let shard_mem_cache = self.shard_mem_cache.borrow();
        let log_segments_cache = self.log_segments_cache.get_latest_read_cursor();
        let kept_before = kept.len();

        // Cache iterates forward (ascending batch index) from from_event_batch_index
        for (batch_idx, write) in shard_mem_cache.get_cached_writes_from(aggregate_key, filters.from_event_batch_index, log_segments_cache.wal_index) {
            // Stop if past upper bound
            if filters.to_event_batch_index.map_or(false, |to| batch_idx > to) {
                break;
            }

            // Apply metablock-level filters
            if !in_memory_filtering::is_include_batch(&write.metablock, filters) {
                continue;
            }

            let batch_size = write.metablock.uncompressed_size;

            // Check if adding this batch would exceed budget
            // (allow at least one batch even if over budget)
            if *cumulative_size + batch_size > max_bytes && !kept.is_empty() {
                *evicted_batch_index = Some(batch_idx);
                break;
            }

            *cumulative_size += batch_size;
            kept.push(EventBatchFromLogSegmentFile {
                log_id: 0, // Cache entries don't need log_id for fetch
                metablock: write.metablock.clone(),
                datablock: write.datablock.clone(),
            });
        }

        let cache_hits = (kept.len() - kept_before) as u64;
        if cache_hits > 0 {
            metrics::counter!("celeriant_cache_recent_write_hits_total").increment(cache_hits);
        } else {
            metrics::counter!("celeriant_cache_recent_write_misses_total").increment(1);
        }
    }

    async fn collect_from_disk_bounded(
        &self,
        aggregate_key: &AggregateKey,
        from_batch: u64,
        to_batch: Option<u64>,
        last_known: MetablockPosition,
        filters: &ReadFilters,
        max_bytes: u64,
        kept: &mut Vec<EventBatchFromLogSegmentFile>,
        cumulative_size: &mut u64,
        evicted_batch_index: &mut Option<u64>,
    ) -> Result<(), ScanError<DiskFormatError>> {
        // Use a VecDeque for efficient eviction from the "newest" end
        let mut disk_kept: VecDeque<EventBatchFromLogSegmentFile> = VecDeque::new();
        let mut disk_cumulative: u64 = 0;

        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            last_known.log_id,
            Some(last_known.metablock_absolute_pos.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64)),
            self.config.read_max_chunk_size,
        )
        .with_bloom_filter(aggregate_key);

        // Budget remaining after cache entries
        let budget_for_disk = max_bytes.saturating_sub(*cumulative_size);

        scanner
            .scan::<(), DiskFormatError>(|log_id, _pos, bytes| {
                if !metablock_bytes::is_matches_aggregate_key(bytes, aggregate_key) {
                    return Ok(None); // Continue - different aggregate
                }

                let batch_index = metablock_bytes::read_event_batch_event_batch_index(bytes);

                // Stop when we've gone past our requested range
                if batch_index < from_batch {
                    return Ok(Some(())); // Stop - past our range
                }

                // Skip if above the range we need (cache already has newer)
                if to_batch.map_or(false, |to| batch_index > to) {
                    return Ok(None); // Continue - will be served from cache
                }

                // Deserialize metablock for filter check
                let metablock = deserialise_metablock(bytes)?;

                if !in_memory_filtering::is_include_batch(&metablock, filters) {
                    return Ok(None); // Continue - filtered out
                }

                // Add to back (we're scanning backwards, newest seen first)
                disk_cumulative += metablock.uncompressed_size;
                disk_kept.push_back(EventBatchFromLogSegmentFile {
                    log_id,
                    metablock,
                    datablock: None,
                });

                // Evict from FRONT (newest) until under budget
                while disk_cumulative > budget_for_disk && disk_kept.len() > 1 {
                    if let Some(evicted) = disk_kept.pop_front() {
                        disk_cumulative -= evicted.metablock.uncompressed_size;
                        let evicted_idx = Self::get_batch_index(&evicted.metablock);
                        // Track lowest evicted as continuation point
                        match evicted_batch_index {
                            Some(existing) if evicted_idx < *existing => {
                                *evicted_batch_index = Some(evicted_idx);
                            }
                            None => {
                                *evicted_batch_index = Some(evicted_idx);
                            }
                            _ => {}
                        }
                    }
                }

                Ok(None) // Continue scanning to from_batch
            })
            .await?;

        *cumulative_size += disk_cumulative;


        // Reverse to get chronological order
        kept.splice(0..0, disk_kept.into_iter().rev());

        Ok(())
    }

    fn build_filtered_response(
        &self,
        batches_with_data: Vec<EventBatchFromLogSegmentFile>,
        filters: &ReadFilters,
    ) -> Vec<AggregateEventBatch> {
        let mut event_batches = Vec::with_capacity(batches_with_data.len());

        for item in batches_with_data {
            let Some(mut datablock) = item.datablock else {
                continue;
            };

            // Apply event-level filters (bloom filter may have false positives)
            if let DatablockKind::EventBatchItem(ref mut eb) = datablock.datablock_kind {
                in_memory_filtering::apply_event_filters(eb, filters);
                if eb.events.is_empty() {
                    continue;
                }
            }

            let Some(batch) = AggregateEventBatch::from_wal(&item.metablock, &datablock) else {
                continue;
            };

            event_batches.push(batch);
        }

        event_batches
    }

    pub async fn enter_s3_catchup(&self) -> Result<S3CatchupResult, S3CatchupError> {
        
        let catchup_status = match self.node_status.get().raw() {
            NodeStatus::Follower { leader_lease_index }
            | NodeStatus::FollowerCatchingUp { leader_lease_index } => {
                NodeStatus::FollowerCatchingUp { leader_lease_index }
            }
            NodeStatus::BootCatchup => NodeStatus::BootCatchup,
            _ => NodeStatus::BootCatchup,
        };
        set_node_status_and_metric(&self.node_status, ValidatedNodeStatus::create_custom_status(catchup_status, 0, 0), self.config.shard_id);

        catchup_from_s3(
            &self.log_segments_cache,
            &self.shard_mem_cache,
            &self.fsync_coordinator,
            &self.watched_aggregates,
            &self.s3_downloader,
            self.config.shard_id,
            self.config.node_id,
            self.peer_node_id.get(),
            self.config.max_catchup_gap_bytes,
        ).await

    }

    pub async fn register_schema(&self, request: celeriant_msg::request::requests::RegisterSchemaRequest) -> Result<SuccessResponse, ShardSchemaError> {
        use celeriant_wal::SchemaType;
        use celeriant_memcache::cached_schema::CachedSchema;

        let max_schema_size = self.config.max_schema_size_bytes as usize;

        // Validate we can accept writes
        let lease_index = match self.node_status.get().effective_node_status() {
            NodeStatus::Leader { lease_index } => lease_index,
            NodeStatus::Standalone => 0,
            _ => return Err(ShardSchemaError::ShardCannotAcceptWrites {
                leader_address: self.leader_client_address.borrow().clone(),
            }),
        };

        let schema_type = SchemaType::try_from(request.schema_type)
            .map_err(|_| ShardSchemaError::UnsupportedSchemaType { schema_type: request.schema_type })?;

        if request.schema.len() > max_schema_size {
            return Err(ShardSchemaError::InvalidSchema {
                schema_type: request.schema_type,
                parse_error: format!("Schema exceeds maximum size of {} bytes", max_schema_size),
            });
        }

        // Validate schema parses and compile it
        let cached_validator = CompiledValidator::compile(schema_type, &request.schema)
            .map_err(|e| ShardSchemaError::InvalidSchema {
                schema_type: request.schema_type,
                parse_error: e,
            })?;

        let schema_key = &request.schema_key;

        // Ensure cache is populated from WAL (handles cold-cache after restart/eviction)
        self.ensure_schema_cached(schema_key).await?;

        {
            let shard_mem_cache = self.shard_mem_cache.borrow();
            if shard_mem_cache.schema_cache_has_schema(schema_key) || shard_mem_cache.schema_is_pending(schema_key) {
                return Err(ShardSchemaError::SchemaAlreadyExists {
                    event_type_major: schema_key.event_type_major,
                    event_type_minor: schema_key.event_type_minor,
                });
            }
        }

        // Build metablock and datablock
        let server_timestamp = self.config.timestamp_config.now();

        let metablock_schema_registration = MetablockSchemaRegistration {
            schema_key: schema_key.clone(),
            client_id: request.client_id,
            user_id: request.user_id,
        };

        let datablock_schema_registration = DatablockSchemaRegistration {
            schema_type,
            schema: request.schema.clone(),
        };

        let datablock = Datablock {
            datablock_kind: DatablockKind::SchemaRegistration(datablock_schema_registration),
        };

        // Serialize datablock - schemas use no compression
        let serialized_datablock = SerialisedDatablock::new(&datablock, celeriant_wal::compression_type::CompressionType::None)
            .map_err(|e| ShardSchemaError::InvalidSchema {
                schema_type: request.schema_type,
                parse_error: format!("Failed to serialize datablock: {:?}", e),
            })?;

        let metablock = Metablock {
            wal_index: 0,
            server_timestamp,
            lease_index,
            node_id: self.config.node_id,
            uncompressed_size: serialized_datablock.uncompressed_size,
            compressed_size: serialized_datablock.compressed_size,
            datablock_version: serialized_datablock.datablock_version,
            datablock_compression_type: serialized_datablock.compression_type,
            datablock: serialized_datablock.storage_kind,
            wal_metablock_type: MetablockKind::SchemaRegistration(metablock_schema_registration),
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
        };

        let shard_log_queue_item = ShardLogQueueItem::new(Some(datablock), serialized_datablock.external_data, metablock);

        // Add to pending queue and populate cache
        {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

            // Double-check after acquiring mutable borrow
            if shard_mem_cache.schema_cache_has_schema(schema_key) || shard_mem_cache.schema_is_pending(schema_key) {
                return Err(ShardSchemaError::SchemaAlreadyExists {
                    event_type_major: schema_key.event_type_major,
                    event_type_minor: schema_key.event_type_minor,
                });
            }

            // Mark as pending before adding to cache
            shard_mem_cache.schema_mark_pending(schema_key.clone());

            // Insert into cache immediately (becomes visible for validation before fsync)
            shard_mem_cache.schema_cache_insert(schema_key.clone(), CachedSchema::Validated(cached_validator));

            // Add to pending fsync queue
            shard_mem_cache.add_to_pending_queue(vec![shard_log_queue_item]);
        }

        // Wait for durability
        self.sync_durable().await?;

        // Replicate if leader
        self.replicate_durable().await?;

        Ok(SuccessResponse {
            correlation_id: request.correlation_id,
        })
    }
}

#[cfg(test)]
impl<R: crate::replication_client::ReplicationClient, D: crate::s3_downloader::S3Downloader> ShardWal<R, D> {
    fn schema_cache_has_schema(&self, key: &SchemaKey) -> bool {
        self.shard_mem_cache.borrow().schema_cache_has_schema(key)
    }

    fn schema_cache_clear(&self) {
        self.shard_mem_cache.borrow_mut().schema_cache_clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::replication_to_follower_error::ReplicateToFollowerError;
    use crate::error::replication_to_s3_error::ReplicateToS3Error;
    use crate::replication_client::StubReplicationClient;
    use crate::s3_downloader::StubS3Downloader;
    use celeriant_msg::request::requests::{ReplicationBatchItem, ReplicationBatchRequest, SingleAggregateDelete, WatchRequest};
    use celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES;
    use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
    use crate::timestamp_config::TimestampConfig;
    use celeriant_disk::files::read_fixed_records_visit_const::{read_fixed_records_visit_const, ReadVisitError};
    use crate::shard_wal_compact::SCAN_CHUNK_SIZE;
    use glommio::{LocalExecutorBuilder, Placement};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    fn test_dir() -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shard");
        (tmp, dir)
    }

    fn test_config(dir: &std::path::Path) -> InternalShardConfig {
        InternalShardConfig {
            node_id: 1,
            shard_id: 1,
            max_open_files: 4,
            shard_log_preallocate_bytes: 4 * 1024 * 1024,
            fsync_delay: Duration::ZERO,
            replication_delay: Duration::ZERO,
            s3_replication_delay: Duration::from_millis(500),
            replication_rollback_cooldown: Duration::ZERO,
            recent_write_cache_bytes: 64 * 1024 * 1024,
            shard_dir: dir.to_path_buf(),
            max_response_size: 16 * 1024 * 1024,
            max_request_size: 16 * 1024 * 1024,
            aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
            aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
            read_max_chunk_size: 32 * 1024,
            timestamp_config: TimestampConfig::default(),
            list_page_size: 100,
            list_max_concurrent: 16,
            list_max_duration: Duration::from_secs(2),
            list_wal_index_cache_bytes: 1024 * 1024,
            schema_cache_bytes: 4 * 1024 * 1024,
            max_schema_size_bytes: 16384,
            pending_replication_high_water_bytes: 64 * 1024 * 1024,
            max_catchup_gap_bytes: 100 * 1024 * 1024,
            compaction_check_interval: Duration::from_secs(600),
            compaction_min_reclaimable_ratio: 0.20,
            compaction_temp_dir: std::path::PathBuf::from("/tmp/test_compaction"),
            max_clock_drift_ms: 500,
            read_max_concurrent: 64,
            cache_warmup_max_duration: Duration::MAX,
        }
    }

    fn key(org: u128, atype: u128, id: u128) -> AggregateKey {
        AggregateKey::new(org, atype, id)
    }

    fn events(count: usize) -> Vec<DatablockAggregateEvent> {
        (1..=count as u64)
            .map(|i| DatablockAggregateEvent {
                client_event_index: i,
                event_type_major: 1,
                event_value: Arc::new(vec![i as u8; 8]),
                ..Default::default()
            })
            .collect()
    }

    fn write_req(agg: AggregateKey, evts: Vec<DatablockAggregateEvent>) -> ClientRequest {
        write_req_full(agg, evts, true, None, 1, false)
    }

    fn write_req_full(
        agg: AggregateKey,
        evts: Vec<DatablockAggregateEvent>,
        allow_create: bool,
        expected_batch: Option<u64>,
        client_id: u128,
        enforce_idempotency: bool,
    ) -> ClientRequest {
        let mut writes = HashMap::new();
        writes.insert(
            agg,
            SingleAggregateWrite {
                events: evts,
                allow_create,
                expected_event_batch_index: expected_batch,
                enforce_client_idempotency: enforce_idempotency,
                compression_type_id: 0,
                compression_level: None,
            },
        );
        ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id,
            user_id: None,
            writes,
        })
    }

    fn read_req(agg: AggregateKey) -> ClientRequest {
        ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: agg,
            filters: ReadFilters::new(0),
        })
    }

    fn read_req_from(agg: AggregateKey, from: u64) -> ClientRequest {
        ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: agg,
            filters: ReadFilters::new(from),
        })
    }

    fn exists_req(agg: AggregateKey) -> ClientRequest {
        ClientRequest::AggregateDetails(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: agg,
        })
    }

    fn delete_req(agg: AggregateKey) -> ClientRequest {
        delete_req_full(agg, false, false, None)
    }

    fn delete_req_full(
        agg: AggregateKey,
        allow_recreate: bool,
        allow_index_continuation: bool,
        expected: Option<u64>,
    ) -> ClientRequest {
        let mut deletes = HashMap::new();
        deletes.insert(
            agg,
            SingleAggregateDelete {
                allow_recreate,
                allow_index_continuation,
                expected_event_batch_index: expected,
            },
        );
        ClientRequest::Delete(DeleteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            deletes,
        })
    }

    fn trim_req(agg: AggregateKey, keep_from: u64) -> ClientRequest {
        ClientRequest::TrimStart(TrimStartRequest {
            correlation_id: None,
            aggregate_key: agg,
            keep_from_event_batch_index: keep_from,
            client_id: 1,
            user_id: None,
        })
    }

    fn list_orgs_req() -> ClientRequest {
        ClientRequest::ListOrgs(ListOrgsRequest {
            correlation_id: None,
            shard_id: 0,
            cursor: None,
        })
    }

    fn list_types_req(org: Option<u128>) -> ClientRequest {
        ClientRequest::ListAggregateTypes(ListAggregateTypesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: org,
            cursor: None,
        })
    }

    fn list_aggs_req(org: Option<u128>, atype: Option<u128>) -> ClientRequest {
        ClientRequest::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: org,
            aggregate_type_id: atype,
            cursor: None,
        })
    }

    async fn open_shard(dir: &std::path::Path) -> ShardWal<StubReplicationClient, StubS3Downloader> {
        ShardWal::open(test_config(dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap()
    }

    async fn process<R: ReplicationClient, D: S3Downloader>(
        shard: &ShardWal<R, D>,
        req: ClientRequest,
    ) -> Result<ClientResponse, ShardError> {
        shard.process_client_request(req).await
    }

    async fn write_ok<R: ReplicationClient, D: S3Downloader>(shard: &ShardWal<R, D>, req: ClientRequest) {
        let result = process(shard, req).await;
        assert!(
            matches!(result, Ok(ClientResponse::Write(_))),
            "write failed: {:?}",
            result.err()
        );
    }

    fn client_write_req(agg: AggregateKey, evts: Vec<DatablockAggregateEvent>) -> ClientRequest {
        let mut writes = HashMap::new();
        writes.insert(
            agg,
            SingleAggregateWrite {
                events: evts,
                allow_create: true,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
                compression_type_id: 0,
                compression_level: None,
            },
        );
        ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        })
    }

    fn client_read_req(agg: AggregateKey) -> ClientRequest {
        ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: agg,
            filters: ReadFilters::new(0),
        })
    }

    fn unwrap_read(result: Result<ClientResponse, ShardError>) -> ReadResponse {
        match result.expect("read should succeed") {
            ClientResponse::Read(r) => r,
            other => panic!("expected Read, got {other:?}"),
        }
    }

    fn unwrap_exists(result: Result<ClientResponse, ShardError>) -> AggregateDetailsResponse {
        match result.expect("exists should succeed") {
            ClientResponse::AggregateDetails(r) => r,
            other => panic!("expected Exists, got {other:?}"),
        }
    }

    fn unwrap_list_orgs(result: Result<ClientResponse, ShardError>) -> ListOrgsResponse {
        match result.expect("list_orgs should succeed") {
            ClientResponse::ListOrgs(r) => r,
            other => panic!("expected ListOrgs, got {other:?}"),
        }
    }

    fn unwrap_list_types(result: Result<ClientResponse, ShardError>) -> ListAggregateTypesResponse {
        match result.expect("list_types should succeed") {
            ClientResponse::ListAggregateTypes(r) => r,
            other => panic!("expected ListAggregateTypes, got {other:?}"),
        }
    }

    fn unwrap_list_aggs(result: Result<ClientResponse, ShardError>) -> ListAggregatesResponse {
        match result.expect("list_aggs should succeed") {
            ClientResponse::ListAggregates(r) => r,
            other => panic!("expected ListAggregates, got {other:?}"),
        }
    }

    // ── Happy path ──

    #[test]
    fn write_and_read_back() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(3))).await;

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);
            assert_eq!(read.event_batches[0].events.len(), 3);
            assert_eq!(read.event_batches[0].event_batch_index, 1);

            shard.close().await;
        });
    }

    #[test]
    fn multiple_writes_produce_sequential_batches() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for batch_num in 1u64..=5 {
                let evts = vec![DatablockAggregateEvent {
                    client_event_index: batch_num,
                    event_type_major: 1,
                    event_value: Arc::new(vec![batch_num as u8]),
                    ..Default::default()
                }];
                write_ok(&shard, write_req(agg.clone(), evts)).await;
            }

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 5);
            for (i, batch) in read.event_batches.iter().enumerate() {
                assert_eq!(batch.event_batch_index, (i + 1) as u64);
            }

            shard.close().await;
        });
    }

    #[test]
    fn write_to_multiple_aggregates() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let keys: Vec<_> = (1..=3).map(|i| key(1, 1, i)).collect();
            for k in &keys {
                write_ok(&shard, write_req(k.clone(), events(2))).await;
            }

            for k in &keys {
                let read = unwrap_read(process(&shard, read_req(k.clone())).await);
                assert_eq!(read.event_batches.len(), 1);
                assert_eq!(read.event_batches[0].events.len(), 2);
            }

            shard.close().await;
        });
    }

    #[test]
    fn read_nonexistent_aggregate_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, read_req(key(1, 1, 999))).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    // ── Exists ──

    #[test]
    fn exists_missing_returns_error() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, exists_req(key(1, 1, 999))).await;
            assert!(matches!(
                result,
                Err(ShardError::AggregateDetails(ShardAggregateDetailsError::AggregateNotExists))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn exists_after_write_returns_details() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let resp = unwrap_exists(process(&shard, exists_req(agg)).await);
            assert_eq!(resp.min_event_batch_index, FIRST_EVENT_BATCH_INDEX);
            assert_eq!(resp.max_event_batch_index, FIRST_EVENT_BATCH_INDEX);
            assert_eq!(resp.max_event_index, 1);
            assert!(!resp.is_deleted);
            assert!(resp.last_server_timestamp > 0);
            assert_eq!(resp.last_client_id, 1);

            shard.close().await;
        });
    }

    // ── Write validation errors ──

    #[test]
    fn write_without_lease_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            shard.node_status.set(ValidatedNodeStatus::create_fenced());

            let result = process(&shard, write_req(key(1, 1, 1), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ShardCannotAcceptWrites { .. }))));

            shard.close().await;
        });
    }

    #[test]
    fn write_rejected_during_rollback_cooldown() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.replication_rollback_cooldown = Duration::from_secs(10);
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

            shard.last_rollback_at.set(Some(std::time::Instant::now()));

            let result = process(&shard, write_req(key(1, 1, 1), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationBackpressure))),
                "expected ReplicationBackpressure while inside cooldown, got {result:?}");

            shard.close().await;
        });
    }

    #[test]
    fn write_accepted_after_rollback_cooldown_expires() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.replication_rollback_cooldown = Duration::from_millis(10);
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

            // Arm the latch far enough in the past that elapsed > cooldown.
            let past = std::time::Instant::now() - Duration::from_secs(60);
            shard.last_rollback_at.set(Some(past));

            let result = process(&shard, write_req(key(1, 1, 1), events(1))).await;
            assert!(result.is_ok(), "expected write to succeed after cooldown, got {result:?}");

            shard.close().await;
        });
    }

    #[test]
    fn write_accepted_when_last_rollback_at_is_none() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.replication_rollback_cooldown = Duration::from_secs(10);
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

            // No rollback has fired — cooldown should not apply.
            assert!(shard.last_rollback_at.get().is_none());

            let result = process(&shard, write_req(key(1, 1, 1), events(1))).await;
            assert!(result.is_ok(), "expected write to succeed when no rollback recorded, got {result:?}");

            shard.close().await;
        });
    }

    #[test]
    fn write_empty_events_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, write_req(key(1, 1, 1), vec![])).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::EmptyEventsList))));

            shard.close().await;
        });
    }

    #[test]
    fn write_zero_event_type_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let evts = vec![DatablockAggregateEvent {
                client_event_index: 1,
                event_type_major: 0,
                event_value: Arc::new(vec![1]),
                ..Default::default()
            }];
            let result = process(&shard, write_req(key(1, 1, 1), evts)).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ZeroEventType { .. }))));

            shard.close().await;
        });
    }

    #[test]
    fn write_without_allow_create_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let req = write_req_full(key(1, 1, 1), events(1), false, None, 1, false);
            let result = process(&shard, req).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::AggregateNotExists))));

            shard.close().await;
        });
    }

    #[test]
    fn optimistic_concurrency_violation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let req = write_req_full(agg, events(1), true, Some(999), 1, false);
            let result = process(&shard, req).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation { .. }))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn client_idempotency_violation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            let req = write_req_full(agg.clone(), events(1), true, None, 42, true);
            write_ok(&shard, req).await;

            let req = write_req_full(agg, events(1), true, None, 42, true);
            let result = process(&shard, req).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn occ_fires_before_idempotency() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            // Write with client_id=1, client_event_index=1 (batch index becomes 1)
            let req = write_req_full(agg.clone(), events(1), true, None, 1, true);
            write_ok(&shard, req).await;

            // Write with client_id=2 to advance batch index to 2
            let req = write_req_full(agg.clone(), events(1), true, Some(1), 2, false);
            write_ok(&shard, req).await;

            // Write with client_id=1, client_event_index=1 (idempotency violation)
            // AND stale expected_event_batch_index=1 (OCC violation, current is 2)
            // OCC should fire first
            let req = write_req_full(agg, events(1), true, Some(1), 1, true);
            let result = process(&shard, req).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation { .. }))
            ));

            shard.close().await;
        });
    }

    // ── Delete ──

    #[test]
    fn delete_then_read_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            let result = process(&shard, read_req(agg)).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    #[test]
    fn delete_then_write_without_recreate_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let _ = process(&shard, delete_req(agg.clone())).await.unwrap();

            let result = process(&shard, write_req(agg, events(1))).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::AggregateRecreateNotAllowed))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn delete_with_recreate_allows_new_write() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let del = delete_req_full(agg.clone(), true, false, None);
            let _ = process(&shard, del).await.unwrap();

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);
            assert_eq!(read.event_batches[0].event_batch_index, FIRST_EVENT_BATCH_INDEX);

            shard.close().await;
        });
    }

    #[test]
    fn delete_with_index_continuation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..3 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            let del = delete_req_full(agg.clone(), true, true, None);
            let _ = process(&shard, del).await.unwrap();

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let read = unwrap_read(process(&shard, read_req_from(agg, 4)).await);
            assert_eq!(read.event_batches.len(), 1);
            assert_eq!(read.event_batches[0].event_batch_index, 4);

            shard.close().await;
        });
    }

    #[test]
    fn delete_validation_errors() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            let empty_delete = ClientRequest::Delete(DeleteRequest {
                correlation_id: None,
                client_id: 1,
                user_id: None,
                deletes: HashMap::new(),
            });
            let result = process(&shard, empty_delete).await;
            assert!(matches!(result, Err(ShardError::Delete(ShardDeleteError::EmptyDeleteList))));

            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(matches!(result, Err(ShardError::Delete(ShardDeleteError::AggregateNotExists))));

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let del = delete_req_full(agg, false, false, Some(999));
            let result = process(&shard, del).await;
            assert!(matches!(
                result,
                Err(ShardError::Delete(ShardDeleteError::OptimisticConcurrencyViolation { .. }))
            ));

            shard.close().await;
        });
    }

    // ── Trim ──

    #[test]
    fn trim_makes_earlier_batches_unavailable() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..5 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            let result = process(&shard, trim_req(agg.clone(), 3)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            let result = process(&shard, read_req_from(agg.clone(), 1)).await;
            assert!(matches!(
                result,
                Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))
            ));

            let read = unwrap_read(process(&shard, read_req_from(agg, 3)).await);
            assert!(!read.event_batches.is_empty());

            shard.close().await;
        });
    }

    #[test]
    fn trim_already_trimmed_is_noop() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..3 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            let _ = process(&shard, trim_req(agg.clone(), 2)).await.unwrap();
            let result = process(&shard, trim_req(agg, 2)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            shard.close().await;
        });
    }

    #[test]
    fn trim_validation_errors() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            let result = process(&shard, trim_req(agg.clone(), 1)).await;
            assert!(matches!(result, Err(ShardError::TrimStart(ShardTrimError::AggregateNotExists))));

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let result = process(&shard, trim_req(agg, 999)).await;
            assert!(matches!(
                result,
                Err(ShardError::TrimStart(ShardTrimError::TrimIndexOutOfRange { .. }))
            ));

            shard.close().await;
        });
    }

    // ── List operations ──

    #[test]
    fn list_orgs_types_aggregates() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            for org in 1..=3u128 {
                for atype in 1..=2u128 {
                    write_ok(&shard, write_req(key(org, atype, 1), events(1))).await;
                }
            }

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            assert_eq!(orgs.orgs.len(), 3);

            let types = unwrap_list_types(process(&shard, list_types_req(None)).await);
            assert_eq!(types.aggregate_types.len(), 6);

            let types_filtered = unwrap_list_types(process(&shard, list_types_req(Some(1))).await);
            assert_eq!(types_filtered.aggregate_types.len(), 2);
            assert!(types_filtered.aggregate_types.iter().all(|t| t.org_id == 1));

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);
            assert_eq!(aggs.aggregates.len(), 1);

            shard.close().await;
        });
    }

    #[test]
    fn list_empty_shard() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            assert!(orgs.orgs.is_empty());
            assert!(orgs.next_cursor.is_none());

            shard.close().await;
        });
    }

    #[test]
    fn list_aggregates_shows_deleted() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let _ = process(&shard, delete_req(agg)).await.unwrap();

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);
            assert_eq!(aggs.aggregates.len(), 1);
            assert!(aggs.aggregates[0].is_deleted);

            shard.close().await;
        });
    }

    // ── Watch rejected ──

    #[test]
    fn watch_rejected() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let watch = ClientRequest::Watch(WatchRequest {
                correlation_id: None,
                requested_latency_ms: None,
                shard_id: None,
                orgs: None,
                aggregate_types: None,
                aggregates: None,
                operation_types: None,
            });
            let result = process(&shard, watch).await;
            assert!(matches!(result, Err(ShardError::WatchRequestInvalid)));

            shard.close().await;
        });
    }

    // ── Exists after trim ──

    #[test]
    fn exists_reflects_trim() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..5 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            let _ = process(&shard, trim_req(agg.clone(), 3)).await.unwrap();

            let resp = unwrap_exists(process(&shard, exists_req(agg)).await);
            assert_eq!(resp.min_event_batch_index, 3);
            assert_eq!(resp.max_event_batch_index, FIRST_EVENT_BATCH_INDEX + 4);
            assert!(!resp.is_deleted);

            shard.close().await;
        });
    }

    /// Replication client that fails both follower and S3 for a configurable
    /// number of calls, then succeeds. Simulates follower-offline + no-S3.
    struct FailThenSucceedReplicationClient {
        follower_failures_remaining: Cell<u32>,
        s3_failures_remaining: Cell<u32>,
    }

    impl FailThenSucceedReplicationClient {
        fn new(follower_failures: u32, s3_failures: u32) -> Self {
            Self {
                follower_failures_remaining: Cell::new(follower_failures),
                s3_failures_remaining: Cell::new(s3_failures),
            }
        }
    }

    impl ReplicationClient for FailThenSucceedReplicationClient {
        fn set_follower_reachable(&self, _: bool) {}
        fn is_follower_reachable(&self) -> bool { true }

        async fn replicate_to_follower(&self, _batches: Vec<celeriant_msg::request::requests::ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError> {
            let remaining = self.follower_failures_remaining.get();
            if remaining > 0 {
                self.follower_failures_remaining.set(remaining - 1);
                return Err(ReplicateToFollowerError::FollowerUnexpectedResponse);
            }
            Ok(())
        }

        async fn replicate_to_s3(&self, _batches: Vec<celeriant_msg::request::requests::ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            let remaining = self.s3_failures_remaining.get();
            if remaining > 0 {
                self.s3_failures_remaining.set(remaining - 1);
                return Err(ReplicateToS3Error::S3NotConfigured);
            }
            Ok(())
        }

        fn set_follower_address(&self, _address: Option<String>) {}

        async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_index: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            glommio::timer::sleep(std::time::Duration::from_millis(10)).await;
            Ok(celeriant_msg::response::responses::HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 10, follower_can_accept_tcp_replication: true })
        }

        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> { Ok(true) }
    }

    async fn open_leader_shard(dir: &std::path::Path, client: FailThenSucceedReplicationClient) -> ShardWal<FailThenSucceedReplicationClient, StubS3Downloader> {
        ShardWal::open(test_config(dir), ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_index: 0 }, 500, now_ms() + 10_000), client, StubS3Downloader)
            .await
            .unwrap()
    }

    /// Replication client with a flag to switch from success to permanent failure.
    struct SwitchableReplicationClient {
        should_fail: Cell<bool>,
    }

    impl SwitchableReplicationClient {
        fn new() -> Self {
            Self { should_fail: Cell::new(false) }
        }
    }

    impl ReplicationClient for SwitchableReplicationClient {
        fn set_follower_address(&self, _: Option<String>) {}
        fn set_follower_reachable(&self, _: bool) {}
        fn is_follower_reachable(&self) -> bool { true }

        async fn replicate_to_follower(&self, _: Vec<celeriant_msg::request::requests::ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError> {
            if self.should_fail.get() {
                return Err(ReplicateToFollowerError::FollowerUnexpectedResponse);
            }
            Ok(())
        }

        async fn replicate_to_s3(&self, _: Vec<celeriant_msg::request::requests::ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            if self.should_fail.get() {
                return Err(ReplicateToS3Error::S3NotConfigured);
            }
            Ok(())
        }

        async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            Ok(celeriant_msg::response::responses::HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 10, follower_can_accept_tcp_replication: true })
        }

        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> { Ok(true) }
    }

    #[test]
    fn rollback_after_rotation_does_not_panic() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let temp_dir = dir.join("compaction_temp");
            std::fs::create_dir_all(&temp_dir).unwrap();

            let config = InternalShardConfig {
                shard_log_preallocate_bytes: 3 * 512 * 1024, // 1.5MB — smallest valid segment
                compaction_temp_dir: temp_dir,
                ..test_config(&dir)
            };

            let client = SwitchableReplicationClient::new();
            let shard = ShardWal::open(
                config,
                ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_index: 0 }, 500, now_ms() + 30_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            let agg = key(1, 1, 1);
            let old_log_id = shard.log_segments_cache.active_log_id();

            // Fill the segment with fat writes until rotation occurs (replication succeeds)
            let mut i = 1u64;
            while shard.log_segments_cache.active_log_id() == old_log_id {
                write_ok(&shard, write_req(agg.clone(), fat_event(i))).await;
                i += 1;
            }

            // Now on a fresh segment with read: None. Flip to fail mode.
            shard.replication_client.should_fail.set(true);

            // This write triggers replication failure → rollback on the new segment.
            // Before the fix, this panics with unwrap() on metadata.read which is None.
            let result = process(&shard, write_req(agg.clone(), fat_event(i))).await;
            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))),
                "expected ReplicationError from rollback, got {:?}", result
            );

            shard.close().await;
        });
    }

    #[test]
    fn write_succeeds_after_replication_rollback() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            let client = FailThenSucceedReplicationClient::new(1, 1);
            let shard = open_leader_shard(&dir, client).await;
            let agg = key(1, 1, 1);

            // Write 1: triggers rollback (follower offline + S3 not configured)
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))),
                "expected ReplicationError, got {:?}", result
            );

            // Write 2: must succeed — stale rollback flags must not block this
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(
                matches!(result, Ok(ClientResponse::Write(_))),
                "write after rollback should succeed, got {:?}", result
            );

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);
            assert_eq!(read.event_batches[0].events.len(), 1);

            shard.close().await;
        });
    }

    #[test]
    fn multiple_writes_succeed_after_replication_rollback() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            let client = FailThenSucceedReplicationClient::new(2, 2);
            let shard = open_leader_shard(&dir, client).await;
            let agg = key(1, 1, 1);

            // Write 1: rollback
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))));

            // Write 2: rollback again
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))));

            // Write 3: should succeed
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(
                matches!(result, Ok(ClientResponse::Write(_))),
                "write after multiple rollbacks should succeed, got {:?}", result
            );

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);

            shard.close().await;
        });
    }

    // ── Replication (handle_replication_batch) ──

    async fn open_follower_shard(dir: &std::path::Path) -> ShardWal<StubReplicationClient, StubS3Downloader> {
        ShardWal::open(test_config(dir), ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_index: 0 }, 500, now_ms() + 10_000), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap()
    }

    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    fn test_metablock(wal_index: u64, previous_tip_hash: [u8; 32]) -> Metablock {
        let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::new(1, 1, 1));
        mb.wal_index = wal_index;
        mb.previous_tip_hash = previous_tip_hash;
        mb
    }

    fn replication_batch_req(batches: Vec<ReplicationBatchItem>) -> ReplicationBatchRequest {
        ReplicationBatchRequest {
            correlation_id: None,
            shard_id: 0,
            leader_timestamp_ms: now_ms(),
            batches,
        }
    }

    fn unwrap_replication(result: Result<ReplicationBatchResponse, crate::error::follower_replication_write_error::FollowerReplicationWriteError>) -> ReplicationBatchResponse {
        result.expect("replication should not error")
    }

    fn replication_item(wal_index: u64, tip_hash: [u8; 32]) -> ReplicationBatchItem {
        ReplicationBatchItem {
            metablock: test_metablock(wal_index, tip_hash),
            datablock: None,
        }
    }

    #[test]
    fn replication_happy_path() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            shard.close().await;
        });
    }

    #[test]
    fn replication_rejected_when_not_follower() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await; // Standalone

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::NotAFollower)));

            shard.close().await;
        });
    }

    #[test]
    fn replication_rejects_empty_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(shard.handle_replication_batch(replication_batch_req(vec![])).await);
            assert!(matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::EmptyBatch)));

            shard.close().await;
        });
    }

    #[test]
    fn replication_rejects_wal_index_gap() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            // Follower at wal_index=0, batch starts at 5 (expects 1)
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(5, GENESIS_HASH)])).await,
            );
            match resp.result {
                ReplicationResult::Rejected(FollowerRejection::WalIndexMismatch { max_follower_wal_index }) => {
                    assert_eq!(max_follower_wal_index, 0);
                }
                other => panic!("expected WalIndexMismatch, got {other:?}"),
            }

            shard.close().await;
        });
    }

    #[test]
    fn replication_rejects_tip_hash_mismatch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            // Correct wal_index but wrong tip hash
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, [0xFF; 32])])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::TipHashMismatch { .. })));

            shard.close().await;
        });
    }

    #[test]
    fn replication_sequential_batches() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            // Batch 1
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            // Read tip_hash after batch 1 for chaining
            let tip_after_1 = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            assert_ne!(tip_after_1, GENESIS_HASH);

            // Batch 2 must chain from batch 1's tip
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(2, tip_after_1)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            // Verify WAL index advanced
            let final_wal_index = shard.log_segments_cache.active().metadata.borrow().write.wal_index;
            assert_eq!(final_wal_index, 2);

            shard.close().await;
        });
    }

    #[test]
    fn replication_rejects_time_drift() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let stale_request = ReplicationBatchRequest {
                correlation_id: None,
                shard_id: 0,
                leader_timestamp_ms: 1000, // ancient timestamp
                batches: vec![replication_item(1, GENESIS_HASH)],
            };
            let resp = unwrap_replication(shard.handle_replication_batch(stale_request).await);
            assert!(matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::TimeDriftTooHigh { .. })));

            shard.close().await;
        });
    }

    // ── Promotion batch upload ──

    struct CapturingReplicationClient {
        s3_uploads: RefCell<Vec<Vec<ReplicationBatchItem>>>,
    }

    impl CapturingReplicationClient {
        fn new() -> Self {
            Self { s3_uploads: RefCell::new(vec![]) }
        }
    }

    impl ReplicationClient for CapturingReplicationClient {
        fn set_follower_address(&self, _address: Option<String>) {}
        fn set_follower_reachable(&self, _: bool) {}
        fn is_follower_reachable(&self) -> bool { true }
        async fn replicate_to_follower(&self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError> { Ok(()) }
        async fn replicate_to_s3(&self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            self.s3_uploads.borrow_mut().push(batches);
            Ok(())
        }
        async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_index: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            Ok(celeriant_msg::response::responses::HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 10, follower_can_accept_tcp_replication: true })
        }
        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> { Ok(true) }
    }

    async fn open_follower_shard_capturing(dir: &std::path::Path, client: CapturingReplicationClient) -> ShardWal<CapturingReplicationClient, StubS3Downloader> {
        ShardWal::open(test_config(dir), ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_index: 0 }, 500, now_ms() + 10_000), client, StubS3Downloader)
            .await
            .unwrap()
    }

    /// Metablock with no datablock — avoids deserialization failures in promotion upload tests
    fn test_metablock_no_datablock(wal_index: u64, previous_tip_hash: [u8; 32]) -> Metablock {
        let mut mb = test_metablock(wal_index, previous_tip_hash);
        mb.datablock = DatablockStorageKind::None;
        mb
    }

    fn replication_item_no_datablock(wal_index: u64, tip_hash: [u8; 32]) -> ReplicationBatchItem {
        ReplicationBatchItem {
            metablock: test_metablock_no_datablock(wal_index, tip_hash),
            datablock: None,
        }
    }

    #[test]
    fn replication_sets_last_received_wal_index() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_index;
            assert_eq!(idx, 1, "should track wal_index of replicated batch");

            shard.close().await;
        });
    }

    #[test]
    fn replication_overwrites_last_received_wal_index_on_subsequent_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(2, tip)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_index;
            assert_eq!(idx, 2, "should overwrite to latest batch wal_index");

            shard.close().await;
        });
    }

    #[test]
    fn upload_promotion_batch_noop_when_field_zero() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            // No replication happened, field is 0
            shard.upload_s3_promotion_batch().await.unwrap();

            assert!(shard.replication_client.s3_uploads.borrow().is_empty(), "should not upload when no pending batch");

            shard.close().await;
        });
    }

    #[test]
    fn upload_promotion_batch_uploads_and_clears() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            shard.upload_s3_promotion_batch().await.unwrap();

            let uploads = shard.replication_client.s3_uploads.borrow();
            assert_eq!(uploads.len(), 1, "should upload exactly one batch");
            assert_eq!(uploads[0].len(), 1, "batch should contain one item");
            assert_eq!(uploads[0][0].metablock.wal_index, 1);
            drop(uploads);

            // Field should be cleared
            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_index;
            assert_eq!(idx, 0, "field should be cleared after upload");

            // Second call should be a noop
            shard.upload_s3_promotion_batch().await.unwrap();
            assert_eq!(shard.replication_client.s3_uploads.borrow().len(), 1, "should not re-upload");

            shard.close().await;
        });
    }

    #[test]
    fn upload_promotion_batch_after_multiple_replications() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            // Replicate batch 1
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            // Replicate batch 2
            let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(2, tip)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            // Upload should contain only entries from wal_index 2 onward (last batch)
            shard.upload_s3_promotion_batch().await.unwrap();

            let uploads = shard.replication_client.s3_uploads.borrow();
            assert_eq!(uploads.len(), 1);
            assert_eq!(uploads[0].len(), 1, "should only upload from last_received index");
            assert_eq!(uploads[0][0].metablock.wal_index, 2);

            shard.close().await;
        });
    }

    // ── Batch 1 Test Gaps: P1-1, P1-6, P1-7 ──

    fn multi_write_req(writes: Vec<(AggregateKey, Vec<DatablockAggregateEvent>, Option<u64>)>) -> ClientRequest {
        let mut map = HashMap::new();
        for (agg, evts, expected) in writes {
            map.insert(
                agg,
                SingleAggregateWrite {
                    events: evts,
                    allow_create: true,
                    expected_event_batch_index: expected,
                    enforce_client_idempotency: false,
                    compression_type_id: 0,
                compression_level: None,
                },
            );
        }
        ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes: map,
        })
    }

    #[test]
    fn multi_aggregate_occ_rollback() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let (a, b) = (key(1, 1, 1), key(1, 1, 2));

            write_ok(&shard, write_req(a.clone(), events(1))).await;
            write_ok(&shard, write_req(b.clone(), events(1))).await;

            // Advance B to batch 2
            write_ok(&shard, write_req_full(b.clone(), events(1), false, Some(1), 1, false)).await;

            // Multi-write: A expects 1 (correct), B expects 1 (stale - now at 2)
            let req = multi_write_req(vec![
                (a.clone(), events(1), Some(1)),
                (b.clone(), events(1), Some(1)),
            ]);
            let result = process(&shard, req).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation { .. }))
            ));

            // Verify A was NOT written (rollback)
            let read_a = unwrap_read(process(&shard, read_req(a)).await);
            assert_eq!(read_a.event_batches.len(), 1);

            shard.close().await;
        });
    }

    #[test]
    fn sequential_writes_produce_contiguous_batch_indices() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for i in 0..5 {
                write_ok(&shard, write_req_full(agg.clone(), events(1), i == 0, None, 1, false)).await;
            }

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            let indices: Vec<u64> = read.event_batches.iter().map(|b| b.event_batch_index).collect();
            assert_eq!(indices, vec![1, 2, 3, 4, 5]);

            shard.close().await;
        });
    }

    #[test]
    fn read_wrong_org_returns_not_found() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 1, 1), events(1))).await;

            let result = process(&shard, read_req(key(2, 1, 1))).await;
            assert!(matches!(
                result,
                Err(ShardError::Read(ShardReadError::AggregateNotExists))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn list_wrong_org_returns_empty() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 1, 1), events(1))).await;

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(2), None)).await);
            assert!(aggs.aggregates.is_empty());

            shard.close().await;
        });
    }

    // ── process_client_request ──

    #[test]
    fn client_request_write_and_read() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            let write_result = shard.process_client_request(client_write_req(agg.clone(), events(3))).await;
            assert!(matches!(write_result, Ok(ClientResponse::Write(_))));

            let read_result = shard.process_client_request(client_read_req(agg)).await;
            match read_result.expect("read should succeed") {
                ClientResponse::Read(r) => {
                    assert_eq!(r.event_batches.len(), 1);
                    assert_eq!(r.event_batches[0].events.len(), 3);
                }
                other => panic!("expected Read, got {other:?}"),
            }

            shard.close().await;
        });
    }

    // ── Schema registration and validation ──

    const NAME_AGE_SCHEMA: &str = r#"{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}"#;

    fn schema_req(org: u128, atype: u128, major: u64, minor: u64, schema: &str) -> ClientRequest {
        ClientRequest::RegisterSchema(celeriant_msg::request::requests::RegisterSchemaRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            schema_key: SchemaKey::new(org, atype, major, minor),
            schema_type: 0,
            schema: schema.to_string(),
        })
    }

    fn schema_req_with_type(org: u128, atype: u128, major: u64, minor: u64, schema_type: u8, schema: &str) -> ClientRequest {
        ClientRequest::RegisterSchema(celeriant_msg::request::requests::RegisterSchemaRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            schema_key: SchemaKey::new(org, atype, major, minor),
            schema_type,
            schema: schema.to_string(),
        })
    }

    fn json_events(payloads: &[&[u8]], major: u64, minor: u64) -> Vec<DatablockAggregateEvent> {
        payloads.iter().enumerate().map(|(i, payload)| DatablockAggregateEvent {
            client_event_index: (i + 1) as u64,
            event_type_major: major,
            event_type_minor: minor,
            event_value: Arc::new(payload.to_vec()),
            ..Default::default()
        }).collect()
    }

    #[test]
    fn schema_register_and_validate_write() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await;
            assert!(matches!(result, Ok(ClientResponse::RegisterSchema(_))));

            let valid = json_events(&[br#"{"name":"alice","age":30}"#], 1, 0);
            write_ok(&shard, write_req(key(1, 1, 1), valid)).await;

            shard.close().await;
        });
    }

    #[test]
    fn schema_rejects_invalid_write() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            let bad_payloads: &[&[u8]] = &[
                br#"{"name":"alice"}"#,              // missing required field
                br#"{"name":"alice","age":"thirty"}"#, // wrong type
                b"not json",                          // non-JSON
            ];
            for payload in bad_payloads {
                let evts = json_events(&[payload], 1, 0);
                let result = process(&shard, write_req(key(1, 1, 1), evts)).await;
                assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))),
                    "expected SchemaValidationFailed for {:?}, got {:?}", std::str::from_utf8(payload), result);
            }

            shard.close().await;
        });
    }

    #[test]
    fn schema_idempotent_registration() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // First registration succeeds
            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            // Second identical registration returns SchemaAlreadyExists (immutable — cannot re-register)
            let result = process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::SchemaAlreadyExists { .. }
            ))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_already_exists_different_schema() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            let different = r#"{"type":"object","properties":{"x":{"type":"number"}}}"#;
            let result = process(&shard, schema_req(1, 1, 1, 0, different)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::SchemaAlreadyExists { event_type_major: 1, event_type_minor: 0 }
            ))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_invalid_json_rejected() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, schema_req(1, 1, 1, 0, "{{not valid json")).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::InvalidSchema { .. }
            ))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_unsupported_type_rejected() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // schema_type 99 = unknown (unsupported)
            let result = process(&shard, schema_req_with_type(1, 1, 1, 0, 99, NAME_AGE_SCHEMA)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::UnsupportedSchemaType { .. }
            ))), "expected UnsupportedSchemaType, got {:?}", result);

            shard.close().await;
        });
    }

    const AVRO_PERSON_SCHEMA: &str = r#"{"type":"record","name":"Person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}"#;

    fn avro_events(payloads: &[&[u8]], major: u64, minor: u64) -> Vec<DatablockAggregateEvent> {
        payloads.iter().enumerate().map(|(i, payload)| DatablockAggregateEvent {
            client_event_index: (i + 1) as u64,
            event_type_major: major,
            event_type_minor: minor,
            event_value: Arc::new(payload.to_vec()),
            ..Default::default()
        }).collect()
    }

    fn avro_encode_person(name: &str, age: i32) -> Vec<u8> {
        let schema = apache_avro::Schema::parse_str(AVRO_PERSON_SCHEMA).unwrap();
        apache_avro::to_avro_datum(
            &schema,
            apache_avro::types::Value::Record(vec![
                ("name".to_string(), apache_avro::types::Value::String(name.to_string())),
                ("age".to_string(), apache_avro::types::Value::Int(age)),
            ]),
        ).unwrap()
    }

    #[test]
    fn schema_avro_register_and_validate() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // Register Avro schema (type 1)
            process(&shard, schema_req_with_type(1, 1, 1, 0, 1, AVRO_PERSON_SCHEMA)).await.unwrap();

            // Write valid Avro event
            let valid = avro_encode_person("alice", 30);
            write_ok(&shard, write_req(key(1, 1, 1), avro_events(&[&valid], 1, 0))).await;

            // Write invalid bytes — should be rejected
            let bad = avro_events(&[b"not avro"], 1, 0);
            let result = process(&shard, write_req(key(1, 1, 1), bad)).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))),
                "expected SchemaValidationFailed, got {:?}", result);

            shard.close().await;
        });
    }

    #[test]
    fn schema_encrypted_event_skips_validation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            // Encrypted event with garbage payload — should pass because iv.is_some() skips validation
            let evts = vec![DatablockAggregateEvent {
                client_event_index: 1,
                event_type_major: 1,
                event_type_minor: 0,
                event_value: Arc::new(b"not valid json at all".to_vec()),
                iv: Some([0u8; 12]),
                ..Default::default()
            }];
            write_ok(&shard, write_req(key(1, 1, 1), evts)).await;

            shard.close().await;
        });
    }

    #[test]
    fn schema_unregistered_event_type_passes_through() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // Register schema for (major=1, minor=0)
            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            // Write to (major=2, minor=0) — no schema, should pass
            let evts = json_events(&[b"literally anything"], 2, 0);
            write_ok(&shard, write_req(key(1, 1, 1), evts)).await;

            shard.close().await;
        });
    }

    #[test]
    fn schema_survives_reopen() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Register and close
            {
                let shard = open_shard(&dir).await;
                process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
                shard.close().await;
            }

            // Reopen — cold cache, must recover from WAL
            {
                let shard = open_shard(&dir).await;

                // Valid write still works
                let valid = json_events(&[br#"{"name":"bob","age":25}"#], 1, 0);
                write_ok(&shard, write_req(key(1, 1, 1), valid)).await;

                // Invalid write still rejected
                let invalid = json_events(&[br#"{"name":"bob"}"#], 1, 0);
                let result = process(&shard, write_req(key(1, 1, 2), invalid)).await;
                assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))));

                shard.close().await;
            }
        });
    }

    #[test]
    fn schema_prewarm_recovers_inline_and_block_datablocks() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Build a schema large enough to exceed MINIBATCH_SIZE_BYTES (512)
            // and force block storage. ~20 properties at ~28 bytes each = ~560+ bytes.
            let mut large_schema = String::from(r#"{"type":"object","properties":{"#);
            for i in 0..20 {
                if i > 0 { large_schema.push(','); }
                large_schema.push_str(&format!(r#""field_{i:02}":{{ "type":"string"}}"#));
            }
            large_schema.push_str(r#"},"required":["field_00"]}"#);
            assert!(large_schema.len() > 512, "schema must exceed inline threshold");

            // Register both an inline schema (event type 1,0) and a block schema (event type 2,0)
            {
                let shard = open_shard(&dir).await;
                process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
                process(&shard, schema_req(1, 1, 2, 0, &large_schema)).await.unwrap();
                shard.close().await;
            }

            // Reopen — pre_warm_cache must recover both inline and block schemas
            {
                let shard = open_shard(&dir).await;

                let inline_key = SchemaKey::new(1, 1, 1, 0);
                let block_key = SchemaKey::new(1, 1, 2, 0);

                // Both schemas must be in cache immediately after open, before any writes
                assert!(shard.schema_cache_has_schema(&inline_key), "inline schema not pre-warmed");
                assert!(shard.schema_cache_has_schema(&block_key), "block schema not pre-warmed");

                // Inline schema: validation works
                let valid = json_events(&[br#"{"name":"bob","age":25}"#], 1, 0);
                write_ok(&shard, write_req(key(1, 1, 1), valid)).await;

                let invalid = json_events(&[br#"{"name":"bob"}"#], 1, 0);
                let result = process(&shard, write_req(key(1, 1, 2), invalid)).await;
                assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))));

                // Block schema: validation works
                let valid = json_events(&[br#"{"field_00":"hello"}"#], 2, 0);
                write_ok(&shard, write_req(key(1, 1, 3), valid)).await;

                let invalid = json_events(&[br#"{"field_00": 42}"#], 2, 0);
                let result = process(&shard, write_req(key(1, 1, 4), invalid)).await;
                assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))));

                // Force-clear the schema cache to test ensure_schema_cached (cold-cache WAL scan)
                shard.schema_cache_clear();
                assert!(!shard.schema_cache_has_schema(&inline_key), "inline schema should be cleared");
                assert!(!shard.schema_cache_has_schema(&block_key), "block schema should be cleared");

                // Write triggers ensure_schema_cached which must WAL-scan and recover inline schema
                let valid = json_events(&[br#"{"name":"alice","age":40}"#], 1, 0);
                write_ok(&shard, write_req(key(1, 1, 5), valid)).await;
                assert!(shard.schema_cache_has_schema(&inline_key), "inline schema not recovered by ensure_schema_cached");

                // Write triggers ensure_schema_cached which must WAL-scan and recover block schema
                let valid = json_events(&[br#"{"field_00":"world"}"#], 2, 0);
                write_ok(&shard, write_req(key(1, 1, 6), valid)).await;
                assert!(shard.schema_cache_has_schema(&block_key), "block schema not recovered by ensure_schema_cached");

                shard.close().await;
            }
        });
    }

    #[test]
    fn schema_cold_cache_rejects_duplicate_registration() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            {
                let shard = open_shard(&dir).await;
                process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
                shard.close().await;
            }

            // Reopen — cold cache, ensure_schema_cached must WAL-scan before rejecting
            {
                let shard = open_shard(&dir).await;

                let result = process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await;
                assert!(matches!(result, Err(ShardError::RegisterSchema(
                    crate::error::shard_schema_error::ShardSchemaError::SchemaAlreadyExists { .. }
                ))));

                let different = r#"{"type":"object","properties":{"x":{"type":"number"}}}"#;
                let result = process(&shard, schema_req(1, 1, 1, 0, different)).await;
                assert!(matches!(result, Err(ShardError::RegisterSchema(
                    crate::error::shard_schema_error::ShardSchemaError::SchemaAlreadyExists { .. }
                ))));

                shard.close().await;
            }
        });
    }

    #[test]
    fn schema_multiple_event_types_independent() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let number_schema = r#"{"type":"object","properties":{"value":{"type":"number"}},"required":["value"]}"#;

            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
            process(&shard, schema_req(1, 1, 2, 0, number_schema)).await.unwrap();

            // Valid for schema 1
            let evts = json_events(&[br#"{"name":"alice","age":30}"#], 1, 0);
            write_ok(&shard, write_req(key(1, 1, 1), evts)).await;

            // Valid for schema 2
            let evts = json_events(&[br#"{"value":42}"#], 2, 0);
            write_ok(&shard, write_req(key(1, 1, 2), evts)).await;

            // Invalid for schema 1 (passes schema 2's shape)
            let evts = json_events(&[br#"{"value":42}"#], 1, 0);
            let result = process(&shard, write_req(key(1, 1, 3), evts)).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_exceeds_max_size_rejected() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // max_schema_size_bytes is 16384 in test_config
            let oversized = format!(r#"{{"type":"object","description":"{}"}}"#, "x".repeat(20000));
            let result = process(&shard, schema_req(1, 1, 1, 0, &oversized)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::InvalidSchema { .. }
            ))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_follower_rejects_registration() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let result = process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::ShardCannotAcceptWrites { .. }
            ))));

            shard.close().await;
        });
    }

    // ── Compaction ──

    /// Config for compaction tests: small segment size to trigger rotation quickly.
    ///
    /// Minimum valid preallocate_bytes is 3 * HEADER_BLOCK_SIZE_BYTES (3 * 512KB = 1.5MB).
    /// This leaves 512KB usable per segment. Each fat write (~9KB) fills it in ~57 writes.
    fn compact_config(dir: &std::path::Path) -> InternalShardConfig {
        let temp_dir = dir.join("compaction_temp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        InternalShardConfig {
            shard_log_preallocate_bytes: 3 * 512 * 1024, // 1.5MB — smallest valid segment size
            compaction_min_reclaimable_ratio: 0.20,
            compaction_temp_dir: temp_dir,
            ..test_config(dir)
        }
    }

    async fn open_compact_shard(dir: &std::path::Path) -> ShardWal<StubReplicationClient, StubS3Downloader> {
        ShardWal::open(compact_config(dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap()
    }

    /// Write one event with an 8KB payload to consume ~9KB of segment space.
    fn fat_event(index: u64) -> Vec<DatablockAggregateEvent> {
        vec![DatablockAggregateEvent {
            client_event_index: index,
            event_type_major: 1,
            event_value: Arc::new(vec![index as u8; 8192]),
            ..Default::default()
        }]
    }

    /// Write `n` fat events to `agg` in the current segment (without triggering rotation).
    /// Each write consumes ~9KB of segment space.
    async fn write_fat<R: ReplicationClient, D: S3Downloader>(
        shard: &ShardWal<R, D>,
        agg: &AggregateKey,
        n: u64,
    ) {
        for i in 1..=n {
            write_ok(shard, write_req(agg.clone(), fat_event(i))).await;
        }
    }

    /// Trigger segment rotation and delete the sentinel to minimize its impact on compaction ratios.
    ///
    /// Fat writes to the sentinel may land in the current segment (before rotation triggers).
    /// Deleting the sentinel afterwards marks those writes as dead, so compaction tests
    /// can still verify that `compacted_size < original_size`.
    async fn trigger_rotation<R: ReplicationClient, D: S3Downloader>(shard: &ShardWal<R, D>) {
        let sentinel = key(99, 99, 99);
        let old_log_id = shard.log_segments_cache.active_log_id();
        // Keep writing until we actually rotate (available space may vary slightly).
        let mut i = 1u64;
        while shard.log_segments_cache.active_log_id() == old_log_id {
            write_ok(shard, write_req(sentinel.clone(), fat_event(i))).await;
            i += 1;
        }
        // Delete the sentinel so any sentinel writes that landed in the sealed segment
        // are also treated as dead data during compaction, preserving high dead ratios.
        // allow_recreate=true so trigger_rotation can be called multiple times in a test.
        let _ = process(shard, delete_req_full(sentinel, true, false, None)).await;
    }

    #[test]
    fn compact_deleted_aggregates_removes_dead_data() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);

            // Fill most of segment 1 with A's data (50 × 9KB ≈ 450KB of the 512KB usable).
            // This ensures A dominates the segment and the dead ratio is well above 20%.
            write_fat(&shard, &agg_a, 50).await;

            // Write a small amount of B data into segment 1.
            write_ok(&shard, write_req(agg_b.clone(), events(3))).await;

            // Soft-delete A — tombstone goes into segment 1 (within-segment tombstone).
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation: next fat write exceeds remaining space, landing in segment 2.
            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() >= 2);

            let seg1_id = 1u64;
            let original_size = shard.log_segments_cache.get(seg1_id).await.unwrap().metadata.borrow().file_len;

            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run (A's fat data dominates dead ratio)");
            let cr = result.unwrap();
            assert_eq!(cr.log_id, seg1_id);
            assert!(cr.compacted_size < cr.original_size, "compacted file should be smaller");
            assert_eq!(cr.original_size, original_size);

            // B still readable with all events (B's data preserved in compacted segment).
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);
            assert_eq!(read_b.event_batches[0].events.len(), 3);

            // A is deleted.
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    #[test]
    fn compact_trimmed_batches_preserves_above_floor() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 1, 1);

            // Write 2 fat events (to be trimmed away) + 3 fat events above the trim floor.
            // 50 total fat events ensures the trimmed-away data dominates the segment.
            // Fat writes: events 1–30 will be trimmed, events 31–50 will be kept.
            // We write 50 fat batches total, then trim to keep from batch 31.
            write_fat(&shard, &agg, 50).await;

            // Trim: keep from batch 31 (30 batches become dead = 60% of total).
            let result = process(&shard, trim_req(agg.clone(), 31)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            let seg1_id = 1u64;
            let original_size = shard.log_segments_cache.get(seg1_id).await.unwrap().metadata.borrow().file_len;

            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run (30/50 batches trimmed = 60% dead)");
            let cr = result.unwrap();
            assert!(cr.compacted_size < original_size, "compacted file should be smaller");

            // Reading from batch 1 should fail (below trim floor of 31).
            let result = process(&shard, read_req_from(agg.clone(), 1)).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))));

            // Reading from batch 31 should succeed.
            let read = unwrap_read(process(&shard, read_req_from(agg.clone(), 31)).await);
            assert!(!read.event_batches.is_empty());
            assert!(read.event_batches.iter().all(|b| b.event_batch_index >= 31));

            shard.close().await;
        });
    }

    #[test]
    fn compact_100_percent_dead_segment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            // Write fat events to 3 aggregates (10 each = 30 total, ~270KB).
            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let agg_c = key(1, 1, 3);

            write_fat(&shard, &agg_a, 10).await;
            write_fat(&shard, &agg_b, 10).await;
            write_fat(&shard, &agg_c, 10).await;

            // Delete ALL aggregates — all 30 event batches become dead.
            for agg in &[&agg_a, &agg_b, &agg_c] {
                let result = process(&shard, delete_req((*agg).clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));
            }

            // Trigger rotation.
            trigger_rotation(&shard).await;

            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run on fully dead segment");
            let cr = result.unwrap();

            // Compacted file should be smaller (only tombstones remain — no event datablocks).
            // The theoretical minimum is 2 × HEADER_BLOCK_SIZE_BYTES + tombstone metablocks.
            // We verify the compacted size is strictly less than the original preallocated size.
            assert!(cr.compacted_size < cr.original_size, "compacted should be smaller than original 1.5MB segment");

            // All aggregates still appear deleted.
            for agg in &[&agg_a, &agg_b, &agg_c] {
                let result = process(&shard, read_req((*agg).clone())).await;
                assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));
            }

            shard.close().await;
        });
    }

    #[test]
    fn compact_below_threshold_skipped() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            // Write 50 fat events to a live aggregate (will remain alive).
            let live_agg = key(1, 1, 1);
            write_fat(&shard, &live_agg, 50).await;

            // Write 1 fat event to a separate aggregate and delete it.
            // 1 dead out of 51+ total = ~2% dead — well below the 20% threshold.
            let dead_agg = key(1, 1, 2);
            write_ok(&shard, write_req(dead_agg.clone(), fat_event(1))).await;
            let result = process(&shard, delete_req(dead_agg.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            let seg1_id = 1u64;
            let size_before = shard.log_segments_cache.get(seg1_id).await.unwrap().metadata.borrow().file_len;

            // Compaction should not run (1 dead / 51+ total ≈ 2% < 20% threshold).
            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_none(), "expected no compaction below threshold");

            // File size unchanged.
            let size_after = shard.log_segments_cache.get(seg1_id).await.unwrap().metadata.borrow().file_len;
            assert_eq!(size_before, size_after);

            shard.close().await;
        });
    }

    #[test]
    fn compact_active_segment_not_eligible() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 1, 1);

            // Write to the active segment (no rotation — still segment 1).
            write_ok(&shard, write_req(agg.clone(), events(3))).await;

            // Delete it — but the active segment is never compacted.
            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Active log_id should still be 1 (we never rotated).
            assert_eq!(shard.log_segments_cache.active_log_id(), 1);

            // compact_oldest_eligible_segment: active_log_id is 1, so there are no sealed segments.
            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_none(), "active segment should not be compacted");

            shard.close().await;
        });
    }

    #[test]
    fn compact_preserves_read_after_compact() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let dead_agg = key(1, 1, 0);
            let agg_b = key(1, 1, 2);
            let agg_c = key(1, 1, 3);
            let agg_d = key(1, 1, 4);

            // Fill most of segment 1 with dead_agg's data.
            write_fat(&shard, &dead_agg, 40).await;

            // Write alive aggregates: B gets 1 batch, C gets 2 batches, D gets 3 batches.
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_c.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_c.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_d.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_d.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_d.clone(), events(2))).await;

            // Delete dead_agg — 40 fat batches become dead (dominates segment).
            let result = process(&shard, delete_req(dead_agg.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            // Compact.
            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run");

            // Verify B (1 batch), C (2 batches), D (3 batches) fully readable after compaction.
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);
            assert_eq!(read_b.event_batches[0].events.len(), 2);

            let read_c = unwrap_read(process(&shard, read_req(agg_c.clone())).await);
            assert_eq!(read_c.event_batches.len(), 2);
            for batch in &read_c.event_batches {
                assert_eq!(batch.events.len(), 2);
            }

            let read_d = unwrap_read(process(&shard, read_req(agg_d.clone())).await);
            assert_eq!(read_d.event_batches.len(), 3);
            for batch in &read_d.event_batches {
                assert_eq!(batch.events.len(), 2);
            }

            shard.close().await;
        });
    }

    #[test]
    fn compact_multiple_rounds() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let agg_c = key(1, 1, 3);
            let agg_d = key(1, 1, 4);

            // Write events to A, B, C, D in segment 1 with enough data so each
            // compaction round has >20% dead fraction:
            //   A: 15 fat batches (deleted in round 1 → 30% dead of ~50 total)
            //   B: 15 fat batches (8 trimmed in round 2 → ~22% dead of ~35 remaining)
            //   C: 15 fat batches (deleted in round 3 → ~55% dead of ~27 remaining)
            //   D:  5 fat batches (always kept)
            write_fat(&shard, &agg_a, 15).await;
            write_fat(&shard, &agg_b, 15).await;
            write_fat(&shard, &agg_c, 15).await;
            write_fat(&shard, &agg_d, 5).await;

            // Trigger rotation to seal segment 1.
            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() >= 2);

            let seg1_original_size = shard.log_segments_cache.get(1).await.unwrap().metadata.borrow().file_len;

            // Delete A — tombstone written to current active segment (cross-segment).
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Round 1: compact segment 1 — removes A's 15 event batches, keeps B, C, D.
            let cr1 = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("round 1: A's 15 batches dead (>20%), compaction should run");
            assert_eq!(cr1.log_id, 1);
            assert!(cr1.compacted_size < seg1_original_size, "round 1 should shrink file");
            let size_after_round1 = cr1.compacted_size;

            // B (15 batches), C (15 batches), D (5 batches) still fully readable.
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 15);
            let read_c = unwrap_read(process(&shard, read_req(agg_c.clone())).await);
            assert_eq!(read_c.event_batches.len(), 15);
            let read_d = unwrap_read(process(&shard, read_req(agg_d.clone())).await);
            assert_eq!(read_d.event_batches.len(), 5);

            // A is deleted.
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            // Trim B: keep from batch 9 (removes 8 batches = 8/35 remaining ≈ 22.8% dead).
            let result = process(&shard, trim_req(agg_b.clone(), 9)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            // Round 2: compact segment 1 again — removes B's 8 trimmed batches.
            let cr2 = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("round 2: 8 of B's batches dead (~22%), compaction should run");
            assert_eq!(cr2.log_id, 1);
            assert!(cr2.compacted_size < size_after_round1, "round 2 should shrink further");
            let size_after_round2 = cr2.compacted_size;

            // B readable (only batches 9+).
            let read_b = unwrap_read(process(&shard, read_req_from(agg_b.clone(), 9)).await);
            assert!(!read_b.event_batches.is_empty());
            assert!(read_b.event_batches.iter().all(|b| b.event_batch_index >= 9));

            // B's early batches unavailable.
            let result = process(&shard, read_req_from(agg_b.clone(), 1)).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))));

            // C (15 batches) and D (5 batches) fully intact.
            let read_c = unwrap_read(process(&shard, read_req(agg_c.clone())).await);
            assert_eq!(read_c.event_batches.len(), 15);
            let read_d = unwrap_read(process(&shard, read_req(agg_d.clone())).await);
            assert_eq!(read_d.event_batches.len(), 5);

            // Delete C — tombstone goes into current active segment.
            let result = process(&shard, delete_req(agg_c.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Round 3: compact segment 1 again — removes C's 15 event batches.
            let cr3 = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("round 3: C's 15 batches dead (~55%), compaction should run");
            assert_eq!(cr3.log_id, 1);
            assert!(cr3.compacted_size < size_after_round2, "round 3 should shrink further");

            // D (5 batches) still fully intact.
            let read_d = unwrap_read(process(&shard, read_req(agg_d.clone())).await);
            assert_eq!(read_d.event_batches.len(), 5);

            // C is deleted.
            let result = process(&shard, read_req(agg_c.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            // A still deleted (tombstone preserved).
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    #[test]
    fn compact_cross_segment_tombstone_resolves() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);

            // Fill most of segment 1 with A's fat data (dominant dead data after deletion).
            write_fat(&shard, &agg_a, 45).await;

            // Write a small amount of B data into segment 1 as a live control.
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;

            // Trigger rotation — A and B remain in segment 1, tombstones go to segment 2.
            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() >= 2);

            // Write SoftDelete for A in segment 2 (CROSS-SEGMENT tombstone).
            // A's event batches are in segment 1; the delete tombstone is in segment 2.
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Compact segment 1: must do cross-segment reverse scan to find A's tombstone
            // in segment 2, then mark A's event batches in segment 1 as dead.
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("cross-segment compaction should run (A's ~45 fat batches dead)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // A is still shown as deleted after compaction.
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            // B is fully intact (its data preserved in compacted segment).
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);
            assert_eq!(read_b.event_batches[0].events.len(), 2);

            shard.close().await;
        });
    }

    #[test]
    fn compact_preserves_hash_chain() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);

            // Fill most of segment 1 with A's fat data (will be deleted, becoming dead).
            write_fat(&shard, &agg_a, 45).await;

            // Write B data (will survive compaction).
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;

            // Delete A so compaction removes its data.
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            // Compact.
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (A's 45 batches dead)");
            assert_eq!(cr.log_id, 1);

            // The compacted segment must be readable — proving the hash chain is consistent
            // and the header was written correctly. If the hash chain were broken, the WAL
            // would detect corruption on the next open.
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);
            assert_eq!(read_b.event_batches[0].events.len(), 2);

            // Verify the compacted segment's header is loaded correctly by checking the tip_hash.
            // The segment now has metablocks (B's event batch + A's tombstone), so the tip_hash
            // must differ from GENESIS_HASH.
            let seg = shard.log_segments_cache.get(cr.log_id).await.unwrap();
            let meta = seg.metadata.borrow();
            assert_ne!(meta.write.tip_hash, GENESIS_HASH, "compacted segment should have non-genesis tip hash");

            shard.close().await;
        });
    }

    #[test]
    fn compact_preserves_bloom_filter() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let agg_c = key(1, 1, 3);

            // Fill most of segment 1 with A's fat data (will be deleted).
            write_fat(&shard, &agg_a, 45).await;

            // Write B and C (small, will survive compaction).
            write_ok(&shard, write_req(agg_b.clone(), events(1))).await;
            write_ok(&shard, write_req(agg_c.clone(), events(1))).await;

            // Delete A — within-segment tombstone.
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            // Compact.
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (A's 45 batches dead)");
            assert_eq!(cr.log_id, 1);

            // After compaction, the segment header includes a rebuilt bloom filter.
            // We verify the bloom filter works correctly by confirming:
            // - B and C are readable (their data survived; bloom must contain them)
            // - A is deleted (tombstone preserved; bloom may contain A — false positives ok)
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);

            let read_c = unwrap_read(process(&shard, read_req(agg_c.clone())).await);
            assert_eq!(read_c.event_batches.len(), 1);

            // A remains deleted.
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    #[test]
    fn compact_softtrim_survives_compaction() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_b = key(1, 1, 2);
            let filler = key(1, 1, 99);

            // Three batches to B, then trim to keep from batch 2 (drops batch 1).
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            let result = process(&shard, trim_req(agg_b.clone(), 2)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            // Fat filler to push dead ratio above the compaction threshold.
            write_fat(&shard, &filler, 40).await;
            let result = process(&shard, delete_req(filler.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            trigger_rotation(&shard).await;
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (filler dominates dead data)");
            assert_eq!(cr.log_id, 1);

            // SoftTrim must survive in the compacted segment and still be applied.
            let result = process(&shard, read_req_from(agg_b.clone(), 1)).await;
            assert!(
                matches!(result, Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))),
                "batch 1 must be unavailable (trimmed after compaction); got {result:?}"
            );
            let read_b = unwrap_read(process(&shard, read_req_from(agg_b.clone(), 2)).await);
            assert_eq!(read_b.event_batches.len(), 2, "batches 2 and 3 must survive compaction");

            // Direct bloom check: SoftTrim key must appear in the rebuilt bloom.
            let seg = shard.log_segments_cache.get(cr.log_id).await.unwrap();
            let bloom_has_b = seg.metadata.borrow()
                .read.as_ref().unwrap()
                .aggregate_key_bloom.may_contain(&agg_b);
            assert!(bloom_has_b, "compacted bloom must include the SoftTrim aggregate key");

            shard.close().await;
        });
    }

    #[test]
    fn compact_schema_registration_bloom_survives_restart() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Round 1: register schema, write valid event, compact, close.
            {
                let shard = open_compact_shard(&dir).await;
                let agg_b = key(1, 1, 2);
                let filler = key(2, 2, 99); // org=2 type=2 — not subject to the org=1 type=1 schema

                process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
                write_ok(&shard, write_req(agg_b.clone(), json_events(&[br#"{"name":"alice","age":30}"#], 1, 0))).await;

                // Filler must be a different org/type so fat (non-JSON) writes bypass the schema.
                write_fat(&shard, &filler, 40).await;
                process(&shard, delete_req(filler)).await.unwrap();

                trigger_rotation(&shard).await;
                shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("compaction should run (schema+event in segment with dead filler)");

                shard.close().await;
            }

            // Round 2: cold restart — schema cache is empty, bloom must locate the schema.
            // If the compacted bloom were wrong, schema lookup would fail, the write would
            // bypass validation, and the assertion would catch the silent correctness failure.
            {
                let shard = open_compact_shard(&dir).await;
                let agg_b = key(1, 1, 2);

                let bad_evts = json_events(&[br#"{"name":"bob"}"#], 1, 0); // missing required "age"
                let result = process(&shard, write_req(agg_b.clone(), bad_evts)).await;
                assert!(
                    matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))),
                    "schema must still be enforced after compaction + restart; got {result:?}"
                );

                shard.close().await;
            }
        });
    }

    // ── Additional compaction tests: list operations, restart, datablock positions, WAL index gaps ──

    fn list_aggs_req_with_cursor(org: Option<u128>, atype: Option<u128>, cursor: Option<u64>) -> ClientRequest {
        ClientRequest::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: org,
            aggregate_type_id: atype,
            cursor,
        })
    }

    fn compact_config_small_page(dir: &std::path::Path, list_page_size: usize) -> InternalShardConfig {
        InternalShardConfig {
            list_page_size,
            ..compact_config(dir)
        }
    }

    #[test]
    fn compact_list_operations_correct_after_compaction() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            // Four aggregates across two orgs and two types.
            let agg_a = key(1, 1, 1); // org=1, type=1, id=1 — will be deleted
            let agg_b = key(1, 1, 2); // org=1, type=1, id=2 — survives
            let agg_c = key(1, 2, 1); // org=1, type=2, id=1 — survives
            let agg_d = key(2, 1, 1); // org=2, type=1, id=1 — survives

            // Fill segment 1 with agg_a fat data (dominates the dead ratio after deletion).
            write_fat(&shard, &agg_a, 40).await;

            // Write small amounts to the surviving aggregates.
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_c.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_d.clone(), events(2))).await;

            // Delete agg_a — dead ratio well above 20%.
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Seal segment 1.
            trigger_rotation(&shard).await;

            // Compact.
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (agg_a's 40 fat batches dead)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // list_orgs: must return both org=1 and org=2.
            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let org_ids: Vec<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            assert!(org_ids.contains(&1), "org=1 should be present; got {org_ids:?}");
            assert!(org_ids.contains(&2), "org=2 should be present; got {org_ids:?}");

            // list_types for org=1: should return type=1 and type=2.
            let types_org1 = unwrap_list_types(process(&shard, list_types_req(Some(1))).await);
            let type_ids_org1: Vec<u128> = types_org1.aggregate_types.iter().map(|t| t.aggregate_type_id).collect();
            assert!(type_ids_org1.contains(&1), "org=1 type=1 should be present; got {type_ids_org1:?}");
            assert!(type_ids_org1.contains(&2), "org=1 type=2 should be present; got {type_ids_org1:?}");
            assert!(types_org1.aggregate_types.iter().all(|t| t.org_id == 1));

            // list_types for org=2: should return type=1 only.
            let types_org2 = unwrap_list_types(process(&shard, list_types_req(Some(2))).await);
            assert_eq!(types_org2.aggregate_types.len(), 1);
            assert_eq!(types_org2.aggregate_types[0].aggregate_type_id, 1);

            // list_aggs for org=1, type=1: must return agg_b (alive) and show agg_a as deleted.
            // Tombstones (SoftDelete metablocks) are preserved during compaction for cross-segment
            // safety, so agg_a still appears in list results — but marked is_deleted=true.
            let aggs_1_1 = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);
            let live_ids: Vec<u128> = aggs_1_1.aggregates.iter().filter(|a| !a.is_deleted).map(|a| a.aggregate_id).collect();
            let deleted_ids: Vec<u128> = aggs_1_1.aggregates.iter().filter(|a| a.is_deleted).map(|a| a.aggregate_id).collect();
            assert!(live_ids.contains(&2), "agg_b (id=2) should be live; live_ids={live_ids:?}");
            assert!(!live_ids.contains(&1), "agg_a (id=1) should NOT be live (was deleted); live_ids={live_ids:?}");
            assert!(deleted_ids.contains(&1), "agg_a (id=1) tombstone should still appear as deleted; deleted_ids={deleted_ids:?}");

            // list_aggs for org=1, type=2: must return agg_c.
            let aggs_1_2 = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(2))).await);
            assert_eq!(aggs_1_2.aggregates.len(), 1);
            assert_eq!(aggs_1_2.aggregates[0].aggregate_id, 1);

            // list_aggs for org=2, type=1: must return agg_d.
            let aggs_2_1 = unwrap_list_aggs(process(&shard, list_aggs_req(Some(2), Some(1))).await);
            assert_eq!(aggs_2_1.aggregates.len(), 1);
            assert_eq!(aggs_2_1.aggregates[0].aggregate_id, 1);

            shard.close().await;
        });
    }

    #[test]
    fn compact_restart_preserves_data() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Round 1: write, delete filler, compact, verify — then close.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let agg_b = key(1, 1, 2);
                let filler = key(1, 1, 99);

                // Small writes to A and B (will survive).
                write_ok(&shard, write_req(agg_a.clone(), events(3))).await;
                write_ok(&shard, write_req(agg_b.clone(), events(2))).await;

                // Fill segment with filler fat data so dead ratio > 20% after deletion.
                write_fat(&shard, &filler, 40).await;

                // Delete filler.
                let result = process(&shard, delete_req(filler.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));

                // Seal segment and compact.
                trigger_rotation(&shard).await;
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("compaction should run (filler dominates dead data)");
                assert_eq!(cr.log_id, 1);
                assert!(cr.compacted_size < cr.original_size);

                // Verify reads before close.
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches.len(), 1);
                assert_eq!(read_a.event_batches[0].events.len(), 3);

                let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
                assert_eq!(read_b.event_batches.len(), 1);
                assert_eq!(read_b.event_batches[0].events.len(), 2);

                let result = process(&shard, read_req(filler.clone())).await;
                assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

                shard.close().await;
            }

            // Round 2: reopen from disk, verify everything survives cold-cache reload.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let agg_b = key(1, 1, 2);
                let filler = key(1, 1, 99);

                // A must be readable with correct event data.
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches.len(), 1, "A: expected 1 batch after restart");
                assert_eq!(read_a.event_batches[0].events.len(), 3, "A: expected 3 events after restart");

                // B must be readable with correct event data.
                let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
                assert_eq!(read_b.event_batches.len(), 1, "B: expected 1 batch after restart");
                assert_eq!(read_b.event_batches[0].events.len(), 2, "B: expected 2 events after restart");

                // Filler must still be deleted.
                let result = process(&shard, read_req(filler.clone())).await;
                assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
                    "filler should still be deleted after restart; got {result:?}");

                // List operations must still work.
                let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
                let org_ids: Vec<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
                assert!(org_ids.contains(&1), "org=1 should still be listed after restart; got {org_ids:?}");

                let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);
                let agg_ids: Vec<u128> = aggs.aggregates.iter()
                    .filter(|a| !a.is_deleted)
                    .map(|a| a.aggregate_id)
                    .collect();
                assert!(agg_ids.contains(&1), "agg_a should be listed after restart; got {agg_ids:?}");
                assert!(agg_ids.contains(&2), "agg_b should be listed after restart; got {agg_ids:?}");

                shard.close().await;
            }
        });
    }

    #[test]
    fn compact_datablock_positions_updated_correctly() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1); // many fat events — deleted, so removed by compaction
            let agg_b = key(1, 1, 2); // some events — kept

            // Fill segment with A's fat data (dominates the dead ratio after deletion).
            write_fat(&shard, &agg_a, 35).await;

            // Write fat batches to B — fat events produce Block-type datablocks (not inline).
            // These will survive compaction, and their datablock_position fields must be updated.
            write_fat(&shard, &agg_b, 3).await;

            // Delete A so its data becomes dead (>20% of segment).
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Seal and compact.
            trigger_rotation(&shard).await;
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (A's 35 batches dead)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // Open the compacted segment and validate datablock positions.
            let seg = shard.log_segments_cache.get(cr.log_id).await.unwrap();
            let (metablocks_start, metablocks_end, file_len) = {
                let meta = seg.metadata.borrow();
                (
                    HEADER_BLOCK_SIZE_BYTES as u64,
                    meta.readable_metablocks_end(),
                    meta.file_len,
                )
            };

            let guard = seg.lock_reader("test_datablock_positions").await.unwrap();
            let dma_file = guard.as_ref().unwrap();

            let tail_header_start = file_len - HEADER_BLOCK_SIZE_BYTES as u64;

            let mut datablock_refs: Vec<(u64, u64, u64)> = Vec::new();
            let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, String>(
                dma_file,
                false,
                metablocks_start,
                metablocks_end,
                SCAN_CHUNK_SIZE,
                |_pos, block| {
                    let mb = deserialise_metablock(block).map_err(|e| format!("deser error: {e:?}"))?;

                    if let DatablockStorageKind::Block(_) = &mb.datablock {
                        // datablock_position must be after the metablock region.
                        assert!(
                            mb.datablock_position >= metablocks_end,
                            "datablock_position ({}) must be >= metablocks_end ({}) for wal_index {}",
                            mb.datablock_position, metablocks_end, mb.wal_index
                        );

                        // datablock_position + compressed_size must be <= tail header start.
                        assert!(
                            mb.datablock_position + mb.compressed_size <= tail_header_start,
                            "datablock at {} + {} = {} exceeds tail header start {} for wal_index {}",
                            mb.datablock_position, mb.compressed_size,
                            mb.datablock_position + mb.compressed_size,
                            tail_header_start, mb.wal_index
                        );

                        datablock_refs.push((mb.datablock_position, mb.compressed_size, mb.wal_index));
                    }

                    Ok::<bool, String>(false)
                },
            )
            .await;

            match result {
                Ok(_) => {}
                Err(ReadVisitError::Visitor(e)) => {
                    panic!("metablock validation failed: {e}");
                }
                Err(ReadVisitError::Io(e)) => {
                    panic!("io error during metablock scan: {e:?}");
                }
            }

            assert!(!datablock_refs.is_empty(), "expected at least one Block-type datablock after compaction");

            for (pos, size, wal_idx) in &datablock_refs {
                let buf = dma_file.read_at(*pos, *size as usize).await
                    .unwrap_or_else(|e| panic!("failed to read datablock at pos={pos} size={size} wal_index={wal_idx}: {e:?}"));
                assert_eq!(
                    buf.len(), *size as usize,
                    "read {} bytes but expected {} for wal_index={wal_idx} at pos={pos}",
                    buf.len(), size
                );
                assert!(
                    !buf.iter().all(|&b| b == 0),
                    "datablock at pos={pos} size={size} wal_index={wal_idx} is all zeros — compaction did not write payload"
                );
            }

            // Verify datablocks do not overlap each other.
            datablock_refs.sort_by_key(|(pos, _, _)| *pos);
            for i in 1..datablock_refs.len() {
                let (prev_pos, prev_size, prev_idx) = datablock_refs[i - 1];
                let (cur_pos, _, cur_idx) = datablock_refs[i];
                assert!(
                    prev_pos + prev_size <= cur_pos,
                    "datablocks overlap: [{prev_pos}, {}) and [{cur_pos}, ...) for wal_indices {prev_idx} and {cur_idx}",
                    prev_pos + prev_size
                );
            }

            drop(guard); // release reader lock before process() needs it
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 3, "B should have 3 fat batches after compaction");

            shard.close().await;
        });
    }

    #[test]
    fn compact_wal_index_gaps_transparent_to_reads() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1); // 5 sequential batches — all kept
            let filler = key(1, 1, 99); // fat data — deleted, creates WAL index gaps

            // Write 5 batches to A first, then bulk filler so filler is interleaved or after.
            // The key is that after compaction, A's 5 metablocks remain but filler's are gone,
            // leaving wal_index gaps in the compacted file.
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 1
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 2
            write_fat(&shard, &filler, 10).await;                        // filler interleaved
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 3
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 4
            write_fat(&shard, &filler, 20).await;                        // more filler
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 5

            // Delete filler — creates wal_index gaps when filler's metablocks are removed.
            let result = process(&shard, delete_req(filler.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Seal and compact.
            trigger_rotation(&shard).await;
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (filler dominates dead data)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // Walk the compacted segment and prove wal_index values have gaps.
            // agg_a's metablocks and filler's SoftDelete tombstone survive; filler's
            // EventBatchMetadata entries are removed, leaving wal_index gaps.
            {
                let seg = shard.log_segments_cache.get(cr.log_id).await.unwrap();
                let (metablocks_start, metablocks_end) = {
                    let meta = seg.metadata.borrow();
                    (HEADER_BLOCK_SIZE_BYTES as u64, meta.readable_metablocks_end())
                };
                let guard = seg.lock_reader("test_wal_index_gaps").await.unwrap();
                let dma_file = guard.as_ref().unwrap();

                let mut wal_indices: Vec<u64> = Vec::new();
                let scan = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, String>(
                    dma_file,
                    false,
                    metablocks_start,
                    metablocks_end,
                    SCAN_CHUNK_SIZE,
                    |_pos, block| {
                        let mb = deserialise_metablock(block).map_err(|e| format!("deser error: {e:?}"))?;
                        wal_indices.push(mb.wal_index);
                        Ok::<bool, String>(false)
                    },
                )
                .await;
                match scan {
                    Ok(_) => {}
                    Err(ReadVisitError::Visitor(e)) => {
                        panic!("wal_index scan failed: {e}");
                    }
                    Err(ReadVisitError::Io(e)) => {
                        panic!("io error during wal_index scan: {e:?}");
                    }
                }

                // Indices must be strictly ascending.
                assert!(
                    wal_indices.windows(2).all(|w| w[1] > w[0]),
                    "wal_indices in compacted file are not strictly ascending: {wal_indices:?}"
                );

                // There must be at least one gap (filler metablocks were removed).
                let has_gap = wal_indices.windows(2).any(|w| w[1] != w[0] + 1);
                assert!(
                    has_gap,
                    "compacted file should have wal_index gaps but indices are contiguous: {wal_indices:?}"
                );
            }

            // Read A from batch 0 — should return all 5 batches.
            let read_all = unwrap_read(process(&shard, read_req_from(agg_a.clone(), 0)).await);
            assert_eq!(read_all.event_batches.len(), 5, "expected all 5 batches; got {:?}",
                read_all.event_batches.iter().map(|b| b.event_batch_index).collect::<Vec<_>>());
            for (i, batch) in read_all.event_batches.iter().enumerate() {
                assert_eq!(batch.event_batch_index, (i + 1) as u64,
                    "batch {} should have index {}", i, i + 1);
                assert_eq!(batch.events.len(), 2, "batch {} should have 2 events", i + 1);
            }

            // Read A from batch 3 onwards — should return batches 3, 4, 5.
            let read_from_3 = unwrap_read(process(&shard, read_req_from(agg_a.clone(), 3)).await);
            assert_eq!(read_from_3.event_batches.len(), 3,
                "expected batches 3, 4, 5; got {:?}",
                read_from_3.event_batches.iter().map(|b| b.event_batch_index).collect::<Vec<_>>());
            assert!(read_from_3.event_batches.iter().all(|b| b.event_batch_index >= 3),
                "all returned batches should have index >= 3");

            shard.close().await;
        });
    }

    #[test]
    fn compact_wal_index_gaps_list_pagination() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            // Use a small page size (3) to force pagination across WAL index gaps.
            let config = compact_config_small_page(&dir, 3);
            let shard = ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();

            // Create 10 live aggregates.
            let live_aggs: Vec<AggregateKey> = (1..=10u128).map(|i| key(1, 1, i)).collect();
            for agg in &live_aggs {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            // Interleave fat filler aggregates that will be deleted (creates wal_index gaps).
            let filler_a = key(1, 2, 1);
            let filler_b = key(1, 2, 2);
            write_fat(&shard, &filler_a, 15).await;
            write_fat(&shard, &filler_b, 15).await;

            // Delete filler — dead data now dominates.
            let result = process(&shard, delete_req(filler_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));
            let result = process(&shard, delete_req(filler_b.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Seal and compact.
            trigger_rotation(&shard).await;
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (filler dead data dominates)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // Paginated list_aggs for org=1, type=1 — must find all 10 live aggregates.
            // With page_size=3, we need up to 4 pages.
            let mut all_found: Vec<u128> = Vec::new();
            let mut cursor: Option<u64> = None;

            loop {
                let resp = unwrap_list_aggs(process(&shard, list_aggs_req_with_cursor(Some(1), Some(1), cursor)).await);

                // Collect non-deleted aggregates.
                for agg in &resp.aggregates {
                    if !agg.is_deleted {
                        all_found.push(agg.aggregate_id);
                    }
                }

                cursor = resp.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }

            // All 10 live aggregates must be found across all pages.
            all_found.sort_unstable();
            let expected: Vec<u128> = (1..=10).collect();
            assert_eq!(all_found, expected,
                "pagination should find all 10 live aggregates across WAL index gaps; found {all_found:?}");

            shard.close().await;
        });
    }

    #[test]
    fn compact_recreated_aggregate_preserves_post_tombstone_data() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);

                // Segment 1: fill with A's fat data and small B data.
                write_fat(&shard, &agg_a, 45).await;

                // Keep writing until we actually rotate (available space may vary slightly).
                let mut i = 1u64;
                while shard.log_segments_cache.active_log_id() == 1 {
                    write_ok(&shard, write_req(agg_a.clone(), fat_event(i))).await;
                    i += 1;
                }
                assert!(shard.log_segments_cache.active_log_id() == 2);

                // Segment 2: soft-delete A (allow_recreate=true), then re-create with new events.
                // Fill segment 2 with extra events pre-delete (compacted later)
                write_fat(&shard, &agg_a, 40).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 2);

                let result = process(&shard, delete_req_full(agg_a.clone(), true, true, None)).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_a.clone(), events(3))).await;

                // Seal segment 2.
                let mut i = 1u64;
                while shard.log_segments_cache.active_log_id() == 2 {
                    write_ok(&shard, write_req(agg_a.clone(), fat_event(i))).await;
                    i += 1;
                }
                assert_eq!(shard.log_segments_cache.active_log_id(), 3);

                // Compact segment 1: A's 45 pre-deletion metablocks are dead (tombstone in seg 2).
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("compaction should run (A's 45 fat batches in seg 1 are dead)");
                assert_eq!(cr.log_id, 1);
                assert!(cr.compacted_size < cr.original_size);

                // Compact segment 2: 
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("compaction should run for segment file 2");
                assert_eq!(cr.log_id, 2);
                assert!(cr.compacted_size < cr.original_size);

                // A's post-recreation events (in seg 2, untouched) are still accessible.
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert!(read_a.event_batches.len() > 1);
                assert!(read_a.event_batches[0].event_batch_index > 1); //continuation of indexing
                assert_eq!(read_a.event_batches[0].events.len(), 3);

                shard.close().await;
            }

            // Reopen from disk (empty cache) and re-verify: compacted layout must be durable.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);

                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert!(read_a.event_batches.len() > 1);
                assert!(read_a.event_batches[0].event_batch_index > 1);
                assert_eq!(read_a.event_batches[0].events.len(), 3);

                shard.close().await;
            }
        });
    }

    /// Two aggregates (A and B) each deleted and recreated twice across multiple log segments.
    /// After compacting every segment and reopening, both aggregates must return only their
    /// post-second-recreation events with correct index continuation.
    #[test]
    fn compact_multi_delete_recreate_two_aggregates() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Helper: seal to the next segment.
            async fn next_seg<R: ReplicationClient, D: S3Downloader>(
                shard: &ShardWal<R, D>,
                filler: &AggregateKey,
                current: u64,
            ) {
                while shard.log_segments_cache.active_log_id() == current {
                    write_ok(shard, write_req(filler.clone(), fat_event(1))).await;
                }
            }

            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let agg_b = key(1, 1, 2);
                // filler is used purely to drive segment rotation; it will be deleted.
                let filler = key(9, 9, 9);

                // ── Segment 1 ──────────────────────────────────────────────────────────
                // Pre-deletion data for A (life 1) and B (life 1).
                write_fat(&shard, &agg_a, 20).await;
                write_fat(&shard, &agg_b, 20).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 1);
                next_seg(&shard, &filler, 1).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 2);

                // ── Segment 2 ──────────────────────────────────────────────────────────
                // First delete+recreate for A and B.
                let r = process(&shard, delete_req_full(agg_a.clone(), true, true, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // A life 2, batch index continues

                let r = process(&shard, delete_req_full(agg_b.clone(), true, true, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_b.clone(), events(4))).await; // B life 2

                // More post-recreation data to also sit in seg 2.
                write_fat(&shard, &agg_a, 10).await;
                write_fat(&shard, &agg_b, 10).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 2);
                next_seg(&shard, &filler, 2).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 3);

                // ── Segment 3 ──────────────────────────────────────────────────────────
                // Second delete+recreate for A and B.
                let r = process(&shard, delete_req_full(agg_a.clone(), true, true, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_a.clone(), events(5))).await; // A life 3

                let r = process(&shard, delete_req_full(agg_b.clone(), true, true, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_b.clone(), events(7))).await; // B life 3

                next_seg(&shard, &filler, 3).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 4);

                // Delete filler so it doesn't interfere with final reads.
                let r = process(&shard, delete_req_full(filler.clone(), false, false, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));

                // ── Compact segments 1, 2, 3 ───────────────────────────────────────────
                // Seg 1: all A and B pre-deletion data is dead (tombstones in seg 2).
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("seg 1 should compact (A+B pre-deletion data is dead)");
                assert_eq!(cr.log_id, 1);
                assert!(cr.compacted_size < cr.original_size);

                // Seg 2: A and B life-2 post-recreation data survives; fat batches + tombstones
                // for second deletion are dead.
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("seg 2 should compact");
                assert_eq!(cr.log_id, 2);
                assert!(cr.compacted_size < cr.original_size);

                // Seg 3: filler deletion tombstone + A/B life-3 events survive.
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("seg 3 should compact");
                assert_eq!(cr.log_id, 3);
                assert!(cr.compacted_size < cr.original_size);

                // Verify A: only life-3 data visible; index must be > 1 (continuation).
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches[0].events.len(), 5, "A life-3 batch should have 5 events");
                assert!(read_a.event_batches[0].event_batch_index > 1, "A index must continue past earlier lives");

                // Verify B: only life-3 data visible; index must be > 1 (continuation).
                let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
                assert_eq!(read_b.event_batches[0].events.len(), 7, "B life-3 batch should have 7 events");
                assert!(read_b.event_batches[0].event_batch_index > 1, "B index must continue past earlier lives");

                shard.close().await;
            }

            // Reopen and re-verify durability.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let agg_b = key(1, 1, 2);

                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches[0].events.len(), 5);
                assert!(read_a.event_batches[0].event_batch_index > 1);

                let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
                assert_eq!(read_b.event_batches[0].events.len(), 7);
                assert!(read_b.event_batches[0].event_batch_index > 1);

                shard.close().await;
            }
        });
    }

    #[test]
    fn compact_restart_multiple_rounds() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Round 1: write data, delete some, seal, compact, close.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_keep = key(1, 1, 1);
                let agg_del = key(2, 2, 2);

                // Fill segment 1 with agg_del fat data (dominates dead ratio after deletion).
                write_fat(&shard, &agg_del, 40).await;

                // Small write to the keeper.
                write_ok(&shard, write_req(agg_keep.clone(), events(3))).await;

                // Delete agg_del — creates dead data in segment 1.
                let result = process(&shard, delete_req(agg_del.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));

                // Seal segment 1 by triggering rotation.
                trigger_rotation(&shard).await;
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("round 1 compaction should run");
                assert_eq!(cr.log_id, 1);
                assert!(cr.compacted_size < cr.original_size);

                let read_keep = unwrap_read(process(&shard, read_req(agg_keep.clone())).await);
                assert_eq!(read_keep.event_batches.len(), 1);

                shard.close().await;
            }

            // Round 2: reopen, fill a new segment with dead data, compact that, close.
            // This tests that segment 1 (already compacted) loads correctly on restart,
            // and that further compaction of other segments doesn't corrupt the state.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_keep = key(1, 1, 1);
                let agg_new = key(3, 3, 3);  // fresh key, never deleted
                let agg_filler = key(4, 4, 4); // fat filler, will be deleted

                // agg_keep must be readable from the compacted segment 1.
                let read_keep = unwrap_read(process(&shard, read_req(agg_keep.clone())).await);
                assert_eq!(read_keep.event_batches.len(), 1, "keeper must survive restart");

                // Write to agg_keep to add another batch.
                write_ok(&shard, write_req(agg_keep.clone(), events(2))).await;

                // Write new aggregate.
                write_ok(&shard, write_req(agg_new.clone(), events(1))).await;

                // Fill the current segment with filler fat data, then delete to create dead data.
                write_fat(&shard, &agg_filler, 40).await;
                let result = process(&shard, delete_req(agg_filler.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));

                // Seal the current segment and compact it.
                let active_before = shard.log_segments_cache.active_log_id();
                // Write to a fresh key to trigger rotation without reusing deleted keys.
                let rotation_filler = key(5, 5, 5);
                while shard.log_segments_cache.active_log_id() == active_before {
                    write_ok(&shard, write_req(rotation_filler.clone(), fat_event(1))).await;
                }

                // Try compacting the newly sealed segment.
                let _ = shard.compact_oldest_eligible_segment().await.unwrap();

                shard.close().await;
            }

            // Round 3: reopen from doubly-processed state and verify correctness.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_keep = key(1, 1, 1);
                let agg_del = key(2, 2, 2);

                // Original keeper must still be readable (it had 2 batches by end of round 2).
                let read_keep = unwrap_read(process(&shard, read_req(agg_keep.clone())).await);
                assert!(!read_keep.event_batches.is_empty(),
                    "keeper should still have events after doubly-processed reload");

                // Round 1's deleted aggregate must still be gone.
                let result = process(&shard, read_req(agg_del.clone())).await;
                assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
                    "agg_del should still be deleted; got {result:?}");

                shard.close().await;
            }
        });
    }

    /// When compaction produces few surviving metablocks plus a small separate
    /// datablock, the datablock DMA buffer's align_down start could reach back into the
    /// metablock region, overwriting metablock content with zeros.
    ///
    /// The restart after compaction is essential: it forces cold-cache disk reads so the
    /// test catches on-disk corruption (in-memory caches would mask it).
    #[test]
    fn compact_small_datablock_does_not_overwrite_metablocks() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            {
                let shard = open_compact_shard(&dir).await;

                let dead = key(1, 1, 0);
                let agg_a = key(1, 1, 1);

                // Fill most of the segment with fat events that will be deleted.
                write_fat(&shard, &dead, 40).await;

                // Write one event batch with a non-compressible payload that forces a
                // separate datablock (> MINIBATCH_SIZE_BYTES after compression).
                // After compaction the file will have few kept metablocks (this batch +
                // the tombstone) with a small datablock — the datablock DMA write's
                // align_down must not reach back into the metablock region.
                let payload: Vec<u8> = (0..600u32)
                    .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
                    .collect();
                write_ok(&shard, write_req(agg_a.clone(), vec![DatablockAggregateEvent {
                    client_event_index: 1,
                    event_type_major: 1,
                    event_value: Arc::new(payload),
                    ..Default::default()
                }])).await;

                // Delete the fat aggregate to create dead space (>20% ratio).
                let result = process(&shard, delete_req(dead.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));

                trigger_rotation(&shard).await;

                let result = shard.compact_oldest_eligible_segment().await.unwrap();
                assert!(result.is_some(), "expected compaction to run");

                shard.close().await;
            }

            // Reopen from disk — caches are cold, reads go to the compacted file.
            // If the datablock DMA alignment overwrote metablock content, this
            // will fail with deserialization or CRC errors.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches.len(), 1, "expected 1 batch after restart");
                assert_eq!(read_a.event_batches[0].events.len(), 1, "expected 1 event after restart");
                assert_eq!(read_a.event_batches[0].events[0].event_value.len(), 600,
                    "event payload should survive compaction");

                shard.close().await;
            }
        });
    }

    // ── Cache warm-up tests ──

    #[test]
    fn warmup_caches_event_batch_and_client() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req(agg.clone(), events(3))).await;
                shard.close().await;
            }

            let shard = open_shard(&dir).await;
            let mut cache = shard.shard_mem_cache.borrow_mut();

            let (in_cache, status) = cache.aggregate_load_status(&agg, CachePath::Write);
            assert!(in_cache, "aggregate should be in cache after warmup");
            assert_eq!(status, AggregateStatus::Found);

            let snap = cache.get_aggregate_snapshot(&agg, CachePath::Write).unwrap();
            assert_eq!(snap.event_index, 3);
            assert_eq!(snap.event_batch_index, 1);

            // Client cache should also be populated from the EventBatch
            let client_key = AggregateClientKey::new(agg.clone(), 1);
            let (client_in_cache, client_event_index) = cache.aggregate_client_load_status(&agg, &client_key);
            assert!(client_in_cache, "client cache should be populated from EventBatch warmup");
            assert_eq!(client_event_index, Some(3));

            drop(cache);
            shard.close().await;
        });
    }

    #[test]
    fn warmup_caches_deleted_aggregate() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                let result = process(&shard, delete_req(agg.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));
                shard.close().await;
            }

            let shard = open_shard(&dir).await;
            let (in_cache, status) = shard.shard_mem_cache.borrow_mut().aggregate_load_status(&agg, CachePath::Write);
            assert!(in_cache, "deleted aggregate should be in cache after warmup");
            assert_eq!(status, AggregateStatus::Deleted);

            shard.close().await;
        });
    }

    #[test]
    fn warmup_caches_trimmed_aggregate() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                let result = process(&shard, trim_req(agg.clone(), 2)).await;
                assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));
                shard.close().await;
            }

            let shard = open_shard(&dir).await;
            let mut cache = shard.shard_mem_cache.borrow_mut();

            let (in_cache, status) = cache.aggregate_load_status(&agg, CachePath::Write);
            assert!(in_cache, "trimmed aggregate should be in cache after warmup");
            assert_eq!(status, AggregateStatus::Found);

            let snap = cache.get_aggregate_snapshot(&agg, CachePath::Write).unwrap();
            assert_eq!(snap.min_event_batch_index, 2, "trim boundary must be reflected in cached snapshot");
            assert_eq!(snap.event_batch_index, 3);
            assert_eq!(snap.event_index, 6);

            // Client cache is populated from the EventBatch (not the SoftTrim itself)
            let client_key = AggregateClientKey::new(agg.clone(), 1);
            let (client_in_cache, client_event_index) = cache.aggregate_client_load_status(&agg, &client_key);
            assert!(client_in_cache, "client cache should be populated from EventBatch after SoftTrim");
            assert_eq!(client_event_index, Some(2));

            drop(cache);
            shard.close().await;
        });
    }

    #[test]
    fn warmup_does_not_cache_deleted_as_found() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);

            {
                let shard = open_shard(&dir).await;
                // Write to both, then delete A
                write_ok(&shard, write_req(agg_a.clone(), events(2))).await;
                write_ok(&shard, write_req(agg_b.clone(), events(3))).await;
                let result = process(&shard, delete_req(agg_a.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));
                shard.close().await;
            }

            let shard = open_shard(&dir).await;

            // A should be Deleted, not Found
            let (in_cache, status) = shard.shard_mem_cache.borrow_mut().aggregate_load_status(&agg_a, CachePath::Write);
            assert!(in_cache);
            assert_eq!(status, AggregateStatus::Deleted, "deleted aggregate must not appear as Found");

            // B should be Found
            let (in_cache, status) = shard.shard_mem_cache.borrow_mut().aggregate_load_status(&agg_b, CachePath::Write);
            assert!(in_cache);
            assert_eq!(status, AggregateStatus::Found);

            shard.close().await;
        });
    }

    #[test]
    fn warmup_respects_timeout() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                shard.close().await;
            }

            // Reopen with zero timeout — warmup should stop immediately
            let mut cfg = test_config(&dir);
            cfg.cache_warmup_max_duration = Duration::ZERO;
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();

            let (in_cache, _) = shard.shard_mem_cache.borrow_mut().aggregate_load_status(&agg, CachePath::Write);
            assert!(!in_cache, "zero-timeout warmup should not populate cache");

            shard.close().await;
        });
    }

    // ── read_segment_summary ──

    #[test]
    fn read_segment_summary_from_rotated_segment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 10, 100);
            let agg_b = key(2, 20, 200);
            write_ok(&shard, write_req(agg_a.clone(), fat_event(1))).await;
            write_ok(&shard, write_req(agg_b.clone(), fat_event(1))).await;

            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() > 1);

            let payload = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await
                .expect("should have summary");

            assert!(payload.orgs.contains(&1));
            assert!(payload.orgs.contains(&2));
            assert!(payload.aggregate_types.contains(&AggregateTypeKey::new(1, 10)));
            assert!(payload.aggregate_types.contains(&AggregateTypeKey::new(2, 20)));
            assert!(payload.aggregates.iter().any(|a| a.aggregate_id == 100));
            assert!(payload.aggregates.iter().any(|a| a.aggregate_id == 200));

            shard.close().await;
        });
    }

    #[test]
    fn read_segment_summary_returns_none_for_active_segment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 1, 1), events(1))).await;

            let active_id = shard.log_segments_cache.active_log_id();
            let result = read_segment_summary(shard.log_segments_cache.shard_dir(), active_id).await;
            assert!(result.is_none());

            shard.close().await;
        });
    }

    #[test]
    fn corrupt_summary_gracefully_returns_none() {
        glommio_test!({
            use crate::shard_wal_sync::summary_path;

            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 10, 100), fat_event(1))).await;
            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() > 1);

            // Corrupt the .summary sidecar file before any read caches it
            let path = summary_path(shard.log_segments_cache.shard_dir(), 1);
            let mut bytes = std::fs::read(&path).expect("summary file should exist");
            bytes[0] ^= 0xFF;
            std::fs::write(&path, &bytes).expect("should write corrupted file");

            let result = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await;
            assert!(result.is_none(), "corrupt summary should return None");

            shard.close().await;
        });
    }

    // ── Summary-based list operations ──

    #[test]
    fn list_active_segment_from_memory() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // Write data without rotation — all in active segment
            write_ok(&shard, write_req(key(1, 10, 100), events(2))).await;
            write_ok(&shard, write_req(key(2, 20, 200), events(3))).await;

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let org_ids: HashSet<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            assert!(org_ids.contains(&1));
            assert!(org_ids.contains(&2));
            assert!(orgs.next_cursor.is_none());

            let types = unwrap_list_types(process(&shard, list_types_req(None)).await);
            assert_eq!(types.aggregate_types.len(), 2);
            assert!(types.next_cursor.is_none());

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(None, None)).await);
            assert_eq!(aggs.aggregates.len(), 2);
            let ids: HashSet<u128> = aggs.aggregates.iter().map(|a| a.aggregate_id).collect();
            assert!(ids.contains(&100));
            assert!(ids.contains(&200));

            shard.close().await;
        });
    }

    #[test]
    fn list_across_segments_with_summaries() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            // Segment 1: two aggregates in different orgs
            write_ok(&shard, write_req(key(1, 10, 100), events(2))).await;
            write_ok(&shard, write_req(key(2, 20, 200), events(1))).await;

            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() > 1);

            // Segment 2 (active): one more aggregate
            write_ok(&shard, write_req(key(3, 30, 300), events(1))).await;

            // list_orgs: all three orgs
            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let org_ids: HashSet<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            assert!(org_ids.contains(&1));
            assert!(org_ids.contains(&2));
            assert!(org_ids.contains(&3));

            // list_types: all three types
            let types = unwrap_list_types(process(&shard, list_types_req(None)).await);
            assert!(types.aggregate_types.len() >= 3);

            // list_types filtered by org=1
            let types_1 = unwrap_list_types(process(&shard, list_types_req(Some(1))).await);
            assert_eq!(types_1.aggregate_types.len(), 1);
            assert_eq!(types_1.aggregate_types[0].aggregate_type_id, 10);

            // list_aggs: all three
            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(None, None)).await);
            let agg_ids: HashSet<u128> = aggs.aggregates.iter().map(|a| a.aggregate_id).collect();
            assert!(agg_ids.contains(&100));
            assert!(agg_ids.contains(&200));
            assert!(agg_ids.contains(&300));

            // Verify stats accumulation: agg 100 had 2 events in 1 batch
            let agg_100 = aggs.aggregates.iter().find(|a| a.aggregate_id == 100).unwrap();
            assert_eq!(agg_100.event_batch_count, 1);
            assert!(!agg_100.is_deleted);

            shard.close().await;
        });
    }

    #[test]
    fn list_aggregates_delete_barrier() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);

            // Segment 1: 3 batches for agg_a
            write_ok(&shard, write_req(agg_a.clone(), events(1))).await;
            write_ok(&shard, write_req(agg_a.clone(), events(1))).await;
            write_ok(&shard, write_req(agg_a.clone(), events(1))).await;

            trigger_rotation(&shard).await;

            // Segment 2 (active): delete agg_a
            let _ = process(&shard, delete_req(agg_a.clone())).await.unwrap();

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);

            // agg_a should appear as deleted
            let agg = aggs.aggregates.iter().find(|a| a.aggregate_id == 1).unwrap();
            assert!(agg.is_deleted);
            // Delete barrier: old batches from segment 1 should NOT be accumulated
            assert_eq!(agg.event_batch_count, 0, "delete barrier should prevent stat accumulation");
            assert_eq!(agg.compressed_size, 0);

            shard.close().await;
        });
    }

    #[test]
    fn standalone_write_updates_segment_summary() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            assert!(shard.shard_mem_cache.borrow().peek_segment_summary().is_empty());

            write_ok(&shard, write_req(key(1, 10, 100), events(1))).await;

            let cache = shard.shard_mem_cache.borrow();
            assert_eq!(cache.peek_segment_summary().len(), 1);
            assert!(cache.peek_segment_summary_orgs().contains(&1));

            shard.close().await;
        });
    }

    #[test]
    fn leader_write_updates_summary_only_after_replication() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Leader with replication that succeeds on first attempt
            let client = FailThenSucceedReplicationClient::new(0, 1);
            let shard = open_leader_shard(&dir, client).await;

            write_ok(&shard, write_req(key(1, 10, 100), events(1))).await;

            // Summary should be populated (replication succeeded → commit_replication ran)
            let cache = shard.shard_mem_cache.borrow();
            assert_eq!(cache.peek_segment_summary().len(), 1,
                "summary should be populated after successful replication");
            assert!(cache.peek_segment_summary_orgs().contains(&1));

            shard.close().await;
        });
    }

    #[test]
    fn leader_rollback_does_not_pollute_segment_summary() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Leader: first write fails replication, second succeeds
            let client = FailThenSucceedReplicationClient::new(1, 1);
            let shard = open_leader_shard(&dir, client).await;

            // Write 1 to org 1: replication fails → rollback
            let result = process(&shard, write_req(key(1, 10, 100), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))));

            // Summary should be empty — rolled-back data must not appear
            assert!(shard.shard_mem_cache.borrow().peek_segment_summary().is_empty(),
                "rolled-back write must not appear in segment summary");

            // Write 2 to org 2: replication succeeds
            write_ok(&shard, write_req(key(2, 20, 200), events(1))).await;

            // Summary should contain only org 2, not org 1
            let cache = shard.shard_mem_cache.borrow();
            assert_eq!(cache.peek_segment_summary().len(), 1);
            assert!(!cache.peek_segment_summary_orgs().contains(&1),
                "rolled-back org 1 must not be in summary");
            assert!(cache.peek_segment_summary_orgs().contains(&2));

            shard.close().await;
        });
    }

    #[test]
    fn prewarm_summary_respects_delete_order() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Write then delete in the active segment, close without rotation
            {
                let shard = open_shard(&dir).await;
                let agg = key(1, 10, 100);
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
                let _ = process(&shard, delete_req(agg)).await.unwrap();
                shard.close().await;
            }

            // Reopen — pre-warm reverse scans the active segment
            let shard = open_shard(&dir).await;

            // The summary must reflect the delete (is_deleted=true), not the write
            let cache = shard.shard_mem_cache.borrow();
            let entry = cache.peek_segment_summary().values().next().unwrap();
            assert!(entry.is_deleted, "pre-warm should replay in forward order: delete after write");

            // list_aggregates should show the aggregate as deleted
            drop(cache);
            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(10))).await);
            let found = aggs.aggregates.iter().find(|a| a.aggregate_id == 100);
            assert!(found.is_some() && found.unwrap().is_deleted,
                "deleted aggregate should appear as deleted after pre-warm");

            shard.close().await;
        });
    }

    #[test]
    fn prewarm_summary_respects_recreate_order() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Write, delete with allow_recreate, recreate — all in active segment
            {
                let shard = open_shard(&dir).await;
                let agg = key(1, 10, 100);
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
                let _ = process(&shard, delete_req_full(agg.clone(), true, false, None)).await.unwrap();
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
                shard.close().await;
            }

            // Reopen
            let shard = open_shard(&dir).await;

            // Summary should show alive with batch_count=1 (new incarnation only)
            let cache = shard.shard_mem_cache.borrow();
            let entry = cache.peek_segment_summary().values().next().unwrap();
            assert!(!entry.is_deleted, "recreated aggregate should not be deleted after pre-warm");
            assert_eq!(entry.event_batch_count, 1,
                "recreated aggregate should have 1 batch (new incarnation only)");

            shard.close().await;
        });
    }

    #[test]
    fn list_orgs_across_segments_after_rotation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Segment 1: write data, then rotate so segment 1 gets a summary
            {
                let shard = open_compact_shard(&dir).await;
                let agg_a = key(1, 10, 100);
                let agg_b = key(2, 20, 200);
                write_ok(&shard, write_req(agg_a.clone(), fat_event(1))).await;
                write_ok(&shard, write_req(agg_b.clone(), fat_event(1))).await;
                trigger_rotation(&shard).await;

                // Segment 2 (active): write more data with a new org
                let agg_c = key(3, 30, 300);
                write_ok(&shard, write_req(agg_c.clone(), events(1))).await;

                shard.close().await;
            }

            // Reopen and verify list_orgs returns orgs from both segments
            let shard = open_compact_shard(&dir).await;

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let mut org_ids: Vec<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            org_ids.sort();
            assert!(org_ids.contains(&1));
            assert!(org_ids.contains(&2));
            assert!(org_ids.contains(&3));

            // list_aggregate_types returns correct types
            let types = unwrap_list_types(process(&shard, list_types_req(Some(1))).await);
            assert_eq!(types.aggregate_types.len(), 1);
            assert_eq!(types.aggregate_types[0].org_id, 1);
            assert_eq!(types.aggregate_types[0].aggregate_type_id, 10);

            // Writes after open are reflected in the active segment summary
            let agg_d = key(4, 40, 400);
            write_ok(&shard, write_req(agg_d, events(1))).await;

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let org_ids: Vec<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            assert!(org_ids.contains(&4), "new writes should appear in active segment summary");

            shard.close().await;
        });
    }
}

