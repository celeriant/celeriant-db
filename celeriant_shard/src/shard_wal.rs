use std::cell::{Cell, RefCell};
use std::collections::{VecDeque};
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use celeriant_disk::files::rwlock_timeout::write_with_timeout;
use celeriant_distributed::node_status::NodeStatus;
use celeriant_rotating_log::errors::ready_up_error::ReadyUpError;
use celeriant_rotating_log::errors::scan_error::ScanError;
use celeriant_wire::disk::disk_format_error::DiskFormatError;
use celeriant_wire::disk::metablock_bytes;
use celeriant_wire::disk::serialised_datablock::{SerialisedDatablock};
use celeriant_wire::disk::versioned_block::deserialise_metablock;
use glommio::sync::RwLock;

use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::mem_snapshot_aggregate::{AggregateStatus, MemSnapshotAggregate};
use celeriant_memcache::metablock_position::MetablockPosition;
use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_msg::process_requests::Request;
use celeriant_msg::process_responses::Response;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{DeleteRequest, ExistsRequest, ListAggregateTypesRequest, ListAggregatesRequest, ListOrgsRequest, ReadRequest, SingleAggregateWrite, TrimStartRequest, WriteRequest};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::{AggregateListItem, AggregateTypeListItem, ExistsResponse, FollowerRejection, ListAggregateTypesResponse, ListAggregatesResponse, ListOrgsResponse, OrgListItem, ReadResponse, ReplicationBatchResponse, ReplicationResult, SuccessResponse};
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_rotating_log::reverse_metablock_scanner::ReverseMetablockScanner;
use celeriant_wal::aggregate_client_key::AggregateClientKey;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::aggregate_type_key::AggregateTypeKey;
use celeriant_wal::constants::{FIRST_EVENT_BATCH_INDEX, FIXED_BLOCK_SIZE_BYTES, GENESIS_HASH};
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use celeriant_wal::datablocks::datablock_kind::DatablockKind;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_event_batch::{EventTypesKind, MetablockEventBatch};
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_wal::metablocks::metablock_soft_delete::MetablockSoftDelete;
use celeriant_wal::metablocks::metablock_soft_trim::MetablockSoftTrim;
use celeriant_watch::aggregate_reader::AggregateReader;
use celeriant_watch::aggregate_watchers::AggregateWatchers;
use lru::LruCache;

use crate::amortisation::coordinator::Coordinator;
use crate::bloom::bloom_filter_cache::BloomFilterCache;
use crate::bloom::event_type_filter::extract_unique_event_types;
use crate::collect_from_disk::{EventBatchFromLogSegmentFile, fetch_datablocks_for_metablocks};
use crate::error::apply_batch_error::ApplyBatchError;
use crate::error::follower_replication_write_error::FollowerReplicationWriteError;
use crate::error::replication_error::ReplicationError;
use crate::error::shard_cache_load_error::ShardCacheLoadError;
use crate::error::shard_delete_error::ShardDeleteError;
use crate::error::shard_error::ShardError;
use crate::error::shard_exists_error::ShardExistsError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::error::shard_listing_error::ShardListingError;
use crate::error::shard_read_error::ShardReadError;
use crate::error::shard_trim_error::ShardTrimError;
use crate::error::shard_write_error::ShardWriteError;
use crate::in_memory_filtering;
use crate::internal_shard_config::InternalShardConfig;
use crate::loading_coordinator::LoadingCoordinator;
use crate::replication_client::ReplicationClient;
use crate::shard_wal_replicate::{capture_replication_snapshot, commit_replication_with_rollback};
use crate::shard_wal_s3_catchup;
use crate::shard_wal_sync::{capture_fsync_snapshot, commit_fsync_with_rollback};

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
pub struct ShardWal<R: ReplicationClient + 'static> {
    /// No async in shard_mem_cache and no interior mutability
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,

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
    pub node_status: Rc<Cell<NodeStatus>>,

    config: InternalShardConfig,

    /// Serializes concurrent aggregate snapshot loading from disk
    aggregate_loading: LoadingCoordinator<AggregateKey>,

    /// Serializes concurrent client event index loading from disk
    aggregate_client_loading: LoadingCoordinator<AggregateClientKey>,

    /// Client for replicating data to followers or S3
    replication_client: Rc<RwLock<R>>,
}

impl<R: ReplicationClient + 'static> AggregateReader for ShardWal<R> {
    fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
        Rc::clone(&self.watched_aggregates)
    }
}

impl<R: ReplicationClient + 'static> ShardWal<R> {
    pub async fn process_request(&self, lease_index: Option<u64>, request: Request) -> Result<Response, ShardError> {
        match request {
            Request::Exists(exists_request) => self.exists(&exists_request).await.map(Response::Exists).map_err(ShardError::Exists),
            Request::Read(read_request) => self.read(&read_request).await.map(Response::Read).map_err(ShardError::Read),
            Request::Write(write_request) => {
                self.write(lease_index, write_request)
                    .await
                    .map(Response::Write)
                    .map_err(ShardError::Write)
            }
            Request::TrimStart(trim_start_request) => {
                self.trim_start(lease_index, trim_start_request)
                    .await
                    .map(Response::TrimStart)
                    .map_err(ShardError::TrimStart)
            }
            Request::Delete(delete_request) => {
                self.delete(lease_index, delete_request)
                    .await
                    .map(Response::Delete)
                    .map_err(ShardError::Delete)
            }
            Request::Watch(_) => Err(ShardError::WatchRequestInvalid),
            Request::ListOrgs(list_request) => {
                self.list_orgs(list_request).await.map(Response::ListOrgs).map_err(ShardError::ListOrgs)
            }
            Request::ListAggregateTypes(list_request) => {
                self.list_aggregate_types(list_request)
                    .await
                    .map(Response::ListAggregateTypes)
                    .map_err(ShardError::ListAggregateTypes)
            }
            Request::ListAggregates(list_request) => {
                self.list_aggregates(list_request)
                    .await
                    .map(Response::ListAggregates)
                    .map_err(ShardError::ListAggregates)
            }
            Request::ReplicationBatch(replication_request) => {
                self.handle_replication_batch(replication_request)
                    .await
                    .map(Response::ReplicationBatch)
                    .map_err(ShardError::ReplicationBatch)
            }
            Request::CatchUp(_) => Err(ShardError::CatchUpRequestInvalid),
            Request::Heartbeat(_) => Err(ShardError::CatchUpRequestInvalid),
        }
    }

    /// Open or create a shard WAL.
    ///
    /// If the shard directory exists with log files, reopens from the latest.
    /// Otherwise creates a new shard with an empty log file.
    pub async fn open(config: InternalShardConfig, node_status: NodeStatus, replication_client: R) -> Result<Self, ReadyUpError> {
        let shard_mem_cache = ShardMemCache::new(
            config.recent_write_cache_bytes,
            config.aggregate_snapshots_cache_bytes,
            config.aggregate_client_snapshots_cache_bytes,
            config.list_wal_index_cache_bytes,
            config.pending_replication_high_water_bytes,
        );

        let log_segments_cache = LogSegmentsCache::ready_up(
            config.shard_dir.clone(),
            config.shard_log_preallocate_bytes,
            config.max_open_files as usize,
        )
        .await?;

        Ok(Self {
            shard_mem_cache: Rc::new(RefCell::new(shard_mem_cache)),
            log_segments_cache: Rc::new(log_segments_cache),
            fsync_coordinator: Rc::new(Coordinator::new()),
            replication_coordinator: Rc::new(Coordinator::new()),
            watched_aggregates: Rc::new(AggregateWatchers::new()),
            bloom_filter_cache: Rc::new(BloomFilterCache::new()),
            node_status: Rc::new(Cell::new(node_status)),
            config,
            aggregate_loading: LoadingCoordinator::new(),
            aggregate_client_loading: LoadingCoordinator::new(),
            replication_client: Rc::new(RwLock::new(replication_client)),
        })
    }

    /// List all unique organizations that have data in this shard.
    /// 
    /// Scans WAL in reverse order, returning orgs with most recent activity first.
    /// Uses bounded LRU for deduplication within a page.
    pub async fn list_orgs(&self, request: ListOrgsRequest) -> Result<ListOrgsResponse, ShardListingError> {
        let start_time = Instant::now();
        let max_duration = self.config.list_max_duration;
        let page_size = self.config.list_page_size;
        let start_wal_index = request.cursor.unwrap_or(u64::MAX);

        // Bounded deduplication: org_id -> ()
        let mut seen: LruCache<u128, ()> = LruCache::new(
            NonZeroUsize::new(page_size.saturating_mul(4).max(100)).unwrap()
        );
        let mut results: Vec<OrgListItem> = Vec::with_capacity(page_size);
        let mut last_wal_index: Option<u64> = None;
        let mut reached_end = false;

        // Try to find a cached starting position
        let (start_log_id, start_pos) = self.find_list_scan_start(start_wal_index).await;

        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            start_log_id,
            start_pos,
            self.config.read_max_chunk_size,
        );

        let scan_result = scanner
            .scan::<bool, ()>(|log_id, pos, bytes| {
                // Check time limit
                if start_time.elapsed() >= max_duration {
                    return Ok(Some(false)); // Stop due to timeout, not end of WAL
                }

                let wal_index = metablock_bytes::read_wal_index(bytes);

                // Skip entries newer than our cursor
                if wal_index > start_wal_index {
                    return Ok(None);
                }

                // Cache this position for future lookups (sample every ~100 entries)
                if wal_index % 100 == 0 {
                    self.shard_mem_cache.borrow_mut().cache_wal_index_position(wal_index, log_id, pos);
                }

                // Only process EventBatch metablocks for org discovery
                if !metablock_bytes::is_metablock_kind_event_batch_metadata(bytes) {
                    return Ok(None);
                }

                let org_id = metablock_bytes::read_event_batch_org_id(bytes);

                if !seen.contains(&org_id) {
                    seen.put(org_id, ());
                    results.push(OrgListItem { org_id });
                    last_wal_index = Some(wal_index);

                    if results.len() >= page_size {
                        return Ok(Some(false)); // Page full
                    }
                }

                Ok(None)
            })
            .await
            .map_err(ShardListingError::ReadFromDiskError)?;

        // If scan completed without early exit, we reached the end
        if scan_result.is_none() {
            reached_end = true;
        }

        let next_cursor = if reached_end || results.is_empty() {
            None
        } else {
            last_wal_index.map(|i| i.saturating_sub(1))
        };

        Ok(ListOrgsResponse {
            correlation_id: request.correlation_id,
            orgs: results,
            next_cursor,
        })
    }

    /// List aggregate types, optionally filtered by org_id.
    ///
    /// Scans WAL in reverse order, returning types with most recent activity first.
    pub async fn list_aggregate_types(&self, request: ListAggregateTypesRequest) -> Result<ListAggregateTypesResponse, ShardListingError> {
        let start_time = Instant::now();
        let max_duration = self.config.list_max_duration;
        let page_size = self.config.list_page_size;
        let start_wal_index = request.cursor.unwrap_or(u64::MAX);
        let filter_org_id = request.org_id;

        // Bounded deduplication: (org_id, aggregate_type_id) -> ()
        let mut seen: LruCache<AggregateTypeKey, ()> = LruCache::new(
            NonZeroUsize::new(page_size.saturating_mul(4).max(100)).unwrap()
        );
        let mut results: Vec<AggregateTypeListItem> = Vec::with_capacity(page_size);
        let mut last_wal_index: Option<u64> = None;
        let mut reached_end = false;

        let (start_log_id, start_pos) = self.find_list_scan_start(start_wal_index).await;

        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            start_log_id,
            start_pos,
            self.config.read_max_chunk_size,
        );

        let scan_result = scanner
            .scan::<bool, ()>(|log_id, pos, bytes| {
                if start_time.elapsed() >= max_duration {
                    return Ok(Some(false));
                }

                let wal_index = metablock_bytes::read_wal_index(bytes);

                if wal_index > start_wal_index {
                    return Ok(None);
                }

                if wal_index % 100 == 0 {
                    self.shard_mem_cache.borrow_mut().cache_wal_index_position(wal_index, log_id, pos);
                }

                if !metablock_bytes::is_metablock_kind_event_batch_metadata(bytes) {
                    return Ok(None);
                }

                let org_id = metablock_bytes::read_event_batch_org_id(bytes);

                // Apply org filter if specified
                if let Some(filter) = filter_org_id {
                    if org_id != filter {
                        return Ok(None);
                    }
                }

                let aggregate_type_id = metablock_bytes::read_event_batch_aggregate_type_id(bytes);
                let type_key = AggregateTypeKey::new(org_id, aggregate_type_id);

                if !seen.contains(&type_key) {
                    seen.put(type_key, ());
                    results.push(AggregateTypeListItem { org_id, aggregate_type_id });
                    last_wal_index = Some(wal_index);

                    if results.len() >= page_size {
                        return Ok(Some(false));
                    }
                }

                Ok(None)
            })
            .await
            .map_err(ShardListingError::ReadFromDiskError)?;

        if scan_result.is_none() {
            reached_end = true;
        }

        let next_cursor = if reached_end || results.is_empty() {
            None
        } else {
            last_wal_index.map(|i| i.saturating_sub(1))
        };

        Ok(ListAggregateTypesResponse {
            correlation_id: request.correlation_id,
            aggregate_types: results,
            next_cursor,
        })
    }

    /// List aggregates, optionally filtered by org_id and/or aggregate_type_id.
    ///
    /// Scans WAL in reverse order. Returns aggregates with accumulated statistics
    /// from batches seen within this page. Client must merge stats across pages.
    pub async fn list_aggregates(&self, request: ListAggregatesRequest) -> Result<ListAggregatesResponse, ShardListingError> {
        let start_time = Instant::now();
        let max_duration = self.config.list_max_duration;
        let page_size = self.config.list_page_size;
        let start_wal_index = request.cursor.unwrap_or(u64::MAX);
        let filter_org_id = request.org_id;
        let filter_aggregate_type_id = request.aggregate_type_id;

        // Track aggregate stats with insertion order preserved
        // Key -> accumulated stats for this page
        struct AggregatePageStats {
            is_deleted: bool,
            event_batch_count: u64,
            min_event_timestamp: u64,
            max_event_timestamp: u64,
            min_server_timestamp: u64,
            max_server_timestamp: u64,
            min_event_batch_index: u64,
            max_event_batch_index: u64,
            min_event_index: u64,
            max_event_index: u64,
            compressed_size: u64,
            uncompressed_size: u64,
        }

        // TODO: LRU behaviour here needs more testing & optimisation
        let mut seen: LruCache<AggregateKey, AggregatePageStats> = LruCache::new(
            NonZeroUsize::new(page_size.saturating_mul(4).max(100)).unwrap()
        );
        let mut result_order: Vec<AggregateKey> = Vec::with_capacity(page_size);
        let mut last_wal_index: Option<u64> = None;
        let mut reached_end = false;
        let mut unique_count = 0usize;

        let (start_log_id, start_pos) = self.find_list_scan_start(start_wal_index).await;

        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            start_log_id,
            start_pos,
            self.config.read_max_chunk_size,
        );

        let scan_result = scanner
            .scan::<bool, ()>(|log_id, pos, bytes| {
                if start_time.elapsed() >= max_duration {
                    return Ok(Some(false));
                }

                let wal_index = metablock_bytes::read_wal_index(bytes);

                if wal_index > start_wal_index {
                    return Ok(None);
                }

                if wal_index % 100 == 0 {
                    self.shard_mem_cache.borrow_mut().cache_wal_index_position(wal_index, log_id, pos);
                }

                // Handle SoftDelete - mark aggregate as deleted
                if metablock_bytes::is_metablock_kind_soft_delete(bytes) {
                    let aggregate_key = metablock_bytes::read_soft_delete_aggregate_key(bytes);
                    
                    if let Some(filter) = filter_org_id {
                        if aggregate_key.org_id != filter {
                            return Ok(None);
                        }
                    }
                    if let Some(filter) = filter_aggregate_type_id {
                        if aggregate_key.aggregate_type_id != filter {
                            return Ok(None);
                        }
                    }

                    if !seen.contains(&aggregate_key) {
                        // First time seeing this aggregate (as deleted)
                        if unique_count >= page_size {
                            return Ok(Some(false)); // Page full
                        }
                        
                        result_order.push(aggregate_key.clone());
                        seen.put(aggregate_key, AggregatePageStats {
                            is_deleted: true,
                            event_batch_count: 0,
                            min_event_timestamp: u64::MAX,
                            max_event_timestamp: 0,
                            min_server_timestamp: u64::MAX,
                            max_server_timestamp: 0,
                            min_event_batch_index: u64::MAX,
                            max_event_batch_index: 0,
                            min_event_index: u64::MAX,
                            max_event_index: 0,
                            uncompressed_size: 0,
                            compressed_size: 0,
                        });
                        unique_count += 1;
                        last_wal_index = Some(wal_index);
                    }
                    return Ok(None);
                }

                // Handle EventBatch
                if !metablock_bytes::is_metablock_kind_event_batch_metadata(bytes) {
                    return Ok(None);
                }

                let aggregate_key = metablock_bytes::read_event_batch_aggregate_key(bytes);

                if let Some(filter) = filter_org_id {
                    if aggregate_key.org_id != filter {
                        return Ok(None);
                    }
                }
                if let Some(filter) = filter_aggregate_type_id {
                    if aggregate_key.aggregate_type_id != filter {
                        return Ok(None);
                    }
                }

                // Read stats from this metablock
                let event_batch_index = metablock_bytes::read_event_batch_event_batch_index(bytes);
                let min_event_ts = metablock_bytes::read_event_batch_min_event_timestamp(bytes);
                let max_event_ts = metablock_bytes::read_event_batch_max_event_timestamp(bytes);
                let server_ts = metablock_bytes::read_server_timestamp(bytes);
                let min_event_idx = metablock_bytes::read_event_batch_min_event_index(bytes);
                let max_event_idx = metablock_bytes::read_event_batch_max_event_index(bytes);
                let compressed_size = metablock_bytes::read_compressed_size(bytes);
                let uncompressed_size = metablock_bytes::read_uncompressed_size(bytes);

                if let Some(stats) = seen.get_mut(&aggregate_key) {
                    // Already seen this aggregate, accumulate stats
                    stats.event_batch_count += 1;
                    stats.min_event_timestamp = stats.min_event_timestamp.min(min_event_ts);
                    stats.max_event_timestamp = stats.max_event_timestamp.max(max_event_ts);
                    stats.min_server_timestamp = stats.min_server_timestamp.min(server_ts);
                    stats.max_server_timestamp = stats.max_server_timestamp.max(server_ts);
                    stats.min_event_batch_index = stats.min_event_batch_index.min(event_batch_index);
                    stats.max_event_batch_index = stats.max_event_batch_index.max(event_batch_index);
                    stats.min_event_index = stats.min_event_index.min(min_event_idx);
                    stats.max_event_index = stats.max_event_index.max(max_event_idx);
                    stats.compressed_size += compressed_size;
                    stats.uncompressed_size += uncompressed_size;
                } else {
                    // First time seeing this aggregate
                    if unique_count >= page_size {
                        return Ok(Some(false)); // Page full
                    }

                    result_order.push(aggregate_key.clone());
                    seen.put(aggregate_key, AggregatePageStats {
                        is_deleted: false,
                        event_batch_count: 1,
                        min_event_timestamp: min_event_ts,
                        max_event_timestamp: max_event_ts,
                        min_server_timestamp: server_ts,
                        max_server_timestamp: server_ts,
                        min_event_batch_index: event_batch_index,
                        max_event_batch_index: event_batch_index,
                        min_event_index: min_event_idx,
                        max_event_index: max_event_idx,
                        compressed_size,
                        uncompressed_size,
                    });
                    unique_count += 1;
                    last_wal_index = Some(wal_index);
                }

                Ok(None)
            })
            .await
            .map_err(ShardListingError::ReadFromDiskError)?;

        if scan_result.is_none() {
            reached_end = true;
        }

        // Convert to results, preserving insertion order
        let results: Vec<AggregateListItem> = result_order
            .into_iter()
            .filter_map(|key| {
                seen.get(&key).map(|stats| AggregateListItem {
                    org_id: key.org_id,
                    aggregate_type_id: key.aggregate_type_id,
                    aggregate_id: key.aggregate_id,
                    is_deleted: stats.is_deleted,
                    event_batch_count: stats.event_batch_count,
                    min_event_timestamp: if stats.min_event_timestamp == u64::MAX { 0 } else { stats.min_event_timestamp },
                    max_event_timestamp: stats.max_event_timestamp,
                    min_event_batch_index: if stats.min_event_batch_index == u64::MAX { 0 } else { stats.min_event_batch_index },
                    max_event_batch_index: stats.max_event_batch_index,
                    min_event_index: if stats.min_event_index == u64::MAX { 0 } else { stats.min_event_index },
                    max_event_index: stats.max_event_index,
                    min_server_timestamp: if stats.min_server_timestamp == u64::MAX { 0 } else { stats.min_server_timestamp },
                    max_server_timestamp: stats.max_server_timestamp,
                    uncompressed_size: stats.uncompressed_size,
                    compressed_size: stats.compressed_size,
                })
            })
            .collect();

        let next_cursor = if reached_end || results.is_empty() {
            None
        } else {
            last_wal_index.map(|i| i.saturating_sub(1))
        };

        Ok(ListAggregatesResponse {
            correlation_id: request.correlation_id,
            aggregates: results,
            next_cursor,
        })
    }

    /// Find the starting position for a list scan.
    /// 
    /// Tries to use cached position if available, otherwise starts from active log.
    async fn find_list_scan_start(&self, mut target_wal_index: u64) -> (u64, Option<u64>) {
        // If no cursor (starting from latest), just use active log
        if target_wal_index == u64::MAX {
            return (self.log_segments_cache.active_log_id(), None);
        }

        // Make sure we can't read the write tip, only from committed read positions
        let read_cursor = self.log_segments_cache.get_latest_read_cursor();
        target_wal_index = target_wal_index.min(read_cursor.wal_index);

        // Try to find cached position at or near target
        let cached = self.shard_mem_cache.borrow_mut().find_nearest_wal_index_position(target_wal_index);
        
        if let Some((cached_wal_index, pos)) = cached {
            //TODO: Benchmarking and testing of this, needs more thought on cache relevance
            if cached_wal_index <= target_wal_index && target_wal_index - cached_wal_index < 1000 {
                return (pos.log_id, Some(pos.metablock_absolute_pos.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64))); //Include self
            }
        }

        // No useful cache hit, start from active log
        (read_cursor.log_id, None)
    }
    
    pub async fn exists(&self, exists_request: &ExistsRequest) -> Result<ExistsResponse, ShardExistsError> {
        let exists = self.aggregate_exists_and_cache(&exists_request.aggregate_key, CachePath::Read).await?;
        if !exists {
            return Ok(ExistsResponse {
                correlation_id: exists_request.correlation_id,
                min_event_batch_index: 0,
            });
        }

        let last_known_metablock = self
            .shard_mem_cache
            .borrow_mut()
            .get_aggregate_last_metablock_pos(&exists_request.aggregate_key, CachePath::Read);

        Ok(ExistsResponse {
            correlation_id: exists_request.correlation_id,
            min_event_batch_index: last_known_metablock.min_event_batch_index,
        })
    }

    pub async fn trim_start(&self, lease_index: Option<u64>, trim_request: TrimStartRequest) -> Result<SuccessResponse, ShardTrimError> {
        
        let lease_index = lease_index.ok_or(ShardTrimError::InvalidLeaseIndex)?;

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
    
    pub async fn delete(&self, lease_index: Option<u64>, delete_request: DeleteRequest) -> Result<SuccessResponse, ShardDeleteError> {
        
        let lease_index = lease_index.ok_or(ShardDeleteError::InvalidLeaseIndex)?;

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
    pub async fn write(&self, lease_index: Option<u64>, write_request: WriteRequest) -> Result<SuccessResponse, ShardWriteError> {
        
        let lease_index = lease_index.ok_or(ShardWriteError::InvalidLeaseIndex)?;

        // Make sure we have at least one aggregate to write
        if write_request.writes.is_empty() {
            return Err(ShardWriteError::EmptyEventsList);
        }

        // Phase 1: Validation and preparation - all checks that can fail happen here
        // No mutations to shard_mem_cache until all validations pass
        let mut prepared_writes = Vec::with_capacity(write_request.writes.len());

        for (aggregate_key, single_write) in &write_request.writes {
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
            let aggregate_exists = self.aggregate_exists_and_cache(aggregate_key, CachePath::Write).await
                .map_err(ShardWriteError::AggregateExistsAndCacheError)?;
            
            if !aggregate_exists {
                // Check if it was deleted with allow_recreate = false
                let (is_loaded, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(aggregate_key, CachePath::Write);
                if is_loaded && status == AggregateStatus::Deleted {
                    let indexes = self.shard_mem_cache.borrow_mut().get_write_event_indexes(aggregate_key);
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
                self.cache_aggregate_client(aggregate_key, &aggregate_client_key).await
                    .map_err(ShardWriteError::CacheAggregateClientError)?;
            }

            // Validate and prepare - reads from memcache but does not mutate
            let prepared = self.validate_and_prepare_write(
                lease_index,
                aggregate_key,
                write_request.client_id,
                write_request.user_id,
                single_write.clone(),
            )?;

            prepared_writes.push(prepared);
        }

        // Phase 2: Append all prepared writes to queue - cannot fail
        self.append_prepared_writes_to_queue(prepared_writes);

        // Wait on disk write, it's batched for performance
        self.sync_durable().await?;

        // Same deal for replication, if we are the leader, 
        // wait on durable replication, also batched
        self.replicate_durable().await?;

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

        // Take an exclusive lock on this aggregate client
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

    async fn aggregate_exists_and_cache(&self, searching_for_aggregate_key: &AggregateKey, cache_path: CachePath) -> Result<bool, ShardCacheLoadError> {
        // If we are cached already
        if let (true, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key, cache_path) {
            return Ok(status == AggregateStatus::Found);
        }

        // Take an exclusive lock on this aggregate
        let aggregate_lock = self.aggregate_loading.acquire(searching_for_aggregate_key);
        let _ = write_with_timeout(&aggregate_lock, "move_aggregate_to_memcache").await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        // We have exclusive access now, check if another concurrent task has already done the work
        if let (true, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key, cache_path) {
            return Ok(status == AggregateStatus::Found);
        }

        let (starting_log_id, start_from_postion) = match cache_path {
            CachePath::Read => {
                let read_cursor = self.log_segments_cache.get_latest_read_cursor();
                (read_cursor.log_id, Some(read_cursor.metablocks_position))
            },
            CachePath::Write => (self.log_segments_cache.active_log_id(), None),
        };

        // Track if we've seen a more recent trim
        let mut seen_trim_min: Option<u64> = None;

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
                            soft_delete.event_index,
                            soft_delete.event_batch_index,
                            soft_delete.allow_recreate,
                            soft_delete.allow_index_continuation,
                            cache_path,
                        );
                    }
                    return Ok(Some(false)); // Found but deleted
                }

                // Check for soft trim - record min but keep scanning for EventBatch
                if metablock_bytes::is_soft_trim_for_aggregate(metablock_bytes, searching_for_aggregate_key) {
                    let trim_min = metablock_bytes::read_soft_trim_keep_from_event_batch_index(metablock_bytes);
                    match seen_trim_min {
                        None => seen_trim_min = Some(trim_min),
                        Some(existing) if trim_min > existing => seen_trim_min = Some(trim_min),
                        _ => {}
                    }
                    return Ok(None);
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

                let mut min_event_batch_index = metablock_bytes::read_event_batch_min_event_batch_index(metablock_bytes);
                    
                // Apply any more recent trim we saw
                if !low_priority {
                    if let Some(trim_min) = seen_trim_min {
                        if trim_min > min_event_batch_index {
                            min_event_batch_index = trim_min;
                        }
                    }
                }

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

        // Validate client idempotency
        if write_request.enforce_client_idempotency {
            if let Some(last_client_event_index) = shard_mem_cache.get_client_event_index(aggregate_key, client_id) {
                let attempted_client_event_index = write_request.events.iter().map(|e| e.client_event_index).min().unwrap_or(0);
                if attempted_client_event_index <= last_client_event_index {
                    return Err(ShardWriteError::ClientIdempotencyViolation {
                        last_client_event_index,
                        attempted_client_event_index,
                    });
                }
            }
        }

        let aggregate_current_indexes = shard_mem_cache.get_write_event_indexes(aggregate_key);

        // There is a soft delete entry in the queue that hasn't been committed yet
        if aggregate_current_indexes.pending_delete_or_deleted && !aggregate_current_indexes.allow_recreate {
            return Err(ShardWriteError::AggregateRecreateNotAllowed);
        }

        // Validate optimistic concurrency (only for existing aggregates, not recreates)
        if let Some(expected) = write_request.expected_event_batch_index {
            if expected != aggregate_current_indexes.event_batch_index {
                return Err(ShardWriteError::OptimisticConcurrencyViolation {
                    expected_event_batch_index: expected,
                    current_event_batch_index: aggregate_current_indexes.event_batch_index,
                });
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
        let serialized_datablock = SerialisedDatablock::new(&datablock, write_request.compression_type)
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

        // Node status goes into fsync because we need to know if we should advance read position (standalone or follower mode)
        let node_status = self.node_status.clone();

        if rotating_log_cache.force_immediate.get() {
            let mc_capture = shard_mem_cache.clone();
            self.fsync_coordinator
                .request_sync_two_phase(
                    None,
                    move || async move { capture_fsync_snapshot(&mc_capture) },
                    move |captured| commit_fsync_with_rollback(node_status.get(), rotating_log_cache, shard_mem_cache, watched_aggregates, captured),
                )
                .await
        } else if !self.config.non_durable_writes {
            let mc_capture = shard_mem_cache.clone();
            self.fsync_coordinator
                .request_sync_two_phase(
                    Some(self.config.fsync_delay),
                    move || async move { capture_fsync_snapshot(&mc_capture) },
                    move |captured| commit_fsync_with_rollback(node_status.get(), rotating_log_cache, shard_mem_cache, watched_aggregates, captured),
                )
                .await
        } else {
            let fsync_coordinator = self.fsync_coordinator.clone();
            glommio::spawn_local(async move {
                let mc_capture = shard_mem_cache.clone();
                let _ = fsync_coordinator
                    .request_sync_two_phase(
                        None,
                        move || async move { capture_fsync_snapshot(&mc_capture) },
                        move |captured| commit_fsync_with_rollback(node_status.get(), rotating_log_cache, shard_mem_cache, watched_aggregates, captured),
                    )
                    .await;
            })
            .detach();
            Ok(())
        }
    }

    async fn replicate_durable(&self) -> Result<(), ReplicationError> {
        // Only leaders need to replicate
        if !self.node_status.get().is_leader() {
            return Ok(());
        }

        let replication_client = self.replication_client.clone();
        let fsync_coordinator = self.fsync_coordinator.clone();
        let rotating_log_cache = self.log_segments_cache.clone();
        let shard_mem_cache = self.shard_mem_cache.clone();
        let watched_aggregates = self.watched_aggregates.clone();
        let max_catchup_gap_bytes = self.config.max_catchup_gap_bytes;
        let max_request_size = self.config.max_request_size;
        let read_max_chunk_size = self.config.read_max_chunk_size;

        if rotating_log_cache.force_immediate.get() {
            let mc_capture = shard_mem_cache.clone();
            self.replication_coordinator
                .request_sync_two_phase(
                    None,
                    move || async move { capture_replication_snapshot(&mc_capture) },
                    move |captured| commit_replication_with_rollback(replication_client, fsync_coordinator, rotating_log_cache, shard_mem_cache, watched_aggregates, captured, max_catchup_gap_bytes, max_request_size, read_max_chunk_size),
                )
                .await
        } else if !self.config.non_durable_writes {
            let mc_capture = shard_mem_cache.clone();
            self.replication_coordinator
                .request_sync_two_phase(
                    Some(self.config.replication_delay),
                    move || async move { capture_replication_snapshot(&mc_capture) },
                    move |captured| commit_replication_with_rollback(replication_client, fsync_coordinator, rotating_log_cache, shard_mem_cache, watched_aggregates, captured, max_catchup_gap_bytes, max_request_size, read_max_chunk_size),
                )
                .await
        } else {
            let replication_coordinator = self.replication_coordinator.clone();
            glommio::spawn_local(async move {
                let mc_capture = shard_mem_cache.clone();
                let _ = replication_coordinator
                    .request_sync_two_phase(
                        None,
                        move || async move { capture_replication_snapshot(&mc_capture) },
                        move |captured| commit_replication_with_rollback(replication_client, fsync_coordinator, rotating_log_cache, shard_mem_cache, watched_aggregates, captured, max_catchup_gap_bytes, max_request_size, read_max_chunk_size),
                    )
                    .await;
            })
            .detach();
            Ok(())
        }
    }
    
    async fn handle_replication_batch(
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

        if !self.node_status.get().is_follower() {
            return Ok(response(ReplicationResult::Rejected(FollowerRejection::NotAFollower)));
        }

        if follower_timestamp_ms.saturating_sub(request.leader_timestamp_ms) > self.config.max_cluster_time_drift_ms {
            return Ok(response(ReplicationResult::Rejected(FollowerRejection::TimeDriftTooHigh {
                leader_ms: request.leader_timestamp_ms,
                follower_ms: follower_timestamp_ms,
                max_allowed_ms: self.config.max_cluster_time_drift_ms,
            })));
        }

        if request.batches.is_empty() {
            return Ok(response(ReplicationResult::Rejected(FollowerRejection::EmptyBatch)));
        }

        //TODO: How to handle the scenario where we have compacted the log but yet to replicate? 
        //It would be a rare scenario, maybe an invariant we can enforce during compaction
        match shard_wal_s3_catchup::apply_external_batch(
            &self.log_segments_cache, &self.shard_mem_cache, &request.batches,
        ) {
            Ok(()) => {}
            Err(ApplyBatchError::WalIndexMismatch { current, .. }) => {
                return Ok(response(ReplicationResult::Rejected(FollowerRejection::WalIndexMismatch {
                    max_follower_wal_index: current,
                })));
            }
            Err(ApplyBatchError::TipHashMismatch { current, batch }) => {
                return Ok(response(ReplicationResult::Rejected(FollowerRejection::TipHashMismatch {
                    follower: current,
                    leader: batch,
                })));
            }
            Err(ApplyBatchError::MissingDatablock) => {
                return Ok(response(ReplicationResult::Rejected(FollowerRejection::MissingDatablock)));
            }
            Err(ApplyBatchError::SerialiseDatablocks(e)) => {
                return Err(FollowerReplicationWriteError::FailedToSerialiseDatablocks(e));
            }
        }

        self.sync_durable().await
            .map_err(FollowerReplicationWriteError::ShardFSyncError)?;

        Ok(response(ReplicationResult::Success {
            last_follower_metablock: None,
        }))
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

impl<R: ReplicationClient + 'static> ShardWal<R> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::replication_to_follower_error::ReplicateToFollowerError;
    use crate::error::replication_to_s3_error::ReplicateToS3Error;
    use crate::replication_client::StubReplicationClient;
    use celeriant_msg::request::requests::{CatchUpRequest, ReplicationBatchItem, SingleAggregateDelete, WatchRequest};
    use celeriant_wal::compression_type::CompressionType;
    use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
    use crate::timestamp_config::TimestampConfig;
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
            max_open_files: 4,
            shard_log_preallocate_bytes: 4 * 1024 * 1024,
            fsync_delay: Duration::ZERO,
            replication_delay: Duration::ZERO,
            recent_write_cache_bytes: 64 * 1024 * 1024,
            non_durable_writes: false,
            shard_dir: dir.to_path_buf(),
            max_response_size: 16 * 1024 * 1024,
            max_request_size: 16 * 1024 * 1024,
            aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
            aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
            read_max_chunk_size: 32 * 1024,
            timestamp_config: TimestampConfig::default(),
            list_page_size: 100,
            list_max_duration: Duration::from_secs(2),
            list_wal_index_cache_bytes: 1024 * 1024,
            pending_replication_high_water_bytes: 64 * 1024 * 1024,
            max_cluster_time_drift_ms: 5000,
            max_catchup_gap_bytes: 100 * 1024 * 1024,
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

    fn write_req(agg: AggregateKey, evts: Vec<DatablockAggregateEvent>) -> Request {
        write_req_full(agg, evts, true, None, 1, false)
    }

    fn write_req_full(
        agg: AggregateKey,
        evts: Vec<DatablockAggregateEvent>,
        allow_create: bool,
        expected_batch: Option<u64>,
        client_id: u128,
        enforce_idempotency: bool,
    ) -> Request {
        let mut writes = HashMap::new();
        writes.insert(
            agg,
            SingleAggregateWrite {
                events: evts,
                allow_create,
                expected_event_batch_index: expected_batch,
                enforce_client_idempotency: enforce_idempotency,
                compression_type: CompressionType::None,
            },
        );
        Request::Write(WriteRequest {
            correlation_id: None,
            client_id,
            user_id: None,
            writes,
        })
    }

    fn read_req(agg: AggregateKey) -> Request {
        Request::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: agg,
            filters: ReadFilters::new(0),
        })
    }

    fn read_req_from(agg: AggregateKey, from: u64) -> Request {
        Request::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: agg,
            filters: ReadFilters::new(from),
        })
    }

    fn exists_req(agg: AggregateKey) -> Request {
        Request::Exists(ExistsRequest {
            correlation_id: None,
            aggregate_key: agg,
        })
    }

    fn delete_req(agg: AggregateKey) -> Request {
        delete_req_full(agg, false, false, None)
    }

    fn delete_req_full(
        agg: AggregateKey,
        allow_recreate: bool,
        allow_index_continuation: bool,
        expected: Option<u64>,
    ) -> Request {
        let mut deletes = HashMap::new();
        deletes.insert(
            agg,
            SingleAggregateDelete {
                allow_recreate,
                allow_index_continuation,
                expected_event_batch_index: expected,
            },
        );
        Request::Delete(DeleteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            deletes,
        })
    }

    fn trim_req(agg: AggregateKey, keep_from: u64) -> Request {
        Request::TrimStart(TrimStartRequest {
            correlation_id: None,
            aggregate_key: agg,
            keep_from_event_batch_index: keep_from,
            client_id: 1,
            user_id: None,
        })
    }

    fn list_orgs_req() -> Request {
        Request::ListOrgs(ListOrgsRequest {
            correlation_id: None,
            shard_id: 0,
            cursor: None,
        })
    }

    fn list_types_req(org: Option<u128>) -> Request {
        Request::ListAggregateTypes(ListAggregateTypesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: org,
            cursor: None,
        })
    }

    fn list_aggs_req(org: Option<u128>, atype: Option<u128>) -> Request {
        Request::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: org,
            aggregate_type_id: atype,
            cursor: None,
        })
    }

    async fn open_shard(dir: &std::path::Path) -> ShardWal<StubReplicationClient> {
        ShardWal::open(test_config(dir), NodeStatus::Standalone, StubReplicationClient)
            .await
            .unwrap()
    }

    async fn process<R: ReplicationClient>(
        shard: &ShardWal<R>,
        lease: Option<u64>,
        req: Request,
    ) -> Result<Response, ShardError> {
        shard.process_request(lease, req).await
    }

    async fn write_ok<R: ReplicationClient>(shard: &ShardWal<R>, req: Request) {
        let result = process(shard, Some(0), req).await;
        assert!(
            matches!(result, Ok(Response::Write(_))),
            "write failed: {:?}",
            result.err()
        );
    }

    fn unwrap_read(result: Result<Response, ShardError>) -> ReadResponse {
        match result.expect("read should succeed") {
            Response::Read(r) => r,
            other => panic!("expected Read, got {other:?}"),
        }
    }

    fn unwrap_exists(result: Result<Response, ShardError>) -> ExistsResponse {
        match result.expect("exists should succeed") {
            Response::Exists(r) => r,
            other => panic!("expected Exists, got {other:?}"),
        }
    }

    fn unwrap_list_orgs(result: Result<Response, ShardError>) -> ListOrgsResponse {
        match result.expect("list_orgs should succeed") {
            Response::ListOrgs(r) => r,
            other => panic!("expected ListOrgs, got {other:?}"),
        }
    }

    fn unwrap_list_types(result: Result<Response, ShardError>) -> ListAggregateTypesResponse {
        match result.expect("list_types should succeed") {
            Response::ListAggregateTypes(r) => r,
            other => panic!("expected ListAggregateTypes, got {other:?}"),
        }
    }

    fn unwrap_list_aggs(result: Result<Response, ShardError>) -> ListAggregatesResponse {
        match result.expect("list_aggs should succeed") {
            Response::ListAggregates(r) => r,
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

            let read = unwrap_read(process(&shard, None, read_req(agg)).await);
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

            let read = unwrap_read(process(&shard, None, read_req(agg)).await);
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
                let read = unwrap_read(process(&shard, None, read_req(k.clone())).await);
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

            let result = process(&shard, None, read_req(key(1, 1, 999))).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    // ── Exists ──

    #[test]
    fn exists_missing_returns_zero() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let resp = unwrap_exists(process(&shard, None, exists_req(key(1, 1, 999))).await);
            assert_eq!(resp.min_event_batch_index, 0);

            shard.close().await;
        });
    }

    #[test]
    fn exists_after_write_returns_min_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let resp = unwrap_exists(process(&shard, None, exists_req(agg)).await);
            assert_eq!(resp.min_event_batch_index, FIRST_EVENT_BATCH_INDEX);

            shard.close().await;
        });
    }

    // ── Write validation errors ──

    #[test]
    fn write_without_lease_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, None, write_req(key(1, 1, 1), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::InvalidLeaseIndex))));

            shard.close().await;
        });
    }

    #[test]
    fn write_empty_events_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, Some(0), write_req(key(1, 1, 1), vec![])).await;
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
            let result = process(&shard, Some(0), write_req(key(1, 1, 1), evts)).await;
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
            let result = process(&shard, Some(0), req).await;
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
            let result = process(&shard, Some(0), req).await;
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
            let result = process(&shard, Some(0), req).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))
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

            let result = process(&shard, Some(0), delete_req(agg.clone())).await;
            assert!(matches!(result, Ok(Response::Delete(_))));

            let result = process(&shard, None, read_req(agg)).await;
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
            let _ = process(&shard, Some(0), delete_req(agg.clone())).await.unwrap();

            let result = process(&shard, Some(0), write_req(agg, events(1))).await;
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
            let _ = process(&shard, Some(0), del).await.unwrap();

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let read = unwrap_read(process(&shard, None, read_req(agg)).await);
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
            let _ = process(&shard, Some(0), del).await.unwrap();

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let read = unwrap_read(process(&shard, None, read_req_from(agg, 4)).await);
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

            let result = process(&shard, None, delete_req(agg.clone())).await;
            assert!(matches!(result, Err(ShardError::Delete(ShardDeleteError::InvalidLeaseIndex))));

            let empty_delete = Request::Delete(DeleteRequest {
                correlation_id: None,
                client_id: 1,
                user_id: None,
                deletes: HashMap::new(),
            });
            let result = process(&shard, Some(0), empty_delete).await;
            assert!(matches!(result, Err(ShardError::Delete(ShardDeleteError::EmptyDeleteList))));

            let result = process(&shard, Some(0), delete_req(agg.clone())).await;
            assert!(matches!(result, Err(ShardError::Delete(ShardDeleteError::AggregateNotExists))));

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let del = delete_req_full(agg, false, false, Some(999));
            let result = process(&shard, Some(0), del).await;
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

            let result = process(&shard, Some(0), trim_req(agg.clone(), 3)).await;
            assert!(matches!(result, Ok(Response::TrimStart(_))));

            let result = process(&shard, None, read_req_from(agg.clone(), 1)).await;
            assert!(matches!(
                result,
                Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))
            ));

            let read = unwrap_read(process(&shard, None, read_req_from(agg, 3)).await);
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

            let _ = process(&shard, Some(0), trim_req(agg.clone(), 2)).await.unwrap();
            let result = process(&shard, Some(0), trim_req(agg, 2)).await;
            assert!(matches!(result, Ok(Response::TrimStart(_))));

            shard.close().await;
        });
    }

    #[test]
    fn trim_validation_errors() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            let result = process(&shard, None, trim_req(agg.clone(), 1)).await;
            assert!(matches!(result, Err(ShardError::TrimStart(ShardTrimError::InvalidLeaseIndex))));

            let result = process(&shard, Some(0), trim_req(agg.clone(), 1)).await;
            assert!(matches!(result, Err(ShardError::TrimStart(ShardTrimError::AggregateNotExists))));

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let result = process(&shard, Some(0), trim_req(agg, 999)).await;
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

            let orgs = unwrap_list_orgs(process(&shard, None, list_orgs_req()).await);
            assert_eq!(orgs.orgs.len(), 3);

            let types = unwrap_list_types(process(&shard, None, list_types_req(None)).await);
            assert_eq!(types.aggregate_types.len(), 6);

            let types_filtered = unwrap_list_types(process(&shard, None, list_types_req(Some(1))).await);
            assert_eq!(types_filtered.aggregate_types.len(), 2);
            assert!(types_filtered.aggregate_types.iter().all(|t| t.org_id == 1));

            let aggs = unwrap_list_aggs(process(&shard, None, list_aggs_req(Some(1), Some(1))).await);
            assert_eq!(aggs.aggregates.len(), 1);

            shard.close().await;
        });
    }

    #[test]
    fn list_empty_shard() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let orgs = unwrap_list_orgs(process(&shard, None, list_orgs_req()).await);
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
            let _ = process(&shard, Some(0), delete_req(agg)).await.unwrap();

            let aggs = unwrap_list_aggs(process(&shard, None, list_aggs_req(Some(1), Some(1))).await);
            assert_eq!(aggs.aggregates.len(), 1);
            assert!(aggs.aggregates[0].is_deleted);

            shard.close().await;
        });
    }

    // ── Watch and CatchUp rejected ──

    #[test]
    fn watch_and_catchup_rejected() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let watch = Request::Watch(WatchRequest {
                correlation_id: None,
                requested_latency_ms: None,
                orgs: None,
                aggregate_types: None,
                aggregates: None,
                operation_types: None,
            });
            let result = process(&shard, None, watch).await;
            assert!(matches!(result, Err(ShardError::WatchRequestInvalid)));

            let catchup = Request::CatchUp(CatchUpRequest {
                correlation_id: None,
                shard_id: 0,
                last_follower_metablock: None,
                follower_tip_hash: None,
            });
            let result = process(&shard, None, catchup).await;
            assert!(matches!(result, Err(ShardError::CatchUpRequestInvalid)));

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

            let _ = process(&shard, Some(0), trim_req(agg.clone(), 3)).await.unwrap();

            let resp = unwrap_exists(process(&shard, None, exists_req(agg)).await);
            assert_eq!(resp.min_event_batch_index, 3);

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
        async fn replicate_to_follower(&mut self, _batches: Vec<celeriant_msg::request::requests::ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError> {
            let remaining = self.follower_failures_remaining.get();
            if remaining > 0 {
                self.follower_failures_remaining.set(remaining - 1);
                return Err(ReplicateToFollowerError::FollowerUnexpectedResponse);
            }
            Ok(())
        }

        async fn replicate_to_s3(&mut self, _batches: Vec<celeriant_msg::request::requests::ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            let remaining = self.s3_failures_remaining.get();
            if remaining > 0 {
                self.s3_failures_remaining.set(remaining - 1);
                return Err(ReplicateToS3Error::S3NotConfigured);
            }
            Ok(())
        }
    }

    async fn open_leader_shard(dir: &std::path::Path, client: FailThenSucceedReplicationClient) -> ShardWal<FailThenSucceedReplicationClient> {
        ShardWal::open(test_config(dir), NodeStatus::Leader { lease_index: 0 }, client)
            .await
            .unwrap()
    }

    #[test]
    fn write_succeeds_after_replication_rollback() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            let client = FailThenSucceedReplicationClient::new(1, 1);
            let shard = open_leader_shard(&dir, client).await;
            let agg = key(1, 1, 1);

            // Write 1: triggers rollback (follower offline + S3 not configured)
            let result = process(&shard, Some(0), write_req(agg.clone(), events(1))).await;
            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))),
                "expected ReplicationError, got {:?}", result
            );

            // Write 2: must succeed — stale rollback flags must not block this
            let result = process(&shard, Some(0), write_req(agg.clone(), events(1))).await;
            assert!(
                matches!(result, Ok(Response::Write(_))),
                "write after rollback should succeed, got {:?}", result
            );

            let read = unwrap_read(process(&shard, None, read_req(agg)).await);
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
            let result = process(&shard, Some(0), write_req(agg.clone(), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))));

            // Write 2: rollback again
            let result = process(&shard, Some(0), write_req(agg.clone(), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))));

            // Write 3: should succeed
            let result = process(&shard, Some(0), write_req(agg.clone(), events(1))).await;
            assert!(
                matches!(result, Ok(Response::Write(_))),
                "write after multiple rollbacks should succeed, got {:?}", result
            );

            let read = unwrap_read(process(&shard, None, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);

            shard.close().await;
        });
    }

    // ── Replication (handle_replication_batch) ──

    async fn open_follower_shard(dir: &std::path::Path) -> ShardWal<StubReplicationClient> {
        ShardWal::open(test_config(dir), NodeStatus::Follower { leader_lease_index: 0 }, StubReplicationClient)
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

    fn replication_batch_req(batches: Vec<ReplicationBatchItem>) -> Request {
        Request::ReplicationBatch(celeriant_msg::request::requests::ReplicationBatchRequest {
            correlation_id: None,
            shard_id: 0,
            leader_timestamp_ms: now_ms(),
            follower_too_far_behind: false,
            batches,
        })
    }

    fn unwrap_replication(result: Result<Response, ShardError>) -> ReplicationBatchResponse {
        match result.expect("replication should not error") {
            Response::ReplicationBatch(r) => r,
            other => panic!("expected ReplicationBatch, got {other:?}"),
        }
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
                process(&shard, None, replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
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
                process(&shard, None, replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
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

            let resp = unwrap_replication(process(&shard, None, replication_batch_req(vec![])).await);
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
                process(&shard, None, replication_batch_req(vec![replication_item(5, GENESIS_HASH)])).await,
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
                process(&shard, None, replication_batch_req(vec![replication_item(1, [0xFF; 32])])).await,
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
                process(&shard, None, replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            // Read tip_hash after batch 1 for chaining
            let tip_after_1 = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            assert_ne!(tip_after_1, GENESIS_HASH);

            // Batch 2 must chain from batch 1's tip
            let resp = unwrap_replication(
                process(&shard, None, replication_batch_req(vec![replication_item(2, tip_after_1)])).await,
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

            let stale_request = Request::ReplicationBatch(celeriant_msg::request::requests::ReplicationBatchRequest {
                correlation_id: None,
                shard_id: 0,
                leader_timestamp_ms: 1000, // ancient timestamp
                follower_too_far_behind: false,
                batches: vec![replication_item(1, GENESIS_HASH)],
            });
            let resp = unwrap_replication(process(&shard, None, stale_request).await);
            assert!(matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::TimeDriftTooHigh { .. })));

            shard.close().await;
        });
    }
}
