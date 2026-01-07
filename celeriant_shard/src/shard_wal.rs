//! Main shard write-ahead log orchestrator.
//!
//! Coordinates validation, building, caching, and durability for a single shard.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Instant;

use celeriant_disk::files::read_objects_absolute;
use celeriant_memcache::mem_snapshot_aggregate::{AggregateStatus, MemSnapshotAggregate};
use celeriant_memcache::metablock_position::MetablockPosition;
use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_memcache::sync_positions_snapshot::SyncPositionsSnapshot;
use celeriant_msg::process_requests::Request;
use celeriant_msg::process_responses::Response;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{DeleteRequest, ExistsRequest, ListAggregateTypesRequest, ListAggregatesRequest, ListOrgsRequest, ReadRequest, SingleAggregateWrite, TrimStartRequest, WriteRequest};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::{AggregateListItem, AggregateTypeListItem, ExistsResponse, ListAggregateTypesResponse, ListAggregatesResponse, ListOrgsResponse, OrgListItem, ReadResponse, SuccessResponse};
use celeriant_rotating_log::log_segment_file::LogSegmentFile;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_rotating_log::reverse_metablock_scanner::ReverseMetablockScanner;
use celeriant_rotating_log::rotating_log_error::RotatingLogError;
use celeriant_rotating_log::rwlock_timeout::write_with_timeout;
use celeriant_wal::aggregate_client_key::AggregateClientKey;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::aggregate_type_key::AggregateTypeKey;
use celeriant_wal::constants::{FIRST_EVENT_BATCH_INDEX, FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK};
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
use celeriant_watch::aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation};
use celeriant_watch::aggregate_watchers::AggregateWatchers;
use celeriant_wire::datablock_serialization::{deserialize_datablock, serialize_datablock};
use celeriant_wire::metablock_bytes;
use celeriant_wire::version_aware_wire_format::serialize_versioned_message;
use deepsize::DeepSizeOf;
use lru::LruCache;

use crate::amortisation::coordinator::Coordinator;
use crate::bloom::bloom_filter_cache::BloomFilterCache;
use crate::bloom::event_type_filter::extract_unique_event_types;
use crate::error::shard_cache_load_error::ShardCacheError;
use crate::error::shard_error::ShardError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::error::shard_read_error::ShardReadError;
use crate::error::shard_write_error::ShardWriteError;
use crate::in_memory_filtering;
use crate::internal_shard_config::InternalShardConfig;
use crate::loading_coordinator::LoadingCoordinator;

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
pub struct ShardWal {
    /// No async in shard_mem_cache and no interior mutability
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,

    /// Uses interior mutability with glommio RwLocks for async access to log files
    log_segments_cache: Rc<LogSegmentsCache>,

    /// Uses interior mutability with glommio RwLocks to select an fsync leader
    fsync_coordinator: Rc<Coordinator<ShardFsyncError>>,

    /// Registry of watchers for aggregates in this shard, uses local channel broadcasting
    pub watched_aggregates: Rc<AggregateWatchers>,

    /// Cache for bloom filter construction to avoid repeated allocations, uses interior mutability
    bloom_filter_cache: Rc<BloomFilterCache>,

    config: InternalShardConfig,

    /// Serializes concurrent aggregate snapshot loading from disk
    aggregate_loading: LoadingCoordinator<AggregateKey>,

    /// Serializes concurrent client event index loading from disk
    aggregate_client_loading: LoadingCoordinator<AggregateClientKey>,
}

impl AggregateReader for ShardWal {
    fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
        Rc::clone(&self.watched_aggregates)
    }
}

impl ShardWal {
    pub async fn process_request(&self, lease_index: Option<u64>, request: Request) -> Result<Response, ShardError> {
        match request {
            Request::Exists(exists_request) => self.exists(&exists_request).await.map(Response::Exists).map_err(ShardError::Read),
            Request::Read(read_request) => self.read(&read_request).await.map(Response::Read).map_err(ShardError::Read),
            Request::Write(write_request) => {
                let lease_index = lease_index.ok_or(ShardWriteError::InvalidLeaseIndex)?;
                self.write(lease_index, write_request)
                    .await
                    .map(Response::Write)
                    .map_err(ShardError::Write)
            }
            Request::TrimStart(trim_start_request) => {
                let lease_index = lease_index.ok_or(ShardWriteError::InvalidLeaseIndex)?;
                self.trim_start(lease_index, trim_start_request)
                    .await
                    .map(Response::TrimStart)
                    .map_err(ShardError::Write)
            }
            Request::Delete(delete_request) => {
                let lease_index = lease_index.ok_or(ShardWriteError::InvalidLeaseIndex)?;
                self.delete(lease_index, delete_request)
                    .await
                    .map(Response::Delete)
                    .map_err(ShardError::Write)
            }
            Request::Watch(_) => Err(ShardError::Read(ShardReadError::IoError(
                "Watch unprocessable via request/response model".to_string(),
            ))),
            Request::ListOrgs(list_request) => {
                self.list_orgs(list_request).await.map(Response::ListOrgs).map_err(ShardError::Read)
            }
            Request::ListAggregateTypes(list_request) => {
                self.list_aggregate_types(list_request)
                    .await
                    .map(Response::ListAggregateTypes)
                    .map_err(ShardError::Read)
            }
            Request::ListAggregates(list_request) => {
                self.list_aggregates(list_request)
                    .await
                    .map(Response::ListAggregates)
                    .map_err(ShardError::Read)
            }
        }
    }

    /// Open or create a shard WAL.
    ///
    /// If the shard directory exists with log files, reopens from the latest.
    /// Otherwise creates a new shard with an empty log file.
    pub async fn open(config: InternalShardConfig) -> Result<Self, RotatingLogError> {
        let shard_mem_cache = ShardMemCache::new(
            config.recent_write_cache_bytes,
            config.aggregate_snapshots_cache_bytes,
            config.aggregate_client_snapshots_cache_bytes,
            config.list_wal_index_cache_bytes,
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
            watched_aggregates: Rc::new(AggregateWatchers::new()),
            bloom_filter_cache: Rc::new(BloomFilterCache::new()),
            config,
            aggregate_loading: LoadingCoordinator::new(),
            aggregate_client_loading: LoadingCoordinator::new(),
        })
    }

    /// List all unique organizations that have data in this shard.
    /// 
    /// Scans WAL in reverse order, returning orgs with most recent activity first.
    /// Uses bounded LRU for deduplication within a page.
    pub async fn list_orgs(&self, request: ListOrgsRequest) -> Result<ListOrgsResponse, ShardReadError> {
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
            .scan::<bool, ShardReadError>(|log_id, pos, bytes| {
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
            .await?;

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
    pub async fn list_aggregate_types(&self, request: ListAggregateTypesRequest) -> Result<ListAggregateTypesResponse, ShardReadError> {
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
            .scan::<bool, ShardReadError>(|log_id, pos, bytes| {
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
            .await?;

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
    pub async fn list_aggregates(&self, request: ListAggregatesRequest) -> Result<ListAggregatesResponse, ShardReadError> {
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
            .scan::<bool, ShardReadError>(|log_id, pos, bytes| {
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
            .await?;

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
    async fn find_list_scan_start(&self, target_wal_index: u64) -> (u64, Option<u64>) {
        // If no cursor (starting from latest), just use active log
        if target_wal_index == u64::MAX {
            return (self.log_segments_cache.active_log_id(), None);
        }

        // Try to find cached position at or near target
        let cached = self.shard_mem_cache.borrow_mut().find_nearest_wal_index_position(target_wal_index);
        
        if let Some((cached_wal_index, pos)) = cached {
            // Only use cache if it's reasonably close (within ~1000 entries)
            // and not too far ahead of target
            if cached_wal_index <= target_wal_index && target_wal_index - cached_wal_index < 1000 {
                // Start just after the cached position to include it in scan
                return (pos.log_id, Some(pos.metablock_absolute_pos.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64)));
            }
        }

        // No useful cache hit, start from active log
        (self.log_segments_cache.active_log_id(), None)
    }
    
    pub async fn exists(&self, exists_request: &ExistsRequest) -> Result<ExistsResponse, ShardReadError> {
        let exists = self.aggregate_exists_and_cache(&exists_request.aggregate_key).await?;
        if !exists {
            return Ok(ExistsResponse {
                correlation_id: exists_request.correlation_id,
                min_event_batch_index: 0,
            });
        }

        let last_known_metablock = self
            .shard_mem_cache
            .borrow_mut()
            .get_aggregate_last_metablock_pos(&exists_request.aggregate_key);

        Ok(ExistsResponse {
            correlation_id: exists_request.correlation_id,
            min_event_batch_index: last_known_metablock.min_event_batch_index,
        })
    }

    pub async fn trim_start(&self, lease_index: u64, trim_request: TrimStartRequest) -> Result<SuccessResponse, ShardWriteError> {
        let aggregate_key = &trim_request.aggregate_key;

        // Ensure aggregate exists
        if !self.aggregate_exists_and_cache(aggregate_key).await? {
            return Err(ShardWriteError::AggregateNotExists);
        }

        let current_indexes = self.shard_mem_cache.borrow_mut().get_event_indexes(aggregate_key);

        // Validate trim index is within valid range
        if trim_request.keep_from_event_batch_index <= current_indexes.min_event_batch_index {
            // Already trimmed to this point or beyond, nothing to do
            return Ok(SuccessResponse {
                correlation_id: trim_request.correlation_id,
            });
        }

        if trim_request.keep_from_event_batch_index > current_indexes.event_batch_index {
            return Err(ShardWriteError::TrimIndexOutOfRange {
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
            datablock: DatablockStorageKind::None,
            wal_metablock_type: MetablockKind::SoftTrim(metablock_soft_trim),
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

        Ok(SuccessResponse {
            correlation_id: trim_request.correlation_id,
        })
    }
    
    pub async fn delete(&self, lease_index: u64, delete_request: DeleteRequest) -> Result<SuccessResponse, ShardWriteError> {
        // Make sure we have at least one aggregate to write
        if delete_request.deletes.is_empty() {
            return Err(ShardWriteError::EmptyEventsList);
        }

        let mut prepared_deletes = Vec::with_capacity(delete_request.deletes.len());
        for (aggregate_key, single_delete) in &delete_request.deletes {
            // Ensure aggregate snapshot is in memcache, loading from disk if necessary
            if !self.aggregate_exists_and_cache(aggregate_key).await? {
                return Err(ShardWriteError::AggregateNotExists);
            }

            let aggregate_current_indexes = self.shard_mem_cache.borrow_mut().get_event_indexes(aggregate_key);

            // Validate optimistic concurrency
            if let Some(expected) = single_delete.expected_event_batch_index {
                if expected != aggregate_current_indexes.event_batch_index {
                    return Err(ShardWriteError::OptimisticConcurrencyViolation {
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
                datablock: DatablockStorageKind::None,
                wal_metablock_type: MetablockKind::SoftDelete(metablock_soft_delete.clone()),
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
    pub async fn write(&self, lease_index: u64, write_request: WriteRequest) -> Result<SuccessResponse, ShardWriteError> {
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
            let aggregate_exists = self.aggregate_exists_and_cache(aggregate_key).await?;
            
            if !aggregate_exists {
                // Check if it was deleted with allow_recreate = false
                let (is_loaded, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(aggregate_key);
                if is_loaded && status == AggregateStatus::Deleted {
                    let indexes = self.shard_mem_cache.borrow_mut().get_event_indexes(aggregate_key);
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
                self.cache_aggregate_client(aggregate_key, &aggregate_client_key).await?;
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

        // Now we wait on disk write before ack to client
        self.sync_durable().await?;

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
        if !self.aggregate_exists_and_cache(aggregate_key).await? {
            return Err(ShardReadError::AggregateNotExists);
        }

        let last_known = self.shard_mem_cache.borrow_mut().get_aggregate_last_metablock_pos(aggregate_key);

        // 2. Validate requested range is available (not trimmed)
        if filters.from_event_batch_index < last_known.min_event_batch_index {
            return Err(ShardReadError::UnavailableBatchIndex {
                minimum_available: last_known.min_event_batch_index,
                requested: filters.from_event_batch_index,
            });
        }

        // 3. Collect metablocks with size-bounded accumulation (NO datablocks yet)
        let collection = self.collect_metablocks_bounded(aggregate_key, filters, max_bytes, last_known).await?;

        // 4. Fetch datablocks only for kept metablocks
        let batches_with_data = self.fetch_datablocks_for_metablocks(&collection.kept_metablocks).await?;

        // 5. Deserialize and apply event-level filters
        let event_batches = self.build_filtered_response(batches_with_data, filters)?;

        Ok(ReadResponse {
            correlation_id: request.correlation_id,
            event_batches,
            next_event_batch_index: collection.next_event_batch_index,
        })
    }

    /// Close the shard WAL, flushing any pending writes.
    pub async fn close(&self) -> Result<(), RotatingLogError> {
        self.log_segments_cache.close().await
    }

    /// Get the watched aggregates registry for this shard.
    pub fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
        self.watched_aggregates.clone()
    }

    async fn cache_aggregate_client(&self, aggregate_key: &AggregateKey, aggregate_client_key: &AggregateClientKey) -> Result<(), ShardWriteError> {
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
        let _ = write_with_timeout(&aggregate_lock, "cache_aggregate_client").await?;

        // We have exclusive access now, check if another concurrent task has already done the work
        if let (true, _exists) = self
            .shard_mem_cache
            .borrow_mut()
            .aggregate_client_load_status(aggregate_key, aggregate_client_key)
        {
            return Ok(());
        }

        let last_known_metablock = self.shard_mem_cache.borrow_mut().get_aggregate_last_metablock_pos(aggregate_key);

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
            .await?;

        let found = find_result.unwrap_or(false);
        if !found {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            shard_mem_cache.put_aggregate_into_cache_as_not_found(aggregate_key.clone());
        }

        Ok(())
    }

    async fn aggregate_exists_and_cache(&self, searching_for_aggregate_key: &AggregateKey) -> Result<bool, ShardCacheError> {
        // If we are cached already
        if let (true, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key) {
            return Ok(status == AggregateStatus::Found);
        }

        // Take an exclusive lock on this aggregate
        let aggregate_lock = self.aggregate_loading.acquire(searching_for_aggregate_key);
        let _ = write_with_timeout(&aggregate_lock, "move_aggregate_to_memcache").await?;

        // We have exclusive access now, check if another concurrent task has already done the work
        if let (true, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key) {
            return Ok(status == AggregateStatus::Found);
        }

        // Track if we've seen a more recent trim
        let mut seen_trim_min: Option<u64> = None;

        // Begin the search from the active log, moving backwards
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            self.log_segments_cache.active_log_id(),
            None,
            self.config.read_max_chunk_size,
        )
        .with_bloom_filter(searching_for_aggregate_key);

        let find_result = scanner
            .scan::<bool, ()>(|log_id, metablock_absolute_pos, metablock_bytes| {
                // Check for soft delete first - if this aggregate was deleted, we're done
                if metablock_bytes::is_soft_delete_for_aggregate(metablock_bytes, searching_for_aggregate_key) {
                    // Deserialize to get the deletion options
                    let (metablock, _version) = celeriant_wire::version_aware_wire_format::deserialize_versioned_metablock(metablock_bytes)
                        .map_err(|_| ())?;
                    
                    if let MetablockKind::SoftDelete(soft_delete) = metablock.wal_metablock_type {
                        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
                        shard_mem_cache.put_aggregate_into_cache_as_deleted(
                            searching_for_aggregate_key.clone(),
                            soft_delete.event_index,
                            soft_delete.event_batch_index,
                            soft_delete.allow_recreate,
                            soft_delete.allow_index_continuation,
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
                if low_priority && shard_mem_cache.is_aggregate_snapshot_full_or_contains(&current_aggregate_key) {
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

                shard_mem_cache.put_aggregate_into_cache(current_aggregate_key, snapshot, client_id, last_client_event_index, low_priority);

                if low_priority {
                    Ok(None) //Haven't found aggregate yet
                } else {
                    Ok(Some(true)) //Done searching
                }
            })
            .await?;

        let found = find_result.unwrap_or(false);
        if find_result.is_none() {
            // Never found any metablock for this aggregate
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            shard_mem_cache.put_aggregate_into_cache_as_not_found(searching_for_aggregate_key.clone());
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

        let aggregate_current_indexes = shard_mem_cache.get_event_indexes(aggregate_key);

        // There is a soft delete entry in the queue that hasn't been committed yet
        if aggregate_current_indexes.pending_delete {
            if !aggregate_current_indexes.allow_recreate {
                return Err(ShardWriteError::AggregateRecreateNotAllowed);
            }
            // Pending delete with allow_recreate - fall through to create logic
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
        let is_recreate = aggregate_current_indexes.pending_delete 
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
        let serialized_datablock = serialize_datablock(&datablock, write_request.compression_type, 0)?;

        let server_timestamp = self.config.timestamp_config.now();

        let metablock = Metablock {
            wal_index: 0,
            server_timestamp,
            lease_index,
            node_id: self.config.node_id,
            uncompressed_size: serialized_datablock.uncompressed_size,
            compressed_size: serialized_datablock.compressed_size,
            datablock: serialized_datablock.storage_kind,
            wal_metablock_type: MetablockKind::EventBatchMetadata(metablock_event_batch),
        };

        let shard_log_queue_item = ShardLogQueueItem::new(Some(datablock), serialized_datablock.external_data, metablock);

        Ok(PreparedWrite {
            aggregate_key: aggregate_key.clone(),
            client_id,
            event_index,
            event_batch_index,
            latest_client_event_index,
            shard_log_queue_item,
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
                prepared.client_id,
                prepared.latest_client_event_index,
                prepared.shard_log_queue_item,
            );
        }
    }

    /// Perform sync with potential delay for amortisation.
    async fn sync_durable(&self) -> Result<(), ShardFsyncError> {
        let rotating_log_cache = self.log_segments_cache.clone();
        let shard_mem_cache = self.shard_mem_cache.clone();
        let watched_aggregates = self.watched_aggregates.clone();

        if rotating_log_cache.force_immediate.get() {
            self.fsync_coordinator
                .request_sync(None, move || sync_with_rollback(rotating_log_cache, shard_mem_cache, watched_aggregates))
                .await
        } else if !self.config.non_durable_writes {
            self.fsync_coordinator
                .request_sync(Some(self.config.fsync_delay), move || {
                    sync_with_rollback(rotating_log_cache, shard_mem_cache, watched_aggregates)
                })
                .await
        } else {
            let fsync_coordinator = self.fsync_coordinator.clone();
            glommio::spawn_local(async move {
                let _ = fsync_coordinator
                    .request_sync(None, move || sync_with_rollback(rotating_log_cache, shard_mem_cache, watched_aggregates))
                    .await;
            })
            .detach();
            Ok(())
        }
    }
}

/// Extends the write batch index bounds in an AggregateWatchEventOperation when writing mulitple batches for the same aggregate.
fn add_write_event(write_events: &mut HashMap<AggregateKey, AggregateWatchEventOperation>, metablock_event_batch: &MetablockEventBatch) {
    write_events
        .entry(metablock_event_batch.aggregate_key.clone())
        .and_modify(|event| {
            if let AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            } = event
            {
                if metablock_event_batch.event_batch_index < *from_event_batch_index {
                    *from_event_batch_index = metablock_event_batch.event_batch_index;
                }
                if metablock_event_batch.event_batch_index > *to_event_batch_index {
                    *to_event_batch_index = metablock_event_batch.event_batch_index;
                }
            }
        })
        .or_insert(AggregateWatchEventOperation::Write {
            from_event_batch_index: metablock_event_batch.event_batch_index,
            to_event_batch_index: metablock_event_batch.event_batch_index,
        });
}

/// Records a create event for an aggregate.
fn add_create_event(create_events: &mut HashMap<AggregateKey, AggregateWatchEventOperation>, aggregate_key: AggregateKey) {
    create_events.entry(aggregate_key).or_insert(AggregateWatchEventOperation::Create {});
}

async fn sync_with_rollback(
    log_segments_cache: Rc<LogSegmentsCache>,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
) -> Result<(), ShardFsyncError> {
    let (required_disk_space, mut sync_positions_snapshot) = {
        let mut shard_mem_cache = shard_mem_cache.borrow_mut();

        if shard_mem_cache.pending_append_queue_is_empty() {
            return Ok(());
        }

        let required_disk_space = shard_mem_cache
            .buffer_size_datablocks()
            .saturating_add(shard_mem_cache.buffer_size_metablocks());
        let sync_positions_snapshot = shard_mem_cache.take_sync_positions_snapshot();

        (required_disk_space, sync_positions_snapshot)
    };

    //TODO: Under rare scenarios (eg. embedded and small log size) this is not enough
    //It should really be partitioning the pending queue into chunks < log size and looping
    log_segments_cache.rotate_to_next_log(required_disk_space).await?;
    let active_log_segment = log_segments_cache.active();

    match sync(active_log_segment, &mut sync_positions_snapshot).await {
        Ok(_) => {
            let mut shard_mem_cache = shard_mem_cache.borrow_mut();

            // Commit moves the carry over bytes back into memcache, so take queue first
            let pending_append_queue = std::mem::take(&mut sync_positions_snapshot.pending_append_queue);
            shard_mem_cache.commit_sync_positions_snapshot(sync_positions_snapshot);

            // Transform the queue into recent writes and broadcast events
            let mut write_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
            let mut create_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
            let mut delete_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
            let mut trim_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();

            for queue_item in pending_append_queue {
                match &queue_item.metablock.wal_metablock_type {
                    MetablockKind::EventBatchMetadata(event_batch_metadata) => {
                        // Build watched_aggregates events
                        add_write_event(&mut write_events, event_batch_metadata);

                        // If first event batch for aggregate, add create event
                        if event_batch_metadata.event_batch_index == FIRST_EVENT_BATCH_INDEX {
                            add_create_event(&mut create_events, event_batch_metadata.aggregate_key.clone());
                        }

                        let size_bytes = ((queue_item.metablock.deep_size_of() + queue_item.datablock.deep_size_of()) * 3) as u64;

                        // Cache the write (only happens after durable write confirmed)
                        shard_mem_cache.cache_recent_write(
                            event_batch_metadata.aggregate_key.clone(),
                            event_batch_metadata.event_batch_index,
                            queue_item.metablock,
                            queue_item.datablock,
                            size_bytes,
                        );
                    }
                    MetablockKind::SoftTrim(soft_trim) => {
                        // Update aggregate min_event_batch_index in cache
                        shard_mem_cache.update_aggregate_min_event_batch_index(
                            &soft_trim.aggregate_key,
                            soft_trim.keep_from_event_batch_index,
                        );
                        
                        // Track trim event for watchers
                        trim_events.entry(soft_trim.aggregate_key.clone())
                            .or_insert(AggregateWatchEventOperation::TrimStart {
                                keep_from_event_batch_index: soft_trim.keep_from_event_batch_index,
                            });
                    }
                    MetablockKind::SoftDelete(soft_delete) => {
                        // Mark aggregate as deleted in cache
                        shard_mem_cache.put_aggregate_into_cache_as_deleted(
                            soft_delete.aggregate_key.clone(),
                            soft_delete.event_index,
                            soft_delete.event_batch_index,
                            soft_delete.allow_recreate,
                            soft_delete.allow_index_continuation,
                        );
                        
                        // Track delete event for watchers
                        delete_events.entry(soft_delete.aggregate_key.clone())
                            .or_insert(AggregateWatchEventOperation::Delete {});
                    }
                    _ => {}
                }
            }

            for (aggregate_key, operation) in create_events {
                watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
            }

            for (aggregate_key, operation) in write_events {
                watched_aggregates.broadcast(AggregateWatchEvent {
                    aggregate_key: aggregate_key.clone(),
                    operation,
                });
            }

            for (aggregate_key, operation) in delete_events {
                watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
            }

            for (aggregate_key, operation) in trim_events {
                watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
            }

            return Ok(());
        }
        Err(e) => {
            let mut shard_mem_cache = shard_mem_cache.borrow_mut();
            shard_mem_cache.rollback_queue_positions();
            log_segments_cache.force_immediate.set(true);
            return Err(e);
        }
    }
}

async fn sync(log_segment_file: Rc<LogSegmentFile>, sync_positions_snapshot: &mut SyncPositionsSnapshot) -> Result<(), ShardFsyncError> {
    let mut log_segment_file_metadata = log_segment_file.metadata.borrow().clone();

    let dma_file_writer = log_segment_file.lock_writer("sync").await?;
    let dma_file_writer = dma_file_writer
        .as_ref()
        .ok_or_else(|| RotatingLogError::IoError("No file handle".to_string()))?;

    // Write datablocks first so we can get the positions to include into metablocks
    let buffer_size_datablocks: u64 = sync_positions_snapshot.buffer_size_datablocks();

    let mut datablocks_absolute_write_positions: Vec<u64> = Vec::with_capacity(sync_positions_snapshot.pending_append_queue.len());
    let mut new_datablocks_position = log_segment_file_metadata.datablocks_position;
    let mut datablocks_carry_over: Option<Vec<u8>> = log_segment_file_metadata.datablocks_carry_over.take();

    if buffer_size_datablocks > 0 {
        let write_to_pos = dma_file_writer.align_up(log_segment_file_metadata.datablocks_position);
        new_datablocks_position = log_segment_file_metadata.datablocks_position.saturating_sub(buffer_size_datablocks);
        let write_from_pos = dma_file_writer.align_down(new_datablocks_position);
        let aligned_buffer_size_datablocks = write_to_pos.saturating_sub(write_from_pos);

        let front_carry_over = new_datablocks_position.saturating_sub(write_from_pos) as usize;
        let end_carry_over = write_to_pos.saturating_sub(log_segment_file_metadata.datablocks_position) as usize;

        let mut buffer_datablocks = dma_file_writer.alloc_dma_buffer(front_carry_over + buffer_size_datablocks as usize + end_carry_over);
        let buffer_datablocks_slice = buffer_datablocks.as_bytes_mut();

        buffer_datablocks_slice.fill(0);

        if end_carry_over > 0 {
            if datablocks_carry_over.is_none() || datablocks_carry_over.as_ref().unwrap().len() != end_carry_over as usize {
                return Err(ShardFsyncError::DatablocksCarryOverBufferNotPresent);
            }
            buffer_datablocks_slice[(aligned_buffer_size_datablocks.saturating_sub(end_carry_over as u64)) as usize..]
                .copy_from_slice(&datablocks_carry_over.as_ref().unwrap());
        }

        let mut position = 0usize;
        for item in &sync_positions_snapshot.pending_append_queue {
            if let Some(datablock_bytes) = &item.datablock_bytes {
                let len = datablock_bytes.len();
                let start_idx = front_carry_over + position;
                let end_idx = front_carry_over + position + len;

                datablocks_absolute_write_positions.push(new_datablocks_position + position as u64);
                buffer_datablocks_slice[start_idx..end_idx].copy_from_slice(datablock_bytes);
                position += len;
            }
        }

        let datablocks_carry_over_size = dma_file_writer.align_up(new_datablocks_position).saturating_sub(new_datablocks_position);
        if datablocks_carry_over_size > 0 {
            datablocks_carry_over =
                Some(buffer_datablocks_slice[front_carry_over..(front_carry_over + datablocks_carry_over_size as usize)].to_vec());
        }

        dma_file_writer
            .write_at(buffer_datablocks, new_datablocks_position.saturating_sub(front_carry_over as u64))
            .await?;
    }

    let buffer_size_metablocks: u64 = sync_positions_snapshot.buffer_size_metablocks();
    let mut buffer_metablocks = dma_file_writer.alloc_dma_buffer(buffer_size_metablocks as usize);
    let buffer_metablocks_slice = buffer_metablocks.as_bytes_mut();
    let mut position = 0usize;
    let mut index = 0;
    for item in &mut sync_positions_snapshot.pending_append_queue {
        if item.datablock_bytes.is_some() && item.datablock.is_some() {
            match &mut item.metablock.datablock {
                DatablockStorageKind::Block(datablock_block_ref) => {
                    datablock_block_ref.datablock_position = datablocks_absolute_write_positions[index];
                }
                _ => {}
            }
            index += 1;
        }

        log_segment_file_metadata.wal_index = log_segment_file_metadata.wal_index.saturating_add(1);
        item.metablock.wal_index = log_segment_file_metadata.wal_index;

        // Track the absolute position where this metablock is written
        let metablock_absolute_pos = log_segment_file_metadata.metablocks_position + position as u64;

        // Update aggregate positions tracking for event batches (only if entry exists)
        if let MetablockKind::EventBatchMetadata(event_batch) = &item.metablock.wal_metablock_type {
            if let Some(aggregate_positions) = sync_positions_snapshot.aggregate_queue_positions.get_mut(&event_batch.aggregate_key) {
                aggregate_positions.log_id = log_segment_file_metadata.log_id;
                aggregate_positions.metablock_absolute_pos = metablock_absolute_pos;
            }
        }

        let mut metablock_bytes = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&item.metablock, WIRE_VERSION_WAL_METABLOCK, &mut metablock_bytes)?;

        //let metablock_bytes: [u8; FIXED_BLOCK_SIZE_BYTES]
        buffer_metablocks_slice[position..position + FIXED_BLOCK_SIZE_BYTES].copy_from_slice(&metablock_bytes);
        position += FIXED_BLOCK_SIZE_BYTES;
    }

    //Write metablocks
    let new_metablocks_position = log_segment_file_metadata.metablocks_position + buffer_metablocks.len() as u64;
    dma_file_writer
        .write_at(buffer_metablocks, log_segment_file_metadata.metablocks_position)
        .await?;

    // Update bloom filter with aggregate keys from this batch
    for item in &sync_positions_snapshot.pending_append_queue {
        if let MetablockKind::EventBatchMetadata(event_batch) = &item.metablock.wal_metablock_type {
            log_segment_file_metadata.aggregate_key_bloom.insert(&event_batch.aggregate_key);
        }
    }

    // Update positions and carry over
    log_segment_file_metadata.metablocks_position = new_metablocks_position;
    log_segment_file_metadata.datablocks_carry_over = datablocks_carry_over;
    log_segment_file_metadata.datablocks_position = new_datablocks_position;

    // Write header
    let header_end_start_pos = log_segment_file_metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
    let header = log_segment_file_metadata.to_shard_log_header();
    celeriant_rotating_log::log_segment_file::write_dual_shard_log_header(&dma_file_writer, header_end_start_pos, &header).await?;

    dma_file_writer.fdatasync().await?;

    let mut updated_log_segment_file_metadata = log_segment_file.metadata.borrow_mut();
    *updated_log_segment_file_metadata = log_segment_file_metadata;

    Ok(())
}

/// Intermediate struct for validated and prepared write data
struct PreparedWrite {
    aggregate_key: AggregateKey,
    client_id: u128,
    event_index: u64,
    event_batch_index: u64,
    latest_client_event_index: u64,
    shard_log_queue_item: ShardLogQueueItem,
}

/// Metablock kept after size-bounded collection
struct KeptMetablock {
    log_id: u64,
    metablock: Metablock,
    /// If from recent write cache, we already have the datablock
    datablock: Option<Datablock>,
    /// Cached size from DatablockStorageKind for running total
    uncompressed_size: u64,
}

/// Result of size-bounded metablock collection
struct MetablockCollection {
    /// Metablocks that fit within max_bytes, sorted by batch index ascending
    kept_metablocks: Vec<KeptMetablock>,
    /// If we hit the size limit, this is the next batch index to continue from
    next_event_batch_index: Option<u64>,
}

impl ShardWal {
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
    ) -> Result<MetablockCollection, ShardReadError> {
        let mut kept: Vec<KeptMetablock> = Vec::new();
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

        // Sort by batch index ascending (disk results were added in reverse)
        kept.sort_by_key(|k| Self::get_batch_index(&k.metablock));

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
        kept: &mut Vec<KeptMetablock>,
        cumulative_size: &mut u64,
        evicted_batch_index: &mut Option<u64>,
    ) {
        let shard_mem_cache = self.shard_mem_cache.borrow();

        // Cache iterates forward (ascending batch index) from from_event_batch_index
        for (batch_idx, write) in shard_mem_cache.get_cached_writes_from(aggregate_key, filters.from_event_batch_index) {
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
            kept.push(KeptMetablock {
                log_id: 0, // Cache entries don't need log_id for fetch
                metablock: write.metablock.clone(),
                uncompressed_size: batch_size,
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
        kept: &mut Vec<KeptMetablock>,
        cumulative_size: &mut u64,
        evicted_batch_index: &mut Option<u64>,
    ) -> Result<(), ShardReadError> {
        // Use a VecDeque for efficient eviction from the "newest" end
        let mut disk_kept: VecDeque<KeptMetablock> = VecDeque::new();
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
            .scan::<(), ShardReadError>(|log_id, _pos, bytes| {
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
                let (metablock, _version) = celeriant_wire::version_aware_wire_format::deserialize_versioned_metablock(bytes)?;

                if !in_memory_filtering::is_include_batch(&metablock, filters) {
                    return Ok(None); // Continue - filtered out
                }

                let batch_size = metablock.uncompressed_size;

                // Add to back (we're scanning backwards, newest seen first)
                disk_cumulative += batch_size;
                disk_kept.push_back(KeptMetablock {
                    log_id,
                    metablock,
                    uncompressed_size: batch_size,
                    datablock: None,
                });

                // Evict from FRONT (newest) until under budget
                while disk_cumulative > budget_for_disk && disk_kept.len() > 1 {
                    if let Some(evicted) = disk_kept.pop_front() {
                        disk_cumulative -= evicted.uncompressed_size;
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
        kept.extend(disk_kept.into_iter());

        Ok(())
    }

    async fn fetch_datablocks_for_metablocks(
        &self,
        kept_metablocks: &[KeptMetablock],
    ) -> Result<Vec<(Metablock, Option<Datablock>)>, ShardReadError> {
        let mut results: Vec<(Metablock, Option<Datablock>)> = Vec::with_capacity(kept_metablocks.len());
        let mut disk_fetches: Vec<(usize, u64, &Metablock)> = Vec::new();

        for (idx, kept) in kept_metablocks.iter().enumerate() {
            // If we have datablock from cache, use it directly
            if kept.datablock.is_some() {
                results.push((kept.metablock.clone(), kept.datablock.clone()));
                continue;
            }

            match &kept.metablock.datablock {
                DatablockStorageKind::None => {
                    results.push((kept.metablock.clone(), None));
                }
                DatablockStorageKind::Inline(_) => {
                    // Can deserialize immediately - no disk I/O needed
                    let datablock = deserialize_datablock(kept.metablock.uncompressed_size, &kept.metablock.datablock, None)?;
                    results.push((kept.metablock.clone(), Some(datablock)));
                }
                DatablockStorageKind::Block(_) => {
                    // Need to fetch from disk
                    results.push((kept.metablock.clone(), None)); // Placeholder
                    disk_fetches.push((idx, kept.log_id, &kept.metablock));
                }
            }
        }

        if disk_fetches.is_empty() {
            return Ok(results);
        }

        // Group fetches by log_id for batch I/O
        let mut by_log: HashMap<u64, Vec<(usize, &Metablock)>> = HashMap::new();
        for (idx, log_id, meta) in disk_fetches {
            by_log.entry(log_id).or_default().push((idx, meta));
        }

        // Batch fetch per log file
        for (log_id, log_fetches) in by_log {
            let positions: Vec<(usize, read_objects_absolute::AbsoluteObjectPosition)> = log_fetches
                .iter()
                .filter_map(|(idx, meta)| {
                    if let DatablockStorageKind::Block(r) = &meta.datablock {
                        Some((
                            *idx,
                            read_objects_absolute::AbsoluteObjectPosition {
                                start_pos: r.datablock_position,
                                end_pos: r.datablock_position + meta.compressed_size,
                            },
                        ))
                    } else {
                        None
                    }
                })
                .collect();

            if positions.is_empty() {
                continue;
            }

            // Sort positions AND indices together by start_pos
            let mut indexed_positions = positions;
            indexed_positions.sort_by_key(|(_, p)| p.start_pos);

            let indices: Vec<usize> = indexed_positions.iter().map(|(i, _)| *i).collect();
            let pos_only: Vec<read_objects_absolute::AbsoluteObjectPosition> = indexed_positions.into_iter().map(|(_, p)| p).collect();

            let blobs = {
                let log_segment_file = self.log_segments_cache.get(log_id).await?;
                let file_len = log_segment_file.metadata.borrow().file_len;
                let guard = log_segment_file.lock_reader("fetch_datablocks").await?;
                let dma = guard
                    .as_ref()
                    .ok_or_else(|| ShardReadError::IoError(format!("No file handle for log {}", log_id)))?;

                read_objects_absolute::read_objects_absolute(dma, file_len, &pos_only, self.config.read_max_chunk_size).await?
            };

            // Deserialize and update results
            for (result_idx, blob) in indices.into_iter().zip(blobs) {
                let metablock = &results[result_idx].0;
                let datablock = deserialize_datablock(metablock.uncompressed_size, &metablock.datablock, Some(&blob))?;
                results[result_idx].1 = Some(datablock);
            }
        }

        Ok(results)
    }

    fn build_filtered_response(
        &self,
        batches_with_data: Vec<(Metablock, Option<Datablock>)>,
        filters: &ReadFilters,
    ) -> Result<Vec<AggregateEventBatch>, ShardReadError> {
        let mut event_batches = Vec::with_capacity(batches_with_data.len());

        for (metablock, datablock) in batches_with_data {
            let Some(mut datablock) = datablock else {
                continue;
            };

            // Apply event-level filters (bloom filter may have false positives)
            if let DatablockKind::EventBatchItem(ref mut eb) = datablock.datablock_kind {
                in_memory_filtering::apply_event_filters(eb, filters);
                if eb.events.is_empty() {
                    continue;
                }
            }

            let Some(batch) = AggregateEventBatch::from_wal(&metablock, &datablock) else {
                continue;
            };

            event_batches.push(batch);
        }

        Ok(event_batches)
    }
}
