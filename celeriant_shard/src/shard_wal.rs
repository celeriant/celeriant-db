//! Main shard write-ahead log orchestrator.
//!
//! Coordinates validation, building, caching, and durability for a single shard.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use celeriant_memcache::internal_shard_config::InternalShardConfig;
use celeriant_memcache::mem_snapshot_aggregate::MemSnapshotAggregate;
use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_memcache::sync_positions_snapshot::SyncPositionsSnapshot;
use celeriant_msg::process_requests::Request;
use celeriant_msg::process_responses::Response;
use celeriant_msg::request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::{ReadResponse, SuccessResponse};
use celeriant_rotating_log::reverse_metablock_scanner::ReverseMetablockScanner;
use celeriant_rotating_log::rotating_log_cache::RotatingLogCache;
use celeriant_rotating_log::rotating_log_error::RotatingLogError;
use celeriant_rotating_log::rwlock_timeout::{read_with_timeout, write_with_timeout};
use celeriant_rotating_log::shard_log_dma_file::ShardLogDmaFile;
use celeriant_wal::aggregate_client_key::{self, AggregateClientKey};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::{FIRST_EVENT_BATCH_INDEX, FIXED_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK};
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use celeriant_wal::datablocks::datablock_kind::DatablockKind;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_event_batch::{EventTypesKind, MetablockEventBatch};
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_watch::aggregate_reader::AggregateReader;
use celeriant_watch::aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation};
use celeriant_watch::aggregate_watchers::AggregateWatchers;
use celeriant_wire::datablock_serialization::serialize_datablock;
use celeriant_wire::metablock_bytes;
use celeriant_wire::version_aware_wire_format::serialize_versioned_message;
use deepsize::DeepSizeOf;
use glommio::sync::RwLock;

use crate::amortisation::coordinator::Coordinator;
use crate::bloom::bloom_filter_cache::BloomFilterCache;
use crate::bloom::event_type_filter::extract_unique_event_types;
use crate::error::shard_error::ShardError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::error::shard_read_error::ShardReadError;
use crate::error::shard_write_error::ShardWriteError;
use crate::in_memory_filtering::{apply_event_filters, trim_end_if_exceeds_max_bytes};
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
    rotating_log_cache: Rc<RotatingLogCache>,

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
    pub async fn process_request(
        &self,
        lease_index: Option<u64>,
        request: Request,
    ) -> Result<Response, ShardError> {
        match request {
            Request::Exists(_exists_request) => Err(ShardError::Read(ShardReadError::NotExists)),
            Request::Read(read_request) => {
                self.read(&read_request)
                    .await
                    .map(Response::Read)
                    .map_err(ShardError::Read)
            },
            Request::Write(write_request) => {
                let lease_index = lease_index.ok_or(ShardWriteError::InvalidLeaseIndex)?;
                self.write(lease_index, write_request)
                    .await
                    .map(Response::Write)
                    .map_err(ShardError::Write)
            }
            Request::TrimStart(_trim_start_request) => {
                Err(ShardError::Read(ShardReadError::NotExists))
            }
            Request::Delete(_delete_request) => Err(ShardError::Read(ShardReadError::NotExists)),
            Request::Watch(_) => Err(ShardError::Read(ShardReadError::IoError(
                "Watch unprocessable via request/response model".to_string(),
            ))),
        }
    }

    /// Open or create a shard WAL.
    ///
    /// If the shard directory exists with log files, reopens from the latest.
    /// Otherwise creates a new shard with an empty log file.
    pub async fn open(config: InternalShardConfig) -> Result<Self, RotatingLogError> {
        let rotating_log_cache = RotatingLogCache::new(
            config.shard_dir.clone(),
            config.shard_log_preallocate_bytes,
            config.max_open_files as usize,
        )
        .await?;

        let shard_mem_cache = {
            let active_lock = rotating_log_cache.active();
            let active_log_file = read_with_timeout(&active_lock, "shard_wal_open_active").await?;
            let datablocks_carry_over = active_log_file.read_datablocks_carry_over_bytes().await?;

            ShardMemCache::new(
                active_log_file.file_len,
                active_log_file.shard_log_header.metablocks_position,
                active_log_file.shard_log_header.datablocks_position,
                active_log_file.shard_log_header.wal_index,
                datablocks_carry_over,
                config.clone(),
                active_log_file.log_id,
            )
        };

        Ok(Self {
            shard_mem_cache: Rc::new(RefCell::new(shard_mem_cache)),
            rotating_log_cache: Rc::new(rotating_log_cache),
            fsync_coordinator: Rc::new(Coordinator::new()),
            watched_aggregates: Rc::new(AggregateWatchers::new()),
            bloom_filter_cache: Rc::new(BloomFilterCache::new()),
            config,
            aggregate_loading: LoadingCoordinator::new(),
            aggregate_client_loading: LoadingCoordinator::new(),
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
    pub async fn write(
        &self,
        lease_index: u64,
        write_request: WriteRequest,
    ) -> Result<SuccessResponse, ShardWriteError> {
        // Make sure we have at least one aggregate to write
        if write_request.writes.is_empty() {
            return Err(ShardWriteError::EmptyEventsList);
        }

        // Phase 1: Validation and preparation - all checks that can fail happen here
        // No mutations to shard_mem_cache until all validations pass
        let mut prepared_writes = Vec::with_capacity(write_request.writes.len());
        let mut wal_index = self.shard_mem_cache.borrow_mut().get_wal_index();

        for (aggregate_key, single_write) in &write_request.writes {
            // Validate events list is not empty
            if single_write.events.is_empty() {
                return Err(ShardWriteError::EmptyEventsList);
            }

            // Validate that no event uses the sentinel 0 event type
            if let Some(ev) = single_write
                .events
                .iter()
                .find(|e| e.event_type_major == 0)
            {
                return Err(ShardWriteError::ZeroEventType {
                    client_event_index: ev.client_event_index,
                });
            }

            // Ensure aggregate snapshot is in memcache, loading from disk if necessary
            if !self.aggregate_exists_and_cache(aggregate_key).await?
                && !single_write.allow_create
            {
                return Err(ShardWriteError::AggregateNotExists);
            }

            let aggregate_client_key = AggregateClientKey::new(aggregate_key.clone(), write_request.client_id);

            // Prep work done outside of validate_and_prepare_write as it's async
            if single_write.enforce_client_idempotency {
                self.cache_aggregate_client(aggregate_key, &aggregate_client_key).await?;
            }

            wal_index = wal_index.saturating_add(1);

            // Validate and prepare - reads from memcache but does not mutate
            let prepared = self.validate_and_prepare_write(
                lease_index,
                aggregate_key,
                write_request.client_id,
                write_request.user_id,
                single_write.clone(),
                wal_index,
            )?;
            
            prepared_writes.push(prepared);
        }

        // Phase 2: Append all prepared writes to queue - cannot fail
        let force_durable = self.append_prepared_writes_to_queue(prepared_writes, wal_index);

        // Now we wait on disk write before ack to client
        self.sync_durable(force_durable).await?;

        Ok(SuccessResponse { correlation_id: write_request.correlation_id })
    }

    /// Read event batches from an aggregate.
    /// Reads from cache when possible, falls back to disk for older batches.
    pub async fn read(&self, request: &ReadRequest) -> Result<ReadResponse, ShardReadError> {
        let shard_mem_cache = self.shard_mem_cache.borrow();

        // Currently read is only served from the recent write cache.
        // We always prioritise the recent write cache, but if the read bounds
        // can't be served due to missing batches, we need to pull them from disk
        // and join it to the already cached data; then return what the client wants
        // If the recent write cache is not *full* we can opportunistically add the 
        // data that we got from disk to the recent write cache for that aggregate;
        // but only if the data is contiguous (creates no event batch index gaps!)
        // and we can't just add *all* the read batches, only add in reverse until cache is full

        // Check if aggregate exists
        if !shard_mem_cache.aggregate_snapshot_in_cache(&request.aggregate_key) {
            return Err(ShardReadError::NotExists);
        }

        // Get cached writes starting from the requested batch index
        let cached_writes = match shard_mem_cache
            .get_cached_writes_from(&request.aggregate_key, request.filters.from_event_batch_index)
        {
            Some(writes) => writes,
            None => {
                return Err(ShardReadError::UnavailableBatchIndex {
                    requested_event_batch_index: request.filters.from_event_batch_index,
                    minimum_available_event_batch_index: u64::MAX,
                });
            }
        };

        // Collect writes into a vec for processing
        let writes: Vec<_> = cached_writes.collect();

        if writes.is_empty() {
            return Err(ShardReadError::UnavailableBatchIndex {
                requested_event_batch_index: request.filters.from_event_batch_index,
                minimum_available_event_batch_index: u64::MAX,
            });
        }

        // Check if the first available batch matches what was requested (detect gaps)
        let first_batch_index = match &writes[0].1.metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(m) => m.event_batch_index,
            _ => {
                return Err(ShardReadError::UnavailableBatchIndex {
                    requested_event_batch_index: request.filters.from_event_batch_index,
                    minimum_available_event_batch_index: u64::MAX,
                })
            }
        };

        if first_batch_index > request.filters.from_event_batch_index {
            return Err(ShardReadError::UnavailableBatchIndex {
                requested_event_batch_index: request.filters.from_event_batch_index,
                minimum_available_event_batch_index: first_batch_index,
            });
        }

        // Extract metablocks for filtering
        let mut metablocks: Vec<Metablock> = writes
            .iter()
            .map(|(_, write)| write.metablock.clone())
            .collect();

        // Apply batch-level filters and pagination based on max_response_size
        let next_event_batch_index = trim_end_if_exceeds_max_bytes(
            &mut metablocks,
            &request.filters,
            Some(self.config.max_response_size as usize),
        )?;

        // Build set of batch indexes that passed filtering
        let kept_batch_indexes: std::collections::HashSet<u64> = metablocks
            .iter()
            .filter_map(|m| match &m.wal_metablock_type {
                MetablockKind::EventBatchMetadata(meta) => Some(meta.event_batch_index),
                _ => None,
            })
            .collect();

        // Extract and filter event batches
        let mut event_batches: Vec<AggregateEventBatch> = Vec::new();

        for (_, write) in &writes {
            let (event_batch_index, client_id, user_id) = match &write.metablock.wal_metablock_type {
                MetablockKind::EventBatchMetadata(m) => (m.event_batch_index, m.client_id, m.user_id),
                _ => continue,
            };

            if !kept_batch_indexes.contains(&event_batch_index) {
                continue;
            }

            if let Some(datablock) = &write.datablock {
                if let DatablockKind::EventBatchItem(batch) = &datablock.datablock_kind {
                    let mut batch_clone = batch.clone();
                    apply_event_filters(&mut batch_clone, &request.filters);
                    if !batch_clone.events.is_empty() {
                        event_batches.push(AggregateEventBatch {
                            event_batch_index,
                            client_id,
                            user_id,
                            server_timestamp: write.metablock.server_timestamp,
                            events: batch_clone.events,
                        });
                    }
                }
            }
        }

        Ok(ReadResponse {
            correlation_id: request.correlation_id,
            event_batches,
            next_event_batch_index,
        })
    }

    /// Close the shard WAL, flushing any pending writes.
    pub async fn close(&self) -> Result<(), RotatingLogError> {
        self.rotating_log_cache.close().await
    }

    /// Get the watched aggregates registry for this shard.
    pub fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
        self.watched_aggregates.clone()
    }

    async fn cache_aggregate_client(&self, aggregate_key: &AggregateKey, aggregate_client_key: &AggregateClientKey) -> Result<(), ShardWriteError> {

        // If we are cached already
        if let (true, _exists) = self.shard_mem_cache.borrow_mut().aggregate_client_load_status(aggregate_key, aggregate_client_key) {
            return Ok(());
        }

        // Take an exclusive lock on this aggregate client
        let aggregate_lock = self.aggregate_client_loading.acquire(aggregate_client_key);
        let _ = write_with_timeout(&aggregate_lock, "cache_aggregate_client").await?;

        // We have exclusive access now, check if another concurrent task has already done the work
        if let (true, _exists) = self.shard_mem_cache.borrow_mut().aggregate_client_load_status(aggregate_key, aggregate_client_key) {
            return Ok(());
        }

        let last_known_metablock = self.shard_mem_cache.borrow_mut().get_aggregate_last_metablock_pos(aggregate_key);

        // Begin the search from the last_known_metablock, moving backwards
        let mut scanner = ReverseMetablockScanner::new(
            &self.rotating_log_cache, 
            last_known_metablock.log_id,
            Some(last_known_metablock.metablock_absolute_pos.saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64)), //Include SELF
            self.config.read_max_chunk_size,
        );

        let find_result = scanner.scan::<bool, ()>(|_log_id, _metablock_absolute_pos, metablock_bytes| {
            if !metablock_bytes::is_matches_aggregate_key(metablock_bytes, aggregate_key) {
                return Ok(None);
            }

            let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
            let low_priority= client_id != aggregate_client_key.client_id;
            let target_aggregate_client_key = AggregateClientKey::new(aggregate_key.clone(), client_id);

            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

            // Not the aggregate we are searching for. Can we eager cache it? If not, skip it.
            if low_priority && shard_mem_cache.is_aggregate_client_cache_full_or_contains(&target_aggregate_client_key) {
                return Ok(None)
            }

            let last_client_event_index = metablock_bytes::read_event_batch_max_client_event_index(metablock_bytes);

            shard_mem_cache.put_aggregate_client_into_cache(target_aggregate_client_key, last_client_event_index, low_priority);

            if low_priority {
                Ok(None)    //Haven't found aggregate client yet
            } else {
                Ok(Some(true)) //Done searching
            }
        }).await?;

        let found = find_result.unwrap_or(false);
        if !found {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            shard_mem_cache.put_aggregate_into_cache_as_not_found(aggregate_key.clone());
        }
        
        Ok(())
    }

    async fn aggregate_exists_and_cache(&self, searching_for_aggregate_key: &AggregateKey) -> Result<bool, ShardWriteError> {

        // If we are cached already
        if let (true, exists) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key) {
            return Ok(exists);
        }

        // Take an exclusive lock on this aggregate
        let aggregate_lock = self.aggregate_loading.acquire(searching_for_aggregate_key);
        let _ = write_with_timeout(&aggregate_lock, "move_aggregate_to_memcache").await?;

        // We have exclusive access now, check if another concurrent task has already done the work
        if let (true, exists) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key) {
            return Ok(exists);
        }

        // Begin the search from the active log, moving backwards
        let mut scanner = ReverseMetablockScanner::new(
            &self.rotating_log_cache, 
            self.rotating_log_cache.active_log_id(),
            None,
            self.config.read_max_chunk_size,
        );

        let find_result = scanner.scan::<bool, ()>(|log_id, metablock_absolute_pos, metablock_bytes| {
            if !metablock_bytes::is_metablock_kind_event_batch_metadata(metablock_bytes) {
                return Ok(None);
            }

            let current_aggregate_key = metablock_bytes::read_event_batch_aggregate_key(metablock_bytes);
            let low_priority= *searching_for_aggregate_key != current_aggregate_key;

            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

            // Not the aggregate we are searching for. Can we eager cache it? If not, skip it.
            if low_priority && shard_mem_cache.is_aggregate_snapshot_full_or_contains(&current_aggregate_key) {
                return Ok(None)
            }

            let snapshot = MemSnapshotAggregate {
                event_index: metablock_bytes::read_event_batch_max_event_index(metablock_bytes),
                event_batch_index: metablock_bytes::read_event_batch_event_batch_index(metablock_bytes),
                log_id,
                metablock_absolute_pos,
            };

            let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
            let last_client_event_index = metablock_bytes::read_event_batch_max_client_event_index(metablock_bytes);

            shard_mem_cache.put_aggregate_into_cache(current_aggregate_key, snapshot, client_id, last_client_event_index, low_priority);

            if low_priority {
                Ok(None)    //Haven't found aggregate yet
            } else {
                Ok(Some(true)) //Done searching
            }
        }).await?;

        let found = find_result.unwrap_or(false);
        if !found {
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
        wal_index: u64,
    ) -> Result<PreparedWrite, ShardWriteError> {
        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

        // Validate client idempotency
        if write_request.enforce_client_idempotency {
            if let Some(last_client_event_index) = shard_mem_cache
                .get_client_event_index(aggregate_key, client_id)
            {
                let attempted_client_event_index = write_request
                    .events
                    .iter()
                    .map(|e| e.client_event_index)
                    .min()
                    .unwrap_or(0);
                if attempted_client_event_index <= last_client_event_index {
                    return Err(ShardWriteError::ClientIdempotencyViolation {
                        last_client_event_index,
                        attempted_client_event_index,
                    });
                }
            }
        }

        let aggregate_current_indexes = shard_mem_cache.get_event_indexes(aggregate_key);

        // Validate optimistic concurrency
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

        // Prepare event data
        let mut events_in_batch = std::mem::take(&mut write_request.events);
        let mut event_index = aggregate_current_indexes.event_index;
        let event_batch_index = aggregate_current_indexes.event_batch_index.saturating_add(1);

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
            &datablock_aggregate_event_batch,
            event_types_data,
        );
        let latest_client_event_index = metablock_event_batch.max_client_event_index;

        let datablock = Datablock {
            datablock_kind: DatablockKind::EventBatchItem(datablock_aggregate_event_batch),
        };

        // Serialize datablock - this can fail
        let serialized_datablock =
            serialize_datablock(&datablock, write_request.compression_type, 0)?;

        let server_timestamp = self.config.timestamp_config.now();

        let metablock = Metablock {
            wal_index,
            server_timestamp,
            lease_index,
            node_id: self.config.node_id,
            datablock: serialized_datablock.storage_kind,
            wal_metablock_type: MetablockKind::EventBatchMetadata(metablock_event_batch),
        };

        let shard_log_queue_item = ShardLogQueueItem::new(
            Some(datablock),
            serialized_datablock.external_data,
            metablock,
        );

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
    fn append_prepared_writes_to_queue(
        &self,
        prepared_writes: Vec<PreparedWrite>,
        final_wal_index: u64,
    ) -> bool {
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

        // Update the wal_index to reflect all appended writes
        shard_mem_cache.set_queue_wal_index(final_wal_index);

        shard_mem_cache.force_durable_on_next_write()
    }

    /// Perform sync with potential delay for amortisation.
    async fn sync_durable(&self, force_immediate: bool) -> Result<(), ShardFsyncError> {
        let rotating_log_cache = self.rotating_log_cache.clone();
        let shard_mem_cache = self.shard_mem_cache.clone();
        let watched_aggregates = self.watched_aggregates.clone();

        if force_immediate {
            self.fsync_coordinator
                .request_sync(None, move || {
                    sync_with_rollback(rotating_log_cache, shard_mem_cache, watched_aggregates)
                })
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
                    .request_sync(None, move || {
                        sync_with_rollback(rotating_log_cache, shard_mem_cache, watched_aggregates)
                    })
                    .await;
            })
            .detach();
            Ok(())
        }
    }
}

/// Extends the write batch index bounds in an AggregateWatchEventOperation when writing mulitple batches for the same aggregate.
fn add_write_event(
    write_events: &mut HashMap<AggregateKey, AggregateWatchEventOperation>,
    metablock_event_batch: &MetablockEventBatch,
) {
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
fn add_create_event(
    create_events: &mut HashMap<AggregateKey, AggregateWatchEventOperation>,
    aggregate_key: AggregateKey,
) {
    create_events
        .entry(aggregate_key)
        .or_insert(AggregateWatchEventOperation::Create {});
}

async fn sync_with_rollback(
    rotating_log_cache: Rc<RotatingLogCache>,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
) -> Result<(), ShardFsyncError> {
    // Lock the writer before we take the snapshot of the queue
    let lockable_active_log_file = rotating_log_cache.active();
    let mut shard_log_dma_file = write_with_timeout(&lockable_active_log_file, "sync_with_rollback").await?;

    let (
        has_enough_free_space,
        _current_log_id,
        preallocate_bytes,
        shard_dir,
        sync_positions_snapshot,
    ) = {
        let mut shard_mem_cache = shard_mem_cache.borrow_mut();
        if !shard_mem_cache.requires_write() {
            // Queue is empty - either another coordinator synced our items,
            // or a previous sync failed and cleared the queue
            if shard_mem_cache.force_durable_on_next_write() {
                return Err(ShardFsyncError::IoError(
                    "Disk sync failure forced queue clear".to_string(),
                ));
            }
            // Another coordinator successfully synced our items
            return Ok(());
        }

        if !shard_mem_cache.has_enough_free_space() {
            (
                false,
                shard_mem_cache.current_log_id(),
                shard_mem_cache.shard_log_preallocate_bytes(),
                Some(shard_mem_cache.shard_dir()),
                None,
            )
        } else {
            (
                true,
                shard_mem_cache.current_log_id(),
                shard_mem_cache.shard_log_preallocate_bytes(),
                None,
                Some(shard_mem_cache.take_sync_positions_snapshot()),
            )
        }
    };

    let mut sync_positions_snapshot = if !has_enough_free_space {
        let shard_dir = shard_dir.as_ref().unwrap();

        rotating_log_cache.rotate_to_next_log(&mut shard_log_dma_file, &shard_dir, preallocate_bytes).await?;

        // Can only acquire shard_mem_cache after async operations
        let mut shard_mem_cache = shard_mem_cache.borrow_mut();
        shard_mem_cache.rotate_to_next_log(
            shard_log_dma_file.log_id,
            shard_log_dma_file.shard_log_header.metablocks_position,
            shard_log_dma_file.shard_log_header.datablocks_position,
            shard_log_dma_file.file_len,
        );
        let snapshot = shard_mem_cache.take_sync_positions_snapshot();

        snapshot
    } else {
        sync_positions_snapshot.unwrap()
    };

    match sync(rotating_log_cache.active_log_id(), &mut shard_log_dma_file, rotating_log_cache.active_reader(), &mut sync_positions_snapshot).await {
        Ok(_) => {
            let mut shard_mem_cache = shard_mem_cache.borrow_mut();

            // Commit moves the carry over bytes back into memcache, so take queue first
            let pending_append_queue =
                std::mem::take(&mut sync_positions_snapshot.pending_append_queue);
            shard_mem_cache.commit_sync_positions_snapshot(sync_positions_snapshot);

            // Transform the queue into recent writes and broadcast events
            let mut write_events: HashMap<AggregateKey, AggregateWatchEventOperation> =
                HashMap::new();
            let mut create_events: HashMap<AggregateKey, AggregateWatchEventOperation> =
                HashMap::new();
            for queue_item in pending_append_queue {
                match &queue_item.metablock.wal_metablock_type {
                    MetablockKind::EventBatchMetadata(event_batch_metadata) => {
                        
                        // Build watched_aggregates events
                        add_write_event(&mut write_events, event_batch_metadata);

                        // If first event batch for aggregate, add create event
                        if event_batch_metadata.event_batch_index == FIRST_EVENT_BATCH_INDEX {
                            add_create_event(&mut create_events, event_batch_metadata.aggregate_key.clone());
                        }

                        let size_bytes = ((queue_item.metablock.deep_size_of()
                            + queue_item.datablock.deep_size_of())
                            * 3) as u64;

                        // Cache the write (only happens after durable write confirmed)
                        shard_mem_cache.cache_recent_write(
                            event_batch_metadata.aggregate_key.clone(),
                            event_batch_metadata.event_batch_index,
                            queue_item.metablock,
                            queue_item.datablock,
                            size_bytes,
                        );
                    }
                    _ => {}
                }
            }

            for (aggregate_key, operation) in create_events {
                watched_aggregates.broadcast(AggregateWatchEvent {
                    aggregate_key,
                    operation,
                });
            }
            
            for (aggregate_key, operation) in write_events {
                watched_aggregates.broadcast(AggregateWatchEvent {
                    aggregate_key: aggregate_key.clone(),
                    operation,
                });
            }

            return Ok(());
        }
        Err(e) => {
            let mut shard_mem_cache = shard_mem_cache.borrow_mut();
            shard_mem_cache.rollback_queue_positions();
            return Err(e);
        }
    }
}

async fn sync(
    log_id: u64,
    shard_log_dma_file: &mut ShardLogDmaFile,
    reader_shard_log_dma_file: Rc<RwLock<ShardLogDmaFile>>,
    sync_positions_snapshot: &mut SyncPositionsSnapshot,
) -> Result<(), ShardFsyncError> {
    let dma_file_writer = shard_log_dma_file.dma_file.as_mut();

    let dma_file_writer = if let Some(dma_file_writer) = dma_file_writer {
        dma_file_writer
    } else {
        return Err(ShardFsyncError::DmaFileNotInitialized);
    };

    // Write datablocks first so we can get the positions to include into metablocks
    if !sync_positions_snapshot.has_enough_free_space() {
        let free_space = sync_positions_snapshot.datablocks_position.saturating_sub(sync_positions_snapshot.metablocks_position);
        let required_space = sync_positions_snapshot.buffer_size_datablocks().saturating_add(sync_positions_snapshot.buffer_size_metablocks());

        return Err(ShardFsyncError::NotEnoughLogFreeSpace {
            required: required_space,
            available: free_space,
        });
    }
    let buffer_size_datablocks: u64 = sync_positions_snapshot.buffer_size_datablocks();

    let mut datablocks_absolute_write_positions: Vec<u64> =
        Vec::with_capacity(sync_positions_snapshot.pending_append_queue.len());
    let mut new_datablocks_position = sync_positions_snapshot.datablocks_position;
    let mut datablocks_carry_over: Option<Vec<u8>> =
        sync_positions_snapshot.datablocks_carry_over.take();
    if buffer_size_datablocks > 0 {
        let write_to_pos = dma_file_writer.align_up(sync_positions_snapshot.datablocks_position);
        let write_from_pos = dma_file_writer.align_down(
            sync_positions_snapshot
                .datablocks_position
                .saturating_sub(buffer_size_datablocks),
        );
        let aligned_buffer_size_datablocks = write_to_pos.saturating_sub(write_from_pos);

        let mut buffer_datablocks =
            dma_file_writer.alloc_dma_buffer(aligned_buffer_size_datablocks as usize);
        let buffer_datablocks_slice = buffer_datablocks.as_bytes_mut();
        buffer_datablocks_slice.fill(0);

        let end_carry_over = dma_file_writer
            .align_up(sync_positions_snapshot.datablocks_position)
            .saturating_sub(sync_positions_snapshot.datablocks_position);

        if end_carry_over > 0 {
            if datablocks_carry_over.is_none()
                || datablocks_carry_over.as_ref().unwrap().len() != end_carry_over as usize
            {
                return Err(ShardFsyncError::DatablocksCarryOverBufferNotPresent);
            }
            buffer_datablocks_slice
                [(aligned_buffer_size_datablocks.saturating_sub(end_carry_over)) as usize..]
                .copy_from_slice(&datablocks_carry_over.as_ref().unwrap());
        }

        new_datablocks_position = sync_positions_snapshot
            .datablocks_position
            .saturating_sub(buffer_size_datablocks);
        let front_carry_over = new_datablocks_position
            .saturating_sub(dma_file_writer.align_down(new_datablocks_position))
            as usize;
        if front_carry_over > 0 {
            buffer_datablocks_slice[..front_carry_over].fill(0);
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

        let datablocks_carry_over_size = dma_file_writer
            .align_up(new_datablocks_position)
            .saturating_sub(new_datablocks_position);
        if datablocks_carry_over_size > 0 {
            datablocks_carry_over = Some(
                buffer_datablocks_slice
                    [front_carry_over..(front_carry_over + datablocks_carry_over_size as usize)]
                    .to_vec(),
            );
        }

        dma_file_writer
            .write_at(
                buffer_datablocks,
                new_datablocks_position.saturating_sub(front_carry_over as u64),
            )
            .await?;
    }

    let buffer_size_metablocks: u64 = sync_positions_snapshot.buffer_size_metablocks();
    let mut buffer_metablocks = dma_file_writer.alloc_dma_buffer(buffer_size_metablocks as usize);
    let buffer_metablocks_slice = buffer_metablocks.as_bytes_mut();
    let mut position = 0usize;
    let mut index = 0;
    let mut new_wal_index = 0;
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

        new_wal_index = item.metablock.wal_index;

        // Track the absolute position where this metablock is written
        let metablock_absolute_pos = sync_positions_snapshot.metablocks_position + position as u64;
        
        // Update aggregate positions tracking for event batches (only if entry exists)
        if let MetablockKind::EventBatchMetadata(event_batch) = &item.metablock.wal_metablock_type {
            if let Some(aggregate_positions) = sync_positions_snapshot
                .aggregate_queue_positions
                .get_mut(&event_batch.aggregate_key)
            {
                aggregate_positions.log_id = log_id;
                aggregate_positions.metablock_absolute_pos = metablock_absolute_pos;
            }
        }

        let mut metablock_bytes = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(
            &item.metablock,
            WIRE_VERSION_WAL_METABLOCK,
            &mut metablock_bytes,
        )?;

        //let metablock_bytes: [u8; FIXED_BLOCK_SIZE_BYTES]
        buffer_metablocks_slice[position..position + FIXED_BLOCK_SIZE_BYTES]
            .copy_from_slice(&metablock_bytes);
        position += FIXED_BLOCK_SIZE_BYTES;
    }

    //Write header front & back
    let new_metablocks_position =
        sync_positions_snapshot.metablocks_position + buffer_metablocks.len() as u64;

    dma_file_writer
        .write_at(
            buffer_metablocks,
            sync_positions_snapshot.metablocks_position,
        )
        .await?;

    shard_log_dma_file
        .write_new_headers_and_fsync(
            new_datablocks_position,
            new_metablocks_position,
            new_wal_index,
        )
        .await?;

    {
        // Now we are 'committed' we need to update the reader, minimising our write lock duration
        // Other tasks use to read and we async lock the metadata and the DmaFile together, so async write lock here is unavoidable
        let mut reader_shard_log_dma_file = write_with_timeout(&reader_shard_log_dma_file, "sync_update_reader").await?;
        reader_shard_log_dma_file.update_from_writer(&shard_log_dma_file);
    }

    sync_positions_snapshot.datablocks_position = new_datablocks_position;
    sync_positions_snapshot.datablocks_carry_over = datablocks_carry_over;
    sync_positions_snapshot.metablocks_position = new_metablocks_position;
    sync_positions_snapshot.wal_index = new_wal_index;

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