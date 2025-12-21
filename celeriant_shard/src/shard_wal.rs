//! Main shard write-ahead log orchestrator.
//! 
//! Coordinates validation, building, caching, and durability for a single shard.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use celeriant_memcache::internal_shard_config::InternalShardConfig;
use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_msg::request;
use celeriant_msg::request::requests::{ReadRequest, WriteRequest};
use celeriant_msg::response::responses::{ReadResponse, WriteResponse};
use celeriant_rotating_log::rotating_log_cache::RotatingLogCache;
use celeriant_rotating_log::rotating_log_error::RotatingLogError;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;
use celeriant_wal::datablocks::datablock::{self, Datablock};
use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use celeriant_wal::datablocks::datablock_kind::DatablockKind;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_event_batch::{EventTypesKind, MetablockEventBatch};
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_watch::aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation};
use celeriant_watch::aggregate_watchers::AggregateWatchers;
use celeriant_wire::datablock_serialization::serialize_datablock;
use celeriant_wire::wire_format::bincode_variable_serialise;

use crate::amortisation::coordinator::Coordinator;
use crate::bloom::bloom_filter_cache::BloomFilterCache;
use crate::bloom::event_type_filter::extract_unique_event_types;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::error::shard_read_error::ShardReadError;
use crate::error::shard_write_error::ShardWriteError;

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
    watched_aggregates: Rc<AggregateWatchers>,

    /// Cache for bloom filter construction to avoid repeated allocations, uses interior mutability
    bloom_filter_cache: Rc<BloomFilterCache>,

    config: InternalShardConfig,
}

impl ShardWal {
    /// Open or create a shard WAL.
    /// 
    /// If the shard directory exists with log files, reopens from the latest.
    /// Otherwise creates a new shard with an empty log file.
    pub async fn open(config: InternalShardConfig) -> Result<Self, RotatingLogError> {
        
        let rotating_log_cache = RotatingLogCache::new(config.shard_dir.clone(), config.shard_log_preallocate_bytes, config.max_open_files as usize).await?;
        
        let shard_mem_cache = {
            let active_lock = rotating_log_cache.active();
            let active_log_file = active_lock.read().await?;
            ShardMemCache::new(active_log_file.file_len, active_log_file.shard_log_header.metablocks_position, active_log_file.shard_log_header.wal_index, active_log_file.shard_log_header.datablocks_position, config.clone(), active_log_file.log_id)
        };

        Ok(Self {
            shard_mem_cache: Rc::new(RefCell::new(shard_mem_cache)),
            rotating_log_cache: Rc::new(rotating_log_cache),
            fsync_coordinator: Rc::new(Coordinator::new()),
            watched_aggregates: Rc::new(AggregateWatchers::new()),
            bloom_filter_cache: Rc::new(BloomFilterCache::new()),
            config,
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
    ) -> Result<WriteResponse, ShardWriteError> {

        // Make sure we have at least one event to write
        if write_request.events.is_empty() {
            return Err(ShardWriteError::EmptyEventsList);
        }

        // Validate that no event uses the sentinel 0 event type
        if let Some(ev) = write_request
            .events
            .iter()
            .find(|e| e.event_type_major == 0)
        {
            return Err(ShardWriteError::ZeroEventType {
                client_event_index: ev.client_event_index,
            });
        }

        let (write_response, force_durable) = self.add_write_request_to_pending_queue(lease_index, write_request)?;

        // Now we wait on disk write before ack to client
        self.sync_durable(force_durable).await?;

        Ok(write_response)
    }

    /// Read event batches from an aggregate.
    /// Reads from cache when possible, falls back to disk for older batches.
    pub async fn read(&self, request: &ReadRequest) -> Result<ReadResponse, ShardReadError> {
        todo!()
    }

    /// Close the shard WAL, flushing any pending writes.
    pub async fn close(&self) -> Result<(), RotatingLogError> {
        self.rotating_log_cache.close().await
    }

    /// Get the watched aggregates registry for this shard.
    pub fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
        self.watched_aggregates.clone()
    }
}

// Internal methods

impl ShardWal {

    /// Validate and add a write request to the pending queue.
    fn add_write_request_to_pending_queue(&self, lease_index: u64, mut write_request: WriteRequest) -> Result<(WriteResponse, bool), ShardWriteError>
    {
        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

        // If checking idempotency, check if client is providing the same events again using client event index, if so, error
        if write_request.enforce_client_idempotency {
            if let Some(last_client_event_index) = shard_mem_cache
                .get_client_event_index(
                    &write_request.aggregate_key,
                    write_request.client_id,
                )
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

        if !write_request.allow_create {
            if !shard_mem_cache.aggregate_exists(&write_request.aggregate_key) {
                return Err(ShardWriteError::AggregateNotExists);
            }
        }

        let aggregate_current_indexes =
            shard_mem_cache.get_event_indexes(&write_request.aggregate_key);

        // If doing optimistic concurrency, check expected event batch index matches current
        if let Some(expected) = write_request.expected_event_batch_index {
            if expected != aggregate_current_indexes.event_batch_index {
                return Err(ShardWriteError::OptimisticConcurrencyViolation {
                    expected_event_batch_index: expected,
                    current_event_batch_index: aggregate_current_indexes.event_batch_index,
                });
            }
        }

        // Update events - set event indexes, server timestamp millis. Keep track of last event index assigned to update state later
        let mut events_in_batch = std::mem::take(&mut write_request.events);

        let mut event_index = aggregate_current_indexes.event_index;
        let start_event_index = event_index.saturating_add(1);
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
            events: events_in_batch 
        };
        
        let metablock_event_batch = MetablockEventBatch::from_batch_item(write_request.client_id, write_request.user_id, write_request.aggregate_key.clone(), &datablock_aggregate_event_batch, event_types_data);
        let latest_client_event_index = metablock_event_batch.max_client_event_index;
        
        let datablock = Datablock {
            datablock_kind: DatablockKind::EventBatchItem(datablock_aggregate_event_batch),
        };

        let serialized_datablock = serialize_datablock(&datablock, write_request.compression_type, 0)?;
        let external_data_len = serialized_datablock.external_data.as_ref().map(|f|f.len()).unwrap_or_default() as u64;

        let server_timestamp = get_server_timestamp_ms();

        let metablock = Metablock {
            wal_index: shard_mem_cache.current_wal_index().saturating_add(1),
            server_timestamp,
            lease_index,
            node_id: self.config.node_id,
            datablock: serialized_datablock.storage_kind,
            wal_metablock_type: MetablockKind::EventBatchMetadata(metablock_event_batch),
        };

        let shard_log_queue_item = ShardLogQueueItem::new(Some(datablock), serialized_datablock.external_data, metablock);

        // Update next event index, next event batch index, client event indexes
        shard_mem_cache.add_to_pending_append_queue(
            &write_request.aggregate_key,
            event_index,
            event_batch_index,
            write_request.client_id,
            latest_client_event_index,
            shard_log_queue_item,
        );

        let write_response = WriteResponse {
            correlation_id: write_request.correlation_id,
            event_batch_index,
            start_event_index,
            server_timestamp,
            compressed_size: FIXED_BLOCK_SIZE_BYTES as u64 + external_data_len,
        };

        Ok((write_response, shard_mem_cache.force_durable_on_next_write()))
    }

    /// Perform sync with potential delay for coalescing.
    async fn sync_durable(&self, force_immediate: bool) -> Result<(), ShardFsyncError> {
        // Now we wait on disk write before ack to client
        // if force_immediate {
        //     sync_with_delay(self.wal_sync_event.clone(), self.rotating_log_cache.clone(), self.shard_mem_cache.clone(), None, self.watched_aggregates.clone()).await?;
        // } else if !self.config.non_durable_writes {
        //     sync_with_delay(self.wal_sync_event.clone(), self.rotating_log_cache.clone(), self.shard_mem_cache.clone(), Some(self.config.fsync_delay), self.watched_aggregates.clone()).await?;
        // } else {
        //     let fsync_delay = self.config.fsync_delay;
        //     let watched_aggregates = self.watched_aggregates.clone();
        //     let shard_mem_cache = self.shard_mem_cache.clone();
        //     let wal_sync_event = self.wal_sync_event.clone();
        //     let rotating_log_cache = self.rotating_log_cache.clone();

        //     glommio::spawn_local(async move {
        //         let _ = sync_with_delay(wal_sync_event, rotating_log_cache, shard_mem_cache.clone(), Some(fsync_delay), watched_aggregates).await;
        //     })
        //     .detach();
        // }

        Ok(())
    }

    /// Execute the actual sync operation (called by sync coordinator).
    async fn do_sync(&self) -> Result<(), ShardFsyncError> {
        todo!()
    }

    /// Broadcast write events to watchers after durable sync.
    fn notify_watchers(
        &self,
        events: HashMap<AggregateKey, AggregateWatchEventOperation>,
    ) {
        for (aggregate_key, operation) in events {
            self.watched_aggregates.broadcast(AggregateWatchEvent {
                aggregate_key: aggregate_key.clone(),
                operation,
            });
        }
    }
}

/// Extends the write batch index bounds in an AggregateWatchEventOperation when writing mulitple batches for the same aggregate.
fn add_write_event(write_events: &mut HashMap<AggregateKey, AggregateWatchEventOperation>, metablock_event_batch: &MetablockEventBatch) {
    write_events
        .entry(metablock_event_batch.aggregate_key.clone())
        .and_modify(|event| {
            if let AggregateWatchEventOperation::Write { from_event_batch_index, to_event_batch_index } = event {
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

fn get_server_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}