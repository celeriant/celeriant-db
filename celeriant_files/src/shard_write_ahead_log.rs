use std::{cell::Cell, collections::HashMap};

use celeriant_wal::{constants::METADATA_BATCH_SIZE_BYTES, wal::{event_batch_item::EventBatchItem, event_batch_metadata::EventBatchMetadata}};
use glommio::sync::RwLock;

use crate::{local_event::LocalEvent, shard_log_file::ShardLogFile, shard_log_write_error::ShardLogWriteError, shard_mem_cache::ShardMemCache};

/// Represents the WAL for each shard. Operations we perform:
///   - Append batches to in-memory queue
///   - Write batches from queue to disk
///   - Read batches either from hot cache or from disk
/// We use RwLock here to allow concurrent, interleaved tasks in the shard.
///   - We can read from cache while appending to queue or writing to disk
///   - We can append to queue while writing to disk
/// The goal is to hold the shortest write locks possible.
pub struct ShardWriteAheadLog {
    pub shard_logs: RwLock<HashMap<u64, ShardLogFile>>,
    pub shard_mem_cache: RwLock<ShardMemCache>,
    pub pending_append_queue: RwLock<Vec<EventBatchQueueItem>>,
    pub wal_sync_event: RwLock<Option<LocalEvent<SyncResult>>>,
    pub has_pending_sync_error: Cell<bool>,
}

pub type SyncResult = Result<(), ShardLogWriteError>;

/// In-memory queue of data waiting to be written to disk + fsync'd
/// We include the structs here too as they go into the cache after fsync
pub struct EventBatchQueueItem {
    pub compressed_event_batch_item: Vec<u8>,
    pub event_batch_item: EventBatchItem,
    pub metadata_bytes: [u8; METADATA_BATCH_SIZE_BYTES],
    pub event_batch_metadata: EventBatchMetadata,
}