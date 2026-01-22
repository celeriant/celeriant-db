use celeriant_wal::{datablocks::datablock::Datablock, metablocks::metablock::Metablock};
use deepsize::DeepSizeOf;

use crate::shard_log_queue_item::ShardLogQueueItem;

const STRUCT_TO_MEMORY_REAL_SIZE: usize = 3;

/// Metablock/datablock pair for caching after replication completes.
/// Unlike ShardLogQueueItem, this excludes datablock_bytes since
/// the serialized bytes are only needed for disk writes (already done).
pub struct PendingCacheItem {
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
    pub metablock_absolute_pos: u64,
}

impl PendingCacheItem {
    pub fn new(queue_item: ShardLogQueueItem) -> Self {
        PendingCacheItem { metablock: queue_item.metablock, datablock: queue_item.datablock, metablock_absolute_pos: queue_item.metablock_absolute_pos }
    }
    pub fn size_bytes(&self) -> u64 {
        ((self.metablock.deep_size_of() + self.datablock.deep_size_of()) * STRUCT_TO_MEMORY_REAL_SIZE) as u64
    }
}
