use celeriant_wal::{constants::{FIXED_BLOCK_SIZE_BYTES, STRUCT_TO_MEMORY_REAL_SIZE}, datablocks::datablock::Datablock, metablocks::metablock::Metablock};

use crate::shard_log_queue_item::ShardLogQueueItem;

/// Metablock/datablock pair for caching after replication completes.
/// Unlike ShardLogQueueItem, this excludes datablock_bytes since
/// the serialized bytes are only needed for disk writes (already done).
pub struct PendingCacheItem {
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
    pub metablock_absolute_pos: u64,
    size_bytes_cached: u64,
}

impl PendingCacheItem {
    pub fn new(queue_item: ShardLogQueueItem) -> Self {
        PendingCacheItem {
            size_bytes_cached: queue_item.size_bytes(),
            metablock: queue_item.metablock,
            datablock: queue_item.datablock,
            metablock_absolute_pos: queue_item.metablock_absolute_pos,
        }
    }

    pub fn from_parts(metablock: Metablock, datablock: Option<Datablock>, metablock_absolute_pos: u64, datablock_bytes_len: usize) -> Self {
        let size_bytes_cached = ((FIXED_BLOCK_SIZE_BYTES + datablock_bytes_len) * STRUCT_TO_MEMORY_REAL_SIZE) as u64;
        PendingCacheItem { metablock, datablock, metablock_absolute_pos, size_bytes_cached }
    }

    #[inline]
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes_cached
    }
}
