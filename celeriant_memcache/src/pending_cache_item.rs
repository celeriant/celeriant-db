use celeriant_wal::{datablocks::datablock::Datablock, metablocks::metablock::Metablock};
use deepsize::DeepSizeOf;

const STRUCT_TO_MEMORY_REAL_SIZE: usize = 3;

/// Metablock/datablock pair for caching after replication completes.
/// Unlike ShardLogQueueItem, this excludes datablock_bytes since
/// the serialized bytes are only needed for disk writes (already done).
pub struct PendingCacheItem {
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
}

impl PendingCacheItem {
    pub fn size_bytes(&self) -> u64 {
        ((self.metablock.deep_size_of() + self.datablock.deep_size_of()) * STRUCT_TO_MEMORY_REAL_SIZE) as u64
    }
}
