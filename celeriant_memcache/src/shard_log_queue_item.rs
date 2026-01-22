use celeriant_wal::{datablocks::{datablock::Datablock}, metablocks::metablock::Metablock};
use deepsize::DeepSizeOf;
const STRUCT_TO_MEMORY_REAL_SIZE: usize = 3;

/// In-memory queue of data waiting to be written to disk + fsync'd
/// We include the structs here too as they go into the cache after fsync
pub struct ShardLogQueueItem {
    pub datablock_bytes: Option<Vec<u8>>,
    pub datablock: Option<Datablock>,
    pub metablock: Metablock,
    pub metablock_absolute_pos: u64,
}

impl ShardLogQueueItem {
    pub fn new(
        datablock: Option<Datablock>,
        datablock_bytes: Option<Vec<u8>>,
        metablock: Metablock,
    ) -> Self {
        Self {
            datablock_bytes,
            datablock,
            metablock,
            metablock_absolute_pos: 0,
        }
    }
    
    pub fn size_bytes(&self) -> u64 {
        ((self.metablock.deep_size_of() + self.datablock.deep_size_of()) * STRUCT_TO_MEMORY_REAL_SIZE) as u64
    }
}