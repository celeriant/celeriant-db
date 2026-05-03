use celeriant_wal::{constants::{FIXED_BLOCK_SIZE_BYTES, STRUCT_TO_MEMORY_REAL_SIZE}, datablocks::datablock::Datablock, metablocks::metablock::Metablock};

/// In-memory queue of data waiting to be written to disk + fsync'd
/// We include the structs here too as they go into the cache after fsync
pub struct ShardLogQueueItem {
    pub datablock_bytes: Option<Vec<u8>>,
    pub datablock: Option<Datablock>,
    pub metablock: Metablock,
    pub metablock_absolute_pos: u64,
    size_bytes_cached: u64,
}

impl ShardLogQueueItem {
    pub fn new(
        datablock: Option<Datablock>,
        datablock_bytes: Option<Vec<u8>>,
        metablock: Metablock,
    ) -> Self {
        let datablock_len = datablock_bytes.as_ref().map_or(0, |v| v.len());
        let size_bytes_cached = ((FIXED_BLOCK_SIZE_BYTES + datablock_len) * STRUCT_TO_MEMORY_REAL_SIZE) as u64;
        Self {
            datablock_bytes,
            datablock,
            metablock,
            metablock_absolute_pos: 0,
            size_bytes_cached,
        }
    }

    #[inline]
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes_cached
    }
}
