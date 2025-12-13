use celeriant_wal::shard_log::shard_log_header::ShardLogHeader;
use glommio::io::DmaFile;

/// Represents the physical file on disk. There are many for each shard.
pub struct ShardLogFile {
    pub shard_log_header: ShardLogHeader,

    /// This is option so we can take it on compact + rename 
    /// operations with the file system
    pub dma_file: Option<DmaFile>,
}