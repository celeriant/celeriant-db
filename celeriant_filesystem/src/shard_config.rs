#[derive(Clone, Debug)]
pub struct ShardConfig {
    /// Pre-allocated size for a shard log file.
    pub preallocate_bytes: u64,

    /// WAL file format version.
    pub shard_log_version: u32,

    pub checkpoint_reserved_bytes_multiple: u64,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            // 1 GiB
            preallocate_bytes: 1024 * 1024 * 1024,

            shard_log_version: 1,

            checkpoint_reserved_bytes_multiple: 100,
        }
    }
}