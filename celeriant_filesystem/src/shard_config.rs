use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct ShardConfig {
    /// Pre-allocated size for a shard log file.
    pub preallocate_bytes: u64,
    pub max_cached_files: u64,
    pub node_id: u128,
    pub async_flush_ms: u64,
    pub durable_write_with_delay_us: Option<u64>,
    pub shard_dir: PathBuf,
    /// Maximum bytes for in-memory recent write cache (0 = disabled)
    pub recent_write_cache_bytes: u64,
}

//14000 connections, 3200 aggregates, 10000us fsync, 342k writes/s