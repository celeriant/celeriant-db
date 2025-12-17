#[derive(Clone, Debug)]
pub struct ShardConfig {
    /// Pre-allocated size for a shard log file.
    pub preallocate_bytes: u64,
    pub node_id: u128,
    pub async_flush_ms: u64,
    pub durable_write_with_delay_us: Option<u64>,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            // 1 GiB
            preallocate_bytes: 1024 * 1024 * 1024,
            node_id: 0,
            async_flush_ms: 100,
            durable_write_with_delay_us: Some(10000),
        }
    }
}

//14000 connections, 3200 aggregates, 10000us fsync, 342k writes/s