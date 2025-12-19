use std::{path::PathBuf, time::Duration};

/// Per-node configuration that gets cloned to each shard.
/// Does not include shard_id since that's determined at runtime.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub node_id: u128,
    pub num_shards: usize,
    pub data_root: PathBuf,
    pub listen_address: String,
    pub async_flush_ms: u64,
    pub max_open_files: u64,
    pub aggregate_read_max_chunk_size: u64,
    pub aggregate_write_max_chunk_size: usize,
    pub aggregate_write_max_data_cache_size_bytes: usize,
    pub cache_trim_factor: usize,
    pub max_request_size: Option<u32>,
    pub max_event_batches_response_size: usize,
    pub slow_client_timeout: Duration,
    pub max_requested_latency_ms: u64,
    pub shard_log_preallocate_bytes: u64,
    pub durable_write_with_delay_us: Option<u64>,
    pub recent_write_cache_bytes: u64,
}