use std::path::PathBuf;

/// Per-node configuration that gets cloned to each shard.
/// Does not include shard_id since that's determined at runtime.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub node_id: u128,
    pub num_shards: usize,
    pub data_root: PathBuf,
    pub listen_address: String,
    pub async_flush_ms: u64,
    pub max_open_aggregates: usize,
    pub aggregate_read_max_chunk_size: u64,
    pub aggregate_write_max_chunk_size: usize,
    pub aggregate_write_max_data_cache_size_bytes: usize,
    pub cache_trim_factor: usize,
    pub max_request_size: usize,
    pub max_event_batches_response_size: usize,
}