use std::{path::PathBuf, time::Duration};

#[derive(Clone, Debug)]
pub struct InternalShardConfig {
    pub node_id: u128,
    pub max_open_files: u64,
    pub shard_log_preallocate_bytes: u64,
    pub fsync_delay: Duration,
    pub recent_write_cache_bytes: u64,
    pub non_durable_writes: bool,
    pub shard_dir: PathBuf,
    pub max_response_size: u64,
}