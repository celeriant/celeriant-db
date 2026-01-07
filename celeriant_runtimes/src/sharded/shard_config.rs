use std::{path::PathBuf, time::Duration};

use celeriant_shard::timestamp_config::TimestampConfig;

use crate::sharded::routing_rule::RoutingRule;

/// Per-node configuration that gets cloned to each shard.
/// Does not include shard_id since that's determined at runtime.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub node_id: u128,
    pub num_shards: u32,
    pub data_root: PathBuf,
    pub listen_address: String,
    pub max_open_files: u64,
    pub read_max_chunk_size: u64,
    pub write_max_chunk_size: u64,
    pub max_request_size: u64,
    pub max_response_size: u64,
    pub slow_client_timeout: Duration,
    pub max_requested_latency: Duration,
    pub shard_log_preallocate_bytes: u64,
    pub fsync_delay: Duration,
    pub recent_write_cache_bytes: u64,
    pub routing_rule: RoutingRule,
    pub non_durable_writes: bool,
    pub aggregate_client_snapshots_cache_bytes: u64,
    pub aggregate_snapshots_cache_bytes: u64,
    pub timestamp_config: TimestampConfig,
    pub list_max_duration: Duration,
    pub list_page_size: usize,
    pub list_wal_index_cache_bytes: u64,
}