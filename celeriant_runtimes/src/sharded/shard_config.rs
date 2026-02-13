use std::{path::PathBuf, time::Duration};

use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_distributed::config::ReplicationConfig;
use celeriant_wal::compression_type::CompressionType;

use crate::sharded::routing_rule::RoutingRule;

/// Per-node configuration that gets cloned to each shard.
/// Does not include shard_id since that's determined at runtime.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub node_id: u128,
    pub num_shards: u32,
    pub replication_config: Option<ReplicationConfig>,
    pub advertised_replication_address: Option<String>,
    pub data_root: PathBuf,
    pub listen_address: String,
    pub client_port: u16,
    pub replication_port: u16,
    pub max_open_files: u64,
    pub read_max_chunk_size: u64,
    pub write_max_chunk_size: u64,
    pub max_request_size: u64,
    pub max_response_size: u64,
    pub internode_connection_timeout: Option<Duration>,
    pub server_compression_algorithm: CompressionType,
    pub slow_client_timeout: Duration,
    pub max_requested_latency: Duration,
    pub shard_log_preallocate_bytes: u64,
    pub fsync_delay: Duration,
    pub s3_download_max_rounds: u32,
    pub replication_delay: Duration,
    pub recent_write_cache_bytes: u64,
    pub routing_rule: RoutingRule,
    pub non_durable_writes: bool,
    pub aggregate_client_snapshots_cache_bytes: u64,
    pub aggregate_snapshots_cache_bytes: u64,
    pub timestamp_config: TimestampConfig,
    pub list_max_duration: Duration,
    pub list_page_size: usize,
    pub list_wal_index_cache_bytes: u64,
    pub pending_replication_high_water_bytes: u64,
    pub max_cluster_time_drift_ms: u64,
    pub max_catchup_gap_bytes: u64,
    pub max_s3_fallback_batch_bytes: u64,
}