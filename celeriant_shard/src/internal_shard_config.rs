use std::{path::PathBuf, time::Duration};

use crate::timestamp_config::TimestampConfig;

#[derive(Clone, Debug)]
pub struct InternalShardConfig {
    pub node_id: u128,
    pub max_open_files: u64,
    pub shard_log_preallocate_bytes: u64,
    pub fsync_delay: Duration,
    pub replication_delay: Duration,
    pub recent_write_cache_bytes: u64,
    pub non_durable_writes: bool,
    pub shard_dir: PathBuf,
    pub max_response_size: u64,
    pub max_request_size: u64,
    pub aggregate_snapshots_cache_bytes: u64,
    pub aggregate_client_snapshots_cache_bytes: u64,
    pub read_max_chunk_size: u64,
    pub timestamp_config: TimestampConfig,
    pub list_page_size: usize,
    pub list_max_duration: Duration,
    pub list_wal_index_cache_bytes: u64,
    pub pending_replication_high_water_bytes: u64,
    pub max_cluster_time_drift_ms: u64,
    pub max_catchup_gap_bytes: u64,
}