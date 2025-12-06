use crate::sidecar::{object_store_retry_config::ObjectStoreRetryConfig, s3_config::S3Config};

#[derive(Clone)]
pub struct SidecarConfig {
    /// Number of Tokio worker threads. Defaults to `max(2, num_shards / 2)`.
    pub worker_threads: usize,
    /// Capacity of the control lane queue (leases, membership).
    pub control_lane_capacity: usize,
    /// Capacity of the data lane queue (batch uploads/downloads).
    pub data_lane_capacity: usize,
    /// Maximum number of in-flight operations across all lanes.
    pub max_inflight_ops: usize,
    /// Heartbeat interval in milliseconds for health checks.
    pub heartbeat_interval_ms: u64,
    pub object_store_retry_config: ObjectStoreRetryConfig,
    /// Configuration if S3 control plane is enabled
    pub s3: Option<S3Config>,
}