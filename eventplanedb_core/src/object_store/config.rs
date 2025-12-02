//! Configuration for the Object Store sidecar runtime.

/// Configuration for the Tokio sidecar runtime.
#[derive(Clone, Debug)]
pub struct ObjectStoreRuntimeConfig {
    /// Number of Tokio worker threads. Defaults to `max(2, num_shards / 2)`.
    pub worker_threads: usize,
    /// Capacity of the control lane queue (leases, membership).
    pub control_lane_capacity: usize,
    /// Capacity of the data lane queue (batch uploads/downloads).
    pub data_lane_capacity: usize,
    /// Capacity of the tiering lane queue (future cold-tier moves).
    pub tiering_lane_capacity: usize,
    /// Maximum number of in-flight operations across all lanes.
    pub max_inflight_ops: usize,
    /// Heartbeat interval in milliseconds for health checks.
    pub heartbeat_interval_ms: u64,
}

impl Default for ObjectStoreRuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: 2,
            control_lane_capacity: 256,
            data_lane_capacity: 1024,
            tiering_lane_capacity: 128,
            max_inflight_ops: 2048,
            heartbeat_interval_ms: 1000,
        }
    }
}

impl ObjectStoreRuntimeConfig {
    pub fn with_num_shards(num_shards: usize) -> Self {
        Self {
            worker_threads: std::cmp::max(2, num_shards / 2),
            ..Default::default()
        }
    }
}

/// Retry and timeout configuration for object store operations.
#[derive(Clone, Debug)]
pub struct ObjectStoreRetryConfig {
    /// Timeout for lease PUT/GET operations in milliseconds.
    pub lease_timeout_ms: u64,
    /// Number of retry attempts for lease operations.
    pub lease_retry_attempts: u32,
    /// Timeout for membership update operations in milliseconds.
    pub membership_timeout_ms: u64,
    /// Number of retry attempts for membership operations.
    pub membership_retry_attempts: u32,
    /// Timeout for batch PUT operations in milliseconds.
    pub batch_put_timeout_ms: u64,
    /// Number of retry attempts for batch PUT operations.
    pub batch_put_retries: u32,
    /// Timeout for batch GET operations in milliseconds.
    pub batch_get_timeout_ms: u64,
    /// Number of retry attempts for batch GET operations.
    pub batch_get_retries: u32,
    /// Timeout for batch DELETE operations in milliseconds.
    pub batch_delete_timeout_ms: u64,
    /// Number of retry attempts for batch DELETE operations.
    pub batch_delete_retries: u32,
    /// Jitter factor for exponential backoff (0.0 - 1.0).
    pub jitter_factor: f64,
    /// Base delay for exponential backoff in milliseconds.
    pub base_backoff_ms: u64,
    /// Maximum delay for exponential backoff in milliseconds.
    pub max_backoff_ms: u64,
}

impl Default for ObjectStoreRetryConfig {
    fn default() -> Self {
        Self {
            lease_timeout_ms: 250,
            lease_retry_attempts: 5,
            membership_timeout_ms: 500,
            membership_retry_attempts: 5,
            batch_put_timeout_ms: 5000,
            batch_put_retries: 3,
            batch_get_timeout_ms: 5000,
            batch_get_retries: 3,
            batch_delete_timeout_ms: 2000,
            batch_delete_retries: 5,
            jitter_factor: 0.2,
            base_backoff_ms: 50,
            max_backoff_ms: 1000,
        }
    }
}