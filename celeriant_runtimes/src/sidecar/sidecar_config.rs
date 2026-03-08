#[derive(Clone)]
pub struct SidecarConfig {
    /// Number of Tokio worker threads. Defaults to `max(2, num_shards / 2)`.
    pub worker_threads: usize,
    /// Capacity of the control lane queue (leases, membership).
    pub control_lane_capacity: usize,
    /// Capacity of the data lane queue (batch uploads/downloads).
    pub data_lane_capacity: usize,
    /// Enable the Prometheus metrics HTTP endpoint.
    pub metrics_enabled: bool,
    /// Port for the metrics and health HTTP server.
    pub metrics_port: u16,
    /// Number of shards (for health endpoint reporting).
    pub num_shards: u32,
    /// Node ID (for health endpoint reporting).
    pub node_id: u128,
}