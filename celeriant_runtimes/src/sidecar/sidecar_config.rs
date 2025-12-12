#[derive(Clone)]
pub struct SidecarConfig {
    /// Number of Tokio worker threads. Defaults to `max(2, num_shards / 2)`.
    pub worker_threads: usize,
    /// Capacity of the control lane queue (leases, membership).
    pub control_lane_capacity: usize,
    /// Capacity of the data lane queue (batch uploads/downloads).
    pub data_lane_capacity: usize,    
}