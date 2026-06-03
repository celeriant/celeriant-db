#[derive(Debug, Clone)]
pub enum WatchSessionError {
    WatchLatencyTooHigh {
        latency_ms: u64,
        max_latency_ms: u64,
    },
    TooManySubscribers {
        active: usize,
        max: usize,
    },
}