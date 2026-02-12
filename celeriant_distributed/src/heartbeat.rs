use std::time::Instant;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Tracks heartbeat/ack liveness for a peer node.
///
/// Used by both leader (tracking follower acks) and follower (tracking leader heartbeats).
/// The caller owns this directly - no Rc/Cell wrapping needed.
pub struct HeartbeatLeaseTracker {
    last_received_at: Option<Instant>,
    lease_duration_ms: u64,
    max_clock_drift_ms: u64,
}

impl HeartbeatLeaseTracker {
    pub fn new(lease_duration_ms: u64, max_clock_drift_ms: u64) -> Self {
        Self {
            last_received_at: None,
            lease_duration_ms,
            max_clock_drift_ms,
        }
    }

    /// Start tracking (e.g., when connection established). Sets initial timestamp.
    pub fn start(&mut self) {
        self.last_received_at = Some(Instant::now());
    }

    /// Record that we received a heartbeat/ack right now.
    pub fn record_received(&mut self) {
        self.last_received_at = Some(Instant::now());
    }

    /// Check if the peer's lease has expired.
    ///
    /// Returns false if we haven't received any messages yet (not started).
    /// Returns true if deadline (last_received + lease_duration + clock_drift) has passed.
    pub fn is_expired(&self) -> bool {
        match self.last_received_at {
            None => false,
            Some(last) => {
                let deadline_ms = self.lease_duration_ms + self.max_clock_drift_ms;
                last.elapsed().as_millis() as u64 > deadline_ms
            }
        }
    }

    /// Reset the tracker (e.g., on reconnect or role change).
    pub fn reset(&mut self) {
        self.last_received_at = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_not_expired_before_started() {
        let tracker = HeartbeatLeaseTracker::new(1500, 500);
        assert!(!tracker.is_expired());
    }

    #[test]
    fn test_not_expired_within_lease() {
        let mut tracker = HeartbeatLeaseTracker::new(1500, 500);
        tracker.record_received();
        assert!(!tracker.is_expired());
    }

    #[test]
    fn test_expired_after_deadline() {
        let mut tracker = HeartbeatLeaseTracker::new(10, 5); // 15ms total
        tracker.record_received();
        std::thread::sleep(Duration::from_millis(20));
        assert!(tracker.is_expired());
    }

    #[test]
    fn test_reset_clears_state() {
        let mut tracker = HeartbeatLeaseTracker::new(10, 5);
        tracker.record_received();
        tracker.reset();
        assert!(!tracker.is_expired());
    }

    #[test]
    fn test_rapid_heartbeats_never_expire() {
        let mut tracker = HeartbeatLeaseTracker::new(50, 10); // 60ms total
        for _ in 0..10 {
            tracker.record_received();
            std::thread::sleep(Duration::from_millis(5));
            assert!(!tracker.is_expired());
        }
    }
}
