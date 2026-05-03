
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Eq)]
pub enum BackpressureCause {
    /// Combined inflight bytes (`pending_append_bytes + pending_replication_bytes`)
    /// have reached `internode_max_request_size`. Holding the bound here keeps the
    /// next replication snapshot fitting in one TCP request.
    InflightPressure,
    /// Within the cooldown window after a replication rollback. `remaining_ms`
    /// is how much longer the cooldown has to run.
    RollbackCooldown { remaining_ms: u64 },
}

impl BackpressureCause {
    pub fn metric_label(&self) -> &'static str {
        match self {
            BackpressureCause::InflightPressure => "inflight_pressure",
            BackpressureCause::RollbackCooldown { .. } => "rollback_cooldown",
        }
    }
}

/// `now` must be `Some` when `last_rollback_at` is `Some`; callers that have
/// no recorded rollback can pass `None` to skip the `Instant::now()` syscall.
pub fn check_replication_backpressure(
    inflight_pressured: bool,
    last_rollback_at: Option<Instant>,
    rollback_cooldown: Duration,
    now: Option<Instant>,
) -> Option<BackpressureCause> {
    if inflight_pressured {
        return Some(BackpressureCause::InflightPressure);
    }
    if let (Some(t), Some(now)) = (last_rollback_at, now) {
        let elapsed = now.saturating_duration_since(t);
        if elapsed < rollback_cooldown {
            let remaining_ms = (rollback_cooldown - elapsed).as_millis() as u64;
            return Some(BackpressureCause::RollbackCooldown { remaining_ms });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_when_no_pressure_and_no_recent_rollback() {
        assert_eq!(
            check_replication_backpressure(false, None, Duration::from_millis(500), None),
            None
        );
    }

    #[test]
    fn inflight_pressure_takes_precedence_over_rollback_cooldown() {
        let now = Instant::now();
        let cause = check_replication_backpressure(
            true,
            Some(now - Duration::from_millis(10)),
            Duration::from_millis(500),
            Some(now),
        );
        assert_eq!(cause, Some(BackpressureCause::InflightPressure));
    }

    #[test]
    fn rollback_cooldown_within_window_rejects_with_remaining_ms() {
        let now = Instant::now();
        let last_rollback = now - Duration::from_millis(100);
        let cause = check_replication_backpressure(
            false,
            Some(last_rollback),
            Duration::from_millis(500),
            Some(now),
        );
        assert!(matches!(
            cause,
            Some(BackpressureCause::RollbackCooldown { remaining_ms }) if (390..=410).contains(&remaining_ms)
        ));
    }

    #[test]
    fn rollback_cooldown_expired_accepts_write() {
        let now = Instant::now();
        let last_rollback = now - Duration::from_millis(600);
        assert_eq!(
            check_replication_backpressure(
                false,
                Some(last_rollback),
                Duration::from_millis(500),
                Some(now),
            ),
            None
        );
    }

    #[test]
    fn rollback_cooldown_at_exact_boundary_accepts() {
        // elapsed == cooldown → not strictly less than → accept.
        let now = Instant::now();
        let last_rollback = now - Duration::from_millis(500);
        assert_eq!(
            check_replication_backpressure(
                false,
                Some(last_rollback),
                Duration::from_millis(500),
                Some(now),
            ),
            None
        );
    }

    #[test]
    fn no_rollback_recorded_skips_cooldown_check() {
        assert_eq!(
            check_replication_backpressure(false, None, Duration::from_millis(500), None),
            None
        );
    }

    #[test]
    fn metric_labels_are_stable_and_distinct() {
        assert_eq!(BackpressureCause::InflightPressure.metric_label(), "inflight_pressure");
        assert_eq!(
            BackpressureCause::RollbackCooldown { remaining_ms: 100 }.metric_label(),
            "rollback_cooldown",
        );
    }
}
