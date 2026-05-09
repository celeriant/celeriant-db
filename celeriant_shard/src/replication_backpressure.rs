
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
    /// A heartbeat to the follower has been in flight longer than
    /// `heartbeat_starve_threshold` (proactive: the leader knows the heartbeat
    /// is hanging RIGHT NOW, before the follower auto-fences). Suppresses
    /// client writes so the NIC has bandwidth for the in-flight ack to land.
    /// Skipped when `is_follower_reachable=false` so genuine follower drops
    /// flow into S3 fallback instead of being rejected.
    FollowerHeartbeatStarved { in_flight_ms: u64 },
}

impl BackpressureCause {
    pub fn metric_label(&self) -> &'static str {
        match self {
            BackpressureCause::InflightPressure => "inflight_pressure",
            BackpressureCause::RollbackCooldown { .. } => "rollback_cooldown",
            BackpressureCause::FollowerHeartbeatStarved { .. } => "follower_heartbeat_starved",
        }
    }
}

/// `now` must be `Some` when `last_rollback_at` is `Some`; callers with no
/// recorded rollback can pass `None` to skip the `Instant::now()` syscall.
///
/// `current_heartbeat_started_at_unix_ms` and `now_unix_ms` are paired —
/// pass `Some` for both when checking heartbeat starvation, `None` for both
/// to skip. The atomic-backed unix-ms representation lets the heartbeat
/// signal cross between shard executor threads (which can't share `Instant`).
///
/// `heartbeat_starve_threshold == 0` disables the heartbeat-starved cause.
#[allow(clippy::too_many_arguments)]
pub fn check_replication_backpressure(
    inflight_pressured: bool,
    last_rollback_at: Option<Instant>,
    rollback_cooldown: Duration,
    current_heartbeat_started_at_unix_ms: Option<u64>,
    now_unix_ms: Option<u64>,
    is_follower_reachable: bool,
    heartbeat_starve_threshold: Duration,
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
    if !heartbeat_starve_threshold.is_zero() && is_follower_reachable {
        if let (Some(start_ms), Some(now_ms)) = (current_heartbeat_started_at_unix_ms, now_unix_ms) {
            let in_flight_ms = now_ms.saturating_sub(start_ms);
            if in_flight_ms > heartbeat_starve_threshold.as_millis() as u64 {
                return Some(BackpressureCause::FollowerHeartbeatStarved { in_flight_ms });
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const HB_OFF: Duration = Duration::ZERO;
    const HB_THRESHOLD: Duration = Duration::from_millis(1000);
    const NOW_UNIX_MS: u64 = 1_700_000_000_000;

    #[test]
    fn accepts_when_no_pressure_and_no_recent_rollback() {
        assert_eq!(
            check_replication_backpressure(false, None, Duration::from_millis(500), None, None, true, HB_OFF, None),
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
            None, None, true, HB_OFF,
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
            None, None, true, HB_OFF,
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
                None, None, true, HB_OFF,
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
                None, None, true, HB_OFF,
                Some(now),
            ),
            None
        );
    }

    #[test]
    fn no_rollback_recorded_skips_cooldown_check() {
        assert_eq!(
            check_replication_backpressure(false, None, Duration::from_millis(500), None, None, true, HB_OFF, None),
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
        assert_eq!(
            BackpressureCause::FollowerHeartbeatStarved { in_flight_ms: 1500 }.metric_label(),
            "follower_heartbeat_starved",
        );
    }

    #[test]
    fn heartbeat_in_flight_within_threshold_accepts() {
        assert_eq!(
            check_replication_backpressure(
                false, None, Duration::from_millis(500),
                Some(NOW_UNIX_MS - 300), Some(NOW_UNIX_MS), true, HB_THRESHOLD, None,
            ),
            None,
        );
    }

    #[test]
    fn heartbeat_in_flight_beyond_threshold_rejects() {
        let cause = check_replication_backpressure(
            false, None, Duration::from_millis(500),
            Some(NOW_UNIX_MS - 1500), Some(NOW_UNIX_MS), true, HB_THRESHOLD, None,
        );
        assert_eq!(cause, Some(BackpressureCause::FollowerHeartbeatStarved { in_flight_ms: 1500 }));
    }

    #[test]
    fn heartbeat_in_flight_with_unreachable_follower_does_not_reject() {
        // Availability invariant: when the follower is unreachable, writes
        // must flow into S3 fallback. Heartbeat-starved must not engage.
        assert_eq!(
            check_replication_backpressure(
                false, None, Duration::from_millis(500),
                Some(NOW_UNIX_MS - 5000), Some(NOW_UNIX_MS), false, HB_THRESHOLD, None,
            ),
            None,
        );
    }

    #[test]
    fn heartbeat_no_in_flight_accepts() {
        // No heartbeat in flight (between sends, after ack/error) → not starved.
        assert_eq!(
            check_replication_backpressure(
                false, None, Duration::from_millis(500),
                None, Some(NOW_UNIX_MS), true, HB_THRESHOLD, None,
            ),
            None,
        );
    }

    #[test]
    fn heartbeat_starved_disabled_when_threshold_zero() {
        assert_eq!(
            check_replication_backpressure(
                false, None, Duration::from_millis(500),
                Some(NOW_UNIX_MS - 5000), Some(NOW_UNIX_MS), true, Duration::ZERO, None,
            ),
            None,
        );
    }

    #[test]
    fn inflight_takes_precedence_over_heartbeat_starved() {
        let cause = check_replication_backpressure(
            true, None, Duration::from_millis(500),
            Some(NOW_UNIX_MS - 2000), Some(NOW_UNIX_MS), true, HB_THRESHOLD, None,
        );
        assert_eq!(cause, Some(BackpressureCause::InflightPressure));
    }

    #[test]
    fn rollback_takes_precedence_over_heartbeat_starved() {
        let now = Instant::now();
        let last_rollback = now - Duration::from_millis(100);
        let cause = check_replication_backpressure(
            false, Some(last_rollback), Duration::from_millis(500),
            Some(NOW_UNIX_MS - 2000), Some(NOW_UNIX_MS), true, HB_THRESHOLD, Some(now),
        );
        assert!(matches!(cause, Some(BackpressureCause::RollbackCooldown { .. })));
    }
}
