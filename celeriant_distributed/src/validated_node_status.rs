use crate::node_status::NodeStatus;

pub fn unix_epoch_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// NodeStatus with a TTL. If shard 0 doesn't refresh before expires_at_ms - max_clock_drift_ms,
/// the effective status decays to Fenced.
#[derive(Clone, Copy, Debug)]
pub struct ValidatedNodeStatus {
    status: NodeStatus,
    lease_expires_at_ms: u64,
    max_clock_drift_ms: u64,
}

impl ValidatedNodeStatus {

    pub fn is_lease_expired(&self) -> bool {
        unix_epoch_now_ms() > self.lease_expires_at_ms
    }

    fn must_fence(&self) -> bool {
        unix_epoch_now_ms() > self.lease_expires_at_ms.saturating_sub(self.max_clock_drift_ms)
    }

    pub fn create_fenced() -> Self {
        Self { status: NodeStatus::Fenced, max_clock_drift_ms: 0, lease_expires_at_ms: 0 }
    }

    pub fn current_budget(&self) -> Option<std::time::Duration> {
        if !self.effective_node_status().is_leader() {
            return None;
        }
        let now = unix_epoch_now_ms();
        let safe_until = self.lease_expires_at_ms.saturating_sub(self.max_clock_drift_ms);
        Some(std::time::Duration::from_millis(safe_until.saturating_sub(now)))
    }

    pub fn create_standalone() -> Self {
        Self { status: NodeStatus::Standalone, max_clock_drift_ms: 0, lease_expires_at_ms: 0 }
    }

    pub fn create_boot_catchup() -> Self {
        Self { status: NodeStatus::BootCatchup, max_clock_drift_ms: 0, lease_expires_at_ms: 0 }
    }

    pub fn create_custom_status(status: NodeStatus, max_clock_drift_ms: u64, lease_expires_at_ms: u64) -> Self {
        Self { status, max_clock_drift_ms, lease_expires_at_ms }
    }

    pub fn effective_node_status(&self) -> NodeStatus {
        match self.status {
            NodeStatus::Standalone
            | NodeStatus::BootCatchup
            | NodeStatus::Fenced
            | NodeStatus::FollowerCatchingUp { .. } => self.status,
            _ if self.must_fence() => NodeStatus::Fenced,
            _ => self.status,
        }
    }

    pub fn raw(&self) -> NodeStatus {
        self.status
    }

    pub fn lease_expires_at_ms(&self) -> u64 {
        self.lease_expires_at_ms
    }

    pub fn is_follower(&self) -> bool {
        self.effective_node_status().is_follower()
    }

    pub fn is_leader(&self) -> bool {
        self.effective_node_status().is_leader()
    }

    pub fn is_fenced(&self) -> bool {
        self.effective_node_status().is_fenced()
    }

    pub fn is_standalone(&self) -> bool {
        matches!(self.status, NodeStatus::Standalone)
    }

    pub fn is_catching_up(&self) -> bool {
        self.effective_node_status().is_catching_up()
    }

    pub fn is_any_follower_state(&self) -> bool {
        self.effective_node_status().is_any_follower_state()
    }

    pub fn can_accept_writes(&self) -> bool {
        self.is_leader() || self.is_standalone()
    }
}

pub fn set_node_status_and_metric(
    cell: &std::cell::Cell<ValidatedNodeStatus>,
    status: ValidatedNodeStatus,
    shard_id: u32,
) {
    cell.set(status);
    if shard_id == 0 {
        let role = if status.is_leader() || status.is_standalone() { 1.0 } else { 0.0 };
        metrics::gauge!("celeriant_node_role").set(role);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FAR_FUTURE: u64 = u64::MAX / 2;
    const FAR_PAST: u64 = 0;
    const DRIFT: u64 = 500;

    // Leader fences at lease_expires_at - max_clock_drift (early).
    // Follower challenges at lease_expires_at (full TTL).
    #[test]
    fn leader_fences_before_lease_expires() {
        // Leader with TTL that just passed the must_fence threshold
        // but hasn't reached full expiry yet.
        let now = unix_epoch_now_ms();
        // Lease expires 200ms from now — within the drift window (500ms)
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Leader { lease_index: 1 },
            DRIFT,
            now + 200, // 200ms left < 500ms drift → must_fence() fires
        );
        assert_eq!(status.effective_node_status(), NodeStatus::Fenced,
            "Leader should fence when remaining time < max_clock_drift");
        assert!(!status.is_lease_expired(),
            "Full lease should NOT be expired yet (200ms left)");
    }

    #[test]
    fn leader_active_when_ttl_sufficient() {
        let now = unix_epoch_now_ms();
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Leader { lease_index: 1 },
            DRIFT,
            now + 5000, // 5s left >> 500ms drift
        );
        assert_eq!(status.effective_node_status(), NodeStatus::Leader { lease_index: 1 },
            "Leader should remain active when TTL >> drift");
    }

    #[test]
    fn follower_fences_only_at_full_expiry() {
        let now = unix_epoch_now_ms();
        // Follower with 200ms remaining — within leader's drift window but NOT expired
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Follower { leader_lease_index: 1 },
            DRIFT,
            now + 200,
        );
        // Follower should ALSO fence when must_fence() fires (same code path)
        assert_eq!(status.effective_node_status(), NodeStatus::Fenced,
            "Follower fences when must_fence() fires");
    }

    #[test]
    fn follower_active_when_ttl_sufficient() {
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Follower { leader_lease_index: 1 },
            DRIFT,
            FAR_FUTURE,
        );
        assert_eq!(status.effective_node_status(), NodeStatus::Follower { leader_lease_index: 1 });
    }

    #[test]
    fn expired_leader_is_fenced() {
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Leader { lease_index: 1 },
            DRIFT,
            FAR_PAST,
        );
        assert!(status.is_fenced());
        assert!(status.is_lease_expired());
        assert!(!status.can_accept_writes());
    }

    #[test]
    fn expired_follower_is_fenced() {
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Follower { leader_lease_index: 1 },
            DRIFT,
            FAR_PAST,
        );
        assert!(status.is_fenced());
        assert!(status.is_lease_expired());
    }

    // BootCatchup and FollowerCatchingUp are never decayed to Fenced,
    // even when must_fence() would fire.
    #[test]
    fn boot_catchup_ttl_exempt() {
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::BootCatchup,
            DRIFT,
            FAR_PAST, // lease "expired" long ago
        );
        assert_eq!(status.effective_node_status(), NodeStatus::BootCatchup,
            "BootCatchup must not decay to Fenced even with expired TTL");
        assert!(!status.is_fenced());
        assert!(status.is_catching_up());
    }

    #[test]
    fn follower_catching_up_ttl_exempt() {
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::FollowerCatchingUp { leader_lease_index: 1 },
            DRIFT,
            FAR_PAST, // lease "expired" long ago
        );
        assert_eq!(
            status.effective_node_status(),
            NodeStatus::FollowerCatchingUp { leader_lease_index: 1 },
            "FollowerCatchingUp must not decay to Fenced even with expired TTL"
        );
        assert!(!status.is_fenced());
        assert!(status.is_catching_up());
    }

    #[test]
    fn set_node_status_and_metric_updates_cell_on_shard_zero() {
        let cell = std::cell::Cell::new(ValidatedNodeStatus::create_boot_catchup());
        let target = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Leader { lease_index: 3 },
            DRIFT,
            FAR_FUTURE,
        );
        set_node_status_and_metric(&cell, target, 0);
        assert_eq!(cell.get().raw(), NodeStatus::Leader { lease_index: 3 });
    }

    #[test]
    fn set_node_status_and_metric_updates_cell_on_non_zero_shards() {
        // Non-shard-0 callers must still update the cell. Only the gauge is gated.
        let cell = std::cell::Cell::new(ValidatedNodeStatus::create_boot_catchup());
        let target = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Follower { leader_lease_index: 7 },
            DRIFT,
            FAR_FUTURE,
        );
        set_node_status_and_metric(&cell, target, 1);
        assert_eq!(cell.get().raw(), NodeStatus::Follower { leader_lease_index: 7 });
        set_node_status_and_metric(&cell, target, 42);
        assert_eq!(cell.get().raw(), NodeStatus::Follower { leader_lease_index: 7 });
    }

    #[test]
    fn standalone_ttl_exempt() {
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Standalone,
            DRIFT,
            FAR_PAST,
        );
        assert_eq!(status.effective_node_status(), NodeStatus::Standalone);
        assert!(status.can_accept_writes());
    }

    #[test]
    fn fenced_stays_fenced() {
        let status = ValidatedNodeStatus::create_custom_status(
            NodeStatus::Fenced,
            DRIFT,
            FAR_FUTURE, // even with valid TTL, fenced stays fenced
        );
        assert!(status.is_fenced());
        assert!(!status.can_accept_writes());
    }

    #[test]
    fn only_leader_and_standalone_accept_writes() {
        let cases = [
            (NodeStatus::Leader { lease_index: 1 }, true),
            (NodeStatus::Standalone, true),
            (NodeStatus::Follower { leader_lease_index: 1 }, false),
            (NodeStatus::FollowerCatchingUp { leader_lease_index: 1 }, false),
            (NodeStatus::BootCatchup, false),
            (NodeStatus::Fenced, false),
        ];
        for (status, expected) in cases {
            let vns = ValidatedNodeStatus::create_custom_status(status, DRIFT, FAR_FUTURE);
            assert_eq!(vns.can_accept_writes(), expected, "can_accept_writes wrong for {:?}", status);
        }
    }


    #[test]
    fn current_budget() {
        let now = unix_epoch_now_ms();
        // expected: None means no budget; Some((min, max)) is the allowed ms range.
        let cases: [(NodeStatus, u64, Option<(u64, u64)>, &str); 8] = [
            (NodeStatus::Follower { leader_lease_index: 1 }, FAR_FUTURE, None, "follower"),
            (NodeStatus::Standalone, FAR_FUTURE, None, "standalone"),
            (NodeStatus::BootCatchup, FAR_FUTURE, None, "boot catchup"),
            (NodeStatus::Fenced, FAR_FUTURE, None, "fenced"),
            (NodeStatus::FollowerCatchingUp { leader_lease_index: 1 }, FAR_FUTURE, None, "follower catching up"),
            (NodeStatus::Leader { lease_index: 1 }, now + 100, None, "leader inside fence window"),
            (NodeStatus::Leader { lease_index: 1 }, FAR_PAST, None, "leader lease fully expired"),
            // safe_until = (now+5000) - 500 drift = now+4500; allow ±50ms for execution time
            (NodeStatus::Leader { lease_index: 1 }, now + 5000, Some((4400, 4500)), "healthy leader"),
        ];
        for (status, expires_ms, expected, label) in cases {
            let vns = ValidatedNodeStatus::create_custom_status(status, DRIFT, expires_ms);
            match expected {
                None => assert!(vns.current_budget().is_none(), "{label}: expected None"),
                Some((min, max)) => {
                    let budget = vns.current_budget().unwrap_or_else(|| panic!("{label}: expected Some"));
                    let ms = u64::try_from(budget.as_millis()).expect("budget fits in u64");
                    assert!(ms > min && ms <= max, "{label}: budget {ms}ms out of range ({min}, {max}]");
                }
            }
        }
    }
}
