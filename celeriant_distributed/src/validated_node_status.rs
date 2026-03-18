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
