use crate::heartbeat::now_ms;
use crate::node_status::NodeStatus;

/// NodeStatus with a TTL. If shard 0 doesn't refresh before `expires_at_ms`,
/// the effective status decays to Fenced.
#[derive(Clone, Copy, Debug)]
pub struct ValidatedNodeStatus {
    status: NodeStatus,
    expires_at_ms: u64,
}

impl ValidatedNodeStatus {

    pub fn fenced() -> Self {
        Self { status: NodeStatus::Fenced, expires_at_ms: 0 }
    }

    pub fn standalone() -> Self {
        Self { status: NodeStatus::Standalone, expires_at_ms: 0 }
    }

    pub fn boot_catchup() -> Self {
        Self { status: NodeStatus::BootCatchup, expires_at_ms: 0 }
    }

    pub fn new(status: NodeStatus, expires_at_ms: u64) -> Self {
        Self { status, expires_at_ms }
    }

    /// Returns the effective status. Fenced if expired.
    /// TTL-exempt: Standalone, BootCatchup, FollowerCatchingUp, FollowerCaughtUp.
    /// Catchup states are orchestrated by shard 0, which provides a fresh TTL on exit.
    pub fn effective(&self) -> NodeStatus {
        match self.status {
            NodeStatus::Standalone
            | NodeStatus::BootCatchup
            | NodeStatus::Fenced
            | NodeStatus::FollowerCatchingUp { .. }
            | NodeStatus::FollowerCaughtUp { .. } => self.status,
            _ if now_ms() > self.expires_at_ms => NodeStatus::Fenced,
            _ => self.status,
        }
    }

    /// Raw status without time check (for logging, debugging).
    pub fn raw(&self) -> NodeStatus {
        self.status
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }

    pub fn is_follower(&self) -> bool {
        self.effective().is_follower()
    }

    pub fn is_leader(&self) -> bool {
        self.effective().is_leader()
    }

    pub fn is_fenced(&self) -> bool {
        self.effective().is_fenced()
    }

    pub fn is_standalone(&self) -> bool {
        matches!(self.status, NodeStatus::Standalone)
    }

    pub fn is_catching_up(&self) -> bool {
        self.effective().is_catching_up()
    }

    pub fn can_accept_writes(&self) -> bool {
        self.is_leader() || self.is_standalone()
    }
}
