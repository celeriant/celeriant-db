use std::time::Instant;

use crate::node_status::NodeStatus;

/// NodeStatus with a TTL. If shard 0 doesn't refresh before `valid_until`,
/// the effective status decays to Fenced.
#[derive(Clone, Copy)]
pub struct ValidatedNodeStatus {
    status: NodeStatus,
    valid_until: Instant,
}

impl ValidatedNodeStatus {

    pub fn fenced() -> Self {
        Self { status: NodeStatus::Fenced, valid_until: Instant::now() }
    }

    pub fn standalone() -> Self {
        Self { status: NodeStatus::Standalone, valid_until: Instant::now() }
    }

    pub fn boot_catchup() -> Self {
        Self { status: NodeStatus::BootCatchup, valid_until: Instant::now() }
    }

    pub fn new(status: NodeStatus, valid_until: Instant) -> Self {
        Self { status, valid_until }
    }

    /// Returns the effective status. Fenced if expired.
    /// Standalone and BootCatchup are TTL-exempt (no shard 0 refresh loop yet).
    pub fn effective(&self) -> NodeStatus {
        match self.status {
            NodeStatus::Standalone | NodeStatus::BootCatchup => self.status,
            _ if Instant::now() > self.valid_until => NodeStatus::Fenced,
            _ => self.status,
        }
    }

    /// Raw status without time check (for logging, debugging).
    pub fn raw(&self) -> NodeStatus {
        self.status
    }

    pub fn valid_until(&self) -> Instant {
        self.valid_until
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
