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

    pub fn standalone() -> Self {
        Self { status: NodeStatus::Standalone, valid_until: Instant::now() }
    }

    pub fn new(status: NodeStatus, valid_until: Instant) -> Self {
        Self { status, valid_until }
    }

    /// Returns the effective status. Fenced if expired.
    pub fn effective(&self) -> NodeStatus {
        match self.status {
            NodeStatus::Standalone => NodeStatus::Standalone, // no TTL
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
        matches!(self.effective(), NodeStatus::Follower { .. })
    }
    
    pub fn is_leader(&self) -> bool {
        matches!(self.effective(), NodeStatus::Leader { .. })
    }
    
    pub fn is_fenced(&self) -> bool {
        matches!(self.effective(), NodeStatus::Fenced { .. })
    }
    
    pub fn is_standalone(&self) -> bool {
        matches!(self.status, NodeStatus::Standalone { .. })
    }

    pub fn can_accept_writes(&self) -> bool {
        self.is_leader() || self.is_standalone()
    }
}