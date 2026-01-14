//! Cluster membership tracking.
//!
//! The membership file (`cluster/membership.bin`) tracks which nodes are in the cluster
//! and their current state.

use bincode::{Decode, Encode};

/// Information about a single node in the cluster.
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct NodeInfo {
    /// Unique identifier for this node
    pub node_id: u128,
    /// Address for client connections
    pub client_address: String,
    /// Port for replication connections
    pub replication_address: String,
}

impl NodeInfo {
    pub fn new(
        node_id: u128,
        client_address: String,
        replication_address: String,
    ) -> Self {
        Self {
            node_id,
            client_address,
            replication_address,
        }
    }
}

/// Cluster membership state stored in S3 at `cluster/membership.bin`.
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct Membership {
    pub version: u64,
    pub leader: Option<NodeInfo>,
    pub follower: Option<NodeInfo>,
}

impl Membership {
    /// Create initial membership with no nodes.
    pub fn empty() -> Self {
        Self {
            version: 0,
            leader: None,
            follower: None,
        }
    }

    /// Update membership, incrementing version.
    pub fn update(&self, leader: Option<NodeInfo>, follower: Option<NodeInfo>) -> Self {
        Self {
            version: self.version + 1,
            leader,
            follower,
        }
    }

    /// Check if we have both leader and follower in active state.
    pub fn is_fully_replicated(&self) -> bool {
        self.leader.is_some() && self.follower.is_some()
    }

    pub fn has_leader(&self) -> bool {
        self.leader.is_some()
    }
    
    pub fn has_follower(&self) -> bool {
        self.follower.is_some()
    }

    /// Get the node that is not the specified node_id.
    pub fn peer_of(&self, node_id: u128) -> Option<&NodeInfo> {
        if self.leader.as_ref().map(|n| n.node_id) == Some(node_id) {
            self.follower.as_ref()
        } else if self.follower.as_ref().map(|n| n.node_id) == Some(node_id) {
            self.leader.as_ref()
        } else {
            None
        }
    }
    
    pub fn new() -> Self {
        Self { version: 1, leader: None, follower: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership_lifecycle() {
        let leader = NodeInfo::new(1, "a:10000".into(), "a:10001".into());
        let mut membership = Membership::new();
        membership.leader = Some(leader);

        assert!(!membership.is_fully_replicated());
        assert_eq!(membership.version, 1);

        let follower = NodeInfo::new(2, "b:10000".into(), "b:10001".into());

        membership = membership.update(None, Some(follower));
        assert_eq!(membership.version, 2);
        // Leader still Joining, so not fully replicated
        assert!(!membership.is_fully_replicated());
    }
}
