use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Information about a cluster member
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ClusterMember {
    /// Unique node identifier
    pub node_id: u128,

    /// Network address for client connections (e.g., "10.0.1.5:10000")
    pub client_address: String,

    /// Network address for inter-node connections (e.g., "10.0.1.5:10000")
    pub replication_address: String,
}

impl ClusterMember {
    pub fn new(node_id: u128, client_address: String, replication_address: String) -> Self {
        Self {
            node_id,
            client_address,
            replication_address
        }
    }
}

/// Cluster membership information
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ClusterMembership {
    pub members: Vec<ClusterMember>,
}

impl ClusterMembership {
    pub fn new(members: Vec<ClusterMember>) -> Self {
        Self { members }
    }

    /// Get all active members except the given node
    pub fn get_followers(&self, node_id: u128) -> Vec<&ClusterMember> {
        self.members
            .iter()
            .filter(|m| m.node_id != node_id)
            .collect()
    }

    /// Find a member by node ID
    pub fn get_node(&self, node_id: u128) -> Option<&ClusterMember> {
        self.members.iter().find(|m| m.node_id == node_id)
    }
}