use std::collections::HashSet;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Information about a lease for an aggregate
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct LeaseInfo {
    /// Monotonically increasing lease index
    pub lease_index: u64,

    /// Node that holds the lease
    pub node_id: u128,

    /// Unix timestamp in milliseconds when the lease expires
    pub lease_expiry_ms: u64,

    /// List of nodes that can become the leader for this aggregate
    pub available_leaders: HashSet<u128>
}

impl LeaseInfo {
    pub fn new(
        lease_index: u64,
        node_id: u128,
        lease_expiry_ms: u64,
        available_leaders: HashSet<u128>
    ) -> Self {
        Self {
            lease_index,
            node_id,
            lease_expiry_ms,
            available_leaders,
        }
    }

    /// Check if this lease is expired at the given timestamp
    pub fn is_expired(&self, current_time_ms: u64) -> bool {
        current_time_ms >= self.lease_expiry_ms
    }

    /// Check if this lease is about to expire (within given margin)
    pub fn is_expiring_soon(&self, current_time_ms: u64, margin_ms: u64) -> bool {
        current_time_ms + margin_ms >= self.lease_expiry_ms
    }

    pub fn can_renew_as_leader(&self, node_id: u128, current_time_ms: u64, margin_ms: u64) -> bool {
        self.node_id == node_id && self.is_expiring_soon(current_time_ms, margin_ms) ||
        self.node_id != node_id && self.is_expired(current_time_ms)
    }
    
    pub fn is_leader(&self, node_id: u128) -> bool {
        self.node_id == node_id
    }
    
    pub fn can_be_leader(&self, node_id: u128) -> bool {
        self.available_leaders.contains(&node_id)
    }
}