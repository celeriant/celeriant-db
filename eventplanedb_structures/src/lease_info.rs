use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};


/// Cached lease with its S3 ETag for conditional updates.
#[derive(Clone)]
pub struct CachedLease {
    pub lease_info: LeaseInfo,
    pub etag: String,
}

/// Information about a lease for a node
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct LeaseInfo {
    /// Monotonically increasing lease index
    pub lease_index: u64,

    /// Node that holds the lease
    pub node_id: u128,

    /// Unix timestamp in milliseconds when the lease expires
    pub lease_expires_on: u64,

    /// Unix timestamp in milliseconds when the lease was granted
    pub lease_started_on: u64
}

impl LeaseInfo {
    pub fn new(
        lease_index: u64,
        node_id: u128,
        lease_expires_on: u64,
        lease_started_on: u64
    ) -> Self {
        Self {
            lease_index,
            node_id,
            lease_expires_on,
            lease_started_on,
        }
    }

    /// Check if this lease is expired at the given timestamp
    pub fn is_expired(&self, current_time_ms: u64) -> bool {
        current_time_ms >= self.lease_expires_on
    }

    /// Check if this lease is about to expire (within given margin)
    pub fn is_expiring_soon(&self, current_time_ms: u64, margin_ms: u64) -> bool {
        current_time_ms + margin_ms >= self.lease_expires_on
    }

    pub fn can_renew_as_leader(&self, node_id: u128, current_time_ms: u64, margin_ms: u64) -> bool {
        self.node_id == node_id && self.is_expiring_soon(current_time_ms, margin_ms) ||
        self.node_id != node_id && self.is_expired(current_time_ms)
    }
    
    pub fn is_active_leader(&self, node_id: u128, current_time_ms: u64) -> bool {
        self.node_id == node_id && !self.is_expired(current_time_ms)
    }
    
    pub fn is_leader(&self, node_id: u128) -> bool {
        self.node_id == node_id
    }
}

impl Default for LeaseInfo {
    fn default() -> Self {
        Self {
            lease_index: 0,
            node_id: 0,
            lease_expires_on: 0,
            lease_started_on: 0,
        }
    }
}