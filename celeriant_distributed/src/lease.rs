//! Lease management for leader election.
//!
//! The lease file (`cluster/lease.bin`) contains the current leader information.
//! Only the leader can write; it uses S3 conditional puts with ETag for atomic updates.

use bincode::{Decode, Encode};

/// Lease state stored in S3 at `cluster/lease.bin`.
///
/// The lease_index acts as a fencing token - it monotonically increases with each
/// new leader. Followers reject replication from leaders with stale lease_index.
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct Lease {
    /// Unique identifier of the current leader node
    pub leader_node_id: u128,
    /// Monotonically increasing fencing token (previous lease_index + 1)
    pub lease_index: u64,
    /// Unix timestamp (millis) when lease was acquired
    pub acquired_at_ms: u64,
    /// Unix timestamp (millis) when lease expires
    pub expires_at_ms: u64,
    /// Address where leader accepts client connections (e.g., "192.168.1.10:10000")
    pub leader_client_address: String,
    /// Address where leader accepts replication connections
    pub leader_replication_address: String,
}

impl Lease {
    /// Create a new lease for initial cluster bootstrap.
    pub fn new_initial(
        leader_node_id: u128,
        now_millis: u64,
        duration_millis: u64,
        leader_client_address: String,
        leader_replication_address: String,
    ) -> Self {
        Self {
            leader_node_id,
            lease_index: 1,
            acquired_at_ms: now_millis,
            expires_at_ms: now_millis + duration_millis,
            leader_client_address,
            leader_replication_address,
        }
    }

    /// Create a renewed lease with same leader, extended expiry.
    pub fn renew(&self, now_millis: u64, duration_millis: u64) -> Self {
        Self {
            expires_at_ms: now_millis + duration_millis,
            ..self.clone()
        }
    }

    /// Create a new lease for a different leader (promotion).
    pub fn promote(
        &self,
        new_leader_node_id: u128,
        now_millis: u64,
        duration_millis: u64,
        leader_client_address: String,
        leader_replication_address: String,
    ) -> Self {
        Self {
            leader_node_id: new_leader_node_id,
            lease_index: self.lease_index + 1,
            acquired_at_ms: now_millis,
            expires_at_ms: now_millis + duration_millis,
            leader_client_address,
            leader_replication_address,
        }
    }

    /// Check if lease is expired at the given time.
    pub fn is_expired(&self, now_millis: u64) -> bool {
        now_millis >= self.expires_at_ms
    }

    /// Time remaining until expiry (0 if already expired).
    pub fn remaining_millis(&self, now_millis: u64) -> u64 {
        self.expires_at_ms.saturating_sub(now_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_lifecycle() {
        let lease = Lease::new_initial(
            42,
            1000,
            5000,
            "localhost:10000".into(),
            "localhost:10001".into(),
        );

        assert_eq!(lease.lease_index, 1);
        assert!(!lease.is_expired(3000));
        assert!(lease.is_expired(6001));
        assert_eq!(lease.remaining_millis(3000), 3000);

        let renewed = lease.renew(4000, 5000);
        assert_eq!(renewed.lease_index, 1);
        assert_eq!(renewed.expires_at_ms, 9000);

        let promoted = lease.promote(
            99,
            7000,
            5000,
            "other:10000".into(),
            "other:10001".into(),
        );
        assert_eq!(promoted.lease_index, 2);
        assert_eq!(promoted.leader_node_id, 99);
    }
}
