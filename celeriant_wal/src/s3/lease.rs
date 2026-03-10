use bincode::{Decode, Encode};
use serde::{Serialize, Deserialize};

/// Lease state stored in S3 at `cluster/lease.json`.
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lease {
    #[serde(with = "super::serde_uuid")]
    pub leader_node_id: u128,
    pub lease_index: u64,
    pub acquired_at_ms: u64,
    pub expires_at_ms: u64,
}

impl Lease {
    pub fn new_initial(
        leader_node_id: u128,
        now_millis: u64,
        duration_millis: u64,
    ) -> Self {
        Self {
            leader_node_id,
            lease_index: 1,
            acquired_at_ms: now_millis,
            expires_at_ms: now_millis + duration_millis,
        }
    }

    pub fn promote(
        &self,
        new_leader_node_id: u128,
        now_millis: u64,
        duration_millis: u64,
    ) -> Self {
        Self {
            leader_node_id: new_leader_node_id,
            lease_index: self.lease_index + 1,
            acquired_at_ms: now_millis,
            expires_at_ms: now_millis + duration_millis,
        }
    }

    pub fn is_expired(&self, now_millis: u64) -> bool {
        now_millis >= self.expires_at_ms
    }

    /// Time remaining until expiry (0 if already expired).
    pub fn remaining_millis(&self, now_millis: u64) -> u64 {
        self.expires_at_ms.saturating_sub(now_millis)
    }

    /// Check if this lease supersedes another lease held by a specific node.
    pub fn supersedes(&self, our_lease_index: u64, our_node_id: u128) -> bool {
        self.lease_index > our_lease_index && self.leader_node_id != our_node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_lifecycle() {
        let lease = Lease::new_initial(42, 1000, 5000);

        assert_eq!(lease.lease_index, 1);
        assert!(!lease.is_expired(3000));
        assert!(lease.is_expired(6001));
        assert_eq!(lease.remaining_millis(3000), 3000);

        let promoted = lease.promote(99, 7000, 5000);
        assert_eq!(promoted.lease_index, 2);
        assert_eq!(promoted.leader_node_id, 99);
    }

    #[test]
    fn test_supersedes_same_index_different_node() {
        let lease = Lease::new_initial(99, 1000, 5000);
        assert!(!lease.supersedes(1, 42));
    }

    #[test]
    fn test_supersedes_higher_index_different_node() {
        let lease = Lease::new_initial(99, 1000, 5000);
        let promoted = lease.promote(999, 2000, 5000);
        assert!(promoted.supersedes(1, 42));
    }

    #[test]
    fn test_supersedes_equal_everything() {
        let lease = Lease::new_initial(42, 1000, 5000);
        assert!(!lease.supersedes(1, 42));
    }

    #[test]
    fn test_supersedes_lower_index() {
        let lease = Lease::new_initial(99, 1000, 5000);
        assert!(!lease.supersedes(5, 42));
    }

    #[test]
    fn test_supersedes_higher_index_same_node() {
        let lease = Lease::new_initial(42, 1000, 5000);
        let promoted = lease.promote(42, 2000, 5000);
        assert!(!promoted.supersedes(1, 42));
    }

    #[test]
    fn test_supersedes_zero_lease_index() {
        let lease = Lease::new_initial(99, 1000, 5000);
        assert!(lease.supersedes(0, 42));
    }

    #[test]
    fn test_supersedes_multiple_promotions() {
        let lease = Lease::new_initial(1, 1000, 5000);
        let p2 = lease.promote(2, 2000, 5000);
        let p3 = p2.promote(3, 3000, 5000);
        let p4 = p3.promote(4, 4000, 5000);

        // Node 1 at index 1 is superseded by all later promotions
        assert!(p2.supersedes(1, 1));
        assert!(p3.supersedes(1, 1));
        assert!(p4.supersedes(1, 1));

        // Node 2 at index 2 is superseded by p3 and p4
        assert!(p3.supersedes(2, 2));
        assert!(p4.supersedes(2, 2));

        // Node 3 at index 3 is superseded by p4
        assert!(p4.supersedes(3, 3));

        // Node 4 at index 4 is not superseded by itself
        assert!(!p4.supersedes(4, 4));
    }
}
