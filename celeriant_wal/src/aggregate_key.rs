use std::fmt;
use std::hash::{Hash, Hasher};

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// High-performance composite key for client event index tracking
/// Optimized for hashing and comparison operations
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct AggregateKey {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    // Pre-computed hash for better performance
    hash: u64,
}

impl AggregateKey {
    pub fn new(org_id: u128, aggregate_type_id: u128, aggregate_id: u128) -> Self {
        let hash = Self::compute_hash(org_id, aggregate_type_id, aggregate_id);
        Self {
            org_id,
            aggregate_type_id,
            aggregate_id,
            hash,
        }
    }

    fn compute_hash(org_id: u128, aggregate_type_id: u128, aggregate_id: u128) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        org_id.hash(&mut hasher);
        aggregate_type_id.hash(&mut hasher);
        aggregate_id.hash(&mut hasher);
        hasher.finish()
    }
}

impl Hash for AggregateKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use pre-computed hash for better performance
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for AggregateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateKey")
            .field("org_id", &self.org_id)
            .field("aggregate_type_id", &self.aggregate_type_id)
            .field("aggregate_id", &self.aggregate_id)
            .finish()
    }
}
