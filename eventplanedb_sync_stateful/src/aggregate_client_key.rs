use std::fmt;
use std::hash::{Hash, Hasher};

/// High-performance composite key for client event index tracking
/// Optimized for hashing and comparison operations
#[derive(Clone, PartialEq, Eq)]
pub struct AggregateClientKey {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub client_id: u128,
    // Pre-computed hash for better performance
    hash: u64,
}

impl AggregateClientKey {
    pub fn new(org_id: u128, aggregate_type_id: u128, aggregate_id: u128, client_id: u128) -> Self {
        let hash = Self::compute_hash(org_id, aggregate_type_id, aggregate_id, client_id);
        Self {
            org_id,
            aggregate_type_id,
            aggregate_id,
            client_id,
            hash,
        }
    }

    fn compute_hash(
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
    ) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        org_id.hash(&mut hasher);
        aggregate_type_id.hash(&mut hasher);
        aggregate_id.hash(&mut hasher);
        client_id.hash(&mut hasher);
        hasher.finish()
    }
}

impl Hash for AggregateClientKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use pre-computed hash for better performance
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for AggregateClientKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateClientKey")
            .field("org_id", &self.org_id)
            .field("aggregate_type_id", &self.aggregate_type_id)
            .field("aggregate_id", &self.aggregate_id)
            .field("client_id", &self.client_id)
            .finish()
    }
}
