use std::collections::HashMap;
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

/// Periodic snapshotting of each aggregate into the WAL to avoid replaying the entire WAL on startup
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct SnapshotAggregate {
    /// Used for idempotent producers, track the last accepted
    /// event_batch_index for each client_id
    pub client_event_indexes: HashMap<u128, u64>,
}

impl SnapshotAggregate {
    pub fn new(
    ) -> Self {
        Self {
            client_event_indexes: HashMap::new(),
        }
    }
}