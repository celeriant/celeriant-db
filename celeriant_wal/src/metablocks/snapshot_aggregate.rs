use bincode::{Decode, Encode};
use crate::aggregate_key::{AggregateKey};

/// Quick metadata about an aggregate. Used for aggregate discovery,
/// the write path to track next indexes and read path to determine 
/// available data
#[derive(Debug, Clone, Encode, Decode)]
pub struct SnapshotAggregate {
    pub aggregate_key: AggregateKey,

    // For write path track indexes    
    pub last_event_index: u64,
    pub last_event_batch_index: u64,

    // Aggregates can get trimmed
    pub min_available_event_index: u64,
    pub min_available_event_batch_index: u64,

    // Filesystem-style metadata for aggregate discovery
    pub compressed_size_bytes: u64,
    pub uncompressed_size_bytes: u64,

    // Updated data comes from latest event batch, doesn't need snapshotting
    pub created_at: u64,
    pub created_by_client_id: u128,
    pub created_by_user_id: Option<u128>,
}

impl SnapshotAggregate {
    pub fn new(
        aggregate_key: AggregateKey,
        created_by_client_id: u128,
        created_by_user_id: Option<u128>,
        current_time_ms: u64,
    ) -> Self {
        Self {
            aggregate_key,
            last_event_index: 0,
            last_event_batch_index: 0,
            min_available_event_batch_index: 0,
            compressed_size_bytes: 0,
            uncompressed_size_bytes: 0,
            created_at: current_time_ms,
            min_available_event_index: 0,
            created_by_client_id,
            created_by_user_id,
        }
    }

    pub fn append_event_batches(
        &mut self,
        last_event_index: u64,
        last_event_batch_index: u64,
        additional_compressed_size_bytes: u64,
        additional_uncompressed_size_bytes: u64,
    ) {
        self.last_event_index = last_event_index;
        self.last_event_batch_index = last_event_batch_index;
        self.compressed_size_bytes = self.compressed_size_bytes.saturating_add(additional_compressed_size_bytes);
        self.uncompressed_size_bytes = self.uncompressed_size_bytes.saturating_add(additional_uncompressed_size_bytes);
    }

    pub fn trim_start(
        &mut self,
        min_available_event_batch_index: u64,
        saved_compressed_size_bytes: u64,
        saved_uncompressed_size_bytes: u64,
    ) {
        self.min_available_event_batch_index = min_available_event_batch_index;
        self.compressed_size_bytes = self.compressed_size_bytes.saturating_sub(saved_compressed_size_bytes);
        self.uncompressed_size_bytes = self.uncompressed_size_bytes.saturating_sub(saved_uncompressed_size_bytes);
    }
}
