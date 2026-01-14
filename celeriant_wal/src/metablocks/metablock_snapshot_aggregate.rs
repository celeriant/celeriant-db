use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use crate::aggregate_key::{AggregateKey};
use serde::{Deserialize, Serialize};

/// Quick metadata about an aggregate. Used for aggregate discovery,
/// the write path to track next indexes and read path to determine 
/// available data
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct MetablockSnapshotAggregate {
    pub aggregate_key: AggregateKey,

    /// Global last written index for aggregate
    pub last_wal_index: u64,

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

impl MetablockSnapshotAggregate {
    // Wire format layout (bincode fixed-int encoding)
    // Update these if field order or types change!

    // AggregateKey contains 3 x u64 fields (org_id, aggregate_type_id, aggregate_id)
    const WIRE_SIZE_LAST_WAL_INDEX: usize = 8;
    const WIRE_SIZE_LAST_EVENT_INDEX: usize = 8;
    const WIRE_SIZE_LAST_EVENT_BATCH_INDEX: usize = 8;
    const WIRE_SIZE_MIN_AVAILABLE_EVENT_INDEX: usize = 8;
    const WIRE_SIZE_MIN_AVAILABLE_EVENT_BATCH_INDEX: usize = 8;
    const WIRE_SIZE_COMPRESSED_SIZE_BYTES: usize = 8;
    const WIRE_SIZE_UNCOMPRESSED_SIZE_BYTES: usize = 8;
    const WIRE_SIZE_CREATED_AT: usize = 8;
    const WIRE_SIZE_CREATED_BY_CLIENT_ID: usize = 16;
    // Option<u128>: 1 byte discriminant + 16 bytes value
    const WIRE_SIZE_CREATED_BY_USER_ID: usize = 1 + 16;

    pub const OFFSET_AGGREGATE_KEY: usize = 0;

    pub const OFFSET_LAST_WAL_INDEX: usize = 
        Self::OFFSET_AGGREGATE_KEY + AggregateKey::WIRE_SIZE_TOTAL;

    pub const OFFSET_LAST_EVENT_INDEX: usize = 
        Self::OFFSET_LAST_WAL_INDEX + Self::WIRE_SIZE_LAST_WAL_INDEX;

    pub const OFFSET_LAST_EVENT_BATCH_INDEX: usize = 
        Self::OFFSET_LAST_EVENT_INDEX + Self::WIRE_SIZE_LAST_EVENT_INDEX;

    pub const OFFSET_MIN_AVAILABLE_EVENT_INDEX: usize = 
        Self::OFFSET_LAST_EVENT_BATCH_INDEX + Self::WIRE_SIZE_LAST_EVENT_BATCH_INDEX;

    pub const OFFSET_MIN_AVAILABLE_EVENT_BATCH_INDEX: usize = 
        Self::OFFSET_MIN_AVAILABLE_EVENT_INDEX + Self::WIRE_SIZE_MIN_AVAILABLE_EVENT_INDEX;

    pub const OFFSET_COMPRESSED_SIZE_BYTES: usize = 
        Self::OFFSET_MIN_AVAILABLE_EVENT_BATCH_INDEX + Self::WIRE_SIZE_MIN_AVAILABLE_EVENT_BATCH_INDEX;

    pub const OFFSET_UNCOMPRESSED_SIZE_BYTES: usize = 
        Self::OFFSET_COMPRESSED_SIZE_BYTES + Self::WIRE_SIZE_COMPRESSED_SIZE_BYTES;

    pub const OFFSET_CREATED_AT: usize = 
        Self::OFFSET_UNCOMPRESSED_SIZE_BYTES + Self::WIRE_SIZE_UNCOMPRESSED_SIZE_BYTES;

    pub const OFFSET_CREATED_BY_CLIENT_ID: usize = 
        Self::OFFSET_CREATED_AT + Self::WIRE_SIZE_CREATED_AT;

    pub const OFFSET_CREATED_BY_USER_ID: usize = 
        Self::OFFSET_CREATED_BY_CLIENT_ID + Self::WIRE_SIZE_CREATED_BY_CLIENT_ID;

    /// Total wire size of MetablockSnapshotAggregate
    pub const WIRE_SIZE_TOTAL: usize = 
        Self::OFFSET_CREATED_BY_USER_ID + Self::WIRE_SIZE_CREATED_BY_USER_ID;

    pub fn append_event_batches(
        &mut self,
        wal_index: u64,
        last_event_index: u64,
        last_event_batch_index: u64,
        additional_compressed_size_bytes: u64,
        additional_uncompressed_size_bytes: u64,
    ) {
        self.last_wal_index = wal_index;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_aggregate() -> MetablockSnapshotAggregate {
        MetablockSnapshotAggregate {
            aggregate_key: AggregateKey::default(),
            last_wal_index: 3,
            last_event_index: 2,
            last_event_batch_index: 1,
            min_available_event_index: 0,
            min_available_event_batch_index: 0,
            compressed_size_bytes: 1000,
            uncompressed_size_bytes: 5000,
            created_at: 0,
            created_by_client_id: 0,
            created_by_user_id: None,
        }
    }

    #[test]
    fn append_event_batches_updates_indexes_and_sizes() {
        let mut aggregate = create_test_aggregate();

        aggregate.append_event_batches(10, 5, 2, 500, 2000);

        assert_eq!(aggregate.last_wal_index, 10);
        assert_eq!(aggregate.last_event_index, 5);
        assert_eq!(aggregate.last_event_batch_index, 2);
        assert_eq!(aggregate.compressed_size_bytes, 1500);
        assert_eq!(aggregate.uncompressed_size_bytes, 7000);
    }

    #[test]
    fn append_event_batches_saturates_on_overflow() {
        let mut aggregate = create_test_aggregate();
        aggregate.compressed_size_bytes = u64::MAX - 100;
        aggregate.uncompressed_size_bytes = u64::MAX - 100;

        aggregate.append_event_batches(1, 1, 1, 500, 500);

        assert_eq!(aggregate.compressed_size_bytes, u64::MAX);
        assert_eq!(aggregate.uncompressed_size_bytes, u64::MAX);
    }

    #[test]
    fn trim_start_updates_min_index_and_reduces_sizes() {
        let mut aggregate = create_test_aggregate();

        aggregate.trim_start(5, 300, 1500);

        assert_eq!(aggregate.min_available_event_batch_index, 5);
        assert_eq!(aggregate.compressed_size_bytes, 700);
        assert_eq!(aggregate.uncompressed_size_bytes, 3500);
        assert_eq!(aggregate.last_wal_index, 3);
        assert_eq!(aggregate.last_event_index, 2);
        assert_eq!(aggregate.last_event_batch_index, 1);
    }

    #[test]
    fn trim_start_saturates_on_underflow() {
        let mut aggregate = create_test_aggregate();

        aggregate.trim_start(5, 2000, 10000);

        assert_eq!(aggregate.min_available_event_batch_index, 5);
        assert_eq!(aggregate.compressed_size_bytes, 0);
        assert_eq!(aggregate.uncompressed_size_bytes, 0);
    }
}