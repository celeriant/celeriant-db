use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::segment_summary::client_set::ClientSet;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct SegmentAggregateEntry {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub is_deleted: bool,
    pub event_batch_count: u64,
    pub last_aggregate_version: u64,
    pub min_aggregate_version: u64,
    pub last_server_timestamp: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub newest_metablock_pos: u64,
    pub client_set: ClientSet,
}

impl SegmentAggregateEntry {
    /// Fixed serialized portion: 3×u128 (48) + bool (1) + 7×u64 (56) = 105 bytes.
    /// The client set makes entries VARIABLE-size; see `wire_size`.
    pub const WIRE_SIZE_FIXED: u64 = 105;

    pub fn wire_size(&self) -> u64 {
        Self::WIRE_SIZE_FIXED + self.client_set.wire_size()
    }

    pub fn new(org_id: u128, aggregate_type_id: u128, aggregate_id: u128) -> Self {
        Self {
            org_id,
            aggregate_type_id,
            aggregate_id,
            is_deleted: false,
            event_batch_count: 0,
            last_aggregate_version: 0,
            min_aggregate_version: 0,
            last_server_timestamp: 0,
            compressed_size: 0,
            uncompressed_size: 0,
            newest_metablock_pos: 0,
            client_set: ClientSet::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_summary::tests::test_entry;

    #[test]
    fn wire_size_fixed_matches_serialized_unknown_entry() {
        let cfg = bincode::config::standard().with_fixed_int_encoding().with_little_endian();
        let entry = test_entry(1, 2, 3);
        let encoded = bincode::encode_to_vec(&entry, cfg).unwrap();
        // Unknown client set adds only the 4-byte enum discriminant.
        assert_eq!(encoded.len() as u64, SegmentAggregateEntry::WIRE_SIZE_FIXED + 4);
        assert_eq!(encoded.len() as u64, entry.wire_size());
    }

    #[test]
    fn wire_size_estimate_matches_serialized_for_variable_entries() {
        for client_set in [
            ClientSet::Unknown,
            ClientSet::Exact(vec![1, 2, 3]),
            ClientSet::Bloom(vec![0u64; 32]),
        ] {
            let cfg = bincode::config::standard().with_fixed_int_encoding().with_little_endian();
            let entry = SegmentAggregateEntry { client_set, ..test_entry(1, 2, 3) };
            let encoded = bincode::encode_to_vec(&entry, cfg).unwrap();
            assert_eq!(encoded.len() as u64, entry.wire_size(), "{:?}", entry.client_set);
        }
    }
}