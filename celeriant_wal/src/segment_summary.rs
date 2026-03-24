use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::aggregate_type_key::AggregateTypeKey;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct SegmentSummaryPayload {
    pub orgs: Vec<u128>,
    pub aggregate_types: Vec<AggregateTypeKey>,
    pub aggregates: Vec<SegmentAggregateEntry>,
}

impl SegmentSummaryPayload {
    /// Bincode fixed-int encoding uses a u64 length prefix per Vec (3 vecs × 8 bytes)
    pub const WIRE_OVERHEAD: u64 = 3 * 8;

    pub fn is_empty(&self) -> bool {
        self.aggregates.is_empty()
    }
}

/// On-disk segment summary, written as a sidecar file at rotation time.
/// Not a WAL entry — no wal_index, no hash chain participation.
#[derive(Debug, Clone, Encode, Decode)]
pub struct SegmentSummaryBlock {
    pub payload: SegmentSummaryPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct SegmentAggregateEntry {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub is_deleted: bool,
    pub event_batch_count: u64,
    pub last_event_batch_index: u64,
    pub min_event_batch_index: u64,
    pub last_server_timestamp: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

impl SegmentAggregateEntry {
    /// Serialized wire size: 3×u128 (48) + bool (1) + 6×u64 (48) = 97 bytes
    pub const WIRE_SIZE: u64 = 97;

    pub fn new(org_id: u128, aggregate_type_id: u128, aggregate_id: u128) -> Self {
        Self {
            org_id,
            aggregate_type_id,
            aggregate_id,
            is_deleted: false,
            event_batch_count: 0,
            last_event_batch_index: 0,
            min_event_batch_index: 0,
            last_server_timestamp: 0,
            compressed_size: 0,
            uncompressed_size: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate_type_key::AggregateTypeKey;

    fn test_entry(org: u128, atype: u128, aid: u128) -> SegmentAggregateEntry {
        SegmentAggregateEntry {
            org_id: org,
            aggregate_type_id: atype,
            aggregate_id: aid,
            is_deleted: false,
            event_batch_count: 5,
            last_event_batch_index: 10,
            min_event_batch_index: 1,
            last_server_timestamp: 999,
            compressed_size: 512,
            uncompressed_size: 1024,
        }
    }

    #[test]
    fn payload_bincode_roundtrip() {
        let payload = SegmentSummaryPayload {
            orgs: vec![1, 2],
            aggregate_types: vec![AggregateTypeKey::new(1, 10), AggregateTypeKey::new(2, 20)],
            aggregates: vec![test_entry(1, 10, 100), test_entry(2, 20, 200)],
        };

        let cfg = bincode::config::standard().with_fixed_int_encoding().with_little_endian();
        let encoded = bincode::encode_to_vec(&payload, cfg).unwrap();
        let (decoded, _): (SegmentSummaryPayload, _) = bincode::decode_from_slice(&encoded, cfg).unwrap();

        assert_eq!(decoded.orgs, payload.orgs);
        assert_eq!(decoded.aggregate_types, payload.aggregate_types);
        assert_eq!(decoded.aggregates.len(), 2);
        assert_eq!(decoded.aggregates[0].org_id, 1);
        assert_eq!(decoded.aggregates[1].event_batch_count, 5);
    }

    #[test]
    fn wire_size_matches_serialized() {
        let cfg = bincode::config::standard().with_fixed_int_encoding().with_little_endian();
        let entry = test_entry(1, 2, 3);
        let encoded = bincode::encode_to_vec(&entry, cfg).unwrap();
        assert_eq!(encoded.len() as u64, SegmentAggregateEntry::WIRE_SIZE);
    }

    #[test]
    fn payload_is_empty_checks_aggregates() {
        let empty = SegmentSummaryPayload { orgs: vec![], aggregate_types: vec![], aggregates: vec![] };
        assert!(empty.is_empty());

        let non_empty = SegmentSummaryPayload {
            orgs: vec![1],
            aggregate_types: vec![AggregateTypeKey::new(1, 2)],
            aggregates: vec![test_entry(1, 2, 3)],
        };
        assert!(!non_empty.is_empty());
    }
}
