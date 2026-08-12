use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::aggregate_type_key::AggregateTypeKey;
use crate::sbbf;
use crate::segment_summary::client_set::ClientSet;
use crate::segment_summary::segment_aggregate_entry::SegmentAggregateEntry;

pub const SUMMARY_PAYLOAD_MAX_BYTES: u64 = 4 * 1024 * 1024;

fn bloom_wire_size(bloom: &Option<Vec<u64>>) -> u64 {
    match bloom {
        Some(words) => 8 + 8 * words.len() as u64,
        None => 0,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct SegmentSummaryPayload {
    pub orgs: Vec<u128>,
    pub aggregate_types: Vec<AggregateTypeKey>,
    pub aggregates: Vec<SegmentAggregateEntry>,
    /// Whether the accumulator provably saw EVERY commit in the segment
    pub complete: bool,
    pub aggregate_bloom: Option<Vec<u64>>,
    pub client_bloom: Option<Vec<u64>>,
    pub schema_bloom: Option<Vec<u64>>,
}

impl SegmentSummaryPayload {
    /// Bincode fixed-int encoding: u64 length prefix per Vec (3 vecs) + 1-byte
    /// completeness flag + 1-byte tag per Option (3)
    pub const WIRE_OVERHEAD: u64 = 3 * 8 + 1 + 3;

    pub fn is_empty(&self) -> bool {
        self.aggregates.is_empty() && !self.schema_bloom.as_ref().is_some_and(|w| w.iter().any(|b| *b != 0))
    }

    pub fn schema_may_contain_hash(&self, hash: u64) -> bool {
        if !self.complete {
            return true;
        }
        match &self.schema_bloom {
            Some(words) => words.is_empty() || words.len() % 4 != 0 || sbbf::contains(words, hash),
            None => true,
        }
    }

    pub fn wire_size(&self) -> u64 {
        let entries: u64 = self.aggregates.iter().map(SegmentAggregateEntry::wire_size).sum();
        Self::WIRE_OVERHEAD
            + self.orgs.len() as u64 * 16
            + self.aggregate_types.len() as u64 * AggregateTypeKey::WIRE_SIZE_TOTAL as u64
            + entries
            + bloom_wire_size(&self.aggregate_bloom)
            + bloom_wire_size(&self.client_bloom)
            + bloom_wire_size(&self.schema_bloom)
    }

    /// Enforce `SUMMARY_PAYLOAD_MAX_BYTES` (already `u64`) by dropping per-aggregate
    /// client sets to `ClientSet::Unknown`, largest saving first, until the payload
    /// fits. Returns how many were dropped; 0 when already under the cap.
    ///
    /// Largest-first is policy, not tuning: every drop costs one aggregate its
    /// negative-lookup skip regardless of the set's size, so the goal is to shed the
    /// fewest sets per byte freed. Plain `aggregates` order would be worse than
    /// arbitrary — the vec is sorted by `(org_id, aggregate_type_id, aggregate_id)`
    /// and binary-searched by the read path, so the lowest org id would absorb every
    /// seal's degradation, forever.
    ///
    /// Entries are never dropped: listing correctness and segment skipping must not
    /// degrade. `Unknown` answers maybe-present, so a drop costs a scan and never a
    /// false absent — and if dropping every set still exceeds the cap, return anyway.
    ///
    /// `wire_size()` is O(n): call it ONCE, then subtract each dropped set's saving —
    /// `client_set.wire_size() - ClientSet::Unknown.wire_size()`, since the entry keeps
    /// paying for the discriminant — from a running total. Skip sets already `Unknown`:
    /// they save nothing and must not count toward the return value. Re-checking
    /// `wire_size()` per drop is O(n²) and stalls the executor.
    pub fn trim_out_client_sets(&mut self) -> usize {
        if self.is_empty() || self.wire_size() <= SUMMARY_PAYLOAD_MAX_BYTES {
            return 0;
        }

        let mut dropped_count = 0;
        let mut current_size = self.wire_size();
    
        // Collect indices of aggregates with non-Unknown client sets, sorted by wire size descending
        let mut aggregate_indices: Vec<(usize, u64)> = self
            .aggregates
            .iter()
            .enumerate()
            .filter_map(|(i, entry)| {
                if let ClientSet::Unknown = entry.client_set {
                    None
                } else {
                    Some((i, entry.client_set.wire_size()))
                }
            })
            .collect();
    
        aggregate_indices.sort_by_key(|&(_, size)| std::cmp::Reverse(size));
    
        for (index, _) in aggregate_indices {
            let entry = &mut self.aggregates[index];
            let old_size = entry.client_set.wire_size();
            let new_size = ClientSet::Unknown.wire_size();
            let saving = old_size - new_size;
        
            entry.client_set = ClientSet::Unknown;
            current_size -= saving;
            dropped_count += 1;
        
            if current_size <= SUMMARY_PAYLOAD_MAX_BYTES {
                break;
            }
        }
    
        dropped_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_summary::tests::{payload_with_schema_bloom, test_entry};

    fn encode(payload: &SegmentSummaryPayload) -> Vec<u8> {
        let cfg = bincode::config::standard().with_fixed_int_encoding().with_little_endian();
        bincode::encode_to_vec(payload, cfg).unwrap()
    }

    #[test]
    fn payload_bincode_roundtrip() {
        let payload = SegmentSummaryPayload {
            orgs: vec![1, 2],
            aggregate_types: vec![AggregateTypeKey::new(1, 10), AggregateTypeKey::new(2, 20)],
            aggregates: vec![
                SegmentAggregateEntry {
                    client_set: ClientSet::Exact(vec![3, 7, 11]),
                    ..test_entry(1, 10, 100)
                },
                SegmentAggregateEntry {
                    client_set: ClientSet::Bloom(vec![0xFF; 8]),
                    ..test_entry(2, 20, 200)
                },
            ],
            complete: false,
            aggregate_bloom: Some(vec![1, 2, 3, 4]),
            client_bloom: None,
            schema_bloom: Some(vec![5, 9, 0, 0]),
        };

        let cfg = bincode::config::standard().with_fixed_int_encoding().with_little_endian();
        let encoded = bincode::encode_to_vec(&payload, cfg).unwrap();
        let (decoded, _): (SegmentSummaryPayload, _) = bincode::decode_from_slice(&encoded, cfg).unwrap();

        assert_eq!(decoded.orgs, payload.orgs);
        assert_eq!(decoded.aggregate_types, payload.aggregate_types);
        assert_eq!(decoded.aggregates.len(), 2);
        assert_eq!(decoded.aggregates[0].newest_metablock_pos, 397_312);
        assert_eq!(decoded.aggregates[0].client_set, ClientSet::Exact(vec![3, 7, 11]));
        assert_eq!(decoded.aggregates[1].client_set, ClientSet::Bloom(vec![0xFF; 8]));
        assert!(!decoded.complete, "the incomplete taint must survive the wire");
        assert_eq!(decoded.aggregate_bloom, Some(vec![1, 2, 3, 4]));
        assert_eq!(decoded.client_bloom, None);
        assert_eq!(decoded.schema_bloom, Some(vec![5, 9, 0, 0]));
    }

    #[test]
    fn payload_wire_size_matches_serialized() {
        let payload = SegmentSummaryPayload {
            orgs: vec![1, 2],
            aggregate_types: vec![AggregateTypeKey::new(1, 10)],
            aggregates: vec![
                SegmentAggregateEntry { client_set: ClientSet::Exact(vec![5]), ..test_entry(1, 10, 100) },
                test_entry(1, 10, 101),
            ],
            complete: true,
            aggregate_bloom: Some(vec![0u64; 8]),
            client_bloom: Some(vec![0u64; 4]),
            schema_bloom: Some(vec![0u64; 4]),
        };
        assert_eq!(encode(&payload).len() as u64, payload.wire_size());

        let empty = SegmentSummaryPayload {
            orgs: vec![],
            aggregate_types: vec![],
            aggregates: vec![],
            complete: true,
            aggregate_bloom: None,
            client_bloom: None,
            schema_bloom: None,
        };
        assert_eq!(encode(&empty).len() as u64, empty.wire_size());
    }

    #[test]
    fn schema_consult_degrades_on_incomplete_none_and_malformed() {
        assert!(payload_with_schema_bloom(true, None).schema_may_contain_hash(42), "None = no information");
        assert!(payload_with_schema_bloom(true, Some(vec![])).schema_may_contain_hash(42));
        assert!(
            payload_with_schema_bloom(true, Some(vec![0; 3])).schema_may_contain_hash(42),
            "non-block-multiple word count must not claim absence"
        );
        assert!(
            payload_with_schema_bloom(false, Some(vec![0u64; 4])).schema_may_contain_hash(42),
            "an incomplete summary must never authorize a skip, even with an empty bloom"
        );
    }

    #[test]
    fn trim_out_client_sets_noop_under_cap() {
        let mut payload = SegmentSummaryPayload {
            orgs: vec![1],
            aggregate_types: vec![AggregateTypeKey::new(1, 2)],
            aggregates: vec![SegmentAggregateEntry {
                client_set: ClientSet::Exact(vec![1, 2, 3]),
                ..test_entry(1, 2, 3)
            }],
            complete: true,
            aggregate_bloom: None,
            client_bloom: None,
            schema_bloom: None,
        };
        assert_eq!(payload.trim_out_client_sets(), 0);
        assert_eq!(payload.aggregates[0].client_set, ClientSet::Exact(vec![1, 2, 3]));
    }

    #[test]
    fn trim_out_client_sets_drops_largest_sets_first_never_entries() {
        // Two big bloom sets + one small exact set; sized so dropping ONLY the
        // two big sets brings the payload under the cap.
        let big_words = (SUMMARY_PAYLOAD_MAX_BYTES / 8 / 2) as usize; // ~half the cap each
        let mut payload = SegmentSummaryPayload {
            orgs: vec![1],
            aggregate_types: vec![AggregateTypeKey::new(1, 2)],
            aggregates: vec![
                SegmentAggregateEntry { client_set: ClientSet::Bloom(vec![0; big_words + 4]), ..test_entry(1, 2, 1) },
                SegmentAggregateEntry { client_set: ClientSet::Exact(vec![1, 2]), ..test_entry(1, 2, 2) },
                SegmentAggregateEntry { client_set: ClientSet::Bloom(vec![0; big_words]), ..test_entry(1, 2, 3) },
            ],
            complete: true,
            aggregate_bloom: None,
            client_bloom: None,
            schema_bloom: None,
        };
        assert!(payload.wire_size() > SUMMARY_PAYLOAD_MAX_BYTES);

        let dropped = payload.trim_out_client_sets();

        assert_eq!(dropped, 1, "dropping the single largest set suffices");
        assert_eq!(payload.aggregates.len(), 3, "entries are never dropped");
        assert_eq!(payload.aggregates[0].client_set, ClientSet::Unknown, "largest set dropped first");
        assert_eq!(payload.aggregates[1].client_set, ClientSet::Exact(vec![1, 2]), "small set survives");
        assert!(matches!(payload.aggregates[2].client_set, ClientSet::Bloom(_)), "second big set fits once the first is gone");
        assert!(payload.wire_size() <= SUMMARY_PAYLOAD_MAX_BYTES);
        assert_eq!(
            payload.aggregates[0].newest_metablock_pos, 397_312,
            "dropping a client set must not touch the tip index"
        );
    }
}
