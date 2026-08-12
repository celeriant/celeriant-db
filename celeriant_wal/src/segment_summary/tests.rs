//! Shared fixtures for the per-file test modules, plus the tests whose contract
//! genuinely spans two files (accumulator produces a bloom, payload consults it).

use std::collections::HashSet;

use crate::aggregate_type_key::AggregateTypeKey;
use crate::segment_summary::client_set::ClientSet;
use crate::segment_summary::schema_hash_accumulator::{SchemaHashAccumulator, SCHEMA_ACCUMULATOR_MAX_HASHES, SCHEMA_BLOOM_MAX_BYTES};
use crate::segment_summary::segment_aggregate_entry::SegmentAggregateEntry;
use crate::segment_summary::segment_summary_payload::SegmentSummaryPayload;

pub(super) fn test_entry(org: u128, atype: u128, aid: u128) -> SegmentAggregateEntry {
    SegmentAggregateEntry {
        org_id: org,
        aggregate_type_id: atype,
        aggregate_id: aid,
        is_deleted: false,
        event_batch_count: 5,
        last_aggregate_version: 10,
        min_aggregate_version: 1,
        last_server_timestamp: 999,
        compressed_size: 512,
        uncompressed_size: 1024,
        newest_metablock_pos: 397_312,
        client_set: ClientSet::Unknown,
    }
}

pub(super) fn hashes(n: u64) -> HashSet<u64> {
    (0..n).map(|i| xxhash_rust::xxh3::xxh3_64(&i.to_le_bytes())).collect()
}

pub(super) fn payload_with_schema_bloom(complete: bool, schema_bloom: Option<Vec<u64>>) -> SegmentSummaryPayload {
    SegmentSummaryPayload {
        orgs: vec![],
        aggregate_types: vec![],
        aggregates: vec![],
        complete,
        aggregate_bloom: None,
        client_bloom: None,
        schema_bloom,
    }
}

#[test]
fn payload_is_empty_checks_aggregates_and_schemas() {
    let empty = SegmentSummaryPayload {
        orgs: vec![],
        aggregate_types: vec![],
        aggregates: vec![],
        complete: true,
        aggregate_bloom: None,
        client_bloom: None,
        // All-zero schema bloom attests nothing: still an empty payload.
        schema_bloom: Some(vec![0u64; 4]),
    };
    assert!(empty.is_empty());

    let non_empty = SegmentSummaryPayload {
        orgs: vec![1],
        aggregate_types: vec![AggregateTypeKey::new(1, 2)],
        aggregates: vec![test_entry(1, 2, 3)],
        complete: true,
        aggregate_bloom: None,
        client_bloom: None,
        schema_bloom: None,
    };
    assert!(!non_empty.is_empty());

    // A schema-only segment still has a sidecar worth writing.
    let mut acc = SchemaHashAccumulator::default();
    acc.insert(42);
    let schema_only = SegmentSummaryPayload { schema_bloom: acc.to_schema_bloom(true), ..empty };
    assert!(!schema_only.is_empty());
}

#[test]
fn empty_schema_bloom_answers_universal_absence_under_complete() {
    let acc = SchemaHashAccumulator::default();
    let bloom = acc.to_schema_bloom(true);
    let words = bloom.clone().unwrap();
    assert_eq!(words.len() * 8, 32, "zero schemas persist as one all-zero SBBF block");
    assert!(words.iter().all(|w| *w == 0));

    let payload = payload_with_schema_bloom(true, bloom);
    assert!(!payload.schema_may_contain_hash(0), "empty bloom answers definite absence for every key");
    assert!(!payload.schema_may_contain_hash(u64::MAX));
}

#[test]
fn schema_bloom_contains_every_inserted_hash_and_answers_absence() {
    let h = hashes(64);
    let mut acc = SchemaHashAccumulator::default();
    for hash in &h {
        acc.insert(*hash);
    }
    let payload = payload_with_schema_bloom(true, acc.to_schema_bloom(true));
    for hash in &h {
        assert!(payload.schema_may_contain_hash(*hash), "no false absent allowed");
        assert!(acc.may_contain(*hash));
    }
    assert!(!payload.schema_may_contain_hash(0xDEAD_BEEF), "a non-member answers definite absence");
    // 64 keys × 10 bits = 640 bits = 80 bytes → next multiple of 32 = 96 bytes.
    assert_eq!(payload.schema_bloom.as_ref().unwrap().len() * 8, 96, "sized from true cardinality");
}

#[test]
fn schema_accumulator_overflow_degrades_to_saturating_bloom_never_none() {
    let h = hashes(SCHEMA_ACCUMULATOR_MAX_HASHES as u64 + 10);
    let mut acc = SchemaHashAccumulator::default();
    for hash in &h {
        acc.insert(*hash);
    }
    let bloom = acc.to_schema_bloom(true).expect("volume overflow must persist a bloom, not None");
    assert_eq!(bloom.len() * 8, SCHEMA_BLOOM_MAX_BYTES, "overflow pins the max-size bloom");
    let payload = payload_with_schema_bloom(true, Some(bloom));
    for hash in &h {
        assert!(payload.schema_may_contain_hash(*hash), "no false absent allowed across the overflow boundary");
        assert!(acc.may_contain(*hash));
    }
}
