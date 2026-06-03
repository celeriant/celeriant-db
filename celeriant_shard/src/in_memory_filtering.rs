use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_wal::{datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch, metablocks::{metablock::Metablock, metablock_event_batch::EventTypesKind, metablock_kind::MetablockKind}};
use crate::bloom::bloom_filter_cache::event_type_hash;

pub fn apply_event_filters(event_batch: &mut DatablockAggregateEventBatch, read_filters: &ReadFilters) {
    // Final event type filtering (bloom filter might have false positives)
    if let Some(event_types) = read_filters.include_event_types.as_deref() && !event_types.is_empty() {
        event_batch
            .events
            .retain(|event| event_types.contains(&event.event_type_major));
    }

    // Final filtering for local_index
    if let Some(min_event_seq) = read_filters.min_event_seq {
        event_batch
            .events
            .retain(|event| event.event_seq >= min_event_seq);
    }

    if let Some(max_event_seq) = read_filters.max_event_seq {
        event_batch
            .events
            .retain(|event| event.event_seq <= max_event_seq);
    }

    // Final filtering for event_time
    if let Some(min_event_time) = read_filters.min_event_timestamp {
        event_batch
            .events
            .retain(|event| event.event_timestamp >= min_event_time);
    }

    if let Some(max_event_time) = read_filters.max_event_timestamp {
        event_batch
            .events
            .retain(|event| event.event_timestamp <= max_event_time);
    }

    // Final filtering for event index
    if let Some(min_client_seq) = read_filters.min_client_seq {
        event_batch
            .events
            .retain(|event| event.client_seq >= min_client_seq);
    }

    if let Some(max_client_seq) = read_filters.max_client_seq {
        event_batch
            .events
            .retain(|event| event.client_seq <= max_client_seq);
    }
}

pub fn is_include_batch(metablock: &Metablock, filters: &ReadFilters) -> bool {
    let metadata = match &metablock.wal_metablock_type {
        MetablockKind::EventBatchMetadata(meta) => meta,
        _ => return false, // Not an EventBatch metablock
    };

    if metadata.aggregate_version < filters.from_aggregate_version {
        return false;
    }

    if filters.to_aggregate_version.map_or(false, |to_aggregate_version| {
        metadata.aggregate_version > to_aggregate_version
    }) {
        return false;
    }

    if filters
        .min_server_timestamp
        .map_or(false, |before_server_time| {
            metablock.server_timestamp < before_server_time
        })
    {
        return false;
    }

    if filters
        .max_server_timestamp
        .map_or(false, |after_server_time| {
            metablock.server_timestamp > after_server_time
        })
    {
        return false;
    }

    if filters
        .exclude_client_id
        .map_or(false, |exclude_client_id| {
            metadata.client_id == exclude_client_id
        })
    {
        return false;
    }

    if filters
        .include_client_id
        .map_or(false, |include_client_id| {
            metadata.client_id != include_client_id
        })
    {
        return false;
    }

    if filters
        .exclude_user_id
        .map_or(false, |exclude_user_id| metadata.user_id == Some(exclude_user_id))
    {
        return false;
    }

    if filters
        .include_user_id
        .map_or(false, |include_user_id| metadata.user_id != Some(include_user_id))
    {
        return false;
    }

    if filters.min_client_seq.map_or(false, |min_index| {
        metadata.max_client_seq < min_index
    }) {
        return false;
    }

    if filters.max_client_seq.map_or(false, |max_index| {
        metadata.min_client_seq > max_index
    }) {
        return false;
    }

    if filters
        .min_event_timestamp
        .map_or(false, |min_time| metadata.max_event_timestamp < min_time)
    {
        return false;
    }

    if filters
        .max_event_timestamp
        .map_or(false, |max_time| metadata.min_event_timestamp > max_time)
    {
        return false;
    }

    if filters
        .min_event_seq
        .map_or(false, |min_index| metadata.max_event_seq < min_index)
    {
        return false;
    }

    if filters
        .max_event_seq
        .map_or(false, |max_index| metadata.min_event_seq > max_index)
    {
        return false;
    }

    if let Some(include_event_types) = &filters.include_event_types {
        if !include_event_types.is_empty() && !check_event_types_match(&metadata.event_types_data, &include_event_types) {
            return false;
        }
    } 

    true
}

fn check_event_types_match(event_types_data: &EventTypesKind, include_event_types: &[u64]) -> bool {
    match event_types_data {
        EventTypesKind::Direct(event_types) => {
            // Check if any of the required types are in the direct array
            if event_types.len() < include_event_types.len() {
                event_types
                    .iter()
                    .any(|&batch_type| include_event_types.contains(&batch_type))
            } else {
                include_event_types
                    .iter()
                    .any(|&include_event_type| event_types.contains(&include_event_type))
            }
        }
        EventTypesKind::Bloom(bloom_bytes) => {
            // Query the owned split-block bloom directly (no reconstruction).
            include_event_types
                .iter()
                .any(|&include_event_type| {
                    celeriant_wal::sbbf::contains(bloom_bytes, event_type_hash(include_event_type))
                })
        }
    }
}

pub fn trim_end_if_exceeds_max_bytes(
    metablocks: &mut Vec<Metablock>,
    read_filters: &ReadFilters,
    max_bytes: Option<usize>,
) -> Option<u64> {
    // Only keep batches where include is true
    metablocks
        .retain(|metablock| is_include_batch(metablock, read_filters));

    // If no max_bytes limit is specified, we don't need to trim
    let max_bytes = match max_bytes {
        Some(limit) => limit as u64,
        None => return None,
    };

    // If after filtering we don't have any batches, return None
    if metablocks.is_empty() {
        return None;
    }

    // Calculate cumulative compressed size
    let mut cumulative_size: u64 = 0;
    let mut cut_index: Option<usize> = None;

    // Batches are sorted by aggregate_version (ascending)
    for (index, batch) in metablocks.iter().enumerate() {
        cumulative_size += batch.uncompressed_size;

        // If we exceed the max_bytes limit, store this index as the cut point
        if cumulative_size > max_bytes {
            cut_index = Some(index);
            break;
        }
    }

    // If we need to trim
    if let Some(index) = cut_index {
        // Get the aggregate_version of the first batch we're trimming
        let next_aggregate_version = if index < metablocks.len() {
            match &metablocks[index].wal_metablock_type {
                MetablockKind::EventBatchMetadata(m) => Some(m.aggregate_version),
                _ => None,
            }
        } else {
            None
        };

        // Keep only the batches that fit within the max_bytes limit
        metablocks.truncate(index);

        next_aggregate_version
    } else {
        // No trimming needed, all batches fit within the limit
        None
    }
}

#[cfg(test)]
mod tests {
    use celeriant_wal::constants::BLOOM_BYTES;
    use celeriant_wal::{aggregate_key::AggregateKey, constants::GENESIS_HASH, datablocks::datablock_aggregate_event::DatablockAggregateEvent, metablocks::{datablock_block_ref::DatablockBlockRef, datablock_storage_kind::DatablockStorageKind, metablock::Metablock, metablock_event_batch::MetablockEventBatch}};

    use super::*;

    fn mk_metadata(
        aggregate_version: u64,
        server_timestamp: u64,
        client_id: u128,
        user_id: u128,
        min_cidx: u64,
        max_cidx: u64,
        min_ts: u64,
        max_ts: u64,
        min_eidx: u64,
        max_eidx: u64,
        uncompressed_size: u64,
        event_types: &[u64; 4],
    ) -> Metablock {
        Metablock {
            wal_seq: 0,
            server_timestamp,
            lease_epoch: 0,
            node_id: 0,
            uncompressed_size,
            compressed_size: 0,
            datablock_version: 0,
            datablock_compression_type: 0,
            datablock: DatablockStorageKind::Block(DatablockBlockRef {
                crc32c: 0,
            }),
            datablock_position: 0,
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: AggregateKey::new(1, 1, 1),
                event_types_data: EventTypesKind::Direct(*event_types),
                aggregate_version,
                trimmed_below_version: 0,
                client_id,
                user_id: Some(user_id),
                min_client_seq: min_cidx,
                max_client_seq: max_cidx,
                min_event_timestamp: min_ts,
                max_event_timestamp: max_ts,
                min_event_seq: min_eidx,
                max_event_seq: max_eidx,
            }),
            previous_tip_hash: GENESIS_HASH,
        }
    }

    fn mk_metadata_bloom(
        aggregate_version: u64,
        server_timestamp: u64,
        client_id: u128,
        user_id: u128,
        min_cidx: u64,
        max_cidx: u64,
        min_ts: u64,
        max_ts: u64,
        min_eidx: u64,
        max_eidx: u64,
        uncompressed_size: u64,
        types_to_insert: &[u64],
    ) -> Metablock {
        let mut bloom_bytes = [0u64; BLOOM_BYTES / 8];
        for &t in types_to_insert {
            celeriant_wal::sbbf::insert(&mut bloom_bytes, event_type_hash(t));
        }

        Metablock {
            wal_seq: 0,
            server_timestamp,
            lease_epoch: 0,
            node_id: 0,
            uncompressed_size,
            compressed_size: 0,
            datablock_version: 0,
            datablock_compression_type: 0,
            datablock: DatablockStorageKind::Block(DatablockBlockRef {
                crc32c: 0,
            }),
            datablock_position: 0,
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: AggregateKey::new(1, 1, 1),
                event_types_data: EventTypesKind::Bloom(bloom_bytes),
                aggregate_version,
                trimmed_below_version: 0,
                client_id,
                user_id: Some(user_id),
                min_client_seq: min_cidx,
                max_client_seq: max_cidx,
                min_event_timestamp: min_ts,
                max_event_timestamp: max_ts,
                min_event_seq: min_eidx,
                max_event_seq: max_eidx,
            }),
            previous_tip_hash: GENESIS_HASH,
        }
    }

    fn mk_event(
        event_type_major: u64,
        event_seq: u64,
        client_seq: u64,
        event_timestamp: u64,
    ) -> DatablockAggregateEvent {
        DatablockAggregateEvent {
            event_type_major,
            event_seq: event_seq,
            client_seq,
            event_timestamp,
            ..Default::default()
        }
    }

    #[test]
    fn is_include_batch_inclusive_aggregate_version_bounds() {
        let meta = mk_metadata(
            10, // bx
            1000,
            1,
            2,
            1,
            5,
            100,
            200,
            10,
            20,
            123,
            &[1, 2, 0, 0],
        );

        // from <= bx is included (inclusive lower bound)
        let filters = ReadFilters::new(10);
        assert!(is_include_batch(&meta, &filters));

        // from > bx excluded
        let filters = ReadFilters::new(11);
        assert!(!is_include_batch(&meta, &filters));

        // to >= bx included (inclusive upper bound)
        let filters = ReadFilters::new(1).to_aggregate_version(10);
        assert!(is_include_batch(&meta, &filters));

        // to < bx excluded
        let filters = ReadFilters::new(1).to_aggregate_version(9);
        assert!(!is_include_batch(&meta, &filters));
    }

    #[test]
    fn is_include_batch_inclusive_server_timestamp_bounds() {
        let base = mk_metadata(5, 1000, 1, 2, 1, 5, 100, 200, 10, 20, 1, &[1, 0, 0, 0]);

        // min_server_timestamp: keep when server_timestamp >= min
        let filters = ReadFilters::new(1).min_server_timestamp(1000);
        assert!(is_include_batch(&base, &filters)); // equal allowed
        let filters = ReadFilters::new(1).min_server_timestamp(1001);
        assert!(!is_include_batch(&base, &filters)); // 1000 < 1001 excluded

        // max_server_timestamp: keep when server_timestamp <= max
        let filters = ReadFilters::new(1).max_server_timestamp(1000);
        assert!(is_include_batch(&base, &filters)); // equal allowed
        let filters = ReadFilters::new(1).max_server_timestamp(999);
        assert!(!is_include_batch(&base, &filters)); // 1000 > 999 excluded
    }

    #[test]
    fn is_include_batch_include_exclude_client_and_user() {
        let meta = mk_metadata(1, 1, 123, 456, 0, 0, 0, 0, 0, 0, 1, &[0, 0, 0, 0]);

        // Exclude matching client
        let filters = ReadFilters::new(1).exclude_client_id(123);
        assert!(!is_include_batch(&meta, &filters));

        // Include non-matching client -> excluded
        let filters = ReadFilters::new(1).include_client_id(124);
        assert!(!is_include_batch(&meta, &filters));

        // Include matching client -> included
        let filters = ReadFilters::new(1).include_client_id(123);
        assert!(is_include_batch(&meta, &filters));

        // Exclude matching user
        let filters = ReadFilters::new(1).exclude_user_id(456);
        assert!(!is_include_batch(&meta, &filters));

        // Include non-matching user -> excluded
        let filters = ReadFilters::new(1).include_user_id(999);
        assert!(!is_include_batch(&meta, &filters));

        // Include matching user -> included
        let filters = ReadFilters::new(1).include_user_id(456);
        assert!(is_include_batch(&meta, &filters));
    }

    #[test]
    fn is_include_batch_inclusive_ranges_for_client_index_event_time_and_event_seq() {
        // Batch with ranges:
        // client_seq: [10, 20]
        // event_timestamp: [1_000, 2_000]
        // event_seq: [100, 200]
        let meta = mk_metadata(1, 0, 0, 0, 10, 20, 1000, 2000, 100, 200, 1, &[0, 0, 0, 0]);

        // min_client_seq: keep when max >= min (inclusive)
        let filters = ReadFilters::new(1).min_client_seq(20);
        assert!(is_include_batch(&meta, &filters)); // edge ok
        let filters = ReadFilters::new(1).min_client_seq(21);
        assert!(!is_include_batch(&meta, &filters)); // 20 < 21 -> no overlap

        // max_client_seq: keep when min <= max (inclusive)
        let filters = ReadFilters::new(1).max_client_seq(10);
        assert!(is_include_batch(&meta, &filters));
        let filters = ReadFilters::new(1).max_client_seq(9);
        assert!(!is_include_batch(&meta, &filters));

        // min_event_timestamp: keep when batch.max_event_timestamp >= min (inclusive)
        let filters = ReadFilters::new(1).min_event_timestamp(2000);
        assert!(is_include_batch(&meta, &filters));
        let filters = ReadFilters::new(1).min_event_timestamp(2001);
        assert!(!is_include_batch(&meta, &filters));

        // max_event_timestamp: keep when batch.min_event_timestamp <= max (inclusive)
        let filters = ReadFilters::new(1).max_event_timestamp(1000);
        assert!(is_include_batch(&meta, &filters));
        let filters = ReadFilters::new(1).max_event_timestamp(999);
        assert!(!is_include_batch(&meta, &filters));

        // min_event_seq: keep when batch.max_event_seq >= min (inclusive)
        let filters = ReadFilters::new(1).min_event_seq(200);
        assert!(is_include_batch(&meta, &filters));
        let filters = ReadFilters::new(1).min_event_seq(201);
        assert!(!is_include_batch(&meta, &filters));

        // max_event_seq: keep when batch.min_event_seq <= max (inclusive)
        let filters = ReadFilters::new(1).max_event_seq(100);
        assert!(is_include_batch(&meta, &filters));
        let filters = ReadFilters::new(1).max_event_seq(99);
        assert!(!is_include_batch(&meta, &filters));
    }

    #[test]
    fn is_include_batch_include_event_types_direct() {
        // Batch types are {2, 4}
        let meta = mk_metadata(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, &[2, 4, 0, 0]);

        // Any overlap -> included
        let filters = ReadFilters::new(1).include_event_types(vec![4, 7, 9]);
        assert!(is_include_batch(&meta, &filters));

        // No overlap -> excluded
        let filters = ReadFilters::new(1).include_event_types(vec![7, 9]);
        assert!(!is_include_batch(&meta, &filters));
    }

    #[test]
    fn is_include_batch_include_event_types_bloom() {
        // Batch types are {2, 4}
        let meta = mk_metadata_bloom(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, &[2, 4, 0, 0, 7, 8, 11]);

        // Any overlap -> included
        let filters = ReadFilters::new(1).include_event_types(vec![4, 7, 9]);
        assert!(is_include_batch(&meta, &filters));

        // No overlap -> excluded
        let filters = ReadFilters::new(1).include_event_types(vec![66, 9]);
        assert!(!is_include_batch(&meta, &filters));
    }

    #[test]
    fn apply_event_filters_keeps_only_matching_events_all_fields_inclusive() {
        // Build a batch with varied events
        // Types: 1, 2, 3
        // event_seq: 9, 10, 11, 12
        // client_seq: 99, 100, 101, 102
        // event_timestamp: 999, 1000, 1001, 1002
        let mut batch = DatablockAggregateEventBatch {
            aggregate_version: 1,
            events: vec![
                mk_event(1, 9, 99, 999),
                mk_event(2, 10, 100, 1000),
                mk_event(2, 11, 101, 1001),
                mk_event(3, 12, 102, 1002),
            ],
        };

        let filters = ReadFilters::new(1)
            .include_event_types(vec![2, 3])
            .min_event_seq(10)
            .max_event_seq(12)
            .min_event_timestamp(1000)
            .max_event_timestamp(1002)
            .min_client_seq(100)
            .max_client_seq(102);

        apply_event_filters(&mut batch, &filters);

        // The first event is filtered out by every numeric min (it's all one less).
        // Remaining 3 meet the inclusive edges; types 2 and 3 are allowed.
        let kept: Vec<(u64, u64, u64, u64)> = batch
            .events
            .iter()
            .map(|e| (e.event_type_major, e.event_seq, e.client_seq, e.event_timestamp))
            .collect();

        assert_eq!(
            kept,
            vec![(2, 10, 100, 1000), (2, 11, 101, 1001), (3, 12, 102, 1002)]
        );
    }

    #[test]
    fn apply_event_filters_type_whitelist_only() {
        let mut batch = DatablockAggregateEventBatch {
            aggregate_version: 1,
            events: vec![mk_event(1, 1, 1, 1), mk_event(2, 2, 2, 2), mk_event(3, 3, 3, 3)],
        };

        let filters = ReadFilters::new(1).include_event_types(vec![2]);
        apply_event_filters(&mut batch, &filters);

        let kept_types: Vec<u64> = batch.events.iter().map(|e| e.event_type_major).collect();
        assert_eq!(kept_types, vec![2]);
    }

    fn get_aggregate_version(meta: &Metablock) -> &MetablockEventBatch {
        match &meta.wal_metablock_type {
            MetablockKind::EventBatchMetadata(m) => m,
            _ => panic!("Expected EventBatchMetadata"),
        }
    }

    #[test]
    fn trim_end_if_exceeds_max_bytes_truncates_and_returns_next_index() {
        // Three batches: sizes 100, 200, 300; total 600
        // max_bytes=250 -> only first fits; next index should be second's bx
        let m1 = mk_metadata(10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, &[0, 0, 0, 0]);
        let m2 = mk_metadata(11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 200, &[0, 0, 0, 0]);
        let m3 = mk_metadata(12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 300, &[0, 0, 0, 0]);

        let mut v = vec![m1, m2, m3];

        // No additional filtering (include all)
        let filters = ReadFilters::new(1);

        let next = trim_end_if_exceeds_max_bytes(&mut v, &filters, Some(250));
        assert_eq!(next, Some(11));
        assert_eq!(v.len(), 1);
        assert_eq!(get_aggregate_version(&v[0]).aggregate_version, 10);
    }

    #[test]
    fn trim_end_if_exceeds_max_bytes_all_fit_returns_none() {
        let m1 = mk_metadata(5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, &[0, 0, 0, 0]);
        let m2 = mk_metadata(6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 150, &[0, 0, 0, 0]);

        let mut v = vec![m1, m2];
        let filters = ReadFilters::new(1);

        let next = trim_end_if_exceeds_max_bytes(&mut v, &filters, Some(300));
        assert_eq!(next, None);
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn trim_end_if_exceeds_max_bytes_too_small_errors() {
        // Single batch size=200; max_bytes=100 -> error
        let m1 = mk_metadata(99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 200, &[0, 0, 0, 0]);
        let mut v = vec![m1];
        let filters = ReadFilters::new(1);

        let next = trim_end_if_exceeds_max_bytes(&mut v, &filters, Some(100));
        assert_eq!(next, Some(99));
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn include_event_types_empty_treated_as_no_filter() {
        let meta = mk_metadata(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, &[2, 4, 0, 0]);
        let filters = ReadFilters::new(1).include_event_types(vec![]);
        assert!(is_include_batch(&meta, &filters));

        let mut batch = DatablockAggregateEventBatch {
            aggregate_version: 1,
            events: vec![mk_event(1, 1, 1, 1), mk_event(2, 2, 2, 2)],
        };
        apply_event_filters(&mut batch, &filters);
        // No filtering took place
        assert_eq!(batch.events.len(), 2);
    }

    #[test]
    fn trim_end_if_exceeds_max_bytes_filters_out_all_returns_none() {
        let m1 = mk_metadata(10, 1000, 1, 2, 0, 0, 0, 0, 0, 0, 100, &[2, 0, 0, 0]);
        let mut v = vec![m1];

        // Filter to a different client_id so batch gets filtered out
        let filters = ReadFilters::new(1).include_client_id(999);
        let next = trim_end_if_exceeds_max_bytes(&mut v, &filters, Some(1000));

        assert!(v.is_empty());
        assert_eq!(next, None);
    }

    #[test]
    fn include_and_exclude_client_conflict_exclude_wins() {
        let meta = mk_metadata(1, 0, 123, 0, 0, 0, 0, 0, 0, 0, 1, &[0, 0, 0, 0]);
        let filters = ReadFilters::new(1)
            .include_client_id(123)
            .exclude_client_id(123);
        assert!(!is_include_batch(&meta, &filters));
    }

    /// `include_event_types=[t]` where t is absent. Returns true iff the batch is
    /// (wrongly) kept — i.e. a wasted datablock read.
    fn batch_kept_for_type(meta: &Metablock, t: u64) -> bool {
        is_include_batch(meta, &ReadFilters::new(1).include_event_types(vec![t]))
    }

    #[test]
    fn event_type_direct_filter_is_exact_no_wasted_reads() {
        // <=4 distinct types -> Direct -> exact, no false positives.
        let meta = mk_metadata(10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, &[10, 20, 30, 40]);
        assert!(batch_kept_for_type(&meta, 20), "present type must be kept");
        for absent in [0u64, 11, 25, 99, 1_000_000, u64::MAX] {
            assert!(!batch_kept_for_type(&meta, absent), "Direct must exactly reject absent type {absent} (zero wasted reads)");
        }
    }

    /// Fraction of absent-type probes that the per-batch bloom (wrongly) keeps,
    /// for a batch holding `num_types` distinct event types.
    fn event_type_bloom_fp(num_types: u64, probes: u64) -> f64 {
        let present: Vec<u64> = (1000..1000 + num_types).collect();
        let meta = mk_metadata_bloom(10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, &present);
        let mut kept = 0u64;
        for p in 0..probes {
            // Absent probe space, disjoint from [1000, 1000+num_types).
            if batch_kept_for_type(&meta, 5_000_000 + p) {
                kept += 1;
            }
        }
        let rate = kept as f64 / probes as f64;
        eprintln!("[event-type-bloom-fp] distinct_types={num_types} probes={probes} wasted_reads={kept} fp_rate={:.3}%", rate * 100.0);
        rate
    }

    #[test]
    fn event_type_bloom_fp_grows_with_distinct_types_per_batch() {
        // Few types: bloom is nearly perfect -> negligible wasted datablock reads.
        assert!(event_type_bloom_fp(8, 20_000) < 0.01, "8 types should be <1% FP");
        // The 256-bit/4-hash per-batch bloom degrades fast once a batch mixes
        // dozens of event types — each FP is a datablock read that returns nothing.
        let fp50 = event_type_bloom_fp(50, 20_000);
        let fp100 = event_type_bloom_fp(100, 20_000);
        assert!(fp50 > 0.02, "50 types should show material FP (got {:.3})", fp50);
        assert!(fp100 > fp50, "FP must grow with distinct types/batch");
        assert!(fp100 > 0.20, "100 types/batch should be heavily saturated (got {:.3})", fp100);
    }
}