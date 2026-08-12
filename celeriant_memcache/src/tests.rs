use std::collections::HashMap;

use crate::cache_path::CachePath;
use crate::cached_schema::{CachedSchema, CachedValidator, UniqueSchemaKeys};
use crate::mem_snapshot_aggregate::{AggregateStatus, MemSnapshotAggregate};
use crate::pending_commit_data::PendingCommitData;
use crate::shard_log_queue_item::ShardLogQueueItem;
use crate::cached_schema::Validate;
use crate::shard_mem_cache::{ClientSeqStatus, ShardMemCache};
use celeriant_distributed::node_status::NodeStatus;
use celeriant_rotating_log::log_segment_file::log_segment_file_metadata::LogSegmentFileMetadata;
use celeriant_wal::aggregate_client_key::AggregateClientKey;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::aggregate_type_key::AggregateTypeKey;
use celeriant_wal::constants::{GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES, MINIBATCH_SIZE_BYTES};
use celeriant_wal::metablocks::datablock_inline_data::DatablockInlineData;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_event_batch::{EventTypesKind, MetablockEventBatch};
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_wal::metablocks::metablock_soft_delete::MetablockSoftDelete;
use celeriant_wal::metablocks::metablock_soft_trim::MetablockSoftTrim;
use celeriant_wal::schema_key::SchemaKey;
use celeriant_wal::segment_summary::client_set::ClientSet;
use celeriant_wal::shard_log_header::{HeaderCursor, ShardLogHeader};

// ── Helpers ──

struct StubValidator;
impl Validate for StubValidator {
    fn validate(&self, _: &[u8]) -> Result<(), String> { Ok(()) }
}

fn cache() -> ShardMemCache<StubValidator> {
    cache_with(64 * 1024, 64 * 1024, 64 * 1024, 1024 * 1024)
}

fn cache_with(
    recent_write_bytes: u64,
    agg_write_snap_bytes: u64,
    agg_client_snap_bytes: u64,
    internode_max_request_size: u64,
) -> ShardMemCache<StubValidator> {
    ShardMemCache::new(
        recent_write_bytes,
        agg_write_snap_bytes,
        agg_client_snap_bytes,
        4 * 1024 * 1024, // schema_cache_bytes
        2 * 1024 * 1024, // negative_lookup_cache_bytes
        internode_max_request_size,
    )
}

fn agg(org: u128, atype: u128, id: u128) -> AggregateKey {
    AggregateKey::new(org, atype, id)
}

fn client_key(aggregate_key: &AggregateKey, client_id: u128) -> AggregateClientKey {
    AggregateClientKey::new(aggregate_key.clone(), client_id)
}

fn test_metablock(aggregate_key: AggregateKey, aggregate_version: u64, max_event_seq: u64, client_id: u128, wal_seq: u64) -> Metablock {
    Metablock {
        wal_seq,
        server_timestamp: 1000,
        lease_epoch: 1,
        node_id: 1,
        uncompressed_size: 128,
        compressed_size: 64,
        datablock_version: 1,
        datablock_compression_type: 0,
        previous_tip_hash: GENESIS_HASH,
        datablock_position: 0,
        previous_aggregate_metablock_pos: 0,
        wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
            aggregate_key,
            aggregate_version,
            trimmed_below_version: 1,
            min_client_seq: 1,
            max_client_seq: max_event_seq,
            min_event_timestamp: 100,
            max_event_timestamp: 200,
            min_event_seq: 1,
            max_event_seq,
            client_id,
            user_id: None,
            event_types_data: EventTypesKind::Direct([1, 0, 0, 0]),
        }),
        datablock: DatablockStorageKind::Inline(DatablockInlineData {
            minibatch: [0u8; MINIBATCH_SIZE_BYTES],
        }),
    }
}

fn test_queue_item(aggregate_key: AggregateKey, aggregate_version: u64, max_event_seq: u64, client_id: u128) -> ShardLogQueueItem {
    let metablock = test_metablock(aggregate_key, aggregate_version, max_event_seq, client_id, 0);
    ShardLogQueueItem::new(None, None, metablock)
}

fn test_event_batch(aggregate_key: AggregateKey, aggregate_version: u64, max_event_seq: u64) -> MetablockEventBatch {
    MetablockEventBatch {
        aggregate_key,
        aggregate_version,
        trimmed_below_version: 1,
        min_client_seq: 1,
        max_client_seq: max_event_seq,
        min_event_timestamp: 100,
        max_event_timestamp: 200,
        min_event_seq: 1,
        max_event_seq,
        client_id: 1,
        user_id: None,
        event_types_data: EventTypesKind::Direct([1, 0, 0, 0]),
    }
}

/// Add a write to the queue and return the queue item's event sequences for assertions
fn queue_write(cache: &mut ShardMemCache<StubValidator>, key: &AggregateKey, event_seq: u64, aggregate_version: u64, client_id: u128, client_seq: u64) {
    let item = test_queue_item(key.clone(), aggregate_version, event_seq, client_id);
    cache.add_to_pending_append_queue(key, event_seq, aggregate_version, 1, client_id, client_seq, item);
}

fn test_pending_commit_data() -> PendingCommitData {
    let cursor = HeaderCursor {
        metablocks_position: HEADER_BLOCK_SIZE_BYTES as u64,
        datablocks_position: 4 * 1024 * 1024 - HEADER_BLOCK_SIZE_BYTES as u64,
        wal_seq: 0,
        tip_hash: GENESIS_HASH,
    };
    let header = ShardLogHeader {
        write: cursor.clone(),
        last_received_replication_wal_seq: 0,
        last_self_acked_wal_seq: 0,
        read: cursor,
    };
    PendingCommitData {
        log_metadata: LogSegmentFileMetadata::new(1, 4 * 1024 * 1024, None, &header, true),
        pending_queue: vec![],
    }
}

/// Take a sync snapshot and commit it as standalone (both read+write caches updated)
fn sync_and_commit_standalone(cache: &mut ShardMemCache<StubValidator>) {
    let snapshot = cache.take_sync_positions_snapshot();
    cache.commit_sync_positions_snapshot(NodeStatus::Standalone, snapshot);
}

/// Take a sync snapshot and commit it as leader (only write cache updated)
fn sync_and_commit_leader(cache: &mut ShardMemCache<StubValidator>) {
    let snapshot = cache.take_sync_positions_snapshot();
    cache.commit_sync_positions_snapshot(NodeStatus::Leader { lease_epoch: 1 }, snapshot);
}

// ── Construction ──

#[test]
fn new_cache_is_empty() {
    let mut c = cache();
    assert!(c.pending_append_queue_is_empty());
    assert_eq!(c.buffer_size_total(), 0);

    let k = agg(1, 1, 1);
    for path in [CachePath::Read, CachePath::Write] {
        let (loaded, status) = c.aggregate_load_status(&k, path);
        assert!(!loaded);
        assert_eq!(status, AggregateStatus::NotFound);
    }
}

// ── Queue Operations ──

#[test]
fn add_to_queue_makes_aggregate_visible_on_write_path_only() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 1, 100, 1);

    // Write path sees queued aggregate as Found
    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);

    // Read path does NOT see the queued data
    let (loaded, _) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(!loaded);
}

#[test]
fn queue_tracks_event_seqes_with_max_wins() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 3, 1, 100, 1);
    queue_write(&mut c, &k, 7, 2, 100, 2);

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.event_seq, 7);
    assert_eq!(indexes.aggregate_version, 2);
    assert!(!indexes.pending_delete_or_deleted);
}

#[test]
fn queue_tracks_client_seqes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 100, 5);
    queue_write(&mut c, &k, 2, 2, 200, 10);

    assert_eq!(c.get_client_seq(&k, 100), Some(5));
    assert_eq!(c.get_client_seq(&k, 200), Some(10));
    assert_eq!(c.get_client_seq(&k, 999), None);
}

#[test]
fn client_seq_max_wins_within_same_client() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 100, 3);
    queue_write(&mut c, &k, 2, 2, 100, 7);

    assert_eq!(c.get_client_seq(&k, 100), Some(7));
}

#[test]
fn pending_delete_in_queue() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let item = test_queue_item(k.clone(), 5, 10, 1);

    c.add_pending_delete_to_queue(&k, 10, 5, false, false, item);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Deleted);

    let indexes = c.get_write_event_seqes(&k);
    assert!(indexes.pending_delete_or_deleted);
    assert!(!indexes.allow_recreate);
}

#[test]
fn pending_delete_with_recreate_flags() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let item = test_queue_item(k.clone(), 5, 10, 1);

    c.add_pending_delete_to_queue(&k, 10, 5, true, true, item);

    let indexes = c.get_write_event_seqes(&k);
    assert!(indexes.pending_delete_or_deleted);
    assert!(indexes.allow_recreate);
    assert!(indexes.allow_sequence_continuation);
}

#[test]
fn pending_trim_updates_min_aggregate_version() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // First add a write so aggregate exists in queue
    queue_write(&mut c, &k, 5, 5, 1, 1);

    let trim_item = test_queue_item(k.clone(), 0, 0, 0);
    c.add_pending_trim_to_queue(&k, 3, 5, 5, trim_item);

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.min_aggregate_version, 3);
}

#[test]
fn trim_only_increases_min_aggregate_version() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 5, 1, 1);

    let item1 = test_queue_item(k.clone(), 0, 0, 0);
    c.add_pending_trim_to_queue(&k, 5, 5, 5, item1);

    // Lower trim should not decrease
    let item2 = test_queue_item(k.clone(), 0, 0, 0);
    c.add_pending_trim_to_queue(&k, 3, 5, 5, item2);

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.min_aggregate_version, 5);
}

#[test]
fn trim_commit_does_not_corrupt_snapshot_log_id() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Write an event, simulate disk write setting log_id=5, then commit
    queue_write(&mut c, &k, 10, 3, 100, 1);
    let mut snapshot = c.take_sync_positions_snapshot();
    snapshot.aggregate_queue_positions.get_mut(&k).unwrap().log_id = 5;
    snapshot.aggregate_queue_positions.get_mut(&k).unwrap().metablock_absolute_pos = 2048;
    c.commit_sync_positions_snapshot(NodeStatus::Standalone, snapshot);

    // Verify position was set correctly
    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Write);
    assert_eq!(pos.log_id, 5);
    assert_eq!(pos.metablock_absolute_pos, 2048);

    // Now add a trim (creates QueueAggregatePositions with default log_id=0)
    let trim_item = test_queue_item(k.clone(), 0, 0, 0);
    c.add_pending_trim_to_queue(&k, 2, 3, 10, trim_item);
    sync_and_commit_standalone(&mut c);

    // log_id and metablock_absolute_pos must NOT have been overwritten to 0
    for path in [CachePath::Write, CachePath::Read] {
        let pos = c.get_aggregate_last_metablock_pos(&k, path);
        assert_eq!(pos.log_id, 5, "trim must not corrupt log_id on {:?}", path);
        assert_eq!(pos.metablock_absolute_pos, 2048, "trim must not corrupt metablock_absolute_pos on {:?}", path);
    }
}

#[test]
fn pending_trim_does_not_shadow_durable_state() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Durable state: version 50, event_seq 10. Then a trim-only pending window.
    queue_write(&mut c, &k, 10, 50, 100, 1);
    sync_and_commit_standalone(&mut c);
    c.add_pending_trim_to_queue(&k, 30, 50, 10, test_queue_item(k.clone(), 0, 0, 0));

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.min_aggregate_version, 30);
    assert_eq!(indexes.aggregate_version, 50, "trim-only queue entry must not shadow durable aggregate_version");
    assert_eq!(indexes.event_seq, 10, "trim-only queue entry must not shadow durable event_seq");
}

#[test]
fn buffer_size_calculations() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    assert_eq!(c.buffer_size_total(), 0);

    // Queue items without datablock_bytes only contribute metablock size
    queue_write(&mut c, &k, 1, 1, 1, 1);
    assert!(c.buffer_size_metablocks() > 0);
    assert_eq!(c.buffer_size_datablocks(), 0);
    assert_eq!(c.buffer_size_total(), c.buffer_size_metablocks());
}

#[test]
fn add_to_pending_queue_bypass_does_not_track_positions() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let items = vec![test_queue_item(k.clone(), 1, 5, 100)];

    c.add_to_pending_queue(items);

    assert!(!c.pending_append_queue_is_empty());

    // Aggregate should NOT be visible since add_to_pending_queue skips tracking
    let (loaded, _) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(!loaded);
}

// ── Snapshot and Commit Lifecycle ──

#[test]
fn take_snapshot_clears_pending_queue_but_preserves_aggregate_positions() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 1, 100, 1);
    assert!(!c.pending_append_queue_is_empty());

    let snapshot = c.take_sync_positions_snapshot();

    // Queue is now empty for new writes
    assert!(c.pending_append_queue_is_empty());

    // But aggregate positions remain visible for concurrent writes
    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);

    // Snapshot captured the data
    assert!(!snapshot.pending_append_queue.is_empty());
    assert!(snapshot.aggregate_queue_positions.contains_key(&k));
}

#[test]
fn commit_standalone_updates_both_read_and_write_snapshots() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 3, 100, 1);
    sync_and_commit_standalone(&mut c);

    for path in [CachePath::Read, CachePath::Write] {
        let (loaded, status) = c.aggregate_load_status(&k, path);
        assert!(loaded, "should be loaded on {:?}", path);
        assert_eq!(status, AggregateStatus::Found, "should be Found on {:?}", path);
    }
}

#[test]
fn commit_leader_updates_only_write_snapshot() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 3, 100, 1);
    sync_and_commit_leader(&mut c);

    // Write path sees committed data
    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);

    // Read path does NOT see it (needs replication commit)
    let (loaded, _) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(!loaded);
}

#[test]
fn commit_cleans_up_queue_positions_when_not_updated_concurrently() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 1, 100, 1);

    let snapshot = c.take_sync_positions_snapshot();

    // No new writes arrived during "sync"
    c.commit_sync_positions_snapshot(NodeStatus::Standalone, snapshot);

    // Queue position should be cleaned up (aggregate_version matches)
    let indexes = c.get_write_event_seqes(&k);
    // Should fall through to snapshot cache, not queue
    assert_eq!(indexes.aggregate_version, 1);
}

#[test]
fn commit_preserves_queue_positions_when_updated_concurrently() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 1, 100, 1);
    let snapshot = c.take_sync_positions_snapshot();

    // Simulate concurrent write during sync
    queue_write(&mut c, &k, 10, 2, 100, 2);

    c.commit_sync_positions_snapshot(NodeStatus::Standalone, snapshot);

    // Queue position for batch 2 should still be present
    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 2);
    assert_eq!(indexes.event_seq, 10);
}

#[test]
fn commit_updates_client_seqes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 1, 100, 42);
    sync_and_commit_standalone(&mut c);

    let ck = client_key(&k, 100);
    let (loaded, last_idx) = c.aggregate_client_load_status(&k, &ck);
    assert!(loaded);
    assert_eq!(last_idx, Some(42));
}

#[test]
fn commit_position_snapshot_updates_read_cache() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let event_batch = test_event_batch(k.clone(), 3, 10);

    c.commit_position_snapshot(&event_batch, 1, 512, CachePath::Read);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);

    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Read);
    assert_eq!(pos.aggregate_version, 3);
    assert_eq!(pos.event_seq, 10);
    assert_eq!(pos.log_id, 1);
    assert_eq!(pos.metablock_absolute_pos, 512);
}

#[test]
fn commit_position_snapshot_advances_existing_entry() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // First commit
    let batch1 = test_event_batch(k.clone(), 1, 5);
    c.commit_position_snapshot(&batch1, 1, 100, CachePath::Read);

    // Second commit with higher indexes on a new log segment
    let batch2 = test_event_batch(k.clone(), 3, 15);
    c.commit_position_snapshot(&batch2, 2, 200, CachePath::Read);

    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Read);
    assert_eq!(pos.aggregate_version, 3);
    assert_eq!(pos.event_seq, 15);
    assert_eq!(pos.log_id, 2);
    assert_eq!(pos.metablock_absolute_pos, 200);
}

#[test]
fn commit_read_position_does_not_regress_indexes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let batch_high = test_event_batch(k.clone(), 10, 50);
    c.commit_position_snapshot(&batch_high, 1, 100, CachePath::Read);

    // Lower indexes should not overwrite, but position still advances
    let batch_low = test_event_batch(k.clone(), 2, 5);
    c.commit_position_snapshot(&batch_low, 2, 50, CachePath::Read);

    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Read);
    assert_eq!(pos.aggregate_version, 10);
    assert_eq!(pos.event_seq, 50);
    assert_eq!(pos.log_id, 2, "log_id should advance even when indexes don't");
    assert_eq!(pos.metablock_absolute_pos, 50, "position should advance even when indexes don't");
}

// ── Copy Write to Read Snapshot ──

#[test]
fn copy_write_to_read_snapshot() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 1, 100, 1);
    sync_and_commit_leader(&mut c);

    // Read path doesn't see it yet
    let (loaded, _) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(!loaded);

    c.copy_write_to_read_snapshot(&k);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);
}

// ── Cache Population (put_*) ──

#[test]
fn put_aggregate_not_found_sentinel() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for path in [CachePath::Read, CachePath::Write] {
        c.put_aggregate_into_cache_as_not_found(k.clone(), path);

        let (loaded, status) = c.aggregate_load_status(&k, path);
        assert!(loaded);
        assert_eq!(status, AggregateStatus::NotFound);
    }
}

#[test]
fn put_aggregate_deleted() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    c.put_aggregate_into_cache_as_deleted(k.clone(), 0, 0, 10, 5, true, true, CachePath::Write);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Deleted);

    let indexes = c.get_write_event_seqes(&k);
    assert!(indexes.pending_delete_or_deleted);
    assert!(indexes.allow_recreate);
    assert!(indexes.allow_sequence_continuation);
}

#[test]
fn put_aggregate_found_with_client() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let snapshot = MemSnapshotAggregate::found(1, 512, 10, 3, 1);

    c.put_aggregate_into_cache(k.clone(), snapshot, 100, 42, false, CachePath::Write);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);

    // Client sequence should also be cached on Write path
    let ck = client_key(&k, 100);
    let (cl_loaded, cl_idx) = c.aggregate_client_load_status(&k, &ck);
    assert!(cl_loaded);
    assert_eq!(cl_idx, Some(42));
}

#[test]
fn put_aggregate_into_read_does_not_cache_client() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let snapshot = MemSnapshotAggregate::found(1, 512, 10, 3, 1);

    c.put_aggregate_into_cache(k.clone(), snapshot, 100, 42, false, CachePath::Read);

    // Aggregate visible on read
    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);

    // Client should NOT be cached (only Write path caches client)
    let ck = client_key(&k, 100);
    let (cl_loaded, _) = c.aggregate_client_load_status(&k, &ck);
    assert!(!cl_loaded);
}

#[test]
fn low_priority_rejected_when_cache_full() {
    // 112 bytes / 112 per entry = capacity of 1
    let mut c = cache_with(0, 112, 0, 1024);
    let k1 = agg(1, 1, 1);
    let k2 = agg(1, 1, 2);

    // Normal priority fills the single slot
    c.put_aggregate_into_cache(k1.clone(), MemSnapshotAggregate::found(1, 0, 1, 1, 1), 1, 1, false, CachePath::Write);

    // Low priority is rejected - no spare capacity
    c.put_aggregate_into_cache(k2.clone(), MemSnapshotAggregate::found(1, 0, 2, 2, 1), 1, 1, true, CachePath::Write);

    let (loaded1, _) = c.aggregate_load_status(&k1, CachePath::Write);
    let (loaded2, _) = c.aggregate_load_status(&k2, CachePath::Write);
    assert!(loaded1);
    assert!(!loaded2); // Rejected due to full cache
}

#[test]
fn low_priority_accepted_when_capacity_available() {
    let mut c = cache(); // Large cache
    let k1 = agg(1, 1, 1);
    let k2 = agg(1, 1, 2);

    c.put_aggregate_into_cache(k1.clone(), MemSnapshotAggregate::found(1, 0, 1, 1, 1), 1, 1, false, CachePath::Write);
    c.put_aggregate_into_cache(k2.clone(), MemSnapshotAggregate::found(1, 0, 2, 2, 1), 1, 1, true, CachePath::Write);

    let (loaded1, _) = c.aggregate_load_status(&k1, CachePath::Write);
    let (loaded2, _) = c.aggregate_load_status(&k2, CachePath::Write);
    assert!(loaded1);
    assert!(loaded2);
}

#[test]
fn low_priority_does_not_promote_existing_entry() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let snap1 = MemSnapshotAggregate::found(1, 0, 1, 1, 1);
    let snap2 = MemSnapshotAggregate::found(2, 0, 5, 5, 1);

    c.put_aggregate_into_cache(k.clone(), snap1, 1, 1, false, CachePath::Write);
    // Low priority with same key should NOT update the existing entry
    c.put_aggregate_into_cache(k.clone(), snap2, 1, 5, true, CachePath::Write);

    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Write);
    // Should still be the original values
    assert_eq!(pos.log_id, 1);
    assert_eq!(pos.event_seq, 1);
}

#[test]
fn client_sentinel_zero_means_checked_but_not_found() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let ck = client_key(&k, 100);

    c.put_aggregate_client_into_cache(ck.clone(), 0, false);

    let (loaded, last_idx) = c.aggregate_client_load_status(&k, &ck);
    assert!(loaded); // We DID check
    assert_eq!(last_idx, None); // But client has no events (sentinel 0)
}

#[test]
fn get_client_seq_sentinel_zero_returns_none() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let ck = client_key(&k, 100);

    c.put_aggregate_client_into_cache(ck, 0, false);

    assert_eq!(c.get_client_seq(&k, 100), None);
}

// ── Aggregate Load Status: Queue vs Snapshot Priority ──

#[test]
fn aggregate_load_status_queue_takes_precedence_on_write_path() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Put as NotFound in snapshot
    c.put_aggregate_into_cache_as_not_found(k.clone(), CachePath::Write);

    // Add to queue (simulates new write to unknown aggregate)
    queue_write(&mut c, &k, 1, 1, 1, 1);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found); // Queue wins over snapshot
}

#[test]
fn client_load_status_queue_takes_precedence() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let ck = client_key(&k, 100);

    // Client has index 5 in snapshot cache
    c.put_aggregate_client_into_cache(ck.clone(), 5, false);

    // Queue has higher index
    queue_write(&mut c, &k, 1, 1, 100, 10);

    let (loaded, last_idx) = c.aggregate_client_load_status(&k, &ck);
    assert!(loaded);
    assert_eq!(last_idx, Some(10)); // Queue wins
}

/// Chaos 16k finding A: a node rebuilding state from applied event batches
/// (follower catchup / leader PCD commit) must carry the trim floor the
/// batch metablock embeds. Dropping it leaves min=0/stale, and a SoftTrim's
/// min bump is a silent no-op whenever the snapshot was evicted in between —
/// reads below an acked trim floor come back on that node.
#[test]
fn commit_position_snapshot_fresh_carries_embedded_trim_floor() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let mut eb = test_event_batch(k.clone(), 7, 7);
    eb.trimmed_below_version = 5;
    c.commit_position_snapshot(&eb, 1, 0, CachePath::Write);

    let idx = c.get_write_event_seqes(&k);
    assert_eq!(idx.aggregate_version, 7);
    assert_eq!(
        idx.min_aggregate_version, 5,
        "fresh snapshot from an applied batch must carry the embedded trim floor",
    );
}

/// The SoftTrim metablock carries full aggregate state, so committing a trim
/// must establish the floor even when the snapshot was LRU-evicted. The
/// silent no-op variant lost acked floors under 16k churn: the next write
/// then embeds floor=1 and every node that rebuilds from it inherits the loss.
#[test]
fn commit_trim_snapshot_inserts_when_snapshot_absent() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    c.commit_trim_snapshot(&k, 94, 95, 95, 7, 1234, CachePath::Write);

    let idx = c.get_write_event_seqes(&k);
    assert_eq!(idx.min_aggregate_version, 94, "trim commit must establish the floor on a cache miss");
    assert_eq!(idx.aggregate_version, 95);
    assert_eq!(idx.event_seq, 95);
}

#[test]
fn commit_trim_snapshot_bumps_existing_and_never_regresses() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let snap = MemSnapshotAggregate::found(1, 0, 99, 99, 2);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Write);

    c.commit_trim_snapshot(&k, 94, 95, 95, 7, 1234, CachePath::Write);
    let idx = c.get_write_event_seqes(&k);
    assert_eq!(idx.min_aggregate_version, 94);
    assert_eq!(idx.aggregate_version, 99, "trim commit must not regress a newer version");
    assert_eq!(idx.event_seq, 99);

    // Stale trim replayed late must not lower the floor.
    c.commit_trim_snapshot(&k, 50, 60, 60, 7, 1234, CachePath::Write);
    assert_eq!(c.get_write_event_seqes(&k).min_aggregate_version, 94);
}

#[test]
fn commit_position_snapshot_existing_max_merges_trim_floor() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let snap = MemSnapshotAggregate::found(1, 0, 4, 4, 2);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Write);

    // Newer batch carries a higher floor — must bump.
    let mut eb = test_event_batch(k.clone(), 7, 7);
    eb.trimmed_below_version = 5;
    c.commit_position_snapshot(&eb, 1, 0, CachePath::Write);
    assert_eq!(c.get_write_event_seqes(&k).min_aggregate_version, 5);

    // Stale batch with a lower embedded floor must NOT regress it.
    let mut eb = test_event_batch(k.clone(), 8, 8);
    eb.trimmed_below_version = 1;
    c.commit_position_snapshot(&eb, 1, 0, CachePath::Write);
    assert_eq!(
        c.get_write_event_seqes(&k).min_aggregate_version, 5,
        "embedded floors are write-time values — min only ever rises",
    );
}

#[test]
fn get_write_event_seqes_queue_over_snapshot() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Snapshot says batch=3, event=10
    let snap = MemSnapshotAggregate::found(1, 0, 10, 3, 1);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Write);

    // Queue says batch=5, event=20
    queue_write(&mut c, &k, 20, 5, 1, 1);

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 5);
    assert_eq!(indexes.event_seq, 20);
}

#[test]
fn get_write_event_seqes_falls_through_to_snapshot() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let snap = MemSnapshotAggregate::found(1, 0, 10, 3, 1);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Write);

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 3);
    assert_eq!(indexes.event_seq, 10);
}

#[test]
fn get_write_event_seqes_returns_zeroes_for_unknown() {
    let mut c = cache();
    let k = agg(99, 99, 99);

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 0);
    assert_eq!(indexes.event_seq, 0);
    assert!(!indexes.pending_delete_or_deleted);
}

// ── Recent Write Cache ──

#[test]
fn cache_recent_write_and_retrieve() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let mb = test_metablock(k.clone(), 1, 5, 100, 1);

    c.cache_recent_write(k.clone(), 1, mb, None, 64);

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].0, 1); // aggregate_version
}

#[test]
fn cache_recent_write_respects_wal_seq_visibility() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for i in 1..=3 {
        let mb = test_metablock(k.clone(), i, i * 5, 100, i * 10);
        c.cache_recent_write(k.clone(), i, mb, None, 64);
    }

    // Only wal_seq <= 20 visible
    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, 20).collect();
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0].0, 1);
    assert_eq!(writes[1].0, 2);
}

#[test]
fn cache_recent_write_iter_from_skips_earlier_batches() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for i in 1..=5 {
        let mb = test_metablock(k.clone(), i, i, 1, i);
        c.cache_recent_write(k.clone(), i, mb, None, 32);
    }

    let writes: Vec<_> = c.get_cached_writes_from(&k, 3, u64::MAX).collect();
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0].0, 3);
    assert_eq!(writes[1].0, 4);
    assert_eq!(writes[2].0, 5);
}

#[test]
fn cache_recent_write_zero_budget_disables_cache() {
    let mut c = cache_with(0, 64 * 1024, 64 * 1024, 1024 * 1024);
    let k = agg(1, 1, 1);
    let mb = test_metablock(k.clone(), 1, 5, 100, 1);

    c.cache_recent_write(k.clone(), 1, mb, None, 64);

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    assert_eq!(writes.len(), 0);
}

#[test]
fn cache_recent_write_evicts_oldest_under_pressure() {
    // Budget for only ~2 entries
    let mut c = cache_with(128, 64 * 1024, 64 * 1024, 1024 * 1024);
    let k = agg(1, 1, 1);

    for i in 1..=3 {
        let mb = test_metablock(k.clone(), i, i, 1, i);
        c.cache_recent_write(k.clone(), i, mb, None, 64);
    }

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    // Oldest should have been evicted
    assert!(writes.len() <= 2);
    if !writes.is_empty() {
        assert!(writes[0].0 >= 2);
    }
}

#[test]
fn cache_recent_write_returns_none_for_unknown_aggregate() {
    let c = cache();
    let k = agg(99, 99, 99);

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    assert_eq!(writes.len(), 0);
}

// ── Deleted Aggregate Clears Recent Write Cache ──

#[test]
fn put_deleted_on_read_path_clears_recent_writes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Cache some writes
    for i in 1..=3 {
        let mb = test_metablock(k.clone(), i, i, 1, i);
        c.cache_recent_write(k.clone(), i, mb, None, 64);
    }

    // Mark deleted on read path - should clear recent writes
    c.put_aggregate_into_cache_as_deleted(k.clone(), 0, 0, 10, 5, false, false, CachePath::Read);

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    assert_eq!(writes.len(), 0);
}

#[test]
fn put_deleted_on_write_path_does_not_clear_recent_writes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let mb = test_metablock(k.clone(), 1, 1, 1, 1);
    c.cache_recent_write(k.clone(), 1, mb, None, 64);

    c.put_aggregate_into_cache_as_deleted(k.clone(), 0, 0, 10, 5, false, false, CachePath::Write);

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    assert_eq!(writes.len(), 1);
}

// ── Trim and Recent Write Interaction ──

#[test]
fn update_min_aggregate_version_read_path_evicts_trimmed_writes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for i in 1..=5 {
        let mb = test_metablock(k.clone(), i, i, 1, i);
        c.cache_recent_write(k.clone(), i, mb, None, 64);
    }

    // Need aggregate in read snapshot first
    let snap = MemSnapshotAggregate::found(1, 0, 5, 5, 1);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Read);

    c.update_aggregate_min_aggregate_version(&k, 3, CachePath::Read);

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    // Batches 1 and 2 should be evicted (< 3)
    assert!(writes.iter().all(|(idx, _)| *idx >= 3));
}

#[test]
fn update_min_aggregate_version_write_path_does_not_evict_recent_writes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for i in 1..=5 {
        let mb = test_metablock(k.clone(), i, i, 1, i);
        c.cache_recent_write(k.clone(), i, mb, None, 64);
    }

    let snap = MemSnapshotAggregate::found(1, 0, 5, 5, 1);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Write);

    c.update_aggregate_min_aggregate_version(&k, 3, CachePath::Write);

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    // All 5 should still be present
    assert_eq!(writes.len(), 5);
}

// ── Metablock Position ──

#[test]
fn get_aggregate_last_metablock_pos_returns_zeroes_for_unknown() {
    let mut c = cache();
    let k = agg(99, 99, 99);

    for path in [CachePath::Read, CachePath::Write] {
        let pos = c.get_aggregate_last_metablock_pos(&k, path);
        assert_eq!(pos.log_id, 0);
        assert_eq!(pos.metablock_absolute_pos, 0);
        assert_eq!(pos.aggregate_version, 0);
        assert_eq!(pos.event_seq, 0);
    }
}

#[test]
fn get_aggregate_last_metablock_pos_from_snapshot() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let snap = MemSnapshotAggregate::found(5, 2048, 20, 8, 2);

    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Write);

    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Write);
    assert_eq!(pos.log_id, 5);
    assert_eq!(pos.metablock_absolute_pos, 2048);
    assert_eq!(pos.aggregate_version, 8);
    assert_eq!(pos.event_seq, 20);
    assert_eq!(pos.min_aggregate_version, 2);
}

// ── Cache Capacity Checks ──

#[test]
fn is_aggregate_snapshot_full_or_contains_reports_presence() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    assert!(!c.is_aggregate_snapshot_full_or_contains(&k, CachePath::Write));

    c.put_aggregate_into_cache_as_not_found(k.clone(), CachePath::Write);

    assert!(c.is_aggregate_snapshot_full_or_contains(&k, CachePath::Write));
}

#[test]
fn is_aggregate_client_cache_full_or_contains_reports_presence() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let ck = client_key(&k, 100);

    assert!(!c.is_aggregate_client_cache_full_or_contains(&ck));

    c.put_aggregate_client_into_cache(ck.clone(), 5, false);

    assert!(c.is_aggregate_client_cache_full_or_contains(&ck));
}

// ── Rollback Operations ──

#[test]
fn fsync_rollback_clears_queue_and_sets_flag() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 1, 1);
    assert!(!c.pending_append_queue_is_empty());

    c.execute_fsync_rollback();

    assert!(c.pending_append_queue_is_empty());
    assert!(c.take_fsync_rollback_flag());
    // Flag is consumed
    assert!(!c.take_fsync_rollback_flag());
}

#[test]
fn fsync_rollback_clears_aggregate_queue_positions() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 1, 100, 1);

    c.execute_fsync_rollback();

    // Queue positions gone - should fall through to snapshot (which is empty)
    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 0);
}

#[test]
fn fsync_rollback_with_empty_queue_does_not_set_flag() {
    let mut c = cache();
    c.execute_fsync_rollback();
    assert!(!c.take_fsync_rollback_flag());
}

#[test]
fn replication_rollback_clears_everything() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Populate write snapshot
    queue_write(&mut c, &k, 5, 1, 100, 1);
    sync_and_commit_leader(&mut c);

    // Populate client snapshot
    let ck = client_key(&k, 100);
    c.put_aggregate_client_into_cache(ck.clone(), 42, false);

    c.execute_replication_rollback();

    // Write snapshot cleared
    let (loaded, _) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(!loaded);

    // Client snapshot cleared
    let (cl_loaded, _) = c.aggregate_client_load_status(&k, &ck);
    assert!(!cl_loaded);
}

#[test]
fn replication_rollback_also_does_fsync_rollback() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 1, 1);

    c.execute_replication_rollback();

    assert!(c.pending_append_queue_is_empty());
    // fsync rollback flag set because queue was non-empty
    assert!(c.take_fsync_rollback_flag());
}

#[test]
fn replication_rollback_flag_set_when_pending_batches_exist() {
    let mut c = cache();

    // Directly push a replication batch
    c.push_pending_replication(test_pending_commit_data());

    c.execute_replication_rollback();

    assert!(c.take_replication_rollback_flag());
    assert!(!c.take_replication_rollback_flag()); // Consumed
}

#[test]
fn replication_rollback_flag_not_set_when_queue_was_empty() {
    let mut c = cache();

    // No pending replication batches pushed queue stays empty

    c.execute_replication_rollback();

    assert!(
        !c.take_replication_rollback_flag(),
        "flag intentionally not set when queue was empty. Prevents spurious bails in capture_replication_snapshot"
    );
}

#[test]
fn capture_after_fsync_rollback_detects_invalidation() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 1, 1);
    c.execute_fsync_rollback();

    // The flag is set - next capture should detect rollback
    assert!(c.take_fsync_rollback_flag());
}

// ── Replication Queue ──

#[test]
fn push_and_take_pending_replication() {
    let mut c = cache();

    c.push_pending_replication(test_pending_commit_data());
    c.push_pending_replication(test_pending_commit_data());

    let batches = c.take_pending_replication();
    assert_eq!(batches.len(), 2);

    // After take, queue is empty
    let empty = c.take_pending_replication();
    assert!(empty.is_empty());
}

#[test]
fn peek_pending_replication() {
    let mut c = cache();

    assert!(c.peek_pending_replication().is_none());

    c.push_pending_replication(test_pending_commit_data());

    assert!(c.peek_pending_replication().is_some());
}

#[test]
fn inflight_pressure_check() {
    let mut c = cache_with(64 * 1024, 64 * 1024, 64 * 1024, 1); // 1 byte cap

    assert!(!c.is_inflight_pressured());

    c.push_pending_replication(test_pending_commit_data());

    assert!(c.is_inflight_pressured());
}

// ── Full Lifecycle: Queue → Sync → Commit → Read ──

#[test]
fn full_lifecycle_standalone() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // 1. Queue writes
    for i in 1..=3u64 {
        queue_write(&mut c, &k, i * 5, i, 100, i);
    }

    // 2. Visible on write path (queued)
    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 3);
    assert_eq!(indexes.event_seq, 15);

    // 3. Sync + commit (standalone updates both paths)
    sync_and_commit_standalone(&mut c);

    // 4. Now visible on both paths
    for path in [CachePath::Read, CachePath::Write] {
        let (loaded, status) = c.aggregate_load_status(&k, path);
        assert!(loaded);
        assert_eq!(status, AggregateStatus::Found);
    }

    // 5. Queue positions cleaned up
    assert!(c.pending_append_queue_is_empty());
}

#[test]
fn full_lifecycle_leader_then_replication_commit() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // 1. Queue + sync as leader
    queue_write(&mut c, &k, 5, 1, 100, 1);
    sync_and_commit_leader(&mut c);

    // 2. Visible on write, NOT on read
    let (_, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert_eq!(status, AggregateStatus::Found);
    let (loaded, _) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(!loaded);

    // 3. Simulate replication commit: copy write → read
    c.copy_write_to_read_snapshot(&k);

    // 4. Now visible on read
    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);
}

#[test]
fn multiple_aggregates_independent() {
    let mut c = cache();
    let keys: Vec<_> = (1..=5).map(|i| agg(1, 1, i)).collect();

    for (i, k) in keys.iter().enumerate() {
        queue_write(&mut c, k, (i + 1) as u64 * 10, (i + 1) as u64, 100, (i + 1) as u64);
    }

    sync_and_commit_standalone(&mut c);

    for (i, k) in keys.iter().enumerate() {
        let indexes = c.get_write_event_seqes(k);
        assert_eq!(indexes.aggregate_version, (i + 1) as u64);
        assert_eq!(indexes.event_seq, (i + 1) as u64 * 10);
    }
}

#[test]
fn delete_then_recreate_lifecycle() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Write
    queue_write(&mut c, &k, 5, 1, 100, 1);
    sync_and_commit_standalone(&mut c);

    // Delete with allow_recreate
    let del_item = test_queue_item(k.clone(), 1, 5, 1);
    c.add_pending_delete_to_queue(&k, 5, 1, true, true, del_item);
    sync_and_commit_standalone(&mut c);

    // Recreate
    queue_write(&mut c, &k, 6, 2, 100, 2);

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 2);
    assert!(!indexes.pending_delete_or_deleted);
}

#[test]
fn interleaved_aggregates_in_queue_each_commit_independently() {
    let mut c = cache();
    let k1 = agg(1, 1, 1);
    let k2 = agg(1, 1, 2);

    queue_write(&mut c, &k1, 5, 1, 100, 1);
    queue_write(&mut c, &k2, 10, 1, 200, 1);

    let snapshot = c.take_sync_positions_snapshot();

    // Write more to k1 during "sync"
    queue_write(&mut c, &k1, 8, 2, 100, 2);

    c.commit_sync_positions_snapshot(NodeStatus::Standalone, snapshot);

    // k1 should still have queue position (batch 2 > committed batch 1)
    let idx1 = c.get_write_event_seqes(&k1);
    assert_eq!(idx1.aggregate_version, 2);

    // k2 should be cleaned up from queue (no concurrent write)
    let idx2 = c.get_write_event_seqes(&k2);
    assert_eq!(idx2.aggregate_version, 1);
}

// ── Edge Cases ──

#[test]
fn fsync_rollback_then_new_writes_succeed() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 1, 1);
    c.execute_fsync_rollback();

    // Drain the flag
    let _ = c.take_fsync_rollback_flag();

    // New writes work
    queue_write(&mut c, &k, 2, 1, 1, 1);
    assert!(!c.pending_append_queue_is_empty());

    sync_and_commit_standalone(&mut c);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);
}

#[test]
fn commit_with_pending_delete_skips_snapshot_update() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Queue a delete
    let del_item = test_queue_item(k.clone(), 5, 10, 1);
    c.add_pending_delete_to_queue(&k, 10, 5, false, false, del_item);

    let snapshot = c.take_sync_positions_snapshot();
    c.commit_sync_positions_snapshot(NodeStatus::Standalone, snapshot);

    // commit_sync_positions_snapshot `continue`s for pending_delete entries,
    // skipping both position updates AND queue cleanup.
    // The queue position persists - deletion is handled separately
    // via put_aggregate_into_cache_as_deleted in the caller.
    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 5);
    assert!(indexes.pending_delete_or_deleted);
}

#[test]
fn multiple_clients_tracked_independently() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for client_id in [100u128, 200, 300] {
        queue_write(&mut c, &k, 1, 1, client_id, client_id as u64);
    }

    for client_id in [100u128, 200, 300] {
        assert_eq!(c.get_client_seq(&k, client_id), Some(client_id as u64));
    }
}

#[test]
fn commit_updates_client_snapshots_only_higher_values() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let ck = client_key(&k, 100);

    // Pre-populate client snapshot with high value
    c.put_aggregate_client_into_cache(ck.clone(), 50, false);

    // Queue a write with lower client event index
    queue_write(&mut c, &k, 1, 1, 100, 10);
    sync_and_commit_standalone(&mut c);

    // Client snapshot should keep the higher value (50)
    let (_, idx) = c.aggregate_client_load_status(&k, &ck);
    assert_eq!(idx, Some(50));
}

#[test]
fn many_writes_then_commit_preserves_latest() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for i in 1..=100u64 {
        queue_write(&mut c, &k, i * 3, i, 1, i);
    }

    sync_and_commit_standalone(&mut c);

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 100);
    assert_eq!(indexes.event_seq, 300);
}

#[test]
fn clear_all_caches_clears_everything() {
    let mut c = cache();
    let k1 = agg(1, 1, 1);
    let k2 = agg(1, 1, 2);

    // Populate aggregate_read_snapshots
    let snap = MemSnapshotAggregate::found(1, 512, 10, 3, 1);
    c.put_aggregate_into_cache(k1.clone(), snap.clone(), 100, 1, false, CachePath::Read);

    // Populate aggregate_write_snapshots via queue + commit
    queue_write(&mut c, &k2, 5, 1, 200, 1);
    sync_and_commit_leader(&mut c);

    // Populate aggregate_recent_writes
    let mb = test_metablock(k1.clone(), 1, 5, 100, 1);
    c.cache_recent_write(k1.clone(), 1, mb, None, 64);

    // Populate aggregate_client_snapshots
    let ck = client_key(&k2, 200);
    c.put_aggregate_client_into_cache(ck.clone(), 42, false);

    // Park a deferred commit
    c.push_parked_commit(parked_pcd(1));

    // Verify things are populated
    let (loaded, _) = c.aggregate_load_status(&k1, CachePath::Read);
    assert!(loaded);
    let (loaded, _) = c.aggregate_load_status(&k2, CachePath::Write);
    assert!(loaded);
    let writes: Vec<_> = c.get_cached_writes_from(&k1, 1, u64::MAX).collect();
    assert_eq!(writes.len(), 1);
    let (loaded, _) = c.aggregate_client_load_status(&k2, &ck);
    assert!(loaded);

    // Execute clear_all_caches
    c.clear_all_caches();

    // Verify all caches are empty
    let (loaded, _) = c.aggregate_load_status(&k1, CachePath::Read);
    assert!(!loaded, "aggregate_read_snapshots should be empty");
    let (loaded, _) = c.aggregate_load_status(&k2, CachePath::Write);
    assert!(!loaded, "aggregate_write_snapshots should be empty");
    let writes: Vec<_> = c.get_cached_writes_from(&k1, 1, u64::MAX).collect();
    assert_eq!(writes.len(), 0, "aggregate_recent_writes should be empty");
    let (loaded, _) = c.aggregate_client_load_status(&k2, &ck);
    assert!(!loaded, "aggregate_client_snapshots should be empty");
    assert_eq!(c.parked_commit_count(), 0, "parked commits must be discarded (their entries left the chain)");
}

// ── Parked deferred commits (follower) ──

fn parked_pcd(wal_seq: u64) -> PendingCommitData {
    let mut pcd = test_pending_commit_data();
    pcd.log_metadata.write.wal_seq = wal_seq;
    pcd
}

/// drain_parked_commits_up_to pops exactly the wal_seq-ordered prefix the
/// confirmed target covers, keeps the rest parked, and keeps byte accounting
/// consistent (zero when empty).
#[test]
fn parked_commits_drain_exactly_the_confirmed_prefix() {
    // (target, drained tips, remaining count)
    let cases: [(u64, &[u64], usize); 5] = [
        (0, &[], 3),
        (1, &[1], 2),
        (3, &[1, 2], 1),
        (5, &[1, 2, 5], 0),
        (u64::MAX, &[1, 2, 5], 0),
    ];
    for (target, expected, remaining) in cases {
        let mut c = cache();
        for seq in [1, 2, 5] {
            c.push_parked_commit(parked_pcd(seq));
        }
        let bytes_before = c.parked_commit_bytes();
        assert!(bytes_before > 0, "target {target}: parked bytes must be accounted on push");

        let drained: Vec<u64> = c
            .drain_parked_commits_up_to(target)
            .iter()
            .map(|p| p.log_metadata.write.wal_seq)
            .collect();

        assert_eq!(drained, expected, "target {target}: wrong drained prefix");
        assert_eq!(c.parked_commit_count(), remaining, "target {target}: wrong remainder");
        if remaining == 0 {
            assert_eq!(c.parked_commit_bytes(), 0, "target {target}: bytes must be zero when empty");
        } else if !expected.is_empty() {
            assert!(c.parked_commit_bytes() < bytes_before, "target {target}: bytes must shrink on drain");
        }

        // A stale re-delivery of the same target must yield nothing new.
        assert!(c.drain_parked_commits_up_to(target).is_empty(), "target {target}: re-drain must be empty");
    }
}

/// take_all_parked_commits returns the whole tail in order (promotion commits
/// everything); clear_parked_commits discards without returning (truncation:
/// the entries are gone, their watch events must never fire).
#[test]
fn parked_commits_take_all_returns_in_order_and_clear_discards() {
    let mut c = cache();
    for seq in [1, 2, 3] {
        c.push_parked_commit(parked_pcd(seq));
    }
    let taken: Vec<u64> = c.take_all_parked_commits().iter().map(|p| p.log_metadata.write.wal_seq).collect();
    assert_eq!(taken, [1, 2, 3]);
    assert_eq!(c.parked_commit_count(), 0);
    assert_eq!(c.parked_commit_bytes(), 0);

    for seq in [4, 5] {
        c.push_parked_commit(parked_pcd(seq));
    }
    assert_eq!(c.clear_parked_commits(), 2);
    assert_eq!(c.parked_commit_count(), 0);
    assert_eq!(c.parked_commit_bytes(), 0);
    assert!(c.drain_parked_commits_up_to(u64::MAX).is_empty(), "cleared batches must never drain");
}

/// A sealed slot must exist even when the segment sealed with an EMPTY
/// accumulator: deferred commits for that segment can still be in flight, and
/// update_segment_summary_for_log must route them to the sealed slot — not
/// into the next segment's active accumulator. Fails on the old empty-skip
/// guard (late commits leak into the next segment; the sealed sidecar is lost).
#[test]
fn sealed_slot_stored_when_empty_routes_late_commits_to_sealed_segment() {
    let mut c = cache();
    // Rotation before anything committed into segment 1: accumulator empty.
    c.store_sealed_segment_summary(1);

    // A deferred commit for segment 1 drains after the rotation.
    let mb = test_metablock(agg(1, 1, 1), 1, 5, 100, 1);
    c.update_segment_summary_for_log(1, 2, &mb, 0);

    assert!(
        c.peek_segment_summary().is_empty(),
        "next segment's accumulator must not absorb sealed-segment commits"
    );
    let payload = c.take_sealed_segment_summary(1).expect("sealed slot must exist for the rotated segment");
    assert!(!payload.is_empty(), "the late commit must land in the sealed segment's summary");
}

/// The inflight-cap tripwire reports overflow without dropping the batch —
/// dropping would silently lose read-side commits and their watch events.
#[test]
fn parked_commit_overflow_reports_but_never_drops() {
    // Cap sized for exactly one parked batch.
    let unit = parked_pcd(1).size_bytes();
    let mut c = cache_with(64 * 1024, 64 * 1024, 64 * 1024, unit + unit / 2);
    assert!(!c.push_parked_commit(parked_pcd(1)), "first push fits under the cap");
    assert!(c.push_parked_commit(parked_pcd(2)), "second push must trip the overflow report");
    assert_eq!(c.parked_commit_count(), 2, "overflow must not drop batches");
}

// ── OCC: wal_seq-qualified client_seq cache ──

#[test]
fn get_client_seq_entry_returns_inflight_in_queue() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 100, 42);

    assert_eq!(c.get_client_seq_entry(&k, 100), Some(ClientSeqStatus::InflightInQueue { client_seq: 42 }));
}

#[test]
fn get_client_seq_entry_returns_fsynced_with_wal_seq_zero_for_disk_scan() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let ck = client_key(&k, 100);

    c.put_aggregate_client_into_cache(ck, 42, false);

    assert_eq!(c.get_client_seq_entry(&k, 100), Some(ClientSeqStatus::Fsynced { client_seq: 42, wal_seq: 0 }));
}

#[test]
fn get_client_seq_entry_returns_none_for_unknown_client() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    assert_eq!(c.get_client_seq_entry(&k, 100), None);
}

#[test]
fn get_client_seq_entry_propagates_wal_seq_from_queue_to_lru_on_commit() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 100, 42);
    let mut snapshot = c.take_sync_positions_snapshot();
    snapshot.aggregate_queue_positions.get_mut(&k).unwrap().wal_seq = 137;
    c.commit_sync_positions_snapshot(NodeStatus::Standalone, snapshot);

    assert_eq!(c.get_client_seq_entry(&k, 100), Some(ClientSeqStatus::Fsynced { client_seq: 42, wal_seq: 137 }));
}

#[test]
fn get_client_seq_entry_queue_wins_over_lru() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let ck = client_key(&k, 100);

    c.put_aggregate_client_into_cache(ck, 5, false);
    queue_write(&mut c, &k, 1, 1, 100, 10);

    assert_eq!(c.get_client_seq_entry(&k, 100), Some(ClientSeqStatus::InflightInQueue { client_seq: 10 }));
}

#[test]
fn commit_lru_max_wins_updates_wal_seq_alongside_client_seq() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 100, 5);
    let mut s1 = c.take_sync_positions_snapshot();
    s1.aggregate_queue_positions.get_mut(&k).unwrap().wal_seq = 10;
    c.commit_sync_positions_snapshot(NodeStatus::Standalone, s1);

    queue_write(&mut c, &k, 2, 2, 100, 8);
    let mut s2 = c.take_sync_positions_snapshot();
    s2.aggregate_queue_positions.get_mut(&k).unwrap().wal_seq = 20;
    c.commit_sync_positions_snapshot(NodeStatus::Standalone, s2);

    assert_eq!(c.get_client_seq_entry(&k, 100), Some(ClientSeqStatus::Fsynced { client_seq: 8, wal_seq: 20 }));
}

#[test]
fn commit_lru_lower_client_seq_does_not_overwrite_wal_seq() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 100, 50);
    let mut s1 = c.take_sync_positions_snapshot();
    s1.aggregate_queue_positions.get_mut(&k).unwrap().wal_seq = 100;
    c.commit_sync_positions_snapshot(NodeStatus::Standalone, s1);

    queue_write(&mut c, &k, 2, 2, 100, 30);
    let mut s2 = c.take_sync_positions_snapshot();
    s2.aggregate_queue_positions.get_mut(&k).unwrap().wal_seq = 200;
    c.commit_sync_positions_snapshot(NodeStatus::Standalone, s2);

    assert_eq!(c.get_client_seq_entry(&k, 100), Some(ClientSeqStatus::Fsynced { client_seq: 50, wal_seq: 100 }));
}

// ── Cull: speculative tail surgical clear ──

#[test]
fn cull_drains_pending_replication_and_clears_write_lrus() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 1, 100, 42);
    sync_and_commit_leader(&mut c);
    c.push_pending_replication(test_pending_commit_data());
    c.push_pending_replication(test_pending_commit_data());

    let drained = c.clear_speculative_write_caches_for_cull();

    assert_eq!(drained, 2);
    assert!(c.peek_pending_replication().is_none());
    assert_eq!(c.aggregate_write_snapshots_len(), 0);
    assert_eq!(c.aggregate_write_client_snapshots_len(), 0);
}

#[test]
fn cull_preserves_queue_positions_and_schema_state() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 1, 100, 42);
    c.schema_cache_insert(sk(1, 0), stub_cached_schema());
    c.no_schema_cache_insert(sk(2, 0));
    c.schema_mark_pending(sk(3, 0));

    c.clear_speculative_write_caches_for_cull();

    let indexes = c.get_write_event_seqes(&k);
    assert_eq!(indexes.aggregate_version, 1);
    assert_eq!(indexes.event_seq, 5);

    assert!(c.schema_cache_has_schema(&sk(1, 0)));
    assert!(c.schema_cache_contains(&sk(2, 0)));
    assert!(c.schema_is_pending(&sk(3, 0)));
}

/// A cull that orphans queued PCDs must look like a rollback to their
/// in-flight writers: generation bump fails the write()-side guard, the
/// flag fails the next capture. Otherwise the writer resolves Ok via
/// NoCaptureRaceButOk for a write the cull just destroyed
#[test]
fn cull_with_drained_items_signals_rollback_to_inflight_writers() {
    let mut c = cache();
    let before = c.rollback_generation();

    c.push_pending_replication(test_pending_commit_data());
    c.clear_speculative_write_caches_for_cull();

    assert_eq!(c.rollback_generation(), before.wrapping_add(1));
    assert!(c.take_replication_rollback_flag());
}

#[test]
fn cull_with_nothing_drained_is_signal_free() {
    let mut c = cache();
    let before = c.rollback_generation();

    c.clear_speculative_write_caches_for_cull();

    assert_eq!(c.rollback_generation(), before);
    assert!(!c.take_replication_rollback_flag());
}

#[test]
fn cull_returns_zero_on_empty_and_leaves_read_snapshots_alone() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let snap = MemSnapshotAggregate::found(1, 512, 10, 3, 1);
    c.put_aggregate_into_cache(k.clone(), snap, 100, 42, false, CachePath::Read);

    let drained = c.clear_speculative_write_caches_for_cull();

    assert_eq!(drained, 0);
    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);
}

// ── UniqueSchemaKeys ──

fn sk(major: u64, minor: u64) -> SchemaKey {
    SchemaKey::new(1, 1, major, minor)
}

#[test]
fn unique_schema_keys_empty() {
    let keys = UniqueSchemaKeys::new();
    assert_eq!(keys.iter().count(), 0);
}

#[test]
fn unique_schema_keys_one() {
    let mut keys = UniqueSchemaKeys::new();
    assert!(keys.try_insert(sk(1, 0)));
    assert_eq!(keys.iter().count(), 1);
}

#[test]
fn unique_schema_keys_two() {
    let mut keys = UniqueSchemaKeys::new();
    assert!(keys.try_insert(sk(1, 0)));
    assert!(keys.try_insert(sk(2, 0)));
    assert_eq!(keys.iter().count(), 2);
}

#[test]
fn unique_schema_keys_duplicate() {
    let mut keys = UniqueSchemaKeys::new();
    assert!(keys.try_insert(sk(1, 0)));
    assert!(!keys.try_insert(sk(1, 0)));
    assert_eq!(keys.iter().count(), 1);
}

#[test]
fn unique_schema_keys_overflow() {
    let mut keys = UniqueSchemaKeys::new();
    assert!(keys.try_insert(sk(1, 0)));
    assert!(keys.try_insert(sk(2, 0)));
    assert!(keys.try_insert(sk(3, 0)));
    assert_eq!(keys.iter().count(), 3);
}

#[test]
fn unique_schema_keys_mixed_duplicates() {
    let mut keys = UniqueSchemaKeys::new();
    assert!(keys.try_insert(sk(1, 0)));
    assert!(keys.try_insert(sk(2, 0)));
    assert!(!keys.try_insert(sk(1, 0)));
    assert!(keys.try_insert(sk(3, 0)));
    assert!(!keys.try_insert(sk(2, 0)));
    assert!(!keys.try_insert(sk(3, 0)));
    assert_eq!(keys.iter().count(), 3);
}

// ── Schema Cache ──

fn stub_cached_schema() -> CachedSchema<StubValidator> {
    CachedSchema::Validated(CachedValidator::new(std::rc::Rc::new(StubValidator), 100))
}

#[test]
fn schema_cache_insert_and_get() {
    let mut c = cache();
    let key = sk(1, 0);
    c.schema_cache_insert(key.clone(), stub_cached_schema());
    assert!(c.schema_cache_get(&key).is_some());
    assert!(c.schema_cache_get(&sk(1, 1)).is_none());
}

#[test]
fn no_schema_cache_insert() {
    let mut c = cache();
    let key = sk(1, 0);
    c.no_schema_cache_insert(key.clone());
    assert!(c.schema_cache_contains(&key));
    assert!(!c.schema_cache_has_schema(&key));
}

#[test]
fn schema_insert_evicts_no_schema() {
    let mut c = cache();
    let key = sk(1, 0);
    c.no_schema_cache_insert(key.clone());
    assert!(c.schema_cache_contains(&key));

    c.schema_cache_insert(key.clone(), stub_cached_schema());
    assert!(c.schema_cache_has_schema(&key));
    assert!(c.schema_cache_contains(&key));
}

#[test]
fn schema_has_schema_false_for_no_schema() {
    let mut c = cache();
    let key = sk(1, 0);
    c.no_schema_cache_insert(key.clone());
    assert!(!c.schema_cache_has_schema(&key));
}

#[test]
fn schema_pending_roundtrip() {
    let mut c = cache();
    let key = sk(1, 0);
    assert!(!c.schema_is_pending(&key));
    c.schema_mark_pending(key.clone());
    assert!(c.schema_is_pending(&key));
    assert!(!c.schema_is_pending(&sk(2, 0)));
}

#[test]
fn take_snapshot_drains_pending_schemas() {
    let mut c = cache();
    c.schema_mark_pending(sk(1, 0));
    c.schema_mark_pending(sk(2, 0));

    let snapshot = c.take_sync_positions_snapshot();
    assert_eq!(snapshot.pending_schema_registrations.len(), 2);
    assert!(snapshot.pending_schema_registrations.contains(&sk(1, 0)));
    assert!(snapshot.pending_schema_registrations.contains(&sk(2, 0)));

    assert!(!c.schema_is_pending(&sk(1, 0)));
    assert!(!c.schema_is_pending(&sk(2, 0)));
}

#[test]
fn fsync_rollback_clears_all_schema_state() {
    let mut c = cache();
    c.schema_cache_insert(sk(1, 0), stub_cached_schema());
    c.no_schema_cache_insert(sk(2, 0));
    c.schema_mark_pending(sk(3, 0));

    c.execute_fsync_rollback();

    assert!(!c.schema_cache_has_schema(&sk(1, 0)));
    assert!(!c.schema_cache_contains(&sk(2, 0)));
    assert!(!c.schema_is_pending(&sk(3, 0)));
}

/// Tiny schema caps (2 entries each) so LRU eviction is drivable in a test.
fn cache_tiny_schema() -> ShardMemCache<StubValidator> {
    ShardMemCache::new(64 * 1024, 64 * 1024, 64 * 1024, 400, 2 * 1024 * 1024, 1024 * 1024)
}

/// Register-then-scan race: the absence scan's no_schema conclusion arrives
/// AFTER a concurrent registration already populated schema_cache (the scan's
/// WAL snapshot predates the registration's blocks). The no_schema insert must
/// yield — otherwise it outlives the Validated entry's LRU eviction and
/// `schema_cache_contains` silently skips validation forever.
#[test]
fn no_schema_insert_yields_to_existing_schema_entry() {
    let mut c = cache_tiny_schema();
    let key = sk(1, 0);
    c.schema_cache_insert(key.clone(), stub_cached_schema());
    c.no_schema_cache_insert(key.clone());

    assert!(c.schema_cache_has_schema(&key), "the registration is authoritative");

    // LRU-evict the Validated entry; a shadowed no_schema entry would now answer.
    c.schema_cache_insert(sk(2, 0), stub_cached_schema());
    c.schema_cache_insert(sk(3, 0), stub_cached_schema());
    assert!(!c.schema_cache_has_schema(&key), "scaffolding: eviction happened");
    assert!(
        !c.schema_cache_contains(&key),
        "stale no_schema shadowed the evicted registration — validation would be silently skipped"
    );
}

/// Scan-then-register: the absence conclusion lands first, the registration
/// commits after. The registration pops the stale no_schema entry, and no
/// later eviction may resurrect it.
#[test]
fn schema_insert_pops_stale_no_schema_entry() {
    let mut c = cache_tiny_schema();
    let key = sk(1, 0);
    c.no_schema_cache_insert(key.clone());
    assert!(c.schema_cache_contains(&key) && !c.schema_cache_has_schema(&key), "scaffolding: absence cached");

    c.schema_cache_insert(key.clone(), stub_cached_schema());
    assert!(c.schema_cache_has_schema(&key));

    c.schema_cache_insert(sk(2, 0), stub_cached_schema());
    c.schema_cache_insert(sk(3, 0), stub_cached_schema());
    assert!(!c.schema_cache_has_schema(&key), "scaffolding: eviction happened");
    assert!(
        !c.schema_cache_contains(&key),
        "evicting the registration resurrected the pre-registration no_schema conclusion"
    );
}

/// A NotYetLoaded warmup placeholder is still proof a registration block
/// exists on disk — the absence insert must yield to it too.
#[test]
fn no_schema_insert_yields_to_not_yet_loaded_placeholder() {
    let mut c = cache_tiny_schema();
    let key = sk(1, 0);
    c.schema_cache_insert(key.clone(), CachedSchema::NotYetLoaded);
    c.no_schema_cache_insert(key.clone());
    c.schema_cache_insert(sk(2, 0), stub_cached_schema());
    c.schema_cache_insert(sk(3, 0), stub_cached_schema());
    assert!(!c.schema_cache_contains(&key), "no_schema must not shadow a placeholder for an on-disk registration");
}

#[test]
fn schema_compilation_failed_counts_as_has_schema() {
    let mut c = cache();
    let key = sk(1, 0);
    c.schema_cache_insert(key.clone(), CachedSchema::CompilationFailed("bad".into()));
    assert!(c.schema_cache_has_schema(&key));
    assert!(c.schema_cache_contains(&key));
}

// ── Follower Replication Path (add_to_pending_queue) ──
// These simulate the follower path where replicated items arrive via
// add_to_pending_queue (no aggregate position tracking), then fsync + commit.

/// Simulate the follower's commit_sync flow at the memcache level:
/// commit_sync_positions_snapshot processes aggregate_queue_positions (empty for replicated items),
/// then commit_sync calls commit_position_snapshot for each EventBatch.
fn follower_commit_with_position_snapshots(cache: &mut ShardMemCache<StubValidator>, log_id: u64) {
    let mut snapshot = cache.take_sync_positions_snapshot();
    let pending_items = std::mem::take(&mut snapshot.pending_append_queue);
    cache.commit_sync_positions_snapshot(NodeStatus::Follower { leader_lease_epoch: 1 }, snapshot);

    // This is what commit_sync does after commit_sync_positions_snapshot
    for item in &pending_items {
        if let MetablockKind::EventBatchMetadata(eb) = &item.metablock.wal_metablock_type {
            cache.commit_position_snapshot(eb, log_id, item.metablock_absolute_pos, CachePath::Read);
            cache.commit_position_snapshot(eb, log_id, item.metablock_absolute_pos, CachePath::Write);
        }
    }
}

#[test]
fn follower_event_batch_via_pending_queue_updates_read_snapshot() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let items = vec![
        test_queue_item(k.clone(), 1, 5, 100),
        test_queue_item(k.clone(), 2, 10, 100),
    ];
    c.add_to_pending_queue(items);
    follower_commit_with_position_snapshots(&mut c, 1);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(loaded, "follower read snapshot should be populated after commit");
    assert_eq!(status, AggregateStatus::Found);

    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Read);
    assert_eq!(pos.aggregate_version, 2);
    assert_eq!(pos.event_seq, 10);
}

#[test]
fn follower_event_batch_via_pending_queue_updates_write_snapshot() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let items = vec![test_queue_item(k.clone(), 1, 5, 100)];
    c.add_to_pending_queue(items);
    follower_commit_with_position_snapshots(&mut c, 1);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Write);
    assert!(loaded, "follower write snapshot should be populated after commit");
    assert_eq!(status, AggregateStatus::Found);
}

#[test]
fn follower_soft_trim_after_events_updates_min_aggregate_version() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Write events first so aggregate exists in cache
    let items = vec![
        test_queue_item(k.clone(), 1, 5, 100),
        test_queue_item(k.clone(), 2, 10, 100),
        test_queue_item(k.clone(), 3, 15, 100),
    ];
    c.add_to_pending_queue(items);
    follower_commit_with_position_snapshots(&mut c, 1);

    // Aggregate is now in cache — trim can update min_aggregate_version
    let (loaded, _) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(loaded);

    // Apply trim
    c.update_aggregate_min_aggregate_version(&k, 2, CachePath::Read);
    c.update_aggregate_min_aggregate_version(&k, 2, CachePath::Write);

    for path in [CachePath::Read, CachePath::Write] {
        let pos = c.get_aggregate_last_metablock_pos(&k, path);
        assert_eq!(pos.min_aggregate_version, 2, "min_aggregate_version should be 2 on {:?}", path);
    }
}

// ── Segment Summary Tests ──

fn eb_metablock(key: AggregateKey, batch_idx: u64, server_ts: u64) -> Metablock {
    Metablock {
        wal_seq: 0,
        server_timestamp: server_ts,
        lease_epoch: 1,
        node_id: 1,
        uncompressed_size: 100,
        compressed_size: 50,
        datablock_version: 1,
        datablock_compression_type: 0,
        previous_tip_hash: GENESIS_HASH,
        datablock_position: 0,
        previous_aggregate_metablock_pos: 0,
        wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
            aggregate_key: key,
            aggregate_version: batch_idx,
            trimmed_below_version: 1,
            min_client_seq: 1,
            max_client_seq: 1,
            min_event_timestamp: 100,
            max_event_timestamp: 200,
            min_event_seq: 1,
            max_event_seq: 1,
            client_id: 1,
            user_id: None,
            event_types_data: EventTypesKind::Direct([1, 0, 0, 0]),
        }),
        datablock: DatablockStorageKind::Inline(DatablockInlineData {
            minibatch: [0u8; MINIBATCH_SIZE_BYTES],
        }),
    }
}

fn sd_metablock(key: AggregateKey) -> Metablock {
    Metablock {
        wal_seq: 0,
        server_timestamp: 0,
        lease_epoch: 1,
        node_id: 1,
        uncompressed_size: 0,
        compressed_size: 0,
        datablock_version: 0,
        datablock_compression_type: 0,
        previous_tip_hash: GENESIS_HASH,
        datablock_position: 0,
        previous_aggregate_metablock_pos: 0,
        wal_metablock_type: MetablockKind::SoftDelete(MetablockSoftDelete {
            aggregate_key: key,
            allow_recreate: false,
            allow_sequence_continuation: false,
            aggregate_version: 1,
            event_seq: 1,
            client_id: 1,
            user_id: None,
        }),
        datablock: DatablockStorageKind::None,
    }
}

fn st_metablock(key: AggregateKey, keep_from: u64) -> Metablock {
    Metablock {
        wal_seq: 0,
        server_timestamp: 0,
        lease_epoch: 1,
        node_id: 1,
        uncompressed_size: 0,
        compressed_size: 0,
        datablock_version: 0,
        datablock_compression_type: 0,
        previous_tip_hash: GENESIS_HASH,
        datablock_position: 0,
        previous_aggregate_metablock_pos: 0,
        wal_metablock_type: MetablockKind::SoftTrim(MetablockSoftTrim {
            aggregate_key: key,
            keep_from_aggregate_version: keep_from,
            aggregate_version: 1,
            event_seq: 1,
            client_id: 1,
            user_id: None,
        }),
        datablock: DatablockStorageKind::None,
    }
}

#[test]
fn segment_summary_tracks_event_batch() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&eb_metablock(key.clone(), 1, 1000), 0);
    c.update_segment_summary(&eb_metablock(key.clone(), 2, 2000), 0);

    let summary = c.peek_segment_summary();
    let entry = summary.get(&key).unwrap();
    assert_eq!(entry.event_batch_count, 2);
    assert_eq!(entry.last_aggregate_version, 2);
    assert_eq!(entry.last_server_timestamp, 2000);
    assert_eq!(entry.compressed_size, 100);
    assert_eq!(entry.uncompressed_size, 200);
    assert!(!entry.is_deleted);
}

#[test]
fn segment_summary_soft_delete_resets_counts() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&eb_metablock(key.clone(), 1, 1000), 0);
    c.update_segment_summary(&sd_metablock(key.clone()), 0);

    let summary = c.peek_segment_summary();
    let entry = summary.get(&key).unwrap();
    assert!(entry.is_deleted);
    assert_eq!(entry.event_batch_count, 0);
    assert_eq!(entry.compressed_size, 0);
    assert_eq!(entry.uncompressed_size, 0);
}

#[test]
fn segment_summary_soft_trim_updates_trimmed_below_version() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&eb_metablock(key.clone(), 1, 1000), 0);
    c.update_segment_summary(&st_metablock(key.clone(), 5), 0);

    let entry = c.peek_segment_summary().get(&key).unwrap();
    assert_eq!(entry.min_aggregate_version, 5);
}

#[test]
fn segment_summary_soft_trim_ignored_for_unknown_aggregate() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&st_metablock(key.clone(), 5), 0);
    assert!(c.peek_segment_summary().is_empty());
}

#[test]
fn take_segment_summary_returns_and_clears() {
    let mut c = cache();
    let k1 = agg(1, 2, 3);
    let k2 = agg(1, 4, 5);
    c.update_segment_summary(&eb_metablock(k1, 1, 1000), 0);
    c.update_segment_summary(&eb_metablock(k2, 1, 1000), 0);

    let payload = c.take_segment_summary();
    assert_eq!(payload.orgs.len(), 1);
    assert_eq!(payload.aggregate_types.len(), 2);
    assert_eq!(payload.aggregates.len(), 2);

    // State is cleared
    assert!(c.peek_segment_summary().is_empty());
    let empty = c.take_segment_summary();
    assert!(empty.aggregates.is_empty());
}

#[test]
fn segment_summary_preserved_on_fsync_rollback() {
    let mut c = cache();
    c.update_segment_summary(&eb_metablock(agg(1, 2, 3), 1, 1000), 0);
    assert!(!c.peek_segment_summary().is_empty());
    c.execute_fsync_rollback();
    assert!(!c.peek_segment_summary().is_empty());
}

#[test]
fn peek_segment_summary_orgs_returns_unique_orgs() {
    let mut c = cache();
    c.update_segment_summary(&eb_metablock(agg(1, 2, 3), 1, 1000), 0);
    c.update_segment_summary(&eb_metablock(agg(1, 4, 5), 1, 1000), 0);
    c.update_segment_summary(&eb_metablock(agg(7, 8, 9), 1, 1000), 0);

    let orgs = c.peek_segment_summary_orgs();
    assert_eq!(orgs.len(), 2);
    assert!(orgs.contains(&1));
    assert!(orgs.contains(&7));
}

#[test]
fn peek_segment_summary_types_returns_unique_types() {
    let mut c = cache();
    c.update_segment_summary(&eb_metablock(agg(1, 2, 3), 1, 1000), 0);
    c.update_segment_summary(&eb_metablock(agg(1, 2, 4), 1, 1000), 0);
    c.update_segment_summary(&eb_metablock(agg(1, 5, 6), 1, 1000), 0);

    let types = c.peek_segment_summary_types();
    assert_eq!(types.len(), 2);
    assert!(types.contains(&AggregateTypeKey::new(1, 2)));
    assert!(types.contains(&AggregateTypeKey::new(1, 5)));
}

#[test]
fn take_segment_summary_clears_segment_state() {
    let mut c = cache();
    c.update_segment_summary(&eb_metablock(agg(1, 2, 3), 1, 1000), 0);
    c.update_segment_summary(&eb_metablock(agg(4, 5, 6), 1, 1000), 0);

    let _payload = c.take_segment_summary();

    assert!(c.peek_segment_summary_orgs().is_empty());
    assert!(c.peek_segment_summary_types().is_empty());
    assert!(c.peek_segment_summary().is_empty());
}

#[test]
fn fsync_rollback_preserves_segment_summary() {
    let mut c = cache();
    c.update_segment_summary(&eb_metablock(agg(1, 2, 3), 1, 1000), 0);
    c.update_segment_summary(&eb_metablock(agg(4, 5, 6), 1, 1000), 0);

    c.execute_fsync_rollback();

    assert_eq!(c.peek_segment_summary_orgs().len(), 2);
    assert_eq!(c.peek_segment_summary_types().len(), 2);
    assert_eq!(c.peek_segment_summary().len(), 2);
}

// ── Segment summary v3: per-segment schema set ──

fn schema_metablock(major: u64, minor: u64) -> Metablock {
    Metablock {
        wal_seq: 0,
        server_timestamp: 0,
        lease_epoch: 1,
        node_id: 1,
        uncompressed_size: 0,
        compressed_size: 0,
        datablock_version: 0,
        datablock_compression_type: 0,
        previous_tip_hash: GENESIS_HASH,
        datablock_position: 0,
        previous_aggregate_metablock_pos: 0,
        wal_metablock_type: MetablockKind::SchemaRegistration(
            celeriant_wal::metablocks::metablock_schema_registration::MetablockSchemaRegistration {
                schema_key: SchemaKey::new(1, 2, major, minor),
                client_id: 1,
                user_id: None,
            },
        ),
        datablock: DatablockStorageKind::None,
    }
}

/// A schema registration must land in the payload's schema set — and ONLY
/// there: it carries no aggregate, so no aggregate entry may appear.
#[test]
fn take_segment_summary_collects_schema_hashes() {
    let mut c = cache();
    c.update_segment_summary(&schema_metablock(3, 4), 0);
    c.update_segment_summary(&eb_metablock(agg(1, 2, 3), 1, 1000), 0);

    let payload = c.take_segment_summary();
    assert_eq!(payload.aggregates.len(), 1, "the registration must not create an aggregate entry");
    let hash = SchemaKey::new(1, 2, 3, 4).bloom_hash();
    assert!(payload.schema_may_contain_hash(hash));
    assert!(!payload.schema_may_contain_hash(hash ^ 1), "the bloom answers definite absence for a non-member");

    // Drained: the next segment starts schema-less — an empty bloom, not None.
    let next = c.take_segment_summary();
    assert!(next.schema_bloom.as_ref().is_some_and(|w| w.iter().all(|b| *b == 0)));
    assert!(!next.schema_may_contain_hash(hash));
}

/// The FullCommit seal window: `take_segment_summary` drains the accumulator
/// while `active_log_id` still names the sealing segment (the fiber parks at
/// the sidecar and rotation awaits). Until the rotation is reported complete,
/// the consult must answer maybe-present — a drained, untainted accumulator
/// claiming absence would hide the sealing segment's committed registrations.
#[test]
fn seal_drain_latches_active_consult_until_rotation_completes() {
    let mut c = cache();
    let hash = SchemaKey::new(1, 2, 3, 4).bloom_hash();
    c.update_segment_summary(&schema_metablock(3, 4), 0);

    let _seal_payload = c.take_segment_summary();
    assert!(c.active_segment_may_contain_schema(hash), "mid-seal window must not answer absence");
    assert!(c.active_segment_may_contain_schema(hash ^ 1), "for ANY hash, not just drained ones");

    c.note_active_segment_rotated();
    assert!(!c.active_segment_may_contain_schema(hash), "post-rotation the fresh accumulator answers absence");
}

/// An incomplete accumulator is a subset; its schema bloom must persist as
/// None — a bloom would claim absences it cannot prove.
#[test]
fn incomplete_accumulator_persists_no_schema_bloom() {
    let mut c = cache();
    c.update_segment_summary(&schema_metablock(3, 4), 0);
    c.mark_segment_summary_incomplete();
    let payload = c.take_segment_summary();
    assert_eq!(payload.schema_bloom, None);
}

/// Schemas ride the sealed slot like the rest of the accumulator: staged at
/// rotation, still fed by late deferred commits, drained into the sidecar payload.
#[test]
fn sealed_slot_carries_schema_hashes_including_late_commits() {
    let mut c = cache();
    c.update_segment_summary(&schema_metablock(3, 4), 0);
    c.store_sealed_segment_summary(1);

    // Deferred commit for the sealed segment after rotation.
    c.update_segment_summary_for_log(1, 2, &schema_metablock(5, 6), 0);

    let payload = c.take_sealed_segment_summary(1).unwrap();
    assert!(payload.schema_may_contain_hash(SchemaKey::new(1, 2, 3, 4).bloom_hash()));
    assert!(payload.schema_may_contain_hash(SchemaKey::new(1, 2, 5, 6).bloom_hash()));
    // Drained but rotation not yet reported: the seal window must not answer
    // absence (active_log_id still names the sealing segment).
    assert!(c.active_segment_may_contain_schema(SchemaKey::new(1, 2, 3, 4).bloom_hash()));
    c.note_active_segment_rotated();
    assert!(!c.active_segment_may_contain_schema(SchemaKey::new(1, 2, 3, 4).bloom_hash()),
        "after rotation the drained set answers for the new segment");
}

/// The active-segment consult: absence is answerable only while the
/// accumulator is taint-free.
#[test]
fn active_segment_schema_consult_respects_taint() {
    let mut c = cache();
    let hash = SchemaKey::new(1, 2, 3, 4).bloom_hash();
    assert!(!c.active_segment_may_contain_schema(hash), "fresh accumulator proves absence");

    c.update_segment_summary(&schema_metablock(3, 4), 0);
    assert!(c.active_segment_may_contain_schema(hash));
    assert!(!c.active_segment_may_contain_schema(hash ^ 1));

    c.mark_segment_summary_incomplete();
    assert!(c.active_segment_may_contain_schema(hash ^ 1), "a tainted accumulator must not prove absence");
}

/// The unwind reset discards the schema hashes with the rest of the accumulator
/// and taints it: the re-activated file may hold registrations it never saw.
#[test]
fn unwind_reset_clears_schema_hashes_and_taints() {
    let mut c = cache();
    c.update_segment_summary(&schema_metablock(3, 4), 0);
    c.reset_segment_summary_after_unwind();
    assert!(c.active_segment_may_contain_schema(0xABCD), "post-unwind consult must degrade to walk");
    assert_eq!(c.take_segment_summary().schema_bloom, None);
}

// ── Segment summary v2: client sets + tip index ──

fn with_client(mut mb: Metablock, client_id: u128) -> Metablock {
    match &mut mb.wal_metablock_type {
        MetablockKind::EventBatchMetadata(eb) => eb.client_id = client_id,
        MetablockKind::SoftDelete(sd) => sd.client_id = client_id,
        MetablockKind::SoftTrim(st) => st.client_id = client_id,
        MetablockKind::SchemaRegistration(sr) => sr.client_id = client_id,
    }
    mb
}

fn client_hash(client_id: u128) -> u64 {
    celeriant_wal::aggregate_client_key::client_id_bloom_hash(client_id)
}

/// Every aggregate-scoped client-bearing kind must feed the client set: a
/// delete-only or trim-only client left out makes the set a subset — a false
/// "absent" and a possible replay.
#[test]
fn take_segment_summary_client_set_includes_delete_and_trim_clients() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&with_client(eb_metablock(key.clone(), 1, 1000), 7), 0);
    c.update_segment_summary(&with_client(st_metablock(key.clone(), 1), 13), 0);
    c.update_segment_summary(&with_client(sd_metablock(key.clone()), 11), 0);

    let payload = c.take_segment_summary();
    let entry = payload.aggregates.iter().find(|e| e.aggregate_id == 3).unwrap();
    let set = &entry.client_set;
    assert_eq!(set.cardinality(), Some(3), "expected Exact set of writer+trimmer+deleter, got {set:?}");
    assert!(set.may_contain_hash(client_hash(7)), "writer must be present");
    assert!(set.may_contain_hash(client_hash(13)), "trim-only client must be present");
    assert!(set.may_contain_hash(client_hash(11)), "delete-only client must be present");
    assert!(!set.may_contain_hash(client_hash(99)), "never-seen client must be definitely absent");
}

#[test]
fn take_segment_summary_converts_to_bloom_above_exact_threshold() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    for client in 1..=33u128 {
        c.update_segment_summary(&with_client(eb_metablock(key.clone(), client as u64, 1000), client), 0);
    }

    let payload = c.take_segment_summary();
    let entry = payload.aggregates.iter().find(|e| e.aggregate_id == 3).unwrap();
    assert!(
        matches!(entry.client_set, ClientSet::Bloom(_)),
        "33 distinct clients must convert to a bloom, got {:?}", entry.client_set
    );
    for client in 1..=33u128 {
        assert!(entry.client_set.may_contain_hash(client_hash(client)), "no false absent for client {client}");
    }
}

/// The fold runs in write order, so the newest position wins — including
/// tombstone kinds, which are chain members and valid seek targets.
#[test]
fn segment_summary_newest_metablock_pos_last_wins() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&eb_metablock(key.clone(), 1, 1000), 397_312);
    c.update_segment_summary(&eb_metablock(key.clone(), 2, 2000), 398_336);
    assert_eq!(c.peek_segment_summary().get(&key).unwrap().newest_metablock_pos, 398_336);

    c.update_segment_summary(&st_metablock(key.clone(), 2), 399_360);
    assert_eq!(c.peek_segment_summary().get(&key).unwrap().newest_metablock_pos, 399_360, "trim is the newest chain member");

    c.update_segment_summary(&sd_metablock(key.clone()), 400_384);
    assert_eq!(c.peek_segment_summary().get(&key).unwrap().newest_metablock_pos, 400_384, "delete is the newest chain member");
}

/// Trim on an aggregate with no other record in the segment stays entry-less:
/// consumers then skip the segment, which matches today's scan (client seqs are
/// only read from EventBatch blocks).
#[test]
fn segment_summary_trim_only_aggregate_stays_absent() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&with_client(st_metablock(key.clone(), 5), 13), 4096);
    assert!(c.peek_segment_summary().is_empty());
    assert!(c.take_segment_summary().is_empty());
}

/// The sealed-slot fold must collect the same client/tip data as the active fold.
#[test]
fn sealed_segment_summary_collects_clients_and_positions() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&with_client(eb_metablock(key.clone(), 1, 1000), 7), 397_312);
    c.store_sealed_segment_summary(1);

    // Late deferred commits for the sealed segment: a delete by another client.
    c.update_segment_summary_for_log(1, 2, &with_client(sd_metablock(key.clone()), 11), 398_336);

    let payload = c.take_sealed_segment_summary(1).unwrap();
    let entry = payload.aggregates.iter().find(|e| e.aggregate_id == 3).unwrap();
    assert_eq!(entry.newest_metablock_pos, 398_336);
    assert!(entry.client_set.may_contain_hash(client_hash(7)));
    assert!(entry.client_set.may_contain_hash(client_hash(11)));
    assert!(!entry.client_set.may_contain_hash(client_hash(99)));
    assert_eq!(entry.client_set.cardinality(), Some(2));
}

/// Rotation must move the active accumulator's client hashes into the sealed
/// slot — losing them would seal Unknown sets and forfeit the skip.
#[test]
fn store_sealed_segment_summary_moves_client_hashes() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&with_client(eb_metablock(key.clone(), 1, 1000), 7), 4096);
    c.store_sealed_segment_summary(1);

    let payload = c.take_sealed_segment_summary(1).unwrap();
    assert_eq!(payload.aggregates[0].client_set.cardinality(), Some(1));

    // And the next active segment starts clean.
    c.update_segment_summary(&with_client(eb_metablock(key.clone(), 2, 2000), 8), 8192);
    let next = c.take_segment_summary();
    let set = &next.aggregates[0].client_set;
    assert!(!set.may_contain_hash(client_hash(7)), "previous segment's client must not leak into the next");
    assert!(set.may_contain_hash(client_hash(8)));
}

/// The sealed slot's payload carries right-sized segment blooms built from the
/// slot's exact key knowledge: one 32-byte block for one key, containing the
/// key/client and answering absence for a non-member.
#[test]
fn take_sealed_segment_summary_builds_sized_blooms() {
    use celeriant_wal::sbbf;
    let mut c = cache();
    let key = agg(1, 2, 3);
    c.update_segment_summary(&with_client(eb_metablock(key.clone(), 1, 1000), 7), 4096);
    c.store_sealed_segment_summary(1);

    let payload = c.take_sealed_segment_summary(1).unwrap();
    let agg_words = payload.aggregate_bloom.expect("complete slot must persist an aggregate bloom");
    let client_words = payload.client_bloom.expect("complete slot must persist a client bloom");
    assert_eq!(agg_words.len() * 8, 32, "one key sizes to a single SBBF block, not 256 KiB");
    assert_eq!(client_words.len() * 8, 32, "one client sizes to a single SBBF block, not 128 KiB");
    assert!(sbbf::contains(&agg_words, key.bloom_hash()));
    assert!(!sbbf::contains(&agg_words, agg(9, 9, 9).bloom_hash()), "sized bloom answers definite absence");
    assert!(sbbf::contains(&client_words, client_hash(7)));
    assert!(!sbbf::contains(&client_words, client_hash(99)));
}

/// A trim-only aggregate gets no summary entry, but its key and client MUST
/// land in the segment blooms: the aggregate-load scan reads the trim floor,
/// and a bloom skip would hand out a stale one.
#[test]
fn sized_blooms_cover_trim_only_keys_and_clients() {
    use celeriant_wal::sbbf;
    let mut c = cache();
    let trimmed = agg(1, 2, 42);
    // SoftTrim with no other block for this aggregate in the segment.
    c.update_segment_summary(&with_client(st_metablock(trimmed.clone(), 5), 13), 4096);
    c.update_segment_summary(&with_client(eb_metablock(agg(1, 2, 3), 1, 1000), 7), 4096);

    let payload = c.take_segment_summary();
    assert!(payload.aggregates.iter().all(|e| e.aggregate_id != 42), "trim-only aggregate stays out of the summary");
    let agg_words = payload.aggregate_bloom.unwrap();
    let client_words = payload.client_bloom.unwrap();
    assert!(sbbf::contains(&agg_words, trimmed.bloom_hash()), "trim-only key must not be bloom-skippable");
    assert!(sbbf::contains(&client_words, client_hash(13)), "trim-only client must not be bloom-skippable");
}

/// An incomplete accumulator persists NO segment blooms — any bloom built from
/// a subset could answer a false "absent".
#[test]
fn incomplete_accumulator_persists_no_segment_blooms() {
    let mut c = cache();
    c.update_segment_summary(&with_client(eb_metablock(agg(1, 2, 3), 1, 1000), 7), 4096);
    c.mark_segment_summary_incomplete();
    let payload = c.take_segment_summary();
    assert_eq!(payload.aggregate_bloom, None);
    assert_eq!(payload.client_bloom, None);
}

// ── Segment summary completeness taint ──

/// The taint propagates into every drained payload and resets at rotation: an
/// incomplete accumulator seals `complete: false` (its aggregate list is a
/// subset — consumers must never Skip on it), while the next segment's fold
/// sees every commit from birth and seals complete again.
#[test]
fn segment_summary_incomplete_taint_propagates_and_resets_at_rotation() {
    let key = agg(1, 2, 3);

    // Direct seal (FullCommit path): taint carried, then reset.
    let mut c = cache();
    c.update_segment_summary(&eb_metablock(key.clone(), 1, 1000), 4096);
    c.mark_segment_summary_incomplete();
    assert!(!c.take_segment_summary().complete, "taint must reach the sealed payload");
    c.update_segment_summary(&eb_metablock(key.clone(), 2, 2000), 4096);
    assert!(c.take_segment_summary().complete, "a fresh segment's fold seals complete");

    // Deferred seal (rotation path): taint carried through the sealed slot,
    // the next active accumulator starts complete.
    let mut c = cache();
    c.update_segment_summary(&eb_metablock(key.clone(), 1, 1000), 4096);
    c.mark_segment_summary_incomplete();
    c.store_sealed_segment_summary(1);
    c.update_segment_summary(&eb_metablock(key.clone(), 2, 2000), 4096);
    assert!(!c.take_sealed_segment_summary(1).unwrap().complete, "sealed slot must preserve the taint");
    assert!(c.take_segment_summary().complete, "rotation resets the active accumulator to complete");
}

/// After a truncate unwinds onto a re-activated sealed segment, the active
/// accumulator is discarded (it described the deleted segment) and tainted:
/// the re-activated file holds commits the accumulator never saw.
#[test]
fn reset_after_unwind_discards_accumulator_and_taints() {
    let mut c = cache();
    c.update_segment_summary(&with_client(eb_metablock(agg(1, 2, 3), 1, 1000), 7), 4096);

    c.reset_segment_summary_after_unwind();

    // Post-truncate surviving commits fold into the tainted accumulator.
    c.update_segment_summary(&with_client(eb_metablock(agg(1, 2, 4), 1, 1000), 8), 8192);
    let payload = c.take_segment_summary();
    assert!(!payload.complete, "a re-activated segment's summary must never authorize skips");
    assert!(payload.aggregates.iter().all(|e| e.aggregate_id != 3), "discarded segment's folds must not survive the unwind");
    assert!(payload.aggregates.iter().any(|e| e.aggregate_id == 4));
}

/// A commit for a sealed segment whose slot is gone must be dropped, never
/// folded into the active accumulator: its positions are cross-file and would
/// break the SeekTo same-file proof.
#[test]
fn update_for_non_active_log_without_slot_is_dropped() {
    let mut c = cache();
    c.update_segment_summary_for_log(1, 2, &eb_metablock(agg(1, 2, 3), 1, 1000), 4096);
    assert!(c.peek_segment_summary().is_empty(), "cross-segment commit must not pollute the active accumulator");
    assert!(c.take_sealed_segment_summary(1).is_none());
}

// ── Compaction position remap ──

/// After a segment is compacted, cached tips in it are remapped to the new offset,
/// evicted if the block was dropped, and tips in other segments are left untouched.
#[test]
fn remap_compacted_positions_remaps_evicts_and_guards_log_id() {
    let mut c = cache();
    let kept = agg(1, 1, 1); // tip in compacted segment, survives → remap
    let dropped = agg(1, 1, 2); // tip in compacted segment, gone → evict
    let newer = agg(1, 1, 3); // tip in a newer segment → must be left alone

    let eb_kept = test_event_batch(kept.clone(), 1, 1);
    let eb_dropped = test_event_batch(dropped.clone(), 1, 1);
    let eb_newer = test_event_batch(newer.clone(), 1, 1);

    for path in [CachePath::Read, CachePath::Write] {
        c.commit_position_snapshot(&eb_kept, 5, 1000, path);
        c.commit_position_snapshot(&eb_dropped, 5, 2000, path);
        c.commit_position_snapshot(&eb_newer, 6, 3000, path);
    }

    let mut new_tips = HashMap::new();
    new_tips.insert(kept.clone(), 1500);
    new_tips.insert(newer.clone(), 9999); // present in tips but lives in log 6: must be ignored

    c.remap_compacted_positions(5, &new_tips);

    for path in [CachePath::Read, CachePath::Write] {
        let k = c.get_aggregate_snapshot(&kept, path).expect("kept tip remapped, not evicted");
        assert_eq!((k.log_id, k.metablock_absolute_pos), (5, 1500));

        assert!(c.get_aggregate_snapshot(&dropped, path).is_none(), "dropped tip must be evicted");

        let n = c.get_aggregate_snapshot(&newer, path).expect("newer-segment tip untouched");
        assert_eq!((n.log_id, n.metablock_absolute_pos), (6, 3000), "only target_log_id is remapped");
    }
}


// ── Negative-lookup per-aggregate client blooms ──

use crate::shard_mem_cache::NegativeLookupAnswer;

fn cache_with_negative_budget(budget: u64) -> ShardMemCache<StubValidator> {
    ShardMemCache::new(64 * 1024, 64 * 1024, 64 * 1024, 4 * 1024 * 1024, budget, 1024 * 1024)
}

fn complete_bloom_for(c: &mut ShardMemCache<StubValidator>, key: &AggregateKey) {
    let generation = c.negative_lookup_try_begin_build(key).expect("begin build");
    assert!(c.negative_lookup_finish_build(key, generation, true));
}

/// AUDIT SURFACE: every commit route funnels through the segment-summary fold,
/// which must insert every client-bearing kind into the resident bloom —
/// including a SoftTrim for an aggregate with no summary entry in the segment
/// (the fold skips its client map, the bloom must not).
#[test]
fn commit_fold_inserts_all_client_bearing_kinds_into_resident_bloom() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    complete_bloom_for(&mut c, &key);
    assert_eq!(c.negative_lookup_check(&key, client_hash(7)), NegativeLookupAnswer::DefinitelyAbsent);

    // Trim FIRST, while the aggregate has no entry in the segment accumulator.
    c.update_segment_summary(&with_client(st_metablock(key.clone(), 1), 13), 4096);
    assert_eq!(
        c.negative_lookup_check(&key, client_hash(13)),
        NegativeLookupAnswer::MaybePresent,
        "trim-only client with no summary entry must still land in the bloom",
    );

    c.update_segment_summary(&with_client(eb_metablock(key.clone(), 1, 1000), 7), 4096);
    c.update_segment_summary(&with_client(sd_metablock(key.clone()), 11), 4096);
    assert_eq!(c.negative_lookup_check(&key, client_hash(7)), NegativeLookupAnswer::MaybePresent);
    assert_eq!(c.negative_lookup_check(&key, client_hash(11)), NegativeLookupAnswer::MaybePresent);
    assert_eq!(c.negative_lookup_check(&key, client_hash(99)), NegativeLookupAnswer::DefinitelyAbsent);
}

/// The sealed-slot and slotless commit routes insert too — a late deferred
/// commit is durable data whether or not its summary slot survived.
#[test]
fn commit_fold_for_sealed_and_slotless_routes_inserts_into_bloom() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    complete_bloom_for(&mut c, &key);

    c.store_sealed_segment_summary(1);
    c.update_segment_summary_for_log(1, 2, &with_client(eb_metablock(key.clone(), 1, 1000), 21), 4096);
    assert_eq!(c.negative_lookup_check(&key, client_hash(21)), NegativeLookupAnswer::MaybePresent, "sealed-slot route");

    c.update_segment_summary_for_log(7, 9, &with_client(eb_metablock(key.clone(), 2, 2000), 22), 4096);
    assert_eq!(c.negative_lookup_check(&key, client_hash(22)), NegativeLookupAnswer::MaybePresent, "slotless route");
}

/// Install-empty-then-populate: from the instant `try_begin_build` returns,
/// commit-side inserts land in the Building entry, and it never answers absent.
#[test]
fn building_entry_absorbs_commits_and_never_answers_absent() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    assert_eq!(c.negative_lookup_check(&key, client_hash(1)), NegativeLookupAnswer::NoEntry);
    let generation = c.negative_lookup_try_begin_build(&key).expect("begin build");
    assert!(c.negative_lookup_try_begin_build(&key).is_none(), "one build in flight per aggregate");
    assert_eq!(c.negative_lookup_check(&key, client_hash(1)), NegativeLookupAnswer::Building);

    c.update_segment_summary(&with_client(eb_metablock(key.clone(), 1, 1000), 42), 4096);
    assert!(c.negative_lookup_finish_build(&key, generation, true));
    assert_eq!(c.negative_lookup_check(&key, client_hash(42)), NegativeLookupAnswer::MaybePresent, "concurrent commit must survive completion");
    assert_eq!(c.negative_lookup_check(&key, client_hash(1)), NegativeLookupAnswer::DefinitelyAbsent);
}

/// A parked (incomplete) build stays Building, keeps its members, and can be
/// resumed by a later miss.
#[test]
fn incomplete_build_parks_and_resumes() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    let generation = c.negative_lookup_try_begin_build(&key).expect("begin build");
    c.negative_lookup_insert(&key, client_hash(5));
    assert!(!c.negative_lookup_finish_build(&key, generation, false), "truncated walk must not complete");
    assert_eq!(c.negative_lookup_check(&key, client_hash(9)), NegativeLookupAnswer::Building);

    let resumed = c.negative_lookup_try_begin_build(&key).expect("parked build must be resumable");
    assert!(c.negative_lookup_finish_build(&key, resumed, true));
    assert_eq!(c.negative_lookup_check(&key, client_hash(5)), NegativeLookupAnswer::MaybePresent, "members collected before the park survive");
    assert_eq!(c.negative_lookup_check(&key, client_hash(9)), NegativeLookupAnswer::DefinitelyAbsent);
}

/// The byte budget bounds the cache at all times; eviction drops whole entries
/// (LRU first) and a dropped aggregate simply rebuilds on its next miss.
#[test]
fn negative_lookup_byte_budget_is_respected() {
    let budget = 2048;
    let mut c = cache_with_negative_budget(budget);
    for i in 0..100u128 {
        let key = agg(1, 2, i);
        let generation = c.negative_lookup_try_begin_build(&key).expect("begin build");
        for client in 0..10u128 {
            c.negative_lookup_insert(&key, client_hash(client));
        }
        c.negative_lookup_finish_build(&key, generation, true);
        assert!(c.negative_lookup_bytes() <= budget, "budget exceeded at aggregate {i}: {}", c.negative_lookup_bytes());
    }
    assert!(c.negative_lookup_len() < 100, "eviction must have fired");
    assert_eq!(c.negative_lookup_check(&agg(1, 2, 0), client_hash(0)), NegativeLookupAnswer::NoEntry, "evicted LRU entry rebuilds on next miss");
}

/// Eager seed: installs Complete only when told the history is confined,
/// merges into existing entries without changing their state, and stops
/// installing new entries past the budget.
#[test]
fn negative_lookup_seed_semantics() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    let hashes: std::collections::HashSet<u64> = [client_hash(1), client_hash(2)].into();
    assert!(c.negative_lookup_seed(&key, &hashes, true));
    assert_eq!(c.negative_lookup_check(&key, client_hash(1)), NegativeLookupAnswer::MaybePresent);
    assert_eq!(c.negative_lookup_check(&key, client_hash(9)), NegativeLookupAnswer::DefinitelyAbsent);

    // Merge into existing Complete entry: no state change, hashes inserted.
    let more: std::collections::HashSet<u64> = [client_hash(3)].into();
    assert!(!c.negative_lookup_seed(&key, &more, false));
    assert_eq!(c.negative_lookup_check(&key, client_hash(3)), NegativeLookupAnswer::MaybePresent);

    // Building seed stays Building.
    let key2 = agg(1, 2, 4);
    assert!(!c.negative_lookup_seed(&key2, &hashes, false));
    assert_eq!(c.negative_lookup_check(&key2, client_hash(1)), NegativeLookupAnswer::Building);

    // Budget stop: a tiny budget refuses new installs but keeps merging.
    let mut small = cache_with_negative_budget(200);
    assert!(small.negative_lookup_seed(&agg(9, 9, 1), &hashes, true));
    assert!(!small.negative_lookup_seed(&agg(9, 9, 2), &hashes, true), "past the budget no new entries install");
    assert_eq!(small.negative_lookup_check(&agg(9, 9, 2), client_hash(1)), NegativeLookupAnswer::NoEntry);
}

/// Sidecar-union soundness at the cache level: Exact hashes insert into the
/// main bloom; Bloom words are carried and OR-ed; refusal parks completeness
/// decisions with the caller.
#[test]
fn negative_lookup_sidecar_unions() {
    let mut c = cache();
    let key = agg(1, 2, 3);
    let generation = c.negative_lookup_try_begin_build(&key).expect("begin build");
    c.negative_lookup_union_exact(&key, &[client_hash(7), client_hash(8)]);
    let mut words = vec![0u64; 8];
    celeriant_wal::sbbf::insert(&mut words, client_hash(9));
    assert!(c.negative_lookup_union_bloom(&key, &words));
    assert!(!c.negative_lookup_union_bloom(&key, &[0u64; 3]), "malformed sidecar words prove nothing");
    assert!(c.negative_lookup_finish_build(&key, generation, true));
    for client in [7u128, 8, 9] {
        assert_eq!(c.negative_lookup_check(&key, client_hash(client)), NegativeLookupAnswer::MaybePresent, "client {client}");
    }
    assert_eq!(c.negative_lookup_check(&key, client_hash(99)), NegativeLookupAnswer::DefinitelyAbsent);
}

/// An entry with an active builder is pinned: eviction pressure must never drop
/// it mid-build (the begin-latch rides on residency), and a competing install is
/// refused when nothing evictable remains. Completion unpins.
#[test]
fn building_entries_are_pinned_against_eviction() {
    // Budget fits one base entry (ENTRY_BASE_BYTES), not two.
    let mut c = cache_with_negative_budget(300);
    let k1 = agg(1, 2, 1);
    let generation = c.negative_lookup_try_begin_build(&k1).expect("begin build");
    assert!(
        c.negative_lookup_try_begin_build(&agg(1, 2, 2)).is_none(),
        "install over budget must be refused while the only evictable entry is pinned",
    );
    assert!(c.negative_lookup_try_begin_build(&agg(1, 2, 3)).is_none());
    assert_eq!(
        c.negative_lookup_check(&k1, client_hash(1)),
        NegativeLookupAnswer::Building,
        "the in-flight build must survive eviction pressure",
    );
    assert!(c.negative_lookup_try_begin_build(&k1).is_none(), "the surviving entry must still hold the one-builder latch");

    // Completion unpins: the entry is evictable again for later installs.
    assert!(c.negative_lookup_finish_build(&k1, generation, true));
    assert!(c.negative_lookup_try_begin_build(&agg(1, 2, 2)).is_some(), "an unpinned Complete entry must be evictable for new installs");
    assert!(c.negative_lookup_bytes() <= 300, "budget must hold throughout");
}

/// The finish/park mutators are builder-identity-sensitive: a dead builder's
/// late finish (either flavor) must never complete, park, or unlatch an entry
/// it no longer owns. Member/aux inserts stay identity-blind on purpose —
/// inserting into any resident entry is superset-safe.
#[test]
fn stale_builder_finish_never_completes_foreign_entry() {
    let mut c = cache();
    let k = agg(1, 2, 3);
    // Builder A begins, collects a member, parks (the guard-drop path).
    let gen_a = c.negative_lookup_try_begin_build(&k).expect("begin build A");
    c.negative_lookup_insert(&k, client_hash(1));
    assert!(!c.negative_lookup_finish_build(&k, gen_a, false));

    // Builder B resumes and unions a sidecar bloom before finishing.
    let gen_b = c.negative_lookup_try_begin_build(&k).expect("resume as B");
    assert_ne!(gen_a, gen_b, "each builder gets its own generation");
    let mut words = vec![0u64; 8];
    celeriant_wal::sbbf::insert(&mut words, client_hash(2));
    assert!(c.negative_lookup_union_bloom(&k, &words));

    // Dead builder A's guard fires late: the park must no-op — B stays latched
    // (an unlatch would let a resume wipe B's unioned aux via reset_aux) …
    c.negative_lookup_finish_build(&k, gen_a, false);
    assert!(c.negative_lookup_try_begin_build(&k).is_none(), "stale park unlatched the active builder");

    // … and a stale complete must never mark B's half-built entry Complete.
    assert!(!c.negative_lookup_finish_build(&k, gen_a, true), "stale finish completed a foreign half-built entry");
    assert_eq!(
        c.negative_lookup_check(&k, client_hash(9)),
        NegativeLookupAnswer::Building,
        "entry must stay Building until ITS builder finishes",
    );

    // B (the live builder) completes; members and aux from both eras answer.
    assert!(c.negative_lookup_finish_build(&k, gen_b, true));
    assert_eq!(c.negative_lookup_check(&k, client_hash(1)), NegativeLookupAnswer::MaybePresent);
    assert_eq!(
        c.negative_lookup_check(&k, client_hash(2)),
        NegativeLookupAnswer::MaybePresent,
        "the stale park wiped the live builder's unioned aux",
    );
    assert_eq!(c.negative_lookup_check(&k, client_hash(9)), NegativeLookupAnswer::DefinitelyAbsent);
}

// ADVERSARIAL EVIDENCE — a seal-retry double-store must merge, not replace.
//
// Reachable trace: on the deferred seal branch, `commit_fsync_with_rollback`
// calls `store_sealed_segment_summary(old_log_id)`, then awaits
// `rotate_to_next_log()`. If rotation fails (ENOSPC/IO — the disk-full
// moment IS when segments seal), the error propagates WITHOUT clearing the
// staged slot or re-activating the segment. The next write's sync_durable
// cycle re-enters, space is still low, and the seal branch runs AGAIN for
// the same still-active old_log_id. A plain `insert` at the second store
// would REPLACE the staged slot — which held every commit folded before the
// first store — with the now-empty accumulator whose `complete` is TRUE
// (the first store reset segment_summary_incomplete). Late deferred commits
// would then fold into the empty slot, the sweep would write a complete=true
// SUBSET sidecar, and every consult it feeds (summary_hint Skip, the
// seal-sized aggregate/client blooms, the schema bloom) could answer a false
// "definitely absent" for durable, ACKed data — with ALL blooms derived from
// the slot, the false absence reaches every consult, not just the entry map.
// `store_sealed_segment_summary` therefore MERGES colliding stores
// (SealedSegmentSummary::merge, era-aware); this test drives the exact trace
// and asserts nothing previously folded is lost.
#[test]
fn adversarial_seal_retry_overwrites_staged_slot_with_empty_complete() {
    let mut c = cache();
    let k_early = agg(1, 2, 3);
    // A commit folded into the active accumulator before the first seal attempt.
    c.update_segment_summary(&eb_metablock(k_early.clone(), 1, 1000), 4096);

    // First seal attempt stages the slot; rotate_to_next_log then FAILS.
    c.store_sealed_segment_summary(7);

    // Retry cycle: segment 7 is still active, seal branch runs again.
    c.store_sealed_segment_summary(7);

    // A late deferred commit for segment 7 lands in the staged slot.
    let k_late = agg(1, 2, 4);
    c.update_segment_summary_for_log(7, 7, &eb_metablock(k_late.clone(), 1, 2000), 8192);

    let payload = c.take_sealed_segment_summary(7).expect("slot staged");
    assert!(payload.complete, "scaffolding: the subset payload claims completeness");
    let has_early = payload.aggregates.iter().any(|e| e.aggregate_id == 3);
    assert!(
        has_early,
        "false absence: the seal-retry overwrite dropped a folded commit from the staged slot; \
         a complete=true sidecar (and P2's blooms sized from it) will deny aggregate 3 exists in segment 7"
    );
}
