use crate::cache_path::CachePath;
use crate::mem_snapshot_aggregate::{AggregateStatus, MemSnapshotAggregate};
use crate::pending_commit_data::PendingCommitData;
use crate::shard_log_queue_item::ShardLogQueueItem;
use crate::shard_mem_cache::ShardMemCache;
use celeriant_distributed::node_status::NodeStatus;
use celeriant_rotating_log::log_segment_file::log_segment_file_metadata::LogSegmentFileMetadata;
use celeriant_wal::aggregate_client_key::AggregateClientKey;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::{GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES, MINIBATCH_SIZE_BYTES};
use celeriant_wal::metablocks::datablock_inline_data::DatablockInlineData;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_event_batch::{EventTypesKind, MetablockEventBatch};
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_wal::shard_log_header::ShardLogHeader;

// ── Helpers ──

fn cache() -> ShardMemCache {
    cache_with(64 * 1024, 64 * 1024, 64 * 1024, 64 * 1024, 1024 * 1024)
}

fn cache_with(
    recent_write_bytes: u64,
    agg_write_snap_bytes: u64,
    agg_client_snap_bytes: u64,
    list_wal_index_bytes: u64,
    replication_high_water: u64,
) -> ShardMemCache {
    ShardMemCache::new(
        recent_write_bytes,
        agg_write_snap_bytes,
        agg_client_snap_bytes,
        list_wal_index_bytes,
        replication_high_water,
    )
}

fn agg(org: u128, atype: u128, id: u128) -> AggregateKey {
    AggregateKey::new(org, atype, id)
}

fn client_key(aggregate_key: &AggregateKey, client_id: u128) -> AggregateClientKey {
    AggregateClientKey::new(aggregate_key.clone(), client_id)
}

fn test_metablock(aggregate_key: AggregateKey, event_batch_index: u64, max_event_index: u64, client_id: u128, wal_index: u64) -> Metablock {
    Metablock {
        wal_index,
        server_timestamp: 1000,
        lease_index: 1,
        node_id: 1,
        uncompressed_size: 128,
        compressed_size: 64,
        datablock_version: 1,
        datablock_compression_type: 0,
        previous_tip_hash: GENESIS_HASH,
        datablock_position: 0,
        wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
            aggregate_key,
            event_batch_index,
            min_event_batch_index: 1,
            min_client_event_index: 1,
            max_client_event_index: max_event_index,
            min_event_timestamp: 100,
            max_event_timestamp: 200,
            min_event_index: 1,
            max_event_index,
            client_id,
            user_id: None,
            event_types_data: EventTypesKind::Direct([1, 0, 0, 0]),
        }),
        datablock: DatablockStorageKind::Inline(DatablockInlineData {
            minibatch: [0u8; MINIBATCH_SIZE_BYTES],
        }),
    }
}

fn test_queue_item(aggregate_key: AggregateKey, event_batch_index: u64, max_event_index: u64, client_id: u128) -> ShardLogQueueItem {
    let metablock = test_metablock(aggregate_key, event_batch_index, max_event_index, client_id, 0);
    ShardLogQueueItem::new(None, None, metablock)
}

fn test_event_batch(aggregate_key: AggregateKey, event_batch_index: u64, max_event_index: u64) -> MetablockEventBatch {
    MetablockEventBatch {
        aggregate_key,
        event_batch_index,
        min_event_batch_index: 1,
        min_client_event_index: 1,
        max_client_event_index: max_event_index,
        min_event_timestamp: 100,
        max_event_timestamp: 200,
        min_event_index: 1,
        max_event_index,
        client_id: 1,
        user_id: None,
        event_types_data: EventTypesKind::Direct([1, 0, 0, 0]),
    }
}

/// Add a write to the queue and return the queue item's event indexes for assertions
fn queue_write(cache: &mut ShardMemCache, key: &AggregateKey, event_index: u64, event_batch_index: u64, client_id: u128, client_event_index: u64) {
    let item = test_queue_item(key.clone(), event_batch_index, event_index, client_id);
    cache.add_to_pending_append_queue(key, event_index, event_batch_index, 1, client_id, client_event_index, item);
}

fn test_pending_commit_data() -> PendingCommitData {
    let header = ShardLogHeader {
        metablocks_position: HEADER_BLOCK_SIZE_BYTES as u64,
        datablocks_position: 4 * 1024 * 1024 - HEADER_BLOCK_SIZE_BYTES as u64,
        wal_index: 0,
        aggregate_bloom: vec![0u64; 4],
        tip_hash: GENESIS_HASH,
    };
    PendingCommitData {
        log_metadata: LogSegmentFileMetadata::new(1, 4 * 1024 * 1024, None, &header, true),
        pending_queue: vec![],
    }
}

/// Take a sync snapshot and commit it as standalone (both read+write caches updated)
fn sync_and_commit_standalone(cache: &mut ShardMemCache) {
    let snapshot = cache.take_sync_positions_snapshot();
    cache.commit_sync_positions_snapshot(NodeStatus::Standalone, snapshot);
}

/// Take a sync snapshot and commit it as leader (only write cache updated)
fn sync_and_commit_leader(cache: &mut ShardMemCache) {
    let snapshot = cache.take_sync_positions_snapshot();
    cache.commit_sync_positions_snapshot(NodeStatus::Leader { lease_index: 1 }, snapshot);
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
fn queue_tracks_event_indexes_with_max_wins() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 3, 1, 100, 1);
    queue_write(&mut c, &k, 7, 2, 100, 2);

    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_index, 7);
    assert_eq!(indexes.event_batch_index, 2);
    assert!(!indexes.pending_delete_or_deleted);
}

#[test]
fn queue_tracks_client_event_indexes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 100, 5);
    queue_write(&mut c, &k, 2, 2, 200, 10);

    assert_eq!(c.get_client_event_index(&k, 100), Some(5));
    assert_eq!(c.get_client_event_index(&k, 200), Some(10));
    assert_eq!(c.get_client_event_index(&k, 999), None);
}

#[test]
fn client_event_index_max_wins_within_same_client() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 1, 1, 100, 3);
    queue_write(&mut c, &k, 2, 2, 100, 7);

    assert_eq!(c.get_client_event_index(&k, 100), Some(7));
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

    let indexes = c.get_write_event_indexes(&k);
    assert!(indexes.pending_delete_or_deleted);
    assert!(!indexes.allow_recreate);
}

#[test]
fn pending_delete_with_recreate_flags() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let item = test_queue_item(k.clone(), 5, 10, 1);

    c.add_pending_delete_to_queue(&k, 10, 5, true, true, item);

    let indexes = c.get_write_event_indexes(&k);
    assert!(indexes.pending_delete_or_deleted);
    assert!(indexes.allow_recreate);
    assert!(indexes.allow_index_continuation);
}

#[test]
fn pending_trim_updates_min_event_batch_index() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // First add a write so aggregate exists in queue
    queue_write(&mut c, &k, 5, 5, 1, 1);

    let trim_item = test_queue_item(k.clone(), 0, 0, 0);
    c.add_pending_trim_to_queue(&k, 3, trim_item);

    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.min_event_batch_index, 3);
}

#[test]
fn trim_only_increases_min_event_batch_index() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    queue_write(&mut c, &k, 5, 5, 1, 1);

    let item1 = test_queue_item(k.clone(), 0, 0, 0);
    c.add_pending_trim_to_queue(&k, 5, item1);

    // Lower trim should not decrease
    let item2 = test_queue_item(k.clone(), 0, 0, 0);
    c.add_pending_trim_to_queue(&k, 3, item2);

    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.min_event_batch_index, 5);
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
    c.add_pending_trim_to_queue(&k, 2, trim_item);
    sync_and_commit_standalone(&mut c);

    // log_id and metablock_absolute_pos must NOT have been overwritten to 0
    for path in [CachePath::Write, CachePath::Read] {
        let pos = c.get_aggregate_last_metablock_pos(&k, path);
        assert_eq!(pos.log_id, 5, "trim must not corrupt log_id on {:?}", path);
        assert_eq!(pos.metablock_absolute_pos, 2048, "trim must not corrupt metablock_absolute_pos on {:?}", path);
    }
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

    // Queue position should be cleaned up (event_batch_index matches)
    let indexes = c.get_write_event_indexes(&k);
    // Should fall through to snapshot cache, not queue
    assert_eq!(indexes.event_batch_index, 1);
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
    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_batch_index, 2);
    assert_eq!(indexes.event_index, 10);
}

#[test]
fn commit_updates_client_event_indexes() {
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
fn commit_read_position_snapshot_updates_read_cache() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let event_batch = test_event_batch(k.clone(), 3, 10);

    c.commit_read_position_snapshot(&event_batch, 1, 512);

    let (loaded, status) = c.aggregate_load_status(&k, CachePath::Read);
    assert!(loaded);
    assert_eq!(status, AggregateStatus::Found);

    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Read);
    assert_eq!(pos.event_batch_index, 3);
    assert_eq!(pos.event_index, 10);
    assert_eq!(pos.log_id, 1);
    assert_eq!(pos.metablock_absolute_pos, 512);
}

#[test]
fn commit_read_position_snapshot_advances_existing_entry() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // First commit
    let batch1 = test_event_batch(k.clone(), 1, 5);
    c.commit_read_position_snapshot(&batch1, 1, 100);

    // Second commit with higher indexes on a new log segment
    let batch2 = test_event_batch(k.clone(), 3, 15);
    c.commit_read_position_snapshot(&batch2, 2, 200);

    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Read);
    assert_eq!(pos.event_batch_index, 3);
    assert_eq!(pos.event_index, 15);
    assert_eq!(pos.log_id, 2);
    assert_eq!(pos.metablock_absolute_pos, 200);
}

#[test]
fn commit_read_position_does_not_regress_indexes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let batch_high = test_event_batch(k.clone(), 10, 50);
    c.commit_read_position_snapshot(&batch_high, 1, 100);

    // Lower indexes should not overwrite, but position still advances
    let batch_low = test_event_batch(k.clone(), 2, 5);
    c.commit_read_position_snapshot(&batch_low, 2, 50);

    let pos = c.get_aggregate_last_metablock_pos(&k, CachePath::Read);
    assert_eq!(pos.event_batch_index, 10);
    assert_eq!(pos.event_index, 50);
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

    let indexes = c.get_write_event_indexes(&k);
    assert!(indexes.pending_delete_or_deleted);
    assert!(indexes.allow_recreate);
    assert!(indexes.allow_index_continuation);
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

    // Client index should also be cached on Write path
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
    let mut c = cache_with(0, 112, 0, 0, 1024);
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
    assert_eq!(pos.event_index, 1);
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
fn get_client_event_index_sentinel_zero_returns_none() {
    let mut c = cache();
    let k = agg(1, 1, 1);
    let ck = client_key(&k, 100);

    c.put_aggregate_client_into_cache(ck, 0, false);

    assert_eq!(c.get_client_event_index(&k, 100), None);
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

#[test]
fn get_write_event_indexes_queue_over_snapshot() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    // Snapshot says batch=3, event=10
    let snap = MemSnapshotAggregate::found(1, 0, 10, 3, 1);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Write);

    // Queue says batch=5, event=20
    queue_write(&mut c, &k, 20, 5, 1, 1);

    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_batch_index, 5);
    assert_eq!(indexes.event_index, 20);
}

#[test]
fn get_write_event_indexes_falls_through_to_snapshot() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    let snap = MemSnapshotAggregate::found(1, 0, 10, 3, 1);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Write);

    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_batch_index, 3);
    assert_eq!(indexes.event_index, 10);
}

#[test]
fn get_write_event_indexes_returns_zeroes_for_unknown() {
    let mut c = cache();
    let k = agg(99, 99, 99);

    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_batch_index, 0);
    assert_eq!(indexes.event_index, 0);
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
    assert_eq!(writes[0].0, 1); // batch_index
}

#[test]
fn cache_recent_write_respects_wal_index_visibility() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for i in 1..=3 {
        let mb = test_metablock(k.clone(), i, i * 5, 100, i * 10);
        c.cache_recent_write(k.clone(), i, mb, None, 64);
    }

    // Only wal_index <= 20 visible
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
    let mut c = cache_with(0, 64 * 1024, 64 * 1024, 64 * 1024, 1024 * 1024);
    let k = agg(1, 1, 1);
    let mb = test_metablock(k.clone(), 1, 5, 100, 1);

    c.cache_recent_write(k.clone(), 1, mb, None, 64);

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    assert_eq!(writes.len(), 0);
}

#[test]
fn cache_recent_write_evicts_oldest_under_pressure() {
    // Budget for only ~2 entries
    let mut c = cache_with(128, 64 * 1024, 64 * 1024, 64 * 1024, 1024 * 1024);
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
fn update_min_event_batch_index_read_path_evicts_trimmed_writes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for i in 1..=5 {
        let mb = test_metablock(k.clone(), i, i, 1, i);
        c.cache_recent_write(k.clone(), i, mb, None, 64);
    }

    // Need aggregate in read snapshot first
    let snap = MemSnapshotAggregate::found(1, 0, 5, 5, 1);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Read);

    c.update_aggregate_min_event_batch_index(&k, 3, CachePath::Read);

    let writes: Vec<_> = c.get_cached_writes_from(&k, 1, u64::MAX).collect();
    // Batches 1 and 2 should be evicted (< 3)
    assert!(writes.iter().all(|(idx, _)| *idx >= 3));
}

#[test]
fn update_min_event_batch_index_write_path_does_not_evict_recent_writes() {
    let mut c = cache();
    let k = agg(1, 1, 1);

    for i in 1..=5 {
        let mb = test_metablock(k.clone(), i, i, 1, i);
        c.cache_recent_write(k.clone(), i, mb, None, 64);
    }

    let snap = MemSnapshotAggregate::found(1, 0, 5, 5, 1);
    c.put_aggregate_into_cache(k.clone(), snap, 1, 1, false, CachePath::Write);

    c.update_aggregate_min_event_batch_index(&k, 3, CachePath::Write);

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
        assert_eq!(pos.event_batch_index, 0);
        assert_eq!(pos.event_index, 0);
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
    assert_eq!(pos.event_batch_index, 8);
    assert_eq!(pos.event_index, 20);
    assert_eq!(pos.min_event_batch_index, 2);
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
    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_batch_index, 0);
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
fn replication_high_water_mark() {
    let mut c = cache_with(64 * 1024, 64 * 1024, 64 * 1024, 64 * 1024, 1); // 1 byte high water

    assert!(!c.is_replication_queue_pressured());

    let exceeded = c.push_pending_replication(test_pending_commit_data());

    assert!(exceeded);
    assert!(c.is_replication_queue_pressured());
}

// ── WAL Index Position Cache ──

#[test]
fn wal_index_position_cache_basic() {
    let mut c = cache();

    c.cache_wal_index_position(100, 1, 512);
    c.cache_wal_index_position(200, 1, 1024);

    let pos = c.get_wal_index_position(100);
    assert!(pos.is_some());
    let pos = pos.unwrap();
    assert_eq!(pos.log_id, 1);
    assert_eq!(pos.metablock_absolute_pos, 512);

    assert!(c.get_wal_index_position(150).is_none());
}

#[test]
fn find_nearest_wal_index_position() {
    let mut c = cache();

    for i in [100, 200, 300, 500] {
        c.cache_wal_index_position(i, 1, i * 10);
    }

    // Exact match
    let (idx, pos) = c.find_nearest_wal_index_position(200).unwrap();
    assert_eq!(idx, 200);
    assert_eq!(pos.metablock_absolute_pos, 2000);

    // Between entries, picks lower
    let (idx, _) = c.find_nearest_wal_index_position(250).unwrap();
    assert_eq!(idx, 200);

    // Past all entries
    let (idx, _) = c.find_nearest_wal_index_position(999).unwrap();
    assert_eq!(idx, 500);

    // Before all entries
    assert!(c.find_nearest_wal_index_position(50).is_none());
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
    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_batch_index, 3);
    assert_eq!(indexes.event_index, 15);

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
        let indexes = c.get_write_event_indexes(k);
        assert_eq!(indexes.event_batch_index, (i + 1) as u64);
        assert_eq!(indexes.event_index, (i + 1) as u64 * 10);
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

    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_batch_index, 2);
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
    let idx1 = c.get_write_event_indexes(&k1);
    assert_eq!(idx1.event_batch_index, 2);

    // k2 should be cleaned up from queue (no concurrent write)
    let idx2 = c.get_write_event_indexes(&k2);
    assert_eq!(idx2.event_batch_index, 1);
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
    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_batch_index, 5);
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
        assert_eq!(c.get_client_event_index(&k, client_id), Some(client_id as u64));
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

    let indexes = c.get_write_event_indexes(&k);
    assert_eq!(indexes.event_batch_index, 100);
    assert_eq!(indexes.event_index, 300);
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

    // Populate wal_index_positions
    c.cache_wal_index_position(100, 1, 512);

    // Populate aggregate_client_snapshots
    let ck = client_key(&k2, 200);
    c.put_aggregate_client_into_cache(ck.clone(), 42, false);

    // Verify things are populated
    let (loaded, _) = c.aggregate_load_status(&k1, CachePath::Read);
    assert!(loaded);
    let (loaded, _) = c.aggregate_load_status(&k2, CachePath::Write);
    assert!(loaded);
    let writes: Vec<_> = c.get_cached_writes_from(&k1, 1, u64::MAX).collect();
    assert_eq!(writes.len(), 1);
    assert!(c.get_wal_index_position(100).is_some());
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
    assert!(c.get_wal_index_position(100).is_none(), "wal_index_positions should be empty");
    let (loaded, _) = c.aggregate_client_load_status(&k2, &ck);
    assert!(!loaded, "aggregate_client_snapshots should be empty");
}
