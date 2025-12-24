#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, time::Duration};

    use crate::{
        queue_aggregate_positions::QueueAggregatePositions,
        internal_shard_config::InternalShardConfig,
        recent_write::RecentWrite,
        shard_log_queue_item::ShardLogQueueItem,
        shard_mem_cache::{ShardMemCache},
        sync_positions_snapshot::SyncPositionsSnapshot,
    };
    use celeriant_wal::{
        aggregate_key::AggregateKey,
        constants::FIXED_BLOCK_SIZE_BYTES,
        datablocks::{
            datablock::Datablock,
            datablock_aggregate_event::DatablockAggregateEvent,
            datablock_aggregate_event_batch::DatablockAggregateEventBatch,
            datablock_kind::DatablockKind,
        },
        metablocks::{
            datablock_storage_kind::DatablockStorageKind,
            metablock::Metablock,
            metablock_event_batch::{EventTypesKind, MetablockEventBatch},
            metablock_kind::MetablockKind,
        },
    };

    // =============================================================================
    // Test Helpers
    // =============================================================================

    fn test_config() -> InternalShardConfig {
        InternalShardConfig {
            node_id: 1,
            max_open_files: 100,
            shard_log_preallocate_bytes: 1024 * 1024 * 1024, // 1GB
            fsync_delay: Duration::from_millis(5),
            recent_write_cache_bytes: 10000,
            non_durable_writes: false,
            shard_dir: PathBuf::from("/tmp/test_shard"),
            max_response_size: 10 * 1024 * 1024,
            aggregate_snapshots_cache_bytes: 100000 * 112,
            aggregate_client_snapshots_cache_bytes: 100000 * 128,
            read_max_chunk_size: 64 * 1024, // 64KB
        }
    }

    fn test_config_no_cache() -> InternalShardConfig {
        InternalShardConfig {
            recent_write_cache_bytes: 0,
            ..test_config()
        }
    }

    fn test_config_small_cache(cache_bytes: u64) -> InternalShardConfig {
        InternalShardConfig {
            recent_write_cache_bytes: cache_bytes,
            ..test_config()
        }
    }

    fn new_cache() -> ShardMemCache {
        let file_len = 1024 * 1024 * 1024; // 1GB
        let metablocks_position = FIXED_BLOCK_SIZE_BYTES as u64;
        let datablocks_position = file_len - FIXED_BLOCK_SIZE_BYTES as u64;
        ShardMemCache::new(
            file_len,
            metablocks_position,
            datablocks_position,
            0,
            test_config(),
            0,
        )
    }

    fn new_cache_with_config(config: InternalShardConfig) -> ShardMemCache {
        let file_len = 1024 * 1024 * 1024;
        let metablocks_position = FIXED_BLOCK_SIZE_BYTES as u64;
        let datablocks_position = file_len - FIXED_BLOCK_SIZE_BYTES as u64;
        ShardMemCache::new(file_len, metablocks_position, datablocks_position, 0, config, 0)
    }

    fn make_aggregate_key(org: u128, agg_type: u128, agg_id: u128) -> AggregateKey {
        AggregateKey::new(org, agg_type, agg_id)
    }

    fn make_queue_item(datablock_size: Option<usize>) -> ShardLogQueueItem {
        let datablock_bytes = datablock_size.map(|size| vec![0u8; size]);
        let datablock = datablock_size.map(|_| Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 1,
                events: vec![],
            }),
        });

        ShardLogQueueItem {
            datablock_bytes,
            datablock,
            metablock: make_metablock(1, make_aggregate_key(1, 1, 1)),
        }
    }

    fn make_metablock(wal_index: u64, aggregate_key: AggregateKey) -> Metablock {
        Metablock {
            wal_index,
            server_timestamp: 1000,
            lease_index: 1,
            node_id: 1,
            datablock: DatablockStorageKind::None,
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key,
                event_types_data: EventTypesKind::Direct([0, 0, 0, 0]),
                event_batch_index: 1,
                client_id: 1,
                user_id: None,
                min_client_event_index: 0,
                max_client_event_index: 0,
                min_event_timestamp: 0,
                max_event_timestamp: 0,
                min_event_index: 0,
                max_event_index: 0,
            }),
        }
    }

    fn make_datablock() -> Datablock {
        Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 1,
                events: vec![DatablockAggregateEvent::default()],
            }),
        }
    }

    // =============================================================================
    // Basic Initialization Tests
    // =============================================================================

    #[test]
    fn new_cache_initializes_with_correct_positions() {
        let file_len = 1024 * 1024 * 1024;
        let metablocks_position = 512;
        let datablocks_position = file_len - 512;

        let cache = ShardMemCache::new(
            file_len,
            metablocks_position,
            datablocks_position,
            0,
            test_config(),
            0,
        );

        assert_eq!(cache.current_log_id(), 0);
        assert!(!cache.requires_write());
        assert!(!cache.force_durable_on_next_write());
    }

    #[test]
    fn shard_dir_returns_configured_path() {
        let cache = new_cache();
        assert_eq!(cache.shard_dir(), PathBuf::from("/tmp/test_shard"));
    }

    #[test]
    fn shard_log_preallocate_bytes_returns_configured_value() {
        let cache = new_cache();
        assert_eq!(cache.shard_log_preallocate_bytes(), 1024 * 1024 * 1024);
    }

    // =============================================================================
    // Pending Append Queue Tests
    // =============================================================================

    #[test]
    fn add_to_pending_queue_sets_requires_write() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        assert!(!cache.requires_write());

        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(Some(100)));

        assert!(cache.requires_write());
    }

    #[test]
    fn add_to_pending_queue_updates_queue_positions() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));

        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 5);
        assert_eq!(indexes.event_batch_index, 3);

        let client_index = cache.get_client_event_index(&key, 100);
        assert_eq!(client_index, Some(10));
    }

    #[test]
    fn add_multiple_writes_same_aggregate_tracks_max_indexes() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // First write
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));

        // Second write with higher indexes
        cache.add_to_pending_append_queue(&key, 10, 7, 100, 15, make_queue_item(None));

        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 10);
        assert_eq!(indexes.event_batch_index, 7);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(15));

        // Third write with lower indexes (should not update)
        cache.add_to_pending_append_queue(&key, 8, 5, 100, 12, make_queue_item(None));

        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 10);
        assert_eq!(indexes.event_batch_index, 7);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(15));
    }

    #[test]
    fn add_writes_multiple_clients_same_aggregate() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));
        cache.add_to_pending_append_queue(&key, 6, 4, 200, 20, make_queue_item(None));
        cache.add_to_pending_append_queue(&key, 7, 5, 300, 30, make_queue_item(None));

        assert_eq!(cache.get_client_event_index(&key, 100), Some(10));
        assert_eq!(cache.get_client_event_index(&key, 200), Some(20));
        assert_eq!(cache.get_client_event_index(&key, 300), Some(30));
        assert_eq!(cache.get_client_event_index(&key, 400), None);
    }

    #[test]
    fn add_writes_multiple_aggregates() {
        let mut cache = new_cache();
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(1, 1, 2);
        let key3 = make_aggregate_key(2, 1, 1);

        cache.add_to_pending_append_queue(&key1, 5, 3, 100, 10, make_queue_item(None));
        cache.add_to_pending_append_queue(&key2, 15, 13, 200, 20, make_queue_item(None));
        cache.add_to_pending_append_queue(&key3, 25, 23, 300, 30, make_queue_item(None));

        assert_eq!(cache.get_event_indexes(&key1).event_index, 5);
        assert_eq!(cache.get_event_indexes(&key2).event_index, 15);
        assert_eq!(cache.get_event_indexes(&key3).event_index, 25);
    }

    #[test]
    fn buffer_size_datablocks_sums_all_pending_datablock_bytes() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(Some(100)));
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(Some(200)));
        cache.add_to_pending_append_queue(&key, 3, 3, 100, 3, make_queue_item(None)); // No datablock

        assert_eq!(cache.buffer_size_datablocks(), 300);
    }

    #[test]
    fn buffer_size_metablocks_counts_pending_items() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        cache.add_to_pending_append_queue(&key, 3, 3, 100, 3, make_queue_item(None));

        assert_eq!(
            cache.buffer_size_metablocks(),
            3 * FIXED_BLOCK_SIZE_BYTES as u64
        );
    }

    // =============================================================================
    // Snapshot Tests
    // =============================================================================

    #[test]
    fn take_snapshot_clears_pending_queue() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(Some(100)));
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(Some(200)));

        assert!(cache.requires_write());

        let snapshot = cache.take_sync_positions_snapshot();

        assert!(!cache.requires_write());
        assert_eq!(snapshot.pending_append_queue.len(), 2);
    }

    #[test]
    fn take_snapshot_clears_queue_positions() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));

        let snapshot = cache.take_sync_positions_snapshot();

        // Queue positions should be cleared
        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 0);
        assert_eq!(indexes.event_batch_index, 0);
        assert_eq!(cache.get_client_event_index(&key, 100), None);

        // But snapshot should have them
        let snapshot_positions = snapshot.aggregate_queue_positions.get(&key).unwrap();
        assert_eq!(snapshot_positions.event_index, 5);
        assert_eq!(snapshot_positions.event_batch_index, 3);
        assert_eq!(snapshot_positions.client_event_indexes.get(&100), Some(&10));
    }

    #[test]
    fn take_snapshot_captures_current_positions() {
        let file_len = 1024 * 1024 * 1024;
        let metablocks_position = 1024;
        let datablocks_position = file_len - 2048;

        let mut cache = ShardMemCache::new(
            file_len,
            metablocks_position,
            datablocks_position,
            0,
            test_config(),
            5,
        );

        let key = make_aggregate_key(1, 1, 1);
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));

        let snapshot = cache.take_sync_positions_snapshot();

        assert_eq!(snapshot.metablocks_position, 1024);
        assert_eq!(snapshot.datablocks_position, file_len - 2048);
        assert_eq!(snapshot.file_len, file_len);
    }

    #[test]
    fn take_snapshot_clones_datablocks_carry_over() {
        let file_len = 1024 * 1024 * 1024;
        let mut cache = ShardMemCache::new(
            file_len,
            FIXED_BLOCK_SIZE_BYTES as u64,
            file_len - FIXED_BLOCK_SIZE_BYTES as u64,
            0,
            test_config(),
            0,
        );

        // Simulate having carry over bytes from a previous write
        let key = make_aggregate_key(1, 1, 1);
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));

        // First snapshot won't have carry over
        let snapshot1 = cache.take_sync_positions_snapshot();
        assert!(snapshot1.datablocks_carry_over.is_none());

        // After commit with carry over, next snapshot should have it
        let modified_snapshot = SyncPositionsSnapshot {
            pending_append_queue: vec![],
            aggregate_queue_positions: HashMap::new(),
            metablocks_position: snapshot1.metablocks_position + 512,
            datablocks_position: snapshot1.datablocks_position - 1000,
            file_len: snapshot1.file_len,
            datablocks_carry_over: Some(vec![1, 2, 3, 4]),
            wal_index: snapshot1.wal_index + 1,
        };
        cache.commit_sync_positions_snapshot(modified_snapshot);

        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        let snapshot2 = cache.take_sync_positions_snapshot();
        assert_eq!(snapshot2.datablocks_carry_over, Some(vec![1, 2, 3, 4]));
        assert_eq!(snapshot2.wal_index, 1);
    }

    #[test]
    fn commit_snapshot_updates_internal_positions() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(Some(500)));

        let mut snapshot = cache.take_sync_positions_snapshot();

        // Simulate disk write updating positions
        let new_meta_pos = snapshot.metablocks_position + FIXED_BLOCK_SIZE_BYTES as u64;
        let new_data_pos = snapshot.datablocks_position - 500;
        let new_file_len = snapshot.file_len + 1000;

        snapshot.metablocks_position = new_meta_pos;
        snapshot.datablocks_position = new_data_pos;
        snapshot.file_len = new_file_len;

        cache.commit_sync_positions_snapshot(snapshot);

        // Take another snapshot to verify positions were actually committed
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        let verification_snapshot = cache.take_sync_positions_snapshot();

        assert_eq!(verification_snapshot.metablocks_position, new_meta_pos);
        assert_eq!(verification_snapshot.datablocks_position, new_data_pos);
        assert_eq!(verification_snapshot.file_len, new_file_len);
    }

    #[test]
    fn queuing_continues_after_snapshot_before_commit() {
        let mut cache = new_cache();
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(1, 1, 2);

        // First batch of writes
        cache.add_to_pending_append_queue(&key1, 1, 1, 100, 1, make_queue_item(None));
        cache.add_to_pending_append_queue(&key1, 2, 2, 100, 2, make_queue_item(None));

        // Take snapshot (simulates fsync starting)
        let snapshot = cache.take_sync_positions_snapshot();
        assert_eq!(snapshot.pending_append_queue.len(), 2);

        // More writes come in while fsync is in progress
        cache.add_to_pending_append_queue(&key1, 3, 3, 100, 3, make_queue_item(None));
        cache.add_to_pending_append_queue(&key2, 1, 1, 200, 1, make_queue_item(None));

        // Queue should have new writes
        assert!(cache.requires_write());

        // New queue positions should be tracked
        assert_eq!(cache.get_event_indexes(&key1).event_index, 3);
        assert_eq!(cache.get_event_indexes(&key2).event_index, 1);

        // Now commit the first snapshot
        cache.commit_sync_positions_snapshot(snapshot);

        // Pending queue should still have the writes that came after snapshot
        assert!(cache.requires_write());
        let second_snapshot = cache.take_sync_positions_snapshot();
        assert_eq!(second_snapshot.pending_append_queue.len(), 2);
    }

    // =============================================================================
    // Commit Tests
    // =============================================================================

    #[test]
    fn commit_snapshot_updates_file_positions() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(Some(500)));

        let mut snapshot = cache.take_sync_positions_snapshot();

        // Simulate disk write updating positions
        snapshot.metablocks_position += FIXED_BLOCK_SIZE_BYTES as u64;
        snapshot.datablocks_position -= 500;

        cache.commit_sync_positions_snapshot(snapshot);

        // File positions should be readable now
        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 5);
        assert_eq!(indexes.event_batch_index, 3);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(10));
    }

    #[test]
    fn commit_snapshot_merges_with_existing_file_positions() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // First write cycle
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));
        let snapshot1 = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot1);

        // Second write cycle with same aggregate, different client
        cache.add_to_pending_append_queue(&key, 10, 7, 200, 20, make_queue_item(None));
        let snapshot2 = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot2);

        // Both clients should be tracked
        assert_eq!(cache.get_client_event_index(&key, 100), Some(10));
        assert_eq!(cache.get_client_event_index(&key, 200), Some(20));

        // Max indexes should be updated
        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 10);
        assert_eq!(indexes.event_batch_index, 7);
    }

    #[test]
    fn commit_snapshot_updates_datablocks_carry_over() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let mut snapshot = cache.take_sync_positions_snapshot();

        snapshot.datablocks_carry_over = Some(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        cache.commit_sync_positions_snapshot(snapshot);

        // Next snapshot should include the carry over
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        let next_snapshot = cache.take_sync_positions_snapshot();
        assert_eq!(
            next_snapshot.datablocks_carry_over,
            Some(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );
    }

    #[test]
    fn commit_clears_fsync_failure_flag() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Simulate a previous fsync failure
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let _ = cache.take_sync_positions_snapshot();
        cache.rollback_queue_positions();

        assert!(cache.force_durable_on_next_write());

        // Now do a successful commit
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        assert!(!cache.force_durable_on_next_write());
    }

    // =============================================================================
    // Rollback Tests
    // =============================================================================

    #[test]
    fn rollback_sets_fsync_failure_flag() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let _ = cache.take_sync_positions_snapshot();

        assert!(!cache.force_durable_on_next_write());

        cache.rollback_queue_positions();

        assert!(cache.force_durable_on_next_write());
    }

    #[test]
    fn rollback_clears_queue_positions() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));

        // Don't take snapshot, just rollback
        cache.rollback_queue_positions();

        // Queue positions should be cleared
        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 0);
        assert_eq!(indexes.event_batch_index, 0);
        assert_eq!(cache.get_client_event_index(&key, 100), None);
    }

    #[test]
    fn rollback_preserves_committed_file_positions() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // First successful write cycle
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));
        let snapshot1 = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot1);

        // Second write cycle that fails
        cache.add_to_pending_append_queue(&key, 10, 7, 100, 15, make_queue_item(None));
        let _ = cache.take_sync_positions_snapshot();
        cache.rollback_queue_positions();

        // File positions from first commit should still be there
        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 5);
        assert_eq!(indexes.event_batch_index, 3);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(10));
    }

    #[test]
    fn get_indexes_falls_back_to_file_after_rollback() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Commit initial state
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Add more to queue
        cache.add_to_pending_append_queue(&key, 10, 7, 100, 15, make_queue_item(None));

        // Queue should show higher values
        assert_eq!(cache.get_event_indexes(&key).event_index, 10);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(15));

        // Rollback
        cache.rollback_queue_positions();

        // Should fall back to file positions
        assert_eq!(cache.get_event_indexes(&key).event_index, 5);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(10));
    }

    // =============================================================================
    // Fsync Failure Flow Tests (Non-Durable Writes)
    // =============================================================================

    #[test]
    fn fsync_failure_flag_persists_until_successful_commit() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // First failed write
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let _ = cache.take_sync_positions_snapshot();
        cache.rollback_queue_positions();

        assert!(cache.force_durable_on_next_write());

        // Second failed write - flag should remain
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        let _ = cache.take_sync_positions_snapshot();
        cache.rollback_queue_positions();

        assert!(cache.force_durable_on_next_write());

        // Successful write - flag should clear
        cache.add_to_pending_append_queue(&key, 3, 3, 100, 3, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        assert!(!cache.force_durable_on_next_write());
    }

    #[test]
    fn multiple_aggregates_rollback_scenario() {
        let mut cache = new_cache();
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(1, 1, 2);

        // Commit some data for key1
        cache.add_to_pending_append_queue(&key1, 5, 3, 100, 10, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Add pending writes to both
        cache.add_to_pending_append_queue(&key1, 10, 7, 100, 15, make_queue_item(None));
        cache.add_to_pending_append_queue(&key2, 20, 17, 200, 25, make_queue_item(None));

        // Rollback
        cache.rollback_queue_positions();

        // key1 should have committed data, key2 should have nothing
        assert_eq!(cache.get_event_indexes(&key1).event_index, 5);
        assert_eq!(cache.get_event_indexes(&key2).event_index, 0);
        assert_eq!(cache.get_client_event_index(&key1, 100), Some(10));
        assert_eq!(cache.get_client_event_index(&key2, 200), None);
    }

    // =============================================================================
    // Recent Write Cache Tests
    // =============================================================================

    #[test]
    fn cache_recent_write_stores_entry() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);
        let metablock = make_metablock(1, key.clone());
        let datablock = make_datablock();

        cache.cache_recent_write(key.clone(), 1, metablock, Some(datablock), 100);

        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, 1);
        assert_eq!(writes[0].1.size_bytes, 100);
    }

    #[test]
    fn cache_recent_write_disabled_when_max_bytes_zero() {
        let mut cache = new_cache_with_config(test_config_no_cache());
        let key = make_aggregate_key(1, 1, 1);
        let metablock = make_metablock(1, key.clone());

        cache.cache_recent_write(key.clone(), 1, metablock, None, 100);

        assert!(cache.get_cached_writes_from(&key, 0).is_none());
    }

    #[test]
    fn cache_recent_write_multiple_batches_same_aggregate() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        for batch_index in 1..=5 {
            let metablock = make_metablock(batch_index, key.clone());
            cache.cache_recent_write(key.clone(), batch_index, metablock, None, 100);
        }

        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert_eq!(writes.len(), 5);

        // Verify ordering
        for (i, (batch_idx, _)) in writes.iter().enumerate() {
            assert_eq!(*batch_idx, (i + 1) as u64);
        }
    }

    #[test]
    fn cache_recent_write_multiple_aggregates() {
        let mut cache = new_cache();
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(1, 1, 2);

        cache.cache_recent_write(key1.clone(), 1, make_metablock(1, key1.clone()), None, 100);
        cache.cache_recent_write(key1.clone(), 2, make_metablock(2, key1.clone()), None, 100);
        cache.cache_recent_write(key2.clone(), 1, make_metablock(3, key2.clone()), None, 100);

        let writes1: Vec<_> = cache.get_cached_writes_from(&key1, 0).unwrap().collect();
        let writes2: Vec<_> = cache.get_cached_writes_from(&key2, 0).unwrap().collect();

        assert_eq!(writes1.len(), 2);
        assert_eq!(writes2.len(), 1);
    }

    #[test]
    fn get_cached_writes_from_filters_by_batch_index() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        for batch_index in 1..=10 {
            let metablock = make_metablock(batch_index, key.clone());
            cache.cache_recent_write(key.clone(), batch_index, metablock, None, 50);
        }

        // Get from batch 5 onwards
        let writes: Vec<_> = cache.get_cached_writes_from(&key, 5).unwrap().collect();
        assert_eq!(writes.len(), 6); // batches 5, 6, 7, 8, 9, 10

        assert_eq!(writes[0].0, 5);
        assert_eq!(writes[5].0, 10);
    }

    #[test]
    fn get_cached_writes_from_nonexistent_aggregate_returns_none() {
        let cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        assert!(cache.get_cached_writes_from(&key, 0).is_none());
    }

    #[test]
    fn get_cached_writes_from_beyond_max_batch_returns_empty_iterator() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.cache_recent_write(key.clone(), 1, make_metablock(1, key.clone()), None, 100);
        cache.cache_recent_write(key.clone(), 2, make_metablock(2, key.clone()), None, 100);

        let writes: Vec<_> = cache.get_cached_writes_from(&key, 100).unwrap().collect();
        assert_eq!(writes.len(), 0);
    }

    // =============================================================================
    // Recent Write Cache - Size-Based Eviction Tests
    // =============================================================================

    #[test]
    fn cache_evicts_oldest_when_exceeding_max_bytes() {
        // Small cache of 500 bytes
        let mut cache = new_cache_with_config(test_config_small_cache(500));
        let key = make_aggregate_key(1, 1, 1);

        // Add entries that will exceed the cache size
        // Each entry is 150 bytes, so after 3 entries (450 bytes), 4th will trigger eviction
        for batch_index in 1..=4 {
            let metablock = make_metablock(batch_index, key.clone());
            cache.cache_recent_write(key.clone(), batch_index, metablock, None, 150);
        }

        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();

        // First entry should have been evicted (oldest)
        assert!(!writes.iter().any(|(idx, _)| *idx == 1));
        // Entries 2, 3, 4 should remain
        assert_eq!(writes.len(), 3);
        assert_eq!(writes[0].0, 2);
    }

    #[test]
    fn cache_evicts_multiple_entries_if_needed() {
        // Very small cache of 200 bytes
        let mut cache = new_cache_with_config(test_config_small_cache(200));
        let key = make_aggregate_key(1, 1, 1);

        // Add small entries first
        for batch_index in 1..=4 {
            let metablock = make_metablock(batch_index, key.clone());
            cache.cache_recent_write(key.clone(), batch_index, metablock, None, 40);
        }

        // All 4 entries fit (160 bytes)
        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert_eq!(writes.len(), 4);

        // Now add a large entry (150 bytes) - should evict multiple old entries
        let metablock = make_metablock(5, key.clone());
        cache.cache_recent_write(key.clone(), 5, metablock, None, 150);

        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();

        // Should have evicted entries until 150 bytes could fit
        // Total after should be <= 200 bytes
        let total_size: u64 = writes.iter().map(|(_, w)| w.size_bytes).sum();
        assert!(total_size <= 200);

        // Entry 5 should definitely be there
        assert!(writes.iter().any(|(idx, _)| *idx == 5));
    }

    #[test]
    fn cache_eviction_across_multiple_aggregates() {
        let mut cache = new_cache_with_config(test_config_small_cache(300));
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(1, 1, 2);

        // Add entries from different aggregates interleaved
        cache.cache_recent_write(key1.clone(), 1, make_metablock(1, key1.clone()), None, 100);
        cache.cache_recent_write(key2.clone(), 1, make_metablock(2, key2.clone()), None, 100);
        cache.cache_recent_write(key1.clone(), 2, make_metablock(3, key1.clone()), None, 100);

        // All fit (300 bytes exactly)
        assert!(cache.get_cached_writes_from(&key1, 0).is_some());
        assert!(cache.get_cached_writes_from(&key2, 0).is_some());

        // Add one more - should evict key1/batch1 (oldest)
        cache.cache_recent_write(key2.clone(), 2, make_metablock(4, key2.clone()), None, 100);

        let writes1: Vec<_> = cache.get_cached_writes_from(&key1, 0).unwrap().collect();
        let writes2: Vec<_> = cache.get_cached_writes_from(&key2, 0).unwrap().collect();

        // key1 batch 1 should be evicted
        assert!(!writes1.iter().any(|(idx, _)| *idx == 1));
        assert_eq!(writes1.len(), 1); // Only batch 2 remains for key1
        assert_eq!(writes2.len(), 2); // Both batches remain for key2
    }

    #[test]
    fn cache_eviction_removes_aggregate_when_all_batches_evicted() {
        let mut cache = new_cache_with_config(test_config_small_cache(200));
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(1, 1, 2);

        // Add one entry for key1
        cache.cache_recent_write(key1.clone(), 1, make_metablock(1, key1.clone()), None, 100);

        // Add one entry for key2
        cache.cache_recent_write(key2.clone(), 1, make_metablock(2, key2.clone()), None, 100);

        // Both exist
        assert!(cache.get_cached_writes_from(&key1, 0).is_some());
        assert!(cache.get_cached_writes_from(&key2, 0).is_some());

        // Add large entry that requires evicting key1's only entry
        cache.cache_recent_write(key2.clone(), 2, make_metablock(3, key2.clone()), None, 150);

        // key1 should be completely removed
        assert!(cache.get_cached_writes_from(&key1, 0).is_none());
        assert!(cache.get_cached_writes_from(&key2, 0).is_some());
    }

    #[test]
    fn cache_handles_entry_larger_than_max_cache_size() {
        let mut cache = new_cache_with_config(test_config_small_cache(100));
        let key = make_aggregate_key(1, 1, 1);

        // Add small entry first
        cache.cache_recent_write(key.clone(), 1, make_metablock(1, key.clone()), None, 50);
        assert_eq!(cache.get_cached_writes_from(&key, 0).unwrap().count(), 1);

        // Add entry larger than entire cache - will evict everything but still can't fit
        cache.cache_recent_write(key.clone(), 2, make_metablock(2, key.clone()), None, 200);

        // The large entry should still be added (eviction loop breaks when cache is empty)
        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, 2);
    }

    #[test]
    fn cache_stores_datablock_in_recent_write() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);
        let metablock = make_metablock(1, key.clone());
        let datablock = make_datablock();

        cache.cache_recent_write(key.clone(), 1, metablock, Some(datablock), 100);

        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert!(writes[0].1.datablock.is_some());
    }

    #[test]
    fn cache_stores_none_datablock_in_recent_write() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);
        let metablock = make_metablock(1, key.clone());

        cache.cache_recent_write(key.clone(), 1, metablock, None, 100);

        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert!(writes[0].1.datablock.is_none());
    }

    // =============================================================================
    // Log Rotation Tests
    // =============================================================================

    #[test]
    fn rotate_to_next_log_updates_positions() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        let new_log_id = 5;
        let new_meta_pos = 1024;
        let new_data_pos = 1024 * 1024 - 2048;
        let new_file_len = 1024 * 1024;

        cache.rotate_to_next_log(new_log_id, new_meta_pos, new_data_pos, new_file_len);

        assert_eq!(cache.current_log_id(), 5);

        // Verify positions were updated by taking a snapshot
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        
        assert_eq!(snapshot.metablocks_position, new_meta_pos);
        assert_eq!(snapshot.datablocks_position, new_data_pos);
        assert_eq!(snapshot.file_len, new_file_len);
    }

    #[test]
    fn rotate_to_next_log_clears_carry_over() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Add some data and commit with carry over
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let mut snapshot = cache.take_sync_positions_snapshot();
        snapshot.datablocks_carry_over = Some(vec![1, 2, 3]);
        cache.commit_sync_positions_snapshot(snapshot);

        // Verify carry over exists
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        let snapshot_before = cache.take_sync_positions_snapshot();
        assert!(snapshot_before.datablocks_carry_over.is_some());

        // Rotate
        cache.rotate_to_next_log(1, 512, 1024 * 1024 - 512, 1024 * 1024);

        // Carry over should be cleared
        cache.add_to_pending_append_queue(&key, 3, 3, 100, 3, make_queue_item(None));
        let snapshot_after = cache.take_sync_positions_snapshot();
        assert!(snapshot_after.datablocks_carry_over.is_none());
    }

    #[test]
    fn rotate_preserves_file_positions_and_queue() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Commit some data
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Add pending data
        cache.add_to_pending_append_queue(&key, 10, 7, 100, 15, make_queue_item(None));

        // Rotate
        cache.rotate_to_next_log(1, 512, 1024 * 1024 - 512, 1024 * 1024);

        // File positions should still be there
        assert_eq!(cache.get_client_event_index(&key, 100), Some(15));

        // Pending queue should still exist
        assert!(cache.requires_write());
    }

    // =============================================================================
    // Space Calculation Tests
    // =============================================================================

    #[test]
    fn has_enough_free_space_with_empty_queue() {
        let cache = new_cache();
        assert!(cache.has_enough_free_space());
    }

    #[test]
    fn has_enough_free_space_with_small_writes() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Add a few small writes
        for i in 1..=10 {
            cache.add_to_pending_append_queue(&key, i, i, 100, i, make_queue_item(Some(100)));
        }

        // Should still have plenty of space in 1GB file
        assert!(cache.has_enough_free_space());
    }

    #[test]
    fn snapshot_has_enough_free_space() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(Some(1000)));

        let snapshot = cache.take_sync_positions_snapshot();

        assert!(snapshot.has_enough_free_space());
        assert_eq!(snapshot.buffer_size_datablocks(), 1000);
        assert_eq!(
            snapshot.buffer_size_metablocks(),
            FIXED_BLOCK_SIZE_BYTES as u64
        );
    }

    // =============================================================================
    // Queue vs File Position Priority Tests
    // =============================================================================

    #[test]
    fn get_event_indexes_prefers_queue_over_file() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Commit low values
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Add higher values to queue
        cache.add_to_pending_append_queue(&key, 20, 15, 100, 25, make_queue_item(None));

        // Should return queue values
        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 20);
        assert_eq!(indexes.event_batch_index, 15);
    }

    #[test]
    fn get_client_event_index_prefers_queue_over_file() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Commit initial value
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Add higher value to queue
        cache.add_to_pending_append_queue(&key, 10, 7, 100, 20, make_queue_item(None));

        // Should return queue value
        assert_eq!(cache.get_client_event_index(&key, 100), Some(20));
    }

    #[test]
    fn get_event_indexes_returns_defaults_for_unknown_aggregate() {
        let mut cache = new_cache();
        let key = make_aggregate_key(999, 999, 999);

        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 0);
        assert_eq!(indexes.event_batch_index, 0);
    }

    #[test]
    fn get_client_event_index_returns_none_for_unknown_client() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));

        // Different client
        assert_eq!(cache.get_client_event_index(&key, 999), None);
    }

    // =============================================================================
    // Full Write Cycle Integration Tests
    // =============================================================================

    #[test]
    fn full_write_cycle_single_aggregate() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Initial state
        assert!(!cache.requires_write());
        assert_eq!(cache.get_event_indexes(&key).event_index, 0);

        // Add writes
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(Some(100)));
        cache.add_to_pending_append_queue(&key, 2, 1, 100, 2, make_queue_item(Some(150)));

        assert!(cache.requires_write());
        assert_eq!(cache.get_event_indexes(&key).event_index, 2);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(2));

        // Take snapshot
        let mut snapshot = cache.take_sync_positions_snapshot();
        assert!(!cache.requires_write());
        assert_eq!(snapshot.pending_append_queue.len(), 2);

        // Simulate disk write updating positions
        let meta_size = 2 * FIXED_BLOCK_SIZE_BYTES as u64;
        let data_size = 250u64;
        snapshot.metablocks_position += meta_size;
        snapshot.datablocks_position -= data_size;

        // Commit
        cache.commit_sync_positions_snapshot(snapshot);

        // Verify committed state
        assert_eq!(cache.get_event_indexes(&key).event_index, 2);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(2));
        assert!(!cache.force_durable_on_next_write());
    }

    #[test]
    fn full_write_cycle_with_interleaved_writes() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // First batch
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let snapshot1 = cache.take_sync_positions_snapshot();

        // While fsync is happening, more writes come in
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        cache.add_to_pending_append_queue(&key, 3, 3, 100, 3, make_queue_item(None));

        // Commit first snapshot
        cache.commit_sync_positions_snapshot(snapshot1);

        // Second batch should still be pending
        assert!(cache.requires_write());
        let snapshot2 = cache.take_sync_positions_snapshot();
        assert_eq!(snapshot2.pending_append_queue.len(), 2);

        // Commit second batch
        cache.commit_sync_positions_snapshot(snapshot2);

        // Final state
        assert!(!cache.requires_write());
        assert_eq!(cache.get_event_indexes(&key).event_index, 3);
    }

    #[test]
    fn full_write_cycle_with_failure_and_recovery() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Successful first write
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let snapshot1 = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot1);

        // Failed second write
        cache.add_to_pending_append_queue(&key, 5, 5, 100, 5, make_queue_item(None));
        let _snapshot2 = cache.take_sync_positions_snapshot();
        cache.rollback_queue_positions();

        assert!(cache.force_durable_on_next_write());
        assert_eq!(cache.get_event_indexes(&key).event_index, 1); // Rolled back to first commit

        // Successful recovery write
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        let snapshot3 = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot3);

        assert!(!cache.force_durable_on_next_write());
        assert_eq!(cache.get_event_indexes(&key).event_index, 2);
    }

    #[test]
    fn full_write_cycle_with_cache_population() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Add writes
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(Some(100)));

        // Take snapshot and commit
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Now populate cache (simulating after durable write)
        let metablock = make_metablock(1, key.clone());
        let datablock = make_datablock();
        cache.cache_recent_write(key.clone(), 1, metablock, Some(datablock), 100);

        // Verify cache is populated
        let cached: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert_eq!(cached.len(), 1);
        assert!(cached[0].1.datablock.is_some());
    }

    // =============================================================================
    // Edge Case Tests
    // =============================================================================

    #[test]
    fn empty_snapshot_commit_is_valid() {
        let mut cache = new_cache();

        // Take snapshot with nothing pending
        let snapshot = cache.take_sync_positions_snapshot();
        assert!(snapshot.pending_append_queue.is_empty());
        assert!(snapshot.aggregate_queue_positions.is_empty());

        // Commit empty snapshot
        cache.commit_sync_positions_snapshot(snapshot);

        assert!(!cache.force_durable_on_next_write());
    }

    #[test]
    fn multiple_clients_same_aggregate_idempotency() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Multiple clients writing
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 10, make_queue_item(None));
        cache.add_to_pending_append_queue(&key, 2, 2, 200, 20, make_queue_item(None));
        cache.add_to_pending_append_queue(&key, 3, 3, 100, 15, make_queue_item(None)); // Client 100 again

        // Client 100 should show max index
        assert_eq!(cache.get_client_event_index(&key, 100), Some(15));
        assert_eq!(cache.get_client_event_index(&key, 200), Some(20));

        // Commit and verify
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        assert_eq!(cache.get_client_event_index(&key, 100), Some(15));
        assert_eq!(cache.get_client_event_index(&key, 200), Some(20));
    }

    #[test]
    fn concurrent_snapshot_and_queue_isolation() {
        let mut cache = new_cache();
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(1, 1, 2);

        // Add to key1
        cache.add_to_pending_append_queue(&key1, 1, 1, 100, 1, make_queue_item(None));

        // Snapshot captures key1
        let snapshot = cache.take_sync_positions_snapshot();
        assert!(snapshot.aggregate_queue_positions.contains_key(&key1));
        assert!(!snapshot.aggregate_queue_positions.contains_key(&key2));

        // Add to key2 after snapshot
        cache.add_to_pending_append_queue(&key2, 1, 1, 200, 1, make_queue_item(None));

        // Commit snapshot - should only affect key1
        cache.commit_sync_positions_snapshot(snapshot);

        // key1 should be in file positions
        assert_eq!(cache.get_client_event_index(&key1, 100), Some(1));

        // key2 should still be in queue only
        assert_eq!(cache.get_client_event_index(&key2, 200), Some(1));

        // Take another snapshot for key2
        let snapshot2 = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot2);

        // Now both should be in file positions
        assert_eq!(cache.get_client_event_index(&key1, 100), Some(1));
        assert_eq!(cache.get_client_event_index(&key2, 200), Some(1));
    }

    #[test]
    fn rollback_then_retry_same_data() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // First attempt - fails
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));
        let _snapshot = cache.take_sync_positions_snapshot();
        cache.rollback_queue_positions();

        assert!(cache.force_durable_on_next_write());
        assert_eq!(cache.get_event_indexes(&key).event_index, 0);

        // Retry with same data
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 10, make_queue_item(None));

        // Queue should work normally
        assert_eq!(cache.get_event_indexes(&key).event_index, 5);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(10));

        // This time commit succeeds
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        assert!(!cache.force_durable_on_next_write());
        assert_eq!(cache.get_event_indexes(&key).event_index, 5);
    }

    #[test]
    fn cache_eviction_with_exact_size_boundary() {
        let mut cache = new_cache_with_config(test_config_small_cache(300));
        let key = make_aggregate_key(1, 1, 1);

        // Add entries that exactly fill the cache
        cache.cache_recent_write(key.clone(), 1, make_metablock(1, key.clone()), None, 100);
        cache.cache_recent_write(key.clone(), 2, make_metablock(2, key.clone()), None, 100);
        cache.cache_recent_write(key.clone(), 3, make_metablock(3, key.clone()), None, 100);

        // All 3 should fit
        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert_eq!(writes.len(), 3);

        // Add one more byte - should trigger eviction
        cache.cache_recent_write(key.clone(), 4, make_metablock(4, key.clone()), None, 1);

        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        // First entry should be evicted
        assert!(!writes.iter().any(|(idx, _)| *idx == 1));
        assert_eq!(writes.len(), 3); // entries 2, 3, 4
    }

    #[test]
    fn cache_handles_zero_size_entries() {
        let mut cache = new_cache_with_config(test_config_small_cache(100));
        let key = make_aggregate_key(1, 1, 1);

        // Add zero-size entries
        cache.cache_recent_write(key.clone(), 1, make_metablock(1, key.clone()), None, 0);
        cache.cache_recent_write(key.clone(), 2, make_metablock(2, key.clone()), None, 0);
        cache.cache_recent_write(key.clone(), 3, make_metablock(3, key.clone()), None, 0);

        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert_eq!(writes.len(), 3);

        // All have zero size
        for (_, write) in writes {
            assert_eq!(write.size_bytes, 0);
        }
    }

    #[test]
    fn snapshot_buffer_calculations_match_cache() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(Some(100)));
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(Some(200)));
        cache.add_to_pending_append_queue(&key, 3, 3, 100, 3, make_queue_item(None));

        // Get values from cache before snapshot
        let cache_datablocks = cache.buffer_size_datablocks();
        let cache_metablocks = cache.buffer_size_metablocks();

        let snapshot = cache.take_sync_positions_snapshot();

        // Snapshot should have same values
        assert_eq!(snapshot.buffer_size_datablocks(), cache_datablocks);
        assert_eq!(snapshot.buffer_size_metablocks(), cache_metablocks);
    }

    // =============================================================================
    // Stress Tests
    // =============================================================================

    #[test]
    fn stress_many_aggregates() {
        let mut cache = new_cache();

        // Add writes to many different aggregates
        for i in 0..1000u128 {
            let key = make_aggregate_key(1, 1, i);
            cache.add_to_pending_append_queue(&key, 1, 1, i, 1, make_queue_item(None));
        }

        assert!(cache.requires_write());

        let snapshot = cache.take_sync_positions_snapshot();
        assert_eq!(snapshot.pending_append_queue.len(), 1000);
        assert_eq!(snapshot.aggregate_queue_positions.len(), 1000);

        cache.commit_sync_positions_snapshot(snapshot);

        // Verify all aggregates are committed
        for i in 0..1000u128 {
            let key = make_aggregate_key(1, 1, i);
            assert_eq!(cache.get_client_event_index(&key, i), Some(1));
        }
    }

    #[test]
    fn stress_many_writes_same_aggregate() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Add many writes to same aggregate
        for i in 1..=1000u64 {
            cache.add_to_pending_append_queue(&key, i, i, 100, i, make_queue_item(None));
        }

        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 1000);
        assert_eq!(indexes.event_batch_index, 1000);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(1000));

        let snapshot = cache.take_sync_positions_snapshot();
        assert_eq!(snapshot.pending_append_queue.len(), 1000);

        cache.commit_sync_positions_snapshot(snapshot);

        assert_eq!(cache.get_event_indexes(&key).event_index, 1000);
    }

    #[test]
    fn stress_many_clients_same_aggregate() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Many different clients writing
        for client_id in 1..=500u128 {
            cache.add_to_pending_append_queue(
                &key,
                client_id as u64,
                client_id as u64,
                client_id,
                client_id as u64 * 10,
                make_queue_item(None),
            );
        }

        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Verify all clients are tracked
        for client_id in 1..=500u128 {
            assert_eq!(
                cache.get_client_event_index(&key, client_id),
                Some(client_id as u64 * 10)
            );
        }
    }

    #[test]
    fn stress_repeated_snapshot_commit_cycles() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        for cycle in 1..=100u64 {
            // Add writes
            cache.add_to_pending_append_queue(&key, cycle, cycle, 100, cycle, make_queue_item(None));

            // Snapshot and commit
            let snapshot = cache.take_sync_positions_snapshot();
            cache.commit_sync_positions_snapshot(snapshot);

            // Verify state
            assert_eq!(cache.get_event_indexes(&key).event_index, cycle);
            assert_eq!(cache.get_client_event_index(&key, 100), Some(cycle));
            assert!(!cache.requires_write());
        }
    }

    #[test]
    fn stress_cache_eviction_many_entries() {
        let mut cache = new_cache_with_config(test_config_small_cache(1000));
        let key = make_aggregate_key(1, 1, 1);

        // Add many small entries - will cause lots of evictions
        for batch_index in 1..=100u64 {
            cache.cache_recent_write(
                key.clone(),
                batch_index,
                make_metablock(batch_index, key.clone()),
                None,
                50,
            );
        }

        // Only most recent entries should remain (1000 / 50 = 20 max entries)
        let writes: Vec<_> = cache.get_cached_writes_from(&key, 0).unwrap().collect();
        assert!(writes.len() <= 20);

        // Most recent should be present
        assert!(writes.iter().any(|(idx, _)| *idx == 100));

        // Oldest should be evicted
        assert!(!writes.iter().any(|(idx, _)| *idx == 1));
    }

    // =============================================================================
    // SyncPositionsSnapshot Tests
    // =============================================================================

    #[test]
    fn sync_positions_snapshot_buffer_size_calculations() {
        let snapshot = SyncPositionsSnapshot {
            pending_append_queue: vec![
                make_queue_item(Some(100)),
                make_queue_item(Some(200)),
                make_queue_item(None),
            ],
            aggregate_queue_positions: HashMap::new(),
            metablocks_position: 512,
            datablocks_position: 1024 * 1024,
            file_len: 1024 * 1024 * 1024,
            wal_index: 13,
            datablocks_carry_over: None,
        };

        assert_eq!(snapshot.buffer_size_datablocks(), 300);
        assert_eq!(
            snapshot.buffer_size_metablocks(),
            3 * FIXED_BLOCK_SIZE_BYTES as u64
        );
    }

    #[test]
    fn sync_positions_snapshot_has_enough_free_space() {
        let snapshot = SyncPositionsSnapshot {
            pending_append_queue: vec![make_queue_item(Some(100))],
            aggregate_queue_positions: HashMap::new(),
            metablocks_position: 512,
            datablocks_position: 10000,
            file_len: 1024 * 1024,
            datablocks_carry_over: None,
            wal_index: 13,
        };

        assert!(snapshot.has_enough_free_space());
    }

    #[test]
    fn sync_positions_snapshot_not_enough_space() {
        let snapshot = SyncPositionsSnapshot {
            pending_append_queue: vec![make_queue_item(Some(10000))],
            aggregate_queue_positions: HashMap::new(),
            metablocks_position: 1000,
            datablocks_position: 1100, // Only 100 bytes free
            file_len: 2000,
            datablocks_carry_over: None,
            wal_index: 13,
        };

        assert!(!snapshot.has_enough_free_space());
    }

    // =============================================================================
    // AggregatePositions Tests
    // =============================================================================

    #[test]
    fn aggregate_positions_default_values() {
        let positions = QueueAggregatePositions::default();

        assert_eq!(positions.event_index, 0);
        assert_eq!(positions.event_batch_index, 0);
        assert!(positions.client_event_indexes.is_empty());
    }

    // =============================================================================
    // RecentWrite Tests
    // =============================================================================

    #[test]
    fn recent_write_stores_all_fields() {
        let key = make_aggregate_key(1, 1, 1);
        let metablock = make_metablock(42, key);
        let datablock = make_datablock();

        let recent_write = RecentWrite {
            metablock: metablock.clone(),
            datablock: Some(datablock),
            size_bytes: 1234,
        };

        assert_eq!(recent_write.metablock.wal_index, 42);
        assert!(recent_write.datablock.is_some());
        assert_eq!(recent_write.size_bytes, 1234);
    }

    // =============================================================================
    // Complex Scenario Tests
    // =============================================================================

    #[test]
    fn scenario_timeseries_non_durable_with_delayed_failure_notification() {
        // Simulates non-durable writes where clients don't wait for fsync
        // but need to be notified of failures on next write
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // First batch - succeeds
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let snapshot1 = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot1);
        assert!(!cache.force_durable_on_next_write());

        // Second batch - fsync fails
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        let _snapshot2 = cache.take_sync_positions_snapshot();
        cache.rollback_queue_positions();

        // Flag should be set - next write needs to force durable to notify client
        assert!(cache.force_durable_on_next_write());

        // Third batch - client sends more data, but server should force sync
        // to notify about the failure
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));

        // Server checks this flag before acking to client
        assert!(cache.force_durable_on_next_write());

        // Now it succeeds
        let snapshot3 = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot3);

        // Flag cleared after successful commit
        assert!(!cache.force_durable_on_next_write());
    }

    #[test]
    fn scenario_multiple_aggregate_types_same_org() {
        let mut cache = new_cache();
        let user_events = make_aggregate_key(1, 1, 100); // org 1, type 1 (users), user 100
        let order_events = make_aggregate_key(1, 2, 500); // org 1, type 2 (orders), order 500
        let payment_events = make_aggregate_key(1, 3, 999); // org 1, type 3 (payments), payment 999

        // Different clients writing to different aggregate types
        cache.add_to_pending_append_queue(&user_events, 1, 1, 1000, 1, make_queue_item(None));
        cache.add_to_pending_append_queue(&order_events, 1, 1, 2000, 1, make_queue_item(None));
        cache.add_to_pending_append_queue(&payment_events, 1, 1, 3000, 1, make_queue_item(None));

        let snapshot = cache.take_sync_positions_snapshot();
        assert_eq!(snapshot.aggregate_queue_positions.len(), 3);

        cache.commit_sync_positions_snapshot(snapshot);

        // Each aggregate type/id combination is independent
        assert_eq!(cache.get_client_event_index(&user_events, 1000), Some(1));
        assert_eq!(cache.get_client_event_index(&order_events, 2000), Some(1));
        assert_eq!(cache.get_client_event_index(&payment_events, 3000), Some(1));

        // Cross-checks: client from one aggregate shouldn't appear in another
        assert_eq!(cache.get_client_event_index(&user_events, 2000), None);
        assert_eq!(cache.get_client_event_index(&order_events, 1000), None);
    }

    #[test]
    fn scenario_high_frequency_writes_with_batching() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Simulate high-frequency writes being batched
        // Multiple events arrive before fsync completes

        // Batch 1 starts
        for i in 1..=10u64 {
            cache.add_to_pending_append_queue(&key, i, i, 100, i, make_queue_item(Some(50)));
        }

        let snapshot1 = cache.take_sync_positions_snapshot();
        assert_eq!(snapshot1.pending_append_queue.len(), 10);

        // While batch 1 is syncing, batch 2 arrives
        for i in 11..=20u64 {
            cache.add_to_pending_append_queue(&key, i, i, 100, i, make_queue_item(Some(50)));
        }

        // Batch 1 commits
        cache.commit_sync_positions_snapshot(snapshot1);

        // Batch 2 is still pending
        assert!(cache.requires_write());
        assert_eq!(cache.get_event_indexes(&key).event_index, 20); // Queue has latest

        // Batch 2 syncs
        let snapshot2 = cache.take_sync_positions_snapshot();
        assert_eq!(snapshot2.pending_append_queue.len(), 10);

        // While batch 2 is syncing, batch 3 arrives
        for i in 21..=30u64 {
            cache.add_to_pending_append_queue(&key, i, i, 100, i, make_queue_item(Some(50)));
        }

        // Batch 2 commits
        cache.commit_sync_positions_snapshot(snapshot2);

        // Final batch 3
        let snapshot3 = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot3);

        // All 30 events should be tracked
        assert_eq!(cache.get_event_indexes(&key).event_index, 30);
        assert_eq!(cache.get_client_event_index(&key, 100), Some(30));
    }

    #[test]
    fn scenario_cache_serves_recent_reads() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Write and commit several batches
        for batch in 1..=5u64 {
            cache.add_to_pending_append_queue(&key, batch, batch, 100, batch, make_queue_item(Some(100)));
            let snapshot = cache.take_sync_positions_snapshot();
            cache.commit_sync_positions_snapshot(snapshot);

            // After commit, populate cache
            cache.cache_recent_write(
                key.clone(),
                batch,
                make_metablock(batch, key.clone()),
                Some(make_datablock()),
                100,
            );
        }

        // Read from cache - should get all 5 batches
        let all_writes: Vec<_> = cache.get_cached_writes_from(&key, 1).unwrap().collect();
        assert_eq!(all_writes.len(), 5);

        // Read from batch 3 onwards
        let recent_writes: Vec<_> = cache.get_cached_writes_from(&key, 3).unwrap().collect();
        assert_eq!(recent_writes.len(), 3);
        assert_eq!(recent_writes[0].0, 3);
        assert_eq!(recent_writes[2].0, 5);
    }

    #[test]
    fn scenario_log_rotation_mid_write() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Initial writes
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // More writes pending
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(None));
        cache.add_to_pending_append_queue(&key, 3, 3, 100, 3, make_queue_item(None));

        // Log file is full - rotate to new log
        cache.rotate_to_next_log(
            1, // new log id
            FIXED_BLOCK_SIZE_BYTES as u64,
            1024 * 1024 - FIXED_BLOCK_SIZE_BYTES as u64,
            1024 * 1024,
        );

        assert_eq!(cache.current_log_id(), 1);

        // Pending writes should still be there
        assert!(cache.requires_write());

        // Committed positions should still be there
        assert_eq!(cache.get_client_event_index(&key, 100), Some(3)); // From queue

        // Complete the pending writes in new log
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        assert!(!cache.requires_write());
        assert_eq!(cache.get_event_indexes(&key).event_index, 3);
    }

    #[test]
    fn scenario_idempotency_rejection_flow() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Client 100 writes event with client_event_index 5
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 5, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Later, client 100 tries to write with client_event_index 3 (already processed)
        // Server checks idempotency before accepting
        let client_last_index = cache.get_client_event_index(&key, 100);
        assert_eq!(client_last_index, Some(5));

        // Server would reject: 3 <= 5, so this is a duplicate
        let proposed_client_index = 3u64;
        let is_duplicate = client_last_index
            .map(|last| proposed_client_index <= last)
            .unwrap_or(false);
        assert!(is_duplicate);

        // Client 100 tries with index 6 - should be accepted
        let proposed_client_index = 6u64;
        let is_duplicate = client_last_index
            .map(|last| proposed_client_index <= last)
            .unwrap_or(false);
        assert!(!is_duplicate);
    }

    #[test]
    fn scenario_queue_idempotency_check_before_disk() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // First write goes to queue (not yet on disk)
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 10, make_queue_item(None));

        // Before disk write completes, same client sends duplicate
        // Idempotency check should work from queue
        let client_last_index = cache.get_client_event_index(&key, 100);
        assert_eq!(client_last_index, Some(10));

        // Duplicate would be rejected even though nothing is on disk yet
        let proposed_client_index = 5u64;
        let is_duplicate = client_last_index
            .map(|last| proposed_client_index <= last)
            .unwrap_or(false);
        assert!(is_duplicate);
    }

    #[test]
    fn scenario_mixed_datablock_sizes() {
        let mut cache = new_cache();
        let key = make_aggregate_key(1, 1, 1);

        // Mix of writes with and without datablocks of various sizes
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(None)); // No datablock
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 2, make_queue_item(Some(100))); // Small
        cache.add_to_pending_append_queue(&key, 3, 3, 100, 3, make_queue_item(Some(10000))); // Large
        cache.add_to_pending_append_queue(&key, 4, 4, 100, 4, make_queue_item(None)); // No datablock
        cache.add_to_pending_append_queue(&key, 5, 5, 100, 5, make_queue_item(Some(50))); // Small

        assert_eq!(cache.buffer_size_datablocks(), 100 + 10000 + 50);
        assert_eq!(
            cache.buffer_size_metablocks(),
            5 * FIXED_BLOCK_SIZE_BYTES as u64
        );

        let snapshot = cache.take_sync_positions_snapshot();
        assert_eq!(snapshot.buffer_size_datablocks(), 100 + 10000 + 50);
    }

    // =============================================================================
    // LRU Cache Capacity Tests
    // =============================================================================

    fn test_config_small_lru_caches(aggregate_cap: u64, client_cap: u64) -> InternalShardConfig {
        InternalShardConfig {
            aggregate_snapshots_cache_bytes: aggregate_cap * 112,
            aggregate_client_snapshots_cache_bytes: client_cap * 128,
            ..test_config()
        }
    }

    #[test]
    fn aggregate_snapshots_cache_respects_capacity() {
        let mut cache = new_cache_with_config(test_config_small_lru_caches(3, 100));

        // Add 5 different aggregates, but capacity is only 3
        for i in 1..=5u128 {
            let key = make_aggregate_key(1, 1, i);
            cache.add_to_pending_append_queue(&key, i as u64, i as u64, 100, 1, make_queue_item(None));
            let snapshot = cache.take_sync_positions_snapshot();
            cache.commit_sync_positions_snapshot(snapshot);
        }

        // Only the 3 most recent should be in cache
        // Aggregates 1 and 2 should have been evicted
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(1, 1, 2);
        let key3 = make_aggregate_key(1, 1, 3);
        let key4 = make_aggregate_key(1, 1, 4);
        let key5 = make_aggregate_key(1, 1, 5);

        // Keys 3, 4, 5 should still be accessible
        assert_eq!(cache.get_event_indexes(&key5).event_index, 5);
        assert_eq!(cache.get_event_indexes(&key4).event_index, 4);
        assert_eq!(cache.get_event_indexes(&key3).event_index, 3);

        // Keys 1, 2 should return defaults (evicted from LRU)
        assert_eq!(cache.get_event_indexes(&key1).event_index, 0);
        assert_eq!(cache.get_event_indexes(&key2).event_index, 0);
    }

    #[test]
    fn aggregate_client_snapshots_cache_respects_capacity() {
        let mut cache = new_cache_with_config(test_config_small_lru_caches(100, 3));
        let key = make_aggregate_key(1, 1, 1);

        // Add 5 different clients, but capacity is only 3
        for client_id in 1..=5u128 {
            cache.add_to_pending_append_queue(
                &key,
                client_id as u64,
                client_id as u64,
                client_id,
                client_id as u64 * 10,
                make_queue_item(None),
            );
            let snapshot = cache.take_sync_positions_snapshot();
            cache.commit_sync_positions_snapshot(snapshot);
        }

        // Only the 3 most recent clients should be in cache
        // Clients 3, 4, 5 should still be accessible
        assert_eq!(cache.get_client_event_index(&key, 5), Some(50));
        assert_eq!(cache.get_client_event_index(&key, 4), Some(40));
        assert_eq!(cache.get_client_event_index(&key, 3), Some(30));

        // Clients 1, 2 should return None (evicted from LRU)
        assert_eq!(cache.get_client_event_index(&key, 1), None);
        assert_eq!(cache.get_client_event_index(&key, 2), None);
    }

    #[test]
    fn aggregate_snapshots_lru_promotes_on_access() {
        let mut cache = new_cache_with_config(test_config_small_lru_caches(3, 100));

        // Add 3 aggregates to fill cache
        for i in 1..=3u128 {
            let key = make_aggregate_key(1, 1, i);
            cache.add_to_pending_append_queue(&key, i as u64, i as u64, 100, 1, make_queue_item(None));
            let snapshot = cache.take_sync_positions_snapshot();
            cache.commit_sync_positions_snapshot(snapshot);
        }

        // Access key1 to promote it (make it recently used)
        let key1 = make_aggregate_key(1, 1, 1);
        let _ = cache.get_event_indexes(&key1);

        // Add a new aggregate - should evict key2 (oldest unused), not key1
        let key4 = make_aggregate_key(1, 1, 4);
        cache.add_to_pending_append_queue(&key4, 4, 4, 100, 1, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // key1 should still be there (was promoted)
        assert_eq!(cache.get_event_indexes(&key1).event_index, 1);

        // key2 should be evicted
        let key2 = make_aggregate_key(1, 1, 2);
        assert_eq!(cache.get_event_indexes(&key2).event_index, 0);

        // key3 and key4 should be there
        let key3 = make_aggregate_key(1, 1, 3);
        assert_eq!(cache.get_event_indexes(&key3).event_index, 3);
        assert_eq!(cache.get_event_indexes(&key4).event_index, 4);
    }

    #[test]
    fn aggregate_client_snapshots_lru_promotes_on_access() {
        let mut cache = new_cache_with_config(test_config_small_lru_caches(100, 3));
        let key = make_aggregate_key(1, 1, 1);

        // Add 3 clients to fill cache
        for client_id in 1..=3u128 {
            cache.add_to_pending_append_queue(&key, 1, 1, client_id, client_id as u64, make_queue_item(None));
            let snapshot = cache.take_sync_positions_snapshot();
            cache.commit_sync_positions_snapshot(snapshot);
        }

        // Access client 1 to promote it
        let _ = cache.get_client_event_index(&key, 1);

        // Add new client - should evict client 2, not client 1
        cache.add_to_pending_append_queue(&key, 2, 2, 4, 4, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Client 1 should still be there (was promoted)
        assert_eq!(cache.get_client_event_index(&key, 1), Some(1));

        // Client 2 should be evicted
        assert_eq!(cache.get_client_event_index(&key, 2), None);

        // Clients 3 and 4 should be there
        assert_eq!(cache.get_client_event_index(&key, 3), Some(3));
        assert_eq!(cache.get_client_event_index(&key, 4), Some(4));
    }

    #[test]
    fn aggregate_snapshots_cache_updates_existing_entries() {
        let mut cache = new_cache_with_config(test_config_small_lru_caches(3, 100));
        let key = make_aggregate_key(1, 1, 1);

        // Add initial entry
        cache.add_to_pending_append_queue(&key, 5, 3, 100, 1, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Update same aggregate with higher values
        cache.add_to_pending_append_queue(&key, 10, 7, 100, 2, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Should have updated values, not create duplicate entry
        let indexes = cache.get_event_indexes(&key);
        assert_eq!(indexes.event_index, 10);
        assert_eq!(indexes.event_batch_index, 7);
    }

    #[test]
    fn aggregate_client_snapshots_cache_updates_existing_entries() {
        let mut cache = new_cache_with_config(test_config_small_lru_caches(100, 3));
        let key = make_aggregate_key(1, 1, 1);

        // Add initial entry for client 100
        cache.add_to_pending_append_queue(&key, 1, 1, 100, 5, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Update same client with higher value
        cache.add_to_pending_append_queue(&key, 2, 2, 100, 15, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // Should have updated value
        assert_eq!(cache.get_client_event_index(&key, 100), Some(15));
    }

    #[test]
    fn aggregate_snapshot_in_cache_checks_both_queue_and_lru() {
        let mut cache = new_cache_with_config(test_config_small_lru_caches(100, 100));
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(1, 1, 2);
        let key3 = make_aggregate_key(1, 1, 3);

        // key1 only in queue (not committed)
        cache.add_to_pending_append_queue(&key1, 1, 1, 100, 1, make_queue_item(None));

        // key2 committed to LRU
        cache.add_to_pending_append_queue(&key2, 1, 1, 100, 1, make_queue_item(None));
        let snapshot = cache.take_sync_positions_snapshot();
        cache.commit_sync_positions_snapshot(snapshot);

        // key1 should still be in queue
        cache.add_to_pending_append_queue(&key1, 2, 2, 100, 2, make_queue_item(None));

        // Both should return true
        assert!(cache.aggregate_snapshot_in_cache(&key1)); // in queue
        assert!(cache.aggregate_snapshot_in_cache(&key2)); // in LRU

        // key3 not anywhere
        assert!(!cache.aggregate_snapshot_in_cache(&key3));
    }

    #[test]
    fn stress_lru_caches_with_many_aggregates_and_clients() {
        let mut cache = new_cache_with_config(test_config_small_lru_caches(50, 100));

        // Add 200 aggregates, each with 2 clients
        for i in 1..=200u128 {
            let key = make_aggregate_key(1, 1, i);
            cache.add_to_pending_append_queue(&key, i as u64, i as u64, i * 1000, i as u64, make_queue_item(None));
            cache.add_to_pending_append_queue(&key, i as u64, i as u64, i * 1000 + 1, i as u64, make_queue_item(None));
            let snapshot = cache.take_sync_positions_snapshot();
            cache.commit_sync_positions_snapshot(snapshot);
        }

        // Only last 50 aggregates should be in aggregate cache
        for i in 1..=150u128 {
            let key = make_aggregate_key(1, 1, i);
            assert_eq!(cache.get_event_indexes(&key).event_index, 0, "Aggregate {} should be evicted", i);
        }

        for i in 151..=200u128 {
            let key = make_aggregate_key(1, 1, i);
            assert_eq!(cache.get_event_indexes(&key).event_index, i as u64, "Aggregate {} should be in cache", i);
        }

        // Only last 100 client entries should be in client cache (200 aggregates * 2 clients = 400 entries)
        // Most recent 100 would be from aggregates 151-200 (100 entries)
        let key_200 = make_aggregate_key(1, 1, 200);
        assert_eq!(cache.get_client_event_index(&key_200, 200 * 1000), Some(200));
        assert_eq!(cache.get_client_event_index(&key_200, 200 * 1000 + 1), Some(200));
    }
}