//! Sync and durability functions for the shard WAL.
//!
//! Handles writing pending queue items to disk, updating metadata,
//! and broadcasting watch events after successful fsync.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_memcache::sync_positions_snapshot::SyncPositionsSnapshot;
use celeriant_rotating_log::log_segment_file::LogSegmentFile;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_rotating_log::rotating_log_error::RotatingLogError;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::{FIRST_EVENT_BATCH_INDEX, FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK};
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_watch::aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation};
use celeriant_watch::aggregate_watchers::AggregateWatchers;
use celeriant_wire::version_aware_wire_format::serialize_versioned_message;
use deepsize::DeepSizeOf;

use crate::error::shard_fsync_error::ShardFsyncError;

const STRUCT_TO_MEMORY_REAL_SIZE: usize = 3;

/// Extends the write batch index bounds in an AggregateWatchEventOperation when writing multiple batches for the same aggregate.
pub(crate) fn add_write_event(write_events: &mut HashMap<AggregateKey, AggregateWatchEventOperation>, metablock_event_batch: &MetablockEventBatch) {
    write_events
        .entry(metablock_event_batch.aggregate_key.clone())
        .and_modify(|event| {
            if let AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            } = event
            {
                if metablock_event_batch.event_batch_index < *from_event_batch_index {
                    *from_event_batch_index = metablock_event_batch.event_batch_index;
                }
                if metablock_event_batch.event_batch_index > *to_event_batch_index {
                    *to_event_batch_index = metablock_event_batch.event_batch_index;
                }
            }
        })
        .or_insert(AggregateWatchEventOperation::Write {
            from_event_batch_index: metablock_event_batch.event_batch_index,
            to_event_batch_index: metablock_event_batch.event_batch_index,
        });
}

/// Records a create event for an aggregate.
pub(crate) fn add_create_event(create_events: &mut HashMap<AggregateKey, AggregateWatchEventOperation>, aggregate_key: AggregateKey) {
    create_events.entry(aggregate_key).or_insert(AggregateWatchEventOperation::Create {});
}

/// Orchestrates the sync process with rollback on failure.
///
/// Takes a snapshot of pending writes, attempts to write them to disk,
/// and either commits the changes (updating caches and broadcasting events)
/// or rolls back on failure.
pub(crate) async fn sync_with_rollback(
    log_segments_cache: Rc<LogSegmentsCache>,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
) -> Result<(), ShardFsyncError> {
    let (required_disk_space, mut sync_positions_snapshot) = {
        let mut shard_mem_cache = shard_mem_cache.borrow_mut();

        if shard_mem_cache.pending_append_queue_is_empty() {
            return Ok(());
        }

        let required_disk_space = shard_mem_cache
            .buffer_size_datablocks()
            .saturating_add(shard_mem_cache.buffer_size_metablocks());
        let sync_positions_snapshot = shard_mem_cache.take_sync_positions_snapshot();

        (required_disk_space, sync_positions_snapshot)
    };

    //TODO: Under rare scenarios (eg. embedded and small log size) this is not enough
    //It should really be partitioning the pending queue into chunks < log size and looping
    log_segments_cache.rotate_to_next_log(required_disk_space).await?;
    let active_log_segment = log_segments_cache.active();

    match sync(active_log_segment, &mut sync_positions_snapshot).await {
        Ok(_) => {
            let mut shard_mem_cache = shard_mem_cache.borrow_mut();

            // Commit moves the carry over bytes back into memcache, so take queue first
            let pending_append_queue = std::mem::take(&mut sync_positions_snapshot.pending_append_queue);
            shard_mem_cache.commit_sync_positions_snapshot(sync_positions_snapshot);

            // Transform the queue into recent writes and broadcast events
            let mut write_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
            let mut create_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
            let mut delete_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
            let mut trim_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();

            for queue_item in pending_append_queue {
                match &queue_item.metablock.wal_metablock_type {
                    MetablockKind::EventBatchMetadata(event_batch_metadata) => {
                        // Build watched_aggregates events
                        add_write_event(&mut write_events, event_batch_metadata);

                        // If first event batch for aggregate, add create event
                        if event_batch_metadata.event_batch_index == FIRST_EVENT_BATCH_INDEX {
                            add_create_event(&mut create_events, event_batch_metadata.aggregate_key.clone());
                        }

                        let size_bytes =
                            ((queue_item.metablock.deep_size_of() + queue_item.datablock.deep_size_of()) * STRUCT_TO_MEMORY_REAL_SIZE) as u64;

                        // Cache the write (only happens after durable write confirmed)
                        shard_mem_cache.cache_recent_write(
                            event_batch_metadata.aggregate_key.clone(),
                            event_batch_metadata.event_batch_index,
                            queue_item.metablock,
                            queue_item.datablock,
                            size_bytes,
                        );
                    }
                    MetablockKind::SoftTrim(soft_trim) => {
                        // Update aggregate min_event_batch_index in cache
                        shard_mem_cache.update_aggregate_min_event_batch_index(&soft_trim.aggregate_key, soft_trim.keep_from_event_batch_index);

                        // Track trim event for watchers
                        trim_events
                            .entry(soft_trim.aggregate_key.clone())
                            .or_insert(AggregateWatchEventOperation::TrimStart {
                                keep_from_event_batch_index: soft_trim.keep_from_event_batch_index,
                            });
                    }
                    MetablockKind::SoftDelete(soft_delete) => {
                        // Mark aggregate as deleted in cache
                        shard_mem_cache.put_aggregate_into_cache_as_deleted(
                            soft_delete.aggregate_key.clone(),
                            soft_delete.event_index,
                            soft_delete.event_batch_index,
                            soft_delete.allow_recreate,
                            soft_delete.allow_index_continuation,
                        );

                        // Track delete event for watchers
                        delete_events
                            .entry(soft_delete.aggregate_key.clone())
                            .or_insert(AggregateWatchEventOperation::Delete {});
                    }
                    _ => {}
                }
            }

            for (aggregate_key, operation) in create_events {
                watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
            }

            for (aggregate_key, operation) in write_events {
                watched_aggregates.broadcast(AggregateWatchEvent {
                    aggregate_key: aggregate_key.clone(),
                    operation,
                });
            }

            for (aggregate_key, operation) in delete_events {
                watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
            }

            for (aggregate_key, operation) in trim_events {
                watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
            }

            return Ok(());
        }
        Err(e) => {
            let mut shard_mem_cache = shard_mem_cache.borrow_mut();
            shard_mem_cache.rollback_queue_positions();
            log_segments_cache.force_immediate.set(true);
            return Err(e);
        }
    }
}

/// Writes pending queue items to disk.
///
/// This function handles the low-level I/O:
/// 1. Writes datablocks first (growing downward from end of file)
/// 2. Updates metablocks with datablock positions
/// 3. Writes metablocks (growing upward from header)
/// 4. Updates bloom filter
/// 5. Writes dual headers
/// 6. Calls fdatasync for durability
pub(crate) async fn sync(log_segment_file: Rc<LogSegmentFile>, sync_positions_snapshot: &mut SyncPositionsSnapshot) -> Result<(), ShardFsyncError> {
    let mut log_segment_file_metadata = log_segment_file.metadata.borrow().clone();

    let dma_file_writer = log_segment_file.lock_writer("sync").await?;
    let dma_file_writer = dma_file_writer
        .as_ref()
        .ok_or_else(|| RotatingLogError::IoError("No file handle".to_string()))?;

    // Write datablocks first so we can get the positions to include into metablocks
    let buffer_size_datablocks: u64 = sync_positions_snapshot.buffer_size_datablocks();

    let mut datablocks_absolute_write_positions: Vec<u64> = Vec::with_capacity(sync_positions_snapshot.pending_append_queue.len());
    let mut new_datablocks_position = log_segment_file_metadata.datablocks_position;
    let mut datablocks_carry_over: Option<Vec<u8>> = log_segment_file_metadata.datablocks_carry_over.take();

    if buffer_size_datablocks > 0 {
        let write_to_pos = dma_file_writer.align_up(log_segment_file_metadata.datablocks_position);
        new_datablocks_position = log_segment_file_metadata.datablocks_position.saturating_sub(buffer_size_datablocks);
        let write_from_pos = dma_file_writer.align_down(new_datablocks_position);
        let aligned_buffer_size_datablocks = write_to_pos.saturating_sub(write_from_pos);

        let front_carry_over = new_datablocks_position.saturating_sub(write_from_pos) as usize;
        let end_carry_over = write_to_pos.saturating_sub(log_segment_file_metadata.datablocks_position) as usize;

        let mut buffer_datablocks = dma_file_writer.alloc_dma_buffer(front_carry_over + buffer_size_datablocks as usize + end_carry_over);
        let buffer_datablocks_slice = buffer_datablocks.as_bytes_mut();

        buffer_datablocks_slice.fill(0);

        if end_carry_over > 0 {
            if datablocks_carry_over.is_none() || datablocks_carry_over.as_ref().unwrap().len() != end_carry_over as usize {
                return Err(ShardFsyncError::DatablocksCarryOverBufferNotPresent);
            }
            buffer_datablocks_slice[(aligned_buffer_size_datablocks.saturating_sub(end_carry_over as u64)) as usize..]
                .copy_from_slice(&datablocks_carry_over.as_ref().unwrap());
        }

        let mut position = 0usize;
        for item in &sync_positions_snapshot.pending_append_queue {
            if let Some(datablock_bytes) = &item.datablock_bytes {
                let len = datablock_bytes.len();
                let start_idx = front_carry_over + position;
                let end_idx = front_carry_over + position + len;

                datablocks_absolute_write_positions.push(new_datablocks_position + position as u64);
                buffer_datablocks_slice[start_idx..end_idx].copy_from_slice(datablock_bytes);
                position += len;
            }
        }

        let datablocks_carry_over_size = dma_file_writer.align_up(new_datablocks_position).saturating_sub(new_datablocks_position);
        if datablocks_carry_over_size > 0 {
            datablocks_carry_over =
                Some(buffer_datablocks_slice[front_carry_over..(front_carry_over + datablocks_carry_over_size as usize)].to_vec());
        }

        dma_file_writer
            .write_at(buffer_datablocks, new_datablocks_position.saturating_sub(front_carry_over as u64))
            .await?;
    }

    let buffer_size_metablocks: u64 = sync_positions_snapshot.buffer_size_metablocks();
    let mut buffer_metablocks = dma_file_writer.alloc_dma_buffer(buffer_size_metablocks as usize);
    let buffer_metablocks_slice = buffer_metablocks.as_bytes_mut();
    let mut position = 0usize;
    let mut index = 0;
    for item in &mut sync_positions_snapshot.pending_append_queue {
        if item.datablock_bytes.is_some() && item.datablock.is_some() {
            match &mut item.metablock.datablock {
                DatablockStorageKind::Block(datablock_block_ref) => {
                    datablock_block_ref.datablock_position = datablocks_absolute_write_positions[index];
                }
                _ => {}
            }
            index += 1;
        }

        log_segment_file_metadata.wal_index = log_segment_file_metadata.wal_index.saturating_add(1);
        item.metablock.wal_index = log_segment_file_metadata.wal_index;

        // Track the absolute position where this metablock is written
        let metablock_absolute_pos = log_segment_file_metadata.metablocks_position + position as u64;

        // Update aggregate positions tracking for event batches (only if entry exists)
        if let MetablockKind::EventBatchMetadata(event_batch) = &item.metablock.wal_metablock_type {
            if let Some(aggregate_positions) = sync_positions_snapshot.aggregate_queue_positions.get_mut(&event_batch.aggregate_key) {
                aggregate_positions.log_id = log_segment_file_metadata.log_id;
                aggregate_positions.metablock_absolute_pos = metablock_absolute_pos;
            }
        }

        let mut metablock_bytes = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&item.metablock, WIRE_VERSION_WAL_METABLOCK, &mut metablock_bytes)?;

        //let metablock_bytes: [u8; FIXED_BLOCK_SIZE_BYTES]
        buffer_metablocks_slice[position..position + FIXED_BLOCK_SIZE_BYTES].copy_from_slice(&metablock_bytes);
        position += FIXED_BLOCK_SIZE_BYTES;
    }

    //Write metablocks
    let new_metablocks_position = log_segment_file_metadata.metablocks_position + buffer_metablocks.len() as u64;
    dma_file_writer
        .write_at(buffer_metablocks, log_segment_file_metadata.metablocks_position)
        .await?;

    // Update bloom filter with aggregate keys from this batch
    for item in &sync_positions_snapshot.pending_append_queue {
        if let MetablockKind::EventBatchMetadata(event_batch) = &item.metablock.wal_metablock_type {
            log_segment_file_metadata.aggregate_key_bloom.insert(&event_batch.aggregate_key);
        }
    }

    // Update positions and carry over
    log_segment_file_metadata.metablocks_position = new_metablocks_position;
    log_segment_file_metadata.datablocks_carry_over = datablocks_carry_over;
    log_segment_file_metadata.datablocks_position = new_datablocks_position;

    // Write header
    let header_end_start_pos = log_segment_file_metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
    let header = log_segment_file_metadata.to_shard_log_header();
    celeriant_rotating_log::log_segment_file::write_dual_shard_log_header(&dma_file_writer, header_end_start_pos, &header).await?;

    dma_file_writer.fdatasync().await?;

    let mut updated_log_segment_file_metadata = log_segment_file.metadata.borrow_mut();
    *updated_log_segment_file_metadata = log_segment_file_metadata;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::constants::BLOOM_BYTES;
    use celeriant_wal::metablocks::metablock_event_batch::EventTypesKind;

    fn make_aggregate_key(org: u128, agg_type: u128, agg_id: u128) -> AggregateKey {
        AggregateKey::new(org, agg_type, agg_id)
    }

    fn make_event_batch(aggregate_key: AggregateKey, event_batch_index: u64) -> MetablockEventBatch {
        MetablockEventBatch {
            client_id: 1,
            user_id: None,
            aggregate_key,
            event_batch_index,
            min_event_batch_index: 1,
            min_event_index: 1,
            max_event_index: 1,
            min_event_timestamp: 0,
            max_event_timestamp: 0,
            min_client_event_index: 1,
            max_client_event_index: 1,
            event_types_data: EventTypesKind::Direct([0u64; BLOOM_BYTES / 8]),
        }
    }

    #[test]
    fn add_write_event_inserts_new_aggregate() {
        let mut events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
        let key = make_aggregate_key(1, 2, 3);
        let batch = make_event_batch(key.clone(), 5);

        add_write_event(&mut events, &batch);

        assert_eq!(events.len(), 1);
        match events.get(&key) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 5);
                assert_eq!(*to_event_batch_index, 5);
            }
            _ => panic!("Expected Write event"),
        }
    }

    #[test]
    fn add_write_event_expands_range_with_lower_index() {
        let mut events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
        let key = make_aggregate_key(1, 2, 3);

        // Insert initial batch at index 10
        let batch1 = make_event_batch(key.clone(), 10);
        add_write_event(&mut events, &batch1);

        // Add batch at lower index 5
        let batch2 = make_event_batch(key.clone(), 5);
        add_write_event(&mut events, &batch2);

        match events.get(&key) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 5);
                assert_eq!(*to_event_batch_index, 10);
            }
            _ => panic!("Expected Write event"),
        }
    }

    #[test]
    fn add_write_event_expands_range_with_higher_index() {
        let mut events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
        let key = make_aggregate_key(1, 2, 3);

        // Insert initial batch at index 5
        let batch1 = make_event_batch(key.clone(), 5);
        add_write_event(&mut events, &batch1);

        // Add batch at higher index 15
        let batch2 = make_event_batch(key.clone(), 15);
        add_write_event(&mut events, &batch2);

        match events.get(&key) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 5);
                assert_eq!(*to_event_batch_index, 15);
            }
            _ => panic!("Expected Write event"),
        }
    }

    #[test]
    fn add_write_event_handles_multiple_aggregates() {
        let mut events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(2, 2, 2);

        add_write_event(&mut events, &make_event_batch(key1.clone(), 5));
        add_write_event(&mut events, &make_event_batch(key2.clone(), 10));
        add_write_event(&mut events, &make_event_batch(key1.clone(), 7));

        assert_eq!(events.len(), 2);

        match events.get(&key1) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 5);
                assert_eq!(*to_event_batch_index, 7);
            }
            _ => panic!("Expected Write event for key1"),
        }

        match events.get(&key2) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 10);
                assert_eq!(*to_event_batch_index, 10);
            }
            _ => panic!("Expected Write event for key2"),
        }
    }

    #[test]
    fn add_create_event_inserts_new_aggregate() {
        let mut events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
        let key = make_aggregate_key(1, 2, 3);

        add_create_event(&mut events, key.clone());

        assert_eq!(events.len(), 1);
        assert!(matches!(events.get(&key), Some(AggregateWatchEventOperation::Create {})));
    }

    #[test]
    fn add_create_event_does_not_duplicate() {
        let mut events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
        let key = make_aggregate_key(1, 2, 3);

        add_create_event(&mut events, key.clone());
        add_create_event(&mut events, key.clone());

        // Should still be only one entry
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn add_create_event_handles_multiple_aggregates() {
        let mut events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(2, 2, 2);
        let key3 = make_aggregate_key(3, 3, 3);

        add_create_event(&mut events, key1.clone());
        add_create_event(&mut events, key2.clone());
        add_create_event(&mut events, key3.clone());

        assert_eq!(events.len(), 3);
        assert!(matches!(events.get(&key1), Some(AggregateWatchEventOperation::Create {})));
        assert!(matches!(events.get(&key2), Some(AggregateWatchEventOperation::Create {})));
        assert!(matches!(events.get(&key3), Some(AggregateWatchEventOperation::Create {})));
    }
}
