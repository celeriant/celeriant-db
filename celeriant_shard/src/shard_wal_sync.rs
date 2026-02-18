use std::cell::RefCell;
use std::rc::Rc;

use celeriant_distributed::node_status::NodeStatus;
use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::pending_cache_item::PendingCacheItem;
use celeriant_memcache::pending_commit_data::PendingCommitData;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_memcache::sync_positions_snapshot::SyncPositionsSnapshot;
use celeriant_rotating_log::log_segment_file::log_segment_file::{LogSegmentFile, write_dual_shard_log_header};
use celeriant_rotating_log::log_segment_file::log_segment_file_metadata::LogSegmentFileMetadata;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::constants::{EntryHashBytes, FIRST_EVENT_BATCH_INDEX, FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK};

use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_watch::aggregate_watchers::AggregateWatchers;
use celeriant_wire::disk::versioned_block;
use celeriant_wire::disk::versioned_block::serialize_versioned_message;

use crate::amortisation::coordinator::CaptureResult;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::watch_event_collector::WatchEventCollector;

pub(crate) struct FsyncCapturedData {
    pub required_disk_space: u64,
    pub sync_positions_snapshot: SyncPositionsSnapshot,
}

pub(crate) fn capture_fsync_snapshot(shard_mem_cache: &Rc<RefCell<ShardMemCache>>) -> CaptureResult<FsyncCapturedData, ShardFsyncError> {
    let mut cache = shard_mem_cache.borrow_mut();

    if cache.take_fsync_rollback_flag() {
        return CaptureResult::Failed(ShardFsyncError::RollbackInvalidatedWrites);
    }

    if cache.pending_append_queue_is_empty() {
        return CaptureResult::NoCaptureRaceButOk;
    }

    let required_disk_space = cache.buffer_size_total();
    let sync_positions_snapshot = cache.take_sync_positions_snapshot();

    CaptureResult::Captured(FsyncCapturedData {
        required_disk_space,
        sync_positions_snapshot,
    })
}

pub(crate) async fn commit_fsync_with_rollback(
    node_status: NodeStatus,
    log_segments_cache: Rc<LogSegmentsCache>,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    mut captured: FsyncCapturedData, // Mutable because we set the datablocks_position while writing in metablocks
) -> Result<(), ShardFsyncError> {
    let available_space = log_segments_cache.active_log_available_space();
    if available_space < captured.required_disk_space {
        if log_segments_cache
            .preallocate_bytes
            .saturating_sub(captured.required_disk_space)
            .saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64 * 2)
            == 0
        {
            return Err(ShardFsyncError::BatchesTooLarge {
                preallocate_bytes: log_segments_cache.preallocate_bytes,
            });
        }

        log_segments_cache
            .rotate_to_next_log()
            .await
            .map_err(ShardFsyncError::UnableToRotateToNewLogSegmentFile)?;
    }

    let active_log_segment = log_segments_cache.active();

    match sync(active_log_segment.clone(), &mut captured.sync_positions_snapshot).await {
        Ok(updated_log_segment_file_metadata) => {
            commit_sync(
                node_status,
                shard_mem_cache,
                watched_aggregates,
                captured.sync_positions_snapshot,
                active_log_segment,
                updated_log_segment_file_metadata,
            );
            Ok(())
        }
        Err(e) => {
            rollback_sync(shard_mem_cache, &log_segments_cache);
            Err(e)
        }
    }
}

/// Commits a successful sync by updating caches and broadcasting watch events.
fn commit_sync(
    node_status: NodeStatus,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    mut sync_positions_snapshot: SyncPositionsSnapshot,
    log_segment_file: Rc<LogSegmentFile>,
    mut new_metadata: LogSegmentFileMetadata,
) {
    if !node_status.is_leader() {
        // Currently single node mode or is follower
        // Data is durable, so we can advance visible position
        new_metadata.advance_visible_position();
    }

    *log_segment_file.metadata.borrow_mut() = new_metadata;

    let mut shard_mem_cache = shard_mem_cache.borrow_mut();

    // Take the queue before committing the snapshot since commit consumes it
    let pending_append_queue = std::mem::take(&mut sync_positions_snapshot.pending_append_queue);

    shard_mem_cache.commit_sync_positions_snapshot(node_status, sync_positions_snapshot);

    let mut pending_commit_data = PendingCommitData {
        log_metadata: log_segment_file.metadata.borrow().clone(),
        pending_queue: Vec::with_capacity(pending_append_queue.len()),
    };

    // Collect watch events and update caches
    let mut event_collector = WatchEventCollector::new();

    for queue_item in pending_append_queue {
        match &queue_item.metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(event_batch_metadata) => {
                if !node_status.is_leader() {
                    event_collector.add_write_event(event_batch_metadata);

                    if event_batch_metadata.event_batch_index == FIRST_EVENT_BATCH_INDEX {
                        event_collector.add_create_event(event_batch_metadata.aggregate_key.clone());
                    }
                    let size_bytes = queue_item.size_bytes();
                    shard_mem_cache.cache_recent_write(
                        event_batch_metadata.aggregate_key.clone(),
                        event_batch_metadata.event_batch_index,
                        queue_item.metablock,
                        queue_item.datablock,
                        size_bytes,
                    );
                } else {
                    pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                }
            }
            MetablockKind::SoftTrim(soft_trim) => {
                shard_mem_cache.update_aggregate_min_event_batch_index(
                    &soft_trim.aggregate_key,
                    soft_trim.keep_from_event_batch_index,
                    CachePath::Write,
                );

                if !node_status.is_leader() {
                    shard_mem_cache.update_aggregate_min_event_batch_index(
                        &soft_trim.aggregate_key,
                        soft_trim.keep_from_event_batch_index,
                        CachePath::Read,
                    );
                    event_collector.add_trim_event(soft_trim.aggregate_key.clone(), soft_trim.keep_from_event_batch_index);
                } else {
                    pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                }
            }
            MetablockKind::SoftDelete(soft_delete) => {
                shard_mem_cache.put_aggregate_into_cache_as_deleted(
                    soft_delete.aggregate_key.clone(),
                    soft_delete.event_index,
                    soft_delete.event_batch_index,
                    soft_delete.allow_recreate,
                    soft_delete.allow_index_continuation,
                    CachePath::Write,
                );

                if !node_status.is_leader() {
                    shard_mem_cache.put_aggregate_into_cache_as_deleted(
                        soft_delete.aggregate_key.clone(),
                        soft_delete.event_index,
                        soft_delete.event_batch_index,
                        soft_delete.allow_recreate,
                        soft_delete.allow_index_continuation,
                        CachePath::Read,
                    );
                    event_collector.add_delete_event(soft_delete.aggregate_key.clone());
                } else {
                    pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                }
            }
            _ => {}
        }
    }

    if !node_status.is_leader() {
        event_collector.broadcast_all(&watched_aggregates);
    } else {
        // As leader, after fsync we can now allow replication to proceed
        shard_mem_cache.push_pending_replication(pending_commit_data);
    }
}

/// Rolls back a failed sync by restoring queue positions and forcing immediate rotation.
fn rollback_sync(shard_mem_cache: Rc<RefCell<ShardMemCache>>, log_segments_cache: &Rc<LogSegmentsCache>) {
    shard_mem_cache.borrow_mut().execute_fsync_rollback();
    log_segments_cache.force_immediate.set(true);
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
/// sync_positions_snapshot is mutable because we need to set the datablocks absolute position as we write (only known at write time)
pub(crate) async fn sync(
    log_segment_file: Rc<LogSegmentFile>,
    sync_positions_snapshot: &mut SyncPositionsSnapshot,
) -> Result<LogSegmentFileMetadata, ShardFsyncError> {
    let mut log_segment_file_metadata = log_segment_file.metadata.borrow().clone();

    let dma_file_writer = log_segment_file.lock_writer("sync").await
        .map_err(|_| ShardFsyncError::WriteLockTimeout)?;
    let dma_file_writer = dma_file_writer
        .as_ref()
        .ok_or_else(|| ShardFsyncError::ActiveWriteFileUnavailable)?;

    // Write datablocks first so we can get the positions to include into metablocks
    let buffer_size_datablocks: u64 = sync_positions_snapshot.buffer_size_datablocks();

    let mut datablocks_absolute_write_positions: Vec<u64> = Vec::with_capacity(sync_positions_snapshot.pending_append_queue.len());
    let mut new_datablocks_position = log_segment_file_metadata.write.datablocks_position;
    let mut datablocks_carry_over: Option<Vec<u8>> = log_segment_file_metadata.datablocks_carry_over.take();

    if buffer_size_datablocks > 0 {
        let write_to_pos = dma_file_writer.align_up(log_segment_file_metadata.write.datablocks_position);
        new_datablocks_position = log_segment_file_metadata.write.datablocks_position.saturating_sub(buffer_size_datablocks);
        let write_from_pos = dma_file_writer.align_down(new_datablocks_position);
        let aligned_buffer_size_datablocks = write_to_pos.saturating_sub(write_from_pos);

        let front_carry_over = new_datablocks_position.saturating_sub(write_from_pos) as usize;
        let end_carry_over = write_to_pos.saturating_sub(log_segment_file_metadata.write.datablocks_position) as usize;

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

        let mut position = buffer_size_datablocks as usize;
        for item in &sync_positions_snapshot.pending_append_queue {
            if let Some(datablock_bytes) = &item.datablock_bytes {
                let len = datablock_bytes.len();
                position -= len;
                let start_idx = front_carry_over + position;
                let end_idx = front_carry_over + position + len;

                datablocks_absolute_write_positions.push(new_datablocks_position + position as u64);
                buffer_datablocks_slice[start_idx..end_idx].copy_from_slice(datablock_bytes);
            }
        }

        let datablocks_carry_over_size = dma_file_writer.align_up(new_datablocks_position).saturating_sub(new_datablocks_position);
        if datablocks_carry_over_size > 0 {
            datablocks_carry_over =
                Some(buffer_datablocks_slice[front_carry_over..(front_carry_over + datablocks_carry_over_size as usize)].to_vec());
        }

        dma_file_writer
            .write_at(buffer_datablocks, new_datablocks_position.saturating_sub(front_carry_over as u64))
            .await
            .map_err(|e| ShardFsyncError::WriteDatablocksError(e.to_string()))?;
    }

    let buffer_size_metablocks: u64 = sync_positions_snapshot.buffer_size_metablocks();
    let mut buffer_metablocks = dma_file_writer.alloc_dma_buffer(buffer_size_metablocks as usize);
    let buffer_metablocks_slice = buffer_metablocks.as_bytes_mut();
    let mut position = 0usize;
    let mut index = 0;
    for item in &mut sync_positions_snapshot.pending_append_queue {
        if item.datablock_bytes.is_some() && item.datablock.is_some() {
            item.metablock.datablock_position = datablocks_absolute_write_positions[index];
            index += 1;
        }

        log_segment_file_metadata.write.wal_index = log_segment_file_metadata.write.wal_index.saturating_add(1);
        item.metablock.wal_index = log_segment_file_metadata.write.wal_index;

        // Track the absolute position where this metablock is written
        let metablock_absolute_pos = log_segment_file_metadata.write.metablocks_position + position as u64;
        item.metablock_absolute_pos = metablock_absolute_pos;

        // Update aggregate positions tracking for event batches (only if entry exists)
        if let MetablockKind::EventBatchMetadata(event_batch) = &item.metablock.wal_metablock_type {
            if let Some(aggregate_positions) = sync_positions_snapshot.aggregate_queue_positions.get_mut(&event_batch.aggregate_key) {
                aggregate_positions.log_id = log_segment_file_metadata.log_id;
                aggregate_positions.metablock_absolute_pos = metablock_absolute_pos;
            }
        }

        // Keep the chain - store the previous hash in the next metablock. Done before serialisation!
        item.metablock.previous_tip_hash = log_segment_file_metadata.write.tip_hash;

        let mut metablock_bytes = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&item.metablock, WIRE_VERSION_WAL_METABLOCK, &mut metablock_bytes)
            .map_err(|e| ShardFsyncError::MetablockSerialisationError(e.to_string()))?;

        // Compute hash chain, excluding datablock_position (node-local offset that differs between nodes)
        log_segment_file_metadata.write.tip_hash = compute_entry_hash(&log_segment_file_metadata.write.tip_hash, &metablock_bytes);

        //let metablock_bytes: [u8; FIXED_BLOCK_SIZE_BYTES]
        buffer_metablocks_slice[position..position + FIXED_BLOCK_SIZE_BYTES].copy_from_slice(&metablock_bytes);
        position += FIXED_BLOCK_SIZE_BYTES;
    }

    //Write metablocks
    let new_metablocks_position = log_segment_file_metadata.write.metablocks_position + buffer_metablocks.len() as u64;
    dma_file_writer
        .write_at(buffer_metablocks, log_segment_file_metadata.write.metablocks_position)
        .await
        .map_err(|e| ShardFsyncError::WriteMetablocksError(e.to_string()))?;

    // Update bloom filter with aggregate keys from this batch
    for item in &sync_positions_snapshot.pending_append_queue {
        if let MetablockKind::EventBatchMetadata(event_batch) = &item.metablock.wal_metablock_type {
            log_segment_file_metadata.write.aggregate_key_bloom.insert(&event_batch.aggregate_key);
        }
    }

    // Update positions and carry over
    log_segment_file_metadata.write.metablocks_position = new_metablocks_position;
    log_segment_file_metadata.datablocks_carry_over = datablocks_carry_over;
    log_segment_file_metadata.write.datablocks_position = new_datablocks_position;

    // Write header
    let header_end_start_pos = log_segment_file_metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
    let header = log_segment_file_metadata.to_shard_log_header();
    write_dual_shard_log_header(&dma_file_writer, header_end_start_pos, &header).await
        .map_err(ShardFsyncError::LogSegmentFileHeaderWriteFailure)?;

    dma_file_writer.fdatasync().await
        .map_err(|e| ShardFsyncError::FDataSyncError(e.to_string()))?;

    Ok(log_segment_file_metadata)
}

/// Hash chain: blake3(previous_hash || metablock_bytes), skipping the CRC (which covers
/// datablock_position) and the datablock_position field itself — both are node-local.
fn compute_entry_hash(previous_hash: &EntryHashBytes, content: &[u8]) -> EntryHashBytes {
    const CRC_END: usize = versioned_block::CRC_SIZE;
    const SKIP_START: usize = versioned_block::HEADER_SIZE + Metablock::OFFSET_DATABLOCK_POSITION;
    const SKIP_END: usize = SKIP_START + Metablock::WIRE_SIZE_DATABLOCK_POSITION;

    let mut hasher = blake3::Hasher::new();
    hasher.update(previous_hash);
    hasher.update(&content[CRC_END..SKIP_START]);
    hasher.update(&content[SKIP_END..]);
    *hasher.finalize().as_bytes()
}