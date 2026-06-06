use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use tracing::{debug, error};

use celeriant_distributed::node_status::NodeStatus;
use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::pending_cache_item::PendingCacheItem;
use celeriant_memcache::pending_commit_data::PendingCommitData;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use crate::schema_validator::CompiledValidator;

type MemCache = ShardMemCache<CompiledValidator>;
use celeriant_memcache::sync_positions_snapshot::SyncPositionsSnapshot;
use celeriant_rotating_log::log_segment_file::log_segment_file::{LogSegmentFile, write_dual_shard_log_header};
use celeriant_rotating_log::log_segment_file::log_segment_file_metadata::LogSegmentFileMetadata;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::constants::{self, EntryHashBytes, FIRST_AGGREGATE_VERSION, FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, MIN_WRITE_ALIGNMENT, WIRE_VERSION_SEGMENT_SUMMARY_BLOCK, WIRE_VERSION_WAL_METABLOCK};
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wal::segment_summary::{SegmentSummaryBlock, SegmentSummaryPayload};

use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_watch::aggregate_watchers::AggregateWatchers;
use celeriant_wire::disk::versioned_block;
use celeriant_wire::disk::versioned_block::{serialize_versioned_message, serialize_versioned_message_heap};

use crate::amortisation::coordinator::CaptureResult;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::watch_event_collector::WatchEventCollector;

pub(crate) struct FsyncCapturedData {
    pub required_disk_space: u64,
    pub sync_positions_snapshot: SyncPositionsSnapshot,
}

pub(crate) fn capture_fsync_snapshot(shard_mem_cache: &Rc<RefCell<MemCache>>) -> CaptureResult<FsyncCapturedData, ShardFsyncError> {
    let mut cache = shard_mem_cache.borrow_mut();

    if cache.take_fsync_rollback_flag() {
        return CaptureResult::Failed(ShardFsyncError::RollbackInvalidatedWrites);
    }

    if cache.pending_append_queue_is_empty() {
        // Writer's data was taken by an earlier cycle. If that cycle
        // succeeded, the data is fsynced + (for leader path) queued for
        // replication. If it failed via fsync rollback, the flag is set
        // and would have been observed above.
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
    shard_mem_cache: Rc<RefCell<MemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    mut captured: FsyncCapturedData, // Mutable because we set the datablocks_position while writing in metablocks
    shard_id: u32,
    dict_codec: Rc<DictCodec>,
) -> Result<(), ShardFsyncError> {
    let start = std::time::Instant::now();
    let batch_size = captured.sync_positions_snapshot.pending_append_queue.len();
    let shard_label = [("shard_id", shard_id.to_string())];

    debug!(
        shard_id,
        batch_count = batch_size,
        required_disk_bytes = captured.required_disk_space,
        is_leader = node_status.is_leader(),
        "Fsync batch captured"
    );

    let metablock_padding = constants::write_padding(captured.sync_positions_snapshot.buffer_size_metablocks());
    let total_required = captured.required_disk_space + metablock_padding + MIN_WRITE_ALIGNMENT - 1;
    let available_space = log_segments_cache.active_log_available_space();
    if available_space < total_required {
        if log_segments_cache
            .preallocate_bytes
            .saturating_sub(total_required)
            .saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64 * 2)
            == 0
        {
            return Err(ShardFsyncError::BatchesTooLarge {
                preallocate_bytes: log_segments_cache.preallocate_bytes,
            });
        }

        if node_status.is_leader() {
            // Leader: defer sidecar write until replication confirms.
            // Snapshot current accumulator for the sealed segment.
            let old_log_id = log_segments_cache.active_log_id();
            shard_mem_cache.borrow_mut().store_sealed_segment_summary(old_log_id);
        } else {
            // Non-leader: summary is complete, write sidecar now.
            write_segment_summary_sidecar(
                log_segments_cache.shard_dir(),
                log_segments_cache.active_log_id(),
                &shard_mem_cache,
            ).await?;
        }

        log_segments_cache
            .rotate_to_next_log()
            .await
            .map_err(ShardFsyncError::UnableToRotateToNewLogSegmentFile)?;
    }

    let active_log_segment = log_segments_cache.active();

    match sync(active_log_segment.clone(), &mut captured.sync_positions_snapshot, node_status).await {
        Ok(updated_log_segment_file_metadata) => {
            let wal_seq = updated_log_segment_file_metadata.write.wal_seq;
            commit_sync(
                node_status,
                shard_mem_cache,
                watched_aggregates,
                captured.sync_positions_snapshot,
                active_log_segment.clone(),
                updated_log_segment_file_metadata,
                &dict_codec,
            );
            metrics::histogram!("celeriant_fsync_duration_seconds", &shard_label).record(start.elapsed().as_secs_f64());
            metrics::histogram!("celeriant_fsync_batch_size", &shard_label).record(batch_size as f64);
            // Cursor gauges: advances set write before read so a scrape
            // landing between the two can only see read lower than truth;
            // read ≤ write holds in every interleaving. Rewind sites
            // (truncate, demotion cull) set read first for the same reason.
            metrics::gauge!("celeriant_wal_seq", &shard_label).set(wal_seq as f64);
            let read_wal_seq = active_log_segment.metadata.borrow().read.as_ref().map_or(0, |r| r.wal_seq);
            metrics::gauge!("celeriant_read_wal_seq", &shard_label).set(read_wal_seq as f64);
            Ok(())
        }
        Err(e) => {
            error!(shard_id, error = ?e, "Fsync failed, rolling back batch");
            rollback_sync(shard_mem_cache);
            Err(e)
        }
    }
}

/// Commits a successful sync by updating caches and broadcasting watch events.
fn commit_sync(
    node_status: NodeStatus,
    shard_mem_cache: Rc<RefCell<MemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    mut sync_positions_snapshot: SyncPositionsSnapshot,
    log_segment_file: Rc<LogSegmentFile>,
    mut new_metadata: LogSegmentFileMetadata,
    dict_codec: &DictCodec,
) {
    // Non-leader: advance read = write here. Leader: read stays behind until commit_pcd.
    if !node_status.is_leader() {
        new_metadata.advance_visible_position();
    }

    *log_segment_file.metadata.borrow_mut() = new_metadata;
    let log_id = log_segment_file.metadata.borrow().log_id;

    let mut shard_mem_cache = shard_mem_cache.borrow_mut();

    // Take the queue before committing the snapshot since commit consumes it
    let pending_append_queue = std::mem::take(&mut sync_positions_snapshot.pending_append_queue);

    // Extract disk positions for deleted aggregates before commit consumes the snapshot.
    // On the follower replication path, aggregate_queue_positions is empty (items arrive
    // via add_to_pending_queue which skips position tracking), so we fall back to the
    // queue item's own metablock_absolute_pos set during sync().
    let has_deletes = sync_positions_snapshot.aggregate_queue_positions.values().any(|pos| pos.pending_delete);
    let deleted_positions: std::collections::HashMap<_, _> = if has_deletes {
        sync_positions_snapshot
            .aggregate_queue_positions
            .iter()
            .filter(|(_, pos)| pos.pending_delete)
            .map(|(key, pos)| (key.clone(), (pos.log_id, pos.metablock_absolute_pos)))
            .collect()
    } else {
        std::collections::HashMap::new()
    };

    shard_mem_cache.commit_sync_positions_snapshot(node_status, sync_positions_snapshot);

    let mut pending_commit_data = PendingCommitData {
        log_metadata: log_segment_file.metadata.borrow().clone(),
        pending_queue: Vec::with_capacity(pending_append_queue.len()),
    };

    // Collect watch events and update caches
    let mut event_collector = WatchEventCollector::new();

    for queue_item in pending_append_queue {
        if !node_status.is_leader() {
            shard_mem_cache.update_segment_summary(&queue_item.metablock);
        }

        match &queue_item.metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(event_batch_metadata) => {
                if !node_status.is_leader() {
                    event_collector.add_write_event(event_batch_metadata);

                    if event_batch_metadata.aggregate_version == FIRST_AGGREGATE_VERSION {
                        event_collector.add_create_event(event_batch_metadata.aggregate_key.clone());
                    }

                    // Update read and write snapshots so the aggregate is visible.
                    // On the follower replication path, aggregate_queue_positions is empty
                    // so commit_sync_positions_snapshot won't update these.
                    shard_mem_cache.commit_position_snapshot(
                        event_batch_metadata, log_id, queue_item.metablock_absolute_pos, CachePath::Read,
                    );
                    shard_mem_cache.commit_position_snapshot(
                        event_batch_metadata, log_id, queue_item.metablock_absolute_pos, CachePath::Write,
                    );

                    let size_bytes = queue_item.size_bytes();
                    shard_mem_cache.cache_recent_write(
                        event_batch_metadata.aggregate_key.clone(),
                        event_batch_metadata.aggregate_version,
                        queue_item.metablock,
                        queue_item.datablock,
                        size_bytes,
                    );
                } else {
                    pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                }
            }
            MetablockKind::SoftTrim(soft_trim) => {
                shard_mem_cache.update_aggregate_min_aggregate_version(
                    &soft_trim.aggregate_key,
                    soft_trim.keep_from_aggregate_version,
                    CachePath::Write,
                );

                if !node_status.is_leader() {
                    shard_mem_cache.update_aggregate_min_aggregate_version(
                        &soft_trim.aggregate_key,
                        soft_trim.keep_from_aggregate_version,
                        CachePath::Read,
                    );
                    event_collector.add_trim_event(soft_trim.aggregate_key.clone(), soft_trim.keep_from_aggregate_version);
                } else {
                    pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                }
            }
            MetablockKind::SoftDelete(soft_delete) => {
                // Use position from aggregate_queue_positions if available (leader write path),
                // otherwise fall back to the queue item's position set during sync() (follower replication path)
                let (del_log_id, del_pos) = deleted_positions
                    .get(&soft_delete.aggregate_key)
                    .copied()
                    .unwrap_or((log_id, queue_item.metablock_absolute_pos));
                shard_mem_cache.put_aggregate_into_cache_as_deleted(
                    soft_delete.aggregate_key.clone(),
                    del_log_id, del_pos,
                    soft_delete.event_seq,
                    soft_delete.aggregate_version,
                    soft_delete.allow_recreate,
                    soft_delete.allow_sequence_continuation,
                    CachePath::Write,
                );

                if !node_status.is_leader() {
                    shard_mem_cache.put_aggregate_into_cache_as_deleted(
                        soft_delete.aggregate_key.clone(),
                        del_log_id, del_pos,
                        soft_delete.event_seq,
                        soft_delete.aggregate_version,
                        soft_delete.allow_recreate,
                        soft_delete.allow_sequence_continuation,
                        CachePath::Read,
                    );
                    event_collector.add_delete_event(soft_delete.aggregate_key.clone());
                } else {
                    pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                }
            }
            MetablockKind::SchemaRegistration(schema_reg) => {
                if !node_status.is_leader() {
                    // Follower: compile and cache schema now that it's durable
                    if let Some(ref datablock) = queue_item.datablock {
                        crate::shard_wal::compile_and_cache_schema(&mut shard_mem_cache, &schema_reg.schema_key, datablock);
                    } else if let Ok(datablock) = celeriant_wire::disk::serialised_datablock::deserialise_datablock(
                        queue_item.metablock.uncompressed_size,
                        queue_item.metablock.compressed_size,
                        queue_item.metablock.datablock_version,
                        queue_item.metablock.datablock_compression_type,
                        &queue_item.metablock.datablock,
                        None,
                        dict_codec,
                    ) {
                        crate::shard_wal::compile_and_cache_schema(&mut shard_mem_cache, &schema_reg.schema_key, &datablock);
                    }
                } else {
                    pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                }
            }
        }
    }

    if !node_status.is_leader() {
        event_collector.broadcast_all(&watched_aggregates);
    } else {
        // As leader, after fsync we can now allow replication to proceed
        shard_mem_cache.push_pending_replication(pending_commit_data);
    }
}

fn rollback_sync(shard_mem_cache: Rc<RefCell<MemCache>>) {
    shard_mem_cache.borrow_mut().execute_fsync_rollback();
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
    node_status: NodeStatus,
) -> Result<LogSegmentFileMetadata, ShardFsyncError> {
    let mut log_segment_file_metadata = log_segment_file.metadata.borrow().clone();

    let dma_file_writer = log_segment_file.lock_writer("sync").await
        .map_err(|_| ShardFsyncError::WriteLockTimeout)?;
    let dma_file_writer = dma_file_writer
        .as_ref()
        .ok_or_else(|| ShardFsyncError::ActiveWriteFileUnavailable)?;

    // Write datablocks first so we can get the positions to include into metablocks
    let buffer_size_datablocks: u64 = sync_positions_snapshot.buffer_size_datablocks();
    let alignment = (dma_file_writer.alignment() as u64).max(MIN_WRITE_ALIGNMENT);

    let mut datablocks_absolute_write_positions: Vec<u64> = Vec::with_capacity(sync_positions_snapshot.pending_append_queue.len());
    let mut new_datablocks_position = log_segment_file_metadata.write.datablocks_position;
    let mut datablocks_carry_over: Option<Vec<u8>> = log_segment_file_metadata.datablocks_carry_over.take();

    if buffer_size_datablocks > 0 {
        let write_to_pos = constants::align_up(log_segment_file_metadata.write.datablocks_position, alignment);
        new_datablocks_position = log_segment_file_metadata.write.datablocks_position.saturating_sub(buffer_size_datablocks);
        let write_from_pos = constants::align_down(new_datablocks_position, alignment);
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

        let datablocks_carry_over_size = constants::align_up(new_datablocks_position, alignment).saturating_sub(new_datablocks_position);
        if datablocks_carry_over_size > 0 {
            datablocks_carry_over =
                Some(buffer_datablocks_slice[front_carry_over..(front_carry_over + datablocks_carry_over_size as usize)].to_vec());
        }

        dma_file_writer
            .write_at(buffer_datablocks, new_datablocks_position.saturating_sub(front_carry_over as u64))
            .await
            .map_err(|e| ShardFsyncError::WriteDatablocksError(e.to_string()))?;
    }

    let content_size_metablocks: u64 = sync_positions_snapshot.buffer_size_metablocks();
    let padded_size_metablocks = constants::align_up(content_size_metablocks, alignment) as usize;
    let mut buffer_metablocks = dma_file_writer.alloc_dma_buffer(padded_size_metablocks);
    let buffer_metablocks_slice = buffer_metablocks.as_bytes_mut();
    let mut position = 0usize;
    let mut index = 0;
    for item in &mut sync_positions_snapshot.pending_append_queue {
        if item.datablock_bytes.is_some() && item.datablock.is_some() {
            item.metablock.datablock_position = datablocks_absolute_write_positions[index];
            index += 1;
        }

        log_segment_file_metadata.write.wal_seq = log_segment_file_metadata.write.wal_seq.saturating_add(1);
        item.metablock.wal_seq = log_segment_file_metadata.write.wal_seq;

        // Track the absolute position where this metablock is written
        let metablock_absolute_pos = log_segment_file_metadata.write.metablocks_position + position as u64;
        item.metablock_absolute_pos = metablock_absolute_pos;

        // Update aggregate positions tracking for event batches (only if entry exists)
        if let MetablockKind::EventBatchMetadata(event_batch) = &item.metablock.wal_metablock_type {
            if let Some(aggregate_positions) = sync_positions_snapshot.aggregate_queue_positions.get_mut(&event_batch.aggregate_key) {
                aggregate_positions.log_id = log_segment_file_metadata.log_id;
                aggregate_positions.metablock_absolute_pos = metablock_absolute_pos;
                aggregate_positions.wal_seq = item.metablock.wal_seq;
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

    //Write metablocks — position advances by content size, not padded size
    let new_metablocks_position = log_segment_file_metadata.write.metablocks_position + content_size_metablocks;
    dma_file_writer
        .write_at(buffer_metablocks, log_segment_file_metadata.write.metablocks_position)
        .await
        .map_err(|e| ShardFsyncError::WriteMetablocksError(e.to_string()))?;

    // Update bloom filter with aggregate keys and schema keys from this batch
    for item in &sync_positions_snapshot.pending_append_queue {
        match &item.metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(event_batch) => {
                log_segment_file_metadata.write.aggregate_key_bloom.insert(&event_batch.aggregate_key);
            }
            MetablockKind::SchemaRegistration(schema_reg) => {
                log_segment_file_metadata.write.aggregate_key_bloom.insert_hash(schema_reg.schema_key.bloom_hash());
            }
            MetablockKind::SoftDelete(soft_delete) => {
                log_segment_file_metadata.write.aggregate_key_bloom.insert(&soft_delete.aggregate_key);
            }
            MetablockKind::SoftTrim(soft_trim) => {
                log_segment_file_metadata.write.aggregate_key_bloom.insert(&soft_trim.aggregate_key);
            }
        }
    }

    // Update positions and carry over
    log_segment_file_metadata.write.metablocks_position = new_metablocks_position;
    log_segment_file_metadata.datablocks_carry_over = datablocks_carry_over;
    log_segment_file_metadata.write.datablocks_position = new_datablocks_position;

    // Non-leader: pre-advance read so the persisted header matches the final state
    // rather than lagging by one fsync.
    if !node_status.is_leader() {
        log_segment_file_metadata.advance_visible_position();
    }
    let header_end_start_pos = log_segment_file_metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
    let header = log_segment_file_metadata.to_shard_log_header();
    write_dual_shard_log_header(&dma_file_writer, header_end_start_pos, &header).await
        .map_err(ShardFsyncError::LogSegmentFileHeaderWriteFailure)?;

    dma_file_writer.fdatasync().await
        .map_err(|e| ShardFsyncError::FDataSyncError(e.to_string()))?;

    log_segment_file.note_header_synced(log_segment_file_metadata.last_self_acked_wal_seq);

    Ok(log_segment_file_metadata)
}

/// Dual-header write + fdatasync, no metablocks/datablocks. Used by replication commit
/// to make `last_self_acked_wal_seq` durable before client Ok.
///
/// Caller MUST serialize via the fsync coordinator (request_sync / acquire_rollback_lock);
/// the DMA lock here only covers file-level write ordering.
pub(crate) async fn sync_header_only(
    log_segment_file: Rc<LogSegmentFile>,
) -> Result<(), ShardFsyncError> {
    let dma_file_writer = log_segment_file.lock_writer("sync_header_only").await
        .map_err(|_| ShardFsyncError::WriteLockTimeout)?;
    let dma_file_writer = dma_file_writer
        .as_ref()
        .ok_or_else(|| ShardFsyncError::ActiveWriteFileUnavailable)?;

    let metadata = log_segment_file.metadata.borrow().clone();
    let header_end_start_pos = metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
    let header = metadata.to_shard_log_header();
    write_dual_shard_log_header(dma_file_writer, header_end_start_pos, &header).await
        .map_err(ShardFsyncError::LogSegmentFileHeaderWriteFailure)?;

    dma_file_writer.fdatasync().await
        .map_err(|e| ShardFsyncError::FDataSyncError(e.to_string()))?;

    log_segment_file.note_header_synced(metadata.last_self_acked_wal_seq);

    Ok(())
}

/// Hash chain: blake3(previous_hash || metablock_bytes), skipping the CRC (which covers
/// datablock_position) and the datablock_position field itself — both are node-local.
pub(crate) fn compute_entry_hash(previous_hash: &EntryHashBytes, content: &[u8]) -> EntryHashBytes {
    const CRC_END: usize = versioned_block::CRC_SIZE;
    const SKIP_START: usize = versioned_block::HEADER_SIZE + Metablock::OFFSET_DATABLOCK_POSITION;
    const SKIP_END: usize = SKIP_START + Metablock::WIRE_SIZE_DATABLOCK_POSITION;

    let mut hasher = blake3::Hasher::new();
    hasher.update(previous_hash);
    hasher.update(&content[CRC_END..SKIP_START]);
    hasher.update(&content[SKIP_END..]);
    *hasher.finalize().as_bytes()
}

pub(crate) fn summary_path(shard_dir: &Path, log_id: u64) -> PathBuf {
    shard_dir.join(format!("log_{log_id}.summary"))
}

pub(crate) async fn write_segment_summary_sidecar_from_payload(
    shard_dir: &Path,
    log_id: u64,
    payload: SegmentSummaryPayload,
) -> Result<(), ShardFsyncError> {
    if payload.is_empty() {
        return Ok(());
    }

    let block = SegmentSummaryBlock { payload };
    let serialized = serialize_versioned_message_heap(&block, WIRE_VERSION_SEGMENT_SUMMARY_BLOCK)
        .map_err(|e| ShardFsyncError::SegmentSummarySidecarWriteError(e.to_string()))?;

    let path = summary_path(shard_dir, log_id);
    let file = glommio::io::BufferedFile::create(&path)
        .await
        .map_err(|e| ShardFsyncError::SegmentSummarySidecarWriteError(e.to_string()))?;
    file.write_at(serialized, 0)
        .await
        .map_err(|e| ShardFsyncError::SegmentSummarySidecarWriteError(e.to_string()))?;
    file.fdatasync()
        .await
        .map_err(|e| ShardFsyncError::SegmentSummarySidecarWriteError(e.to_string()))?;

    Ok(())
}

async fn write_segment_summary_sidecar(
    shard_dir: &Path,
    log_id: u64,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
) -> Result<(), ShardFsyncError> {
    let payload = shard_mem_cache.borrow_mut().take_segment_summary();
    write_segment_summary_sidecar_from_payload(shard_dir, log_id, payload).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use glommio::{LocalExecutorBuilder, Placement};

    use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::constants::{GENESIS_HASH, MINIBATCH_SIZE_BYTES};
    use celeriant_wal::metablocks::datablock_inline_data::DatablockInlineData;
    use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
    use celeriant_wal::metablocks::metablock_event_batch::{EventTypesKind, MetablockEventBatch};
    use celeriant_wal::metablocks::metablock_soft_delete::MetablockSoftDelete;
    use celeriant_wal::metablocks::metablock_soft_trim::MetablockSoftTrim;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    fn test_codec() -> DictCodec {
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile")
    }

    fn test_dir() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shard");
        (tmp, dir)
    }

    fn event_batch_metablock(aggregate_key: AggregateKey, aggregate_version: u64, max_event_seq: u64) -> Metablock {
        Metablock {
            wal_seq: 0,
            server_timestamp: 1000,
            lease_epoch: 1,
            node_id: 1,
            uncompressed_size: 128,
            compressed_size: 64,
            datablock_version: 1,
            datablock_compression_type: 0,
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
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
                client_id: 1,
                user_id: None,
                event_types_data: EventTypesKind::Direct([1, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::Inline(DatablockInlineData {
                minibatch: [0u8; MINIBATCH_SIZE_BYTES],
            }),
        }
    }

    fn soft_delete_metablock(aggregate_key: AggregateKey, aggregate_version: u64, event_seq: u64) -> Metablock {
        Metablock {
            wal_seq: 0,
            server_timestamp: 1000,
            lease_epoch: 1,
            node_id: 1,
            uncompressed_size: 0,
            compressed_size: 0,
            datablock_version: 0,
            datablock_compression_type: 0,
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
            wal_metablock_type: MetablockKind::SoftDelete(MetablockSoftDelete {
                aggregate_key,
                allow_recreate: false,
                allow_sequence_continuation: false,
                aggregate_version,
                event_seq,
                client_id: 1,
                user_id: None,
            }),
            datablock: DatablockStorageKind::Inline(DatablockInlineData {
                minibatch: [0u8; MINIBATCH_SIZE_BYTES],
            }),
        }
    }

    fn soft_trim_metablock(aggregate_key: AggregateKey, keep_from: u64) -> Metablock {
        Metablock {
            wal_seq: 0,
            server_timestamp: 1000,
            lease_epoch: 1,
            node_id: 1,
            uncompressed_size: 0,
            compressed_size: 0,
            datablock_version: 0,
            datablock_compression_type: 0,
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
            wal_metablock_type: MetablockKind::SoftTrim(MetablockSoftTrim {
                aggregate_key,
                keep_from_aggregate_version: keep_from,
                aggregate_version: 0,
                event_seq: 0,
                client_id: 1,
                user_id: None,
            }),
            datablock: DatablockStorageKind::Inline(DatablockInlineData {
                minibatch: [0u8; MINIBATCH_SIZE_BYTES],
            }),
        }
    }

    fn queue_item(metablock: Metablock) -> ShardLogQueueItem {
        ShardLogQueueItem::new(None, None, metablock)
    }

    /// Simulates the non-leader commit path: add_to_pending_queue → sync → commit_sync.
    fn non_leader_commit_sync(
        node_status: NodeStatus,
        shard_mem_cache: &Rc<RefCell<MemCache>>,
        watched_aggregates: &Rc<AggregateWatchers>,
        log_segment_file: &Rc<LogSegmentFile>,
    ) {
        let sync_positions_snapshot = shard_mem_cache.borrow_mut().take_sync_positions_snapshot();
        let new_metadata = log_segment_file.metadata.borrow().clone();
        commit_sync(
            node_status,
            shard_mem_cache.clone(),
            watched_aggregates.clone(),
            sync_positions_snapshot,
            log_segment_file.clone(),
            new_metadata,
            &test_codec(),
        );
    }

    fn non_leader_statuses() -> [NodeStatus; 2] {
        [NodeStatus::Follower { leader_lease_epoch: 1 }, NodeStatus::Standalone]
    }

    #[test]
    fn non_leader_event_batch_via_pending_queue_populates_read_snapshot() {
        glommio_test!({
            for node_status in non_leader_statuses() {
                let (_tmp, dir) = test_dir();
                let lsc = Rc::new(
                    LogSegmentsCache::ready_up(dir, 4 * 1024 * 1024, 4, 0).await.unwrap()
                );
                let smc = Rc::new(RefCell::new(
                    MemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024)
                ));
                let watched = Rc::new(AggregateWatchers::new());
                let log_segment = lsc.active();

                let k = AggregateKey::new(1, 1, 1);

                smc.borrow_mut().add_to_pending_queue(vec![
                    queue_item(event_batch_metablock(k.clone(), 1, 5)),
                    queue_item(event_batch_metablock(k.clone(), 2, 10)),
                ]);

                non_leader_commit_sync(node_status, &smc, &watched, &log_segment);

                let (loaded, status) = smc.borrow_mut().aggregate_load_status(&k, CachePath::Read);
                assert!(loaded, "{:?} read snapshot should be populated after commit_sync", node_status);
                assert_eq!(status, celeriant_memcache::mem_snapshot_aggregate::AggregateStatus::Found);

                let pos = smc.borrow_mut().get_aggregate_last_metablock_pos(&k, CachePath::Read);
                assert_eq!(pos.aggregate_version, 2, "should have latest aggregate_version");
                assert_eq!(pos.event_seq, 10, "should have latest event_seq");

                let (loaded, status) = smc.borrow_mut().aggregate_load_status(&k, CachePath::Write);
                assert!(loaded, "{:?} write snapshot should be populated after commit_sync", node_status);
                assert_eq!(status, celeriant_memcache::mem_snapshot_aggregate::AggregateStatus::Found);

                lsc.close().await;
            }
        });
    }

    #[test]
    fn non_leader_soft_delete_via_pending_queue_marks_deleted_with_position() {
        glommio_test!({
            for node_status in non_leader_statuses() {
                let (_tmp, dir) = test_dir();
                let lsc = Rc::new(
                    LogSegmentsCache::ready_up(dir, 4 * 1024 * 1024, 4, 0).await.unwrap()
                );
                let smc = Rc::new(RefCell::new(
                    MemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024)
                ));
                let watched = Rc::new(AggregateWatchers::new());
                let log_segment = lsc.active();

                let k = AggregateKey::new(1, 1, 1);

                smc.borrow_mut().add_to_pending_queue(vec![
                    queue_item(event_batch_metablock(k.clone(), 1, 5)),
                ]);
                non_leader_commit_sync(node_status, &smc, &watched, &log_segment);

                let mut delete_item = queue_item(soft_delete_metablock(k.clone(), 1, 5));
                delete_item.metablock_absolute_pos = 4096;
                smc.borrow_mut().add_to_pending_queue(vec![delete_item]);
                non_leader_commit_sync(node_status, &smc, &watched, &log_segment);

                for path in [CachePath::Read, CachePath::Write] {
                    let (loaded, status) = smc.borrow_mut().aggregate_load_status(&k, path);
                    assert!(loaded, "{:?} should be loaded on {:?}", node_status, path);
                    assert_eq!(
                        status,
                        celeriant_memcache::mem_snapshot_aggregate::AggregateStatus::Deleted,
                        "{:?} should be Deleted on {:?}", node_status, path
                    );
                }

                let snap = smc.borrow_mut().get_aggregate_snapshot(&k, CachePath::Read).unwrap();
                assert_ne!(snap.metablock_absolute_pos, 0,
                    "{:?} deleted aggregate should have real disk position", node_status);

                lsc.close().await;
            }
        });
    }

    #[test]
    fn non_leader_soft_trim_via_pending_queue_updates_trimmed_below_version() {
        glommio_test!({
            for node_status in non_leader_statuses() {
                let (_tmp, dir) = test_dir();
                let lsc = Rc::new(
                    LogSegmentsCache::ready_up(dir, 4 * 1024 * 1024, 4, 0).await.unwrap()
                );
                let smc = Rc::new(RefCell::new(
                    MemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024)
                ));
                let watched = Rc::new(AggregateWatchers::new());
                let log_segment = lsc.active();

                let k = AggregateKey::new(1, 1, 1);

                smc.borrow_mut().add_to_pending_queue(vec![
                    queue_item(event_batch_metablock(k.clone(), 1, 5)),
                    queue_item(event_batch_metablock(k.clone(), 2, 10)),
                    queue_item(event_batch_metablock(k.clone(), 3, 15)),
                ]);
                non_leader_commit_sync(node_status, &smc, &watched, &log_segment);

                smc.borrow_mut().add_to_pending_queue(vec![
                    queue_item(soft_trim_metablock(k.clone(), 2)),
                ]);
                non_leader_commit_sync(node_status, &smc, &watched, &log_segment);

                for path in [CachePath::Read, CachePath::Write] {
                    let pos = smc.borrow_mut().get_aggregate_last_metablock_pos(&k, path);
                    assert_eq!(pos.min_aggregate_version, 2,
                        "{:?} min_aggregate_version should be 2 on {:?}", node_status, path);
                }

                lsc.close().await;
            }
        });
    }

    fn leader_commit_sync(
        shard_mem_cache: &Rc<RefCell<MemCache>>,
        watched_aggregates: &Rc<AggregateWatchers>,
        log_segment_file: &Rc<LogSegmentFile>,
    ) {
        let sync_positions_snapshot = shard_mem_cache.borrow_mut().take_sync_positions_snapshot();
        let new_metadata = log_segment_file.metadata.borrow().clone();
        commit_sync(
            NodeStatus::Leader { lease_epoch: 1 },
            shard_mem_cache.clone(),
            watched_aggregates.clone(),
            sync_positions_snapshot,
            log_segment_file.clone(),
            new_metadata,
            &test_codec(),
        );
    }

    #[test]
    fn leader_event_batch_does_not_advance_read_snapshot() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let lsc = Rc::new(
                LogSegmentsCache::ready_up(dir, 4 * 1024 * 1024, 4, 0).await.unwrap()
            );
            let smc = Rc::new(RefCell::new(
                MemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024)
            ));
            let watched = Rc::new(AggregateWatchers::new());
            let log_segment = lsc.active();

            let k = AggregateKey::new(1, 1, 1);

            // Leader writes via the normal write path (not add_to_pending_queue)
            smc.borrow_mut().add_to_pending_queue(vec![
                queue_item(event_batch_metablock(k.clone(), 1, 5)),
            ]);
            leader_commit_sync(&smc, &watched, &log_segment);

            // Read snapshot must NOT be populated — leader defers to replication
            let (loaded, _) = smc.borrow_mut().aggregate_load_status(&k, CachePath::Read);
            assert!(!loaded, "leader must not advance read snapshot before replication");

            // Data should be in pending replication queue instead
            let pending = smc.borrow_mut().take_pending_replication();
            assert_eq!(pending.len(), 1, "leader should have queued data for replication");
            assert!(!pending[0].pending_queue.is_empty());

            lsc.close().await;
        });
    }

    #[test]
    fn empty_segment_no_summary_written() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            std::fs::create_dir_all(&dir).unwrap();

            let smc = Rc::new(RefCell::new(
                MemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024)
            ));

            // No writes → empty segment summary → no file should be created
            write_segment_summary_sidecar(&dir, 1, &smc).await.unwrap();

            let path = summary_path(&dir, 1);
            assert!(!path.exists(), "empty segment should not produce a .summary file");
        });
    }
}