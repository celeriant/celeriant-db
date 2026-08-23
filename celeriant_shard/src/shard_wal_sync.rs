use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use celeriant_wal::segment_summary::segment_summary_payload::SegmentSummaryPayload;
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

use celeriant_wal::aggregate_client_key::{AggregateClientKey, client_id_bloom_hash};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_watch::aggregate_watchers::AggregateWatchers;
use celeriant_wire::disk::versioned_block;
use celeriant_wire::disk::versioned_block::{serialize_versioned_message, serialize_versioned_message_heap};

use crate::amortisation::coordinator::CaptureResult;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::watch_event_collector::WatchEventCollector;

/// Who commits the read-side (visibility) half of a durable batch, and when.
/// Provenance decides this, not `is_leader()`: the four write sources in the
/// system map onto three commit rules.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum CommitTarget {
    /// Leader write: read untouched at fsync; commit_pcd advances it on replication ACK.
    DeferToReplicationAck,
    /// Follower live-TCP apply: park the read-side commit; drained when a carrier's
    /// `leader_confirmed_wal_seq` covers the batch tip.
    DeferToLeaderConfirmed,
    /// S3 catchup + standalone: read = write at fsync.
    FullCommit,
}

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
    commit_target: CommitTarget,
    log_segments_cache: Rc<LogSegmentsCache>,
    shard_mem_cache: Rc<RefCell<MemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    mut captured: FsyncCapturedData, // Mutable because we set the datablocks_position while writing in metablocks
    shard_id: u32,
    dict_codec: Rc<DictCodec>,
    join_data_meta_writes: bool,
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

        if commit_target == CommitTarget::FullCommit {
            // Full commit: summary is complete, write sidecar now (the active
            // segment is still the sealing one — its cursor holds the blooms).
            write_segment_summary_sidecar(
                &log_segments_cache,
                log_segments_cache.active_log_id(),
                &shard_mem_cache,
            ).await?;
        } else {
            // Deferred commit: the summary is incomplete until the deferred
            // read-side commit drains. Snapshot the accumulator for the sealed
            // segment; the post-commit sweep builds the right-sized segment
            // blooms from its exact key knowledge and writes the sidecar.
            let old_log_id = log_segments_cache.active_log_id();
            shard_mem_cache.borrow_mut().store_sealed_segment_summary(old_log_id);
        }

        log_segments_cache
            .rotate_to_next_log()
            .await
            .map_err(ShardFsyncError::UnableToRotateToNewLogSegmentFile)?;
        // Rotation visible: the drained accumulator now describes the new
        // active segment, so the schema-absence consult may trust it again.
        // (Both seal branches above drained it, which latched the consult to
        // maybe-present for the parked-await window.)
        shard_mem_cache.borrow_mut().note_active_segment_rotated();
    }

    let active_log_segment = log_segments_cache.active();

    match sync(active_log_segment.clone(), &mut captured.sync_positions_snapshot, commit_target, join_data_meta_writes).await {
        Ok(updated_log_segment_file_metadata) => {
            let wal_seq = updated_log_segment_file_metadata.write.wal_seq;
            commit_sync(
                node_status,
                commit_target,
                shard_mem_cache.clone(),
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
            // Shard-level committed cursor: the active segment's read is None
            // right after rotation while the cursor sits in the predecessor.
            let read_wal_seq = log_segments_cache.committed_read_wal_seq();
            metrics::gauge!("celeriant_read_wal_seq", &shard_label).set(read_wal_seq as f64);
            if commit_target == CommitTarget::DeferToLeaderConfirmed {
                metrics::gauge!("celeriant_follower_read_lag", &shard_label).set(wal_seq.saturating_sub(read_wal_seq) as f64);
                metrics::gauge!("celeriant_parked_commit_queue_depth", &shard_label)
                    .set(shard_mem_cache.borrow().parked_commit_count() as f64);
            }
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
    commit_target: CommitTarget,
    shard_mem_cache: Rc<RefCell<MemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    mut sync_positions_snapshot: SyncPositionsSnapshot,
    log_segment_file: Rc<LogSegmentFile>,
    mut new_metadata: LogSegmentFileMetadata,
    dict_codec: &DictCodec,
) {
    // Full commit: advance read = write here. Deferred targets: read stays
    // behind until commit_pcd (replication ACK or leader-confirmed drain).
    if commit_target == CommitTarget::FullCommit {
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
        // Commit this metablock as its aggregate's latest in-segment position so the
        // next append back-links to it. Matches what sync() wrote into the bytes.
        if let Some(key) = chain_aggregate_key(&queue_item.metablock) {
            log_segment_file.aggregate_chain_tips.borrow_mut().insert(key.clone(), queue_item.metablock_absolute_pos);
        }

        if commit_target == CommitTarget::FullCommit {
            shard_mem_cache.update_segment_summary(&queue_item.metablock, queue_item.metablock_absolute_pos);
        }

        match &queue_item.metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(event_batch_metadata) => {
                // For follower to keep track of latest client_seq. Leader tracks it pre-fsync
                if commit_target != CommitTarget::DeferToReplicationAck && event_batch_metadata.max_client_seq > 0 {
                    shard_mem_cache.merge_aggregate_client_seq_max(
                        AggregateClientKey::new(event_batch_metadata.aggregate_key.clone(), event_batch_metadata.client_id),
                        event_batch_metadata.max_client_seq,
                        queue_item.metablock.wal_seq,
                    );
                    metrics::counter!("celeriant_client_seq_merge_on_apply_total").increment(1);
                }

                match commit_target {
                    CommitTarget::FullCommit => {
                        event_collector.add_write_event(event_batch_metadata);

                        if event_batch_metadata.aggregate_version == FIRST_AGGREGATE_VERSION {
                            event_collector.add_create_event(event_batch_metadata.aggregate_key.clone());
                        }

                        // Update read and write snapshots so the aggregate is visible.
                        // On the replication path, aggregate_queue_positions is empty
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
                    }
                    CommitTarget::DeferToLeaderConfirmed => {
                        // Write-side (OCC) state commits at fsync like the leader's,
                        // but the leader gets it via commit_sync_positions_snapshot —
                        // empty on the replication path — so update it explicitly.
                        // Read side stays parked until the carrier confirms.
                        shard_mem_cache.commit_position_snapshot(
                            event_batch_metadata, log_id, queue_item.metablock_absolute_pos, CachePath::Write,
                        );
                        pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                    }
                    CommitTarget::DeferToReplicationAck => {
                        pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                    }
                }
            }
            MetablockKind::SoftTrim(soft_trim) => {
                shard_mem_cache.commit_trim_snapshot(
                    &soft_trim.aggregate_key,
                    soft_trim.keep_from_aggregate_version,
                    soft_trim.aggregate_version,
                    soft_trim.event_seq,
                    log_id,
                    queue_item.metablock_absolute_pos,
                    CachePath::Write,
                );

                if commit_target == CommitTarget::FullCommit {
                    shard_mem_cache.commit_trim_snapshot(
                        &soft_trim.aggregate_key,
                        soft_trim.keep_from_aggregate_version,
                        soft_trim.aggregate_version,
                        soft_trim.event_seq,
                        log_id,
                        queue_item.metablock_absolute_pos,
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

                if commit_target == CommitTarget::FullCommit {
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
                if commit_target == CommitTarget::FullCommit {
                    // Compile and cache schema now that it's durable
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
                    // Both defer targets park it. The leader compiled at write time
                    // (commit_pcd ignores it); the follower drain compiles on commit.
                    pending_commit_data.pending_queue.push(PendingCacheItem::new(queue_item));
                }
            }
        }
    }

    match commit_target {
        CommitTarget::FullCommit => event_collector.broadcast_all(&watched_aggregates),
        // After fsync the leader can let replication proceed
        CommitTarget::DeferToReplicationAck => shard_mem_cache.push_pending_replication(pending_commit_data),
        CommitTarget::DeferToLeaderConfirmed => {
            if shard_mem_cache.push_parked_commit(pending_commit_data) {
                tracing::warn!(
                    log_id,
                    parked_bytes = shard_mem_cache.parked_commit_bytes(),
                    "parked commit queue exceeded the inflight cap — drain is lagging carriers"
                );
            }
        }
    }
}

fn rollback_sync(shard_mem_cache: Rc<RefCell<MemCache>>) {
    shard_mem_cache.borrow_mut().execute_fsync_rollback();
}

fn verify_full_write(got: usize, expected: usize, stage: &'static str, offset: u64) -> Result<(), ShardFsyncError> {
    if got == expected {
        Ok(())
    } else {
        Err(ShardFsyncError::ShortWrite { stage, offset, expected, got })
    }
}

fn check_batch_write_ranges(
    data_start: u64,
    data_end: u64,
    meta_start: u64,
    meta_end: u64,
) -> Result<(), ShardFsyncError> {
    if data_start < data_end && meta_start < meta_end && meta_end <= data_start {
        return Ok(());
    }
    Err(ShardFsyncError::OverlappingWriteRanges { data_start, data_end, meta_start, meta_end })
}

#[cfg(test)]
thread_local! {
    /// Nanoseconds spent in the last `sync()`'s three I/O phases: the datablock +
    /// metablock pair, the dual header, then fdatasync. Compiled out of every
    /// non-test build; the C6 join bench reads it to keep the flush term (unstable
    /// and bistable on consumer NVMe) out of the write-portion comparison.
    pub(crate) static LAST_SYNC_PHASE_NS: std::cell::Cell<(u64, u64, u64)> = const { std::cell::Cell::new((0, 0, 0)) };
}

/// Writes pending queue items to disk.
///
/// This function handles the low-level I/O:
/// 1. Builds the datablock buffer (growing downward from end of file), which fixes
///    each datablock's absolute position
/// 2. Builds the metablock buffer (growing upward from header), stamping those positions
///    and extending the hash chain
/// 3. Writes both — serially, or as one joined pair when `join_data_meta_writes` is set
/// 4. Updates bloom filter
/// 5. Writes dual headers — never before both writes above have completed
/// 6. Calls fdatasync for durability
///
/// Step 5 is sequenced after step 3 on purpose and must stay that way: on
/// durable-on-ack storage completion IS durability, boot CRCs only the header
/// block and the chain-tip rebuild scan is CRC-free, so a header that landed
/// over unlanded metablocks is unrecoverable AND undetectable. Only data and
/// meta may be joined.
///
/// sync_positions_snapshot is mutable because we need to set the datablocks absolute position as we write (only known at write time)
pub(crate) async fn sync(
    log_segment_file: Rc<LogSegmentFile>,
    sync_positions_snapshot: &mut SyncPositionsSnapshot,
    commit_target: CommitTarget,
    join_data_meta_writes: bool,
) -> Result<LogSegmentFileMetadata, ShardFsyncError> {
    let mut log_segment_file_metadata = log_segment_file.metadata.borrow().clone();

    let dma_file_writer = log_segment_file.lock_writer("sync").await
        .map_err(|_| ShardFsyncError::WriteLockTimeout)?;
    let dma_file_writer = dma_file_writer
        .as_ref()
        .ok_or_else(|| ShardFsyncError::ActiveWriteFileUnavailable)?;

    // Lay out datablocks first so we can get the positions to include into metablocks
    let buffer_size_datablocks: u64 = sync_positions_snapshot.buffer_size_datablocks();
    let alignment = (dma_file_writer.alignment() as u64).max(MIN_WRITE_ALIGNMENT);

    let content_size_metablocks: u64 = sync_positions_snapshot.buffer_size_metablocks();
    let padded_size_metablocks = constants::align_up(content_size_metablocks, alignment) as usize;
    let metablocks_write_start = log_segment_file_metadata.write.metablocks_position;
    let metablocks_write_end = metablocks_write_start.saturating_add(padded_size_metablocks as u64);

    let mut datablocks_absolute_write_positions: Vec<u64> = Vec::with_capacity(sync_positions_snapshot.pending_append_queue.len());
    let mut new_datablocks_position = log_segment_file_metadata.write.datablocks_position;
    let mut datablocks_carry_over: Option<Vec<u8>> = log_segment_file_metadata.datablocks_carry_over.take();

    let mut pending_datablocks_write: Option<(glommio::io::DmaBuffer, u64)> = None;

    if buffer_size_datablocks > 0 {
        let write_to_pos = constants::align_up(log_segment_file_metadata.write.datablocks_position, alignment);
        new_datablocks_position = log_segment_file_metadata.write.datablocks_position.saturating_sub(buffer_size_datablocks);
        let write_from_pos = constants::align_down(new_datablocks_position, alignment);
        let aligned_buffer_size_datablocks = write_to_pos.saturating_sub(write_from_pos);

        let ranges = check_batch_write_ranges(
            write_from_pos,
            write_to_pos,
            metablocks_write_start,
            metablocks_write_end,
        );
        debug_assert!(ranges.is_ok(), "{ranges:?}");
        ranges?;

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

        pending_datablocks_write = Some((
            buffer_datablocks,
            new_datablocks_position.saturating_sub(front_carry_over as u64),
        ));
    }

    let mut buffer_metablocks = dma_file_writer.alloc_dma_buffer(padded_size_metablocks);
    let buffer_metablocks_slice = buffer_metablocks.as_bytes_mut();
    // DMA buffers are recycled so zero out our allocated slice otherwise we write garbage
    buffer_metablocks_slice[content_size_metablocks as usize..].fill(0);
    let mut position = 0usize;
    let mut index = 0;
    // Within-batch view of each aggregate's latest metablock position, layered over
    // the segment's committed tips, so multiple metablocks for one aggregate in this
    // batch chain to each other. Applied to the live tips only on commit.
    let mut chain_overlay: HashMap<AggregateKey, u64> = HashMap::new();
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

        // Update aggregate positions tracking (only if entry exists). SoftDelete must
        // record its position too: the commit path's deleted_positions map reads it,
        // and a delete-only window otherwise leaves the or_insert default (0, 0) —
        // exists() then chases a metablock at log_0/pos_0 and errors.
        let positions_key = match &item.metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(event_batch) => Some(&event_batch.aggregate_key),
            MetablockKind::SoftDelete(soft_delete) => Some(&soft_delete.aggregate_key),
            _ => None,
        };
        if let Some(key) = positions_key {
            if let Some(aggregate_positions) = sync_positions_snapshot.aggregate_queue_positions.get_mut(key) {
                aggregate_positions.log_id = log_segment_file_metadata.log_id;
                aggregate_positions.metablock_absolute_pos = metablock_absolute_pos;
                aggregate_positions.wal_seq = item.metablock.wal_seq;
            }
        }

        // Per-aggregate backlink to this aggregate's previous metablock in this segment
        // (0 = none). Excluded from the hash chain, recomputed locally on every node.
        if let Some(key) = chain_aggregate_key(&item.metablock).cloned() {
            let prev = chain_overlay
                .get(&key)
                .copied()
                .or_else(|| log_segment_file.aggregate_chain_tips.borrow().get(&key).copied())
                .unwrap_or(0);
            item.metablock.previous_aggregate_metablock_pos = prev;
            chain_overlay.insert(key, metablock_absolute_pos);
        }

        // Keep the chain - store the previous hash in the next metablock. Done before serialisation!
        item.metablock.previous_tip_hash = log_segment_file_metadata.write.tip_hash;

        // Serialise straight into the DMA buffer. `serialize_versioned_message` writes the CRC,
        // the version and the payload and then zero-fills the rest of the slice it is given, so
        // it leaves no byte of a FIXED_BLOCK_SIZE_BYTES destination untouched
        let metablock_bytes = &mut buffer_metablocks_slice[position..position + FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&item.metablock, WIRE_VERSION_WAL_METABLOCK, metablock_bytes)
            .map_err(|e| ShardFsyncError::MetablockSerialisationError(e.to_string()))?;

        // Compute hash chain, excluding datablock_position (node-local offset that differs between nodes)
        log_segment_file_metadata.write.tip_hash = compute_entry_hash(&log_segment_file_metadata.write.tip_hash, metablock_bytes);

        position += FIXED_BLOCK_SIZE_BYTES;

        // Play nice and yield as we do a lot of cpu heavy work in this loop before writing anything
        glommio::yield_if_needed().await;
    }

    // Write the batch's data. Still two write calls, but we can parralelise by 
    // doing both metablock and datablock writes concurrently (submits both writes to io_uring queue together)
    let new_metablocks_position = log_segment_file_metadata.write.metablocks_position + content_size_metablocks;
    let metablocks_len = padded_size_metablocks;
    #[cfg(test)]
    let writes_started = std::time::Instant::now();
    match pending_datablocks_write {
        Some((buffer_datablocks, datablocks_write_position)) if join_data_meta_writes => {
            let datablocks_len = buffer_datablocks.len();
            let (datablocks, metablocks) = futures_lite::future::zip(
                dma_file_writer.write_at(buffer_datablocks, datablocks_write_position),
                dma_file_writer.write_at(buffer_metablocks, metablocks_write_start),
            )
            .await;
            let got = datablocks.map_err(|e| ShardFsyncError::WriteDatablocksError(e.to_string()))?;
            verify_full_write(got, datablocks_len, "datablocks", datablocks_write_position)?;
            let got = metablocks.map_err(|e| ShardFsyncError::WriteMetablocksError(e.to_string()))?;
            verify_full_write(got, metablocks_len, "metablocks", metablocks_write_start)?;
        }
        Some((buffer_datablocks, datablocks_write_position)) => {
            let datablocks_len = buffer_datablocks.len();
            let got = dma_file_writer
                .write_at(buffer_datablocks, datablocks_write_position)
                .await
                .map_err(|e| ShardFsyncError::WriteDatablocksError(e.to_string()))?;
            verify_full_write(got, datablocks_len, "datablocks", datablocks_write_position)?;
            let got = dma_file_writer
                .write_at(buffer_metablocks, metablocks_write_start)
                .await
                .map_err(|e| ShardFsyncError::WriteMetablocksError(e.to_string()))?;
            verify_full_write(got, metablocks_len, "metablocks", metablocks_write_start)?;
        }
        None => {
            let got = dma_file_writer
                .write_at(buffer_metablocks, metablocks_write_start)
                .await
                .map_err(|e| ShardFsyncError::WriteMetablocksError(e.to_string()))?;
            verify_full_write(got, metablocks_len, "metablocks", metablocks_write_start)?;
        }
    }
    #[cfg(test)]
    let writes_done = std::time::Instant::now();

    // Update bloom filter with aggregate keys and schema keys from this batch
    for item in &sync_positions_snapshot.pending_append_queue {
        match &item.metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(event_batch) => {
                log_segment_file_metadata.write.aggregate_key_bloom.borrow_mut().insert(&event_batch.aggregate_key);
                log_segment_file_metadata.write.client_id_bloom.borrow_mut().insert_hash(client_id_bloom_hash(event_batch.client_id));
            }
            // Schema keys live in the per-segment schema set (fed at commit and
            // by the open-time rebuild), never in the aggregate bloom: aggregate
            // blooms answer aggregate questions only, and mixing the two hash
            // domains is what made schema-absence checks unable to skip segments.
            MetablockKind::SchemaRegistration(_) => {}
            // Delete/trim carry a client_id too: every aggregate-scoped client-bearing
            // kind lands in the client bloom, or a tombstone-only client makes it a
            // subset (false "absent"). SchemaRegistration touches no aggregate: exempt.
            MetablockKind::SoftDelete(soft_delete) => {
                log_segment_file_metadata.write.aggregate_key_bloom.borrow_mut().insert(&soft_delete.aggregate_key);
                log_segment_file_metadata.write.client_id_bloom.borrow_mut().insert_hash(client_id_bloom_hash(soft_delete.client_id));
            }
            MetablockKind::SoftTrim(soft_trim) => {
                log_segment_file_metadata.write.aggregate_key_bloom.borrow_mut().insert(&soft_trim.aggregate_key);
                log_segment_file_metadata.write.client_id_bloom.borrow_mut().insert_hash(client_id_bloom_hash(soft_trim.client_id));
            }
        }
    }

    // Update positions and carry over
    log_segment_file_metadata.write.metablocks_position = new_metablocks_position;
    log_segment_file_metadata.datablocks_carry_over = datablocks_carry_over;
    log_segment_file_metadata.write.datablocks_position = new_datablocks_position;

    // Full commit: pre-advance read so the persisted header matches the final state
    // rather than lagging by one fsync. Deferred targets persist the current read
    // as-is (a follower's drain may already have advanced it before this fsync).
    if commit_target == CommitTarget::FullCommit {
        log_segment_file_metadata.advance_visible_position();
    }
    let header_end_start_pos = log_segment_file_metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
    let header = log_segment_file_metadata.to_shard_log_header();
    #[cfg(test)]
    let header_started = std::time::Instant::now();
    write_dual_shard_log_header(&dma_file_writer, header_end_start_pos, &header).await
        .map_err(ShardFsyncError::LogSegmentFileHeaderWriteFailure)?;

    #[cfg(test)]
    let flush_started = std::time::Instant::now();
    dma_file_writer.fdatasync().await
        .map_err(|e| ShardFsyncError::FDataSyncError(e.to_string()))?;

    #[cfg(test)]
    LAST_SYNC_PHASE_NS.with(|c| {
        c.set((
            (writes_done - writes_started).as_nanos() as u64,
            (flush_started - header_started).as_nanos() as u64,
            flush_started.elapsed().as_nanos() as u64,
        ))
    });

    log_segment_file.note_header_synced(
        log_segment_file_metadata.last_self_acked_wal_seq,
        log_segment_file_metadata.read.as_ref().map_or(0, |r| r.wal_seq),
    );

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

    log_segment_file.note_header_synced(
        metadata.last_self_acked_wal_seq,
        metadata.read.as_ref().map_or(0, |r| r.wal_seq),
    );

    Ok(())
}

/// Aggregate key of any metablock in the per-aggregate backlink chain (event
/// batch, soft delete, soft trim). None for schema registrations.
fn chain_aggregate_key(metablock: &Metablock) -> Option<&AggregateKey> {
    match &metablock.wal_metablock_type {
        MetablockKind::EventBatchMetadata(eb) => Some(&eb.aggregate_key),
        MetablockKind::SoftDelete(sd) => Some(&sd.aggregate_key),
        MetablockKind::SoftTrim(st) => Some(&st.aggregate_key),
        _ => None,
    }
}

/// Hash chain: blake3(previous_hash || metablock_bytes), skipping the CRC and the
/// contiguous node-local fields datablock_position + previous_aggregate_metablock_pos.
pub(crate) fn compute_entry_hash(previous_hash: &EntryHashBytes, content: &[u8]) -> EntryHashBytes {
    const CRC_END: usize = versioned_block::CRC_SIZE;
    const SKIP_START: usize = versioned_block::HEADER_SIZE + Metablock::OFFSET_DATABLOCK_POSITION;
    const SKIP_END: usize = SKIP_START
        + Metablock::WIRE_SIZE_DATABLOCK_POSITION
        + Metablock::WIRE_SIZE_PREVIOUS_AGGREGATE_METABLOCK_POS;

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
    mut payload: SegmentSummaryPayload,
) -> Result<(), ShardFsyncError> {
    if payload.is_empty() {
        // No file for an empty segment — but a STALE file must not survive it
        // (a truncated-then-reused id re-sealing empty would otherwise leave
        // the old segment's complete sidecar answering for the new one).
        let path = summary_path(shard_dir, log_id);
        return match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ShardFsyncError::SegmentSummarySidecarWriteError(
                format!("failed to delete stale sidecar for empty segment: {e}"),
            )),
        };
    }

    // Cap AFTER the blooms are attached so the bound covers the real file size.
    let dropped = payload.trim_out_client_sets();
    if dropped > 0 {
        metrics::counter!("celeriant_segment_summary_client_sets_dropped_total").increment(dropped as u64);
    }

    let aggregate_count = payload.aggregates.len();
    let serialized = serialize_versioned_message_heap(&payload, WIRE_VERSION_SEGMENT_SUMMARY_BLOCK)
        .map_err(|e| ShardFsyncError::SegmentSummarySidecarWriteError(e.to_string()))?;

    // Seal-time observability (gauges, deliberately not histograms — the global
    // buckets are seconds-scaled): most recent sealed summary's size and width.
    metrics::gauge!("celeriant_segment_summary_last_bytes").set(serialized.len() as f64);
    metrics::gauge!("celeriant_segment_summary_last_aggregates").set(aggregate_count as f64);

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

/// FullCommit seal: the accumulator drain builds the right-sized segment blooms
/// from its exact key knowledge — the fixed-size live cursor blooms stay
/// in-memory only.
async fn write_segment_summary_sidecar(
    log_segments_cache: &Rc<LogSegmentsCache>,
    log_id: u64,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
) -> Result<(), ShardFsyncError> {
    let payload = shard_mem_cache.borrow_mut().take_segment_summary();
    let result = write_segment_summary_sidecar_from_payload(log_segments_cache.shard_dir(), log_id, payload).await;
    if result.is_err() {
        // Drained but never landed: whatever this still-active segment seals
        // with later is a subset. Retaint so it can't authorize skips.
        shard_mem_cache.borrow_mut().mark_segment_summary_incomplete();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use celeriant_wal::segment_summary::client_set::ClientSet;
use celeriant_wal::segment_summary::segment_aggregate_entry::SegmentAggregateEntry;
use celeriant_wal::segment_summary::segment_summary_payload::SUMMARY_PAYLOAD_MAX_BYTES;
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
            previous_aggregate_metablock_pos: 0,
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
            previous_aggregate_metablock_pos: 0,
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

    fn test_schema_key() -> celeriant_wal::schema_key::SchemaKey {
        celeriant_wal::schema_key::SchemaKey::new(1, 1, 7, 0)
    }

    fn schema_queue_item() -> ShardLogQueueItem {
        use celeriant_wal::datablocks::datablock::Datablock;
        use celeriant_wal::datablocks::datablock_kind::DatablockKind;
        use celeriant_wal::datablocks::datablock_schema_registration::DatablockSchemaRegistration;
        use celeriant_wal::metablocks::metablock_schema_registration::MetablockSchemaRegistration;
        use celeriant_wal::schema_type::SchemaType;

        let datablock = Datablock {
            datablock_kind: DatablockKind::SchemaRegistration(DatablockSchemaRegistration {
                schema_type: SchemaType::Json,
                schema: r#"{"type":"object"}"#.to_string(),
            }),
        };
        let metablock = Metablock {
            wal_seq: 0,
            server_timestamp: 1000,
            lease_epoch: 1,
            node_id: 1,
            uncompressed_size: 0,
            compressed_size: 0,
            datablock_version: 1,
            datablock_compression_type: 0,
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
            previous_aggregate_metablock_pos: 0,
            wal_metablock_type: MetablockKind::SchemaRegistration(MetablockSchemaRegistration {
                schema_key: test_schema_key(),
                client_id: 1,
                user_id: None,
            }),
            datablock: DatablockStorageKind::Inline(DatablockInlineData {
                minibatch: [0u8; MINIBATCH_SIZE_BYTES],
            }),
        };
        ShardLogQueueItem::new(Some(datablock), None, metablock)
    }

    fn watch_everything_request() -> celeriant_msg::request::requests::WatchRequest {
        celeriant_msg::request::requests::WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        }
    }

    /// Non-blocking watch poll after letting the executor settle: by then an
    /// event is either in the channel or was never broadcast.
    async fn poll_watch_event(
        subscriber: &Rc<RefCell<celeriant_watch::subscribed_client::SubscribedClient>>,
    ) -> Option<celeriant_watch::aggregate_watch_event::AggregateWatchEvent> {
        glommio::timer::sleep(std::time::Duration::from_millis(10)).await;
        futures_lite::future::poll_once(subscriber.borrow().receiver.recv()).await.flatten()
    }

    struct TestShard {
        _tmp: tempfile::TempDir,
        lsc: Rc<LogSegmentsCache>,
        smc: Rc<RefCell<MemCache>>,
        watched: Rc<AggregateWatchers>,
        log_segment: Rc<LogSegmentFile>,
    }

    async fn test_shard() -> TestShard {
        let (tmp, dir) = test_dir();
        let lsc = Rc::new(LogSegmentsCache::ready_up(dir, 4 * 1024 * 1024, 4, 0).await.unwrap());
        let smc = Rc::new(RefCell::new(
            MemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 4 * 1024 * 1024, 2 * 1024 * 1024, 64 * 1024 * 1024)
        ));
        let watched = Rc::new(AggregateWatchers::new());
        let log_segment = lsc.active();
        TestShard { _tmp: tmp, lsc, smc, watched, log_segment }
    }

    /// Drives the post-fsync commit path: take snapshot → commit_sync.
    fn run_commit_sync(t: &TestShard, node_status: NodeStatus, commit_target: CommitTarget) {
        let sync_positions_snapshot = t.smc.borrow_mut().take_sync_positions_snapshot();
        let new_metadata = t.log_segment.metadata.borrow().clone();
        commit_sync(
            node_status,
            commit_target,
            t.smc.clone(),
            t.watched.clone(),
            sync_positions_snapshot,
            t.log_segment.clone(),
            new_metadata,
            &test_codec(),
        );
    }

    fn full_commit_sync(t: &TestShard) {
        run_commit_sync(t, NodeStatus::Standalone, CommitTarget::FullCommit);
    }

    fn deferred_follower_commit_sync(t: &TestShard) {
        run_commit_sync(t, NodeStatus::Follower { leader_lease_epoch: 1 }, CommitTarget::DeferToLeaderConfirmed);
    }

    /// An empty re-seal (e.g. a truncated-then-reused id whose new life holds
    /// no aggregates) must remove the previous incarnation's sidecar, not
    /// leave it answering for the new segment.
    #[test]
    fn empty_payload_deletes_stale_sidecar() {
        glommio_test!({
            let t = test_shard().await;
            let k = AggregateKey::new(1, 1, 1);
            t.smc.borrow_mut().update_segment_summary(&event_batch_metablock(k, 1, 5), HEADER_BLOCK_SIZE_BYTES as u64);
            write_segment_summary_sidecar(&t.lsc, 1, &t.smc).await.unwrap();
            let path = summary_path(t.lsc.shard_dir(), 1);
            assert!(path.exists(), "scaffolding: first seal must write the sidecar");

            // Accumulator drained by the write above: the next call is an empty re-seal.
            write_segment_summary_sidecar(&t.lsc, 1, &t.smc).await.unwrap();
            assert!(!path.exists(), "an empty re-seal must delete the stale sidecar");
        });
    }

    /// A failed sidecar write after the accumulator drained leaves the segment
    /// active with a hole in its summary state. If a later commit re-seals it
    /// as complete, the sidecar is a subset and can authorize false skips.
    #[test]
    fn sidecar_write_failure_retaints_the_accumulator() {
        glommio_test!({
            let t = test_shard().await;
            let k = AggregateKey::new(1, 1, 1);
            t.smc.borrow_mut().update_segment_summary(&event_batch_metablock(k, 1, 5), HEADER_BLOCK_SIZE_BYTES as u64);

            // Poison the sidecar path: a directory named log_1.summary makes create fail.
            std::fs::create_dir_all(summary_path(t.lsc.shard_dir(), 1)).unwrap();

            let result = write_segment_summary_sidecar(&t.lsc, 1, &t.smc).await;
            assert!(result.is_err(), "scaffolding: the poisoned path must fail the write");

            assert!(
                !t.smc.borrow_mut().take_segment_summary().complete,
                "a drained-but-unwritten summary must retaint the accumulator so a \
                 later seal cannot claim completeness over the lost entries"
            );
        });
    }

    /// FullCommit (standalone / catchup) commits the read side at fsync: the
    /// aggregate is visible on both cache paths immediately.
    #[test]
    fn full_commit_event_batch_populates_read_snapshot_at_fsync() {
        glommio_test!({
            let t = test_shard().await;
            let k = AggregateKey::new(1, 1, 1);

            t.smc.borrow_mut().add_to_pending_queue(vec![
                queue_item(event_batch_metablock(k.clone(), 1, 5)),
                queue_item(event_batch_metablock(k.clone(), 2, 10)),
            ]);
            full_commit_sync(&t);

            for path in [CachePath::Read, CachePath::Write] {
                let (loaded, status) = t.smc.borrow_mut().aggregate_load_status(&k, path);
                assert!(loaded, "{:?} snapshot should be populated after full commit", path);
                assert_eq!(status, celeriant_memcache::mem_snapshot_aggregate::AggregateStatus::Found);
            }

            let pos = t.smc.borrow_mut().get_aggregate_last_metablock_pos(&k, CachePath::Read);
            assert_eq!(pos.aggregate_version, 2, "should have latest aggregate_version");
            assert_eq!(pos.event_seq, 10, "should have latest event_seq");

            t.lsc.close().await;
        });
    }

    /// FullCommit soft delete is visible on both paths at fsync, carrying the
    /// delete metablock's real disk position (not a manufactured 0,0).
    #[test]
    fn full_commit_soft_delete_marks_deleted_with_position_at_fsync() {
        glommio_test!({
            let t = test_shard().await;
            let k = AggregateKey::new(1, 1, 1);

            t.smc.borrow_mut().add_to_pending_queue(vec![
                queue_item(event_batch_metablock(k.clone(), 1, 5)),
            ]);
            full_commit_sync(&t);

            let mut delete_item = queue_item(soft_delete_metablock(k.clone(), 1, 5));
            delete_item.metablock_absolute_pos = 4096;
            t.smc.borrow_mut().add_to_pending_queue(vec![delete_item]);
            full_commit_sync(&t);

            for path in [CachePath::Read, CachePath::Write] {
                let (loaded, status) = t.smc.borrow_mut().aggregate_load_status(&k, path);
                assert!(loaded, "should be loaded on {:?}", path);
                assert_eq!(status, celeriant_memcache::mem_snapshot_aggregate::AggregateStatus::Deleted,
                    "should be Deleted on {:?}", path);
            }

            let snap = t.smc.borrow_mut().get_aggregate_snapshot(&k, CachePath::Read).unwrap();
            assert_ne!(snap.metablock_absolute_pos, 0, "deleted aggregate should have real disk position");

            t.lsc.close().await;
        });
    }

    /// FullCommit soft trim advances min_aggregate_version on both paths at fsync.
    #[test]
    fn full_commit_soft_trim_updates_trimmed_below_version_at_fsync() {
        glommio_test!({
            let t = test_shard().await;
            let k = AggregateKey::new(1, 1, 1);

            t.smc.borrow_mut().add_to_pending_queue(vec![
                queue_item(event_batch_metablock(k.clone(), 1, 5)),
                queue_item(event_batch_metablock(k.clone(), 2, 10)),
                queue_item(event_batch_metablock(k.clone(), 3, 15)),
            ]);
            full_commit_sync(&t);

            t.smc.borrow_mut().add_to_pending_queue(vec![
                queue_item(soft_trim_metablock(k.clone(), 2)),
            ]);
            full_commit_sync(&t);

            for path in [CachePath::Read, CachePath::Write] {
                let pos = t.smc.borrow_mut().get_aggregate_last_metablock_pos(&k, path);
                assert_eq!(pos.min_aggregate_version, 2, "min_aggregate_version should be 2 on {:?}", path);
            }

            t.lsc.close().await;
        });
    }

    /// Leader-style write path (add_pending_trim_to_queue populates
    /// aggregate_queue_positions) with COLD snapshot caches at commit —
    /// simulates the LRU evicting the snapshot between trim validation and
    /// fsync commit. The positions loop must not manufacture a (0,0) Found
    /// snapshot from the trim-only entry; the SoftTrim commit arm owns the
    /// insert, with the metablock's real disk position.
    #[test]
    fn trim_only_window_cache_miss_commits_real_snapshot() {
        glommio_test!({
            let t = test_shard().await;
            let k = AggregateKey::new(1, 1, 1);

            let mut trim_item = queue_item(soft_trim_metablock(k.clone(), 2));
            trim_item.metablock_absolute_pos = 4096;
            t.smc.borrow_mut().add_pending_trim_to_queue(&k, 2, 0, 0, trim_item);
            full_commit_sync(&t);

            for path in [CachePath::Read, CachePath::Write] {
                let snap = t.smc.borrow_mut().get_aggregate_snapshot(&k, path)
                    .unwrap_or_else(|| panic!("snapshot must exist on {:?}", path));
                assert_eq!(snap.metablock_absolute_pos, 4096,
                    "trim-only commit must carry the trim metablock's position on {:?}", path);
                assert_eq!(snap.min_aggregate_version, 2, "trim floor must land on {:?}", path);
            }

            t.lsc.close().await;
        });
    }

    #[test]
    fn leader_event_batch_does_not_advance_read_snapshot() {
        glommio_test!({
            let t = test_shard().await;
            let k = AggregateKey::new(1, 1, 1);

            t.smc.borrow_mut().add_to_pending_queue(vec![
                queue_item(event_batch_metablock(k.clone(), 1, 5)),
            ]);
            run_commit_sync(&t, NodeStatus::Leader { lease_epoch: 1 }, CommitTarget::DeferToReplicationAck);

            // Read snapshot must NOT be populated — leader defers to replication
            let (loaded, _) = t.smc.borrow_mut().aggregate_load_status(&k, CachePath::Read);
            assert!(!loaded, "leader must not advance read snapshot before replication");

            // Data should be in pending replication queue instead
            let pending = t.smc.borrow_mut().take_pending_replication();
            assert_eq!(pending.len(), 1, "leader should have queued data for replication");
            assert!(!pending[0].pending_queue.is_empty());

            t.lsc.close().await;
        });
    }

    /// The deferred follower's fsync commits ONLY write-side (OCC) state; the
    /// read side — snapshots, recent-write cache, watch events, schema compile,
    /// read cursor — parks until a carrier confirms. One parked batch holds all
    /// metablock kinds.
    #[test]
    fn deferred_follower_fsync_parks_read_side_and_commits_write_side() {
        glommio_test!({
            let t = test_shard().await;
            let k_event = AggregateKey::new(1, 1, 1);
            let k_trim = AggregateKey::new(1, 1, 2);
            let k_del = AggregateKey::new(1, 1, 3);

            let (_id, subscriber) = t.watched.add_subscriber(watch_everything_request());

            t.smc.borrow_mut().add_to_pending_queue(vec![
                queue_item(event_batch_metablock(k_event.clone(), 1, 5)),
                queue_item(soft_trim_metablock(k_trim.clone(), 2)),
                queue_item(soft_delete_metablock(k_del.clone(), 1, 5)),
                schema_queue_item(),
            ]);
            deferred_follower_commit_sync(&t);

            // Write side committed at fsync (OCC parity with the leader).
            let (loaded, status) = t.smc.borrow_mut().aggregate_load_status(&k_event, CachePath::Write);
            assert!(loaded, "event write snapshot must commit at fsync");
            assert_eq!(status, celeriant_memcache::mem_snapshot_aggregate::AggregateStatus::Found);
            let pos = t.smc.borrow_mut().get_aggregate_last_metablock_pos(&k_trim, CachePath::Write);
            assert_eq!(pos.min_aggregate_version, 2, "trim floor must commit on the write path at fsync");
            let (_, status) = t.smc.borrow_mut().aggregate_load_status(&k_del, CachePath::Write);
            assert_eq!(status, celeriant_memcache::mem_snapshot_aggregate::AggregateStatus::Deleted,
                "delete must commit on the write path at fsync");

            // Read side parked: nothing visible, no events, no schema, cursor unmoved.
            for k in [&k_event, &k_trim, &k_del] {
                let (loaded, _) = t.smc.borrow_mut().aggregate_load_status(k, CachePath::Read);
                assert!(!loaded, "read snapshot for {:?} must stay cold until confirmed", k);
            }
            assert!(!t.smc.borrow().schema_cache_has_schema(&test_schema_key()),
                "schema must not compile at fsync on the deferred path");
            assert!(t.log_segment.metadata.borrow().read.as_ref().map_or(0, |r| r.wal_seq) == 0,
                "read cursor must not advance at fsync");
            assert!(poll_watch_event(&subscriber).await.is_none(),
                "no watch event may fire at fsync");

            assert_eq!(t.smc.borrow().parked_commit_count(), 1, "the batch must be parked whole");
            assert_eq!(t.smc.borrow_mut().drain_parked_commits_up_to(u64::MAX)[0].pending_queue.len(), 4,
                "every kind parks into the same batch");

            t.lsc.close().await;
        });
    }

    /// Committing a parked batch (commit_pcd with a schema codec) applies the
    /// full read-side set: snapshots, schema compile, watch events, read cursor.
    #[test]
    fn deferred_follower_drain_applies_full_read_side_set() {
        glommio_test!({
            let t = test_shard().await;
            let k_event = AggregateKey::new(1, 1, 1);
            let k_trim = AggregateKey::new(1, 1, 2);
            let k_del = AggregateKey::new(1, 1, 3);

            let (_id, subscriber) = t.watched.add_subscriber(watch_everything_request());

            let mut items = vec![
                queue_item(event_batch_metablock(k_event.clone(), 1, 5)),
                queue_item(soft_trim_metablock(k_trim.clone(), 2)),
                queue_item(soft_delete_metablock(k_del.clone(), 1, 5)),
                schema_queue_item(),
            ];
            for (i, item) in items.iter_mut().enumerate() {
                item.metablock.wal_seq = i as u64 + 1;
                item.metablock_absolute_pos = 4096 * (i as u64 + 1);
            }
            t.smc.borrow_mut().add_to_pending_queue(items);
            t.log_segment.metadata.borrow_mut().write.wal_seq = 4;
            deferred_follower_commit_sync(&t);

            let pcds = t.smc.borrow_mut().drain_parked_commits_up_to(4);
            assert_eq!(pcds.len(), 1);
            for pcd in pcds {
                crate::shard_wal_replicate::commit_pcd(&t.lsc, &t.smc, &t.watched, pcd, Some(&test_codec()));
            }

            let (loaded, status) = t.smc.borrow_mut().aggregate_load_status(&k_event, CachePath::Read);
            assert!(loaded, "event read snapshot must be populated on drain");
            assert_eq!(status, celeriant_memcache::mem_snapshot_aggregate::AggregateStatus::Found);
            let pos = t.smc.borrow_mut().get_aggregate_last_metablock_pos(&k_trim, CachePath::Read);
            assert_eq!(pos.min_aggregate_version, 2, "trim floor must land on the read path on drain");
            let (_, status) = t.smc.borrow_mut().aggregate_load_status(&k_del, CachePath::Read);
            assert_eq!(status, celeriant_memcache::mem_snapshot_aggregate::AggregateStatus::Deleted,
                "delete must land on the read path on drain");
            assert!(t.smc.borrow().schema_cache_has_schema(&test_schema_key()),
                "schema must compile on drain");
            assert_eq!(t.log_segment.metadata.borrow().read.as_ref().map_or(0, |r| r.wal_seq), 4,
                "read cursor must land on the parked batch tip");
            assert!(poll_watch_event(&subscriber).await.is_some(),
                "parked watch events must fire on drain");

            t.lsc.close().await;
        });
    }


    #[test]
    fn empty_segment_no_summary_written() {
        glommio_test!({
            let t = test_shard().await;

            // No writes → empty segment summary → no file should be created
            write_segment_summary_sidecar(&t.lsc, 1, &t.smc).await.unwrap();

            let path = summary_path(t.lsc.shard_dir(), 1);
            assert!(!path.exists(), "empty segment should not produce a .summary file");

            t.lsc.close().await;
        });
    }

    /// The sealed sidecar must carry the segment's blooms at seal — right-sized
    /// from the accumulator's true cardinality (one 32-byte block here), `Some`
    /// even for a sparsely-written segment, and answering exactly like the fixed
    /// in-memory bloom for the same keys. `None` is reserved for the degrade
    /// path ("no bloom"), never written at a complete seal.
    #[test]
    fn sidecar_written_at_seal_carries_sized_segment_blooms() {
        glommio_test!({
            use celeriant_wal::sbbf;
            let t = test_shard().await;
            let k = AggregateKey::new(1, 1, 1);

            t.smc.borrow_mut().add_to_pending_queue(vec![
                queue_item(event_batch_metablock(k.clone(), 1, 5)),
            ]);
            let mut snapshot = t.smc.borrow_mut().take_sync_positions_snapshot();
            let new_metadata = sync(t.log_segment.clone(), &mut snapshot, CommitTarget::FullCommit, true).await.unwrap();
            *t.log_segment.metadata.borrow_mut() = new_metadata;
            t.smc.borrow_mut().update_segment_summary(&event_batch_metablock(k.clone(), 1, 5), 4096);

            write_segment_summary_sidecar(&t.lsc, t.lsc.active_log_id(), &t.smc).await.unwrap();

            let payload = crate::shard_wal::read_segment_summary(t.lsc.shard_dir(), t.lsc.active_log_id()).await
                .expect("sidecar must exist");
            let agg_words = payload.aggregate_bloom.expect("complete seal must persist an aggregate bloom");
            let client_words = payload.client_bloom.expect("complete seal must persist a client bloom");
            assert_eq!(agg_words.len() * 8, 32, "one aggregate sizes to a single SBBF block, not 256 KiB");
            assert_eq!(client_words.len() * 8, 32, "one client sizes to a single SBBF block, not 128 KiB");
            // Answers must match the live fixed-size bloom for the same keys:
            // member present in both, non-member absent in both.
            let live = t.log_segment.metadata.borrow().write.aggregate_key_bloom.borrow().clone();
            assert!(sbbf::contains(&agg_words, k.bloom_hash()) && live.may_contain(&k));
            let other = AggregateKey::new(9, 9, 9);
            assert!(!sbbf::contains(&agg_words, other.bloom_hash()) && !live.may_contain(&other));

            t.lsc.close().await;
        });
    }

    /// The 4 MiB cap is enforced at write time, AFTER the blooms attach: a
    /// payload that only overflows once the ~384 KiB of segment blooms are
    /// counted must still shed client sets so the on-disk file honors the
    /// documented bound. Blooms and entries are never dropped.
    #[test]
    fn sidecar_write_caps_payload_including_bloom_bytes() {
        glommio_test!({
            use celeriant_wal::aggregate_type_key::AggregateTypeKey;
            use celeriant_wal::constants::{AGGREGATE_BLOOM_BYTES, CLIENT_BLOOM_BYTES};

            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path().join("shard");
            std::fs::create_dir_all(&dir).unwrap();

            let mut entry = SegmentAggregateEntry::new(1, 2, 3);
            entry.newest_metablock_pos = 4096;
            // Sized to fit the cap on its own; only the blooms push it over.
            entry.client_set = ClientSet::Bloom(vec![0u64; (SUMMARY_PAYLOAD_MAX_BYTES / 8) as usize - 30_000]);
            let payload = SegmentSummaryPayload {
                orgs: vec![1],
                aggregate_types: vec![AggregateTypeKey::new(1, 2)],
                aggregates: vec![entry],
                complete: true,
                aggregate_bloom: Some(vec![0u64; AGGREGATE_BLOOM_BYTES / 8]),
                client_bloom: Some(vec![0u64; CLIENT_BLOOM_BYTES / 8]),
                schema_bloom: None,
            };
            let bloom_bytes = 2 * 8 + (AGGREGATE_BLOOM_BYTES + CLIENT_BLOOM_BYTES) as u64;
            assert!(payload.wire_size() - bloom_bytes <= SUMMARY_PAYLOAD_MAX_BYTES, "scaffolding: under the cap without blooms");
            assert!(payload.wire_size() > SUMMARY_PAYLOAD_MAX_BYTES, "scaffolding: over the cap with blooms");

            write_segment_summary_sidecar_from_payload(&dir, 1, payload).await.unwrap();

            let decoded = crate::shard_wal::read_segment_summary(&dir, 1).await.expect("sidecar must decode");
            assert!(decoded.wire_size() <= SUMMARY_PAYLOAD_MAX_BYTES, "the on-disk payload must honor the cap including bloom bytes");
            assert_eq!(decoded.aggregates[0].client_set, ClientSet::Unknown, "the client set pays for the overflow");
            assert!(decoded.aggregate_bloom.is_some() && decoded.client_bloom.is_some(), "blooms are never dropped");
        });
    }

    /// Both flag positions must leave the same segment behind: the join changes
    /// submission order only, never layout, chaining or content. Covers the mixed
    /// shape (datablock-bearing + metablock-only items) that exercises both arms
    /// of the write match.
    #[test]
    fn joined_and_serial_writes_leave_identical_segments() {
        glommio_test!({
            let mut results = Vec::new();
            for joined in [false, true] {
                let t = test_shard().await;
                let key = AggregateKey::new(1, 1, 1);
                t.smc.borrow_mut().add_to_pending_queue(vec![
                    datablock_queue_item(key.clone(), 1, 8192),
                    queue_item(soft_delete_metablock(AggregateKey::new(1, 1, 2), 1, 1)),
                    datablock_queue_item(AggregateKey::new(1, 1, 3), 1, 4096),
                ]);
                let mut snapshot = t.smc.borrow_mut().take_sync_positions_snapshot();
                let metadata = sync(t.log_segment.clone(), &mut snapshot, CommitTarget::FullCommit, joined)
                    .await
                    .unwrap();
                let log_id = t.lsc.active_log_id();
                t.lsc.close().await;
                let bytes = std::fs::read(t.lsc.shard_dir().join(format!("log_{log_id}.wal"))).unwrap();
                results.push((
                    metadata.write.wal_seq,
                    metadata.write.metablocks_position,
                    metadata.write.datablocks_position,
                    metadata.write.tip_hash,
                    compute_entry_hash(&GENESIS_HASH, &bytes),
                ));
            }
            assert_eq!(results[0], results[1], "joined and serial must produce byte-identical segments");
        });
    }

    fn overlapping(result: Result<(), ShardFsyncError>) -> bool {
        matches!(result, Err(ShardFsyncError::OverlappingWriteRanges { .. }))
    }

    #[test]
    fn disjoint_ranges_accepted_touching_or_gapped() {
        assert!(check_batch_write_ranges(8192, 12288, 4096, 8192).is_ok());
        assert!(check_batch_write_ranges(1 << 30, (1 << 30) + 4096, 4096, 8192).is_ok());
    }

    #[test]
    fn metablock_range_running_into_datablocks_rejected() {
        // One byte of overlap is enough.
        assert!(overlapping(check_batch_write_ranges(8192, 12288, 4096, 8193)));
        assert!(overlapping(check_batch_write_ranges(8192, 12288, 4096, 16384)));
        // Metablocks entirely above the datablock write: ordering inverted.
        assert!(overlapping(check_batch_write_ranges(4096, 8192, 12288, 16384)));
    }

    #[test]
    fn short_write_is_a_typed_error_full_write_is_ok() {
        assert!(verify_full_write(4096, 4096, "metablocks", 4096).is_ok());
        match verify_full_write(4095, 4096, "metablocks", 8192) {
            Err(ShardFsyncError::ShortWrite { stage, offset, expected, got }) => {
                assert_eq!((stage, offset, expected, got), ("metablocks", 8192, 4096, 4095));
            }
            other => panic!("expected ShortWrite, got {other:?}"),
        }
    }

    #[test]
    fn empty_or_wrapped_ranges_rejected() {
        assert!(overlapping(check_batch_write_ranges(8192, 8192, 4096, 8192)));
        assert!(overlapping(check_batch_write_ranges(12288, 8192, 4096, 8192)));
        assert!(overlapping(check_batch_write_ranges(8192, 12288, 4096, 4096)));
        // A saturated metablock end (the wrap the caller's saturating_add produces)
        // can never sit at or below a datablock start.
        assert!(overlapping(check_batch_write_ranges(8192, 12288, 4096, u64::MAX)));
    }

    /// C7 measurement harness, not a CI assertion (goal Phase 3): the rotation-batch
    /// ack penalty of the inline FullCommit sidecar write — serialize + BufferedFile
    /// create + write + fdatasync. Run explicitly:
    /// `cargo test -p celeriant_shard --release -- --ignored sidecar_write_wall_time --nocapture`
    #[test]
    #[ignore]
    fn sidecar_write_wall_time_measurement() {
        glommio_test!({
            let tmp = tempfile::tempdir().unwrap();
            for &aggregates in &[2_000usize, 38_000] {
                for rep in 0..3 {
                    let payload = SegmentSummaryPayload {
                        orgs: vec![1],
                        aggregate_types: Vec::new(),
                        aggregates: (0..aggregates)
                            .map(|i| SegmentAggregateEntry::new(1, 1, i as u128))
                            .collect(),
                        complete: true,
                        aggregate_bloom: Some(vec![0u64; 8192]),
                        client_bloom: Some(vec![0u64; 8192]),
                        schema_bloom: None,
                    };
                    let wire = payload.wire_size();
                    let t0 = std::time::Instant::now();
                    write_segment_summary_sidecar_from_payload(tmp.path(), rep, payload)
                        .await
                        .unwrap();
                    println!(
                        "aggregates={aggregates} wire_bytes={wire} rep={rep} total_us={}",
                        t0.elapsed().as_micros()
                    );
                }
            }
        });
    }

    /// C6 measurement harness, not a CI assertion (goal Phase 4): serial vs joined
    /// datablock+metablock submit in `sync()`, driven through the real function on
    /// real DmaFiles. Run explicitly, one process per run, three runs or more:
    /// `cargo test -p celeriant_shard --release -- --ignored wal_join_write_wall_time --nocapture`
    ///
    /// Methodology (goal Phase 4, binding): modes interleave in blocks inside one
    /// process; the segment's unwritten extents are converted before the first
    /// measured batch (otherwise the first mode pays xfs extent conversion inside
    /// every fdatasync and the A/B is invalid); the write portion is reported apart
    /// from the flush term, which on consumer NVMe is ~0.6-0.9 ms, unstable and
    /// bistable, and must not decide the verdict.
    #[test]
    #[ignore]
    fn wal_join_write_wall_time_measurement() {
        const SEGMENT_BYTES: u64 = 512 * 1024 * 1024;
        const ITERATIONS_PER_MODE: usize = 220;
        const WARMUP_PER_MODE: usize = 20;
        const MODE_BLOCK: usize = 4;

        // Pin CPU: default 2 (this box's bench core); CELERIANT_BENCH_CPU overrides
        // for machines with fewer cores (the EBS A/B runs on a 2-vCPU c7g.large).
        let cpu: usize = std::env::var("CELERIANT_BENCH_CPU")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2);
        LocalExecutorBuilder::new(Placement::Fixed(cpu))
            .preempt_timer(std::time::Duration::from_micros(250))
            .spawn(move || async move {
                for &(shape, items_per_batch, datablock_bytes) in
                    &[("32KiB x 8", 8usize, 32 * 1024usize), ("4KiB x 1", 1, 4 * 1024)]
                {
                    let (_tmp, dir) = test_dir();
                    let lsc = Rc::new(LogSegmentsCache::ready_up(dir, SEGMENT_BYTES, 4, 0).await.unwrap());
                    let smc = Rc::new(RefCell::new(MemCache::new(
                        256 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024,
                        4 * 1024 * 1024, 2 * 1024 * 1024, 64 * 1024 * 1024,
                    )));
                    let segment = lsc.active();
                    preconvert_extents(&segment, SEGMENT_BYTES).await;

                    // (write portion, header, flush) per mode; [0] = serial, [1] = joined.
                    let mut samples: [Vec<(u64, u64, u64)>; 2] = [Vec::new(), Vec::new()];
                    for iteration in 0..(ITERATIONS_PER_MODE + WARMUP_PER_MODE) * 2 {
                        // Blocks, not strict alternation: consecutive fdatasyncs on this
                        // device fall into a fast/slow 2-cycle locked to iteration parity
                        // (~0.55 ms / ~0.87 ms), so a 1:1 alternation hands one mode the
                        // fast phase for the whole run and the flush column becomes a coin
                        // flip on run order. The block length must be EVEN: with an odd one
                        // the block start parity tracks the mode and it aliases again.
                        let joined = (iteration / MODE_BLOCK) % 2 == 1;
                        let batch: Vec<ShardLogQueueItem> = (0..items_per_batch)
                            .map(|i| {
                                datablock_queue_item(
                                    AggregateKey::new(1, 1, (iteration * items_per_batch + i) as u128),
                                    1,
                                    datablock_bytes,
                                )
                            })
                            .collect();
                        smc.borrow_mut().add_to_pending_queue(batch);
                        let mut snapshot = smc.borrow_mut().take_sync_positions_snapshot();
                        let metadata = sync(segment.clone(), &mut snapshot, CommitTarget::FullCommit, joined)
                            .await
                            .unwrap();
                        *segment.metadata.borrow_mut() = metadata;
                        if iteration >= WARMUP_PER_MODE * 2 {
                            samples[joined as usize].push(LAST_SYNC_PHASE_NS.with(|c| c.get()));
                        }
                    }

                    for (mode, rows) in samples.iter().enumerate() {
                        let label = if mode == 1 { "joined" } else { "serial" };
                        // flush p50 is labelled bimodal: MODE_BLOCK de-aliases the flush
                        // MEAN only; within a run the p50 still splits by which phase of
                        // the device's fast/slow fdatasync 2-cycle a mode's samples hit.
                        // Compare flush via the mean; the verdict column is the write pair.
                        println!(
                            "shape={shape} mode={label} n={} \
                             write_p50_us={} write_p99_us={} write_mean_us={} \
                             header_p50_us={} header_p99_us={} \
                             flush_p50_bimodal_us={} flush_p99_us={} flush_mean_us={} \
                             total_p50_us={} total_p99_us={} total_mean_us={}",
                            rows.len(),
                            percentile_us(rows, 0, 50), percentile_us(rows, 0, 99), mean_us(rows, 0),
                            percentile_us(rows, 1, 50), percentile_us(rows, 1, 99),
                            percentile_us(rows, 2, 50), percentile_us(rows, 2, 99), mean_us(rows, 2),
                            percentile_us(rows, 3, 50), percentile_us(rows, 3, 99), mean_us(rows, 3),
                        );
                    }
                    lsc.close().await;
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// xfs `pre_allocate(keep_size=false)` leaves unwritten extents; the first write
    /// to each converts them, and that conversion lands inside fdatasync. Write the
    /// whole segment region once so the measured batches pay only their own cost.
    async fn preconvert_extents(segment: &Rc<LogSegmentFile>, segment_bytes: u64) {
        let guard = segment.lock_writer("preconvert").await.unwrap();
        let writer = guard.as_ref().unwrap();
        let chunk = 8 * 1024 * 1024usize;
        let mut offset = 0u64;
        while offset < segment_bytes {
            let len = chunk.min((segment_bytes - offset) as usize);
            let mut buffer = writer.alloc_dma_buffer(len);
            buffer.as_bytes_mut().fill(0);
            writer.write_at(buffer, offset).await.unwrap();
            offset += len as u64;
        }
        writer.fdatasync().await.unwrap();
    }

    /// A queue item carrying a real out-of-line datablock payload. `sync()` writes
    /// `datablock_bytes` and only needs `datablock` present to stamp a position.
    fn datablock_queue_item(aggregate_key: AggregateKey, aggregate_version: u64, payload_bytes: usize) -> ShardLogQueueItem {
        use celeriant_wal::datablocks::datablock::Datablock;
        use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
        use celeriant_wal::datablocks::datablock_kind::DatablockKind;

        let datablock = Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                aggregate_version,
                events: Vec::new(),
            }),
        };
        ShardLogQueueItem::new(
            Some(datablock),
            Some(vec![7u8; payload_bytes]),
            event_batch_metablock(aggregate_key, aggregate_version, 1),
        )
    }

    fn phase_ns(row: &(u64, u64, u64), phase: usize) -> u64 {
        match phase {
            0 => row.0,
            1 => row.1,
            2 => row.2,
            _ => row.0 + row.1 + row.2,
        }
    }

    fn percentile_us(rows: &[(u64, u64, u64)], phase: usize, percentile: usize) -> u64 {
        let mut values: Vec<u64> = rows.iter().map(|r| phase_ns(r, phase)).collect();
        values.sort_unstable();
        values[(values.len() - 1) * percentile / 100] / 1000
    }

    /// The flush term is bimodal 50/50 here, which makes its p50 a coin flip on
    /// the split; the mean is the stable comparator for it.
    fn mean_us(rows: &[(u64, u64, u64)], phase: usize) -> u64 {
        rows.iter().map(|r| phase_ns(r, phase)).sum::<u64>() / rows.len() as u64 / 1000
    }

    /// C5 measurement harness, not a CI assertion (goal Phase 3): wall time of the
    /// sync-path serialize + hash-chain loop at production batch sizes, to compare
    /// against the chosen preempt timer. Run explicitly:
    /// `cargo test -p celeriant_shard --release -- --ignored checksum_loop_wall_time --nocapture`
    #[test]
    #[ignore]
    fn checksum_loop_wall_time_measurement() {
        // Runs under a 1 ms preempt timer (the shipped default is 250 us — 4x the
        // fire rate measured here; re-measure debt recorded in session scraps), not
        // glommio_test!'s default
        // (100 ms, under which yield_if_needed never fires and the with_yield rows
        // measure nothing — the tautology the Phase 3 review caught).
        LocalExecutorBuilder::new(Placement::Fixed(0))
            .preempt_timer(std::time::Duration::from_millis(1))
            .spawn(|| async move {
            for &with_yield in &[false, true] {
                for &batch in &[64usize, 256, 1024, 4096] {
                    let items: Vec<Metablock> = (0..batch)
                        .map(|i| event_batch_metablock(AggregateKey::new(1, 1, i as u128), i as u64 + 1, 1))
                        .collect();
                    let mut buf = vec![0u8; batch * FIXED_BLOCK_SIZE_BYTES];
                    let mut tip = GENESIS_HASH;
                    let t0 = std::time::Instant::now();
                    let mut position = 0usize;
                    for item in &items {
                        let slot = &mut buf[position..position + FIXED_BLOCK_SIZE_BYTES];
                        serialize_versioned_message(item, WIRE_VERSION_WAL_METABLOCK, slot).unwrap();
                        tip = compute_entry_hash(&tip, slot);
                        position += FIXED_BLOCK_SIZE_BYTES;
                        if with_yield {
                            glommio::yield_if_needed().await;
                        }
                    }
                    let el = t0.elapsed();
                    std::hint::black_box(tip);
                    println!(
                        "with_yield={with_yield} batch={batch} total_us={} per_item_ns={}",
                        el.as_micros(),
                        el.as_nanos() as u64 / batch as u64
                    );
                }
            }
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
