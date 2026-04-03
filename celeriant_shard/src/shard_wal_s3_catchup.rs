use std::cell::{RefCell};
use std::rc::Rc;

use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::paths::{fallback_shard_prefix, parse_fallback_path};
use celeriant_msg::request::requests::ReplicationBatchItem;
use celeriant_rotating_log::log_segment_file::log_segment_file::{read_datablocks_carry_over_bytes, write_dual_shard_log_header};
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES};
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wire::disk::serialised_datablock::SerialisedDatablock;
use celeriant_wire::disk::versioned_block::{deserialise_fallback_batch, deserialise_metablock};

use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use crate::schema_validator::CompiledValidator;

type MemCache = ShardMemCache<CompiledValidator>;
use celeriant_watch::aggregate_watchers::AggregateWatchers;

use crate::amortisation::coordinator::Coordinator;
use crate::error::apply_batch_error::ApplyBatchError;
use crate::error::s3_catchup_error::S3CatchupError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::s3_downloader::S3Downloader;
use crate::shard_wal_sync::{capture_fsync_snapshot, commit_fsync_with_rollback};

#[derive(Debug, Clone)]
pub struct S3CatchupResult {
    pub batches_applied: u64,
    pub bytes_downloaded: u64,
    pub rounds: u32,
    pub fully_caught_up: bool,
}

struct FallbackBatchRef {
    path: String,
    start_wal_index: u64,
    end_wal_index: u64,
    node_id: u128,
}

/// Keep only batches uploaded by the peer node. Filters out self-uploaded
/// batches (leader must not consume its own fallback) AND batches from
/// unknown node_ids (stale data from a previous cluster generation).
fn retain_peer_batches(batches: Vec<FallbackBatchRef>, self_node_id: u128, peer_node_id: Option<u128>) -> Vec<FallbackBatchRef> {
    batches.into_iter().filter(|b| {
        if b.node_id == self_node_id {
            return false;
        }
        match peer_node_id {
            Some(peer) => b.node_id == peer,
            None => true, // no peer known yet — accept all non-self batches
        }
    }).collect()
}

pub(crate) async fn catchup_from_s3<D: S3Downloader>(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    downloader: &Rc<D>,
    shard_id: u32,
    node_id: u128,
    peer_node_id: Option<u128>,
    max_rounds: u32,
) -> Result<S3CatchupResult, S3CatchupError> {
    let prefix = fallback_shard_prefix(shard_id);
    let mut result = S3CatchupResult {
        batches_applied: 0,
        bytes_downloaded: 0,
        rounds: 0,
        fully_caught_up: false,
    };

    for _ in 0..max_rounds {
        result.rounds += 1;

        let round = catchup_round(
            log_segments_cache, shard_mem_cache, fsync_coordinator,
            watched_aggregates, downloader, &prefix, shard_id, node_id, peer_node_id,
        ).await?;

        // After truncation, continue loop to re-apply from S3
        if round.truncated {
            result.bytes_downloaded += round.bytes;
            continue;
        }

        if round.batches == 0 {
            result.fully_caught_up = true;
            break;
        }

        result.batches_applied += round.batches;
        result.bytes_downloaded += round.bytes;
    }

    if result.rounds > 0 {
        metrics::counter!("celeriant_s3_catchup_rounds_total").increment(result.rounds as u64);
    }
    Ok(result)
}

struct RoundApplied {
    batches: u64,
    bytes: u64,
    truncated: bool,
}

async fn catchup_round<D: S3Downloader>(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    downloader: &Rc<D>,
    prefix: &str,
    shard_id: u32,
    node_id: u128,
    peer_node_id: Option<u128>,
) -> Result<RoundApplied, S3CatchupError> {
    let objects = downloader.list_objects(prefix).await?;

    let current_wal_index = {
        let active = log_segments_cache.active();
        active.metadata.borrow().write.wal_index
    };

    let batches: Vec<FallbackBatchRef> = objects
        .into_iter()
        .filter_map(|obj| {
            let (_sid, start, end, nid) = parse_fallback_path(&obj.path)?;
            Some(FallbackBatchRef { path: obj.path, start_wal_index: start, end_wal_index: end, node_id: nid })
        })
        .filter(|b| b.end_wal_index > current_wal_index)
        .collect();

    let mut batches = retain_peer_batches(batches, node_id, peer_node_id);

    batches.sort_by_key(|b| b.start_wal_index);

    let mut orphan_paths: Vec<String> = Vec::new();
    let mut deduped: Vec<FallbackBatchRef> = Vec::with_capacity(batches.len());
    for batch in batches {
        match deduped.last_mut() {
            Some(prev) if prev.start_wal_index == batch.start_wal_index => {
                // Same-start duplicate: keep whichever covers more entries.
                if batch.end_wal_index > prev.end_wal_index {
                    orphan_paths.push(std::mem::replace(&mut prev.path, batch.path));
                    prev.end_wal_index = batch.end_wal_index;
                } else {
                    orphan_paths.push(batch.path);
                }
            }
            Some(prev) if batch.end_wal_index <= prev.end_wal_index => {
                // Fully contained within previous batch
                orphan_paths.push(batch.path);
            }
            _ => deduped.push(batch),
        }
    }
    let batches = deduped;

    for orphan_path in &orphan_paths {
        if let Err(e) = downloader.delete(orphan_path).await {
            tracing::warn!(path = %orphan_path, error = ?e, "Failed to delete orphaned S3 batch, will retry next round");
        }
    }

    if batches.is_empty() {
        return Ok(RoundApplied { batches: 0, bytes: 0, truncated: false });
    }

    for window in batches.windows(2) {
        let expected = window[0].end_wal_index + 1;
        let got = window[1].start_wal_index;
        if got > expected {
            return Err(S3CatchupError::WalIndexGap { expected, got });
        }
    }

    let mut round = RoundApplied { batches: 0, bytes: 0, truncated: false };

    for batch_ref in &batches {
        let data = downloader.download(&batch_ref.path).await?;
        round.bytes += data.len() as u64;

        let fallback_batch = deserialise_fallback_batch(&data)
            .map_err(|e| S3CatchupError::DeserializationFailed {
                path: batch_ref.path.clone(),
                source: e,
            })?;

        // Never process batches uploaded by this node. A leader must not
        // consume its own S3 fallback batches; they exist for the follower to catch up.
        if fallback_batch.uploaded_by_node_id == node_id {
            tracing::warn!(
                shard_id,
                path = %batch_ref.path,
                "Skipping S3 batch uploaded by this node (self-catchup prevented)"
            );
            continue;
        }

        let all_items: Vec<ReplicationBatchItem> = fallback_batch
            .items
            .into_iter()
            .map(|fi| ReplicationBatchItem { metablock: fi.metablock, datablock: fi.datablock })
            .collect();

        // Skip already-applied entries within partially-overlapping batches
        let current_wal = log_segments_cache.active().metadata.borrow().write.wal_index;
        let skip = all_items.iter()
            .position(|item| item.metablock.wal_index > current_wal)
            .unwrap_or(all_items.len());
        let items = &all_items[skip..];

        if items.is_empty() {
            downloader.delete(&batch_ref.path).await?;
            continue;
        }

        match apply_external_batch(log_segments_cache, shard_mem_cache, items) {
            Ok(()) => {}
            Err(ApplyBatchError::TipHashMismatch { current_wal_index, batch_wal_index, .. }) => {
                // Local WAL diverged from S3 (hash chain mismatch).
                // Find the common ancestor and truncate divergent local entries.
                let (common_ancestor_hash, divergent_wal_index, divergent_entry_position) =
                    match find_divergence_from_batch(log_segments_cache, &all_items).await {
                        Ok(result) => result,
                        Err(_) => find_divergence_via_s3(
                            log_segments_cache, downloader, prefix, current_wal_index, node_id, peer_node_id,
                        ).await?,
                    };

                tracing::warn!(
                    shard_id, current_wal_index, batch_wal_index, divergent_wal_index,
                    "TipHashMismatch detected, truncating divergent WAL entries"
                );
                truncate_wal(
                    log_segments_cache, shard_mem_cache, fsync_coordinator,
                    common_ancestor_hash, divergent_wal_index, divergent_entry_position
                ).await.map_err(S3CatchupError::TruncationFailed)?;

                return Ok(RoundApplied { batches: round.batches, bytes: round.bytes, truncated: true });
            }
            // TODO: Defence in depth - a bug the makes it impossible to catchup via s3
            // Can fallback to TCP. Need to enhance tcp replication side to allow larger catchups in this scenario
            // Err(ApplyBatchError::WalIndexMismatch { current: current_wal_index, batch_first: batch_wal_index }) => {
            //     // Batch starts ahead of local WAL. The follower has orphaned entries
            //     // from a leader rollback that were TCP-replicated but never committed
            //     // to S3. S3 cannot fill this gap — only the leader has those entries.
            //     // Stop this catchup round and let TCP replication fill the gap once
            //     // the follower reconnects to the leader.
            //     tracing::warn!(
            //         shard_id, current_wal_index, batch_wal_index,
            //         gap = batch_wal_index - current_wal_index - 1,
            //         "S3 batch starts ahead of local WAL (leader rollback gap), deferring to TCP replication"
            //     );
            //     return Ok(RoundApplied { batches: round.batches, bytes: round.bytes, truncated: false });
            // }
            Err(e) => return Err(S3CatchupError::ApplyFailed(e)),
        }

        sync_applied_batch(
            log_segments_cache, shard_mem_cache, fsync_coordinator,
            watched_aggregates, shard_id,
        ).await.map_err(S3CatchupError::FsyncFailed)?;

        let shard_label = [("shard_id", shard_id.to_string())];
        let applied_bytes: u64 = items.iter().map(|i| i.metablock.uncompressed_size).sum();
        metrics::counter!("celeriant_replication_applied_events_total", &shard_label).increment(items.len() as u64);
        metrics::counter!("celeriant_replication_applied_bytes_total", &shard_label).increment(applied_bytes);

        downloader.delete(&batch_ref.path).await?;
        round.batches += 1;
    }

    Ok(round)
}

/// Validate WAL continuity and queue entries. Does not fsync.
///
/// Handles mid-batch resume: if the batch contains entries at or before the
/// current WAL index (from a previous partial application that crashed before
/// completing the full batch), those entries are skipped and application
/// resumes from `current_wal_index + 1`.
pub(crate) fn apply_external_batch(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    items: &[ReplicationBatchItem],
) -> Result<(), ApplyBatchError> {
    let (current_tip_hash, current_wal_index) = {
        let active = log_segments_cache.active();
        let metadata = active.metadata.borrow();
        (metadata.write.tip_hash, metadata.write.wal_index)
    };

    // Skip entries already applied (mid-batch resume after crash)
    let skip = items.iter()
        .position(|item| item.metablock.wal_index > current_wal_index)
        .unwrap_or(items.len());
    let items = &items[skip..];

    if items.is_empty() {
        return Ok(());
    }

    let batch_wal_index = items[0].metablock.wal_index;
    let batch_tip_hash = items[0].metablock.previous_tip_hash;

    if current_wal_index.saturating_add(1) != batch_wal_index {
        return Err(ApplyBatchError::WalIndexMismatch {
            current: current_wal_index,
            batch_first: batch_wal_index,
        });
    }
    if current_tip_hash != batch_tip_hash {
        return Err(ApplyBatchError::TipHashMismatch {
            current: current_tip_hash,
            current_wal_index,
            batch: batch_tip_hash,
            batch_wal_index
        });
    }

    queue_replicated_entries(shard_mem_cache, items)
}

fn queue_replicated_entries(
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    items: &[ReplicationBatchItem],
) -> Result<(), ApplyBatchError> {
    for (i, w) in items.windows(2).enumerate() {
        if w[0].metablock.wal_index + 1 != w[1].metablock.wal_index {
            return Err(ApplyBatchError::BatchWalIndexGap {
                index: i + 1,
                expected: w[0].metablock.wal_index + 1,
                actual: w[1].metablock.wal_index,
            });
        }
    }

    let mut prepared = Vec::with_capacity(items.len());

    for item in items {
        let (datablock_bytes, datablock) = match &item.metablock.datablock {
            DatablockStorageKind::None | DatablockStorageKind::Inline(_) => (None, None),
            DatablockStorageKind::Block(_) => {
                if let Some(datablock) = &item.datablock {
                    let compression_type = CompressionType::from_tuple(item.metablock.datablock_compression_type, None);
                    let serialized = SerialisedDatablock::new(datablock, compression_type)
                        .map_err(ApplyBatchError::SerialiseDatablocks)?;
                    let external_data = serialized.external_data
                        .ok_or(ApplyBatchError::BlockBecameInline)?;
                    (Some(external_data), Some(datablock.clone()))
                } else {
                    return Err(ApplyBatchError::MissingDatablock);
                }
            }
        };
        prepared.push(ShardLogQueueItem::new(datablock, datablock_bytes, item.metablock.clone()));
    }

    shard_mem_cache.borrow_mut().add_to_pending_queue(prepared);
    Ok(())
}

/// Fsync via the coordinator (immediate, no amortisation delay).
async fn sync_applied_batch(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    shard_id: u32,
) -> Result<(), ShardFsyncError> {
    let lsc = log_segments_cache.clone();
    let smc = shard_mem_cache.clone();
    let wa = watched_aggregates.clone();
    let mc_capture = smc.clone();

    // We hardcode node status to standalone as we are in offline-catchup mode
    // and can advance the read position immediately (no follower replication)
    fsync_coordinator
        .request_sync_two_phase(
            None,
            move || async move { capture_fsync_snapshot(&mc_capture) },
            move |captured| commit_fsync_with_rollback(NodeStatus::Standalone, lsc, smc, wa, captured, shard_id),
        )
        .await
}

/// Find the common ancestor using the already-downloaded batch from catchup_round.
///
/// The batch that triggered TipHashMismatch often overlaps local WAL entries
/// (items were skipped because wal_index <= current). The earliest item's
/// `previous_tip_hash` points to the state before any of the remote leader's
/// writes in that range — the common ancestor. This avoids any additional S3 calls.
async fn find_divergence_from_batch(
    log_segments_cache: &Rc<LogSegmentsCache>,
    batch_items: &[ReplicationBatchItem],
) -> Result<([u8; 32], u64, u64), S3CatchupError> {
    let candidate_hash = batch_items.first()
        .ok_or_else(|| S3CatchupError::TruncationFailed(
            ShardFsyncError::MetablockSerialisationError("empty batch".into())
        ))?
        .metablock.previous_tip_hash;

    scan_local_metablocks_for_hash(log_segments_cache, candidate_hash).await
}

/// Fallback: find common ancestor by downloading earlier S3 batches one at a time.
///
/// Used when the triggering batch doesn't overlap local data (e.g. batch starts
/// after the local WAL index). Downloads batches backward from the divergence
/// point, stopping as soon as the common ancestor is found.
async fn find_divergence_via_s3<D: S3Downloader>(
    log_segments_cache: &Rc<LogSegmentsCache>,
    downloader: &Rc<D>,
    prefix: &str,
    current_wal_index: u64,
    node_id: u128,
    peer_node_id: Option<u128>,
) -> Result<([u8; 32], u64, u64), S3CatchupError> {
    let objects = downloader.list_objects(prefix).await?;

    let earlier_batches: Vec<FallbackBatchRef> = objects
        .into_iter()
        .filter_map(|obj| {
            let (_sid, start, end, nid) = parse_fallback_path(&obj.path)?;
            Some(FallbackBatchRef { path: obj.path, start_wal_index: start, end_wal_index: end, node_id: nid })
        })
        .filter(|b| b.start_wal_index <= current_wal_index)
        .collect();

    let mut earlier_batches = retain_peer_batches(earlier_batches, node_id, peer_node_id);

    earlier_batches.sort_by(|a, b| b.start_wal_index.cmp(&a.start_wal_index));

    for batch_ref in &earlier_batches {
        let data = downloader.download(&batch_ref.path).await?;
        let fallback_batch = deserialise_fallback_batch(&data)
            .map_err(|e| S3CatchupError::DeserializationFailed {
                path: batch_ref.path.clone(),
                source: e,
            })?;

        let candidate_hash = fallback_batch.items.first()
            .ok_or_else(|| S3CatchupError::TruncationFailed(
                ShardFsyncError::MetablockSerialisationError("empty S3 batch".into())
            ))?
            .metablock.previous_tip_hash;

        if let Ok(result) = scan_local_metablocks_for_hash(log_segments_cache, candidate_hash).await {
            return Ok(result);
        }
    }

    Err(S3CatchupError::TruncationFailed(
        ShardFsyncError::MetablockSerialisationError(
            "no S3 batch shares common ancestor with local WAL".into()
        )
    ))
}

/// Scan backward through local metablocks looking for one whose `previous_tip_hash`
/// matches the candidate. Returns (hash, divergent_wal_index, divergent_position).
async fn scan_local_metablocks_for_hash(
    log_segments_cache: &Rc<LogSegmentsCache>,
    candidate_hash: [u8; 32],
) -> Result<([u8; 32], u64, u64), S3CatchupError> {
    let active = log_segments_cache.active();
    let current_metablocks_position = active.metadata.borrow().write.metablocks_position;

    let dma_file_reader = active.lock_reader("catchup_divergence").await
        .map_err(|_| S3CatchupError::TruncationFailed(ShardFsyncError::WriteLockTimeout))?;
    let dma_file_reader = dma_file_reader.as_ref()
        .ok_or_else(|| S3CatchupError::TruncationFailed(ShardFsyncError::ActiveWriteFileUnavailable))?;

    let min_position = HEADER_BLOCK_SIZE_BYTES as u64;
    let mut position = current_metablocks_position;

    while let Some(pos) = position.checked_sub(FIXED_BLOCK_SIZE_BYTES as u64)
        .filter(|p| *p >= min_position)
    {
        position = pos;
        let buf = dma_file_reader.read_at(position, FIXED_BLOCK_SIZE_BYTES).await
            .map_err(|e| S3CatchupError::TruncationFailed(
                ShardFsyncError::MetablockSerialisationError(format!("read_at failed: {:?}", e))
            ))?;

        let (chunks, _) = (*buf).as_chunks::<FIXED_BLOCK_SIZE_BYTES>();
        let block = match chunks.first() {
            Some(b) => b,
            None => continue,
        };

        let metablock = match deserialise_metablock(block) {
            Ok(mb) => mb,
            Err(_) => continue,
        };

        if metablock.previous_tip_hash == candidate_hash {
            return Ok((candidate_hash, metablock.wal_index, position));
        }
    }

    Err(S3CatchupError::TruncationFailed(
        ShardFsyncError::MetablockSerialisationError(
            "candidate hash not found in local metablocks".into()
        )
    ))
}

/// Truncate the active WAL file to the common ancestor when divergent entries are detected.
/// Uses the already-known divergent entry position from the caller to avoid re-scanning.
async fn truncate_wal(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    common_ancestor_hash: [u8; 32],
    divergent_wal_index: u64,
    divergent_entry_position: u64,
) -> Result<u64, ShardFsyncError> {
    // Step 1: Acquire rollback lock to block concurrent writes
    let _fsync_gate = fsync_coordinator
        .acquire_rollback_lock()
        .await
        .ok_or(ShardFsyncError::WriteLockTimeout)?;

    // Step 2: Clear all caches (including read snapshots and recent writes)
    shard_mem_cache.borrow_mut().clear_all_caches();

    let active = log_segments_cache.active();
    let current_wal_index = active.metadata.borrow().write.wal_index;

    // Calculate how many entries to truncate (including the divergent one)
    let divergent_count = current_wal_index.saturating_sub(divergent_wal_index).saturating_add(1);
    let new_wal_index = divergent_wal_index.saturating_sub(1);
    let new_metablocks_position = divergent_entry_position;


    // Step 3: Rewind cursors (both read and write)
    {
        let mut metadata = active.metadata.borrow_mut();
        metadata.write.wal_index = new_wal_index;
        metadata.write.tip_hash = common_ancestor_hash;
        metadata.write.metablocks_position = new_metablocks_position;

        // Also update read cursor to match
        if let Some(ref mut read) = metadata.read {
            read.wal_index = new_wal_index;
            read.tip_hash = common_ancestor_hash;
            read.metablocks_position = new_metablocks_position;
        }
    }

    // Step 4: Write dual headers and fsync
    let dma_file_writer = active.lock_writer("truncate_wal").await
        .map_err(|_| ShardFsyncError::WriteLockTimeout)?;
    let dma_file_writer = dma_file_writer.as_ref()
        .ok_or(ShardFsyncError::ActiveWriteFileUnavailable)?;

    let (header, header_end_start_pos) = {
        let metadata = active.metadata.borrow();
        let shard_log_header_end_pos = metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
        (metadata.to_shard_log_header(), shard_log_header_end_pos)
    };

    write_dual_shard_log_header(dma_file_writer, header_end_start_pos, &header).await
        .map_err(ShardFsyncError::LogSegmentFileHeaderWriteFailure)?;

    dma_file_writer.fdatasync().await
        .map_err(|e| ShardFsyncError::FDataSyncError(format!("{:?}", e)))?;

    // Step 5: Update datablocks_carry_over
    {
        let mut metadata = active.metadata.borrow_mut();
        metadata.datablocks_carry_over = read_datablocks_carry_over_bytes(dma_file_writer, metadata.write.datablocks_position)
            .await
            .map_err(|e| ShardFsyncError::MetablockSerialisationError(format!("carry-over read failed: {:?}", e)))?;
    }

    tracing::warn!(
        divergent_count,
        new_wal_index,
        "WAL truncated due to divergent entries"
    );

    Ok(divergent_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::path::PathBuf;

    use bytes::Bytes;
    use celeriant_wal::datablocks::datablock::Datablock;
    use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
    use celeriant_wal::datablocks::datablock_kind::DatablockKind;
    use celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch;
    use celeriant_wal::metablocks::metablock_kind::MetablockKind;
    use glommio::{LocalExecutorBuilder, Placement};

    use celeriant_distributed::paths::fallback_batch_path;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::constants::{GENESIS_HASH, WIRE_VERSION_S3_FALLBACK_BATCH};
    use celeriant_wal::metablocks::metablock::Metablock;
    use celeriant_wal::s3::fallback_batch::{FallbackBatch, FallbackItem};
    use celeriant_wire::disk::versioned_block::serialize_versioned_message_heap;

    use crate::s3_downloader::S3ObjectRef;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    const PREALLOCATE: u64 = 4 * 1024 * 1024;

    fn test_dir() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shard");
        (tmp, dir)
    }

    fn test_metablock(wal_index: u64, previous_tip_hash: [u8; 32]) -> Metablock {
        let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::new(1, 1, 1));
        mb.wal_index = wal_index;
        mb.previous_tip_hash = previous_tip_hash;
        mb
    }

    fn serialize_fallback_batch(batch: &FallbackBatch) -> Bytes {
        let data = serialize_versioned_message_heap(batch, WIRE_VERSION_S3_FALLBACK_BATCH).unwrap();
        Bytes::from(data)
    }

    fn make_fallback_batch(shard_id: u32, start: u64, end: u64, tip_hash: [u8; 32]) -> (String, Bytes) {
        make_fallback_batch_with_node(shard_id, start, end, tip_hash, 0)
    }

    fn make_fallback_batch_with_node(shard_id: u32, start: u64, end: u64, tip_hash: [u8; 32], node_id: u128) -> (String, Bytes) {
        let mut batch = FallbackBatch::new(start, end, shard_id, node_id);
        for wal_index in start..=end {
            batch.push_item(FallbackItem {
                metablock: test_metablock(wal_index, tip_hash),
                datablock: None,
            });
        }
        let path = fallback_batch_path(shard_id, start, end, node_id);
        (path, serialize_fallback_batch(&batch))
    }

    // ── Mock S3Downloader ──

    struct MockDownloader {
        objects: RefCell<HashMap<String, Bytes>>,
        download_log: RefCell<Vec<String>>,
        delete_log: RefCell<Vec<String>>,
        list_call_count: Cell<u32>,
        on_list_hooks: RefCell<HashMap<u32, Vec<Box<dyn Fn(&MockDownloader)>>>>,
    }

    impl MockDownloader {
        fn new() -> Self {
            Self {
                objects: RefCell::new(HashMap::new()),
                download_log: RefCell::new(Vec::new()),
                delete_log: RefCell::new(Vec::new()),
                list_call_count: Cell::new(0),
                on_list_hooks: RefCell::new(HashMap::new()),
            }
        }

        fn insert(&self, path: String, data: Bytes) {
            self.objects.borrow_mut().insert(path, data);
        }

        fn downloaded_paths(&self) -> Vec<String> {
            self.download_log.borrow().clone()
        }

        fn deleted_paths(&self) -> Vec<String> {
            self.delete_log.borrow().clone()
        }

        fn on_list(&self, call_index: u32, hook: impl Fn(&Self) + 'static) {
            self.on_list_hooks.borrow_mut()
                .entry(call_index)
                .or_default()
                .push(Box::new(hook));
        }
    }

    impl S3Downloader for MockDownloader {
        async fn list_objects(&self, prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError> {
            let call = self.list_call_count.get();
            self.list_call_count.set(call + 1);
            if let Some(hooks) = self.on_list_hooks.borrow_mut().remove(&call) {
                for hook in hooks {
                    hook(self);
                }
            }
            Ok(self.objects.borrow().iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| S3ObjectRef { path: k.clone(), size: v.len() as u64 })
                .collect())
        }

        async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError> {
            self.download_log.borrow_mut().push(path.to_string());
            self.objects.borrow().get(path).cloned().ok_or_else(|| S3CatchupError::S3GetFailed {
                path: path.to_string(),
                message: "not found".to_string(),
            })
        }

        async fn delete(&self, path: &str) -> Result<(), S3CatchupError> {
            self.objects.borrow_mut().remove(path);
            self.delete_log.borrow_mut().push(path.to_string());
            Ok(())
        }
    }

    // ── Component setup ──

    struct TestComponents {
        log_segments_cache: Rc<LogSegmentsCache>,
        shard_mem_cache: Rc<RefCell<MemCache>>,
        fsync_coordinator: Rc<Coordinator<ShardFsyncError>>,
        watched_aggregates: Rc<AggregateWatchers>,
    }

    impl TestComponents {
        async fn new(dir: &std::path::Path) -> Self {
            let log_segments_cache = LogSegmentsCache::ready_up(dir.to_path_buf(), PREALLOCATE, 4, 0)
                .await
                .unwrap();
            Self {
                log_segments_cache: Rc::new(log_segments_cache),
                shard_mem_cache: Rc::new(RefCell::new(MemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 1024 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024))),
                fsync_coordinator: Rc::new(Coordinator::new()),
                watched_aggregates: Rc::new(AggregateWatchers::new()),
            }
        }

        fn wal_index(&self) -> u64 {
            self.log_segments_cache.active().metadata.borrow().write.wal_index
        }

        fn tip_hash(&self) -> [u8; 32] {
            self.log_segments_cache.active().metadata.borrow().write.tip_hash
        }

        async fn catchup(&self, downloader: &Rc<MockDownloader>, shard_id: u32, max_rounds: u32) -> Result<S3CatchupResult, S3CatchupError> {
            self.catchup_with_peer(downloader, shard_id, max_rounds, None).await
        }

        async fn catchup_with_peer(&self, downloader: &Rc<MockDownloader>, shard_id: u32, max_rounds: u32, peer_node_id: Option<u128>) -> Result<S3CatchupResult, S3CatchupError> {
            catchup_from_s3(
                &self.log_segments_cache, &self.shard_mem_cache, &self.fsync_coordinator,
                &self.watched_aggregates,
                downloader, shard_id, 99, peer_node_id, max_rounds,
            ).await
        }

        async fn close(&self) {
            self.log_segments_cache.close().await;
        }
    }

    // ── apply_external_batch tests ──

    #[test]
    fn apply_rejects_wal_index_mismatch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            let item = ReplicationBatchItem {
                metablock: test_metablock(99, GENESIS_HASH),
                datablock: None,
            };
            let err = apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item]).unwrap_err();
            assert!(matches!(err, ApplyBatchError::WalIndexMismatch { current: 0, batch_first: 99 }));

            tc.close().await;
        });
    }

    #[test]
    fn apply_rejects_tip_hash_mismatch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            let item = ReplicationBatchItem {
                metablock: test_metablock(1, [0xAB; 32]),
                datablock: None,
            };
            let err = apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item]).unwrap_err();
            assert!(matches!(err, ApplyBatchError::TipHashMismatch { .. }));

            tc.close().await;
        });
    }

    #[test]
    fn apply_queues_valid_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            let item = ReplicationBatchItem {
                metablock: test_metablock(1, GENESIS_HASH),
                datablock: None,
            };
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item]).unwrap();
            assert!(!tc.shard_mem_cache.borrow().pending_append_queue_is_empty());

            tc.close().await;
        });
    }

    #[test]
    fn apply_skips_already_applied_entries_mid_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            // Apply entry 1 to advance WAL
            let item1 = ReplicationBatchItem {
                metablock: test_metablock(1, GENESIS_HASH),
                datablock: None,
            };
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item1]).unwrap();
            // Flush the pending queue so WAL index advances
            sync_applied_batch(
                &tc.log_segments_cache, &tc.shard_mem_cache,
                &tc.fsync_coordinator, &tc.watched_aggregates, 0,
            ).await.unwrap();
            assert_eq!(tc.wal_index(), 1);

            // Now send a batch [1, 2] — entry 1 should be skipped, entry 2 applied
            let tip = tc.tip_hash();
            let stale = ReplicationBatchItem {
                metablock: test_metablock(1, GENESIS_HASH),
                datablock: None,
            };
            let fresh = ReplicationBatchItem {
                metablock: test_metablock(2, tip),
                datablock: None,
            };
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[stale, fresh]).unwrap();

            tc.close().await;
        });
    }

    #[test]
    fn apply_returns_ok_when_batch_fully_applied() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            // Apply entry 1
            let item = ReplicationBatchItem {
                metablock: test_metablock(1, GENESIS_HASH),
                datablock: None,
            };
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item.clone()]).unwrap();
            sync_applied_batch(
                &tc.log_segments_cache, &tc.shard_mem_cache,
                &tc.fsync_coordinator, &tc.watched_aggregates, 0,
            ).await.unwrap();
            assert_eq!(tc.wal_index(), 1);

            // Re-send same entry — should be a no-op
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item]).unwrap();

            tc.close().await;
        });
    }

    // ── catchup_from_s3 tests ──

    #[test]
    fn catchup_empty_listing_returns_zero() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 0);
            assert!(result.fully_caught_up);
            assert_eq!(result.rounds, 1);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_applies_single_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path, data) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert!(result.fully_caught_up);
            assert_eq!(result.rounds, 2);
            assert_eq!(tc.wal_index(), 1);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_applies_batch_with_multiple_entries() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path, data) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_index(), 5);
            assert!(result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_detects_wal_index_gap() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path, data) = make_fallback_batch(0, 1, 2, GENESIS_HASH);
            dl.insert(path, data);
            // Gap: missing batch 3-4
            let (path, data) = make_fallback_batch(0, 5, 6, GENESIS_HASH);
            dl.insert(path, data);

            let err = tc.catchup(&dl, 0, 10).await.unwrap_err();
            assert!(matches!(err, S3CatchupError::WalIndexGap { expected: 3, got: 5 }));

            tc.close().await;
        });
    }

    #[test]
    fn catchup_deletes_applied_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            let expected_path = path.clone();
            dl.insert(path, data);

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(dl.deleted_paths(), vec![expected_path]);
            assert!(dl.objects.borrow().is_empty());

            tc.close().await;
        });
    }

    #[test]
    fn catchup_respects_max_rounds() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Simulate leader writing faster than we catch up:
            // After each round's deletes, inject a new batch for the next round
            let (path, data) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path, data);

            // max_rounds=1: apply batch 1, then stop without re-listing
            let result = tc.catchup(&dl, 0, 1).await.unwrap();
            assert_eq!(result.rounds, 1);
            assert_eq!(result.batches_applied, 1);
            assert!(!result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_skips_already_applied_batches() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Apply batch 1 first
            let (path, data) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 1);

            // Add batch 2, but also re-add batch 1 (already applied)
            let tip = tc.tip_hash();
            let (path1, data1) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path1, data1);
            let (path2, data2) = make_fallback_batch(0, 2, 2, tip);
            dl.insert(path2, data2);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_index(), 2);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_filters_by_shard_id() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Shard 0 batch
            let (path, data) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path, data);
            // Shard 1 batch (should be ignored when catching up shard 0)
            let (path, data) = make_fallback_batch(1, 1, 1, GENESIS_HASH);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_index(), 1);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_handles_partial_overlap() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Apply batch 1-3 first
            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 3);

            // Add overlapping batch 2-6: entries 2-3 already applied, 4-6 are new.
            // All items get the same tip_hash; only item 4 (first after slicing) is checked.
            let tip = tc.tip_hash();
            let (path, data) = make_fallback_batch(0, 2, 6, tip);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_index(), 6);
            assert!(result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_multi_round_picks_up_new_batches() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Round 1: batch 1-3 available immediately
            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            dl.insert(path, data);

            // Round 2: inject batch 4-6 when list_objects is called the second time
            // (simulates leader uploading while we applied round 1)
            let lsc = tc.log_segments_cache.clone();
            dl.on_list(1, move |dl| {
                let tip = lsc.active().metadata.borrow().write.tip_hash;
                let (path, data) = make_fallback_batch(0, 4, 6, tip);
                dl.insert(path, data);
            });

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 2);
            assert_eq!(result.rounds, 3); // round 1: apply 1-3, round 2: apply 4-6, round 3: empty
            assert_eq!(tc.wal_index(), 6);
            assert!(result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_truncates_divergent_entries_and_retries() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Step 1: Apply entries 1-5 normally
            let (path1_5, data1_5) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            dl.insert(path1_5, data1_5);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 5);
            let tip_after_5 = tc.tip_hash();

            // Step 2: Apply divergent entry 6 (simulates follower receiving from old leader)
            let (path6_divergent, data6_divergent) = make_fallback_batch(0, 6, 6, tip_after_5);
            dl.insert(path6_divergent, data6_divergent);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 6);
            let tip_after_divergent_6 = tc.tip_hash();
            assert_ne!(tip_after_5, tip_after_divergent_6, "Tip hash should change after entry 6");

            // Step 3: S3 now has "correct" batch 6-8 from new leader with previous_tip = tip_after_5
            // This will mismatch our current tip (tip_after_divergent_6), triggering truncation
            dl.objects.borrow_mut().clear();
            let (path6_8, data6_8) = make_fallback_batch(0, 6, 8, tip_after_5);
            dl.insert(path6_8, data6_8);

            // Step 4: Catchup detects TipHashMismatch, truncates entry 6, re-applies 6-8
            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 8, "Should catch up to wal_index 8 after truncation");
            assert!(result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn truncate_old_s3_files_not_downloaded() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Apply entries 1-5
            let (path, data) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            let tip_after_5 = tc.tip_hash();

            // Apply divergent entry 6
            let (path, data) = make_fallback_batch(0, 6, 6, tip_after_5);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();

            // S3 now has old batch 1-5 (stale) + correct batch 6-8 from new leader.
            // Old batch should NOT be downloaded during divergence repair.
            dl.objects.borrow_mut().clear();
            dl.download_log.borrow_mut().clear();

            let (old_path, old_data) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            dl.insert(old_path.clone(), old_data);
            let (new_path, new_data) = make_fallback_batch(0, 6, 8, tip_after_5);
            dl.insert(new_path.clone(), new_data);

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 8);

            // The old batch (1-5) should never have been downloaded
            let downloads = dl.downloaded_paths();
            assert!(!downloads.contains(&old_path), "Old batch 1-5 should not be downloaded, got: {:?}", downloads);

            tc.close().await;
        });
    }

    #[test]
    fn truncate_multiple_divergent_local_entries() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Apply entries 1-5
            let (path, data) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            let tip_after_5 = tc.tip_hash();

            // Apply 3 divergent entries (6-8) from old leader
            let (path, data) = make_fallback_batch(0, 6, 8, tip_after_5);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 8);

            // New leader wrote 6-12 (overlapping batch starting before our divergence)
            dl.objects.borrow_mut().clear();
            let (path, data) = make_fallback_batch(0, 6, 12, tip_after_5);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 12);
            assert!(result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn truncate_deep_divergence_both_nodes() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Apply entries 1-3
            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            let tip_after_3 = tc.tip_hash();

            // A diverges with entries 4-8 (5 divergent entries)
            let (path, data) = make_fallback_batch(0, 4, 8, tip_after_3);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 8);

            // B wrote 4-14 (5 more entries than A, starting from same common ancestor)
            dl.objects.borrow_mut().clear();
            let (path, data) = make_fallback_batch(0, 4, 14, tip_after_3);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 14);
            assert!(result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn truncate_unrecoverable_divergence_errors() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Apply entries 1-3
            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            let tip_after_3 = tc.tip_hash();

            // Apply divergent entry 4
            let (path, data) = make_fallback_batch(0, 4, 4, tip_after_3);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();

            // S3 has batch 4-6 with a completely unknown previous_tip_hash.
            // No local metablock will match — divergence is unrecoverable.
            dl.objects.borrow_mut().clear();
            let (path, data) = make_fallback_batch(0, 4, 6, [0xFF; 32]);
            dl.insert(path, data);

            let err = tc.catchup(&dl, 0, 10).await.unwrap_err();
            assert!(matches!(err, S3CatchupError::TruncationFailed(_)));

            tc.close().await;
        });
    }

    #[test]
    fn truncate_s3_fallback_finds_ancestor_from_earlier_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            // Apply entries 1-5, then divergent entry 6
            let dl_setup = Rc::new(MockDownloader::new());
            let (path, data) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            dl_setup.insert(path, data);
            tc.catchup(&dl_setup, 0, 10).await.unwrap();
            let tip_after_5 = tc.tip_hash();

            let (path, data) = make_fallback_batch(0, 6, 6, tip_after_5);
            dl_setup.insert(path, data);
            tc.catchup(&dl_setup, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 6);

            // Fresh downloader for the divergence scenario.
            // S3 has batch 6-6 (filtered out by catchup_round since end=6 = current)
            // and batch 7-9 with a B-specific hash the fast path can't resolve locally.
            let dl = Rc::new(MockDownloader::new());
            let (path6, data6) = make_fallback_batch(0, 6, 6, tip_after_5);
            dl.insert(path6, data6);
            let (path7_9, data7_9) = make_fallback_batch(0, 7, 9, [0xBB; 32]);
            dl.insert(path7_9, data7_9);

            let batch_7_9_path = fallback_batch_path(0, 7, 9, 0);

            // Call 2 (retry after truncation): remove batch 7-9 so only 6-6 applies this round
            let p = batch_7_9_path.clone();
            dl.on_list(2, move |dl| {
                dl.objects.borrow_mut().remove(&p);
            });

            // Call 3 (next round): inject batch 7-9 with correct tip from newly-applied entry 6
            let lsc = tc.log_segments_cache.clone();
            dl.on_list(3, move |dl| {
                let tip = lsc.active().metadata.borrow().write.tip_hash;
                let (path, data) = make_fallback_batch(0, 7, 9, tip);
                dl.insert(path, data);
            });

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 9);
            assert!(result.fully_caught_up);

            // Verify the S3 fallback downloaded batch 6-6 to find the ancestor
            let downloads = dl.downloaded_paths();
            let batch_6_path = fallback_batch_path(0, 6, 6, 0);
            assert!(
                downloads.contains(&batch_6_path),
                "S3 fallback should have downloaded batch 6-6 to find ancestor, got: {:?}",
                downloads
            );

            tc.close().await;
        });
    }

    #[test]
    fn test_fallback_batch_s3_path() {
        let batch = FallbackBatch::new(5, 10, 2, 0);
        assert_eq!(fallback_batch_path(batch.shard_id, batch.fallback_index, batch.end_wal_index, batch.uploaded_by_node_id), "cluster/fallback/shard_002/batch_000000005_000000010_00000000-0000-0000-0000-000000000000.bin");
    }

    #[test]
    fn test_fallback_batch_bincode_roundtrip() {
        let aggregate_key = AggregateKey::new(1, 2, 3);

        let metablock1 = Metablock {
            wal_index: 42,
            server_timestamp: 1000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: 10,
                min_event_batch_index: 1,
                min_client_event_index: 1,
                max_client_event_index: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_index: 1,
                max_event_index: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::Block(celeriant_wal::metablocks::datablock_block_ref::DatablockBlockRef {
                crc32c: 0,
            }),
            datablock_position: 1000,
        };

        let metablock2 = Metablock {
            wal_index: 43,
            server_timestamp: 2000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 2048,
            compressed_size: 1024,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [2u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: 11,
                min_event_batch_index: 1,
                min_client_event_index: 6,
                max_client_event_index: 10,
                min_event_timestamp: 600,
                max_event_timestamp: 1000,
                min_event_index: 6,
                max_event_index: 10,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
            datablock_position: 0,
        };

        let datablock1 = Some(Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 10,
                events: vec![],
            }),
        });

        let original_batch = FallbackBatch {
            fallback_index: 42,
            end_wal_index: 43,
            shard_id: 7,
            uploaded_by_node_id: 1,
            items: vec![
                FallbackItem {
                    metablock: metablock1,
                    datablock: datablock1,
                },
                FallbackItem {
                    metablock: metablock2,
                    datablock: None,
                },
            ],
        };

        let serialized = celeriant_wire::disk::versioned_block::serialize_versioned_message_heap(
            &original_batch,
            celeriant_wal::constants::WIRE_VERSION_S3_FALLBACK_BATCH,
        ).expect("serialization should succeed");

        let deserialized = celeriant_wire::disk::versioned_block::deserialise_fallback_batch(&serialized)
            .expect("deserialization should succeed");

        assert_eq!(deserialized.fallback_index, 42);
        assert_eq!(deserialized.end_wal_index, 43);
        assert_eq!(deserialized.shard_id, 7);
        assert_eq!(deserialized.items.len(), 2);

        assert_eq!(deserialized.items[0].metablock.wal_index, 42);
        assert_eq!(deserialized.items[0].metablock.server_timestamp, 1000);
        assert!(deserialized.items[0].datablock.is_some());

        assert_eq!(deserialized.items[1].metablock.wal_index, 43);
        assert_eq!(deserialized.items[1].metablock.server_timestamp, 2000);
        assert!(deserialized.items[1].datablock.is_none());
    }

    #[test]
    fn test_fallback_index_is_first_wal_index() {
        let aggregate_key = AggregateKey::new(1, 2, 3);

        let metablock_first = Metablock {
            wal_index: 100,
            server_timestamp: 1000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: 10,
                min_event_batch_index: 1,
                min_client_event_index: 1,
                max_client_event_index: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_index: 1,
                max_event_index: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
            datablock_position: 0,
        };

        let metablock_second = Metablock {
            wal_index: 101,
            ..metablock_first.clone()
        };

        let batch = FallbackBatch {
            fallback_index: 100,
            end_wal_index: 101,
            shard_id: 5,
            uploaded_by_node_id: 1,
            items: vec![
                FallbackItem {
                    metablock: metablock_first,
                    datablock: None,
                },
                FallbackItem {
                    metablock: metablock_second,
                    datablock: None,
                },
            ],
        };

        assert_eq!(batch.fallback_index, batch.items[0].metablock.wal_index);
        assert_eq!(batch.end_wal_index, batch.items[batch.items.len() - 1].metablock.wal_index);
    }

    #[test]
    fn test_shard_id_narrowing() {
        let batch_0 = FallbackBatch::new(1, 5, 0, 0);
        assert_eq!(
            fallback_batch_path(batch_0.shard_id, batch_0.fallback_index, batch_0.end_wal_index, batch_0.uploaded_by_node_id),
            "cluster/fallback/shard_000/batch_000000001_000000005_00000000-0000-0000-0000-000000000000.bin"
        );

        let batch_999 = FallbackBatch::new(1, 10, 999, 0);
        assert_eq!(
            fallback_batch_path(batch_999.shard_id, batch_999.fallback_index, batch_999.end_wal_index, batch_999.uploaded_by_node_id),
            "cluster/fallback/shard_999/batch_000000001_000000010_00000000-0000-0000-0000-000000000000.bin"
        );

        assert!(u32::MAX > 999);
    }

    #[test]
    fn dedup_keeps_longer_batch_and_deletes_orphan() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path_short, data_short) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            let (path_long, data_long) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            let orphan_path = path_short.clone();
            dl.insert(path_short, data_short);
            dl.insert(path_long, data_long);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 5);
            assert_eq!(result.batches_applied, 1);
            assert!(dl.deleted_paths().contains(&orphan_path));

            tc.close().await;
        });
    }

    #[test]
    fn dedup_with_multiple_duplicates() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path_1_2, data_1_2) = make_fallback_batch(0, 1, 2, GENESIS_HASH);
            let (path_1_4, data_1_4) = make_fallback_batch(0, 1, 4, GENESIS_HASH);
            let (path_1_6, data_1_6) = make_fallback_batch(0, 1, 6, GENESIS_HASH);
            let orphan_1_2 = path_1_2.clone();
            let orphan_1_4 = path_1_4.clone();
            dl.insert(path_1_2, data_1_2);
            dl.insert(path_1_4, data_1_4);
            dl.insert(path_1_6, data_1_6);

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 6);
            let deleted = dl.deleted_paths();
            assert!(deleted.contains(&orphan_1_2));
            assert!(deleted.contains(&orphan_1_4));

            tc.close().await;
        });
    }

    #[test]
    fn dedup_preserves_non_overlapping_batches() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path_1_3, data_1_3) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            let tip = {
                // Apply batch 1-3 to get tip hash, then reset
                let tmp_dl = Rc::new(MockDownloader::new());
                tmp_dl.insert(path_1_3.clone(), data_1_3.clone());
                tc.catchup(&tmp_dl, 0, 10).await.unwrap();
                tc.tip_hash()
            };
            // WAL is now at 3, insert batch 4-6 with correct tip
            let (path_4_6, data_4_6) = make_fallback_batch(0, 4, 6, tip);
            dl.insert(path_4_6.clone(), data_4_6);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 6);
            assert_eq!(result.batches_applied, 1);
            // Only the post-apply deletion of batch 4-6 should exist, no orphan deletions
            let deleted = dl.deleted_paths();
            assert_eq!(deleted, vec![path_4_6]);

            tc.close().await;
        });
    }

    #[test]
    fn self_uploaded_batches_filtered_before_gap_check() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // node_id 99 is the catchup node (see TestComponents::catchup).
            // Batch 1-5 from node_id=0 (other node) — will be applied.
            let (path_real, data_real) = make_fallback_batch_with_node(0, 1, 5, GENESIS_HASH, 0);
            dl.insert(path_real, data_real);

            // Batch 1-3 from node_id=99 (self) — filtered out before dedup/gap check.
            // Without the filter, dedup would handle this (keeps longer 1-5), but filtering
            // is defence-in-depth: self-uploaded batches never participate in gap validation.
            let (path_self, data_self) = make_fallback_batch_with_node(0, 1, 3, GENESIS_HASH, 99);
            let self_path = path_self.clone();
            dl.insert(path_self, data_self);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 5);
            assert_eq!(result.batches_applied, 1);

            // Self-uploaded batch must NOT be deleted — the other node may need it
            assert!(
                !dl.deleted_paths().contains(&self_path),
                "Self-uploaded batches must not be deleted from S3"
            );

            tc.close().await;
        });
    }

    #[test]
    fn self_uploaded_batches_filtered_not_deleted() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Insert only self-uploaded batches (node_id=99)
            let (path1, data1) = make_fallback_batch_with_node(0, 1, 3, GENESIS_HASH, 99);
            dl.insert(path1, data1);
            let (path2, data2) = make_fallback_batch_with_node(0, 4, 6, GENESIS_HASH, 99);
            dl.insert(path2, data2);

            // Catchup should filter all self-uploaded batches and report nothing to apply
            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 0);
            assert!(result.fully_caught_up);

            // Self-uploaded batches must NOT be deleted — the other node may need them
            assert!(dl.deleted_paths().is_empty(), "Self-uploaded batches must not be deleted from S3");
            assert_eq!(dl.objects.borrow().len(), 2, "Both S3 objects must remain");

            tc.close().await;
        });
    }

    #[test]
    fn dedup_discards_batch_fully_contained_in_previous() {
        // Leadership flap: batch from old term is fully contained within a batch
        // from a later term (different starts). Must not cause WalIndexGap.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Large batch from one leadership term: 1-20
            let (path_wide, data_wide) = make_fallback_batch(0, 1, 20, GENESIS_HASH);
            // Smaller stale batch from old term: 5-10, fully inside 1-20
            let (path_stale, data_stale) = make_fallback_batch_with_node(0, 5, 10, GENESIS_HASH, 1);
            let stale_path = path_stale.clone();
            dl.insert(path_wide, data_wide);
            dl.insert(path_stale, data_stale);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 20);
            assert_eq!(result.batches_applied, 1);
            assert!(dl.deleted_paths().contains(&stale_path));

            tc.close().await;
        });
    }

    #[test]
    fn partial_overlap_does_not_trigger_wal_index_gap() {
        // Two batches from different leadership terms that partially overlap.
        // The contiguity check must allow overlaps (got < expected), only forward
        // gaps are fatal. The apply path handles divergence via TipHashMismatch.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Batch 1-10 from node 0
            let (path_a, data_a) = make_fallback_batch(0, 1, 10, GENESIS_HASH);
            // Batch 5-15 from node 1 (partial overlap: indices 5-10)
            let (path_b, data_b) = make_fallback_batch_with_node(0, 5, 15, GENESIS_HASH, 1);
            dl.insert(path_a, data_a);
            dl.insert(path_b, data_b);

            let result = tc.catchup(&dl, 0, 10).await;
            // Must NOT be WalIndexGap. overlaps are allowed
            assert!(!matches!(result, Err(S3CatchupError::WalIndexGap { .. })),
                "Overlapping batches must not trigger WalIndexGap, got: {:?}", result);

            tc.close().await;
        });
    }

    #[test]
    fn forward_gap_between_batches_still_fatal() {
        // Genuine forward gap (missing entries between batches) must still error.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Batch 1-5
            let (path_a, data_a) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            // Batch 10-15: gap at 6-9
            let (path_b, data_b) = make_fallback_batch_with_node(0, 10, 15, GENESIS_HASH, 1);
            dl.insert(path_a, data_a);
            dl.insert(path_b, data_b);

            let result = tc.catchup(&dl, 0, 10).await;
            assert!(matches!(result, Err(S3CatchupError::WalIndexGap { expected: 6, got: 10 })));

            tc.close().await;
        });
    }

    #[test]
    fn unknown_node_batches_ignored_when_peer_known() {
        // Stale S3 batches from a previous cluster generation (different node_ids)
        // must be ignored when the current peer is known. This prevents
        // WalIndexGap errors from leftover data after a partial teardown.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let peer_id: u128 = 42;
            let stale_id: u128 = 777; // old node, not current peer

            // Batch 1-5 from current peer — should be applied
            let (path_peer, data_peer) = make_fallback_batch_with_node(0, 1, 5, GENESIS_HASH, peer_id);
            dl.insert(path_peer, data_peer);

            // Batch 10-15 from stale node — would cause WalIndexGap if not filtered
            let (path_stale, data_stale) = make_fallback_batch_with_node(0, 10, 15, GENESIS_HASH, stale_id);
            let stale_path = path_stale.clone();
            dl.insert(path_stale, data_stale);

            let result = tc.catchup_with_peer(&dl, 0, 10, Some(peer_id)).await.unwrap();
            assert_eq!(tc.wal_index(), 5);
            assert_eq!(result.batches_applied, 1);

            // Stale batch must not be deleted (not our responsibility)
            assert!(!dl.deleted_paths().contains(&stale_path));

            tc.close().await;
        });
    }

    #[test]
    fn unknown_node_batches_accepted_when_no_peer_known() {
        // When no peer is known yet (boot before first election), accept
        // all non-self batches as before.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path, data) = make_fallback_batch_with_node(0, 1, 5, GENESIS_HASH, 777);
            dl.insert(path, data);

            let result = tc.catchup_with_peer(&dl, 0, 10, None).await.unwrap();
            assert_eq!(tc.wal_index(), 5);
            assert_eq!(result.batches_applied, 1);

            tc.close().await;
        });
    }

    #[test]
    fn dedup_resolves_gap_from_orphaned_partial_upload() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Advance WAL to 47
            let (path, data) = make_fallback_batch(0, 1, 47, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 47);
            let tip = tc.tip_hash();

            // Orphan from failed chunk + successful retry
            let (path_48_52, data_48_52) = make_fallback_batch(0, 48, 52, tip);
            let (path_48_60, data_48_60) = make_fallback_batch(0, 48, 60, tip);
            let orphan_path = path_48_52.clone();
            dl.insert(path_48_52, data_48_52);
            dl.insert(path_48_60, data_48_60);

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 60);
            assert!(dl.deleted_paths().contains(&orphan_path));

            tc.close().await;
        });
    }
}