use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use tracing::{info, warn};

use celeriant_disk::files::open_dma_files::create_file_dma;
use celeriant_disk::files::read_fixed_records_visit_const::{read_fixed_records_visit_const, ReadVisitError};
use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::mem_snapshot_aggregate::AggregateStatus;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_rotating_log::errors::scan_error::ScanError;
use celeriant_rotating_log::log_segment_file::aggregate_key_bloom::AggregateKeyBloom;
use celeriant_rotating_log::log_segment_file::log_segment_file::write_dual_shard_log_header;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK};
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_wal::shard_log_header::ShardLogHeader;
use celeriant_wire::disk::versioned_block::{deserialise_metablock, serialize_versioned_message};

use crate::error::compaction_error::CompactionError;
use crate::schema_validator::CompiledValidator;


type MemCache = ShardMemCache<CompiledValidator>;

/// The resolved compaction state for a single aggregate.
#[derive(Debug, Clone)]
pub enum AggregateCompactionState {
    /// Aggregate is alive; all event batches should be kept.
    Alive,
    /// Aggregate was soft-deleted; all event batches should be skipped.
    Deleted,
    /// Aggregate was trimmed; batches below `min_event_batch_index` should be skipped.
    Trimmed { min_event_batch_index: u64 },
    /// Aggregate was deleted then recreated; event batches with `wal_index <= deletion_wal_index`
    /// are dead (pre-deletion), those with `wal_index > deletion_wal_index` are alive.
    RecreatedAfter { deletion_wal_index: u64 },
}

/// Default chunk size for forward metablock scanning (32KB = 32 metablocks at 1KB each).
pub(crate) const SCAN_CHUNK_SIZE: u64 = 32 * 1024;

/// Resolve the compaction state for every aggregate key present in the target segment.
///
/// Algorithm:
///   Pass 1: Forward-scan target segment metablocks to collect aggregate keys only.
///   Pass 2a: Check ShardMemCache for aggregates definitively deleted in a newer segment.
///   Pass 2b: Reverse WAL scan from the active segment down to (and including) the target.
///            Scanning INTO the target segment is essential for correctness: when an aggregate
///            is soft-deleted and then recreated within the same segment, the reverse scan
///            sees the post-recreation batches first, then the SoftDelete — allowing us to
///            emit RecreatedAfter rather than Deleted and preserve the post-recreation data.
///            Segments are skipped via bloom filter when no unresolved key is present.
///
/// The result maps each aggregate key encountered in the segment to its current state.
async fn resolve_aggregate_states(
    target_log_id: u64,
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
) -> Result<HashMap<AggregateKey, AggregateCompactionState>, CompactionError> {
    // Guard: never compact the active segment — it may still be receiving writes.
    if target_log_id >= log_segments_cache.active_log_id() {
        return Err(CompactionError::ActiveSegmentTarget { log_id: target_log_id });
    }

    let mut state_map: HashMap<AggregateKey, AggregateCompactionState> = HashMap::new();
    let mut unresolved: HashSet<AggregateKey> = HashSet::new();

    // -------------------------------------------------------------------------
    // Pass 1: Collect aggregate keys from the target segment (no tombstone logic).
    // State is resolved entirely by the reverse scan below.
    // -------------------------------------------------------------------------
    {
        let log_segment = log_segments_cache.get(target_log_id).await?;
        let metablocks_start = HEADER_BLOCK_SIZE_BYTES as u64;
        let metablocks_end = log_segment.metadata.borrow().readable_metablocks_end();

        if metablocks_end > metablocks_start {
            let guard = log_segment
                .lock_reader("compact_pass1")
                .await
                .map_err(|_| CompactionError::LockTimeout)?;
            let dma_file = guard
                .as_ref()
                .ok_or(CompactionError::SegmentUnavailable { log_id: target_log_id })?;

            let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, CompactionError>(
                dma_file,
                false,
                metablocks_start,
                metablocks_end,
                SCAN_CHUNK_SIZE,
                |_pos, block| {
                    let metablock = deserialise_metablock(block)
                        .map_err(CompactionError::MetablockDeserialise)?;
                    if let MetablockKind::EventBatchMetadata(eb) = metablock.wal_metablock_type {
                        unresolved.insert(eb.aggregate_key);
                    }
                    Ok(false)
                },
            )
            .await;

            match result {
                Ok(_) => {}
                Err(ReadVisitError::Visitor(e)) => return Err(e),
                Err(ReadVisitError::Io(e)) => {
                    return Err(CompactionError::ForwardScanIo {
                        log_id: target_log_id,
                        source: e.to_string(),
                    });
                }
            }
        }
    }

    if unresolved.is_empty() {
        return Ok(state_map);
    }

    // -------------------------------------------------------------------------
    // Pass 2a: Cache check — aggregates deleted in a newer segment need no WAL scan.
    // -------------------------------------------------------------------------
    {
        let mut cache = shard_mem_cache.borrow_mut();
        unresolved.retain(|key| {
            let Some(snapshot) = cache.get_aggregate_snapshot(key, CachePath::Read) else {
                return true; // still unresolved
            };
            if snapshot.status == AggregateStatus::Deleted && snapshot.log_id > target_log_id {
                state_map.insert(key.clone(), AggregateCompactionState::Deleted);
                return false; // resolved
            }
            true
        });
    }

    if unresolved.is_empty() {
        return Ok(state_map);
    }

    // -------------------------------------------------------------------------
    // Pass 2b: Reverse WAL scan from the active segment down to (and including)
    // the target segment.
    //
    // Scanning in reverse (newest → oldest) means post-recreation event batches
    // are seen before the SoftDelete that preceded them. When we encounter a
    // SoftDelete after already seeing live batches for the same key, the aggregate
    // was recreated: emit RecreatedAfter so only pre-deletion batches are dropped.
    //
    // Segments where no unresolved key appears in the bloom filter are skipped.
    // -------------------------------------------------------------------------
    let active_log_id = log_segments_cache.active_log_id();
    // Per-key: saw any EventBatch newer than the first SoftDelete we'll encounter.
    let mut seen_event_batch: HashSet<AggregateKey> = HashSet::new();
    // Per-key: most recent SoftTrim floor (first-seen in reverse = most recent).
    let mut trim_pending: HashMap<AggregateKey, u64> = HashMap::new();

    let mut current_log_id = active_log_id;
    loop {
        if unresolved.is_empty() {
            break;
        }

        let log_segment = log_segments_cache.get(current_log_id).await?;

        let Some((metablocks_end, bloom_hit)) = (|| {
            let metadata = log_segment.metadata.borrow();
            let read = metadata.read.as_ref()?;
            let bloom_hit = unresolved.iter().any(|k| read.aggregate_key_bloom.may_contain(k));
            Some((read.metablocks_position, bloom_hit))
        })() else {
            // No committed read cursor for this segment — skip it.
            if current_log_id <= target_log_id {
                break;
            }
            current_log_id -= 1;
            continue;
        };

        if bloom_hit {
            let metablocks_start = HEADER_BLOCK_SIZE_BYTES as u64;
            if metablocks_end > metablocks_start {
                let guard = log_segment
                    .lock_reader("compact_pass2b")
                    .await
                    .map_err(|_| CompactionError::LockTimeout)?;
                let dma_file = guard
                    .as_ref()
                    .ok_or(CompactionError::SegmentUnavailable { log_id: current_log_id })?;

                let scan_log_id = current_log_id;
                let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, CompactionError>(
                    dma_file,
                    true, // reverse
                    metablocks_start,
                    metablocks_end,
                    SCAN_CHUNK_SIZE,
                    |_pos, block| {
                        if unresolved.is_empty() {
                            return Ok(true); // early exit
                        }
                        let metablock = deserialise_metablock(block)
                            .map_err(CompactionError::MetablockDeserialise)?;

                        match metablock.wal_metablock_type {
                            MetablockKind::SoftDelete(sd) => {
                                if unresolved.contains(&sd.aggregate_key) {
                                    let state = if seen_event_batch.contains(&sd.aggregate_key) {
                                        // Live batches exist after this deletion → recreated.
                                        AggregateCompactionState::RecreatedAfter {
                                            deletion_wal_index: metablock.wal_index,
                                        }
                                    } else {
                                        AggregateCompactionState::Deleted
                                    };
                                    state_map.insert(sd.aggregate_key.clone(), state);
                                    unresolved.remove(&sd.aggregate_key);
                                    trim_pending.remove(&sd.aggregate_key);
                                }
                            }
                            MetablockKind::SoftTrim(st) => {
                                if unresolved.contains(&st.aggregate_key) {
                                    // First-seen in reverse = most recent trim floor.
                                    trim_pending
                                        .entry(st.aggregate_key.clone())
                                        .or_insert(st.keep_from_event_batch_index);
                                }
                            }
                            MetablockKind::EventBatchMetadata(eb) => {
                                if unresolved.contains(&eb.aggregate_key) {
                                    seen_event_batch.insert(eb.aggregate_key.clone());
                                }
                            }
                            _ => {}
                        }
                        Ok(false)
                    },
                )
                .await;

                match result {
                    Ok(_) => {}
                    Err(ReadVisitError::Visitor(e)) => return Err(e),
                    Err(ReadVisitError::Io(e)) => {
                        return Err(CompactionError::ReverseScan(ScanError::Io {
                            log_id: scan_log_id,
                            source: e.to_string(),
                        }));
                    }
                }
            }
        }

        if current_log_id <= target_log_id {
            break;
        }
        current_log_id -= 1;
    }

    // Finalize: keys with only a SoftTrim (no SoftDelete) → Trimmed.
    for (key, min_index) in trim_pending {
        state_map.insert(
            key,
            AggregateCompactionState::Trimmed {
                min_event_batch_index: min_index,
            },
        );
    }

    // Keys still not in state_map have no tombstone anywhere → Alive.
    for key in unresolved {
        state_map.entry(key).or_insert(AggregateCompactionState::Alive);
    }

    Ok(state_map)
}

/// Space accounting result from the estimation pass.
struct ReclaimableSpaceEstimate {
    /// Number of metablocks to keep (for exact file-size calculation).
    kept_metablock_count: u64,
    /// Total size of datablocks that belong to kept metablocks.
    kept_datablock_bytes: u64,
}

/// Decide whether a metablock should be kept or skipped during compaction.
///
/// Returns `true` when the metablock (and its datablock, if any) should be preserved in
/// the compacted file, `false` when it should be dropped.
fn should_keep_metablock(
    metablock: &celeriant_wal::metablocks::metablock::Metablock,
    state_map: &HashMap<AggregateKey, AggregateCompactionState>,
) -> bool {
    match &metablock.wal_metablock_type {
        MetablockKind::EventBatchMetadata(eb) => {
            match state_map.get(&eb.aggregate_key) {
                Some(AggregateCompactionState::Deleted) => false,
                Some(AggregateCompactionState::Trimmed { min_event_batch_index }) => {
                    eb.event_batch_index >= *min_event_batch_index
                }
                Some(AggregateCompactionState::RecreatedAfter { deletion_wal_index }) => {
                    metablock.wal_index > *deletion_wal_index
                }
                // Alive or missing from map (treat as alive).
                _ => true,
            }
        }
        // Tombstones: always keep (no datablock, tiny cost, required for cross-segment safety).
        MetablockKind::SoftDelete(_) | MetablockKind::SoftTrim(_) => true,
        // Schema registrations: always keep — immutable, cannot be regenerated.
        MetablockKind::SchemaRegistration(_) => true,
    }
}

/// Returns the size of a metablock's datablock in bytes, or 0 if there is no separate datablock.
fn datablock_size(metablock: &celeriant_wal::metablocks::metablock::Metablock) -> u64 {
    match &metablock.datablock {
        DatablockStorageKind::Block(_) => metablock.compressed_size,
        DatablockStorageKind::Inline(_) | DatablockStorageKind::None => 0,
    }
}

/// Forward-scan the target segment's metablocks and estimate how many bytes are reclaimable.
///
/// Returns `None` if the reclaimable fraction is below `min_reclaimable_ratio` (no compaction
/// needed), or the estimate otherwise.
async fn estimate_reclaimable_space(
    target_log_id: u64,
    log_segments_cache: &Rc<LogSegmentsCache>,
    state_map: &HashMap<AggregateKey, AggregateCompactionState>,
    min_reclaimable_ratio: f64,
) -> Result<Option<ReclaimableSpaceEstimate>, CompactionError> {
    let log_segment = log_segments_cache.get(target_log_id).await?;
    let metablocks_start = HEADER_BLOCK_SIZE_BYTES as u64;
    let metablocks_end = log_segment.metadata.borrow().readable_metablocks_end();

    if metablocks_end <= metablocks_start {
        // Empty segment — nothing to compact.
        return Ok(None);
    }

    let guard = log_segment
        .lock_reader("compact_estimate")
        .await
        .map_err(|_| CompactionError::LockTimeout)?;
    let src_file = guard
        .as_ref()
        .ok_or(CompactionError::SegmentUnavailable { log_id: target_log_id })?;

    let mut reclaimable_bytes: u64 = 0;
    let mut kept_bytes: u64 = 0;
    let mut kept_metablock_count: u64 = 0;
    let mut kept_datablock_bytes: u64 = 0;

    let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, CompactionError>(
        src_file,
        false,
        metablocks_start,
        metablocks_end,
        SCAN_CHUNK_SIZE,
        |_pos, block| {
            let metablock = deserialise_metablock(block)
                .map_err(CompactionError::MetablockDeserialise)?;

            let db_size = datablock_size(&metablock);

            if should_keep_metablock(&metablock, state_map) {
                kept_bytes += FIXED_BLOCK_SIZE_BYTES as u64 + db_size;
                kept_metablock_count += 1;
                kept_datablock_bytes += db_size;
            } else {
                reclaimable_bytes += FIXED_BLOCK_SIZE_BYTES as u64 + db_size;
            }

            Ok(false)
        },
    )
    .await;

    match result {
        Ok(_) => {}
        Err(ReadVisitError::Visitor(e)) => return Err(e),
        Err(ReadVisitError::Io(e)) => {
            return Err(CompactionError::ForwardScanIo {
                log_id: target_log_id,
                source: e.to_string(),
            });
        }
    }

    let total = reclaimable_bytes + kept_bytes;
    if total == 0 {
        return Ok(None);
    }

    let ratio = reclaimable_bytes as f64 / total as f64;
    if ratio < min_reclaimable_ratio {
        return Ok(None);
    }

    Ok(Some(ReclaimableSpaceEstimate {
        kept_metablock_count,
        kept_datablock_bytes,
    }))
}

/// Lightweight record of a datablock that must be copied from source to destination.
/// The corresponding metablock bytes are already written into the metablock DMA buffer
/// at offset `metablock_buf_offset` during the forward scan.
struct DatablockRef {
    /// Original position of the datablock in the source file.
    src_pos: u64,
    /// Size of the datablock in bytes.
    size: u64,
    /// New position of the datablock in the output file.
    new_pos: u64,
}

/// Build a compacted copy of `target_log_id` into a temp file.
///
/// The temp file is written to `{temp_dir}/log_{target_log_id}.compacting`. The caller
/// (Phase 3) is responsible for atomically renaming it over the original.
///
/// Returns `(temp_path, compacted_size_bytes)`.
async fn build_compacted_file(
    target_log_id: u64,
    log_segments_cache: &Rc<LogSegmentsCache>,
    state_map: &HashMap<AggregateKey, AggregateCompactionState>,
    estimate: &ReclaimableSpaceEstimate,
    temp_dir: &Path,
) -> Result<(PathBuf, u64), CompactionError> {
    // -------------------------------------------------------------------------
    // 1. Open the source segment for reading.
    //    We acquire the lock once and hold it across both the metablock scan
    //    (Pass A) and the datablock copy (Pass B). This avoids re-acquiring and
    //    releasing the reader lock between passes.
    //    Opening the source first lets us use its align_up() for the tail header
    //    position, matching shard_wal_sync.rs and handling 512-byte filesystems.
    // -------------------------------------------------------------------------
    let src_segment = log_segments_cache.get(target_log_id).await?;

    let (metablocks_end, original_tip_hash, original_wal_index) = {
        let meta = src_segment.metadata.borrow();
        (meta.readable_metablocks_end(), meta.write.tip_hash, meta.write.wal_index)
    };
    let metablocks_start = HEADER_BLOCK_SIZE_BYTES as u64;

    // Acquire reader lock once; held across Pass A and Pass B.
    let src_guard = src_segment
        .lock_reader("compact_build")
        .await
        .map_err(|_| CompactionError::LockTimeout)?;
    let src_file = src_guard
        .as_ref()
        .ok_or(CompactionError::SegmentUnavailable { log_id: target_log_id })?;

    // -------------------------------------------------------------------------
    // 2. Calculate exact file size and create the temp file.
    //
    // Use the source file's align_up() for the tail header position so the
    // alignment matches the actual filesystem block size (may be 512 bytes,
    // not necessarily 4096), consistent with shard_wal_sync.rs.
    // -------------------------------------------------------------------------
    let data_end = HEADER_BLOCK_SIZE_BYTES as u64
        + estimate.kept_metablock_count * FIXED_BLOCK_SIZE_BYTES as u64
        + estimate.kept_datablock_bytes;
    let tail_header_pos = src_file.align_up(data_end);
    let new_file_size = tail_header_pos + HEADER_BLOCK_SIZE_BYTES as u64;

    let temp_path = temp_dir.join(format!("log_{target_log_id}.compacting"));

    let new_file = create_file_dma(&temp_path, Some(new_file_size))
        .await
        .map_err(|e| CompactionError::CreateTempFile {
            path: temp_path.to_string_lossy().into_owned(),
            source: e.to_string(),
        })?;

    // -------------------------------------------------------------------------
    // 3. Allocate output buffers upfront.
    //
    //    Metablocks: one DMA buffer covering all kept metablocks — written in one
    //    `write_at` after the scan (matches shard_wal_sync.rs:302-346).
    //
    //    Datablocks: one DMA buffer covering the entire datablock region, aligned
    //    up to a DMA boundary. This avoids per-datablock alignment issues entirely:
    //    the buffer start is aligned (alloc_dma_buffer guarantees this) and the
    //    size is rounded up, so every write within the buffer lands correctly.
    // -------------------------------------------------------------------------
    let metablocks_buf_size = estimate.kept_metablock_count as usize * FIXED_BLOCK_SIZE_BYTES;

    // Datablock region: datablocks grow backward from the tail header position.
    // The region starts at `tail_header_pos - kept_datablock_bytes` and ends at `tail_header_pos`.
    let datablocks_region_end = tail_header_pos;
    let datablocks_region_start = datablocks_region_end - estimate.kept_datablock_bytes;

    // Align the write start down and the write end up so the single write_at is DMA-aligned.
    let aligned_db_write_start = new_file.align_down(datablocks_region_start);
    let aligned_db_write_end = new_file.align_up(datablocks_region_end);
    let datablocks_buf_size = (aligned_db_write_end - aligned_db_write_start) as usize;

    // Only allocate metablock buffer when there are kept metablocks.
    // Datablock buffer allocated only when there are kept datablocks.
    let mut mb_buf = if metablocks_buf_size > 0 {
        Some(new_file.alloc_dma_buffer(metablocks_buf_size))
    } else {
        None
    };

    let mut db_buf = if datablocks_buf_size > 0 {
        let mut buf = new_file.alloc_dma_buffer(datablocks_buf_size);
        buf.as_bytes_mut().fill(0);
        Some(buf)
    } else {
        None
    };

    // -------------------------------------------------------------------------
    // 4. Pass A: Forward-scan metablocks.
    //    - Serialize each kept metablock directly into mb_buf.
    //    - Record lightweight DatablockRef entries for datablocks that need copying.
    // -------------------------------------------------------------------------
    let mut datablock_refs: Vec<DatablockRef> = Vec::with_capacity(estimate.kept_metablock_count as usize);
    let mut bloom = AggregateKeyBloom::new();

    // Cursor walking backward through the datablock region (absolute file offsets).
    let mut new_datablocks_cursor: u64 = datablocks_region_end;
    // Offset within mb_buf for the next metablock.
    let mut mb_offset: usize = 0;

    if metablocks_end > metablocks_start {
        let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, CompactionError>(
            src_file,
            false,
            metablocks_start,
            metablocks_end,
            SCAN_CHUNK_SIZE,
            |_pos, block| {
                let mut metablock = deserialise_metablock(block)
                    .map_err(CompactionError::MetablockDeserialise)?;

                if !should_keep_metablock(&metablock, state_map) {
                    return Ok(false);
                }

                let db_bytes = datablock_size(&metablock);
                let src_db_pos = metablock.datablock_position;

                if db_bytes > 0 {
                    new_datablocks_cursor -= db_bytes;
                    metablock.datablock_position = new_datablocks_cursor;
                    datablock_refs.push(DatablockRef {
                        src_pos: src_db_pos,
                        size: db_bytes,
                        new_pos: new_datablocks_cursor,
                    });
                }

                // Serialize directly into the metablock DMA buffer at the current offset.
                let mb_slice = mb_buf
                    .as_mut()
                    .expect("mb_buf absent despite kept metablock count > 0");
                let slot = &mut mb_slice.as_bytes_mut()[mb_offset..mb_offset + FIXED_BLOCK_SIZE_BYTES];

                serialize_versioned_message(&metablock, WIRE_VERSION_WAL_METABLOCK, slot)
                    .map_err(|e| CompactionError::MetablockSerialise(e.to_string()))?;

                match &metablock.wal_metablock_type {
                    MetablockKind::EventBatchMetadata(eb) => bloom.insert(&eb.aggregate_key),
                    MetablockKind::SoftDelete(sd) => bloom.insert(&sd.aggregate_key),
                    MetablockKind::SoftTrim(st) => bloom.insert(&st.aggregate_key),
                    MetablockKind::SchemaRegistration(sr) => {
                        bloom.insert_hash(&sr.schema_key.hash_bytes())
                    }
                }

                mb_offset += FIXED_BLOCK_SIZE_BYTES;
                Ok(false)
            },
        )
        .await;

        match result {
            Ok(_) => {}
            Err(ReadVisitError::Visitor(e)) => return Err(e),
            Err(ReadVisitError::Io(e)) => {
                return Err(CompactionError::ForwardScanIo {
                    log_id: target_log_id,
                    source: e.to_string(),
                });
            }
        }
    }

    // -------------------------------------------------------------------------
    // 5. Pass B: Read each datablock from the source file and copy into db_buf.
    //    Then write db_buf in one shot.
    // -------------------------------------------------------------------------
    for dr in &datablock_refs {
        let data = src_file
            .read_at(dr.src_pos, dr.size as usize)
            .await
            .map_err(|e| CompactionError::ReadDatablock {
                log_id: target_log_id,
                position: dr.src_pos,
                source: e.to_string(),
            })?;

        // Copy into the correct offset within the DMA buffer.
        // `dr.new_pos` is the absolute file position; subtract the aligned write start
        // to get the buffer-relative offset.
        let buf_offset = (dr.new_pos - aligned_db_write_start) as usize;
        db_buf
            .as_mut()
            .expect("db_buf absent despite datablock_refs non-empty")
            .as_bytes_mut()[buf_offset..buf_offset + dr.size as usize]
            .copy_from_slice(&data);

        glommio::yield_if_needed().await;
    }

    // Release source file lock — no more reads needed.
    drop(src_guard);

    // -------------------------------------------------------------------------
    // 6. Write metablocks and datablocks to the new file (one syscall each).
    // -------------------------------------------------------------------------
    if let Some(buf) = mb_buf {
        new_file
            .write_at(buf, HEADER_BLOCK_SIZE_BYTES as u64)
            .await
            .map_err(|e| CompactionError::WriteFailed {
                step: "write_metablocks",
                source: e.to_string(),
            })?;
    }

    glommio::yield_if_needed().await;

    if let Some(buf) = db_buf {
        new_file
            .write_at(buf, aligned_db_write_start)
            .await
            .map_err(|e| CompactionError::WriteFailed {
                step: "write_datablocks",
                source: e.to_string(),
            })?;
    }

    // -------------------------------------------------------------------------
    // 7. Write dual headers and fdatasync.
    // -------------------------------------------------------------------------
    let final_metablocks_position =
        HEADER_BLOCK_SIZE_BYTES as u64 + estimate.kept_metablock_count * FIXED_BLOCK_SIZE_BYTES as u64;
    // `new_datablocks_cursor` now points to the start of the lowest datablock written.
    // If no datablocks were kept it equals `datablocks_region_end`.
    let final_datablocks_position = new_datablocks_cursor;

    // Preserve the original tip_hash and wal_index from before compaction rather than
    // recomputing them from surviving metablocks. This is intentional: the segment was
    // fully replicated before compaction (enforced by the is_pending_advance guard), so
    // the hash chain and wal_index represent already-verified state. The compacted content
    // no longer chains to this tip_hash — correctness is maintained by metablocks_position.
    let header = ShardLogHeader {
        metablocks_position: final_metablocks_position,
        datablocks_position: final_datablocks_position,
        wal_index: original_wal_index,
        tip_hash: original_tip_hash,
        aggregate_bloom: bloom.to_bytes(),
    };

    write_dual_shard_log_header(&new_file, tail_header_pos, &header)
        .await
        .map_err(CompactionError::WriteHeader)?;

    new_file
        .fdatasync()
        .await
        .map_err(|e| CompactionError::WriteFailed {
            step: "fdatasync",
            source: e.to_string(),
        })?;

    glommio::yield_if_needed().await;

    new_file
        .close()
        .await
        .map_err(|e| CompactionError::WriteFailed {
            step: "close",
            source: e.to_string(),
        })?;

    Ok((temp_path, new_file_size))
}

/// Result of a completed compaction: sizes before and after the atomic swap.
pub struct CompactionResult {
    pub log_id: u64,
    pub original_size: u64,
    pub compacted_size: u64,
}

/// Atomically swap the compacted temp file over the original segment file.
///
/// Steps:
///   1. Evict the segment from the LRU cache (drops the cache's `Rc`, closing the FD when the
///      last reference drops — in-flight reads on the old inode continue unaffected on Linux).
///   2. `std::fs::rename(temp_path, original_path)` — atomic on Linux when same filesystem.
///   3. The next `log_segments_cache.get(log_id)` will open the new file and read its header.
fn atomic_swap_segment(
    log_id: u64,
    temp_path: &Path,
    log_segments_cache: &Rc<LogSegmentsCache>,
) -> Result<(), CompactionError> {
    // Evict before rename so the cache's reference drops promptly.
    log_segments_cache.evict_from_lru(log_id);

    let original_path = log_segments_cache.shard_dir().join(format!("log_{log_id}.wal"));

    std::fs::rename(temp_path, &original_path).map_err(|e| CompactionError::AtomicSwap {
        temp_path: temp_path.to_string_lossy().into_owned(),
        target_path: original_path.to_string_lossy().into_owned(),
        source: e.to_string(),
    })
}

/// Top-level compaction entry point for a sealed log segment.
///
/// Orchestrates: resolve aggregate states → estimate reclaimable space → build compacted file →
/// atomic swap. Returns `Some(CompactionResult)` if compaction ran, `None` if the segment does
/// not meet the reclaimable threshold.
pub async fn compact_segment(
    target_log_id: u64,
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    min_reclaimable_ratio: f64,
    temp_dir: &Path,
) -> Result<Option<CompactionResult>, CompactionError> {
    // Guard: segment must be fully replicated (read == write) before compacting.
    // If write is ahead of read, the segment has uncommitted data — not eligible yet.
    {
        let segment = log_segments_cache.get(target_log_id).await?;
        if segment.metadata.borrow().is_pending_advance() {
            return Ok(None);
        }
    }

    let state_map = resolve_aggregate_states(target_log_id, log_segments_cache, shard_mem_cache).await?;

    glommio::yield_if_needed().await;

    let Some(estimate) = estimate_reclaimable_space(
        target_log_id,
        log_segments_cache,
        &state_map,
        min_reclaimable_ratio,
    )
    .await?
    else {
        return Ok(None);
    };

    glommio::yield_if_needed().await;

    // Record the original file size before building the new file.
    let original_size = {
        let segment = log_segments_cache.get(target_log_id).await?;
        segment.metadata.borrow().file_len
    };

    let (temp_path, compacted_size) = build_compacted_file(
        target_log_id,
        log_segments_cache,
        &state_map,
        &estimate,
        temp_dir,
    )
    .await?;

    atomic_swap_segment(target_log_id, &temp_path, log_segments_cache)?;

    Ok(Some(CompactionResult {
        log_id: target_log_id,
        original_size,
        compacted_size,
    }))
}

/// Delete any orphaned `.compacting` temp files left by a previous crashed compaction.
///
/// Called during startup (before the async executor starts) to ensure no stale temp files
/// accumulate. Each `.compacting` file is an incomplete compaction — the original segment
/// file is intact and takes precedence.
pub fn cleanup_orphaned_compacting_files(temp_dir: &Path) -> Result<(), CompactionError> {
    let entries = match std::fs::read_dir(temp_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(CompactionError::CleanupFailed {
                path: temp_dir.to_string_lossy().into_owned(),
                source: e.to_string(),
            })
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                warn!(path = %temp_dir.display(), error = %e, "failed to read compaction temp dir entry, skipping");
                continue;
            }
        };

        let path = entry.path();
        let is_compacting = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with(".compacting"))
            .unwrap_or(false);

        if !is_compacting {
            continue;
        }

        info!(path = %path.display(), "removing orphaned compaction temp file");

        if let Err(e) = std::fs::remove_file(&path) {
            warn!(path = %path.display(), error = %e, "failed to remove orphaned compaction temp file, skipping");
        }
    }

    Ok(())
}
