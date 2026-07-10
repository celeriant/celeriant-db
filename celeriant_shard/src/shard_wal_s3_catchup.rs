use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Duration;

use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::paths::{fallback_shard_prefix, parse_fallback_path};
use celeriant_msg::request::requests::ReplicationBatchItem;
use celeriant_rotating_log::log_segment_file::log_segment_file::{read_datablocks_carry_over_bytes, write_dual_shard_log_header};
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_rotating_log::reverse_metablock_scanner::ReverseMetablockScanner;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK};
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wal::s3::fallback_batch::FallbackBatch;
use celeriant_wire::disk::serialised_datablock::{CompressionPolicy, SerialisedDatablock};
use celeriant_wire::disk::versioned_block::{deserialise_fallback_batch, deserialise_metablock, serialize_versioned_message};

use crate::schema_validator::CompiledValidator;
use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
use celeriant_memcache::shard_mem_cache::ShardMemCache;

type MemCache = ShardMemCache<CompiledValidator>;
use celeriant_watch::aggregate_watchers::AggregateWatchers;

use crate::amortisation::coordinator::Coordinator;
use crate::error::apply_batch_error::ApplyBatchError;
use crate::error::s3_catchup_error::S3CatchupError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::s3_downloader::{S3Downloader};
use crate::shard_wal_sync::{capture_fsync_snapshot, commit_fsync_with_rollback, compute_entry_hash, CommitTarget};
use crate::watch_event_collector::WatchEventCollector;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchupCompletion {
    Caught,
    Retry,
}

#[derive(Debug, Clone)]
pub struct S3CatchupResult {
    pub batches_applied: u64,
    pub bytes_downloaded: u64,
    pub rounds: u32,
    pub completion: CatchupCompletion,
}

struct FallbackBatchRef {
    path: String,
    start_wal_seq: u64,
    node_id: u128,
}

fn hex_short(h: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(16);
    for b in &h[..8] { write!(s, "{:02x}", b).unwrap(); }
    s
}

/// Keep only batches uploaded by the peer node. Filters out self-uploaded
/// batches (leader must not consume its own fallback) AND batches from
/// unknown node_ids (stale data from a previous cluster generation).
fn retain_peer_batches(batches: Vec<FallbackBatchRef>, self_node_id: u128, peer_node_id: Option<u128>) -> Vec<FallbackBatchRef> {
    batches
        .into_iter()
        .filter(|b| {
            if b.node_id == self_node_id {
                return false;
            }
            match peer_node_id {
                Some(peer) => b.node_id == peer,
                None => true, // no peer known yet - accept all non-self batches
            }
        })
        .collect()
}

struct CatchupCandidate {
    path: String,
    size: u64,
    start_wal_seq: u64,
    end_wal_seq: u64,
}

// Bounded settle window for the promotion-catchup drain barrier.
//
// After winning the S3 lease (CAS), the predecessor is fenced for future uploads.
// However, uploads that were in-flight BEFORE the CAS may still land or become
// list-visible after our final list. The drain re-lists up to this many times
// with a short sleep between each, waiting for S3 list visibility to stabilise.
//
// Production: DRAIN_MAX_ROUNDS × DRAIN_SETTLE_INTERVAL = 5 × 500ms = 2.5s maximum wait.
// Justified by: S3 PUT round-trip is typically <1s; predecessor is fenced so
// its pre-fence set is finite and drains quickly. The wait is one-shot per
// catchup completion, not a hot path.
//
// In tests the interval is collapsed to 1ms so the drain logic is exercised
// without adding seconds to every test that applies a batch.
#[cfg(not(test))]
const DRAIN_MAX_ROUNDS: u32 = 5;
#[cfg(test)]
const DRAIN_MAX_ROUNDS: u32 = 3;

#[cfg(not(test))]
const DRAIN_SETTLE_INTERVAL: Duration = Duration::from_millis(500);
#[cfg(test)]
const DRAIN_SETTLE_INTERVAL: Duration = Duration::from_millis(1);

/// Re-list S3 up to `DRAIN_MAX_ROUNDS` times with `DRAIN_SETTLE_INTERVAL` sleep
/// between attempts, looking for late-landing covering files not yet in `processed_paths`.
///
/// Returns `true` if at least one new covering candidate was found (caller should
/// continue the main loop to apply it). Returns `false` if the settle window passed
/// with no new covering candidates (safe to declare `Caught`).
///
/// Logs and counts each late file caught. Also logs the drain outcome summary.
async fn drain_settle_barrier<D: S3Downloader + 'static>(
    downloader: &Rc<D>,
    prefix: &str,
    shard_id: u32,
    node_id: u128,
    peer_node_id: Option<u128>,
    next_wal_seq: u64,
    processed_paths: &HashSet<String>,
) -> Result<bool, S3CatchupError> {
    let mut late_files_caught: u32 = 0;

    for round in 0..DRAIN_MAX_ROUNDS {
        glommio::timer::sleep(DRAIN_SETTLE_INTERVAL).await;

        let objects = downloader.list_objects(prefix).await?;

        for obj in &objects {
            let Some((_sid, start_wal_seq, end_wal_seq, source_node_id)) = parse_fallback_path(&obj.path) else {
                continue;
            };
            if source_node_id == node_id {
                continue;
            }
            if let Some(peer) = peer_node_id {
                if source_node_id != peer {
                    continue;
                }
            }
            // Only a file that actually covers the gap can advance us. A file
            // strictly ahead of an unfilled next_wal_seq is not a bridging
            // predecessor and must not hold the barrier open.
            if start_wal_seq <= next_wal_seq && end_wal_seq >= next_wal_seq && !processed_paths.contains(&obj.path) {
                tracing::warn!(
                    shard_id,
                    path = %obj.path,
                    late_wal_seq = end_wal_seq,
                    drain_round = round,
                    "promotion catchup drain caught a late predecessor S3 file"
                );
                metrics::counter!(
                    "celeriant_catchup_drain_late_files_total",
                    "shard_id" => shard_id.to_string()
                ).increment(1);
                late_files_caught += 1;
            }
        }

        if late_files_caught > 0 {
            tracing::info!(
                shard_id,
                drain_rounds = round + 1,
                late_files_caught,
                next_wal_seq,
                "promotion catchup drain: late files found, continuing catchup"
            );
            return Ok(true);
        }
    }

    tracing::info!(
        shard_id,
        drain_rounds = DRAIN_MAX_ROUNDS,
        late_files_caught = 0u32,
        next_wal_seq,
        "promotion catchup drain: settle window stable, declaring Caught"
    );
    Ok(false)
}

/// A batch is contiguous when every adjacent pair of items differs by exactly one in wal_seq.
fn is_batch_contiguous(batch: &FallbackBatch) -> bool {
    batch.items.windows(2).all(|w| w[0].metablock.wal_seq + 1 == w[1].metablock.wal_seq)
}

fn is_batch_lease_consistent(batch: &FallbackBatch) -> bool {
    batch.items.iter().all(|i| i.metablock.lease_epoch == batch.lease_epoch)
}

pub(crate) async fn catchup_from_s3<D: S3Downloader + 'static>(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    downloader: &Rc<D>,
    shard_id: u32,
    node_id: u128,
    peer_node_id: Option<u128>,
    max_catchup_gap_bytes: Option<u64>,
    dict_codec: Rc<DictCodec>,
) -> Result<S3CatchupResult, S3CatchupError> {
    let mut result = S3CatchupResult {
        batches_applied: 0,
        bytes_downloaded: 0,
        rounds: 0,
        completion: CatchupCompletion::Retry,
    };
    let current_wal_seq = {
        let active = log_segments_cache.active();
        active.metadata.borrow().write.wal_seq
    };
    let mut next_wal_seq = current_wal_seq + 1;

    // List all available files we can work with
    let prefix = fallback_shard_prefix(shard_id);

    let mut first_iteration = true;
    let mut processed_paths: HashSet<String> = HashSet::new();

    loop {
        result.rounds += 1;
        // Count at execution, not at clean exit: a round that truncates then returns
        // early via `?` (replacement chain not yet in S3, handed to TCP) still ran.
        metrics::counter!("celeriant_s3_catchup_rounds_total").increment(1);

        let inner_applied = result.batches_applied;
        let mut truncated = false;
        let mut entries: HashMap<u64, Vec<CatchupCandidate>> = HashMap::new();

        // Filter and order stage. not reading from s3 yet.
        for obj in downloader.list_objects(&prefix).await? {
            // Parse out file name
            let Some((_sid, start_wal_seq, end_wal_seq, source_node_id)) = parse_fallback_path(&obj.path) else {
                continue;
            };

            // Skip our own uploads. Removing this filter caused a truncate
            // versus self-apply loop in chaos.
            if source_node_id == node_id {
                metrics::counter!(
                    "celeriant_s3_catchup_self_uploads_seen_total",
                    "shard_id" => shard_id.to_string()
                ).increment(1);
                continue;
            }

            if let Some(peer) = peer_node_id {
                if source_node_id != peer {
                    continue;
                }
            }

            // Add to set with start index as key
            entries.entry(start_wal_seq).or_default().push(CatchupCandidate {
                path: obj.path,
                size: obj.size,
                start_wal_seq,
                end_wal_seq,
            });
        }

        let remaining_bytes: u64 = entries
            .values()
            .flatten()
            .filter(|c| c.end_wal_seq >= next_wal_seq)
            .map(|c| c.size)
            .sum();

        if !first_iteration && max_catchup_gap_bytes.map_or(true, |cap| remaining_bytes < cap) {
            // Run the drain barrier before declaring Caught, but only if we applied
            // at least one batch this invocation. If batches_applied==0, there are no
            // predecessor uploads to drain (no predecessor chain was established with us).
            if result.batches_applied > 0 {
                // A predecessor's in-flight uploads may still be landing / becoming
                // list-visible after our final list. The predecessor is fenced (we won the
                // CAS), so this set is finite and drains within the settle window.
                let late_found = drain_settle_barrier(downloader, &prefix, shard_id, node_id, peer_node_id, next_wal_seq, &processed_paths).await?;
                if late_found {
                    // Late files found — outer loop re-lists and applies them.
                    first_iteration = true;
                    continue;
                }
            }
            result.completion = CatchupCompletion::Caught;
            break;
        }

        loop {
            // Gather every candidate that could supply next_wal_seq (start <= next_wal_seq
            // <= end). Download each and deserialize. Batches that fail contiguity checks are
            // dropped (leader-side rollback cascade artefacts).
            let covering: Vec<&CatchupCandidate> = entries
                .values()
                .flatten()
                .filter(|c| c.start_wal_seq <= next_wal_seq && c.end_wal_seq >= next_wal_seq)
                .filter(|c| !processed_paths.contains(&c.path))
                .collect();

            if covering.is_empty() {
                break;
            }

            let mut downloaded: Vec<(FallbackBatch, &CatchupCandidate)> = Vec::new();
            for candidate in covering {
                let data = downloader.download(&candidate.path).await?;
                result.bytes_downloaded += data.len() as u64;

                let batch = match deserialise_fallback_batch(&data) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(shard_id, path = %candidate.path, error = ?e, "skipping corrupt S3 batch");
                        processed_paths.insert(candidate.path.clone());
                        continue;
                    }
                };

                // Skip batches with internal wal_seq gaps - observed under cascades of
                // leader rollback + re-upload where items from two chain generations get
                // stitched into a single file. Never safe to apply.
                if !is_batch_contiguous(&batch) {
                    tracing::warn!(shard_id, path = %candidate.path, "skipping S3 batch with internal wal_seq gap");
                    processed_paths.insert(candidate.path.clone());
                    continue;
                }

                if !is_batch_lease_consistent(&batch) {
                    tracing::warn!(shard_id, path = %candidate.path, batch_lease = batch.lease_epoch, "skipping S3 batch with inconsistent per-item lease_epoch");
                    processed_paths.insert(candidate.path.clone());
                    continue;
                }

                downloaded.push((batch, candidate));
            }

            // Dedupe per start_wal_seq by (lease_epoch, upload_sequence) lex. lease_epoch is
            // globally monotonic via S3 CAS election; upload_sequence is per-process and resets
            // on handover, so it is only a within-lease tiebreaker.
            let mut by_start: HashMap<u64, (FallbackBatch, &CatchupCandidate)> = HashMap::new();
            for (batch, cand) in downloaded {
                let start = cand.start_wal_seq;
                let key = (batch.lease_epoch, batch.upload_sequence);
                // Same-epoch batches at one start must be byte-identical, which is what lets the
                // restart-resettable upload_sequence be a safe within-epoch tiebreaker. Divergent
                // content at the same epoch means a re-author fork slipped the cull-skip; surface it.
                if let Some((existing, _)) = by_start.get(&start) {
                    if same_epoch_content_divergence(existing, &batch) {
                        tracing::error!(
                            shard_id, start, lease_epoch = batch.lease_epoch,
                            "same-(lease_epoch,wal_seq) S3 batches with divergent content — content-immutability invariant violated (cull-skip regression?)"
                        );
                        metrics::counter!(
                            "celeriant_s3_catchup_same_epoch_divergence_total",
                            "shard_id" => shard_id.to_string()
                        ).increment(1);
                    }
                }
                let replace = match by_start.get(&start) {
                    Some((prev, _)) => key > (prev.lease_epoch, prev.upload_sequence),
                    None => true,
                };
                if replace {
                    if let Some((prev_batch, prev_cand)) = by_start.insert(start, (batch, cand)) {
                        tracing::debug!(shard_id, path = %prev_cand.path, stale_lease = prev_batch.lease_epoch, stale_seq = prev_batch.upload_sequence, "excluded stale same-start batch");
                    }
                } else if let Some((winner_batch, _)) = by_start.get(&start) {
                    tracing::debug!(shard_id, path = %cand.path, stale_lease = batch.lease_epoch, stale_seq = batch.upload_sequence, winner_lease = winner_batch.lease_epoch, winner_seq = winner_batch.upload_sequence, "excluded stale same-start batch");
                }
            }

            let best = by_start.into_values().max_by_key(|(batch, _)| (batch.lease_epoch, batch.upload_sequence));

            let (batch, candidate) = match best {
                Some(b) => b,
                None => break, // all candidates were corrupt
            };

            // Skip already-applied entries within the batch (partial overlap)
            let current_wal = log_segments_cache.active().metadata.borrow().write.wal_seq;
            let current_tip = log_segments_cache.active().metadata.borrow().write.tip_hash;

            let all_items: Vec<ReplicationBatchItem> = batch
                .items
                .into_iter()
                .map(|fi| ReplicationBatchItem {
                    metablock: fi.metablock,
                    datablock: fi.datablock,
                })
                .collect();

            let skip = all_items
                .iter()
                .position(|item| item.metablock.wal_seq > current_wal)
                .unwrap_or(all_items.len());
            let items = &all_items[skip..];

            if items.is_empty() {
                let new_next = candidate.end_wal_seq + 1;
                if new_next <= next_wal_seq {
                    break; // candidate can't advance us, stop inner loop
                }
                next_wal_seq = new_next;
                continue;
            }

            // Hash chain check: does this batch connect to our local tip?
            if items[0].metablock.previous_tip_hash != current_tip {
                // Local WAL diverged from S3. Find the common ancestor using the
                // already-downloaded batch first (no extra I/O); fall back to
                // scanning earlier S3 batches if the triggering batch doesn't
                // overlap our local data.
                let divergence = match find_divergence_from_batch(log_segments_cache, &all_items).await {
                    Ok(r) => Some(r),
                    Err(_) => find_divergence_via_s3(log_segments_cache, downloader, &prefix, current_wal, node_id, peer_node_id).await.ok(),
                };
                let (ancestor_hash, ancestor_log_id, divergent_wal_seq, divergent_position) = match divergence {
                    Some(r) => r,
                    None => {
                        // No common ancestor in any S3 batch we can see right now.
                        // Could be transient (new leader hasn't uploaded enough yet) or
                        // structural (operator-territory). Either way: not safe to
                        // truncate, not fatal — skip this batch and let the loop bail to
                        // Retry. The caller's retry will re-list S3 next round.
                        let batch_first_wal = all_items.first().map(|i| i.metablock.wal_seq).unwrap_or(0);
                        let batch_first_prev_hash = all_items.first().map(|i| hex_short(&i.metablock.previous_tip_hash)).unwrap_or_default();
                        // Post-skip values — the chain check uses items[0], not all_items[0].
                        let mismatch_wal = items.first().map(|i| i.metablock.wal_seq).unwrap_or(0);
                        let mismatch_prev_hash = items.first().map(|i| hex_short(&i.metablock.previous_tip_hash)).unwrap_or_default();
                        // Re-fetch live state at warn-time. `current_wal`/`current_tip`
                        // captured earlier may be stale because we awaited S3 downloads
                        // and other futures can run on this executor in between.
                        let local_active_log_id = log_segments_cache.active_log_id();
                        let (live_write_wal, live_write_tip, local_read_wal, local_read_tip_hash) = {
                            let active = log_segments_cache.active();
                            let m = active.metadata.borrow();
                            let (rw, rt) = match &m.read {
                                Some(r) => (r.wal_seq, hex_short(&r.tip_hash)),
                                None => (0u64, "none".to_string()),
                            };
                            (m.write.wal_seq, hex_short(&m.write.tip_hash), rw, rt)
                        };
                        tracing::warn!(
                            shard_id,
                            stale_current_wal = current_wal,
                            stale_current_tip = %hex_short(&current_tip),
                            local_active_log_id,
                            live_write_wal,
                            live_write_tip = %live_write_tip,
                            local_read_wal,
                            local_read_tip_hash = %local_read_tip_hash,
                            batch_first_wal,
                            batch_first_prev_hash = %batch_first_prev_hash,
                            mismatch_wal,
                            mismatch_prev_hash = %mismatch_prev_hash,
                            batch_lease = batch.lease_epoch,
                            batch_seq = batch.upload_sequence,
                            path = %candidate.path,
                            "Chain mismatch with no common ancestor — skipping batch, will retry catchup"
                        );
                        metrics::counter!(
                            "celeriant_s3_catchup_no_common_ancestor_total",
                            "shard_id" => shard_id.to_string()
                        ).increment(1);
                        processed_paths.insert(candidate.path.clone());
                        break;
                    }
                };

                tracing::warn!(
                    shard_id,
                    current_wal,
                    ancestor_log_id,
                    divergent_wal_seq,
                    "TipHashMismatch - truncating divergent WAL entries"
                );
                truncate_wal(
                    log_segments_cache,
                    shard_mem_cache,
                    fsync_coordinator,
                    watched_aggregates,
                    &dict_codec,
                    ancestor_hash,
                    ancestor_log_id,
                    divergent_wal_seq,
                    divergent_position,
                )
                .await
                .map_err(S3CatchupError::TruncationFailed)?;

                // truncate_wal emptied the parked queue (surviving prefix committed,
                // rest discarded); same stale-gauge hazard as the sync_applied_batch
                // drain above.
                metrics::gauge!("celeriant_parked_commit_queue_depth", "shard_id" => shard_id.to_string()).set(0.0);

                // Break inner loop so the outer loop re-lists S3 and retries
                // from the truncated state with a fresh view of candidates.
                truncated = true;
                next_wal_seq = log_segments_cache.active().metadata.borrow().write.wal_seq + 1;
                break;
            }

            // Apply the winner to the WAL
            apply_external_batch(log_segments_cache, shard_mem_cache, items, &dict_codec).map_err(S3CatchupError::ApplyFailed)?;

            sync_applied_batch(log_segments_cache, shard_mem_cache, fsync_coordinator, watched_aggregates, shard_id, dict_codec.clone())
                .await
                .map_err(S3CatchupError::FsyncFailed)?;

            processed_paths.insert(candidate.path.clone());

            let shard_label = [("shard_id", shard_id.to_string())];
            let applied_bytes: u64 = items.iter().map(|i| i.metablock.uncompressed_size).sum();
            metrics::counter!("celeriant_replication_applied_events_total", &shard_label).increment(items.len() as u64);
            metrics::counter!("celeriant_replication_applied_bytes_total", &shard_label).increment(applied_bytes);

            result.batches_applied += 1;

            // What's our next position to pull down
            next_wal_seq = candidate.end_wal_seq + 1;
        }

        if !truncated && result.batches_applied == inner_applied {
            // No progress this iteration. If the remaining gap is small,
            // TCP replication can bridge it once we exit catchup. Otherwise
            // leave the default Retry; leader is likely still uploading.
            if max_catchup_gap_bytes.map_or(true, |cap| remaining_bytes < cap) {
                // Run the drain barrier before declaring Caught, but only if we applied
                // at least one batch this invocation (same rationale as the pre-inner-loop
                // check above: zero applied means no predecessor stream to drain).
                if result.batches_applied > 0 {
                    let late_found = drain_settle_barrier(downloader, &prefix, shard_id, node_id, peer_node_id, next_wal_seq, &processed_paths).await?;
                    if late_found {
                        first_iteration = true;
                        continue;
                    }
                }
                result.completion = CatchupCompletion::Caught;
                break;
            }
            break;
        }

        // After a truncation, force the next round to attempt apply regardless
        // of remaining_bytes: the replacement chain covering the truncated range
        // typically fits well under max_catchup_gap_bytes, and the pre-inner
        // bailout would otherwise declare "fully caught up" without ever trying.
        first_iteration = truncated;
    }

    Ok(result)
}

/// Validate WAL continuity and queue entries. Does not fsync.
///
/// Handles mid-batch resume: if the batch contains entries at or before the
/// current WAL sequence (from a previous partial application that crashed before
/// completing the full batch), those entries are skipped and application
/// resumes from `current_wal_seq + 1`.
pub(crate) fn apply_external_batch(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    items: &[ReplicationBatchItem],
    dict_codec: &DictCodec,
) -> Result<(), ApplyBatchError> {
    let (current_tip_hash, current_wal_seq) = {
        let active = log_segments_cache.active();
        let metadata = active.metadata.borrow();
        (metadata.write.tip_hash, metadata.write.wal_seq)
    };

    // Skip entries already applied (mid-batch resume after crash)
    let skip = items
        .iter()
        .position(|item| item.metablock.wal_seq > current_wal_seq)
        .unwrap_or(items.len());
    let items = &items[skip..];

    if items.is_empty() {
        return Ok(());
    }

    let batch_wal_seq = items[0].metablock.wal_seq;
    let batch_tip_hash = items[0].metablock.previous_tip_hash;

    if current_wal_seq.saturating_add(1) != batch_wal_seq {
        return Err(ApplyBatchError::WalSeqMismatch {
            current: current_wal_seq,
            batch_first: batch_wal_seq,
        });
    }
    if current_tip_hash != batch_tip_hash {
        return Err(ApplyBatchError::TipHashMismatch {
            current: current_tip_hash,
            current_wal_seq,
            batch: batch_tip_hash,
            batch_wal_seq,
        });
    }

    queue_replicated_entries(shard_mem_cache, items, dict_codec)
}

fn queue_replicated_entries(shard_mem_cache: &Rc<RefCell<MemCache>>, items: &[ReplicationBatchItem], dict_codec: &DictCodec) -> Result<(), ApplyBatchError> {
    for (i, w) in items.windows(2).enumerate() {
        if w[0].metablock.wal_seq + 1 != w[1].metablock.wal_seq {
            return Err(ApplyBatchError::BatchWalSeqGap {
                index: i + 1,
                expected: w[0].metablock.wal_seq + 1,
                actual: w[1].metablock.wal_seq,
            });
        }
    }

    let mut prepared = Vec::with_capacity(items.len());

    for item in items {
        let (datablock_bytes, datablock) = match &item.metablock.datablock {
            DatablockStorageKind::None | DatablockStorageKind::Inline(_) => (None, None),
            DatablockStorageKind::Block(_) => {
                if let Some(datablock) = &item.datablock {
                    let compression_type = CompressionType::from_byte(item.metablock.datablock_compression_type)
                        .map_err(|b| ApplyBatchError::SerialiseDatablocks(
                            celeriant_wire::codec::codec_error::CodecError::Compression(
                                format!("unknown compression byte {b}")
                            )
                        ))?;
                    let serialized = SerialisedDatablock::new(datablock, CompressionPolicy::Fixed(compression_type), dict_codec).map_err(ApplyBatchError::SerialiseDatablocks)?;
                    let external_data = serialized.external_data.ok_or(ApplyBatchError::BlockBecameInline)?;
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
    dict_codec: Rc<DictCodec>,
) -> Result<(), ShardFsyncError> {
    // Catchup full-commits on apply, so its read cursor jumps to the new write
    // tip. Any live-TCP commits still parked cover entries below that tip on the
    // same chain (the batch chain-validated onto them): commit them first so the
    // read cursor stays monotonic and their watch events fire exactly once.
    let parked = shard_mem_cache.borrow_mut().take_all_parked_commits();
    for pcd in parked {
        crate::shard_wal_replicate::commit_pcd(
            log_segments_cache, shard_mem_cache, watched_aggregates, pcd, Some(&dict_codec),
        );
    }
    // The deferred-commit path owns this gauge but only refreshes it on its own
    // fsyncs/drains; without this reset an idle shard displays the pre-catchup
    // depth forever — a permanent false drain-leak alarm.
    metrics::gauge!("celeriant_parked_commit_queue_depth", "shard_id" => shard_id.to_string()).set(0.0);

    let lsc = log_segments_cache.clone();
    let smc = shard_mem_cache.clone();
    let wa = watched_aggregates.clone();
    let mc_capture = smc.clone();

    // Standalone in offline-catchup mode: advance read position immediately (no follower replication).
    fsync_coordinator
        .request_sync_two_phase(
            None,
            ShardFsyncError::WriteLockTimeout,
            move || capture_fsync_snapshot(&mc_capture),
            move |captured| commit_fsync_with_rollback(NodeStatus::Standalone, CommitTarget::FullCommit, lsc, smc, wa, captured, shard_id, dict_codec),
        )
        .await
}

/// Two same-start batches sharing a lease epoch must agree over their overlap.
/// Returns true only if the chains actually diverge there — a same-epoch re-author
/// fork. A different extent is not divergence: a shorter batch can be a clean prefix
/// of a longer retry. Items are contiguous from the shared start, so zipping lines
/// up matching seqs.
fn same_epoch_content_divergence(a: &FallbackBatch, b: &FallbackBatch) -> bool {
    if a.lease_epoch != b.lease_epoch {
        return false;
    }
    a.items.iter().zip(b.items.iter()).any(|(ia, ib)| {
        ia.metablock.wal_seq != ib.metablock.wal_seq
            || ia.metablock.previous_tip_hash != ib.metablock.previous_tip_hash
    })
}

/// Find the common ancestor using the already-downloaded batch from catchup_round.
///
/// The batch that triggered TipHashMismatch often overlaps local WAL entries
/// (items were skipped because wal_seq <= current). The earliest item's
/// `previous_tip_hash` points to the state before any of the remote leader's
/// writes in that range - the common ancestor. This avoids any additional S3 calls.
async fn find_divergence_from_batch(
    log_segments_cache: &Rc<LogSegmentsCache>,
    batch_items: &[ReplicationBatchItem],
) -> Result<([u8; 32], u64, u64, u64), S3CatchupError> {
    let candidate_hash = batch_items
        .first()
        .ok_or_else(|| S3CatchupError::TruncationFailed(ShardFsyncError::MetablockSerialisationError("empty batch".into())))?
        .metablock
        .previous_tip_hash;

    let mut set = HashSet::with_capacity(1);
    set.insert(candidate_hash);
    // Single-batch fast path: no floor available, miss falls through to find_divergence_via_s3.
    let (hash, log_id, wal_seq, position) = scan_local_metablocks_for_hashes(log_segments_cache, &set, 0).await?;

    // Walk the batch forward, advancing past any byte-identical prefix.
    let metablock_refs: Vec<&_> = batch_items.iter().map(|i| &i.metablock).collect();
    let refined = refine_divergence_by_byte_match(
        log_segments_cache, &metablock_refs, hash, log_id, wal_seq, position,
    ).await;
    if refined.2 != wal_seq {
        tracing::info!(
            from_wal_seq = wal_seq,
            to_wal_seq = refined.2,
            advanced = refined.2 - wal_seq,
            "find_divergence_from_batch: refined divergent_wal_seq past byte-identical prefix"
        );
    }
    Ok(refined)
}

/// Fallback: download earlier S3 batches in parallel, scan local WAL once against
/// the union of their previous_tip_hashes. Scan floor is `min(candidate.start) - 1`.
async fn find_divergence_via_s3<D: S3Downloader + 'static>(
    log_segments_cache: &Rc<LogSegmentsCache>,
    downloader: &Rc<D>,
    prefix: &str,
    current_wal_seq: u64,
    node_id: u128,
    peer_node_id: Option<u128>,
) -> Result<([u8; 32], u64, u64, u64), S3CatchupError> {
    let objects = downloader.list_objects(prefix).await?;

    let earlier_batches: Vec<FallbackBatchRef> = objects
        .into_iter()
        .filter_map(|obj| {
            let (_sid, start, _end, nid) = parse_fallback_path(&obj.path)?;
            Some(FallbackBatchRef {
                path: obj.path,
                start_wal_seq: start,
                node_id: nid,
            })
        })
        .filter(|b| b.start_wal_seq <= current_wal_seq)
        .collect();

    let earlier_total_unfiltered = earlier_batches.len();
    let mut earlier_batches = retain_peer_batches(earlier_batches, node_id, peer_node_id);
    let earlier_total_after_peer_filter = earlier_batches.len();

    earlier_batches.sort_by(|a, b| b.start_wal_seq.cmp(&a.start_wal_seq));

    // 16-way pipeline: must outpace leader's concurrent fallback uploads or follower never closes the gap.
    const PARALLEL_DOWNLOADS: usize = 16;
    let mut candidate_hashes: HashSet<[u8; 32]> = HashSet::new();
    let mut hash_to_batch_path: HashMap<[u8; 32], (String, u64)> = HashMap::new();
    let mut tried = 0u64;
    for chunk in earlier_batches.chunks(PARALLEL_DOWNLOADS) {
        let mut handles = Vec::with_capacity(chunk.len());
        for batch_ref in chunk {
            let downloader = downloader.clone();
            let path = batch_ref.path.clone();
            let start_wal = batch_ref.start_wal_seq;
            handles.push(glommio::spawn_local(async move {
                let data = match downloader.download(&path).await {
                    Ok(d) => d,
                    Err(e) => return Err((path, start_wal, format!("download: {e:?}"))),
                };
                let batch = match deserialise_fallback_batch(&data) {
                    Ok(b) => b,
                    Err(e) => return Err((path, start_wal, format!("deserialise: {e:?}"))),
                };
                let hash = match batch.items.first() {
                    Some(i) => i.metablock.previous_tip_hash,
                    None => return Err((path, start_wal, "empty batch".into())),
                };
                Ok((path, start_wal, hash))
            }));
        }
        for handle in handles {
            tried += 1;
            match handle.await {
                Ok((path, start_wal, hash)) => {
                    candidate_hashes.insert(hash);
                    hash_to_batch_path.entry(hash).or_insert((path, start_wal));
                    metrics::counter!("celeriant_s3_catchup_via_s3_step_total", "outcome" => "downloaded").increment(1);
                }
                Err((path, _start_wal, reason)) => {
                    tracing::warn!(path = %path, reason = %reason, "find_divergence_via_s3: candidate skipped");
                    metrics::counter!("celeriant_s3_catchup_via_s3_step_total", "outcome" => "skip").increment(1);
                }
            }
        }
    }

    if candidate_hashes.is_empty() {
        tracing::warn!(
            earlier_total_unfiltered,
            earlier_total_after_peer_filter,
            tried,
            current_wal_seq,
            "find_divergence_via_s3: no usable candidates"
        );
        metrics::counter!("celeriant_s3_catchup_via_s3_exhausted_total").increment(1);
        return Err(S3CatchupError::TruncationFailed(ShardFsyncError::MetablockSerialisationError(
            "no S3 batch shares common ancestor with local WAL".into(),
        )));
    }

    // A candidate hash at batch start S can only match local wal_seq = S, so floor at min - 1.
    let min_candidate_start = hash_to_batch_path.values().map(|(_, s)| *s).min().unwrap_or(0);
    let scan_floor_wal_seq = min_candidate_start.saturating_sub(1);
    let mut result = scan_local_metablocks_for_hashes(log_segments_cache, &candidate_hashes, scan_floor_wal_seq).await;

    if let Ok((hash, log_id, wal_seq, position)) = result.as_ref() {
        let path = hash_to_batch_path.get(hash).map(|(p, _)| p.as_str()).unwrap_or("<unknown>");
        let batch_start_wal = hash_to_batch_path.get(hash).map(|(_, s)| *s).unwrap_or(0);
        tracing::info!(
            path = %path,
            batch_start_wal,
            candidate_hash = %hex_short(hash),
            divergent_wal_seq = wal_seq,
            divergent_log_id = log_id,
            divergent_position = position,
            tried,
            candidates = candidate_hashes.len(),
            "find_divergence_via_s3: ancestor found"
        );
        metrics::counter!("celeriant_s3_catchup_via_s3_step_total", "outcome" => "match").increment(1);

        // Refine past byte-identical prefix in the matched batch (TCP-replicated overlap).
        let matched_path = hash_to_batch_path.get(hash).map(|(p, _)| p.clone());
        let (h, lid, ws, pos) = (*hash, *log_id, *wal_seq, *position);
        if let Some(path) = matched_path {
            match downloader.download(&path).await {
                Ok(data) => match deserialise_fallback_batch(&data) {
                    Ok(batch) => {
                        let metablock_refs: Vec<&_> = batch.items.iter().map(|fi| &fi.metablock).collect();
                        let refined = refine_divergence_by_byte_match(
                            log_segments_cache, &metablock_refs, h, lid, ws, pos,
                        ).await;
                        if refined.2 != ws {
                            tracing::info!(
                                from_wal_seq = ws,
                                to_wal_seq = refined.2,
                                advanced = refined.2 - ws,
                                "find_divergence_via_s3: refined divergent_wal_seq past byte-identical prefix"
                            );
                        }
                        result = Ok(refined);
                    }
                    Err(e) => {
                        tracing::warn!(path = %path, error = ?e, "find_divergence_via_s3: refinement download deserialise failed; using conservative divergent_wal_seq");
                    }
                },
                Err(e) => {
                    tracing::warn!(path = %path, error = ?e, "find_divergence_via_s3: refinement download failed; using conservative divergent_wal_seq");
                }
            }
        }
    } else {
        // No local metablock chained onto any S3 batch start, so the speculative tail
        // (read, write] forked from the committed chain. If an authoritative batch starts
        // at read+1 and chains onto the read tip, then read IS the common ancestor; reframe
        // divergence there. Require a real read cursor first: a None read's zero sentinel
        // would false-match a genesis batch (prev == GENESIS_HASH == [0;32], start == 1).
        let read_cursor = {
            let active = log_segments_cache.active();
            let m = active.metadata.borrow();
            m.read.as_ref().map(|r| (r.wal_seq, r.tip_hash, r.log_id, r.metablocks_position))
        };
        let anchor_at_read = read_cursor.and_then(|(read_wal, read_tip, read_log_id, read_position)| {
            hash_to_batch_path
                .iter()
                .find(|(h, (_, start))| **h == read_tip && *start == read_wal + 1)
                .map(|(_, (path, _))| (path.clone(), read_tip, read_log_id, read_wal, read_position))
        });

        match anchor_at_read {
            Some((anchor_path, read_tip, read_log_id, read_wal, read_position)) => {
                // Refine past any byte-identical prefix. truncate_wal stays ack-barrier gated.
                match downloader.download(&anchor_path).await {
                    Ok(data) => match deserialise_fallback_batch(&data) {
                        Ok(batch) => {
                            let metablock_refs: Vec<&_> = batch.items.iter().map(|fi| &fi.metablock).collect();
                            let refined = refine_divergence_by_byte_match(
                                log_segments_cache, &metablock_refs, read_tip, read_log_id, read_wal + 1, read_position,
                            ).await;
                            tracing::warn!(
                                read_wal,
                                divergent_wal_seq = refined.2,
                                anchor_path = %anchor_path,
                                "find_divergence_via_s3: reframed divergence at read cursor (authority chains onto read tip)"
                            );
                            metrics::counter!("celeriant_s3_catchup_reframed_at_read_total").increment(1);
                            result = Ok(refined);
                        }
                        Err(e) => {
                            tracing::warn!(anchor_path = %anchor_path, error = ?e, "find_divergence_via_s3: read-reframe anchor deserialise failed");
                            metrics::counter!("celeriant_s3_catchup_via_s3_exhausted_total").increment(1);
                        }
                    },
                    Err(e) => {
                        tracing::warn!(anchor_path = %anchor_path, error = ?e, "find_divergence_via_s3: read-reframe anchor download failed");
                        metrics::counter!("celeriant_s3_catchup_via_s3_exhausted_total").increment(1);
                    }
                }
            }
            None => {
                // No S3 batch anchors the divergence: either no read cursor, or no batch
                // chains onto the read tip (a coverage hole, or divergence below read). Once
                // the read+1 reframe above handles the recoverable cases, this is the
                // genuinely unreconcilable residue — not safe to truncate, so the caller
                // retries (and stays in catching-up). Worth a loud diagnostic if it ever fires.
                let (read_wal, read_tip) = read_cursor.map(|(w, t, _, _)| (w, t)).unwrap_or((0, [0u8; 32]));
                tracing::warn!(
                    current_wal_seq,
                    read_wal,
                    read_tip = %hex_short(&read_tip),
                    min_candidate_start,
                    "find_divergence_via_s3: no S3 batch anchors the catchup divergence \
                     (no read anchor / coverage hole / divergence below read) — cannot reconcile, retrying"
                );
                metrics::counter!("celeriant_s3_catchup_via_s3_exhausted_total").increment(1);
            }
        }
    }

    result
}

/// Reverse-scan local WAL for a metablock whose `previous_tip_hash` is in `candidate_hashes`.
/// Stops at `scan_floor_wal_seq` (0 disables). First match is the divergence boundary.
async fn scan_local_metablocks_for_hashes(
    log_segments_cache: &Rc<LogSegmentsCache>,
    candidate_hashes: &HashSet<[u8; 32]>,
    scan_floor_wal_seq: u64,
) -> Result<([u8; 32], u64, u64, u64), S3CatchupError> {
    const READ_CHUNK_SIZE: u64 = 64 * 1024;
    let active_log_id = log_segments_cache.active_log_id();
    let mut scanner = ReverseMetablockScanner::new(log_segments_cache, active_log_id, None, READ_CHUNK_SIZE);

    enum ScanHit {
        Match([u8; 32], u64, u64, u64),
        BelowFloor,
    }

    let result = scanner
        .scan(|log_id, pos, block| -> Result<Option<ScanHit>, ()> {
            let Ok(metablock) = deserialise_metablock(block) else {
                return Ok(None);
            };
            if scan_floor_wal_seq > 0 && metablock.wal_seq < scan_floor_wal_seq {
                return Ok(Some(ScanHit::BelowFloor));
            }
            if candidate_hashes.contains(&metablock.previous_tip_hash) {
                Ok(Some(ScanHit::Match(metablock.previous_tip_hash, log_id, metablock.wal_seq, pos)))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|e| S3CatchupError::TruncationFailed(ShardFsyncError::MetablockSerialisationError(format!("scan error: {:?}", e))))?;

    match result {
        Some(ScanHit::Match(hash, log_id, wal_seq, position)) => Ok((hash, log_id, wal_seq, position)),
        Some(ScanHit::BelowFloor) | None => Err(S3CatchupError::TruncationFailed(ShardFsyncError::MetablockSerialisationError(
            "candidate hash not found in local metablocks above scan floor".into(),
        ))),
    }
}

/// Advance divergent_wal_seq past byte-identical prefix in the matched S3 batch.
/// `find_divergence_*` returns the START of the matched batch (conservative), but
/// the local node often has TCP-replicated copies of the early items. Truncating
/// at the conservative point can trip the ack barrier. Falls back to the input
/// (always safe) on any error or chain mismatch.
async fn refine_divergence_by_byte_match(
    log_segments_cache: &Rc<LogSegmentsCache>,
    matched_batch_metablocks: &[&celeriant_wal::metablocks::metablock::Metablock],
    initial_ancestor_hash: [u8; 32],
    initial_ancestor_log_id: u64,
    initial_divergent_wal_seq: u64,
    initial_divergent_position: u64,
) -> ([u8; 32], u64, u64, u64) {
    let fallback = (
        initial_ancestor_hash,
        initial_ancestor_log_id,
        initial_divergent_wal_seq,
        initial_divergent_position,
    );

    let Some(start_idx) = matched_batch_metablocks
        .iter()
        .position(|m| m.wal_seq == initial_divergent_wal_seq)
    else {
        return fallback;
    };

    // Only walk within the active segment; sealed-segment crossing is out of scope.
    if initial_ancestor_log_id != log_segments_cache.active_log_id() {
        return fallback;
    }

    let active = log_segments_cache.active();
    let (metablocks_end, current_wal_seq) = {
        let meta = active.metadata.borrow();
        (meta.write.metablocks_position, meta.write.wal_seq)
    };

    let reader_guard = match active.lock_reader("refine_divergence_by_byte_match").await {
        Ok(g) => g,
        Err(_) => return fallback,
    };
    let Some(dma_file) = reader_guard.as_ref() else {
        return fallback;
    };

    let mut ancestor_hash = initial_ancestor_hash;
    let log_id = initial_ancestor_log_id;
    let mut wal_seq = initial_divergent_wal_seq;
    let mut position = initial_divergent_position;
    let mut advanced = 0u64;

    // Compare body bytes only. Skip the versioned header, previous_tip_hash
    // (chain-derived), and the contiguous node-local fields datablock_position +
    // previous_aggregate_metablock_pos.
    use celeriant_wal::metablocks::metablock::Metablock as Mb;
    const HEADER_SIZE: usize = celeriant_wire::disk::versioned_block::HEADER_SIZE;
    const CONTENT_START: usize = HEADER_SIZE;
    const PREV_HASH_START: usize = HEADER_SIZE + Mb::OFFSET_PREVIOUS_TIP_HASH;
    const NODE_LOCAL_END: usize = HEADER_SIZE
        + Mb::OFFSET_DATABLOCK_POSITION
        + Mb::WIRE_SIZE_DATABLOCK_POSITION
        + Mb::WIRE_SIZE_PREVIOUS_AGGREGATE_METABLOCK_POS;

    for batch_metablock in &matched_batch_metablocks[start_idx..] {
        let batch_metablock = *batch_metablock;
        if batch_metablock.wal_seq != wal_seq {
            break;
        }

        // Chain match must precede the bound check; otherwise an early break can
        // leave us partially advanced and stuck in a no-op truncate retry loop.
        if batch_metablock.previous_tip_hash != ancestor_hash {
            return fallback;
        }

        if position + FIXED_BLOCK_SIZE_BYTES as u64 > metablocks_end {
            break;
        }
        if wal_seq > current_wal_seq {
            break;
        }

        let buf = match dma_file.read_at(position, FIXED_BLOCK_SIZE_BYTES).await {
            Ok(b) => b,
            Err(_) => break,
        };
        let (chunks, _) = (*buf).as_chunks::<FIXED_BLOCK_SIZE_BYTES>();
        let Some(local_block) = chunks.first() else { break };

        let mut batch_block = [0u8; FIXED_BLOCK_SIZE_BYTES];
        if serialize_versioned_message(batch_metablock, WIRE_VERSION_WAL_METABLOCK, &mut batch_block).is_err() {
            break;
        }

        let content_match = local_block[CONTENT_START..PREV_HASH_START]
            == batch_block[CONTENT_START..PREV_HASH_START]
            && local_block[NODE_LOCAL_END..] == batch_block[NODE_LOCAL_END..];
        if !content_match {
            break;
        }

        ancestor_hash = compute_entry_hash(&ancestor_hash, local_block);
        wal_seq += 1;
        position += FIXED_BLOCK_SIZE_BYTES as u64;
        advanced += 1;
    }

    // Advancing past local's current_wal_seq leaves nothing to truncate;
    // fall back so truncate at least drops the original divergent wal_seq.
    if wal_seq > current_wal_seq {
        return fallback;
    }

    if advanced > 0 {
        metrics::counter!("celeriant_truncate_divergence_advanced_total").increment(1);
        metrics::counter!("celeriant_truncate_divergence_advanced_wal_seqs_total").increment(advanced);
    }

    (ancestor_hash, log_id, wal_seq, position)
}

/// Emit an alarm (counter + error log) when a truncate is dropping wal_seqs this
/// node acked to a client as leader. Doesn't change control flow; truncate proceeds.
fn alarm_if_truncate_drops_self_acked(
    log_segments_cache: &Rc<LogSegmentsCache>,
    divergent_wal_seq: u64,
    divergent_entry_position: u64,
) {
    let last_self_acked = log_segments_cache.active().metadata.borrow().last_self_acked_wal_seq;
    let new_wal_seq_after_truncate = divergent_wal_seq.saturating_sub(1);
    if last_self_acked > new_wal_seq_after_truncate {
        let lost = last_self_acked - new_wal_seq_after_truncate;
        metrics::counter!("celeriant_truncate_dropped_self_acked_events_total").increment(1);
        metrics::counter!("celeriant_truncate_dropped_self_acked_wal_seqs_total").increment(lost);
        tracing::error!(
            divergent_wal_seq,
            divergent_entry_position,
            new_wal_seq_after_truncate,
            last_self_acked,
            lost_self_acked_wal_seqs = lost,
            "truncate_wal: dropping wal_seqs this node acked as leader (false ack, durability contract violated)"
        );
    }
}

/// Truncate the WAL to the common ancestor when divergent entries are detected.
/// Uses the already-known divergent entry position from the caller to avoid re-scanning.
///
/// If the ancestor lives in a sealed segment (`divergent_log_id != active_log_id`),
/// the current active segment and any intermediate sealed segments are discarded
/// from disk, and the segment containing the ancestor is swapped back in as the
/// active file before its headers are rewritten. Safe under the rollback lock:
/// no concurrent reads or writes can hold references to the discarded segments.
async fn truncate_wal(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    dict_codec: &DictCodec,
    common_ancestor_hash: [u8; 32],
    divergent_log_id: u64,
    divergent_wal_seq: u64,
    divergent_entry_position: u64,
) -> Result<u64, ShardFsyncError> {
    // Ack barrier: only refuse truncates that would drop wal_seqs this node returned Ok
    // for as leader. last_received_replication_wal_seq and read.wal_seq are intentionally
    // NOT in the barrier: they bump on receive/apply paths that don't reflect what
    // bytes are actually on disk.
    let last_self_acked = log_segments_cache.active().metadata.borrow().last_self_acked_wal_seq;
    let barrier = last_self_acked;
    if barrier >= divergent_wal_seq {
        metrics::counter!("celeriant_truncate_refused_due_to_ack_barrier_total").increment(1);
        let would_lose = barrier - divergent_wal_seq.saturating_sub(1);
        tracing::error!(
            divergent_wal_seq,
            divergent_log_id,
            divergent_entry_position,
            last_self_acked,
            barrier,
            would_lose_wal_seqs = would_lose,
            "truncate_wal refused. Would drop wal_seqs this node acked as leader. \
             Staying in catching-up state; the cluster's authoritative chain disagrees \
             with this node's local chain at self-acked wal_seqs. Operator intervention \
             required: investigate the divergent leadership cycle."
        );
        return Err(ShardFsyncError::TruncateRefusedByAckBarrier {
            divergent_wal_seq,
            barrier,
        });
    }

    // Step 1: Acquire rollback lock to block concurrent writes
    let _fsync_gate = fsync_coordinator
        .acquire_rollback_lock()
        .await
        .ok_or(ShardFsyncError::WriteLockTimeout)?;

    alarm_if_truncate_drops_self_acked(log_segments_cache, divergent_wal_seq, divergent_entry_position);

    // Step 2a: Commit the surviving parked prefix. Entries below the divergence
    // are on the authoritative chain and stay durable; their watch events fire
    // exactly once, here. Whole batches below the divergence commit normally;
    // the (at most one, by tip-ascending construction) batch straddling it gets
    // only its surviving items' watch events and segment-summary contributions —
    // deliberately NOT commit_pcd: the cursor is owned by Step 3's rewind onto
    // the truncated tip, and the read caches are cleared right below. Items
    // at-or-past the divergence never fire — their entries leave the chain.
    // Summary preservation holds for the ACTIVE accumulator only: contributions
    // routed to a sealed segment's slot are wiped by Step 2b's clear_all_caches
    // (sealed_segment_summaries.clear()), so a sealed divergent segment keeps
    // the listing gap it has on main — pre-existing, tracked, not widened here.
    let surviving = shard_mem_cache.borrow_mut().drain_parked_commits_up_to(divergent_wal_seq.saturating_sub(1));
    for pcd in surviving {
        crate::shard_wal_replicate::commit_pcd(log_segments_cache, shard_mem_cache, watched_aggregates, pcd, Some(dict_codec));
    }
    {
        let discarded = shard_mem_cache.borrow_mut().take_all_parked_commits();
        let mut event_collector = WatchEventCollector::new();
        {
            let mut cache = shard_mem_cache.borrow_mut();
            for pcd in &discarded {
                for item in pcd.pending_queue.iter().filter(|i| i.metablock.wal_seq < divergent_wal_seq) {
                    cache.update_segment_summary_for_log(pcd.log_id(), &item.metablock);
                    crate::shard_wal_replicate::collect_watch_event(&mut event_collector, &item.metablock);
                }
            }
        }
        event_collector.broadcast_all(watched_aggregates);
    }

    // Step 2b: Clear all caches (read snapshots, recent writes; the parked
    // queue is already empty from Step 2a)
    shard_mem_cache.borrow_mut().clear_all_caches();

    // Step 2c: Sealed-segment ancestor - discard the active segment and any
    // intermediates, swap the sealed segment back in. After this call,
    // log_segments_cache.active() returns the file that will receive the
    // header rewrite below.
    if divergent_log_id != log_segments_cache.active_log_id() {
        log_segments_cache
            .unwind_active_to_sealed(divergent_log_id)
            .await
            .map_err(|e| ShardFsyncError::MetablockSerialisationError(format!("unwind_active_to_sealed failed: {e}")))?;
    }

    let active = log_segments_cache.active();
    let current_wal_seq = active.metadata.borrow().write.wal_seq;

    // Calculate how many entries to truncate (including the divergent one)
    let divergent_count = current_wal_seq.saturating_sub(divergent_wal_seq).saturating_add(1);
    let new_wal_seq = divergent_wal_seq.saturating_sub(1);
    let new_metablocks_position = divergent_entry_position;

    // Step 3: Rewind cursors (both read and write)
    {
        let mut metadata = active.metadata.borrow_mut();
        metadata.write.wal_seq = new_wal_seq;
        metadata.write.tip_hash = common_ancestor_hash;
        metadata.write.metablocks_position = new_metablocks_position;

        // Also update read cursor to match
        if let Some(ref mut read) = metadata.read {
            read.wal_seq = new_wal_seq;
            read.tip_hash = common_ancestor_hash;
            read.metablocks_position = new_metablocks_position;
        }
    }
    log_segments_cache.publish_cursor_gauges();

    // Step 4: Write dual headers and fsync
    let dma_file_writer = active.lock_writer("truncate_wal").await.map_err(|_| ShardFsyncError::WriteLockTimeout)?;
    let dma_file_writer = dma_file_writer.as_ref().ok_or(ShardFsyncError::ActiveWriteFileUnavailable)?;

    let (header, header_end_start_pos) = {
        let metadata = active.metadata.borrow();
        let shard_log_header_end_pos = metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
        (metadata.to_shard_log_header(), shard_log_header_end_pos)
    };

    write_dual_shard_log_header(dma_file_writer, header_end_start_pos, &header)
        .await
        .map_err(ShardFsyncError::LogSegmentFileHeaderWriteFailure)?;

    dma_file_writer
        .fdatasync()
        .await
        .map_err(|e| ShardFsyncError::FDataSyncError(format!("{:?}", e)))?;

    // Step 5: Update datablocks_carry_over
    {
        let mut metadata = active.metadata.borrow_mut();
        metadata.datablocks_carry_over = read_datablocks_carry_over_bytes(dma_file_writer, metadata.write.datablocks_position)
            .await
            .map_err(|e| ShardFsyncError::MetablockSerialisationError(format!("carry-over read failed: {:?}", e)))?;
    }

    // Rewound cursor invalidated the active segment's backlink tips; rebuild from disk.
    crate::shard_wal::rebuild_active_segment_chain_tips(log_segments_cache, 64 * 1024)
        .await
        .map_err(|e| ShardFsyncError::MetablockSerialisationError(format!("chain-tips rebuild after truncate failed: {e:?}")))?;

    tracing::warn!(divergent_count, new_wal_seq, "WAL truncated due to divergent entries");

    Ok(divergent_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::collections::{HashMap, HashSet};
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

    fn test_metablock(wal_seq: u64, previous_tip_hash: [u8; 32]) -> Metablock {
        let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::new(1, 1, 1));
        mb.wal_seq = wal_seq;
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
        make_fallback_batch_with_seq(shard_id, start, end, tip_hash, node_id, 0)
    }

    fn make_fallback_batch_with_seq(shard_id: u32, start: u64, end: u64, tip_hash: [u8; 32], node_id: u128, upload_sequence: u64) -> (String, Bytes) {
        make_fallback_batch_with_lease_seq(shard_id, start, end, tip_hash, node_id, upload_sequence, 0)
    }

    fn make_fallback_batch_with_lease_seq(shard_id: u32, start: u64, end: u64, tip_hash: [u8; 32], node_id: u128, upload_sequence: u64, lease_epoch: u64) -> (String, Bytes) {
        let mut batch = FallbackBatch::new(start, end, shard_id, node_id, upload_sequence, lease_epoch);
        for wal_seq in start..=end {
            let mut mb = test_metablock(wal_seq, tip_hash);
            mb.lease_epoch = lease_epoch;
            batch.push_item(FallbackItem {
                metablock: mb,
                datablock: None,
            });
        }
        let path = fallback_batch_path(shard_id, start, end, node_id);
        (path, serialize_fallback_batch(&batch))
    }

    fn build_batch(start: u64, prevs: &[[u8; 32]], lease_epoch: u64) -> FallbackBatch {
        let mut b = FallbackBatch::new(start, start + prevs.len() as u64 - 1, 0, 0, 0, lease_epoch);
        for (i, &prev) in prevs.iter().enumerate() {
            let mut mb = test_metablock(start + i as u64, prev);
            mb.lease_epoch = lease_epoch;
            b.push_item(FallbackItem { metablock: mb, datablock: None });
        }
        b
    }

    /// The content-immutability-per-epoch detector flags a same-epoch chain that
    /// diverges within the overlap, but treats a matching prefix (a fence-and-retry
    /// re-upload with a different extent) as fine, and never flags a cross-epoch
    /// re-author.
    #[test]
    fn same_epoch_content_divergence_flags_only_real_forks() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        // identical -> not flagged (the normal dedup tiebreak case)
        assert!(!same_epoch_content_divergence(&build_batch(3, &[a, a, a], 1), &build_batch(3, &[a, a, a], 1)));
        // divergent from the start -> flagged
        assert!(same_epoch_content_divergence(&build_batch(3, &[a, a, a], 1), &build_batch(3, &[b, b, b], 1)));
        // different extent, matching prefix -> NOT flagged (regression: a shorter
        // batch is a clean prefix of the retry; the old extent check false-fired here)
        assert!(!same_epoch_content_divergence(&build_batch(3, &[a, a, a], 1), &build_batch(3, &[a, a, a, a, a], 1)));
        // divergence only after a common prefix -> flagged
        assert!(same_epoch_content_divergence(&build_batch(3, &[a, a, b], 1), &build_batch(3, &[a, a, a], 1)));
        // different epoch -> not flagged; lease_epoch legitimately decides the dedup
        assert!(!same_epoch_content_divergence(&build_batch(3, &[a, a, a], 1), &build_batch(3, &[b, b, b], 2)));
    }

    /// Degenerate inputs must not panic or false-fire: an empty batch has no
    /// overlap to disagree on, so it is never a divergence.
    #[test]
    fn same_epoch_content_divergence_handles_empty_overlap() {
        let a = [1u8; 32];
        let empty = build_batch(3, &[], 1);
        let full = build_batch(3, &[a, a], 1);
        assert!(!same_epoch_content_divergence(&empty, &full));
        assert!(!same_epoch_content_divergence(&full, &empty));
        assert!(!same_epoch_content_divergence(&empty, &empty));
    }

    // ── Mock S3Downloader ──

    struct MockDownloader {
        objects: RefCell<HashMap<String, Bytes>>,
        download_log: RefCell<Vec<String>>,
        delete_log: RefCell<Vec<String>>,
        list_call_count: Cell<u32>,
        on_list_hooks: RefCell<HashMap<u32, Vec<Box<dyn Fn(&MockDownloader)>>>>,
        fail_paths: RefCell<HashSet<String>>,
    }

    impl MockDownloader {
        fn new() -> Self {
            Self {
                objects: RefCell::new(HashMap::new()),
                download_log: RefCell::new(Vec::new()),
                delete_log: RefCell::new(Vec::new()),
                list_call_count: Cell::new(0),
                on_list_hooks: RefCell::new(HashMap::new()),
                fail_paths: RefCell::new(HashSet::new()),
            }
        }

        fn insert(&self, path: String, data: Bytes) {
            self.objects.borrow_mut().insert(path, data);
        }

        fn fail_download(&self, path: String) {
            self.fail_paths.borrow_mut().insert(path);
        }

        fn downloaded_paths(&self) -> Vec<String> {
            self.download_log.borrow().clone()
        }

        fn deleted_paths(&self) -> Vec<String> {
            self.delete_log.borrow().clone()
        }

        fn on_list(&self, call_index: u32, hook: impl Fn(&Self) + 'static) {
            self.on_list_hooks.borrow_mut().entry(call_index).or_default().push(Box::new(hook));
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
            Ok(self
                .objects
                .borrow()
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| S3ObjectRef {
                    path: k.clone(),
                    size: v.len() as u64,
                })
                .collect())
        }

        async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError> {
            self.download_log.borrow_mut().push(path.to_string());
            if self.fail_paths.borrow().contains(path) {
                return Err(S3CatchupError::S3GetFailed {
                    path: path.to_string(),
                    message: "injected failure".to_string(),
                });
            }
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

    fn test_codec() -> Rc<DictCodec> {
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        Rc::new(DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile"))
    }

    impl TestComponents {
        async fn new(dir: &std::path::Path) -> Self {
            let log_segments_cache = LogSegmentsCache::ready_up(dir.to_path_buf(), PREALLOCATE, 4, 0).await.unwrap();
            Self {
                log_segments_cache: Rc::new(log_segments_cache),
                shard_mem_cache: Rc::new(RefCell::new(MemCache::new(
                    64 * 1024 * 1024,
                    64 * 1024 * 1024,
                    32 * 1024 * 1024,
                    4 * 1024 * 1024,
                    64 * 1024 * 1024,
                ))),
                fsync_coordinator: Rc::new(Coordinator::new()),
                watched_aggregates: Rc::new(AggregateWatchers::new()),
            }
        }

        fn wal_seq(&self) -> u64 {
            self.log_segments_cache.active().metadata.borrow().write.wal_seq
        }

        fn tip_hash(&self) -> [u8; 32] {
            self.log_segments_cache.active().metadata.borrow().write.tip_hash
        }

        async fn catchup(&self, downloader: &Rc<MockDownloader>, shard_id: u32, _max_rounds: u32) -> Result<S3CatchupResult, S3CatchupError> {
            self.catchup_with_peer(downloader, shard_id, None).await
        }

        async fn catchup_with_peer(
            &self,
            downloader: &Rc<MockDownloader>,
            shard_id: u32,
            peer_node_id: Option<u128>,
        ) -> Result<S3CatchupResult, S3CatchupError> {
            catchup_from_s3(
                &self.log_segments_cache,
                &self.shard_mem_cache,
                &self.fsync_coordinator,
                &self.watched_aggregates,
                downloader,
                shard_id,
                99,
                peer_node_id,
                Some(100), // max_catchup_gap_bytes
                test_codec(),
            )
            .await
        }

        async fn close(&self) {
            self.log_segments_cache.close().await;
        }

        /// Apply 1..=end. Returns the tip captured at wal=end-1, which equals
        /// local @ wal=end's previous_tip_hash.
        async fn seed_chain(&self, end: u64) -> [u8; 32] {
            assert!(end >= 2);
            let dl = Rc::new(MockDownloader::new());
            let (p, d) = make_fallback_batch(0, 1, end - 1, GENESIS_HASH);
            dl.insert(p, d);
            self.catchup(&dl, 0, 10).await.unwrap();
            let prev = self.tip_hash();
            let (p, d) = make_fallback_batch(0, end, end, prev);
            dl.insert(p, d);
            self.catchup(&dl, 0, 10).await.unwrap();
            prev
        }
    }

    fn pos_at(wal_seq: u64) -> u64 {
        HEADER_BLOCK_SIZE_BYTES as u64 + (wal_seq - 1) * FIXED_BLOCK_SIZE_BYTES as u64
    }

    fn test_metablock_for_agg(wal_seq: u64, prev: [u8; 32], agg: AggregateKey) -> Metablock {
        let mut mb = Metablock::default_inline_event_batch_metadata(agg);
        mb.wal_seq = wal_seq;
        mb.previous_tip_hash = prev;
        mb
    }

    /// Apply 1..=end one wal_seq at a time, returning tips[i] = tip after wal_seq=i+1.
    async fn seed_capturing_tips(tc: &TestComponents, end: u64) -> Vec<[u8; 32]> {
        let dl = Rc::new(MockDownloader::new());
        let mut tips = Vec::with_capacity(end as usize);
        for wal_seq in 1..=end {
            let prev = *tips.last().unwrap_or(&GENESIS_HASH);
            let (p, d) = make_fallback_batch(0, wal_seq, wal_seq, prev);
            dl.insert(p, d);
            tc.catchup(&dl, 0, 10).await.unwrap();
            tips.push(tc.tip_hash());
        }
        tips
    }

    /// Local at wal_seq=6; S3 holds a 6..=8 chain anchored at tip_after_5, which
    /// trips TipHashMismatch on local's wal_seq=7 expectation and drives
    /// truncate_wal at divergent_wal_seq=6. Returns the prepped downloader.
    async fn divergence_at_6(tc: &TestComponents) -> Rc<MockDownloader> {
        let dl = Rc::new(MockDownloader::new());
        let (p, d) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
        dl.insert(p, d);
        tc.catchup(&dl, 0, 10).await.unwrap();
        let tip_after_5 = tc.tip_hash();
        let (p, d) = make_fallback_batch(0, 6, 6, tip_after_5);
        dl.insert(p, d);
        tc.catchup(&dl, 0, 10).await.unwrap();
        dl.objects.borrow_mut().clear();
        let (p, d) = make_fallback_batch(0, 6, 8, tip_after_5);
        dl.insert(p, d);
        dl
    }

    /// After find_divergence_via_s3 truncates, drop the stale trigger and plant a fresh
    /// one anchored at the live local tip so catchup can converge.
    fn resume_after_truncate(
        dl: &Rc<MockDownloader>,
        lsc: Rc<LogSegmentsCache>,
        bad_trigger_path: String,
        resume_start: u64,
        resume_end: u64,
    ) {
        dl.on_list(2, move |dl| {
            dl.objects.borrow_mut().remove(&bad_trigger_path);
        });
        dl.on_list(3, move |dl| {
            let tip = lsc.active().metadata.borrow().write.tip_hash;
            let (p, d) = make_fallback_batch(0, resume_start, resume_end, tip);
            dl.insert(p, d);
        });
    }

    // ── apply_external_batch tests ──

    #[test]
    fn apply_rejects_wal_seq_mismatch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            let item = ReplicationBatchItem {
                metablock: test_metablock(99, GENESIS_HASH),
                datablock: None,
            };
            let err = apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item], &test_codec()).unwrap_err();
            assert!(matches!(err, ApplyBatchError::WalSeqMismatch { current: 0, batch_first: 99 }));

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
            let err = apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item], &test_codec()).unwrap_err();
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
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item], &test_codec()).unwrap();
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
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item1], &test_codec()).unwrap();
            // Flush the pending queue so WAL sequence advances
            sync_applied_batch(
                &tc.log_segments_cache,
                &tc.shard_mem_cache,
                &tc.fsync_coordinator,
                &tc.watched_aggregates,
                0,
                test_codec(),
            )
            .await
            .unwrap();
            assert_eq!(tc.wal_seq(), 1);

            // Now send a batch [1, 2] - entry 1 should be skipped, entry 2 applied
            let tip = tc.tip_hash();
            let stale = ReplicationBatchItem {
                metablock: test_metablock(1, GENESIS_HASH),
                datablock: None,
            };
            let fresh = ReplicationBatchItem {
                metablock: test_metablock(2, tip),
                datablock: None,
            };
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[stale, fresh], &test_codec()).unwrap();

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
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item.clone()], &test_codec()).unwrap();
            sync_applied_batch(
                &tc.log_segments_cache,
                &tc.shard_mem_cache,
                &tc.fsync_coordinator,
                &tc.watched_aggregates,
                0,
                test_codec(),
            )
            .await
            .unwrap();
            assert_eq!(tc.wal_seq(), 1);

            // Re-send same entry - should be a no-op
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item], &test_codec()).unwrap();

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
            assert_eq!(result.completion, CatchupCompletion::Caught);

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
            assert_eq!(result.completion, CatchupCompletion::Caught);
            assert_eq!(tc.wal_seq(), 1);

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
            assert_eq!(tc.wal_seq(), 5);
            assert_eq!(result.completion, CatchupCompletion::Caught);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_applies_contiguous_prefix_on_gap() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path, data) = make_fallback_batch(0, 1, 2, GENESIS_HASH);
            dl.insert(path, data);
            // Gap: missing batch 3-4
            let (path, data) = make_fallback_batch(0, 5, 6, GENESIS_HASH);
            dl.insert(path, data);

            // Gaps are handled gracefully - apply [1-2], stop at gap.
            // remaining gap is small enough for TCP, so completion is Caught.
            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 2);
            assert_eq!(result.batches_applied, 1);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_does_not_delete_batches() {
        // S3 cleanup is deferred until the follower returns to Follower state.
        // During catchup, batches stay in S3 in case truncation needs them.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            dl.insert(path, data);

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 3);
            assert!(dl.deleted_paths().is_empty());

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
            assert_eq!(tc.wal_seq(), 1);

            // Add batch 2, but also re-add batch 1 (already applied)
            let tip = tc.tip_hash();
            let (path1, data1) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path1, data1);
            let (path2, data2) = make_fallback_batch(0, 2, 2, tip);
            dl.insert(path2, data2);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_seq(), 2);

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
            assert_eq!(tc.wal_seq(), 1);

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
            assert_eq!(tc.wal_seq(), 3);

            // Add overlapping batch 2-6: entries 2-3 already applied, 4-6 are new.
            // All items get the same tip_hash; only item 4 (first after slicing) is checked.
            let tip = tc.tip_hash();
            let (path, data) = make_fallback_batch(0, 2, 6, tip);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_seq(), 6);
            assert_eq!(result.completion, CatchupCompletion::Caught);

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
            assert_eq!(tc.wal_seq(), 6);
            assert_eq!(result.completion, CatchupCompletion::Caught);

            tc.close().await;
        });
    }

    // ── Truncation tests ──

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
            assert_eq!(tc.wal_seq(), 5);
            let tip_after_5 = tc.tip_hash();

            // Step 2: Apply divergent entry 6 (simulates follower receiving from old leader)
            let (path6_divergent, data6_divergent) = make_fallback_batch(0, 6, 6, tip_after_5);
            dl.insert(path6_divergent, data6_divergent);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 6);
            let tip_after_divergent_6 = tc.tip_hash();
            assert_ne!(tip_after_5, tip_after_divergent_6, "Tip hash should change after entry 6");

            // Step 3: S3 now has "correct" batch 6-8 from new leader with previous_tip = tip_after_5
            // This will mismatch our current tip (tip_after_divergent_6), triggering truncation
            dl.objects.borrow_mut().clear();
            let (path6_8, data6_8) = make_fallback_batch(0, 6, 8, tip_after_5);
            dl.insert(path6_8, data6_8);

            // Step 4: Catchup detects TipHashMismatch, truncates entry 6, re-applies 6-8
            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 8, "Should catch up to wal_seq 8 after truncation");
            assert_eq!(result.completion, CatchupCompletion::Caught);

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
            assert_eq!(tc.wal_seq(), 8);

            // The old batch (1-5) should never have been downloaded
            let downloads = dl.downloaded_paths();
            assert!(
                !downloads.contains(&old_path),
                "Old batch 1-5 should not be downloaded, got: {:?}",
                downloads
            );

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
            assert_eq!(tc.wal_seq(), 8);

            // New leader wrote 6-12 (overlapping batch starting before our divergence)
            dl.objects.borrow_mut().clear();
            let (path, data) = make_fallback_batch(0, 6, 12, tip_after_5);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 12);
            assert_eq!(result.completion, CatchupCompletion::Caught);

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
            assert_eq!(tc.wal_seq(), 8);

            // B wrote 4-14 (5 more entries than A, starting from same common ancestor)
            dl.objects.borrow_mut().clear();
            let (path, data) = make_fallback_batch(0, 4, 14, tip_after_3);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 14);
            assert_eq!(result.completion, CatchupCompletion::Caught);

            tc.close().await;
        });
    }

    #[test]
    fn truncate_refused_when_follower_signal_covers_divergent_wal_seq() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let tip_5 = tc.seed_chain(6).await;
            assert_eq!(tc.wal_seq(), 6);

            tc.log_segments_cache.active().metadata.borrow_mut().last_self_acked_wal_seq = 6;

            // Divergent chain 6..=8 anchored at tip_5; divergent_wal_seq=6 = barrier, refused.
            let dl = Rc::new(MockDownloader::new());
            let (path, data) = make_fallback_batch(0, 6, 8, tip_5);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await;
            assert!(
                result.is_err(),
                "catchup should err due to ack barrier refusal, got {:?}", result.as_ref().map(|r| &r.completion),
            );
            let err = result.unwrap_err();
            assert!(
                matches!(err, S3CatchupError::TruncationFailed(ShardFsyncError::TruncateRefusedByAckBarrier { .. })),
                "expected TruncateRefusedByAckBarrier, got: {err:?}",
            );
            assert!(err.is_retriable(), "barrier refusal must be retriable (not fatal): {err:?}");
            assert_eq!(tc.wal_seq(), 6, "wal_seq must not regress under barrier refusal");

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
            // No local metablock will match — divergence is unrecoverable in
            // this round. Catchup should bail with Retry (caller can re-list
            // S3 next round; if the chain still doesn't reconcile, it's
            // surfaced via a persistent Retry signal, not a fatal panic).
            dl.objects.borrow_mut().clear();
            let (path, data) = make_fallback_batch(0, 4, 6, [0xFF; 32]);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.completion, CatchupCompletion::Retry);

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
            assert_eq!(tc.wal_seq(), 6);

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
            assert_eq!(tc.wal_seq(), 9);
            assert_eq!(result.completion, CatchupCompletion::Caught);

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
    fn s3_fallback_downloads_multiple_candidates_and_finds_match() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let tip_5 = tc.seed_chain(6).await;

            let dl = Rc::new(MockDownloader::new());
            let (trigger_path, trigger_data) = make_fallback_batch(0, 7, 9, [0xEE; 32]);
            dl.insert(trigger_path.clone(), trigger_data);
            for (start, prev) in [(2u64, [0xBB; 32]), (4, [0xCC; 32]), (6, tip_5)] {
                let (p, d) = make_fallback_batch(0, start, start, prev);
                dl.insert(p, d);
            }

            resume_after_truncate(&dl, tc.log_segments_cache.clone(), trigger_path, 7, 9);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 9);
            assert_eq!(result.completion, CatchupCompletion::Caught);

            let downloads = dl.downloaded_paths();
            for start in [2u64, 4, 6] {
                let p = fallback_batch_path(0, start, start, 0);
                assert!(downloads.contains(&p), "candidate at start={start} should have been downloaded");
            }
        });
    }

    #[test]
    fn s3_fallback_picks_most_recent_among_multiple_matching_candidates() {
        // If the older anchor at wal=2 wins, truncate-to-1 stops 6-6 (prev=tip_5) from ever applying.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            let dl_setup = Rc::new(MockDownloader::new());
            let (p, d) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl_setup.insert(p, d);
            tc.catchup(&dl_setup, 0, 10).await.unwrap();
            let tip_1 = tc.tip_hash();
            let (p, d) = make_fallback_batch(0, 2, 5, tip_1);
            dl_setup.insert(p, d);
            tc.catchup(&dl_setup, 0, 10).await.unwrap();
            let tip_5 = tc.tip_hash();
            let (p, d) = make_fallback_batch(0, 6, 6, tip_5);
            dl_setup.insert(p, d);
            tc.catchup(&dl_setup, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 6);

            let dl = Rc::new(MockDownloader::new());
            let (trigger_path, trigger_data) = make_fallback_batch(0, 7, 9, [0xEE; 32]);
            dl.insert(trigger_path.clone(), trigger_data);
            for (start, prev) in [(2u64, tip_1), (6, tip_5)] {
                let (p, d) = make_fallback_batch(0, start, start, prev);
                dl.insert(p, d);
            }

            resume_after_truncate(&dl, tc.log_segments_cache.clone(), trigger_path, 7, 9);

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 9);
        });
    }

    #[test]
    fn s3_fallback_scan_floor_prevents_match_below_min_candidate_start() {
        // Without the floor, the candidate at start=10 (prev=tip_1) falsely matches local @ wal=2.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            let dl_setup = Rc::new(MockDownloader::new());
            let (p, d) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl_setup.insert(p, d);
            tc.catchup(&dl_setup, 0, 10).await.unwrap();
            let tip_1 = tc.tip_hash();
            let (p, d) = make_fallback_batch(0, 2, 15, tip_1);
            dl_setup.insert(p, d);
            tc.catchup(&dl_setup, 0, 10).await.unwrap();
            let tip_15 = tc.tip_hash();
            let (p, d) = make_fallback_batch(0, 16, 16, tip_15);
            dl_setup.insert(p, d);
            tc.catchup(&dl_setup, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 16);

            let dl = Rc::new(MockDownloader::new());
            let (p, d) = make_fallback_batch(0, 17, 19, [0xEE; 32]);
            dl.insert(p, d);
            let (p, d) = make_fallback_batch(0, 10, 12, tip_1);
            dl.insert(p, d);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.completion, CatchupCompletion::Retry);
            assert_eq!(tc.wal_seq(), 16, "scan floor must block the collisional match at wal=2");
        });
    }

    #[test]
    fn s3_fallback_succeeds_when_some_downloads_fail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let tip_5 = tc.seed_chain(6).await;

            let dl = Rc::new(MockDownloader::new());
            let (trigger_path, trigger_data) = make_fallback_batch(0, 7, 9, [0xEE; 32]);
            dl.insert(trigger_path.clone(), trigger_data);
            for (start, prev) in [(2u64, [0xBB; 32]), (4, [0xCC; 32]), (6, tip_5)] {
                let (p, d) = make_fallback_batch(0, start, start, prev);
                dl.insert(p, d);
            }
            dl.fail_download(fallback_batch_path(0, 2, 2, 0));
            dl.fail_download(fallback_batch_path(0, 4, 4, 0));

            resume_after_truncate(&dl, tc.log_segments_cache.clone(), trigger_path, 7, 9);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 9);
            assert_eq!(result.completion, CatchupCompletion::Caught);
        });
    }

    #[test]
    fn s3_fallback_chunks_candidates_above_parallel_limit() {
        // 18 candidates straddle two chunks of PARALLEL_DOWNLOADS=16. None match by design.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            tc.seed_chain(18).await;

            let dl = Rc::new(MockDownloader::new());
            let (p, d) = make_fallback_batch(0, 19, 21, [0xEE; 32]);
            dl.insert(p, d);
            for start in 1..=18u64 {
                let (p, d) = make_fallback_batch(0, start, start, [start as u8; 32]);
                dl.insert(p, d);
            }

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.completion, CatchupCompletion::Retry);
            assert_eq!(tc.wal_seq(), 18, "no candidate matched; local must not be truncated");

            let downloads = dl.downloaded_paths();
            for start in 1..=18u64 {
                let p = fallback_batch_path(0, start, start, 0);
                assert!(downloads.contains(&p), "missing parallel download of start={start}");
            }
        });
    }

    #[test]
    fn test_fallback_batch_s3_path() {
        let batch = FallbackBatch::new(5, 10, 2, 0, 0, 0);
        assert_eq!(
            fallback_batch_path(batch.shard_id, batch.fallback_index, batch.end_wal_seq, batch.uploaded_by_node_id),
            "cluster/fallback/shard_002/batch_000000005_000000010_00000000-0000-0000-0000-000000000000.bin"
        );
    }

    #[test]
    fn test_fallback_batch_bincode_roundtrip() {
        let aggregate_key = AggregateKey::new(1, 2, 3);

        let metablock1 = Metablock {
            wal_seq: 42,
            server_timestamp: 1000,
            lease_epoch: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                aggregate_version: 10,
                trimmed_below_version: 1,
                min_client_seq: 1,
                max_client_seq: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_seq: 1,
                max_event_seq: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::Block(celeriant_wal::metablocks::datablock_block_ref::DatablockBlockRef { crc32c: 0 }),
            datablock_position: 1000,
            previous_aggregate_metablock_pos: 0,
        };

        let metablock2 = Metablock {
            wal_seq: 43,
            server_timestamp: 2000,
            lease_epoch: 5,
            node_id: 999,
            uncompressed_size: 2048,
            compressed_size: 1024,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [2u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                aggregate_version: 11,
                trimmed_below_version: 1,
                min_client_seq: 6,
                max_client_seq: 10,
                min_event_timestamp: 600,
                max_event_timestamp: 1000,
                min_event_seq: 6,
                max_event_seq: 10,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
            datablock_position: 0,
            previous_aggregate_metablock_pos: 0,
        };

        let datablock1 = Some(Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                aggregate_version: 10,
                events: vec![],
            }),
        });

        let original_batch = FallbackBatch {
            fallback_index: 42,
            end_wal_seq: 43,
            shard_id: 7,
            uploaded_by_node_id: 1,
            upload_sequence: 0,
            lease_epoch: 0,
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
        )
        .expect("serialization should succeed");

        let deserialized = celeriant_wire::disk::versioned_block::deserialise_fallback_batch(&serialized).expect("deserialization should succeed");

        assert_eq!(deserialized.fallback_index, 42);
        assert_eq!(deserialized.end_wal_seq, 43);
        assert_eq!(deserialized.shard_id, 7);
        assert_eq!(deserialized.items.len(), 2);

        assert_eq!(deserialized.items[0].metablock.wal_seq, 42);
        assert_eq!(deserialized.items[0].metablock.server_timestamp, 1000);
        assert!(deserialized.items[0].datablock.is_some());

        assert_eq!(deserialized.items[1].metablock.wal_seq, 43);
        assert_eq!(deserialized.items[1].metablock.server_timestamp, 2000);
        assert!(deserialized.items[1].datablock.is_none());
    }

    #[test]
    fn test_fallback_index_is_first_wal_seq() {
        let aggregate_key = AggregateKey::new(1, 2, 3);

        let metablock_first = Metablock {
            wal_seq: 100,
            server_timestamp: 1000,
            lease_epoch: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                aggregate_version: 10,
                trimmed_below_version: 1,
                min_client_seq: 1,
                max_client_seq: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_seq: 1,
                max_event_seq: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
            datablock_position: 0,
            previous_aggregate_metablock_pos: 0,
        };

        let metablock_second = Metablock {
            wal_seq: 101,
            ..metablock_first.clone()
        };

        let batch = FallbackBatch {
            fallback_index: 100,
            end_wal_seq: 101,
            shard_id: 5,
            uploaded_by_node_id: 1,
            upload_sequence: 0,
            lease_epoch: 0,
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

        assert_eq!(batch.fallback_index, batch.items[0].metablock.wal_seq);
        assert_eq!(batch.end_wal_seq, batch.items[batch.items.len() - 1].metablock.wal_seq);
    }

    #[test]
    fn test_shard_id_narrowing() {
        let batch_0 = FallbackBatch::new(1, 5, 0, 0, 0, 0);
        assert_eq!(
            fallback_batch_path(
                batch_0.shard_id,
                batch_0.fallback_index,
                batch_0.end_wal_seq,
                batch_0.uploaded_by_node_id
            ),
            "cluster/fallback/shard_000/batch_000000001_000000005_00000000-0000-0000-0000-000000000000.bin"
        );

        let batch_999 = FallbackBatch::new(1, 10, 999, 0, 0, 0);
        assert_eq!(
            fallback_batch_path(
                batch_999.shard_id,
                batch_999.fallback_index,
                batch_999.end_wal_seq,
                batch_999.uploaded_by_node_id
            ),
            "cluster/fallback/shard_999/batch_000000001_000000010_00000000-0000-0000-0000-000000000000.bin"
        );

        assert!(u32::MAX > 999);
    }

    #[test]
    fn dedup_picks_winner_from_same_start() {
        // Two batches at same start_wal_seq: the stale one (lower upload_sequence)
        // must be excluded from consideration entirely; only the winner is applied.
        // Both stay in S3 (no deletes during catchup).
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Stale: seq=1, ends at 3. Winner: seq=2, ends at 5.
            let (path_stale, data_stale) = make_fallback_batch_with_seq(0, 1, 3, GENESIS_HASH, 0, 1);
            let (path_winner, data_winner) = make_fallback_batch_with_seq(0, 1, 5, GENESIS_HASH, 0, 2);
            dl.insert(path_stale, data_stale);
            dl.insert(path_winner, data_winner);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 5, "winner (seq=2, end=5) should be applied, not stale (seq=1, end=3)");
            assert_eq!(result.batches_applied, 1);
            assert!(dl.deleted_paths().is_empty());

            tc.close().await;
        });
    }

    #[test]
    fn divergent_s3_paths_higher_lease_epoch_wins() {
        // Cross-leader scenario: prior leader (lease=1) uploaded chain A with seq=5.
        // New leader (lease=2) just uploaded chain B with seq=1. Under upload_sequence
        // alone the stale chain A would win (seq=5 > seq=1) and we'd diverge.
        // (lease_epoch, upload_sequence) lex must pick B because lease_epoch is
        // globally monotonic via S3 CAS.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path_a, data_a) = make_fallback_batch_with_lease_seq(0, 1, 3, GENESIS_HASH, 1, 5, 1);
            let (path_b, data_b) = make_fallback_batch_with_lease_seq(0, 1, 5, GENESIS_HASH, 2, 1, 2);
            dl.insert(path_a, data_a);
            dl.insert(path_b, data_b);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 5, "higher-lease chain B (lease=2, seq=1, end=5) must win over stale chain A (lease=1, seq=5, end=3)");
            assert_eq!(result.batches_applied, 1);

            tc.close().await;
        });
    }

    #[test]
    fn divergent_s3_paths_same_lease_higher_upload_sequence_wins() {
        // Within a single lease, upload_sequence remains the tiebreaker.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path_a, data_a) = make_fallback_batch_with_lease_seq(0, 1, 3, GENESIS_HASH, 1, 2, 1);
            let (path_b, data_b) = make_fallback_batch_with_lease_seq(0, 1, 5, GENESIS_HASH, 2, 5, 1);
            dl.insert(path_a, data_a);
            dl.insert(path_b, data_b);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 5, "within-lease tiebreaker: seq=5 wins over seq=2");
            assert_eq!(result.batches_applied, 1);

            tc.close().await;
        });
    }

    #[test]
    fn sequential_batches_both_applied() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path_1_3, data_1_3) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            let tip = {
                let tmp_dl = Rc::new(MockDownloader::new());
                tmp_dl.insert(path_1_3.clone(), data_1_3.clone());
                tc.catchup(&tmp_dl, 0, 10).await.unwrap();
                tc.tip_hash()
            };
            // WAL is now at 3, insert batch 4-6 with correct tip
            let (path_4_6, data_4_6) = make_fallback_batch(0, 4, 6, tip);
            dl.insert(path_4_6, data_4_6);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 6);
            assert_eq!(result.batches_applied, 1);

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
            // Batch 1-5 from node_id=0 (other node) - will be applied.
            let (path_real, data_real) = make_fallback_batch_with_node(0, 1, 5, GENESIS_HASH, 0);
            dl.insert(path_real, data_real);

            // Batch 1-3 from node_id=99 (self) - filtered out before dedup/gap check.
            // Without the filter, dedup would handle this (keeps longer 1-5), but filtering
            // is defence-in-depth: self-uploaded batches never participate in gap validation.
            let (path_self, data_self) = make_fallback_batch_with_node(0, 1, 3, GENESIS_HASH, 99);
            let self_path = path_self.clone();
            dl.insert(path_self, data_self);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 5);
            assert_eq!(result.batches_applied, 1);

            // Self-uploaded batch must NOT be deleted - the other node may need it
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
            assert_eq!(result.completion, CatchupCompletion::Caught);

            // Self-uploaded batches must NOT be deleted - the other node may need them
            assert!(dl.deleted_paths().is_empty(), "Self-uploaded batches must not be deleted from S3");
            assert_eq!(dl.objects.borrow().len(), 2, "Both S3 objects must remain");

            tc.close().await;
        });
    }

    #[test]
    fn contained_batch_skipped_when_wider_applied_first() {
        // Large batch [1-20] applied first. Smaller [5-10] is fully behind
        // next_wal_seq after that, so it's never downloaded.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path_wide, data_wide) = make_fallback_batch(0, 1, 20, GENESIS_HASH);
            let (path_stale, data_stale) = make_fallback_batch_with_node(0, 5, 10, GENESIS_HASH, 1);
            dl.insert(path_wide, data_wide);
            dl.insert(path_stale, data_stale);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 20);
            assert_eq!(result.batches_applied, 1);

            tc.close().await;
        });
    }

    #[test]
    fn partial_overlap_does_not_trigger_wal_seq_gap() {
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
            // Must NOT be WalSeqGap. overlaps are allowed
            assert!(
                !matches!(result, Err(S3CatchupError::WalSeqGap { .. })),
                "Overlapping batches must not trigger WalSeqGap, got: {:?}",
                result
            );

            tc.close().await;
        });
    }

    #[test]
    fn forward_gap_applies_prefix_and_stops() {
        // Gap at 6-9: apply [1-5], stop. Defer gap-fill to TCP.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path_a, data_a) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            let (path_b, data_b) = make_fallback_batch_with_node(0, 10, 15, GENESIS_HASH, 1);
            dl.insert(path_a, data_a);
            dl.insert(path_b, data_b);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 5);
            assert_eq!(result.batches_applied, 1);

            tc.close().await;
        });
    }

    #[test]
    fn unknown_node_batches_ignored_when_peer_known() {
        // Stale S3 batches from a previous cluster generation (different node_ids)
        // must be ignored when the current peer is known. This prevents
        // WalSeqGap errors from leftover data after a partial teardown.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let peer_id: u128 = 42;
            let stale_id: u128 = 777; // old node, not current peer

            // Batch 1-5 from current peer - should be applied
            let (path_peer, data_peer) = make_fallback_batch_with_node(0, 1, 5, GENESIS_HASH, peer_id);
            dl.insert(path_peer, data_peer);

            // Batch 10-15 from stale node - would cause WalSeqGap if not filtered
            let (path_stale, data_stale) = make_fallback_batch_with_node(0, 10, 15, GENESIS_HASH, stale_id);
            let stale_path = path_stale.clone();
            dl.insert(path_stale, data_stale);

            let result = tc.catchup_with_peer(&dl, 0, Some(peer_id)).await.unwrap();
            assert_eq!(tc.wal_seq(), 5);
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

            let result = tc.catchup_with_peer(&dl, 0, None).await.unwrap();
            assert_eq!(tc.wal_seq(), 5);
            assert_eq!(result.batches_applied, 1);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_with_speculative_tail_misses_peer_batches_if_not_culled_first() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Speculative tail: write=200, peer batch covers 1..5.
            {
                let active = tc.log_segments_cache.active();
                let mut meta = active.metadata.borrow_mut();
                meta.write.wal_seq = 200;
            }

            let (path, data) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            dl.insert(path, data);

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 200, "without cull, speculative write suppresses peer batch");

            // Simulate cull_speculative_tail_for_promotion (TestComponents has no read cursor).
            {
                let active = tc.log_segments_cache.active();
                let mut meta = active.metadata.borrow_mut();
                meta.write.wal_seq = 0;
            }

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 5, "after cull, peer batch must be applied");

            tc.close().await;
        });
    }

    /// Cross-epoch catchup liveness wedge. A demoted ex-leader holds an un-acked speculative
    /// tail (read, write] that forked from the committed chain — its entries don't chain onto
    /// the read tip. Authority arrives at read+1 with a divergent body, which both
    /// find_divergence_from_batch and the reverse scan miss, so pre-reframe the node wedges
    /// at "no common ancestor". The reframe anchors at read, truncates the tail, applies
    /// authority.
    ///
    /// The forked tip is the crux: a natural (matching) tail chains onto the read tip and
    /// self-heals via the existing scan, so this test would false-pass without it.
    #[test]
    fn catchup_reframes_forked_speculative_tail_at_read_cursor() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            // Local: committed [1..5] + un-acked speculative tail [6..8] (write = 8).
            seed_capturing_tips(&tc, 8).await;
            assert_eq!(tc.wal_seq(), 8);

            // Read cursor at 5, but its tip is the authoritative tip the forked tail never
            // chained onto. Sentinel models that disagreement.
            let read_tip = [0x5A; 32];
            {
                let active = tc.log_segments_cache.active();
                let mut m = active.metadata.borrow_mut();
                let read = m.read.as_mut().expect("seed advanced the read cursor");
                read.wal_seq = 5;
                read.tip_hash = read_tip;
                read.metablocks_position = pos_at(6);
            }

            // Authority covering the tail and extending past write, anchored at the read tip,
            // divergent body (lease_epoch 3 vs the local tail's 0).
            let dl = Rc::new(MockDownloader::new());
            let (p, d) = make_fallback_batch_with_lease_seq(0, 6, 10, read_tip, 0, 1, 3);
            dl.insert(p, d);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 10, "reframe must truncate the forked tail to read and apply authority");
            assert_eq!(result.completion, CatchupCompletion::Caught);

            tc.close().await;
        });
    }

    /// The reframe stays behind the ack barrier: a divergence at/below last_self_acked_wal_seq
    /// is refused, not truncated. Same shape as the wedge above, barrier set at read+1.
    #[test]
    fn reframe_truncate_stays_ack_barrier_gated() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            seed_capturing_tips(&tc, 8).await;

            let read_tip = [0x5A; 32];
            {
                let active = tc.log_segments_cache.active();
                let mut m = active.metadata.borrow_mut();
                let read = m.read.as_mut().expect("seed advanced the read cursor");
                read.wal_seq = 5;
                read.tip_hash = read_tip;
                read.metablocks_position = pos_at(6);
                // Barrier at/above the reframe divergence (read+1 = 6): refuse.
                m.last_self_acked_wal_seq = 6;
            }

            let dl = Rc::new(MockDownloader::new());
            let (p, d) = make_fallback_batch_with_lease_seq(0, 6, 10, read_tip, 0, 1, 3);
            dl.insert(p, d);

            let err = tc.catchup(&dl, 0, 10).await.unwrap_err();
            assert!(
                matches!(err, S3CatchupError::TruncationFailed(ShardFsyncError::TruncateRefusedByAckBarrier { .. })),
                "expected ack-barrier refusal under the reframe, got {err:?}",
            );
            assert_eq!(tc.wal_seq(), 8, "barrier refusal must not regress write");

            tc.close().await;
        });
    }

    /// The reframe must not fire when no batch chains onto the read tip. A forked tail whose
    /// authority doesn't anchor at read+1 stays a no-truncate Retry, guarding over-firing.
    #[test]
    fn reframe_does_not_fire_without_read_anchor() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            seed_capturing_tips(&tc, 8).await;

            let read_tip = [0x5A; 32];
            {
                let active = tc.log_segments_cache.active();
                let mut m = active.metadata.borrow_mut();
                let read = m.read.as_mut().expect("seed advanced the read cursor");
                read.wal_seq = 5;
                read.tip_hash = read_tip;
                read.metablocks_position = pos_at(6);
            }

            // Authority covers [6..10] but does NOT chain onto our read tip (prev != read_tip),
            // so nothing anchors at read+1.
            let dl = Rc::new(MockDownloader::new());
            let (p, d) = make_fallback_batch_with_lease_seq(0, 6, 10, [0xC3; 32], 0, 1, 3);
            dl.insert(p, d);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.completion, CatchupCompletion::Retry, "no read-anchor: must stay conservative Retry");
            assert_eq!(tc.wal_seq(), 8, "no truncate without a read-anchor");

            tc.close().await;
        });
    }

    #[test]
    fn duplicate_start_picks_winner_and_applies() {
        // Two batches at start=48 (pre-rollback and post-rollback generations).
        // The one with higher upload_sequence is authoritative; the other is stale
        // and must be excluded, not just deprioritized.
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Advance WAL to 47
            let (path, data) = make_fallback_batch(0, 1, 47, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 47);
            let tip = tc.tip_hash();

            // Stale: seq=1, ends at 52. Winner: seq=2, ends at 60.
            let (path_stale, data_stale) = make_fallback_batch_with_seq(0, 48, 52, tip, 0, 1);
            let (path_winner, data_winner) = make_fallback_batch_with_seq(0, 48, 60, tip, 0, 2);
            dl.insert(path_stale, data_stale);
            dl.insert(path_winner, data_winner);

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_seq(), 60, "winner (seq=2, end=60) should be applied, not stale (seq=1, end=52)");

            tc.close().await;
        });
    }

    // ── Ack barrier ──

    #[test]
    fn truncate_succeeds_when_last_self_acked_below_divergent() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = divergence_at_6(&tc).await;
            tc.log_segments_cache.active().metadata.borrow_mut().last_self_acked_wal_seq = 3;

            tc.catchup(&dl, 0, 10).await.unwrap();

            assert_eq!(tc.wal_seq(), 8);
            tc.close().await;
        });
    }

    #[test]
    fn truncate_barrier_ignores_last_received_replication_wal_seq() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = divergence_at_6(&tc).await;
            {
                let active = tc.log_segments_cache.active();
                let mut meta = active.metadata.borrow_mut();
                meta.last_self_acked_wal_seq = 3;
                meta.last_received_replication_wal_seq = 99;
            }

            tc.catchup(&dl, 0, 10).await.unwrap();

            assert_eq!(tc.wal_seq(), 8);
            tc.close().await;
        });
    }

    // ── refine_divergence_by_byte_match ──

    #[test]
    fn refine_advances_through_byte_identical_prefix() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let tips = seed_capturing_tips(&tc, 8).await;
            let log_id = tc.log_segments_cache.active_log_id();

            // Batch entries 5,6 byte-match local; entry 7 uses a different
            // aggregate_key so its body differs from local's wal_seq=7.
            let mb5 = test_metablock(5, tips[3]);
            let mb6 = test_metablock(6, tips[4]);
            let mb7 = test_metablock_for_agg(7, tips[5], AggregateKey::new(2, 2, 2));
            let batch: Vec<&Metablock> = vec![&mb5, &mb6, &mb7];

            let result = refine_divergence_by_byte_match(
                &tc.log_segments_cache, &batch, tips[3], log_id, 5, pos_at(5),
            ).await;

            assert_eq!(result, (tips[5], log_id, 7, pos_at(7)));
            tc.close().await;
        });
    }

    #[test]
    fn refine_falls_back_on_chain_mismatch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let tips = seed_capturing_tips(&tc, 6).await;
            let log_id = tc.log_segments_cache.active_log_id();

            // batch[0].previous_tip_hash deliberately doesn't match the ancestor we pass.
            let mb5 = test_metablock(5, [0xCC; 32]);
            let batch: Vec<&Metablock> = vec![&mb5];

            let result = refine_divergence_by_byte_match(
                &tc.log_segments_cache, &batch, tips[3], log_id, 5, pos_at(5),
            ).await;

            assert_eq!(result, (tips[3], log_id, 5, pos_at(5)));
            tc.close().await;
        });
    }

    #[test]
    fn refine_falls_back_on_sealed_segment_crossing() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let foreign_log_id = tc.log_segments_cache.active_log_id().wrapping_add(1);
            let mb = test_metablock(5, [0; 32]);
            let batch: Vec<&Metablock> = vec![&mb];

            let result = refine_divergence_by_byte_match(
                &tc.log_segments_cache, &batch, [0xAA; 32], foreign_log_id, 5, 999,
            ).await;

            assert_eq!(result, ([0xAA; 32], foreign_log_id, 5, 999));
            tc.close().await;
        });
    }

    // ── Drain barrier tests ──

    /// A predecessor's S3 upload lands AFTER our final list but BEFORE serving.
    /// The drain barrier must detect it and cause it to be applied before declaring Caught.
    ///
    /// List call index map for this scenario:
    ///   0 — outer iteration 1: finds batch 1..=3, inner loop applies it
    ///   1 — outer iteration 2: finds batch 1..=3 only (end=3 < next=4); enters drain
    ///   2 — drain round 0: nothing new
    ///   3 — drain round 1: hook injects batch 4..=5 → drain returns true
    ///   4 — outer iteration 3: finds batches 1..=3 and 4..=5; applies 4..=5
    ///   5 — outer iteration 4: nothing covering next=6; enters drain; settle window cleans → Caught
    ///
    /// Without the barrier, the old code would have declared Caught at list call 1,
    /// missing batch 4..=5 entirely (final wal_seq=3 instead of 5).
    #[test]
    fn drain_barrier_catches_late_landing_predecessor_file() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Predecessor's batch 1..=3 is already visible.
            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            dl.insert(path, data);

            // Simulate a late-landing predecessor upload: batch 4..=5 becomes
            // list-visible only during drain round 1 (list call 3).
            let lsc = tc.log_segments_cache.clone();
            dl.on_list(3, move |dl| {
                let tip = lsc.active().metadata.borrow().write.tip_hash;
                let (path, data) = make_fallback_batch(0, 4, 5, tip);
                dl.insert(path, data);
            });

            let result = tc.catchup(&dl, 0, 10).await.unwrap();

            // The drain barrier must have caught the late file and applied it.
            assert_eq!(
                tc.wal_seq(), 5,
                "drain barrier must apply the late-landing batch 4..=5"
            );
            assert_eq!(result.completion, CatchupCompletion::Caught);
            // At minimum: batch 1..=3 and batch 4..=5
            assert!(
                result.batches_applied >= 2,
                "expected at least 2 batches applied, got {}", result.batches_applied
            );
        });
    }

    /// When the drain window passes with no new covering files, Caught is declared
    /// immediately (no regression on the common / no-late-files path).
    #[test]
    fn drain_barrier_stable_window_declares_caught() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            dl.insert(path, data);

            // No late injections — drain rounds all return empty.
            let result = tc.catchup(&dl, 0, 10).await.unwrap();

            assert_eq!(tc.wal_seq(), 3);
            assert_eq!(result.completion, CatchupCompletion::Caught);
        });
    }

    /// A file strictly ahead of an unfilled gap (start > next_wal_seq) is not a
    /// bridging predecessor and must not hold the drain barrier open. Otherwise an
    /// unfillable middle gap (corrupt/deleted batch) wedges catchup forever instead
    /// of handing the gap to TCP extended catchup.
    #[test]
    fn drain_barrier_ignores_file_beyond_gap() {
        glommio_test!({
            let dl = Rc::new(MockDownloader::new());
            // Waiting on seq 3, but the only visible file covers 5..=5 — ahead of the gap.
            let (path, data) = make_fallback_batch(0, 5, 5, GENESIS_HASH);
            dl.insert(path, data);

            let prefix = celeriant_distributed::paths::fallback_shard_prefix(0);
            let late = drain_settle_barrier(&dl, &prefix, 0, 99, None, 3, &std::collections::HashSet::new())
                .await
                .unwrap();

            assert!(!late, "a file ahead of the gap must not count as a late predecessor");
        });
    }

    /// Catchup full-commits on apply, jumping the read cursor to the extended
    /// write tip. Live-TCP commits still parked below that tip must commit
    /// FIRST: the read cursor never regresses and their watch events fire
    /// exactly once, before the catchup batch's own events.
    #[test]
    fn catchup_commits_parked_deferred_batches_before_full_commit() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let (_id, subscriber) = tc.watched_aggregates.add_subscriber(watch_everything());

            // Follower live-TCP apply of entries 1..=2, deferred: durable, parked,
            // read still at 0.
            apply_deferred_live_batch(&tc, 1, 2).await;
            assert_eq!(tc.shard_mem_cache.borrow().parked_commit_count(), 1, "scaffolding: apply must park");
            assert!(drain_watch_events(&subscriber).await.is_empty(), "nothing may fire before catchup");

            // S3 catchup extends the chain with 3..=4.
            let dl = Rc::new(MockDownloader::new());
            let (p, d) = make_fallback_batch(0, 3, 4, tc.tip_hash());
            dl.insert(p, d);
            tc.catchup(&dl, 0, 10).await.unwrap();

            assert_eq!(tc.shard_mem_cache.borrow().parked_commit_count(), 0, "catchup must commit parked batches");
            let meta = tc.log_segments_cache.active().metadata.borrow().clone();
            assert_eq!(meta.write.wal_seq, 4);
            assert_eq!(meta.read.as_ref().map_or(0, |r| r.wal_seq), 4, "catchup full-commits to the new tip");

            let writes: Vec<(u64, u64)> = drain_watch_events(&subscriber)
                .await
                .into_iter()
                .filter_map(|e| match e.operation {
                    celeriant_watch::aggregate_watch_event::AggregateWatchEventOperation::Write {
                        from_aggregate_version, to_aggregate_version,
                    } => Some((from_aggregate_version, to_aggregate_version)),
                    _ => None,
                })
                .collect();
            assert_eq!(writes.len(), 2, "one parked broadcast then one catchup broadcast, got {writes:?}");
            assert_eq!(writes[0], (101, 102), "parked events must fire first, exactly once");

            tc.close().await;
        });
    }

    /// Apply entries `first..=last` the way the follower live-TCP path does:
    /// queue, fsync with the deferred target, park the read-side commit.
    /// Aggregate versions are `100 + seq` so watch assertions can identify entries.
    async fn apply_deferred_live_batch(tc: &TestComponents, first: u64, last: u64) {
        let mut items = Vec::new();
        let mut prev = tc.tip_hash();
        for seq in first..=last {
            let mut mb = test_metablock(seq, prev);
            if let MetablockKind::EventBatchMetadata(ref mut eb) = mb.wal_metablock_type {
                eb.aggregate_version = 100 + seq;
            }
            prev = [0u8; 32]; // only the first item's prev hash is chain-checked
            items.push(ReplicationBatchItem { metablock: mb, datablock: None });
        }
        apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &items, &test_codec()).unwrap();
        let lsc = tc.log_segments_cache.clone();
        let smc = tc.shard_mem_cache.clone();
        let wa = tc.watched_aggregates.clone();
        let mc_capture = smc.clone();
        tc.fsync_coordinator
            .request_sync_two_phase(
                None,
                ShardFsyncError::WriteLockTimeout,
                move || capture_fsync_snapshot(&mc_capture),
                move |captured| commit_fsync_with_rollback(
                    NodeStatus::Follower { leader_lease_epoch: 0 },
                    CommitTarget::DeferToLeaderConfirmed,
                    lsc, smc, wa, captured, 0, test_codec(),
                ),
            )
            .await
            .unwrap();
    }

    /// Divergence truncation with parked deferred commits spanning the
    /// divergence point: whole batches below it commit and fire exactly once;
    /// the straddling batch's surviving items fire their events and land in
    /// the ACTIVE segment summary (their entries stay on the chain and become
    /// visible via the truncated-tip cursor); nothing at-or-past the divergence
    /// fires or is summarised; the read cursor lands on the truncated tip.
    /// Covers the active-accumulator case only — a sealed divergent segment's
    /// summary slot is cleared in Step 2b (pre-existing on main).
    #[test]
    fn truncate_commits_surviving_parked_prefix_and_discards_the_rest() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let (_id, subscriber) = tc.watched_aggregates.add_subscriber(watch_everything());

            // Whole survivor 1..=2, straddler 3..=5, wholly discarded 6..=7;
            // divergence at 5.
            apply_deferred_live_batch(&tc, 1, 2).await;
            let pos_after_2 = tc.log_segments_cache.active().metadata.borrow().write.metablocks_position;
            apply_deferred_live_batch(&tc, 3, 5).await;
            apply_deferred_live_batch(&tc, 6, 7).await;
            assert_eq!(tc.shard_mem_cache.borrow().parked_commit_count(), 3, "scaffolding: all batches parked");
            assert!(drain_watch_events(&subscriber).await.is_empty(), "nothing may fire before the truncate");

            // Entry 5 starts two metablocks past the end of entry 2.
            let divergent_pos = pos_after_2 + 2 * FIXED_BLOCK_SIZE_BYTES as u64;
            truncate_wal(
                &tc.log_segments_cache,
                &tc.shard_mem_cache,
                &tc.fsync_coordinator,
                &tc.watched_aggregates,
                &test_codec(),
                [9u8; 32],
                1,
                5,
                divergent_pos,
            )
            .await
            .unwrap();

            assert_eq!(tc.shard_mem_cache.borrow().parked_commit_count(), 0);
            let meta = tc.log_segments_cache.active().metadata.borrow().clone();
            assert_eq!(meta.write.wal_seq, 4, "write rewinds onto the truncated tip");
            assert_eq!(meta.read.as_ref().map_or(0, |r| r.wal_seq), 4, "read lands on the truncated tip");

            let writes: Vec<(u64, u64)> = drain_watch_events(&subscriber)
                .await
                .into_iter()
                .filter_map(|e| match e.operation {
                    celeriant_watch::aggregate_watch_event::AggregateWatchEventOperation::Write {
                        from_aggregate_version, to_aggregate_version,
                    } => Some((from_aggregate_version, to_aggregate_version)),
                    _ => None,
                })
                .collect();
            assert_eq!(
                writes, [(101, 102), (103, 104)],
                "whole survivor then the straddler's surviving slice, exactly once each; \
                 versions 105..=107 never fire"
            );

            // The straddler's surviving items must reach the segment summary —
            // losing them silently drops committed data from listings forever.
            let summary = tc.shard_mem_cache.borrow_mut().take_segment_summary();
            let entry = summary.aggregates.iter().find(|a| a.aggregate_id == 1)
                .expect("summary entry for the aggregate must exist");
            assert_eq!(entry.event_batch_count, 4, "entries 1..=4 summarised, 5..=7 not");
            assert_eq!(entry.last_aggregate_version, 104, "summary must stop at the surviving slice");

            tc.close().await;
        });
    }

    fn watch_everything() -> celeriant_msg::request::requests::WatchRequest {
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

    async fn drain_watch_events(
        subscriber: &Rc<RefCell<celeriant_watch::subscribed_client::SubscribedClient>>,
    ) -> Vec<celeriant_watch::aggregate_watch_event::AggregateWatchEvent> {
        glommio::timer::sleep(std::time::Duration::from_millis(10)).await;
        let mut events = Vec::new();
        while let Some(e) = futures_lite::future::poll_once(subscriber.borrow().receiver.recv()).await.flatten() {
            events.push(e);
        }
        events
    }

    /// A parked deferred entry whose range the catchup's divergence check
    /// truncates must never fire its watch events, and the competing S3 chain
    /// wins with the read cursor landing on its tip. This is the mechanism the
    /// promotion ordering relies on: the tail commit runs only AFTER this
    /// catchup, so a rolled-back-then-re-authored entry can never be committed
    /// (events fired, read persisted) and then truncated.
    #[test]
    fn catchup_divergence_discards_parked_fork_without_events() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = Rc::new(MockDownloader::new());

            // Shared prefix 1..=5 via catchup (full-commit: read == write == 5).
            let (p, d) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            dl.insert(p, d);
            tc.catchup(&dl, 0, 10).await.unwrap();
            let tip_after_5 = tc.tip_hash();
            let (_id, subscriber) = tc.watched_aggregates.add_subscriber(watch_everything());

            // The old leader delivers entry 6 (version 106) over live TCP — it
            // parks — then rolls it back and re-authors 6..=7 into S3 fallback.
            apply_deferred_live_batch(&tc, 6, 6).await;
            assert_eq!(tc.shard_mem_cache.borrow().parked_commit_count(), 1, "scaffolding: fork parked");
            assert!(drain_watch_events(&subscriber).await.is_empty(), "nothing may fire before catchup");

            dl.objects.borrow_mut().clear();
            let (p, d) = make_fallback_batch(0, 6, 7, tip_after_5);
            dl.insert(p, d);
            tc.catchup(&dl, 0, 10).await.unwrap();

            assert_eq!(tc.shard_mem_cache.borrow().parked_commit_count(), 0, "the truncate must not orphan the parked fork");
            let meta = tc.log_segments_cache.active().metadata.borrow().clone();
            assert_eq!(
                (meta.write.wal_seq, meta.read.as_ref().map_or(0, |r| r.wal_seq)), (7, 7),
                "the S3 chain wins and commits",
            );

            let writes: Vec<u64> = drain_watch_events(&subscriber)
                .await
                .into_iter()
                .filter_map(|e| match e.operation {
                    celeriant_watch::aggregate_watch_event::AggregateWatchEventOperation::Write {
                        to_aggregate_version, ..
                    } => Some(to_aggregate_version),
                    _ => None,
                })
                .collect();
            assert!(!writes.contains(&106), "events for the truncated fork must never fire: {writes:?}");
            // The re-authored entries share one aggregate, so the catchup's single
            // broadcast coalesces them into one Write event.
            assert!(!writes.is_empty(), "the winning chain's events must fire");

            tc.close().await;
        });
    }
}
