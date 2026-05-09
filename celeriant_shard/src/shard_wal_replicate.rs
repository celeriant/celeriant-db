use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};
use celeriant_disk::files::rwlock_timeout::with_budget;

use celeriant_distributed::validated_node_status::{ValidatedNodeStatus, set_node_status_and_metric};
use celeriant_msg::request::requests::ReplicationBatchItem;
use celeriant_rotating_log::log_segment_file::aggregate_key_bloom::AggregateKeyBloom;
use celeriant_rotating_log::log_segment_file::log_segment_file::{read_datablocks_carry_over_bytes, write_dual_shard_log_header};
use celeriant_wal::shard_log_header::ShardLogHeader;
use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::pending_commit_data::PendingCommitData;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use crate::schema_validator::CompiledValidator;

type MemCache = ShardMemCache<CompiledValidator>;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::constants::{FIRST_EVENT_BATCH_INDEX, GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES};
use celeriant_wal::segment_summary::SegmentSummaryPayload;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_watch::aggregate_watchers::AggregateWatchers;

use crate::amortisation::coordinator::{CaptureResult, Coordinator};
use crate::intra_batch_chain::validate_intra_batch_chain;
use crate::shard_wal_sync::write_segment_summary_sidecar_from_payload;
use crate::error::replication_error::ReplicationError;
use crate::error::replication_rollback_failure::ReplicationRollbackFailure;
use crate::error::replication_to_follower_error::ReplicateToFollowerError;
use crate::error::replication_to_s3_error::ReplicateToS3Error;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::replication_client::ReplicationClient;
use crate::watch_event_collector::WatchEventCollector;

/// What kicked off this replication cycle. `Probe` lets the coordinator path
/// run with an empty pending-replication queue: when the snapshot is empty,
/// `replicate_loop` synthesises a single-entry batch from the latest local
/// wal_index to flush out a follower drift gap.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReplicationTrigger {
    Write,
    Probe,
}

/// Captured data from the replication snapshot phase.
pub(crate) struct ReplicationCapturedData {
    pub replication_snapshot: Vec<PendingCommitData>,
}

/// Capture phase: take replication snapshot from memcache.
/// Must be called while the coordinator still holds the orchestrator event.
pub(crate) fn capture_replication_snapshot(
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    trigger: ReplicationTrigger,
) -> CaptureResult<ReplicationCapturedData, ReplicationError> {
    let mut cache = shard_mem_cache.borrow_mut();

    metrics::gauge!("celeriant_replication_queue_bytes").set(cache.pending_replication_bytes() as f64);
    let replication_snapshot = cache.take_pending_replication();

    if cache.take_replication_rollback_flag() {
        return CaptureResult::Failed(ReplicationError::RollbackInProgress);
    }

    if replication_snapshot.is_empty() && matches!(trigger, ReplicationTrigger::Write) {
        return CaptureResult::NoCaptureRaceButOk;
    }

    CaptureResult::Captured(ReplicationCapturedData {
        replication_snapshot,
    })
}

pub enum ReplicationDetails {
    ReplicatedToFollower,
    ReplicatedToS3,
}

/// Outcome of trying to send the whole snapshot in one TCP request.
enum SnapshotSendOutcome {
    Sent,
    FallbackToS3,
}

/// Outcome of one TCP send attempt against the follower.
enum SingleSendOutcome {
    Ok,
    /// Network/lock failure, or follower rejected for a non-mismatch reason.
    Failed,
    /// Follower is behind. `max_follower_wal_index` is the follower's tip.
    /// The caller can run catchup and retry.
    WalIndexMismatch { max_follower_wal_index: u64 },
}

pub(crate) async fn commit_replication_with_rollback<R: ReplicationClient + 'static>(
    replication_client: Rc<R>,
    fsync_coordinator: Rc<Coordinator<ShardFsyncError>>,
    log_segments_cache: Rc<LogSegmentsCache>,
    shard_mem_cache: Rc<RefCell<MemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    node_status: Rc<Cell<ValidatedNodeStatus>>,
    last_rollback_at: Rc<Cell<Option<Instant>>>,
    replication_captured_data: ReplicationCapturedData,
    trigger: ReplicationTrigger,
    max_catchup_gap_bytes: Option<u64>,
    max_request_size: u64,
    read_max_chunk_size: u64,
    max_clock_drift_ms: u64,
    shard_id: u32,
) -> Result<(), ReplicationError> {
    let start = Instant::now();
    let shard_label = [("shard_id", shard_id.to_string())];
    let ReplicationCapturedData { mut replication_snapshot } = replication_captured_data;
    let initial_batch_count: usize = replication_snapshot.iter().map(|c| c.pending_queue.len()).sum();

    let outcome = replicate_loop(
        &replication_client, &log_segments_cache, &shard_mem_cache, &watched_aggregates, &node_status,
        &mut replication_snapshot, 
        max_catchup_gap_bytes, max_request_size, read_max_chunk_size, max_clock_drift_ms, shard_id,
    ).await;

    // Sweep memcache for any rotated-and-sealed segments whose read cursor has now caught up
    let sealed_ready = collect_eligible_sealed_summaries(&log_segments_cache, &shard_mem_cache);
    for (log_id, payload) in sealed_ready {
        if let Err(e) = write_segment_summary_sidecar_from_payload(log_segments_cache.shard_dir(), log_id, payload).await {
            error!(shard_id, log_id, error = ?e, "Failed to write segment summary sidecar");
        }
    }

    let details = match outcome {
        Ok(d) => d,
        Err(e) => {
            // Probes don't need to rollback, zero fsync batch captures
            if matches!(trigger, ReplicationTrigger::Probe) {
                return Err(e);
            }
            return finish_with_rollback(&fsync_coordinator, &log_segments_cache, &shard_mem_cache, &last_rollback_at, replication_snapshot, e, shard_id).await;
        }
    };

    log_replication_outcome(initial_batch_count, &details, start, shard_id);

    metrics::histogram!("celeriant_replication_duration_seconds", &shard_label).record(start.elapsed().as_secs_f64());
    metrics::histogram!("celeriant_replication_batch_size", &shard_label).record(initial_batch_count as f64);
    Ok(())
}

async fn replicate_loop<R: ReplicationClient + 'static>(
    replication_client: &Rc<R>,
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    node_status: &Rc<Cell<ValidatedNodeStatus>>,
    snapshot: &mut Vec<PendingCommitData>,
    max_catchup_gap_bytes: Option<u64>,
    max_request_size: u64,
    read_max_chunk_size: u64,
    max_clock_drift_ms: u64,
    shard_id: u32,
) -> Result<ReplicationDetails, ReplicationError> {
    if !node_status.get().is_leader() {
        warn!(shard_id, "Leader fenced before replication");
        return Err(ReplicationError::LeaderFenced);
    }

    let reachable_at_commit = replication_client.is_follower_reachable();

    if snapshot.is_empty() {
        return run_probe_send(
            replication_client, log_segments_cache, node_status,
            reachable_at_commit, max_catchup_gap_bytes, max_request_size,
            read_max_chunk_size, max_clock_drift_ms, shard_id,
        ).await;
    }

    if !reachable_at_commit {
        debug!(shard_id, "Follower unreachable, will use S3 fallback");
    }

    let initial_workset_bytes: u64 = snapshot.iter().map(|c| c.size_bytes()).sum();
    let force_s3 = !reachable_at_commit
        || max_catchup_gap_bytes.is_some_and(|cap| initial_workset_bytes > cap);

    if force_s3 {
        run_s3_fallback(
            replication_client, log_segments_cache, shard_mem_cache, watched_aggregates,
            node_status, snapshot, shard_id,
        ).await?;
        return Ok(ReplicationDetails::ReplicatedToS3);
    }

    match tcp_send_snapshot(
        replication_client, log_segments_cache, node_status, snapshot,
        max_request_size, max_catchup_gap_bytes, read_max_chunk_size, max_clock_drift_ms, shard_id,
    ).await? {
        SnapshotSendOutcome::Sent => {
            let pcds = std::mem::take(snapshot);
            for pcd in pcds {
                commit_pcd(log_segments_cache, shard_mem_cache, watched_aggregates, pcd);
            }
            Ok(ReplicationDetails::ReplicatedToFollower)
        }
        SnapshotSendOutcome::FallbackToS3 => {
            run_s3_fallback(
                replication_client, log_segments_cache, shard_mem_cache, watched_aggregates,
                node_status, snapshot, shard_id,
            ).await?;
            Ok(ReplicationDetails::ReplicatedToS3)
        }
    }
}

async fn run_probe_send<R: ReplicationClient + 'static>(
    replication_client: &Rc<R>,
    log_segments_cache: &Rc<LogSegmentsCache>,
    node_status: &Rc<Cell<ValidatedNodeStatus>>,
    reachable: bool,
    max_catchup_gap_bytes: Option<u64>,
    max_request_size: u64,
    read_max_chunk_size: u64,
    max_clock_drift_ms: u64,
    shard_id: u32,
) -> Result<ReplicationDetails, ReplicationError> {
    if !reachable {
        return Ok(ReplicationDetails::ReplicatedToFollower);
    }

    let leader_wal_index = {
        let active = log_segments_cache.active();
        let metadata = active.metadata.borrow();
        if metadata.write.wal_index == 0 {
            return Ok(ReplicationDetails::ReplicatedToFollower);
        }
        metadata.write.wal_index
    };

    metrics::counter!("celeriant_probe_total").increment(1);
    let probe_start = Instant::now();

    let mut probe_entries = match crate::fetch_catchup_entries::fetch_catchup_entries(
        log_segments_cache,
        leader_wal_index.saturating_sub(1),
        leader_wal_index.saturating_add(1),
        Some(max_request_size),
        read_max_chunk_size,
    ).await {
        Ok(e) => e,
        Err(e) => {
            debug!(shard_id, error = ?e, "probe: failed to read local entry, skipping");
            return Ok(ReplicationDetails::ReplicatedToFollower);
        }
    };
    if probe_entries.is_empty() {
        return Ok(ReplicationDetails::ReplicatedToFollower);
    }
    if let Err(e) = crate::collect_from_disk::fetch_datablocks_for_metablocks(
        &mut probe_entries, read_max_chunk_size, log_segments_cache,
    ).await {
        debug!(shard_id, error = ?e, "probe: datablock fetch failed, skipping");
        return Ok(ReplicationDetails::ReplicatedToFollower);
    }

    let probe_items: Vec<ReplicationBatchItem> = probe_entries.into_iter()
        .map(|e| ReplicationBatchItem { metablock: e.metablock, datablock: e.datablock })
        .collect();
    info!(shard_id, leader_wal_index, "Sending reconciliation probe to follower");

    let outcome = single_send(replication_client, node_status, probe_items, max_clock_drift_ms, shard_id).await?;
    match outcome {
        SingleSendOutcome::Ok => {
            metrics::counter!("celeriant_probe_outcome_current_total").increment(1);
            debug!(shard_id, leader_wal_index, "Probe: follower already current");
        }
        SingleSendOutcome::WalIndexMismatch { max_follower_wal_index } => {
            let gap_size = leader_wal_index.saturating_sub(max_follower_wal_index);
            metrics::counter!("celeriant_probe_outcome_gap_detected_total").increment(1);
            metrics::histogram!("celeriant_probe_gap_size").record(gap_size as f64);
            info!(
                shard_id, follower_wal = max_follower_wal_index, leader_wal = leader_wal_index,
                gap = gap_size, "Probe: follower behind, running catchup",
            );
            match crate::replicate_follower_catchup::replicate_follower_catchup(
                replication_client, log_segments_cache, node_status,
                max_follower_wal_index, leader_wal_index.saturating_add(1),
                max_catchup_gap_bytes, max_request_size, read_max_chunk_size, shard_id,
            ).await? {
                crate::replicate_follower_catchup::CatchupOutcome::Caught => {
                    metrics::counter!("celeriant_probe_gap_send_success_total").increment(1);
                }
                crate::replicate_follower_catchup::CatchupOutcome::FallbackToS3 => {
                    metrics::counter!("celeriant_probe_gap_send_failed_total").increment(1);
                    debug!(shard_id, "Probe catchup needs S3 fallback; next real write will route through S3");
                }
            }
        }
        SingleSendOutcome::Failed => {
            metrics::counter!("celeriant_probe_outcome_network_error_total").increment(1);
            debug!(shard_id, "Probe send failed; normal paths will retry");
        }
    }
    metrics::histogram!("celeriant_probe_duration_seconds").record(probe_start.elapsed().as_secs_f64());
    Ok(ReplicationDetails::ReplicatedToFollower)
}

/// Drain every remaining PCD into one S3 batch. On success, commit each PCD inline so
/// the read cursor advances and the PCDs leave the snapshot. On failure, the snapshot is
/// untouched (caller routes it into rollback).
async fn run_s3_fallback<R: ReplicationClient + 'static>(
    replication_client: &Rc<R>,
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    node_status: &Rc<Cell<ValidatedNodeStatus>>,
    snapshot: &mut Vec<PendingCommitData>,
    shard_id: u32,
) -> Result<(), ReplicationError> {
    if snapshot.is_empty() {
        return Ok(());
    }

    let s3_budget = acquire_lease_budget(node_status, "s3_fallback", shard_id)?;
    metrics::counter!("celeriant_replication_s3_fallbacks_total", &[("shard_id", shard_id.to_string())]).increment(1);

    let items: Vec<ReplicationBatchItem> = snapshot.iter()
        .flat_map(|pcd| pcd.pending_queue.iter())
        .map(|item| ReplicationBatchItem {
            metablock: item.metablock.clone(),
            datablock: item.datablock.clone(),
        })
        .collect();

    let workset_size_bytes: u64 = items.iter().map(|c| c.size_bytes()).sum();
    let first_wal = items.first().map(|b| b.metablock.wal_index).unwrap_or(0);
    let last_wal = items.last().map(|b| b.metablock.wal_index).unwrap_or(0);

    validate_chain_or_err(&items, shard_id).map_err(ReplicationError::ReplicateToS3Error)?;

    warn!(shard_id, batch_count = items.len(), first_wal, last_wal, workset_size_bytes, "S3 fallback: uploading batch");
    let s3_start = Instant::now();
    let upload = with_budget(s3_budget, replication_client.replicate_to_s3(items))
        .await
        .ok_or_else(|| {
            metrics::counter!("celeriant_lease_budget_exhausted_total", &[("op", "s3_fallback")]).increment(1);
            ReplicationError::BudgetExhausted
        })?;
    upload.map_err(|e| {
        error!(shard_id, error = ?e, "S3 fallback upload failed");
        ReplicationError::ReplicateToS3Error(e)
    })?;

    let s3_ms = s3_start.elapsed().as_millis() as u64;
    if s3_ms > 500 {
        warn!(shard_id, s3_ms, workset_size_bytes, "S3 fallback upload slow");
    }
    spawn_kick(replication_client, node_status);

    // S3 owns durability for the entire workset now. Commit every PCD in order so that read
    // cursors advance and downstream consumers see the new visible state.
    let pcds = std::mem::take(snapshot);
    for pcd in pcds {
        commit_pcd(log_segments_cache, shard_mem_cache, watched_aggregates, pcd);
    }
    Ok(())
}

/// Try to ship the whole snapshot in one TCP request. On `WalIndexMismatch`,
/// invoke catchup once and retry; a second mismatch (or any other failure)
/// signals fallback to S3. The leader's commit step happens in `replicate_loop`
/// after this returns `Sent`.
async fn tcp_send_snapshot<R: ReplicationClient + 'static>(
    replication_client: &Rc<R>,
    log_segments_cache: &Rc<LogSegmentsCache>,
    node_status: &Rc<Cell<ValidatedNodeStatus>>,
    snapshot: &[PendingCommitData],
    max_request_size: u64,
    max_catchup_gap_bytes: Option<u64>,
    read_max_chunk_size: u64,
    max_clock_drift_ms: u64,
    shard_id: u32,
) -> Result<SnapshotSendOutcome, ReplicationError> {
    debug_assert!(!snapshot.is_empty());

    let items: Vec<ReplicationBatchItem> = snapshot.iter()
        .flat_map(|pcd| pcd.pending_queue.iter())
        .map(|item| ReplicationBatchItem {
            metablock: item.metablock.clone(),
            datablock: item.datablock.clone(),
        })
        .collect();
    let leader_first_wal = items.first().map(|i| i.metablock.wal_index).unwrap_or(0);

    match single_send(replication_client, node_status, items.clone(), max_clock_drift_ms, shard_id).await? {
        SingleSendOutcome::Ok => return Ok(SnapshotSendOutcome::Sent),
        SingleSendOutcome::Failed => return Ok(SnapshotSendOutcome::FallbackToS3),
        SingleSendOutcome::WalIndexMismatch { max_follower_wal_index } => {
            debug!(shard_id, max_follower_wal_index, leader_first_wal, "Follower behind; running catchup");
            match crate::replicate_follower_catchup::replicate_follower_catchup(
                replication_client, log_segments_cache, node_status,
                max_follower_wal_index, leader_first_wal,
                max_catchup_gap_bytes, max_request_size, read_max_chunk_size, shard_id,
            ).await? {
                crate::replicate_follower_catchup::CatchupOutcome::Caught => {}
                crate::replicate_follower_catchup::CatchupOutcome::FallbackToS3 => {
                    return Ok(SnapshotSendOutcome::FallbackToS3);
                }
            }
        }
    }

    // Retry the original send exactly once. A second WalIndexMismatch means
    // local-WAL replay can't fix this follower in one cycle; escalate to S3.
    match single_send(replication_client, node_status, items, max_clock_drift_ms, shard_id).await? {
        SingleSendOutcome::Ok => Ok(SnapshotSendOutcome::Sent),
        SingleSendOutcome::Failed | SingleSendOutcome::WalIndexMismatch { .. } => {
            Ok(SnapshotSendOutcome::FallbackToS3)
        }
    }
}

/// One TCP round-trip against the follower. Maps low-level errors into the
/// caller's three-way outcome. `TipHashMismatch` short-circuits to `Failed`
/// after spawning a follower kick, since local-WAL catchup cannot fix a
/// divergent tip. `StaleLease` self-fences when the follower's known leader
/// lease has moved past ours (genuine split-brain).
async fn single_send<R: ReplicationClient + 'static>(
    replication_client: &Rc<R>,
    node_status: &Rc<Cell<ValidatedNodeStatus>>,
    items: Vec<ReplicationBatchItem>,
    max_clock_drift_ms: u64,
    shard_id: u32,
) -> Result<SingleSendOutcome, ReplicationError> {
    if !node_status.get().is_leader() {
        return Err(ReplicationError::LeaderFenced);
    }

    let tcp_budget = acquire_lease_budget(node_status, "replicate", shard_id)?;
    let tcp_start = Instant::now();
    debug!(shard_id, batch_count = items.len(), "TCP replication attempt starting");
    let send = with_budget(tcp_budget, replication_client.replicate_to_follower(items))
        .await
        .ok_or_else(|| {
            metrics::counter!("celeriant_lease_budget_exhausted_total", &[("op", "replicate")]).increment(1);
            replication_client.set_follower_reachable(false);
            ReplicationError::BudgetExhausted
        })?;

    let err = match send {
        Ok(()) => return Ok(SingleSendOutcome::Ok),
        Err(e) => e,
    };
    debug!(shard_id, elapsed_ms = tcp_start.elapsed().as_millis() as u64, err_kind = err_kind_label(&err), "TCP replication failed");

    if matches!(err, ReplicateToFollowerError::FollowerNetworkError(_) | ReplicateToFollowerError::LockTimeout) {
        replication_client.set_follower_reachable(false);
        return Ok(SingleSendOutcome::Failed);
    }

    let rejection = match err {
        ReplicateToFollowerError::FollowerRejected(r) => r,
        _ => return Ok(SingleSendOutcome::Failed),
    };

    use celeriant_msg::response::responses::FollowerRejection;
    match rejection {
        FollowerRejection::WalIndexMismatch { max_follower_wal_index } => {
            Ok(SingleSendOutcome::WalIndexMismatch { max_follower_wal_index })
        }
        FollowerRejection::TipHashMismatch { follower_wal_index, leader_wal_index, .. } => {
            warn!(shard_id, follower_wal_index, leader_wal_index, "Follower TipHashMismatch; kicking into S3 catchup");
            metrics::counter!("celeriant_replication_tip_hash_mismatch_kick_total").increment(1);
            spawn_kick(replication_client, node_status);
            Ok(SingleSendOutcome::Failed)
        }
        FollowerRejection::StaleLease { follower_lease_index, received_lease_index } => {
            // (a) our_lease < follower_lease_index — genuine split-brain. Some
            //     other node took leadership and the follower learned about it;
            //     fence self so we stop writing under a dead lease.
            // (b) our_lease >= follower_lease_index — benign rejection of an
            //     OLD metablock (queued under a previous leader / lease).
            //     Leadership is still valid, no action.
            let current = node_status.get();
            let raw = current.raw();
            let our_lease = raw.lease_index().unwrap_or(0);
            metrics::counter!("celeriant_replicate_stale_lease_total").increment(1);
            if raw.is_leader() && our_lease < follower_lease_index {
                warn!(
                    shard_id, our_lease, received_lease_index, follower_lease_index,
                    "StaleLease: cluster has moved on (split-brain) — fencing self",
                );
                // Technically shard-0 controlled for source-of-truth
                // But we can get away with the eventual convergance and better than
                // shard still thinking its leader
                let fenced = ValidatedNodeStatus::create_custom_status(raw, max_clock_drift_ms, 0);
                set_node_status_and_metric(node_status, fenced, shard_id);
            } else {
                debug!(
                    shard_id, our_lease, received_lease_index, follower_lease_index,
                    "StaleLease for old metablock — leadership still valid, no action",
                );
            }
            Ok(SingleSendOutcome::Failed)
        }
        _ => Ok(SingleSendOutcome::Failed),
    }
}

fn acquire_lease_budget(
    node_status: &Rc<Cell<ValidatedNodeStatus>>,
    op: &'static str,
    shard_id: u32,
) -> Result<Duration, ReplicationError> {
    match node_status.get().current_budget() {
        None => {
            warn!(shard_id, op, "Not leader before lease-bounded op");
            Err(ReplicationError::LeaderFenced)
        }
        Some(b) if b.is_zero() => {
            warn!(shard_id, op, "Lease budget exhausted before lease-bounded op");
            metrics::counter!("celeriant_lease_budget_exhausted_total", &[("op", op)]).increment(1);
            Err(ReplicationError::BudgetExhausted)
        }
        Some(b) => Ok(b),
    }
}

fn validate_chain_or_err(
    batches: &[ReplicationBatchItem],
    shard_id: u32,
) -> Result<(), ReplicateToS3Error> {
    match validate_intra_batch_chain(batches) {
        Ok(()) => Ok(()),
        Err(crate::intra_batch_chain::ValidateChainError::ChainBreak(chain_err)) => {
            metrics::counter!("celeriant_replication_intra_batch_chain_break_total").increment(1);
            let first_wal = batches.first().map(|b| b.metablock.wal_index).unwrap_or(0);
            let last_wal = batches.last().map(|b| b.metablock.wal_index).unwrap_or(0);
            error!(
                shard_id, first_wal, last_wal,
                at_index = chain_err.at_index,
                producer_wal = chain_err.producer_wal_index,
                consumer_wal = chain_err.consumer_wal_index,
                "Intra-batch chain break on S3 path; refusing upload",
            );
            Err(ReplicateToS3Error::IntraBatchChainBreak(chain_err))
        }
        Err(crate::intra_batch_chain::ValidateChainError::SerialiseMetablock(e)) => {
            error!(shard_id, error = ?e, "Metablock re-serialise failed on S3 path");
            Err(ReplicateToS3Error::SerializationFailed(format!("{:?}", e)))
        }
    }
}

fn spawn_kick<R: ReplicationClient + 'static>(
    replication_client: &Rc<R>,
    node_status: &Rc<Cell<ValidatedNodeStatus>>,
) {
    if !replication_client.try_acquire_kick() {
        return;
    }
    let rc = replication_client.clone();
    let kick_budget = node_status.get().current_budget();
    glommio::spawn_local(async move {
        match kick_budget {
            Some(b) if !b.is_zero() => {
                if with_budget(b, rc.send_kick()).await.is_none() {
                    metrics::counter!("celeriant_lease_budget_exhausted_total", &[("op", "kick")]).increment(1);
                }
            }
            _ => {
                metrics::counter!("celeriant_lease_budget_exhausted_total", &[("op", "kick")]).increment(1);
            }
        }
        rc.release_kick();
    }).detach();
}

fn err_kind_label(err: &ReplicateToFollowerError) -> &'static str {
    match err {
        ReplicateToFollowerError::FollowerNetworkError(_) => "NetworkError",
        ReplicateToFollowerError::FollowerRejected(_) => "Rejected",
        ReplicateToFollowerError::FollowerUnexpectedResponse => "UnexpectedResponse",
        ReplicateToFollowerError::FollowerTooFarBehind => "TooFarBehind",
        ReplicateToFollowerError::LockTimeout => "LockTimeout",
        ReplicateToFollowerError::SystemTimeError(_) => "SystemTimeError",
        ReplicateToFollowerError::BudgetExhausted => "BudgetExhausted",
    }
}

fn log_replication_outcome(initial_batch_count: usize, details: &ReplicationDetails, start: Instant, shard_id: u32) {
    let path = match details {
        ReplicationDetails::ReplicatedToFollower => "tcp",
        ReplicationDetails::ReplicatedToS3 => "s3",
    };
    let commit_ms = start.elapsed().as_millis() as u64;
    if commit_ms > 1000 {
        warn!(shard_id, batch_count = initial_batch_count, path, duration_ms = commit_ms, "Slow replication commit (>1s)");
    } else {
        debug!(shard_id, batch_count = initial_batch_count, path, duration_ms = commit_ms, "Replication batch committed");
    }
}

async fn finish_with_rollback(
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    last_rollback_at: &Rc<Cell<Option<Instant>>>,
    snapshot: Vec<PendingCommitData>,
    err: ReplicationError,
    shard_id: u32,
) -> Result<(), ReplicationError> {
    match rollback_or_requeue(fsync_coordinator, log_segments_cache, shard_mem_cache, last_rollback_at, snapshot, shard_id).await {
        Ok(()) => Err(err),
        Err(rb) => Err(ReplicationError::RollbackFailed(rb)),
    }
}

/// Advance the read cursor in-memory, update caches, broadcast watch events
fn commit_pcd(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    commit_data: PendingCommitData,
) {
    let log_id = commit_data.log_id();

    if let Some(log_segment) = log_segments_cache.get_if_cached(log_id) {
        let mut metadata = log_segment.metadata.borrow_mut();
        metadata.read = Some(commit_data.log_metadata.write.clone());
    }

    let mut event_collector = WatchEventCollector::new();
    let mut shard_mem_cache = shard_mem_cache.borrow_mut();

    for item in commit_data.pending_queue {
        shard_mem_cache.update_segment_summary_for_log(log_id, &item.metablock);

        match &item.metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(event_batch) => {
                shard_mem_cache.commit_position_snapshot(&event_batch, log_id, item.metablock_absolute_pos, CachePath::Read);

                event_collector.add_write_event(event_batch);

                if event_batch.event_batch_index == FIRST_EVENT_BATCH_INDEX {
                    event_collector.add_create_event(event_batch.aggregate_key.clone());
                }

                let size_bytes = item.size_bytes();
                shard_mem_cache.cache_recent_write(
                    event_batch.aggregate_key.clone(),
                    event_batch.event_batch_index,
                    item.metablock,
                    item.datablock,
                    size_bytes,
                );
            }
            MetablockKind::SoftTrim(soft_trim) => {
                shard_mem_cache.update_aggregate_min_event_batch_index(
                    &soft_trim.aggregate_key,
                    soft_trim.keep_from_event_batch_index,
                    CachePath::Write,
                );
                shard_mem_cache.update_aggregate_min_event_batch_index(
                    &soft_trim.aggregate_key,
                    soft_trim.keep_from_event_batch_index,
                    CachePath::Read,
                );
                event_collector.add_trim_event(soft_trim.aggregate_key.clone(), soft_trim.keep_from_event_batch_index);
            }
            MetablockKind::SoftDelete(soft_delete) => {
                shard_mem_cache.put_aggregate_into_cache_as_deleted(
                    soft_delete.aggregate_key.clone(),
                    log_id, item.metablock_absolute_pos,
                    soft_delete.event_index,
                    soft_delete.event_batch_index,
                    soft_delete.allow_recreate,
                    soft_delete.allow_index_continuation,
                    CachePath::Read,
                );
                event_collector.add_delete_event(soft_delete.aggregate_key.clone());
            }
            MetablockKind::SchemaRegistration(_) => {}
        }
    }

    drop(shard_mem_cache);
    event_collector.broadcast_all(&watched_aggregates);
}

/// Sweep the leader's "rotated but sidecar not yet written" queue and return any whose
/// read cursor has caught up (i.e., the segment is fully replicated)
fn collect_eligible_sealed_summaries(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
) -> Vec<(u64, SegmentSummaryPayload)> {
    let active_log_id = log_segments_cache.active_log_id();
    let mut cache = shard_mem_cache.borrow_mut();
    let pending = cache.pending_sealed_summary_log_ids();
    let mut ready = Vec::with_capacity(pending.len());
    for log_id in pending {
        if log_id == active_log_id {
            continue;
        }
        let fully_replicated = match log_segments_cache.get_if_cached(log_id) {
            Some(log_segment) => !log_segment.metadata.borrow().is_pending_advance(),
            None => true,
        };
        if !fully_replicated {
            continue;
        }
        if let Some(payload) = cache.take_sealed_segment_summary(log_id) {
            ready.push((log_id, payload));
        }
    }
    ready
}

/// Attempt rollback; if it fails, return entries to the pending replication queue
/// so the next replication cycle can retry S3 upload. Entries must never be silently dropped.
async fn rollback_or_requeue(
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    last_rollback_at: &Rc<Cell<Option<Instant>>>,
    replication_snapshot: Vec<PendingCommitData>,
    shard_id: u32,
) -> Result<(), ReplicationRollbackFailure> {
    last_rollback_at.set(Some(Instant::now()));
    match rollback_replicate(fsync_coordinator, log_segments_cache, shard_mem_cache, &replication_snapshot, shard_id).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let batch_count = replication_snapshot.len();
            error!(shard_id, batch_count, error = ?e, "Rollback failed; returning entries to pending replication queue");
            shard_mem_cache.borrow_mut().return_to_pending_replication(replication_snapshot);
            Err(e)
        }
    }
}

async fn rollback_replicate(
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    replication_snapshot: &[PendingCommitData],
    shard_id: u32,
) -> Result<(), ReplicationRollbackFailure> {
    let batches_to_rollback = replication_snapshot.len();
    error!(shard_id, batches_to_rollback, "Replication failed to both follower and S3, rolling back");
    metrics::counter!("celeriant_replication_rollbacks_total").increment(1);
    // In replication rollback, we modify the write positions in the file header
    // So we must drain and block any more writes to disk until rollback completes
    let _fsync_gate = fsync_coordinator
        .acquire_rollback_lock()
        .await
        .ok_or_else(|| ReplicationRollbackFailure::FsyncAmortisedBatchLockTimeout)?;

    // We nuke all the in-memory caches at this step at let everything rebuild naturally
    // this failure mode is rare - follower and S3 must both be down
    shard_mem_cache.borrow_mut().execute_replication_rollback();
    info!(shard_id, "Replication rollback: pending_replication_batches cleared");

    // PCDs land in fsync order, log_ids only advance across rotations, so consecutive
    // dedup is sufficient AND preserves chronological order
    let mut log_ids: Vec<u64> = replication_snapshot.iter().map(|c| c.log_id()).collect();
    log_ids.dedup();

    let mut trailing_shard_log_header: Option<ShardLogHeader> = None;

    // Rollback each log segment file. Normally only one file, but could be two files if a rotation occurred
    // Failure to rollback of a file stops entire process
    for log_id in log_ids {
        if let Some(log_segment) = log_segments_cache.get_if_cached(log_id) {
            let predecessor_cursor: Option<(u64, [u8; 32])> = if trailing_shard_log_header.is_none() && log_id > 1 {
                log_segments_cache.get_if_cached(log_id - 1).map(|pred| {
                    let m = pred.metadata.borrow();
                    (m.write.wal_index, m.write.tip_hash)
                })
            } else {
                None
            };

            let (header, header_end_start_pos) = {
                let mut metadata = log_segment.metadata.borrow_mut();
                let shard_log_header_end_pos = metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);

                // This is actually a valid scenario - we have *just* rotated a log
                // while still having uncommitted data in the previous log segment file
                // *or* this is the first log file ever for this shard and we have to rollback the first batch
                if metadata.read.is_none() {
                    // Segment was never replicated (e.g. freshly rotated on a leader).
                    // Reset write cursor to empty state; nothing to roll back to.
                    metadata.write.datablocks_position = shard_log_header_end_pos;
                    metadata.write.metablocks_position = HEADER_BLOCK_SIZE_BYTES as u64;
                    metadata.write.aggregate_key_bloom = AggregateKeyBloom::new();

                    if let Some(trailing_shard_log_header) = trailing_shard_log_header {
                        metadata.write.wal_index = trailing_shard_log_header.wal_index;
                        metadata.write.tip_hash = trailing_shard_log_header.tip_hash;
                    } else if let Some((pred_wal_index, pred_tip_hash)) = predecessor_cursor {
                        metadata.write.wal_index = pred_wal_index;
                        metadata.write.tip_hash = pred_tip_hash;
                    } else {
                        if log_id > 1 {
                            error!(shard_id, log_id, "Rollback predecessor not in cache; wal_index reset to 0 — probe will skip this shard until fresh write");
                        }
                        metadata.write.wal_index = 0;
                        metadata.write.tip_hash = GENESIS_HASH;
                    }
                } else {
                    // Roll write cursor back to last replicated position.
                    metadata.write = metadata.read.as_ref().unwrap().clone();
                }

                (metadata.to_shard_log_header(), shard_log_header_end_pos)
            };

            let dma_file_writer = log_segment
                .lock_writer("rollback_replicate")
                .await
                .map_err(|_| ReplicationRollbackFailure::WriteLockTimeout { log_id })?;
            let dma_file_writer = dma_file_writer
                .as_ref()
                .ok_or_else(|| ReplicationRollbackFailure::LogSegmentFileUnavailable { log_id })?;

            write_dual_shard_log_header(dma_file_writer, header_end_start_pos, &header)
                .await
                .map_err(|source| ReplicationRollbackFailure::WriteDualHeaderError { log_id, source })?;

            dma_file_writer
                .fdatasync()
                .await
                .map_err(|_| ReplicationRollbackFailure::HeaderFsyncFailed { log_id })?;

            // Now that the write position has changed for datablocks, we need to prepare a new datablocks carry over bytes for the next write
            let mut metadata = log_segment.metadata.borrow_mut();
            metadata.datablocks_carry_over = read_datablocks_carry_over_bytes(&dma_file_writer, metadata.write.datablocks_position)
                .await
                .map_err(|e| ReplicationRollbackFailure::UnableToReadDatablocksCarryOver {
                    log_id,
                    source: e.to_string(),
                })?;

            // Critical we keep this to handle rollback of now-empty-shard log segment files
            trailing_shard_log_header = Some(header);
        }
    }

    // Resync celeriant_wal_index gauge to the rolled-back active write cursor.
    // The fsync path is the only other writer of this gauge, so without this
    // line the gauge stays pinned to the pre-rollback high-water mark until
    // the next fresh write. That stale value makes EventualConvergence look
    // like a stuck-follower bug when the cluster has actually converged.
    let active_wal_index = log_segments_cache.active().metadata.borrow().write.wal_index;
    metrics::gauge!("celeriant_wal_index", &[("shard_id", shard_id.to_string())]).set(active_wal_index as f64);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use glommio::{LocalExecutorBuilder, Placement};

    use celeriant_memcache::cache_path::CachePath;
    use celeriant_memcache::mem_snapshot_aggregate::{AggregateStatus, MemSnapshotAggregate};
    use celeriant_memcache::pending_cache_item::PendingCacheItem;
    use celeriant_msg::request::requests::ReplicationBatchItem;
    use celeriant_msg::response::responses::FollowerRejection;
    use celeriant_rotating_log::log_segment_file::log_segment_cursor::LogSegmentCursor;
    use celeriant_rotating_log::log_segment_file::log_segment_file_metadata::LogSegmentFileMetadata;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, GENESIS_HASH as TEST_GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES as TEST_HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK as TEST_WIRE_VERSION};
    use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
    use celeriant_wal::metablocks::metablock::Metablock;
    use celeriant_wal::metablocks::metablock_kind::MetablockKind;
    use celeriant_wal::metablocks::metablock_soft_delete::MetablockSoftDelete;
    use celeriant_wal::metablocks::metablock_soft_trim::MetablockSoftTrim;
    use celeriant_wire::disk::versioned_block::serialize_versioned_message as test_serialize;

    use crate::error::replication_to_s3_error::ReplicateToS3Error;
    use crate::shard_wal_sync::compute_entry_hash as test_hash;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    fn item() -> ReplicationBatchItem {
        ReplicationBatchItem {
            metablock: Metablock::default_inline_event_batch_metadata(AggregateKey::default()),
            datablock: None,
        }
    }

    fn event_kind() -> MetablockKind {
        item().metablock.wal_metablock_type
    }

    fn test_dir() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shard");
        (tmp, dir)
    }

    fn leader_status() -> ValidatedNodeStatus {
        ValidatedNodeStatus::create_custom_status(
            celeriant_distributed::node_status::NodeStatus::Leader { lease_index: 1 },
            500,
            u64::MAX / 2,
        )
    }

    fn fresh_memcache() -> MemCache {
        MemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 1024 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024)
    }

    /// Builds `kinds.len()` PendingCacheItems chained from `start_tip` at wal=`start_wal`.
    /// Returns the final tip so chains can extend across PCD boundaries.
    fn chained_items(
        kinds: Vec<MetablockKind>,
        start_wal: u64,
        start_tip: [u8; 32],
    ) -> (Vec<PendingCacheItem>, [u8; 32]) {
        let mut prev_tip = start_tip;
        let mut out = Vec::with_capacity(kinds.len());
        for (i, kind) in kinds.into_iter().enumerate() {
            let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::default());
            mb.wal_index = start_wal + i as u64;
            mb.previous_tip_hash = prev_tip;
            mb.wal_metablock_type = kind;
            let mut buf = [0u8; FIXED_BLOCK_SIZE_BYTES];
            if test_serialize(&mb, TEST_WIRE_VERSION, &mut buf).is_ok() {
                prev_tip = test_hash(&prev_tip, &buf);
            }
            out.push(PendingCacheItem::from_parts(mb, None, 0, 0));
        }
        (out, prev_tip)
    }

    fn make_pcd(items: Vec<PendingCacheItem>) -> PendingCommitData {
        make_pcd_for_log(items, 999_999)
    }

    fn make_pcd_for_log(items: Vec<PendingCacheItem>, log_id: u64) -> PendingCommitData {
        PendingCommitData {
            log_metadata: LogSegmentFileMetadata {
                log_id,
                file_len: 0,
                write: LogSegmentCursor::default(),
                read: None,
                datablocks_carry_over: None,
                last_received_replication_wal_index: 0,
            },
            pending_queue: items,
        }
    }

    /// Build a snapshot with one PCD per `pcd_sizes` entry, all chained across PCD boundaries.
    fn make_captured(pcd_sizes: &[usize]) -> ReplicationCapturedData {
        make_captured_at(pcd_sizes, 1)
    }

    /// Same as `make_captured` but lets the caller pin the first wal_index. Useful when the
    /// snapshot must sit above wal indexes already on disk (catchup tests).
    fn make_captured_at(pcd_sizes: &[usize], start_wal: u64) -> ReplicationCapturedData {
        let mut current_wal = start_wal;
        let mut prev_tip = TEST_GENESIS_HASH;
        let mut snapshot = Vec::with_capacity(pcd_sizes.len());
        for &size in pcd_sizes {
            let kinds = vec![event_kind(); size];
            let (items, end_tip) = chained_items(kinds, current_wal, prev_tip);
            prev_tip = end_tip;
            current_wal += size as u64;
            snapshot.push(make_pcd(items));
        }
        ReplicationCapturedData {
            replication_snapshot: snapshot,
        }
    }

    /// Write metablocks at the given wal indexes into the active segment and advance the read
    /// cursor so `ReverseMetablockScanner` sees them as committed. Datablocks set to `None`
    /// to keep the post-scan datablock fetch a no-op.
    async fn seed_disk_at_wal(lsc: &Rc<LogSegmentsCache>, wal_indexes: &[u64]) {
        let active = lsc.active();
        let guard = active.lock_writer("test_seed_disk").await.unwrap();
        let dma = guard.as_ref().unwrap();

        let alignment = dma.alignment() as usize;
        let metablocks_bytes = wal_indexes.len() * FIXED_BLOCK_SIZE_BYTES;
        let padded = ((metablocks_bytes + alignment - 1) / alignment) * alignment;
        let mut buffer = dma.alloc_dma_buffer(padded);
        let slice = buffer.as_bytes_mut();

        for (i, &wal) in wal_indexes.iter().enumerate() {
            let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::default());
            mb.wal_index = wal;
            mb.datablock = DatablockStorageKind::None;
            let mut block = [0u8; FIXED_BLOCK_SIZE_BYTES];
            test_serialize(&mb, TEST_WIRE_VERSION, &mut block).unwrap();
            let start = i * FIXED_BLOCK_SIZE_BYTES;
            slice[start..start + FIXED_BLOCK_SIZE_BYTES].copy_from_slice(&block);
        }

        dma.write_at(buffer, TEST_HEADER_BLOCK_SIZE_BYTES as u64).await.unwrap();
        dma.fdatasync().await.unwrap();
        drop(guard);

        let metablocks_end = TEST_HEADER_BLOCK_SIZE_BYTES as u64 + metablocks_bytes as u64;
        let last_wal = wal_indexes.last().copied().unwrap_or(0);
        let cursor = LogSegmentCursor {
            log_id: lsc.active_log_id(),
            metablocks_position: metablocks_end,
            datablocks_position: (4 * 1024 * 1024u64).saturating_sub(TEST_HEADER_BLOCK_SIZE_BYTES as u64),
            wal_index: last_wal,
            aggregate_key_bloom: Default::default(),
            tip_hash: [0u8; 32],
        };
        let mut metadata = active.metadata.borrow_mut();
        metadata.read = Some(cursor.clone());
        metadata.write = cursor;
    }

    type TcpResponder = Box<dyn FnMut(usize) -> Result<(), ReplicateToFollowerError>>;
    type S3Responder = Box<dyn FnMut(usize) -> Result<(), ReplicateToS3Error>>;

    struct MockClient {
        follower_reachable: Cell<bool>,
        tcp: RefCell<TcpResponder>,
        s3: RefCell<S3Responder>,
        tcp_calls: Rc<RefCell<Vec<usize>>>,
        s3_calls: Rc<RefCell<Vec<usize>>>,
    }

    impl MockClient {
        fn build() -> (Self, Rc<RefCell<Vec<usize>>>, Rc<RefCell<Vec<usize>>>) {
            let tcp_calls = Rc::new(RefCell::new(Vec::new()));
            let s3_calls = Rc::new(RefCell::new(Vec::new()));
            let mc = Self {
                follower_reachable: Cell::new(true),
                tcp: RefCell::new(Box::new(|_| Ok(()))),
                s3: RefCell::new(Box::new(|_| Ok(()))),
                tcp_calls: tcp_calls.clone(),
                s3_calls: s3_calls.clone(),
            };
            (mc, tcp_calls, s3_calls)
        }

        fn with_tcp(self, f: impl FnMut(usize) -> Result<(), ReplicateToFollowerError> + 'static) -> Self {
            *self.tcp.borrow_mut() = Box::new(f);
            self
        }

        fn with_s3(self, f: impl FnMut(usize) -> Result<(), ReplicateToS3Error> + 'static) -> Self {
            *self.s3.borrow_mut() = Box::new(f);
            self
        }

        fn unreachable(self) -> Self {
            self.follower_reachable.set(false);
            self
        }
    }

    impl ReplicationClient for MockClient {
        fn set_follower_address(&self, _: Option<String>) {}
        fn set_follower_reachable(&self, r: bool) { self.follower_reachable.set(r); }
        fn is_follower_reachable(&self) -> bool { self.follower_reachable.get() }
        fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { None }
        fn set_heartbeat_in_flight(&self, _unix_ms: Option<u64>) {}
        fn reset_heartbeat_state(&self) {}

        async fn replicate_to_follower(&self, b: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError> {
            let n = b.len();
            self.tcp_calls.borrow_mut().push(n);
            (self.tcp.borrow_mut())(n)
        }

        async fn replicate_to_s3(&self, b: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            let n = b.len();
            self.s3_calls.borrow_mut().push(n);
            (self.s3.borrow_mut())(n)
        }

        async fn send_heartbeat(&self, _: u64, _: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            unreachable!("tests do not exercise heartbeat")
        }

        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> {
            Ok(true)
        }
    }

    /// `responses[i]` is returned on call `i`; further calls return `Ok(())`.
    fn programmed_tcp(responses: Vec<Result<(), ReplicateToFollowerError>>) -> TcpResponder {
        let mut iter = responses.into_iter();
        Box::new(move |_| iter.next().unwrap_or(Ok(())))
    }

    struct Harness {
        _tmp: tempfile::TempDir,
        lsc: Rc<LogSegmentsCache>,
        smc: Rc<RefCell<MemCache>>,
        node_status: Rc<Cell<ValidatedNodeStatus>>,
        coordinator: Rc<Coordinator<ShardFsyncError>>,
        last_rollback_at: Rc<Cell<Option<Instant>>>,
        watched: Rc<AggregateWatchers>,
    }

    impl Harness {
        async fn new() -> Self {
            let (tmp, dir) = test_dir();
            let lsc = Rc::new(LogSegmentsCache::ready_up(dir, 4 * 1024 * 1024, 4, 0).await.unwrap());
            Self {
                _tmp: tmp,
                lsc,
                smc: Rc::new(RefCell::new(fresh_memcache())),
                node_status: Rc::new(Cell::new(leader_status())),
                coordinator: Rc::new(Coordinator::new()),
                last_rollback_at: Rc::new(Cell::new(None)),
                watched: Rc::new(AggregateWatchers::new()),
            }
        }

        async fn commit<R: ReplicationClient + 'static>(
            &self,
            client: Rc<R>,
            captured: ReplicationCapturedData,
            max_request_size: u64,
        ) -> Result<(), ReplicationError> {
            self.commit_with_trigger(client, captured, ReplicationTrigger::Write, max_request_size).await
        }

        async fn commit_with_trigger<R: ReplicationClient + 'static>(
            &self,
            client: Rc<R>,
            captured: ReplicationCapturedData,
            trigger: ReplicationTrigger,
            max_request_size: u64,
        ) -> Result<(), ReplicationError> {
            commit_replication_with_rollback(
                client,
                self.coordinator.clone(),
                self.lsc.clone(),
                self.smc.clone(),
                self.watched.clone(),
                self.node_status.clone(),
                self.last_rollback_at.clone(),
                captured,
                trigger,
                None,
                max_request_size,
                64 * 1024,
                500,
                0,
            ).await
        }

        async fn close(self) {
            self.lsc.close().await;
        }
    }

    // --- capture phase ---

    #[test]
    fn capture_write_returns_no_capture_when_queue_empty() {
        let smc = Rc::new(RefCell::new(fresh_memcache()));
        match capture_replication_snapshot(&smc, ReplicationTrigger::Write) {
            CaptureResult::NoCaptureRaceButOk => {}
            CaptureResult::Captured(_) => panic!("empty queue must not produce a capture under Write"),
            CaptureResult::Failed(e) => panic!("expected NoCapture, got Failed({e:?})"),
        }
    }

    #[test]
    fn capture_probe_returns_captured_empty_when_queue_empty() {
        let smc = Rc::new(RefCell::new(fresh_memcache()));
        match capture_replication_snapshot(&smc, ReplicationTrigger::Probe) {
            CaptureResult::Captured(d) => {
                assert!(d.replication_snapshot.is_empty(), "Probe with empty queue must produce empty Captured");
            }
            CaptureResult::NoCaptureRaceButOk => panic!("Probe must NOT short-circuit on empty queue"),
            CaptureResult::Failed(e) => panic!("expected Captured(empty), got Failed({e:?})"),
        }
    }

    #[test]
    fn capture_returns_rollback_when_flag_set() {
        // execute_replication_rollback only sets the flag if the queue was non-empty when called.
        let smc = Rc::new(RefCell::new(fresh_memcache()));
        let pcd = make_captured(&[1]).replication_snapshot.remove(0);
        smc.borrow_mut().push_pending_replication(pcd);
        smc.borrow_mut().execute_replication_rollback();
        match capture_replication_snapshot(&smc, ReplicationTrigger::Write) {
            CaptureResult::Failed(ReplicationError::RollbackInProgress) => {}
            CaptureResult::Failed(e) => panic!("expected RollbackInProgress, got Failed({e:?})"),
            CaptureResult::NoCaptureRaceButOk => panic!("expected RollbackInProgress, got NoCapture"),
            CaptureResult::Captured(_) => panic!("expected RollbackInProgress, got Captured"),
        }
    }

    #[test]
    fn capture_drains_queue_when_populated() {
        let smc = Rc::new(RefCell::new(fresh_memcache()));
        let pcd = make_captured(&[3]).replication_snapshot.remove(0);
        smc.borrow_mut().push_pending_replication(pcd);
        match capture_replication_snapshot(&smc, ReplicationTrigger::Write) {
            CaptureResult::Captured(d) => {
                assert_eq!(d.replication_snapshot.len(), 1);
                assert_eq!(d.replication_snapshot[0].pending_queue.len(), 3);
                assert_eq!(smc.borrow().pending_replication_count(), 0, "queue must drain on capture");
            }
            CaptureResult::Failed(e) => panic!("expected Captured, got Failed({e:?})"),
            CaptureResult::NoCaptureRaceButOk => panic!("expected Captured, got NoCapture"),
        }
    }

    // --- end-to-end paths ---

    #[test]
    fn s3_fallback_uploads_entire_batch() {
        glommio_test!({
            let h = Harness::new().await;
            let (mock, tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock.unreachable());
            let captured = make_captured(&[5]);

            h.commit(client, captured, u64::MAX).await.unwrap();
            assert_eq!(*s3_calls.borrow(), vec![5]);
            assert!(tcp_calls.borrow().is_empty());

            h.close().await;
        });
    }

    /// Bad batch must never land in S3 (no self-deletion of corrupt data).
    #[test]
    fn leader_rejects_s3_upload_on_intra_batch_chain_break() {
        glommio_test!({
            let h = Harness::new().await;
            let (mock, _tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock.unreachable());
            let mut captured = make_captured(&[2]);
            captured.replication_snapshot[0].pending_queue[1].metablock.previous_tip_hash = [0xAB; 32];

            let result = h.commit(client, captured, u64::MAX).await;
            assert!(s3_calls.borrow().is_empty(), "S3 upload must be refused on chain break");
            assert!(
                matches!(result, Err(ReplicationError::ReplicateToS3Error(ReplicateToS3Error::IntraBatchChainBreak(_)))),
                "got {result:?}"
            );

            h.close().await;
        });
    }

    // --- TCP failure → S3 fallback (one row per failure variant) ---

    #[test]
    fn tcp_failures_fall_through_to_s3() {
        let cases: Vec<(&str, fn() -> ReplicateToFollowerError)> = vec![
            ("network", || ReplicateToFollowerError::FollowerNetworkError(celeriant_client_glommio::ClientError::NoAddress)),
            ("lock_timeout", || ReplicateToFollowerError::LockTimeout),
            ("unexpected_response", || ReplicateToFollowerError::FollowerUnexpectedResponse),
            ("too_far_behind", || ReplicateToFollowerError::FollowerTooFarBehind),
            ("tip_hash_mismatch", || ReplicateToFollowerError::FollowerRejected(FollowerRejection::TipHashMismatch {
                follower: [0xCD; 32], follower_wal_index: 1, leader: [0xEF; 32], leader_wal_index: 2,
            })),
            ("not_a_follower", || ReplicateToFollowerError::FollowerRejected(FollowerRejection::NotAFollower)),
            ("stale_lease", || ReplicateToFollowerError::FollowerRejected(FollowerRejection::StaleLease {
                follower_lease_index: 0, received_lease_index: 1,
            })),
        ];
        for (label, mk_err) in cases {
            glommio_test!({
                let h = Harness::new().await;
                let (mock, tcp_calls, s3_calls) = MockClient::build();
                let mut once = Some(mk_err());
                let client = Rc::new(mock.with_tcp(move |_| {
                    if let Some(e) = once.take() { Err(e) } else { Ok(()) }
                }));
                let result = h.commit(client, make_captured(&[3]), u64::MAX).await;
                assert!(result.is_ok(), "[{label}] expected Ok via S3 fallback, got {result:?}");
                assert_eq!(*s3_calls.borrow(), vec![3], "[{label}] expected single 3-item S3 upload");
                assert_eq!(tcp_calls.borrow().as_slice(), &[3], "[{label}] one TCP attempt before fallback");
                h.close().await;
            });
        }
    }

    // --- WalIndexMismatch → catchup ---

    fn wal_mismatch(max_follower_wal_index: u64) -> ReplicateToFollowerError {
        ReplicateToFollowerError::FollowerRejected(FollowerRejection::WalIndexMismatch { max_follower_wal_index })
    }

    /// Empty disk: catchup fetch returns no entries, so the loop resends the original PCD chunk
    /// with `catchup_already_added=true`. Second TCP call succeeds and the PCD commits.
    #[test]
    fn wal_index_mismatch_resends_after_empty_catchup() {
        glommio_test!({
            let h = Harness::new().await;
            let (mock, tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock.with_tcp(programmed_tcp(vec![Err(wal_mismatch(0)), Ok(())])));
            let result = h.commit(client, make_captured(&[1]), u64::MAX).await;
            assert!(result.is_ok(), "expected Ok after catchup-and-resend, got {result:?}");
            assert_eq!(tcp_calls.borrow().len(), 2, "first call rejected, second resends");
            assert!(s3_calls.borrow().is_empty());
            h.close().await;
        });
    }

    /// Follower behind: leader fetches the wal=1..=2 gap from disk, ships it as a separate
    /// TCP batch, then resends the original snapshot starting at wal=3. Three TCP calls
    /// total ([snapshot, catchup, snapshot]), no S3 fallback.
    #[test]
    fn wal_index_mismatch_resends_after_real_catchup() {
        glommio_test!({
            let h = Harness::new().await;
            seed_disk_at_wal(&h.lsc, &[1, 2]).await;

            let (mock, tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock.with_tcp(programmed_tcp(vec![
                Err(wal_mismatch(0)),
                Ok(()),
                Ok(()),
            ])));

            let captured = make_captured_at(&[3], 3);
            let result = h.commit(client, captured, u64::MAX).await;
            assert!(result.is_ok(), "expected Ok after catchup-and-resend, got {result:?}");
            assert_eq!(
                tcp_calls.borrow().as_slice(),
                &[3, 2, 3],
                "snapshot first try, catchup batch of 2, snapshot resend",
            );
            assert!(s3_calls.borrow().is_empty(), "no S3 fallback when catchup succeeds");

            h.close().await;
        });
    }

    /// Two consecutive WalIndexMismatches must escape to S3 (cannot loop).
    #[test]
    fn wal_index_mismatch_twice_escapes_to_s3() {
        glommio_test!({
            let h = Harness::new().await;
            let (mock, tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock.with_tcp(programmed_tcp(vec![Err(wal_mismatch(0)), Err(wal_mismatch(0))])));
            let result = h.commit(client, make_captured(&[2]), u64::MAX).await;
            assert!(result.is_ok(), "got {result:?}");
            assert_eq!(*s3_calls.borrow(), vec![2]);
            assert_eq!(tcp_calls.borrow().len(), 2, "no third TCP attempt after catchup_already_added trips");
            h.close().await;
        });
    }

    // --- multi-PCD whole-snapshot semantics ---

    /// All PCDs ship in one TCP request. On TCP failure, S3 drains the entire
    /// snapshot in one batch (no per-PCD partial-commit).
    #[test]
    fn multi_pcd_snapshot_falls_back_to_s3_as_one_unit() {
        glommio_test!({
            let h = Harness::new().await;
            let (mock, tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock.with_tcp(programmed_tcp(vec![
                Err(ReplicateToFollowerError::FollowerNetworkError(celeriant_client_glommio::ClientError::NoAddress)),
            ])));
            let result = h.commit(client, make_captured(&[2, 2, 1]), u64::MAX).await;
            assert!(result.is_ok(), "got {result:?}");
            assert_eq!(tcp_calls.borrow().as_slice(), &[5], "one TCP attempt with whole snapshot");
            assert_eq!(*s3_calls.borrow(), vec![5], "S3 carries entire snapshot in one upload");
            h.close().await;
        });
    }

    /// All PCDs commit in lock-step on a single successful TCP send.
    #[test]
    fn multi_pcd_snapshot_commits_in_lock_step() {
        glommio_test!({
            let h = Harness::new().await;
            let (mock, tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock);
            let result = h.commit(client, make_captured(&[2, 2, 1]), u64::MAX).await;
            assert!(result.is_ok(), "got {result:?}");
            assert_eq!(tcp_calls.borrow().as_slice(), &[5], "one TCP attempt with all 5 items");
            assert!(s3_calls.borrow().is_empty(), "no S3 fallback on success");
            h.close().await;
        });
    }

    // --- commit_pcd metablock-kind routing ---

    /// Drives commit_pcd via the S3 path with non-EventBatch kinds and asserts the resulting
    /// memcache state reflects the kind's logic.
    #[test]
    fn commit_pcd_routes_soft_trim_and_soft_delete() {
        let key = AggregateKey::new(1, 2, 3);

        let cases: Vec<(&'static str, MetablockKind, fn(&mut MemCache, &AggregateKey))> = vec![
            (
                "soft_trim",
                MetablockKind::SoftTrim(MetablockSoftTrim {
                    aggregate_key: key.clone(),
                    keep_from_event_batch_index: 50,
                    event_batch_index: 0, event_index: 0, client_id: 0, user_id: None,
                }),
                |smc, key| {
                    let snap = smc.get_aggregate_snapshot(key, CachePath::Read).expect("seeded snapshot present");
                    assert_eq!(snap.min_event_batch_index, 50, "SoftTrim must bump min_event_batch_index");
                },
            ),
            (
                "soft_delete",
                MetablockKind::SoftDelete(MetablockSoftDelete {
                    aggregate_key: key.clone(),
                    allow_recreate: true, allow_index_continuation: false,
                    event_batch_index: 9, event_index: 7, client_id: 0, user_id: None,
                }),
                |smc, key| {
                    let snap = smc.get_aggregate_snapshot(key, CachePath::Read).expect("delete writes snapshot");
                    assert_eq!(snap.status, AggregateStatus::Deleted);
                    assert!(snap.allow_recreate);
                    assert_eq!(snap.event_batch_index, 9);
                },
            ),
        ];

        for (label, kind, verify) in cases {
            let key = key.clone();
            glommio_test!({
                let h = Harness::new().await;
                h.smc.borrow_mut().put_aggregate_snapshot_only(
                    key.clone(),
                    MemSnapshotAggregate::found(0, 0, 0, 0, 0),
                    false, CachePath::Read,
                );

                let (mock, _tcp_calls, _s3_calls) = MockClient::build();
                let client = Rc::new(mock.unreachable());
                let mut captured = make_captured(&[1]);
                captured.replication_snapshot[0].pending_queue[0].metablock.wal_metablock_type = kind;

                h.commit(client, captured, u64::MAX).await
                    .unwrap_or_else(|e| panic!("[{label}] commit failed: {e:?}"));
                verify(&mut *h.smc.borrow_mut(), &key);
                h.close().await;
            });
        }
    }

    // --- probe path (empty snapshot + Probe trigger) ---

    /// Probe with empty queue synthesises a one-item batch from the latest
    /// local entry and pings the follower. Follower current → no further work.
    #[test]
    fn probe_with_empty_snapshot_pings_latest_local() {
        glommio_test!({
            let h = Harness::new().await;
            seed_disk_at_wal(&h.lsc, &[1, 2, 3]).await;

            let (mock, tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock);
            let captured = ReplicationCapturedData { replication_snapshot: vec![] };

            let result = h.commit_with_trigger(client, captured, ReplicationTrigger::Probe, u64::MAX).await;
            assert!(result.is_ok(), "probe + follower-current returns Ok; got {result:?}");
            assert_eq!(tcp_calls.borrow().as_slice(), &[1], "probe ships one synthesised entry");
            assert!(s3_calls.borrow().is_empty(), "probe never falls back to S3");
            h.close().await;
        });
    }

    /// Probe + WalIndexMismatch delegates to replicate_follower_catchup;
    /// gap-fill goes through the chunked path, not the probe's old inline send.
    #[test]
    fn probe_runs_catchup_on_wal_index_mismatch() {
        glommio_test!({
            let h = Harness::new().await;
            seed_disk_at_wal(&h.lsc, &[1, 2, 3]).await;

            let (mock, tcp_calls, s3_calls) = MockClient::build();
            // First call = probe ping → mismatch at follower=1; second call = catchup chunk → ok.
            let client = Rc::new(mock.with_tcp(programmed_tcp(vec![Err(wal_mismatch(1)), Ok(())])));
            let captured = ReplicationCapturedData { replication_snapshot: vec![] };

            let result = h.commit_with_trigger(client, captured, ReplicationTrigger::Probe, u64::MAX).await;
            assert!(result.is_ok(), "probe + catchup-success returns Ok; got {result:?}");
            assert_eq!(
                tcp_calls.borrow().as_slice(),
                &[1, 2],
                "probe ping (1 entry) + catchup gap (entries 2..=3, 2 items)",
            );
            assert!(s3_calls.borrow().is_empty(), "probe catchup runs over TCP");
            h.close().await;
        });
    }

    /// Probe + unreachable follower no-ops without touching TCP or S3.
    #[test]
    fn probe_with_unreachable_follower_is_noop() {
        glommio_test!({
            let h = Harness::new().await;
            seed_disk_at_wal(&h.lsc, &[1, 2, 3]).await;

            let (mock, tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock.unreachable());
            let captured = ReplicationCapturedData { replication_snapshot: vec![] };

            let result = h.commit_with_trigger(client, captured, ReplicationTrigger::Probe, u64::MAX).await;
            assert!(result.is_ok(), "probe + unreachable returns Ok cleanly; got {result:?}");
            assert!(tcp_calls.borrow().is_empty(), "probe must not contact follower when unreachable");
            assert!(s3_calls.borrow().is_empty(), "probe never falls back to S3");
            h.close().await;
        });
    }

    /// Probe with empty disk (wal_index=0) bails silently.
    #[test]
    fn probe_with_empty_disk_is_noop() {
        glommio_test!({
            let h = Harness::new().await;
            // No seed_disk_at_wal — active segment's write.wal_index is 0.
            let (mock, tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock);
            let captured = ReplicationCapturedData { replication_snapshot: vec![] };

            let result = h.commit_with_trigger(client, captured, ReplicationTrigger::Probe, u64::MAX).await;
            assert!(result.is_ok());
            assert!(tcp_calls.borrow().is_empty());
            assert!(s3_calls.borrow().is_empty());
            h.close().await;
        });
    }

    // --- StaleLease split-brain detection in single_send ---

    /// Case (a): our_lease (1) < follower's known leader_lease (5) → fence self.
    /// The write path (not just the probe) must self-fence on genuine split-brain.
    #[test]
    fn stale_lease_split_brain_fences_self_on_write() {
        glommio_test!({
            let h = Harness::new().await;
            // Default leader_status() has lease_index=1.
            let (mock, tcp_calls, _s3) = MockClient::build();
            let client = Rc::new(mock.with_tcp(programmed_tcp(vec![
                Err(ReplicateToFollowerError::FollowerRejected(FollowerRejection::StaleLease {
                    follower_lease_index: 5, received_lease_index: 1,
                })),
            ])));

            assert!(h.node_status.get().is_leader(), "must start as leader");
            let result = h.commit(client, make_captured(&[1]), u64::MAX).await;
            assert!(result.is_err(), "commit must error after self-fence; got {result:?}");
            assert_eq!(tcp_calls.borrow().len(), 1, "one TCP attempt → StaleLease → fence");
            assert!(!h.node_status.get().is_leader(), "split-brain must fence self");
            h.close().await;
        });
    }

    /// Case (b): our_lease (2) >= follower's known leader_lease (2), metablock
    /// tagged at lease 1 → benign rejection of an OLD metablock; leadership intact.
    #[test]
    fn stale_lease_old_metablock_keeps_leadership() {
        glommio_test!({
            let h = Harness::new().await;
            let bumped = ValidatedNodeStatus::create_custom_status(
                celeriant_distributed::node_status::NodeStatus::Leader { lease_index: 2 },
                500, u64::MAX / 2,
            );
            h.node_status.set(bumped);

            let (mock, _tcp_calls, s3_calls) = MockClient::build();
            let client = Rc::new(mock.with_tcp(programmed_tcp(vec![
                Err(ReplicateToFollowerError::FollowerRejected(FollowerRejection::StaleLease {
                    follower_lease_index: 2, received_lease_index: 1,
                })),
            ])));

            let result = h.commit(client, make_captured(&[1]), u64::MAX).await;
            assert!(result.is_ok(), "case (b) falls through to S3; got {result:?}");
            assert_eq!(*s3_calls.borrow(), vec![1], "case (b) reaches S3 fallback");
            assert!(h.node_status.get().is_leader(), "leadership stays on case (b)");
            h.close().await;
        });
    }

    /// Regression test for the fresh-rotation rollback bug:
    /// when the only log_id in the rollback batch is a segment with `read=None` and no predecessor
    /// was in the same batch, wal_index must be recovered from the predecessor in the cache —
    /// NOT reset to 0.
    #[test]
    fn rollback_fresh_rotation_preserves_wal_index_from_predecessor() {
        glommio_test!({
            let h = Harness::new().await;

            // Seed predecessor (log_id=1) with a known replicated position.
            let predecessor_wal_index: u64 = 260_000;
            let predecessor_tip: [u8; 32] = [0xAA; 32];
            {
                let seg1 = h.lsc.get_if_cached(1).expect("log_id=1 must be active initially");
                let mut m = seg1.metadata.borrow_mut();
                m.write.wal_index = predecessor_wal_index;
                m.write.tip_hash = predecessor_tip;
                // Mark as replicated so rotation is valid.
                m.read = Some(m.write.clone());
            }

            // Rotate: log_id=1 moves to LRU, log_id=2 becomes active (read=None).
            h.lsc.rotate_to_next_log().await.unwrap();
            assert_eq!(h.lsc.active_log_id(), 2);

            // Confirm log_id=2 starts with read=None and wal_index from predecessor cursor.
            {
                let seg2 = h.lsc.active();
                let m = seg2.metadata.borrow();
                assert!(m.read.is_none(), "fresh segment must have read=None");
            }

            // Build a PCD whose log_id matches the active segment so rollback processes it.
            let (items, _) = chained_items(vec![event_kind(); 3], predecessor_wal_index + 1, predecessor_tip);
            let captured = ReplicationCapturedData {
                replication_snapshot: vec![make_pcd_for_log(items, 2)],
            };

            // Trigger rollback: follower unreachable + S3 fails.
            let (mock, _tcp, _s3) = MockClient::build();
            let client = Rc::new(
                mock.unreachable()
                    .with_s3(|_| Err(ReplicateToS3Error::S3Unavailable)),
            );

            // Rollback must succeed (no panic / Err from the rollback path itself).
            let result = h.commit(client, captured, u64::MAX).await;
            // Expect an error from the replication (both paths failed), but rollback itself ran.
            assert!(
                matches!(result, Err(ReplicationError::RollbackFailed(_)) | Err(ReplicationError::ReplicateToS3Error(_))),
                "unexpected result: {result:?}",
            );

            // The critical assertion: wal_index must not have been zeroed.
            let seg2 = h.lsc.active();
            let m = seg2.metadata.borrow();
            assert_eq!(
                m.write.wal_index, predecessor_wal_index,
                "rollback of fresh segment must restore predecessor wal_index, not zero it",
            );
            assert_eq!(
                m.write.tip_hash, predecessor_tip,
                "rollback of fresh segment must restore predecessor tip_hash, not GENESIS_HASH",
            );

            h.close().await;
        });
    }
}
