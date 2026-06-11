//! Delete/trim side-load for chaos scenarios.
//!
//! The main idempotent bench proves the write ACK contract under chaos; this
//! workload proves the same contract for the two operations where a false ACK
//! is least recoverable — data destruction. Each task owns one aggregate
//! (org 2, away from the main bench's org 1) and cycles write → trim →
//! delete → sequence-continuation recreate, keeping a client-side ledger of
//! what the server acked. Two failure classes are detected:
//!
//! - **Version regression** (live, per ack): an acked write whose returned
//!   `max_aggregate_version` is not above every previously acked version for
//!   that incarnation chain. This is the stale-tombstone corruption signature
//!   — a delete recording regressed indexes feeds a continuation recreate
//!   that re-issues already-acked versions.
//! - **False-acked destruction** (post-settle audit): an acked delete whose
//!   aggregate still reads live, or an acked trim whose floor never moved.
//!
//! Ops hitting `RollbackInProgress` / `ReplicationBackpressure` / transport
//! errors retry — those are the chaos windows this workload exists to drive
//! deletes and trims into.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::collections::HashMap;

use celeriant_client_tokio::pool::CeleriantPool;
use celeriant_client_tokio::server_error::{DeleteError, DetailsError, ServerError, TrimError, WriteError};
use celeriant_client_tokio::{ClientError, WriteEventsOptions};
use celeriant_msg::request::requests::{
    AggregateDetailsRequest, DeleteRequest, SingleAggregateDelete, TrimStartRequest,
};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use tokio::sync::Barrier;
use tokio::time::Instant;

/// Batches written between destructive ops. Small on purpose: the point is
/// op-type churn under chaos, not write throughput (the main bench owns that).
const WRITES_PER_CYCLE: u64 = 4;

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DeleteTrimCounters {
    pub write_acks: u64,
    pub trim_acks: u64,
    pub delete_acks: u64,
    pub recreate_acks: u64,
    /// Retries across all op types (rollback-in-progress, backpressure,
    /// transport). Non-zero under chaos is the workload doing its job.
    pub retries: u64,
    /// Ledger resyncs after a definitive rejection (OCC, out-of-range) that
    /// contradicts client-side belief — expected when a transport-lost ack
    /// actually landed, so a resync is bookkeeping, not a violation.
    pub occ_resyncs: u64,
    /// Acked writes whose returned version did not move strictly upward.
    /// Must be zero — this is the duplicate-version corruption signature.
    pub version_regressions: u64,
    pub fatal_errors: u64,
}

/// What one task believes the server acked. Audited post-settle.
#[derive(Debug, Clone)]
pub struct DeleteTrimLedger {
    pub aggregate_key: AggregateKey,
    pub client_id: u128,
    /// Highest `max_aggregate_version` ever acked across the whole
    /// delete/recreate chain (sequence continuation keeps versions rising).
    pub highest_acked_version: u64,
    /// Highest `keep_from_aggregate_version` acked by a trim AGAINST THE
    /// CURRENT INCARNATION. An acked delete clears it — the delete destroys
    /// the whole aggregate, floor included, and the continuation recreate
    /// starts a fresh never-trimmed incarnation (`min_aggregate_version` 0).
    pub acked_trim_floor: u64,
    /// True when the last acked destructive state is "deleted, no recreate
    /// acked since".
    pub expect_deleted: bool,
    /// Highest version the SERVER returned in a write ack. The regression
    /// check compares against this, never against the +1-on-2002 estimate —
    /// a 2002 carries no version, so estimates can't indict the server.
    pub highest_server_acked_version: u64,
    /// A recreate attempt errored in transit while `expect_deleted` — it may
    /// have landed without an ack, in which case the aggregate legitimately
    /// reads live. The audit downgrades those to ambiguous, not false-ack.
    pub recreate_maybe_landed: bool,
}

#[derive(Debug)]
pub struct DeleteTrimOutcome {
    pub counters: DeleteTrimCounters,
    pub ledgers: Vec<DeleteTrimLedger>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DeleteTrimAuditReport {
    pub tasks_audited: u64,
    /// Acked delete, aggregate still reads live. The false-ack the
    /// rollback-generation checks exist to prevent.
    pub false_acked_deletes: u64,
    /// Acked trim floor not reflected in `min_aggregate_version`.
    pub trim_floor_breaches: u64,
    /// Live aggregate whose `max_aggregate_version` is below the highest
    /// acked version — acked data lost.
    pub acked_version_loss: u64,
    /// Unacked delete that actually landed (transport-lost ack). Normal
    /// under chaos, reported for context, not a violation.
    pub unacked_deletes_landed: u64,
    /// Acked delete + live aggregate, but an unacked recreate may have
    /// landed in between — can't indict the delete ack. Context only.
    pub ambiguous_recreates_landed: u64,
    /// Pinned per-node reads disagreed on min/deleted state for a flagged
    /// aggregate — a node-visibility bug, distinct from durable loss.
    pub node_divergences: u64,
    pub tasks_unreadable: u64,
    pub samples: Vec<String>,
}

impl DeleteTrimAuditReport {
    pub fn violations(&self) -> u64 {
        self.false_acked_deletes + self.trim_floor_breaches + self.acked_version_loss
    }
}

fn retriable_write(e: &ClientError) -> bool {
    match e {
        ClientError::Server(ServerError::Write { kind, .. }) => matches!(
            kind,
            WriteError::ReplicationError
                | WriteError::ReplicationBackpressure
                | WriteError::FsyncError
                | WriteError::InflightDuplicateWrite { .. }
                | WriteError::CacheAggregateClientError
                | WriteError::AggregateExistsCacheError
        ),
        ClientError::Server(_) => false,
        _ => true, // transport / timeout / routing — retriable
    }
}

async fn jittered_backoff(backoff_ms: &mut u64, seed_a: u64, seed_b: u64) {
    const INITIAL_MS: u64 = 10;
    const MAX_MS: u64 = 500;
    let next = if *backoff_ms == 0 { INITIAL_MS } else { (*backoff_ms * 2).min(MAX_MS) };
    let jitter = seed_a.wrapping_mul(2654435761).wrapping_add(seed_b) % 1000;
    tokio::time::sleep(Duration::from_millis(next / 2 + (next * jitter) / 1000)).await;
    *backoff_ms = next;
}

pub async fn run_delete_trim_workload(
    pool: &Arc<CeleriantPool>,
    num_tasks: usize,
    duration_secs: u64,
) -> DeleteTrimOutcome {
    let barrier = Arc::new(Barrier::new(num_tasks));
    let c_write = Arc::new(AtomicU64::new(0));
    let c_trim = Arc::new(AtomicU64::new(0));
    let c_delete = Arc::new(AtomicU64::new(0));
    let c_recreate = Arc::new(AtomicU64::new(0));
    let c_retries = Arc::new(AtomicU64::new(0));
    let c_resyncs = Arc::new(AtomicU64::new(0));
    let c_regressions = Arc::new(AtomicU64::new(0));
    let c_fatal = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::with_capacity(num_tasks);
    for id in 0..num_tasks {
        let pool = Arc::clone(pool);
        let barrier = barrier.clone();
        let c_write = c_write.clone();
        let c_trim = c_trim.clone();
        let c_delete = c_delete.clone();
        let c_recreate = c_recreate.clone();
        let c_retries = c_retries.clone();
        let c_resyncs = c_resyncs.clone();
        let c_regressions = c_regressions.clone();
        let c_fatal = c_fatal.clone();

        // org 2 keeps these aggregates disjoint from the main bench (org 1);
        // the client_id offset keeps the server-side idempotency cache
        // disjoint too.
        let aggregate_key = AggregateKey::new(2, 1, id as u128);
        let client_id: u128 = (1u128 << 32) + id as u128;
        // Every 4th task never deletes: its trim floor persists across the
        // whole run, so the audit's floor check gets real coverage (deleting
        // tasks clear their floor with each acked delete and usually end
        // with nothing to assert).
        let trim_only = id % 4 == 3;

        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let deadline = Instant::now() + Duration::from_secs(duration_secs);

            let mut ledger = DeleteTrimLedger {
                aggregate_key: aggregate_key.clone(),
                client_id,
                highest_acked_version: 0,
                acked_trim_floor: 0,
                expect_deleted: false,
                highest_server_acked_version: 0,
                recreate_maybe_landed: false,
            };
            let mut client_seq: u64 = 1;
            let mut backoff_ms: u64 = 0;
            // Set when a delete attempt errored in transit — the next
            // AggregateNotExists is then "my delete landed", not an anomaly.
            let mut delete_maybe_landed = false;

            'outer: while Instant::now() < deadline {
                // ── Write phase (doubles as the recreate after a delete) ──
                for _ in 0..WRITES_PER_CYCLE {
                    if Instant::now() >= deadline { break 'outer; }
                    let recreate = ledger.expect_deleted;
                    let event = DatablockAggregateEvent {
                        client_seq,
                        event_seq: 0,
                        event_id: None,
                        event_timestamp: 0,
                        event_type_major: 1,
                        event_type_minor: 0,
                        event_value: Arc::new(format!("[dt-{id}-s-{client_seq}]").into_bytes()),
                        iv: None,
                    };
                    let res = pool.write_events_with(
                        aggregate_key.clone(),
                        vec![event],
                        client_id,
                        WriteEventsOptions {
                            allow_create: true,
                            expected_version: None,
                            enforce_client_idempotency: true,
                        },
                    ).await;
                    match res {
                        Ok(ack) => {
                            if let Some(v) = ack.max_aggregate_version {
                                // Strict monotonicity across the whole
                                // delete/recreate chain, judged only on
                                // versions the server itself returned. A
                                // regression here is the server acking a
                                // version at or below one it already acked —
                                // re-issued versions, WAL corruption, never
                                // load noise.
                                if v <= ledger.highest_server_acked_version {
                                    c_regressions.fetch_add(1, Ordering::Relaxed);
                                    eprintln!(
                                        "[delete-trim {id}] VERSION REGRESSION: acked v{v} after acked v{}",
                                        ledger.highest_server_acked_version
                                    );
                                }
                                ledger.highest_server_acked_version = ledger.highest_server_acked_version.max(v);
                                ledger.highest_acked_version = ledger.highest_acked_version.max(v);
                            } else {
                                ledger.highest_acked_version += 1;
                            }
                            if recreate {
                                c_recreate.fetch_add(1, Ordering::Relaxed);
                            } else {
                                c_write.fetch_add(1, Ordering::Relaxed);
                            }
                            ledger.expect_deleted = false;
                            ledger.recreate_maybe_landed = false;
                            client_seq += 1;
                            backoff_ms = 0;
                        }
                        Err(ClientError::Server(ServerError::Write {
                            kind: WriteError::ClientIdempotencyViolation { .. }, ..
                        })) => {
                            // Retried write whose first attempt landed: same
                            // 1-batch-per-seq algebra as the main bench.
                            ledger.highest_acked_version += 1;
                            ledger.expect_deleted = false;
                            ledger.recreate_maybe_landed = false;
                            client_seq += 1;
                            backoff_ms = 0;
                        }
                        Err(e) if retriable_write(&e) => {
                            if recreate {
                                ledger.recreate_maybe_landed = true;
                            }
                            c_retries.fetch_add(1, Ordering::Relaxed);
                            jittered_backoff(&mut backoff_ms, id as u64, client_seq).await;
                        }
                        Err(e) => {
                            c_fatal.fetch_add(1, Ordering::Relaxed);
                            eprintln!("[delete-trim {id}] fatal write at seq {client_seq}: {e}");
                            break 'outer;
                        }
                    }
                }

                // ── Trim: keep only the latest acked batch ──
                if ledger.highest_acked_version > 1 {
                    let keep_from = ledger.highest_acked_version;
                    loop {
                        if Instant::now() >= deadline { break 'outer; }
                        let res = pool.trim_start(TrimStartRequest {
                            correlation_id: None,
                            aggregate_key: aggregate_key.clone(),
                            keep_from_aggregate_version: keep_from,
                            client_id,
                            user_id: None,
                        }).await;
                        match res {
                            Ok(_) => {
                                ledger.acked_trim_floor = ledger.acked_trim_floor.max(keep_from);
                                c_trim.fetch_add(1, Ordering::Relaxed);
                                backoff_ms = 0;
                                break;
                            }
                            Err(ClientError::Server(ServerError::Trim {
                                kind: TrimError::IndexOutOfRange, ..
                            })) => {
                                // Belief diverged from server state (lost-ack
                                // write landed, version higher than tracked).
                                // Resync and move on.
                                c_resyncs.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(ClientError::Server(ServerError::Trim {
                                kind: TrimError::ReplicationError | TrimError::ReplicationBackpressure | TrimError::FsyncError | TrimError::CacheError, ..
                            })) => {
                                c_retries.fetch_add(1, Ordering::Relaxed);
                                jittered_backoff(&mut backoff_ms, id as u64, keep_from).await;
                            }
                            Err(ClientError::Server(_)) => {
                                c_resyncs.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(_) => {
                                c_retries.fetch_add(1, Ordering::Relaxed);
                                jittered_backoff(&mut backoff_ms, id as u64, keep_from).await;
                            }
                        }
                    }
                }

                // ── Delete (OCC) then recreate next cycle via continuation ──
                if trim_only {
                    continue;
                }
                loop {
                    if Instant::now() >= deadline { break 'outer; }
                    let mut deletes = HashMap::new();
                    deletes.insert(aggregate_key.clone(), SingleAggregateDelete {
                        allow_recreate: true,
                        allow_sequence_continuation: true,
                        expected_version: Some(ledger.highest_acked_version),
                    });
                    let res = pool.delete(DeleteRequest {
                        correlation_id: None,
                        client_id,
                        user_id: None,
                        deletes,
                    }).await;
                    match res {
                        Ok(_) => {
                            ledger.expect_deleted = true;
                            ledger.acked_trim_floor = 0;
                            delete_maybe_landed = false;
                            c_delete.fetch_add(1, Ordering::Relaxed);
                            backoff_ms = 0;
                            break;
                        }
                        Err(ClientError::Server(ServerError::Delete {
                            kind: DeleteError::AggregateNotExists, ..
                        })) if delete_maybe_landed => {
                            // Our earlier attempt landed; the ack was lost in
                            // transit. Same destructive outcome — treat as acked.
                            ledger.expect_deleted = true;
                            ledger.acked_trim_floor = 0;
                            delete_maybe_landed = false;
                            c_delete.fetch_add(1, Ordering::Relaxed);
                            backoff_ms = 0;
                            break;
                        }
                        Err(ClientError::Server(ServerError::Delete {
                            kind: DeleteError::AggregateNotExists, ..
                        })) => {
                            // No attempt in flight, so the ledger has drifted
                            // from server state (e.g. an acked recreate lost
                            // across failover left the old tombstone). Don't
                            // assert a deletion we never acked — resync and
                            // recreate on the next write phase.
                            c_resyncs.fetch_add(1, Ordering::Relaxed);
                            ledger.expect_deleted = false;
                            ledger.acked_trim_floor = 0;
                            break;
                        }
                        Err(ClientError::Server(ServerError::Delete {
                            kind: DeleteError::OptimisticConcurrencyViolation { current_aggregate_version, .. }, ..
                        })) => {
                            // Single writer, so a genuine concurrent change is
                            // impossible — this is a lost-ack write that landed.
                            // Resync the expected version and retry.
                            c_resyncs.fetch_add(1, Ordering::Relaxed);
                            if let Some(v) = current_aggregate_version {
                                ledger.highest_acked_version = ledger.highest_acked_version.max(v);
                            }
                            jittered_backoff(&mut backoff_ms, id as u64, client_seq).await;
                        }
                        Err(ClientError::Server(ServerError::Delete {
                            kind: DeleteError::ReplicationError | DeleteError::ReplicationBackpressure | DeleteError::FsyncError | DeleteError::CacheError, ..
                        })) => {
                            delete_maybe_landed = true;
                            c_retries.fetch_add(1, Ordering::Relaxed);
                            jittered_backoff(&mut backoff_ms, id as u64, client_seq).await;
                        }
                        Err(ClientError::Server(_)) => {
                            c_fatal.fetch_add(1, Ordering::Relaxed);
                            eprintln!("[delete-trim {id}] fatal delete: unhandled server rejection");
                            break 'outer;
                        }
                        Err(_) => {
                            delete_maybe_landed = true;
                            c_retries.fetch_add(1, Ordering::Relaxed);
                            jittered_backoff(&mut backoff_ms, id as u64, client_seq).await;
                        }
                    }
                }
            }

            ledger
        }));
    }

    let mut ledgers = Vec::with_capacity(num_tasks);
    for t in tasks {
        if let Ok(l) = t.await {
            ledgers.push(l);
        }
    }

    DeleteTrimOutcome {
        counters: DeleteTrimCounters {
            write_acks: c_write.load(Ordering::Relaxed),
            trim_acks: c_trim.load(Ordering::Relaxed),
            delete_acks: c_delete.load(Ordering::Relaxed),
            recreate_acks: c_recreate.load(Ordering::Relaxed),
            retries: c_retries.load(Ordering::Relaxed),
            occ_resyncs: c_resyncs.load(Ordering::Relaxed),
            version_regressions: c_regressions.load(Ordering::Relaxed),
            fatal_errors: c_fatal.load(Ordering::Relaxed),
        },
        ledgers,
    }
}

/// Post-settle false-ack audit. Run after the cluster has converged (same
/// settle the integrity audit uses) so read staleness can't masquerade as a
/// violation.
pub async fn audit_delete_trim(
    pool: &Arc<CeleriantPool>,
    outcome: &DeleteTrimOutcome,
) -> DeleteTrimAuditReport {
    audit_delete_trim_pinned(pool, outcome, &[]).await
}

/// `audit_delete_trim` with per-node pinned pools: every flagged aggregate is
/// re-read from each node so "durably lost" separates from "invisible on one
/// node" — the two have entirely different root causes.
pub async fn audit_delete_trim_pinned(
    pool: &Arc<CeleriantPool>,
    outcome: &DeleteTrimOutcome,
    pinned: &[(String, Arc<CeleriantPool>)],
) -> DeleteTrimAuditReport {
    const READ_MAX_ATTEMPTS: u32 = 6;
    const MAX_SAMPLES: usize = 16;
    let mut report = DeleteTrimAuditReport::default();

    for ledger in &outcome.ledgers {
        if ledger.highest_acked_version == 0 {
            continue;
        }
        report.tasks_audited += 1;

        let mut details = None;
        let mut deleted_observed = false;
        let mut backoff_ms: u64 = 100;
        for attempt in 1..=READ_MAX_ATTEMPTS {
            match pool.aggregate_details(AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: ledger.aggregate_key.clone(),
            }).await {
                Ok(d) => {
                    deleted_observed = d.is_deleted;
                    details = Some(d);
                    break;
                }
                Err(ClientError::Server(ServerError::Details {
                    kind: DetailsError::AggregateNotExists, ..
                })) => {
                    deleted_observed = true;
                    break;
                }
                Err(_) if attempt < READ_MAX_ATTEMPTS => {
                    jittered_backoff(&mut backoff_ms, ledger.client_id as u64, attempt as u64).await;
                }
                Err(_) => {
                    report.tasks_unreadable += 1;
                }
            }
        }

        // On a violation, re-read from every pinned node: "durably lost"
        // (all nodes agree) and "invisible on one node" have entirely
        // different root causes, and the sample must say which.
        let mut flag = |report: &mut DeleteTrimAuditReport, base: String, per_node: Option<(String, bool)>| {
            let s = match per_node {
                Some((nodes, disagree)) => {
                    if disagree {
                        report.node_divergences += 1;
                    }
                    format!("{base} [{nodes}]")
                }
                None => base,
            };
            if report.samples.len() < MAX_SAMPLES {
                report.samples.push(s);
            }
        };

        if ledger.expect_deleted {
            if !deleted_observed && details.is_some() {
                if ledger.recreate_maybe_landed {
                    // A recreate attempt errored in transit after the delete
                    // ack — it may have landed, making "live" legitimate.
                    report.ambiguous_recreates_landed += 1;
                } else {
                    report.false_acked_deletes += 1;
                    let per_node = per_node_state(pinned, &ledger.aggregate_key).await;
                    flag(&mut report, format!(
                        "{}: delete acked but aggregate reads live at v{}",
                        ledger.aggregate_key,
                        details.as_ref().map(|d| d.max_aggregate_version).unwrap_or(0),
                    ), per_node);
                }
            }
            continue;
        }

        if deleted_observed {
            // We never got the delete ack but it landed — chaos ate the
            // response. Contextual, not a contract breach.
            report.unacked_deletes_landed += 1;
            continue;
        }

        if let Some(d) = details {
            if d.max_aggregate_version < ledger.highest_acked_version {
                report.acked_version_loss += 1;
                let per_node = per_node_state(pinned, &ledger.aggregate_key).await;
                flag(&mut report, format!(
                    "{}: acked v{} but server reports v{}",
                    ledger.aggregate_key, ledger.highest_acked_version, d.max_aggregate_version,
                ), per_node);
            }
            if ledger.acked_trim_floor > 0 && d.min_aggregate_version < ledger.acked_trim_floor {
                report.trim_floor_breaches += 1;
                let per_node = per_node_state(pinned, &ledger.aggregate_key).await;
                flag(&mut report, format!(
                    "{}: trim to {} acked but min_aggregate_version is {}",
                    ledger.aggregate_key, ledger.acked_trim_floor, d.min_aggregate_version,
                ), per_node);
            }
        }
    }

    report
}

/// One details read per pinned node. Returns the rendered per-node summary
/// and whether any two readable nodes disagree on (deleted, min, max).
async fn per_node_state(
    pinned: &[(String, Arc<CeleriantPool>)],
    key: &AggregateKey,
) -> Option<(String, bool)> {
    if pinned.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    let mut states: Vec<Option<(bool, u64, u64)>> = Vec::new();
    for (node, p) in pinned {
        match p.aggregate_details(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: key.clone(),
        }).await {
            Ok(d) => {
                states.push(Some((d.is_deleted, d.min_aggregate_version, d.max_aggregate_version)));
                parts.push(format!("{node}: deleted={} min={} max={}", d.is_deleted, d.min_aggregate_version, d.max_aggregate_version));
            }
            Err(ClientError::Server(ServerError::Details {
                kind: DetailsError::AggregateNotExists, ..
            })) => {
                states.push(Some((true, 0, 0)));
                parts.push(format!("{node}: not-exists"));
            }
            Err(e) => {
                states.push(None);
                parts.push(format!("{node}: read-err {e}"));
            }
        }
    }
    let known: Vec<&(bool, u64, u64)> = states.iter().flatten().collect();
    let disagree = known.len() >= 2 && known.windows(2).any(|w| w[0] != w[1]);
    Some((parts.join("; "), disagree))
}
