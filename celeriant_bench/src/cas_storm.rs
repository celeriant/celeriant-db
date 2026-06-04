//! CAS storm workload: N writers, one aggregate, same `expected_version`.
//!
//! Each round, every writer submits a write with the same CAS token
//! (`expected_version = V`), barrier-synchronized so the attempts land
//! inside a few-millisecond window. OCC must admit exactly one: one `Ok`,
//! the rest `OccConflict` — never two acks, and a conflict must be a
//! definitive rejection, not a timeout. (`s3_concurrent_cas` covers *lease*
//! CAS; this covers client OCC.)
//!
//! The authoritative oracle is `HistoryOcc` over the recorded history (at
//! most one `ok` per `(aggregate, expected_version)` group); the returned
//! counters are corroborating colour. Rounds advance lock-step: losers learn
//! the new version from the conflict error's `current_aggregate_version`.

use crate::history::HistoryRecorder;
use celeriant_client_tokio::pool::CeleriantPool;
use celeriant_client_tokio::{ClientError, ServerError, WriteEventsOptions, WriteError};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::Instant;

/// Distinct keyspace from the idempotent bench (`(1, 1, task_id)`): one
/// shared aggregate all writers contend on.
pub fn cas_storm_aggregate() -> AggregateKey {
    AggregateKey::new(1, 2, 1)
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CasStormOutcome {
    pub rounds: u64,
    pub ok_writes: u64,
    pub occ_conflicts: u64,
    /// Attempts whose outcome is unknown (timeouts, connection loss). Under
    /// chaos these are expected; an ambiguous attempt may have committed,
    /// which the history checkers account for.
    pub ambiguous: u64,
    /// Definitive non-OCC rejections (not-leader, busy, validation).
    pub other_failures: u64,
    pub elapsed_secs: f64,
}

pub async fn run_cas_storm(
    pool: &Arc<CeleriantPool>,
    writers: usize,
    duration_secs: u64,
    history: Option<Arc<HistoryRecorder>>,
) -> Result<CasStormOutcome, String> {
    let key = cas_storm_aggregate();

    // Seed the aggregate so every storm round writes with allow_create=false.
    let seed_event = make_event(0, 0);
    pool.write_events_with(
        key.clone(),
        vec![seed_event],
        9_000,
        WriteEventsOptions {
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    )
    .await
    .map_err(|e| format!("cas_storm seed write: {e}"))?;

    // After the seed batch the aggregate sits at version 1.
    let version = Arc::new(AtomicU64::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(writers));
    let ok_writes = Arc::new(AtomicU64::new(0));
    let occ_conflicts = Arc::new(AtomicU64::new(0));
    let ambiguous = Arc::new(AtomicU64::new(0));
    let other_failures = Arc::new(AtomicU64::new(0));
    let rounds = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration_secs);

    let mut tasks = Vec::with_capacity(writers);
    for w in 0..writers {
        let pool = Arc::clone(pool);
        let key = key.clone();
        let version = version.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let ok_writes = ok_writes.clone();
        let occ_conflicts = occ_conflicts.clone();
        let ambiguous = ambiguous.clone();
        let other_failures = other_failures.clone();
        let rounds = rounds.clone();
        let history = history.clone();
        let client_id: u128 = 9_001 + w as u128;

        tasks.push(tokio::spawn(async move {
            loop {
                let gate = barrier.wait().await;
                // `stop` is only ever set between the end-of-round barrier
                // and this one, so every writer reads the same value here
                // and the barrier never deadlocks on uneven exits.
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                if gate.is_leader() {
                    rounds.fetch_add(1, Ordering::Relaxed);
                }

                let v = version.load(Ordering::Relaxed);
                let req_start = Instant::now();
                let res = pool
                    .write_events_with(
                        key.clone(),
                        vec![make_event(w as u64, v)],
                        client_id,
                        WriteEventsOptions {
                            allow_create: false,
                            expected_version: Some(v),
                            enforce_client_idempotency: false,
                        },
                    )
                    .await;

                if let Some(h) = &history {
                    // client_seq doubles as the round's CAS token for
                    // readability; the OCC checker groups on expected_version.
                    h.record_op(w as u32, &key, client_id, v, Some(v), &res, req_start);
                }

                match &res {
                    Ok(_) => {
                        ok_writes.fetch_add(1, Ordering::Relaxed);
                        version.fetch_max(v + 1, Ordering::Relaxed);
                    }
                    Err(ClientError::Server(ServerError::Write {
                        kind: WriteError::OptimisticConcurrencyViolation { current_aggregate_version, .. },
                        ..
                    })) => {
                        occ_conflicts.fetch_add(1, Ordering::Relaxed);
                        if let Some(cv) = current_aggregate_version {
                            version.fetch_max(*cv, Ordering::Relaxed);
                        }
                    }
                    Err(e) => match crate::history::classify_error(e).0 {
                        crate::history::OpOutcome::Fail => {
                            other_failures.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            ambiguous.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                }

                let end = barrier.wait().await;
                if end.is_leader() && Instant::now() >= deadline {
                    stop.store(true, Ordering::Relaxed);
                }
            }
        }));
    }

    // A panicked writer would leave the rest parked on the barrier forever —
    // bound the join and abort survivors rather than wedging the scenario.
    // Grace covers the deadline plus one slow round (pool request timeout).
    let abort_handles: Vec<_> = tasks.iter().map(|t| t.abort_handle()).collect();
    let join_all = async {
        for task in tasks {
            let _ = task.await;
        }
    };
    let grace = Duration::from_secs(duration_secs + 30);
    if tokio::time::timeout(grace, join_all).await.is_err() {
        for handle in abort_handles {
            handle.abort();
        }
        return Err(format!(
            "cas_storm writers did not finish within {}s past start — a writer panicked or hung; survivors aborted",
            grace.as_secs()
        ));
    }

    Ok(CasStormOutcome {
        rounds: rounds.load(Ordering::Relaxed),
        ok_writes: ok_writes.load(Ordering::Relaxed),
        occ_conflicts: occ_conflicts.load(Ordering::Relaxed),
        ambiguous: ambiguous.load(Ordering::Relaxed),
        other_failures: other_failures.load(Ordering::Relaxed),
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

fn make_event(writer: u64, round: u64) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: None,
        event_timestamp: 0,
        event_type_major: 2,
        event_type_minor: 0,
        event_value: Arc::new(format!("[cas-w{writer}-r{round}]").into_bytes()),
        iv: None,
    }
}

