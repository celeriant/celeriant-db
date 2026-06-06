//! Adversarial watch load for the chaos harness.
//!
//! Drives the watch lifecycle hard against a live cluster, concurrently with the
//! normal write bench: rapid connect/disconnect churn (the CLOSE-WAIT leak
//! vector), slow/never-reading watchers (server-side back-pressure), and
//! long-lived watchers that must keep receiving events throughout. The scenario
//! that calls this then checks the server stayed healthy, the subscriber gauge
//! drained, and a fresh dial is still prompt.
//!
//! Every watch request filters on org=1, aggregate_type=1 AND a set of aggregate
//! ids. Providing all three sharding dimensions makes the request route validly
//! whatever the cluster's `RoutingRule` is, and the filter matches the write
//! bench's `(1, 1, id)` keys so long-lived watchers actually see events.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::ClientTlsConfig;
use celeriant_client_tokio::{WatchConnection, WatchOptions};
use celeriant_msg::request::requests::WatchRequest;
use tokio::time::Instant;

use crate::history::HistoryRecorder;

/// Operation discriminant for the operation-type filter (WRITE). Literal to avoid
/// depending on the server-internal celeriant_watch crate.
const OP_WRITE: u8 = 1;

#[derive(Debug, Clone)]
pub struct WatchFloodParams {
    pub duration_secs: u64,
    pub churn_tasks: usize,
    pub long_lived_tasks: usize,
    pub slow_tasks: usize,
    /// Aggregate ids to watch/filter. Overlap the write bench's key space so the
    /// long-lived watchers receive events.
    pub aggregate_ids: Vec<u128>,
}

impl Default for WatchFloodParams {
    fn default() -> Self {
        Self {
            duration_secs: 60,
            churn_tasks: 64,
            long_lived_tasks: 8,
            slow_tasks: 4,
            aggregate_ids: (0u128..64).collect(),
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct WatchFloodResult {
    pub connect_attempts: u64,
    pub connect_errors: u64,
    /// Successful connect → (read a few frames) → disconnect cycles.
    pub cycles: u64,
    pub frames_received: u64,
    /// Non-empty watch events delivered to long-lived watchers.
    pub events_received: u64,
}

fn watch_opts(tls: &ClientTlsConfig) -> WatchOptions {
    WatchOptions {
        timeout: Some(Duration::from_secs(10)),
        tls_config: Some(tls.clone()),
        ..WatchOptions::default()
    }
}

fn watch_req(aggregates: HashSet<u128>, op_filter: bool) -> WatchRequest {
    WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: Some(HashSet::from([1])),
        aggregate_types: Some(HashSet::from([1])),
        aggregates: Some(aggregates),
        operation_types: op_filter.then(|| HashSet::from([OP_WRITE])),
    }
}

/// Run the watch flood for `duration_secs`, then drop every connection and return
/// counts. The caller measures the post-flood drain/dial separately.
pub async fn run_watch_flood(
    address: &str,
    tls: ClientTlsConfig,
    params: WatchFloodParams,
) -> WatchFloodResult {
    run_watch_flood_inner(address, tls, params, None).await
}

/// Like `run_watch_flood` but records each delivered watch event to `history`.
/// Long-lived watchers emit a `WatchDelivery` line per event, enabling the
/// `WatchPerConnectionOrdered` and `WatchDeliveredDurable` checkers.
pub async fn run_watch_flood_with_history(
    address: &str,
    tls: ClientTlsConfig,
    params: WatchFloodParams,
    history: Arc<HistoryRecorder>,
) -> WatchFloodResult {
    run_watch_flood_inner(address, tls, params, Some(history)).await
}

async fn run_watch_flood_inner(
    address: &str,
    tls: ClientTlsConfig,
    params: WatchFloodParams,
    history: Option<Arc<HistoryRecorder>>,
) -> WatchFloodResult {
    let running = Arc::new(AtomicBool::new(true));
    let attempts = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let cycles = Arc::new(AtomicU64::new(0));
    let frames = Arc::new(AtomicU64::new(0));
    let events = Arc::new(AtomicU64::new(0));
    let ids = Arc::new(params.aggregate_ids.clone());

    let mut handles = Vec::new();

    for i in 0..params.churn_tasks {
        let address = address.to_string();
        let tls = tls.clone();
        let ids = Arc::clone(&ids);
        let running = Arc::clone(&running);
        let attempts = Arc::clone(&attempts);
        let errors = Arc::clone(&errors);
        let cycles = Arc::clone(&cycles);
        let frames = Arc::clone(&frames);
        handles.push(tokio::spawn(async move {
            churn_watcher(i, address, tls, ids, running, attempts, errors, cycles, frames).await;
        }));
    }

    for i in 0..params.long_lived_tasks {
        let address = address.to_string();
        let tls = tls.clone();
        let ids = Arc::clone(&ids);
        let running = Arc::clone(&running);
        let attempts = Arc::clone(&attempts);
        let errors = Arc::clone(&errors);
        let events = Arc::clone(&events);
        let history = history.clone();
        handles.push(tokio::spawn(async move {
            long_lived_watcher(i, address, tls, ids, running, attempts, errors, events, history).await;
        }));
    }

    for _ in 0..params.slow_tasks {
        let address = address.to_string();
        let tls = tls.clone();
        let ids = Arc::clone(&ids);
        let running = Arc::clone(&running);
        let attempts = Arc::clone(&attempts);
        let errors = Arc::clone(&errors);
        handles.push(tokio::spawn(async move {
            slow_watcher(address, tls, ids, running, attempts, errors).await;
        }));
    }

    tokio::time::sleep(Duration::from_secs(params.duration_secs)).await;
    running.store(false, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    WatchFloodResult {
        connect_attempts: attempts.load(Ordering::Relaxed),
        connect_errors: errors.load(Ordering::Relaxed),
        cycles: cycles.load(Ordering::Relaxed),
        frames_received: frames.load(Ordering::Relaxed),
        events_received: events.load(Ordering::Relaxed),
    }
}

/// Pick a filter shape by rotating `variant`. Mixes single-shard (one id) and
/// multi-shard (subset / full pool) routing, plus an operation-type filter.
fn churn_request(variant: u64, ids: &[u128]) -> WatchRequest {
    let len = ids.len().max(1);
    match variant % 4 {
        0 => watch_req(HashSet::from([ids[(variant as usize) % len]]), false),
        1 => watch_req(ids.iter().take(4).copied().collect(), false),
        2 => watch_req(ids.iter().copied().collect(), false),
        _ => watch_req(HashSet::from([ids[(variant as usize) % len]]), true),
    }
}

#[allow(clippy::too_many_arguments)]
async fn churn_watcher(
    id: usize,
    address: String,
    tls: ClientTlsConfig,
    ids: Arc<Vec<u128>>,
    running: Arc<AtomicBool>,
    attempts: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    cycles: Arc<AtomicU64>,
    frames: Arc<AtomicU64>,
) {
    let opts = watch_opts(&tls);
    let mut variant = id as u64;

    while running.load(Ordering::Relaxed) {
        let req = churn_request(variant, &ids);
        let read_frames = (variant % 3) as usize;
        variant = variant.wrapping_add(1);

        attempts.fetch_add(1, Ordering::Relaxed);
        match WatchConnection::connect(&address, req, opts.clone()).await {
            Ok(mut watch) => {
                cycles.fetch_add(1, Ordering::Relaxed);
                for _ in 0..read_frames {
                    match watch.next_timeout(Duration::from_millis(50)).await {
                        Ok(Some(_)) => {
                            frames.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(None) => {}
                        Err(_) => break,
                    }
                }
                // watch drops here -> FIN.
            }
            Err(_) => {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Per-task-staggered gap. Keeps the aggregate churn a sustained storm
        // while bounding client-side TIME_WAIT so the *runner* doesn't exhaust
        // ephemeral ports over a 60s window.
        let gap = 40 + (variant.wrapping_mul(2654435761) % 40);
        tokio::time::sleep(Duration::from_millis(gap)).await;
    }
}

/// Watch a single aggregate (robust single-shard) for the whole run, retrying the
/// connect and reconnecting if the storm drops us. Single-aggregate so it can't be
/// starved by multi-shard fan-out, and `ids[index]` is a key the write bench hits,
/// so events are guaranteed once connected. This is the positive delivery signal.
#[allow(clippy::too_many_arguments)]
async fn long_lived_watcher(
    index: usize,
    address: String,
    tls: ClientTlsConfig,
    ids: Arc<Vec<u128>>,
    running: Arc<AtomicBool>,
    attempts: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    events: Arc<AtomicU64>,
    history: Option<Arc<HistoryRecorder>>,
) {
    let agg = ids[index % ids.len().max(1)];
    let opts = watch_opts(&tls);
    // Stagger so the long-lived watchers don't pile into the t=0 connect herd
    // with the churn tasks (which would make their single connect fragile).
    tokio::time::sleep(Duration::from_millis(50 * index as u64)).await;

    // Bumped per successful connect: ordering guarantees hold per TCP stream,
    // and reconnects legally re-deliver the boundary range.
    let mut epoch: u32 = 0;
    while running.load(Ordering::Relaxed) {
        attempts.fetch_add(1, Ordering::Relaxed);
        let mut watch = match WatchConnection::connect(&address, watch_req(HashSet::from([agg]), false), opts.clone()).await {
            Ok(w) => w,
            Err(_) => {
                errors.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        epoch = epoch.wrapping_add(1);
        while running.load(Ordering::Relaxed) {
            match watch.next_timeout(Duration::from_millis(200)).await {
                Ok(Some(resp)) => {
                    if !resp.events.is_empty() {
                        events.fetch_add(resp.events.len() as u64, Ordering::Relaxed);
                        if let Some(h) = &history {
                            for ev in &resp.events {
                                if let (Some(from), Some(to)) = (ev.from_aggregate_version, ev.to_aggregate_version) {
                                    h.record_watch_delivery(
                                        index as u32,
                                        epoch,
                                        ev.org_id,
                                        ev.aggregate_type_id,
                                        ev.aggregate_id,
                                        from,
                                        to,
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(None) => {}
                Err(_) => break, // dropped under load — reconnect
            }
        }
    }
}

/// Hold a watch open on the whole pool and never read it (back-pressure), retrying
/// the connect and periodically reopening so it also adds churn.
async fn slow_watcher(
    address: String,
    tls: ClientTlsConfig,
    ids: Arc<Vec<u128>>,
    running: Arc<AtomicBool>,
    attempts: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) {
    let opts = watch_opts(&tls);
    while running.load(Ordering::Relaxed) {
        attempts.fetch_add(1, Ordering::Relaxed);
        let _watch = match WatchConnection::connect(&address, watch_req(ids.iter().copied().collect(), false), opts.clone()).await {
            Ok(w) => w,
            Err(_) => {
                errors.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let held = Instant::now();
        while running.load(Ordering::Relaxed) && held.elapsed() < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // drop, then reopen
    }
}

/// Open a single fresh watch and return how long the dial took (connect + ack).
/// Bounded by `timeout`; returns Err if it doesn't ack in time. Used post-flood
/// to prove the watch path didn't degrade (the original 503 / ~5s-dial symptom).
pub async fn watch_dial_probe(
    address: &str,
    tls: ClientTlsConfig,
    aggregate_id: u128,
    timeout: Duration,
) -> Result<Duration, String> {
    let req = watch_req(HashSet::from([aggregate_id]), false);
    let start = Instant::now();
    match tokio::time::timeout(timeout, WatchConnection::connect(address, req, watch_opts(&tls))).await {
        Ok(Ok(_watch)) => Ok(start.elapsed()),
        Ok(Err(e)) => Err(format!("dial failed: {e}")),
        Err(_) => Err(format!("dial did not ack within {timeout:?}")),
    }
}
