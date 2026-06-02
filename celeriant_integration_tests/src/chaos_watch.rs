//! Watch Chaos Test — adversarial watch lifecycle under concurrent read/write load.
//!
//! Hammers every dimension of the watch path at once and checks the server can't
//! be taken down or leaked through it:
//!
//! - Filter variety: single-aggregate, multi-aggregate, org-level, type-level,
//!   and operation-type filters (single-shard and multi-shard routing).
//! - Rapid connect/disconnect churn (the CLOSE-WAIT pileup vector).
//! - Abandoned half-open sockets: raw connect, send watch request, drop without
//!   ever reading the ack (FIN before the first frame).
//! - Slow/never-reading watchers (server-side back-pressure must stay bounded).
//! - Long-lived watchers that must keep receiving events while all of the above
//!   churns and writers/readers pound the same aggregates.
//!
//! After the storm: the server must still be alive, `watch_subscribers_active`
//! must drain back to zero (no leaked sessions), and a fresh watch dial must
//! still ack promptly (no permanent degradation — the original 503 symptom).
//!
//! Run with: cargo run -p celeriant_integration_tests --release -- --test chaos_watch

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::{ServerConfig, TestServer};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::{WatchConnection, WatchOptions};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::{
        read_filters::ReadFilters,
        requests::{ReadRequest, SingleAggregateWrite, WatchRequest, WriteRequest},
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey, datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use celeriant_wire::network::wire_header::PROTOCOL_VERSION_V2;
use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration as TokioDuration, Instant};
use tokio_util::compat::TokioAsyncReadCompatExt;

const NUM_SHARDS: usize = 4;
const DURATION_SECS: u64 = 15;

const WRITERS: usize = 6;
const READERS: usize = 4;
const CHURN_WATCHERS: usize = 5;
const ABANDONED_WATCHERS: usize = 3;
const LONG_LIVED_WATCHERS: usize = 3;

// Operation discriminant for the operation-type filter (WRITE). Kept as a literal
// to avoid depending on the server-internal celeriant_watch crate.
const OP_WRITE: u8 = 1;

/// The aggregate pool every worker shares. Spread across orgs/types/ids so the
/// keys hash to different shards (multi-shard watch fallback) and so org/type
/// filters span more than one aggregate.
fn aggregate_pool() -> Vec<AggregateKey> {
    let mut pool = Vec::new();
    for org in 1u128..=2 {
        for aggregate_type in 1u128..=2 {
            for id in 0u128..4 {
                pool.push(AggregateKey::new(org, aggregate_type, 1000 + org * 100 + aggregate_type * 10 + id));
            }
        }
    }
    pool
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Watch Chaos Test ===\n");

    let port = 10100 + (std::process::id() % 100) as u16;
    let config = ServerConfig {
        num_shards: Some(NUM_SHARDS),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let mut server = TestServer::start_with_config(port, config).await?;
    let addr = server.address().to_string();
    let metrics_port = server.config().metrics_port;
    println!("Server started at {} ({} shards)\n", addr, NUM_SHARDS);

    let pool = Arc::new(aggregate_pool());
    let running = Arc::new(AtomicBool::new(true));
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_write_errors = Arc::new(AtomicU64::new(0));
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_events_received = Arc::new(AtomicU64::new(0));
    let churn_cycles = Arc::new(AtomicU64::new(0));

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Writers — drive the events the watchers must receive.
    for i in 0..WRITERS {
        let addr = addr.clone();
        let pool = Arc::clone(&pool);
        let running = Arc::clone(&running);
        let writes = Arc::clone(&total_writes);
        let errors = Arc::clone(&total_write_errors);
        handles.push(tokio::spawn(async move {
            writer_task(i, addr, pool, running, writes, errors).await;
        }));
    }

    // Readers — concurrent read load against the same aggregates.
    for i in 0..READERS {
        let addr = addr.clone();
        let pool = Arc::clone(&pool);
        let running = Arc::clone(&running);
        let reads = Arc::clone(&total_reads);
        handles.push(tokio::spawn(async move {
            reader_task(i, addr, pool, running, reads).await;
        }));
    }

    // Churn — connect a watcher with a randomly chosen filter shape, read a few
    // frames, drop. The high connect/disconnect rate is the leak vector.
    for i in 0..CHURN_WATCHERS {
        let addr = addr.clone();
        let pool = Arc::clone(&pool);
        let running = Arc::clone(&running);
        let cycles = Arc::clone(&churn_cycles);
        handles.push(tokio::spawn(async move {
            churn_watcher_task(i, addr, pool, running, cycles).await;
        }));
    }

    // Abandoned half-open — raw connect, send watch request, drop without reading.
    for i in 0..ABANDONED_WATCHERS {
        let addr = addr.clone();
        let pool = Arc::clone(&pool);
        let running = Arc::clone(&running);
        let cycles = Arc::clone(&churn_cycles);
        handles.push(tokio::spawn(async move {
            abandoned_watcher_task(i, addr, pool, running, cycles).await;
        }));
    }

    // Slow reader — open a watch and never read it. Server-side accumulation must
    // stay bounded and must not wedge the other watchers.
    {
        let addr = addr.clone();
        let pool = Arc::clone(&pool);
        let running = Arc::clone(&running);
        handles.push(tokio::spawn(async move {
            slow_watcher_task(addr, pool, running).await;
        }));
    }

    // Long-lived watchers — watch the whole pool and keep receiving events for the
    // duration. These prove delivery survives the churn.
    for i in 0..LONG_LIVED_WATCHERS {
        let addr = addr.clone();
        let pool = Arc::clone(&pool);
        let running = Arc::clone(&running);
        let events = Arc::clone(&total_events_received);
        handles.push(tokio::spawn(async move {
            long_lived_watcher_task(i, addr, pool, running, events).await;
        }));
    }

    println!("Running adversarial watch load for {}s...", DURATION_SECS);
    sleep(TokioDuration::from_secs(DURATION_SECS)).await;
    running.store(false, Ordering::Relaxed);

    // Every task exits on the flag and drops its connections (FIN) on the way out.
    for h in handles {
        let _ = h.await;
    }

    let writes = total_writes.load(Ordering::Relaxed);
    let write_errors = total_write_errors.load(Ordering::Relaxed);
    let reads = total_reads.load(Ordering::Relaxed);
    let events = total_events_received.load(Ordering::Relaxed);
    let cycles = churn_cycles.load(Ordering::Relaxed);

    println!("\n=== Watch Chaos Results ===");
    println!("  writes:          {} ({} errors)", writes, write_errors);
    println!("  reads:           {}", reads);
    println!("  watch cycles:    {}", cycles);
    println!("  events received: {}", events);

    // --- Invariants ---

    // The server must still be running after the storm.
    server
        .check_alive()
        .map_err(|e| format!("server died during watch chaos: {}", e))?;

    if writes == 0 {
        return Err("no writes succeeded — load never ran".into());
    }
    // Writes must not be broadly failing (occasional SERVER_BUSY is tolerated).
    if write_errors.saturating_mul(5) > writes {
        return Err(format!(
            "excessive write errors under watch load: {}/{}",
            write_errors, writes
        )
        .into());
    }
    if cycles == 0 {
        return Err("no watch connect/disconnect cycles ran".into());
    }
    // Delivery must survive the churn: long-lived watchers saw real events.
    if events == 0 {
        return Err("long-lived watchers received no events under churn".into());
    }

    // No leaked sessions: with every client gone, the gauge must drain to zero.
    // Pre-fix, sessions linger until their next ~5s heartbeat write fails.
    let drained = wait_for_subscribers_zero(metrics_port, Duration::from_secs(5)).await?;
    if !drained {
        let remaining =
            crate::scrape_counter("127.0.0.1", metrics_port, "celeriant_watch_subscribers_active")
                .await
                .unwrap_or(u64::MAX);
        return Err(format!(
            "watch sessions leaked: {} still active 5s after all clients disconnected",
            remaining
        )
        .into());
    }
    println!("  subscribers drained to 0 after disconnect");

    // No permanent degradation: a fresh dial after the storm still acks promptly.
    let single = AggregateKey::new(1, 1, 9_999);
    let req = watch_request(Some(HashSet::from([single.aggregate_id])), None, None, None);
    let dial_start = Instant::now();
    let watch = WatchConnection::connect(&addr, req, WatchOptions::default())
        .await
        .map_err(|e| format!("post-storm dial failed: {}", e))?;
    let dial = dial_start.elapsed();
    drop(watch);
    if dial > Duration::from_secs(1) {
        return Err(format!(
            "post-storm dial took {:?} (>1s) — watch path degraded",
            dial
        )
        .into());
    }
    println!("  post-storm dial acked in {:?}", dial);

    println!("\nPASS");
    Ok(())
}

fn watch_request(
    aggregates: Option<HashSet<u128>>,
    orgs: Option<HashSet<u128>>,
    aggregate_types: Option<HashSet<u128>>,
    operation_types: Option<HashSet<u8>>,
) -> WatchRequest {
    WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs,
        aggregate_types,
        aggregates,
        operation_types,
    }
}

async fn writer_task(
    id: usize,
    addr: String,
    pool: Arc<Vec<AggregateKey>>,
    running: Arc<AtomicBool>,
    writes: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) {
    let mut client = match CeleriantClient::connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[writer {}] connect failed: {}", id, e);
            return;
        }
    };
    let mut rng = StdRng::seed_from_u64(id as u64 + 1);
    let mut seq = 0u64;

    while running.load(Ordering::Relaxed) {
        let key = pool[rng.gen_range(0..pool.len())].clone();
        let event = DatablockAggregateEvent {
            client_seq: seq,
            event_seq: 0,
            event_id: Some(rng.r#gen()),
            event_timestamp: 0,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(vec![0u8; rng.gen_range(8..256)]),
            iv: None,
        };
        let mut w = HashMap::new();
        w.insert(
            key,
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_version: None,
                enforce_client_idempotency: false,
            },
        );
        let req = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: id as u128,
            user_id: None,
            writes: w,
        });
        match client.send_request(&req).await {
            Ok(_) => {
                writes.fetch_add(1, Ordering::Relaxed);
                seq += 1;
            }
            Err(ClientError::RequestTimeout) => {
                errors.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn reader_task(
    id: usize,
    addr: String,
    pool: Arc<Vec<AggregateKey>>,
    running: Arc<AtomicBool>,
    reads: Arc<AtomicU64>,
) {
    let mut client = match CeleriantClient::connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[reader {}] connect failed: {}", id, e);
            return;
        }
    };
    let mut rng = StdRng::seed_from_u64(100 + id as u64);

    while running.load(Ordering::Relaxed) {
        let key = pool[rng.gen_range(0..pool.len())].clone();
        let req = ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: key,
            filters: ReadFilters::new(1),
        });
        // Reads of not-yet-created aggregates error harmlessly; we only count load.
        if client.send_request(&req).await.is_ok() {
            reads.fetch_add(1, Ordering::Relaxed);
        }
        sleep(TokioDuration::from_millis(5)).await;
    }
}

/// Rotate through every filter shape so single-shard and multi-shard routing,
/// plus org/type/operation filters, all get exercised under churn.
fn churn_request(variant: usize, pool: &[AggregateKey], rng: &mut StdRng) -> WatchRequest {
    match variant % 5 {
        // Single aggregate -> single shard.
        0 => {
            let key = &pool[rng.gen_range(0..pool.len())];
            watch_request(Some(HashSet::from([key.aggregate_id])), None, None, None)
        }
        // Whole pool -> multi-shard fallback.
        1 => watch_request(Some(pool.iter().map(|k| k.aggregate_id).collect()), None, None, None),
        // Org-level filter -> spans shards.
        2 => watch_request(None, Some(HashSet::from([1])), None, None),
        // Type-level filter -> spans shards.
        3 => watch_request(None, None, Some(HashSet::from([1])), None),
        // Aggregate + operation-type filter.
        _ => {
            let key = &pool[rng.gen_range(0..pool.len())];
            watch_request(
                Some(HashSet::from([key.aggregate_id])),
                None,
                None,
                Some(HashSet::from([OP_WRITE])),
            )
        }
    }
}

async fn churn_watcher_task(
    id: usize,
    addr: String,
    pool: Arc<Vec<AggregateKey>>,
    running: Arc<AtomicBool>,
    cycles: Arc<AtomicU64>,
) {
    let mut rng = StdRng::seed_from_u64(200 + id as u64);
    let mut variant = id;

    while running.load(Ordering::Relaxed) {
        let req = churn_request(variant, &pool, &mut rng);
        variant = variant.wrapping_add(1);

        match WatchConnection::connect(&addr, req, WatchOptions::default()).await {
            Ok(mut watch) => {
                cycles.fetch_add(1, Ordering::Relaxed);
                // Read a few frames, then drop (sends FIN).
                let frames = rng.gen_range(0..3);
                for _ in 0..frames {
                    if watch
                        .next_timeout(TokioDuration::from_millis(50))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                // watch drops here -> connection closed.
            }
            Err(_) => {
                // Transient SERVER_BUSY / routing churn is fine; keep cycling.
            }
        }
        sleep(TokioDuration::from_millis(rng.gen_range(10..30))).await;
    }
}

/// Raw connect, send a watch request, drop without ever reading the ack. This is
/// the precise leak shape from the bug report: the client FINs before the first
/// server frame, so the server only notices if it polls the socket for EOF.
async fn abandoned_watcher_task(
    id: usize,
    addr: String,
    pool: Arc<Vec<AggregateKey>>,
    running: Arc<AtomicBool>,
    cycles: Arc<AtomicU64>,
) {
    let mut rng = StdRng::seed_from_u64(300 + id as u64);

    while running.load(Ordering::Relaxed) {
        let key = &pool[rng.gen_range(0..pool.len())];
        let req = watch_request(Some(HashSet::from([key.aggregate_id])), None, None, None);

        if let Ok(stream) = TcpStream::connect(&addr).await {
            let _ = stream.set_nodelay(true);
            let mut stream = stream.compat();
            let _ = ClientRequest::write_request(
                &mut stream,
                &ClientRequest::Watch(req),
                10_000_000,
                PROTOCOL_VERSION_V2,
            )
            .await;
            cycles.fetch_add(1, Ordering::Relaxed);
            // Drop immediately without reading — half-open FIN.
            drop(stream);
        }
        sleep(TokioDuration::from_millis(rng.gen_range(10..30))).await;
    }
}

/// Open a watch on the whole pool and never read. Server-side event accumulation
/// must stay bounded; it must not wedge other watchers.
async fn slow_watcher_task(addr: String, pool: Arc<Vec<AggregateKey>>, running: Arc<AtomicBool>) {
    let req = watch_request(Some(pool.iter().map(|k| k.aggregate_id).collect()), None, None, None);
    let _watch = match WatchConnection::connect(&addr, req, WatchOptions::default()).await {
        Ok(w) => w,
        Err(_) => return,
    };
    while running.load(Ordering::Relaxed) {
        sleep(TokioDuration::from_millis(200)).await;
    }
    // _watch drops here.
}

async fn long_lived_watcher_task(
    id: usize,
    addr: String,
    pool: Arc<Vec<AggregateKey>>,
    running: Arc<AtomicBool>,
    events: Arc<AtomicU64>,
) {
    let req = watch_request(Some(pool.iter().map(|k| k.aggregate_id).collect()), None, None, None);
    let mut watch = match WatchConnection::connect(&addr, req, WatchOptions::default()).await {
        Ok(w) => w,
        Err(e) => {
            eprintln!("[long-lived {}] connect failed: {}", id, e);
            return;
        }
    };
    while running.load(Ordering::Relaxed) {
        match watch.next_timeout(TokioDuration::from_millis(200)).await {
            Ok(Some(resp)) => {
                if !resp.events.is_empty() {
                    events.fetch_add(resp.events.len() as u64, Ordering::Relaxed);
                }
            }
            Ok(None) => {} // timeout, keep going
            Err(_) => break,
        }
    }
    // watch drops here.
}

async fn wait_for_subscribers_zero(
    metrics_port: u16,
    within: Duration,
) -> Result<bool, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + within;
    loop {
        let active =
            crate::scrape_counter("127.0.0.1", metrics_port, "celeriant_watch_subscribers_active")
                .await?;
        if active == 0 {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        sleep(TokioDuration::from_millis(150)).await;
    }
}
