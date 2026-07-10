//! Watch delivery to a follower-side subscriber across a promotion.
//!
//! `watch_failover` covers the leader-side subscriber (reconnect to the new
//! leader, subset/no-dup/no-reorder with a permitted gap). This sibling covers
//! the other placement: the subscriber sits on the FOLLOWER, whose connection
//! survives the leader kill because its node never goes down. That closes the
//! gap `watch_failover` cannot assert — with no reconnect there is no permitted
//! gap, so delivery must be COMPLETE:
//!
//! - **Exactly once, in order, at range granularity.** One connection for the
//!   whole run; watch coalesces writes per flush window into (from, to)
//!   ranges, so the oracle asserts the delivered ranges tile 1..=TOTAL exactly
//!   — a gap is a lost delivery, an overlap is a double-fire.
//! - **Parked events fire on the promotion commit.** The final pre-kill writes
//!   are acked (durable on the follower) but the leader dies immediately after,
//!   so their commit carriers may never arrive. Promotion commits the durable
//!   tail; the parked watch events must fire then. Losing them fails the
//!   completeness check. A run is only counted when this straddle actually
//!   happened: some tail-range delivery must arrive AFTER the kill instant,
//!   else the attempt is vacuous and retried (bounded).
//!
//! Honest scope: this oracle falsifies boundary loss/duplication/reorder at
//! RANGE granularity across the flip. It structurally cannot falsify per-entry
//! interior loss inside one coalescing flush window — the accumulator merges
//! to (min from, max to), so the wire bytes are identical with or without an
//! interior event, by product design. The phase 1-3 unit exactly-once oracles
//! are the falsifier for interior loss.
//!
//! Follower watch events fire on commit (leader-confirmed drain), never on
//! follower fsync — that timing split is not observable from a client on a
//! healthy link, so it is proven by the unit oracles; here we assert the
//! client-visible consequences: completeness, uniqueness, order.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::watch_connection::{WatchConnection, WatchOptions};
use celeriant_msg::request::requests::WatchRequest;
use crate::{
    metamorphic_common::{format_key, wait_for_promotion},
    read_all_batches, s3_cluster_config, wait_for_election_and_replication, write_event,
    MinioContainer, TcpProxy, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NUM_SHARDS: usize = 2;
const PRE_EVENTS: u64 = 15;
/// Written back-to-back with no pacing, leader killed immediately after the
/// last ack — maximizes the chance these are parked (durable, unconfirmed) on
/// the follower at promotion time.
const TAIL_EVENTS: u64 = 5;
const POST_EVENTS: u64 = 10;
const TOTAL_EVENTS: u64 = PRE_EVENTS + TAIL_EVENTS + POST_EVENTS;
/// Whether the tail straddles the flip is probabilistic (commit-notify racing
/// the SIGKILL); a vacuous attempt proves nothing and is retried.
const MAX_ATTEMPTS: u32 = 3;

#[derive(Clone, Copy, Debug)]
struct Delivered {
    agg_id: u128,
    /// Inclusive range: watch coalesces writes per flush window
    /// (WatchEventAccumulator merges to (min from, max to)), so per-version
    /// assertions are wrong by contract — completeness is range tiling.
    from_version: u64,
    to_version: u64,
    at: Instant,
}

enum Attempt {
    Straddled,
    /// Every tail-range delivery arrived before the kill: commit-notify beat
    /// the SIGKILL and the parked flip-drain path was never exercised.
    Vacuous,
}

/// Per aggregate, the same expect_next walk the oracle uses: contiguous
/// delivered coverage from 1 reaching TOTAL_EVENTS. Comparing delivered RANGE
/// entries against a per-event total can never trigger under coalescing.
fn coverage_complete(history: &[Delivered], watched: &HashSet<u128>) -> bool {
    let mut expect: HashMap<u128, u64> = watched.iter().map(|&id| (id, 1)).collect();
    for d in history {
        if let Some(e) = expect.get_mut(&d.agg_id)
            && d.from_version == *e
        {
            *e = d.to_version + 1;
        }
    }
    expect.values().all(|&e| e == TOTAL_EVENTS + 1)
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Watch Follower Promotion ===\n");
    for attempt in 1..=MAX_ATTEMPTS {
        println!("--- attempt {attempt}/{MAX_ATTEMPTS} ---");
        match run_attempt(attempt).await? {
            Attempt::Straddled => {
                println!("\n=== PASS: follower subscriber delivered every event exactly once, in order, across the promotion (straddle attested) ===");
                return Ok(());
            }
            Attempt::Vacuous => {
                println!("  attempt {attempt} VACUOUS: commit-notify beat the kill; parked tail never straddled the flip — retrying");
            }
        }
    }
    Err(format!(
        "straddle never exercised in {MAX_ATTEMPTS} attempts: every tail delivery arrived before the kill, so the parked flip-drain path was not covered — not a pass"
    ).into())
}

async fn run_attempt(attempt: u32) -> Result<Attempt, Box<dyn std::error::Error>> {
    let port_base = 17200 + (std::process::id() % 100) as u16 + (attempt as u16 - 1) * 100;
    let node_a_port = port_base;
    let node_b_port = port_base + 60;
    let minio_port = port_base + 15;
    let proxy_b_port = port_base + 30;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let mut config = s3_cluster_config(
        NUM_SHARDS, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 10_000;
    config.s3_lease_duration_ms = 10_000;

    let mut a_config = config.clone();
    a_config.client_port = node_a_port;
    println!("Starting node A (leader) on port {}...", node_a_port);
    let mut node_a = TestServer::start_with_config_labeled(node_a_port, a_config, "node-a".into()).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Leader→follower replication rides a proxy so the tail's commit-notify can
    // be held in the network and dropped at the kill. The notify fires from a
    // detached timer a short delay after the last ack (not on a raw loopback the
    // kill could beat), so the proxy models the real failure: notify in flight
    // when the leader dies, and the parked tail must not straddle the flip.
    let proxy_b = TcpProxy::start(proxy_b_port, format!("127.0.0.1:{}", node_b_port + 1)).await?;
    let mut b_config = config.clone();
    b_config.client_port = node_b_port;
    b_config.advertised_replication_address = Some(proxy_b.address());
    println!("Starting node B (follower) on port {}...", node_b_port);
    let node_b = TestServer::start_with_config_labeled(node_b_port, b_config, "node-b".into()).await?;

    println!("Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    let keys: Vec<AggregateKey> =
        (1..=NUM_SHARDS as u128).map(|id| AggregateKey::new(1, id, id)).collect();
    let watched_aggs: HashSet<u128> = keys.iter().map(|k| k.aggregate_id).collect();

    let delivered = Arc::new(Mutex::new(Vec::<Delivered>::new()));
    let conn_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // One subscription to the follower for the whole run. Node B never
    // restarts, so a dropped connection is itself a failure: with no
    // historical replay on re-subscribe, a drop at promotion would lose the
    // parked events the contract says must fire.
    let watcher = {
        let delivered = delivered.clone();
        let conn_dropped = conn_dropped.clone();
        let stop = stop.clone();
        let watched = watched_aggs.clone();
        let addr_b = node_b.address().to_string();
        tokio::spawn(async move {
            let request = WatchRequest {
                correlation_id: None,
                requested_latency_ms: Some(100),
                shard_id: None,
                orgs: Some(HashSet::from([1u128])),
                aggregate_types: None,
                aggregates: Some(watched),
                operation_types: None,
            };
            let options = WatchOptions {
                timeout: Some(Duration::from_secs(2)),
                max_shard_hint: Some((NUM_SHARDS - 1) as u64),
                ..Default::default()
            };
            let mut conn = match WatchConnection::connect(&addr_b, request, options).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("  watcher: initial connect to follower failed: {}", e);
                    conn_dropped.store(true, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
            };
            loop {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
                match conn.next_timeout(Duration::from_millis(500)).await {
                    Ok(Some(resp)) => {
                        let at = Instant::now();
                        let mut d = delivered.lock().unwrap();
                        for ev in resp.events {
                            // Only Write events enter the tiling — a Read
                            // event on a watched aggregate must not paper
                            // over a gap. (1 = AggregateWatchEvent::WRITE.)
                            if ev.operation != 1 {
                                continue;
                            }
                            if let Some(to) = ev.to_aggregate_version {
                                d.push(Delivered {
                                    agg_id: ev.aggregate_id,
                                    from_version: ev.from_aggregate_version.unwrap_or(to),
                                    to_version: to,
                                    at,
                                });
                            }
                        }
                    }
                    Ok(None) => {} // idle/heartbeat
                    Err(e) => {
                        if !stop.load(std::sync::atomic::Ordering::Relaxed) {
                            eprintln!("  watcher: follower connection dropped: {}", e);
                            conn_dropped.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                        return;
                    }
                }
            }
        })
    };

    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── Paced pre-kill writes to leader A ─────────────────────────────────
    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    println!("Pre-kill: writing events 1..={} × {} aggregates to leader A...", PRE_EVENTS, keys.len());
    for event_num in 1..=PRE_EVENTS {
        for key in &keys {
            write_event(&mut a_client, key, event_num, event_num == 1).await?;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // ── Unpaced tail burst, then kill A on the last ack ───────────────────
    println!("Tail burst: events {}..={} with no pacing, then SIGKILL...", PRE_EVENTS + 1, PRE_EVENTS + TAIL_EVENTS);
    for event_num in (PRE_EVENTS + 1)..=(PRE_EVENTS + TAIL_EVENTS) {
        if event_num == PRE_EVENTS + TAIL_EVENTS {
            // Hold the link after forwarding the final batch: the proxy
            // sleeps per chunk AFTER forwarding, so the write still acks but
            // the post-ack commit-notify sits in the held connection. Fresh
            // connections (heartbeats, reconnects) are refused so no side
            // channel can carry the confirm.
            proxy_b.throttle(500);
            proxy_b.refuse_new();
        }
        for key in &keys {
            write_event(&mut a_client, key, event_num, false).await?;
        }
    }
    // Drop the in-flight notify, then kill: the acked tail is durable and
    // parked on B with its commit carrier gone — the straddle the contract
    // is about.
    proxy_b.block();
    drop(a_client);
    node_a.stop();
    // Conservative straddle reference: taken after the process is dead, so
    // any delivery stamped later definitely arrived post-kill.
    let kill_at = Instant::now();
    println!("Leader A killed. Waiting for B to promote...");
    let mut b_client = wait_for_promotion(node_b.address()).await?;
    // Lags the actual promotion commit by wait_for_promotion's 1s poll.
    let promoted_at = Instant::now();
    println!("  Node B promoted.");

    // ── Post-promotion writes to new leader B ─────────────────────────────
    println!("Post-promotion: writing events {}..={} to leader B...", PRE_EVENTS + TAIL_EVENTS + 1, TOTAL_EVENTS);
    for event_num in (PRE_EVENTS + TAIL_EVENTS + 1)..=TOTAL_EVENTS {
        for key in &keys {
            write_event(&mut b_client, key, event_num, false).await?;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // ── Bounded drain: wait for full contiguous coverage, then stop ───────
    let drain_deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let complete = coverage_complete(&delivered.lock().unwrap(), &watched_aggs);
        if complete || Instant::now() >= drain_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(5), watcher).await;
    let history = delivered.lock().unwrap().clone();
    println!("\nWatcher delivered {} range(s) across {} aggregates.", history.len(), watched_aggs.len());

    // ── Final read from the promoted leader ───────────────────────────────
    let mut final_versions: HashMap<u128, HashSet<u64>> = HashMap::new();
    for key in &keys {
        let batches = read_all_batches(&mut b_client, key).await?;
        let versions: HashSet<u64> = batches.iter().map(|b| b.aggregate_version).collect();
        println!("  aggregate {}: {} versions in final read", format_key(key), versions.len());
        final_versions.insert(key.aggregate_id, versions);
    }

    // ── Oracles ───────────────────────────────────────────────────────────
    let mut failures = Vec::<String>::new();

    if conn_dropped.load(std::sync::atomic::Ordering::Relaxed) {
        failures.push("follower watch connection dropped — subscriber must survive the promotion".into());
    }

    // Every acked write survives the promotion (durability cross-check).
    for key in &keys {
        let versions = final_versions.get(&key.aggregate_id).unwrap();
        for v in 1..=TOTAL_EVENTS {
            if !versions.contains(&v) {
                failures.push(format!(
                    "acked version missing from final read: agg={} version={}",
                    key.aggregate_id, v
                ));
            }
        }
    }

    // No phantoms: everything delivered exists in the final read.
    for d in &history {
        match final_versions.get(&d.agg_id) {
            Some(v) if v.contains(&d.to_version) => {}
            _ => failures.push(format!(
                "phantom delivery: agg={} version={} delivered but absent from final read",
                d.agg_id, d.to_version
            )),
        }
    }

    // Exactly once + complete, at range granularity (the wire contract —
    // coalescing makes per-version assertions undecidable): per aggregate,
    // the delivered ranges in arrival order must tile 1..=TOTAL_EVENTS
    // exactly. A gap means lost delivery (fsync-time AND drain-time both
    // failed to fire); an overlap means double-fire.
    let mut ranges: HashMap<u128, Vec<(u64, u64)>> = HashMap::new();
    for d in &history {
        ranges.entry(d.agg_id).or_default().push((d.from_version, d.to_version));
    }
    for key in &keys {
        let tiles = ranges.get(&key.aggregate_id).cloned().unwrap_or_default();
        let mut expect_next = 1u64;
        for (from, to) in &tiles {
            if *from != expect_next {
                failures.push(format!(
                    "range break: agg={} delivered [{}-{}] but expected next version {} (tail burst was {}..={})",
                    key.aggregate_id, from, to, expect_next, PRE_EVENTS + 1, PRE_EVENTS + TAIL_EVENTS
                ));
            }
            expect_next = to + 1;
        }
        if expect_next != TOTAL_EVENTS + 1 {
            failures.push(format!(
                "incomplete coverage: agg={} delivered through {} of {}",
                key.aggregate_id,
                expect_next - 1,
                TOTAL_EVENTS
            ));
        }
    }

    // In order: strictly increasing per aggregate across the single connection,
    // including across the promotion boundary (parked tail must not reorder).
    let mut last: HashMap<u128, u64> = HashMap::new();
    for d in &history {
        if let Some(&prev) = last.get(&d.agg_id) {
            if d.to_version <= prev {
                failures.push(format!(
                    "non-monotonic delivery: agg={} version {} after {}",
                    d.agg_id, d.to_version, prev
                ));
            }
        }
        last.insert(d.agg_id, d.to_version);
    }

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("  FAIL: {}", f);
        }
        return Err(format!("watch_follower_promotion: {} oracle violation(s)", failures.len()).into());
    }

    // ── Straddle attestation ──────────────────────────────────────────────
    // The headline scenario only happened if some tail-range delivery fired
    // at the PROMOTION, not around the kill: a commit-notify that beat the
    // SIGKILL still flushes to the client up to a watch window (~100ms)
    // after it, so "after the kill instant" alone blesses vacuous runs.
    // Promotion lands a heartbeat-TTL (10s) later; a tail delivery in its
    // window (2s slack covers the 1s promotion poll + flush latency) can
    // only be the flip-drain committing the parked tail.
    let straddle = history.iter().find(|d| {
        d.at > kill_at
            && d.at + Duration::from_secs(2) >= promoted_at
            && d.from_version <= PRE_EVENTS + TAIL_EVENTS
            && d.to_version > PRE_EVENTS
    });
    match straddle {
        Some(d) => {
            println!(
                "  straddle attested: agg={} range [{}-{}] delivered {}ms after the kill, in the promotion window",
                d.agg_id,
                d.from_version,
                d.to_version,
                d.at.duration_since(kill_at).as_millis()
            );
            Ok(Attempt::Straddled)
        }
        None => {
            for d in history.iter().filter(|d| {
                d.from_version <= PRE_EVENTS + TAIL_EVENTS && d.to_version > PRE_EVENTS
            }) {
                let rel_kill = d.at.checked_duration_since(kill_at).map(|x| x.as_millis() as i128)
                    .unwrap_or(-(kill_at.duration_since(d.at).as_millis() as i128));
                let rel_promo = d.at.checked_duration_since(promoted_at).map(|x| x.as_millis() as i128)
                    .unwrap_or(-(promoted_at.duration_since(d.at).as_millis() as i128));
                println!(
                    "  tail-range delivery: agg={} [{}-{}] kill{:+}ms promo{:+}ms",
                    d.agg_id, d.from_version, d.to_version, rel_kill, rel_promo
                );
            }
            Ok(Attempt::Vacuous)
        }
    }
}
