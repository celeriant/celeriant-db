//! Watch delivery across a leader failover (remaining-tests.md item 4).
//!
//! Records a watcher's delivered-event history while writing across a leader
//! kill + promotion, re-subscribing to the new leader when the connection
//! drops, then reads the full event list from the new leader and asserts the
//! watch history is consistent with that final read — the contract added to
//! `invariants.md` "Watch Subscriptions / Behaviour across failover":
//!
//! - **Subset.** Every delivered `to_aggregate_version` exists in the final
//!   read for its aggregate. A watcher must never see a version the cluster
//!   didn't durably keep (the strongest failure: phantom delivery of a write
//!   that a promoted leader rolled back).
//! - **No dup / no reorder within a connection.** Per aggregate, each
//!   connection's stream delivers strictly increasing versions.
//! - **Liveness both sides.** The watcher delivered events before AND after
//!   the failover — proving the disconnect was observed (not a silent hang)
//!   and the promoted leader delivers post-durability.
//!
//! A gap across the reconnect is allowed by the contract (no historical
//! replay on re-subscribe) and is NOT asserted as loss: the missing versions
//! live in the final read, which is a superset.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::watch_connection::{WatchConnection, WatchOptions};
use celeriant_msg::request::requests::WatchRequest;
use crate::{
    metamorphic_common::{format_key, wait_for_promotion},
    read_all_batches, s3_cluster_config, wait_for_election_and_replication, write_event,
    MinioContainer, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const NUM_SHARDS: usize = 2;
const PRE_EVENTS: u64 = 15;
const POST_EVENTS: u64 = 15;

/// One delivered watch event, reduced to what the oracle needs.
#[derive(Clone, Copy, Debug)]
struct Delivered {
    epoch: u32, // connection generation: 0 = pre-failover, 1 = post
    agg_id: u128,
    to_version: u64,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Watch Failover ===\n");

    let port_base = 16900 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 60;
    let minio_port = port_base + 15;

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

    let mut b_config = config.clone();
    b_config.client_port = node_b_port;
    println!("Starting node B (follower) on port {}...", node_b_port);
    let node_b = TestServer::start_with_config_labeled(node_b_port, b_config, "node-b".into()).await?;

    println!("Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    // s3_cluster_config routes by aggregate_type_id; spread over both shards.
    let keys: Vec<AggregateKey> =
        (1..=NUM_SHARDS as u128).map(|id| AggregateKey::new(1, id, id)).collect();
    let watched_aggs: HashSet<u128> = keys.iter().map(|k| k.aggregate_id).collect();

    let delivered = Arc::new(Mutex::new(Vec::<Delivered>::new()));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // ── Watcher pump: subscribe, collect, reconnect on drop ───────────────
    // The pump targets whichever address currently leads; on a connection
    // error it bumps the epoch and re-resolves the leader. Reads run with a
    // short per-event timeout so kill is observed promptly.
    let watcher = {
        let delivered = delivered.clone();
        let stop = stop.clone();
        let watched = watched_aggs.clone();
        let addr_a = node_a.address().to_string();
        let addr_b = node_b.address().to_string();
        tokio::spawn(async move {
            let mut epoch: u32 = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // First connection targets A; reconnects probe both for the
                // current leader (writes-accepting node).
                let addr = if epoch == 0 {
                    addr_a.clone()
                } else {
                    match resolve_leader(&[&addr_a, &addr_b]).await {
                        Some(a) => a,
                        None => {
                            tokio::time::sleep(Duration::from_millis(300)).await;
                            continue;
                        }
                    }
                };
                let request = WatchRequest {
                    correlation_id: None,
                    requested_latency_ms: Some(100),
                    shard_id: None,
                    orgs: Some(HashSet::from([1u128])),
                    aggregate_types: None,
                    aggregates: Some(watched.clone()),
                    operation_types: None,
                };
                let options = WatchOptions {
                    timeout: Some(Duration::from_secs(2)),
                    max_shard_hint: Some((NUM_SHARDS - 1) as u64),
                    ..Default::default()
                };
                let mut conn = match WatchConnection::connect(&addr, request, options).await {
                    Ok(c) => c,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        continue;
                    }
                };
                // Pump until the connection errors (failover) or stop is set.
                loop {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    match conn.next_timeout(Duration::from_millis(500)).await {
                        Ok(Some(resp)) => {
                            let mut d = delivered.lock().unwrap();
                            for ev in resp.events {
                                if let Some(to) = ev.to_aggregate_version {
                                    d.push(Delivered { epoch, agg_id: ev.aggregate_id, to_version: to });
                                }
                            }
                        }
                        Ok(None) => {} // idle/heartbeat
                        Err(_) => break, // connection dropped — reconnect
                    }
                }
                epoch += 1;
            }
        })
    };

    // Give the watcher a moment to establish before the first writes.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── Pre-failover writes ───────────────────────────────────────────────
    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    println!("Pre-failover: writing events 1..={} × {} aggregates to leader A...", PRE_EVENTS, keys.len());
    for event_num in 1..=PRE_EVENTS {
        for key in &keys {
            write_event(&mut a_client, key, event_num, event_num == 1).await?;
        }
        tokio::time::sleep(Duration::from_millis(40)).await; // let watch delivery keep pace
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let pre_count = delivered.lock().unwrap().len();
    println!("  Watcher delivered {} events pre-failover.", pre_count);

    // ── Kill leader, promote B ────────────────────────────────────────────
    drop(a_client);
    node_a.stop();
    println!("\nLeader A killed. Waiting for B to promote...");
    let mut b_client = wait_for_promotion(node_b.address()).await?;
    println!("  Node B promoted.");

    // ── Post-failover writes to new leader ────────────────────────────────
    println!("Post-failover: writing events {}..={} × {} aggregates to leader B...", PRE_EVENTS + 1, PRE_EVENTS + POST_EVENTS, keys.len());
    for event_num in (PRE_EVENTS + 1)..=(PRE_EVENTS + POST_EVENTS) {
        for key in &keys {
            write_event(&mut b_client, key, event_num, false).await?;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── Stop the watcher, snapshot its history ────────────────────────────
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(5), watcher).await;
    let history = delivered.lock().unwrap().clone();
    println!("\nWatcher delivered {} events total across {} epoch(s).",
        history.len(),
        history.iter().map(|d| d.epoch).max().map(|m| m + 1).unwrap_or(0),
    );

    // ── Final read from the new leader ────────────────────────────────────
    let mut final_versions: std::collections::HashMap<u128, HashSet<u64>> = std::collections::HashMap::new();
    for key in &keys {
        let batches = read_all_batches(&mut b_client, key).await?;
        let versions: HashSet<u64> = batches.iter().map(|b| b.aggregate_version).collect();
        println!("  aggregate {}: {} versions in final read", format_key(key), versions.len());
        final_versions.insert(key.aggregate_id, versions);
    }

    // ── Oracles ───────────────────────────────────────────────────────────
    let mut failures = Vec::<String>::new();

    // Subset: every delivered version exists in the final read.
    for d in &history {
        match final_versions.get(&d.agg_id) {
            Some(v) if v.contains(&d.to_version) => {}
            _ => failures.push(format!(
                "phantom delivery: agg={} version={} (epoch {}) delivered to watcher but absent from final read",
                d.agg_id, d.to_version, d.epoch
            )),
        }
    }

    // No dup / no reorder within a connection epoch, per aggregate.
    let mut seen_max: std::collections::HashMap<(u32, u128), u64> = std::collections::HashMap::new();
    for d in &history {
        let key = (d.epoch, d.agg_id);
        if let Some(&prev) = seen_max.get(&key) {
            if d.to_version <= prev {
                failures.push(format!(
                    "non-monotonic delivery within epoch {}: agg={} version {} after {}",
                    d.epoch, d.agg_id, d.to_version, prev
                ));
            }
        }
        seen_max.insert(key, d.to_version);
    }

    // Liveness both sides of the failover.
    let saw_pre = history.iter().any(|d| d.epoch == 0);
    let saw_post = history.iter().any(|d| d.epoch >= 1);
    if !saw_pre {
        failures.push("watcher delivered nothing before failover".into());
    }
    if !saw_post {
        failures.push("watcher delivered nothing after failover — reconnect to new leader did not resume delivery".into());
    }

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("  FAIL: {}", f);
        }
        return Err(format!("watch_failover: {} oracle violation(s)", failures.len()).into());
    }

    println!("\n=== PASS: watch history consistent with final read across failover ===");
    Ok(())
}

/// Probe addresses for the one currently accepting writes (the leader),
/// using the same fresh-probe-key trick as `wait_for_promotion`: a unique
/// `allow_create` key per attempt so it never collides with a prior probe.
async fn resolve_leader(addrs: &[&str]) -> Option<String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static PROBE: AtomicU64 = AtomicU64::new(0);
    let n = PROBE.fetch_add(1, Ordering::Relaxed) as u128;
    for addr in addrs {
        if let Ok(mut c) = CeleriantClient::connect(addr).await {
            let probe = AggregateKey::new(8888, 8888, n);
            if write_event(&mut c, &probe, 1, true).await.is_ok() {
                return Some(addr.to_string());
            }
        }
    }
    None
}
