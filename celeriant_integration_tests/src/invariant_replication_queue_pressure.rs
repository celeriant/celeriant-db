//! Inflight Backpressure Under Throttled Replication
//!
//! Verifies the post-refactor `internode_max_request_size` cap:
//! when a slow follower stalls the in-flight queue, writes are rejected at
//! entry with `ReplicationBackpressure` (wire: `ServerBusy`). Once the
//! follower drains and the queue empties, writes are accepted again and
//! both nodes converge.
//!
//! Phases:
//!   1. Baseline TCP replication on shard 0 (sanity).
//!   2. Throttle proxy + 100 concurrent writers on shard 1 → expect a
//!      meaningful share of writes rejected with `ServerBusy`, scraped from
//!      `celeriant_writes_rejected_backpressure_total{cause="inflight_pressure"}`.
//!   3. Unthrottle, wait for the queue to drain, assert leader/follower
//!      converge on every shard 1 aggregate.
//!   4. Hammer shard 1 again — expect TCP to take every write (no more
//!      `ServerBusy`) and follower to track leader.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{
    count_events, s3_cluster_config, scrape_counter, write_event, MinioContainer, TcpProxy, TestServer,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use tokio::sync::Barrier;
use tokio::time::Instant;

const NUM_CONNECTIONS: usize = 100;
const NUM_AGGREGATES: usize = 50;
const PRESSURE_DURATION_SECS: u64 = 10;
const PAYLOAD_BYTES: usize = 3024;
const CLIENTSIDE_TIMEOUT_S: u64 = 60;
/// Proxy adds this delay per 8KB chunk forwarded — slow enough that the leader's
/// in-flight queue saturates within a single fsync cycle.
const THROTTLE_MS_PER_CHUNK: u64 = 200;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Inflight Backpressure Under Throttled Replication ===\n");

    let port_base = 10700 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;
    let proxy_port = port_base + 200;
    let leader_metrics_port = leader_port + 2;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-pressure").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 2;

    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_port + 1)).await?;
    println!("  Proxy {} -> follower replication port {}", proxy_port, follower_port + 1);

    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    // Tight inflight cap so a handful of fsync batches saturate it under throttle.
    // 256 KiB is well above one PCD (~3 KB payload * fsync window) but below the
    // pile-up that 100 writers produce when replication is slowed 25x.
    config.internode_max_request_size = 262144;
    // Disable the catchup-gap shortcut: we want to exercise the inflight cap, not
    // the workset-size S3 escape hatch.
    config.max_catchup_gap_bytes = Some(100_000_000);
    config.heartbeat_lease_duration_ms = 30_000;
    config.s3_lease_duration_ms = 30_000;
    config.internode_connection_timeout_ms = 60_000;

    let mut follower_config = config.clone();
    follower_config.advertised_replication_address = Some(proxy.address());

    println!("Starting two-node cluster (2 shards, internode_max_request_size=256KB)...");
    let mut _leader = TestServer::start_with_config_labeled(leader_port, config, "leader".into()).await?;
    let mut _follower =
        TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into())
            .await?;
    println!("  Leader at {}, Follower at {}", _leader.address(), _follower.address());

    println!("Waiting for election + discovery + replication connection...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let probe_shard0 = AggregateKey::new(1, 0, 999);

    // ── Phase 1: baseline TCP ────────────────────────────────────────────
    println!("\nPHASE 1: Normal TCP replication");
    println!("-------------------------------");

    let mut leader_client = CeleriantClient::connect(_leader.address()).await?;
    println!("  Writing 3 events to shard 0 probe key...");
    for i in 1..=3 {
        write_event(&mut leader_client, &probe_shard0, i, i == 1).await?;
    }

    let mut follower_client = CeleriantClient::connect(_follower.address()).await?;
    let fc = count_events(&mut follower_client, &probe_shard0).await?;
    println!("  Follower shard 0: {} events", fc);
    assert_eq!(fc, 3, "Follower should have 3 events on shard 0");

    let backpressure_before = scrape_counter(
        "127.0.0.1",
        leader_metrics_port,
        "celeriant_writes_rejected_backpressure_total",
    )
    .await
    .unwrap_or(0);
    println!("  Backpressure rejections before pressure: {}", backpressure_before);

    // ── Phase 2: throttle + concurrent load → expect ServerBusy ───────────
    println!("\nPHASE 2: Throttle proxy + concurrent write pressure on shard 1");
    println!("--------------------------------------------------------------");

    proxy.throttle(THROTTLE_MS_PER_CHUNK);
    println!("  Proxy THROTTLED ({}ms per 8KB chunk)", THROTTLE_MS_PER_CHUNK);

    println!(
        "  Spawning {} connections, writing ~{}B events for {}s to {} aggregates on shard 1...",
        NUM_CONNECTIONS, PAYLOAD_BYTES, PRESSURE_DURATION_SECS, NUM_AGGREGATES
    );

    let stats = run_pressure_writes(
        _leader.address(),
        NUM_CONNECTIONS,
        NUM_AGGREGATES,
        PRESSURE_DURATION_SECS,
    )
    .await?;

    println!(
        "  Writes accepted: {}, ServerBusy rejections: {}, other errors: {}",
        stats.accepted, stats.server_busy, stats.other_errors,
    );
    assert!(stats.accepted > 0, "throttled writes must still make some forward progress");
    assert!(
        stats.server_busy > 0,
        "throttled load on a tight inflight cap must produce ServerBusy rejections; got 0",
    );

    let backpressure_after = scrape_counter(
        "127.0.0.1",
        leader_metrics_port,
        "celeriant_writes_rejected_backpressure_total",
    )
    .await?;
    let backpressure_delta = backpressure_after.saturating_sub(backpressure_before);
    println!(
        "  Backpressure rejections during pressure: +{} (total {})",
        backpressure_delta, backpressure_after
    );
    assert!(
        backpressure_delta as usize >= stats.server_busy,
        "backpressure metric (+{}) must cover client-side ServerBusy count ({})",
        backpressure_delta, stats.server_busy,
    );

    // ── Phase 3: unthrottle, wait for convergence ────────────────────────
    println!("\nPHASE 3: Unthrottle + wait for queue drain + follower convergence");
    println!("-----------------------------------------------------------------");

    proxy.unthrottle();
    println!("  Proxy UNTHROTTLED");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let leader_counts: Vec<(AggregateKey, usize)> = {
        let mut lc = CeleriantClient::connect(_leader.address()).await?;
        let mut counts = Vec::new();
        for agg_id in 0..NUM_AGGREGATES {
            let key = AggregateKey::new(1, 1, agg_id as u128);
            let c = count_events(&mut lc, &key).await?;
            counts.push((key, c));
        }
        counts
    };
    let total_leader: usize = leader_counts.iter().map(|(_, c)| c).sum();
    println!(
        "  Leader total across {} aggregates: {} events",
        NUM_AGGREGATES, total_leader
    );
    assert_eq!(
        total_leader, stats.accepted,
        "leader visible count must equal accepted writes",
    );

    let timeout = Duration::from_secs(60);
    let start = std::time::Instant::now();
    let mut caught_up = false;
    while start.elapsed() < timeout {
        if let Err(msg) = _follower.check_alive() {
            panic!("{} — check stderr for the root cause", msg);
        }
        let mut fc = match CeleriantClient::connect_with_timeout(
            _follower.address(),
            Some(Duration::from_secs(10)),
            None,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                println!(
                    "  Follower connect failed: {} ({:.0}s elapsed)",
                    e,
                    start.elapsed().as_secs_f64()
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let mut follower_total = 0usize;
        let mut all_match = true;
        let mut read_failed = false;
        for (key, leader_count) in &leader_counts {
            match count_events(&mut fc, key).await {
                Ok(c) => {
                    follower_total += c;
                    if c != *leader_count {
                        all_match = false;
                    }
                }
                Err(e) => {
                    println!("  Read error for key {:?}: {} — will retry", key, e);
                    read_failed = true;
                    break;
                }
            }
        }
        if read_failed {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        println!(
            "  Follower: {}/{} events ({:.0}s elapsed)",
            follower_total, total_leader, start.elapsed().as_secs_f64()
        );
        if all_match {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    assert!(
        caught_up,
        "Follower should have converged within {}s after unthrottle",
        timeout.as_secs()
    );

    // ── Phase 4: TCP resumes cleanly under post-recovery load ────────────
    println!("\nPHASE 4: Post-recovery hammer — expect zero ServerBusy");
    println!("------------------------------------------------------");

    let post_stats = run_pressure_writes(_leader.address(), 10, NUM_AGGREGATES, 5).await?;
    println!(
        "  Writes accepted: {}, ServerBusy rejections: {}, other errors: {}",
        post_stats.accepted, post_stats.server_busy, post_stats.other_errors,
    );
    assert!(post_stats.accepted > 0, "post-recovery writes should succeed");
    assert_eq!(
        post_stats.server_busy, 0,
        "post-recovery, unthrottled cluster must not surface ServerBusy",
    );

    let mut leader_client = CeleriantClient::connect(_leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(_follower.address()).await?;
    let verify_key = AggregateKey::new(1, 1, 0);
    let lc = count_events(&mut leader_client, &verify_key).await?;
    let fc = count_events(&mut follower_client, &verify_key).await?;
    println!("  Verify key (agg 0): leader={}, follower={}", lc, fc);
    assert_eq!(fc, lc, "Follower should track leader on post-recovery writes");

    let lc0 = count_events(&mut leader_client, &probe_shard0).await?;
    let fc0 = count_events(&mut follower_client, &probe_shard0).await?;
    println!("  Shard 0 probe: leader={}, follower={}", lc0, fc0);
    assert_eq!(lc0, fc0, "Shard 0 counts must match");

    let mut mismatches = 0;
    for agg_id in 0..NUM_AGGREGATES {
        let key = AggregateKey::new(1, 1, agg_id as u128);
        let lc = count_events(&mut leader_client, &key).await?;
        let fc = count_events(&mut follower_client, &key).await?;
        if lc != fc {
            println!("  MISMATCH agg_id={}: leader={}, follower={}", agg_id, lc, fc);
            mismatches += 1;
        }
    }
    println!(
        "  Checked {} shard 1 aggregates: {} mismatches",
        NUM_AGGREGATES, mismatches
    );
    assert_eq!(mismatches, 0, "All aggregates must converge");

    println!("\n=== All Tests Passed ===\n");
    Ok(())
}

#[derive(Default, Debug)]
struct PressureStats {
    accepted: usize,
    server_busy: usize,
    other_errors: usize,
}

async fn run_pressure_writes(
    server_address: &str,
    num_connections: usize,
    num_aggregates: usize,
    duration_secs: u64,
) -> Result<PressureStats, Box<dyn std::error::Error>> {
    let mut connection_tasks = Vec::with_capacity(num_connections);
    for id in 0..num_connections {
        let addr = server_address.to_string();
        connection_tasks.push(tokio::spawn(async move {
            CeleriantClient::connect_with_timeout(
                &addr,
                Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
                None,
            )
            .await
            .map(|c| (id, c.with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S))))
            .map_err(|e| format!("conn {}: {}", id, e))
        }));
    }

    let mut clients = Vec::with_capacity(num_connections);
    let mut failed = 0;
    for task in connection_tasks {
        match task.await {
            Ok(Ok(pair)) => clients.push(pair),
            _ => failed += 1,
        }
    }
    println!("    Established {} connections ({} failed)", clients.len(), failed);

    if clients.is_empty() {
        return Err("No connections established".into());
    }

    let accepted = Arc::new(AtomicU64::new(0));
    let busy = Arc::new(AtomicU64::new(0));
    let other = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(clients.len()));

    let mut tasks = Vec::with_capacity(clients.len());
    for (id, client) in clients {
        let barrier = barrier.clone();
        let acc = accepted.clone();
        let bsy = busy.clone();
        let oth = other.clone();
        tasks.push(tokio::spawn(async move {
            pressure_writer(id, client, barrier, acc, bsy, oth, num_aggregates, duration_secs).await;
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(PressureStats {
        accepted: accepted.load(Ordering::Relaxed) as usize,
        server_busy: busy.load(Ordering::Relaxed) as usize,
        other_errors: other.load(Ordering::Relaxed) as usize,
    })
}

#[allow(clippy::too_many_arguments)]
async fn pressure_writer(
    id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
    accepted: Arc<AtomicU64>,
    busy: Arc<AtomicU64>,
    other: Arc<AtomicU64>,
    num_aggregates: usize,
    duration_secs: u64,
) {
    barrier.wait().await;
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let mut count = 0u64;

    let pad = "x".repeat(PAYLOAD_BYTES.saturating_sub(40));

    while Instant::now() < deadline {
        let agg_id = (id + count as usize) % num_aggregates;
        let key = AggregateKey::new(1, 1, agg_id as u128);

        let payload = format!("{{\"c\":{},\"e\":{},\"p\":\"{}\"}}", id, count, pad);

        let event = DatablockAggregateEvent {
            client_event_index: count,
            event_index: 0,
            event_id: None,
            event_timestamp: 0,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(payload.into_bytes()),
            iv: None,
        };

        let mut writes = HashMap::new();
        writes.insert(
            key,
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
            },
        );

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: id as u128,
            user_id: None,
            writes,
        });

        match client.send_request(&request).await {
            Ok(_) => {
                count += 1;
                accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(ClientError::ServerBusy) => {
                busy.fetch_add(1, Ordering::Relaxed);
                // Brief backoff so we keep the connection alive and try again
                // — this is the contract clients are expected to follow.
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => {
                other.fetch_add(1, Ordering::Relaxed);
                eprintln!("    Writer {id} non-busy error after {count} writes: {e:?}");
                break;
            }
        }
    }
}
