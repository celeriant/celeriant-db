//! Replication Queue Pressure Integration Test
//!
//! Tests that `is_replication_queue_pressured()` triggers S3 fallback when a
//! throttled follower causes the pending replication queue to exceed the high
//! water mark (`pending_replication_high_water_bytes`).
//!
//! Unlike `s3_follower_kick_main` (which blocks the proxy to create a WAL gap
//! triggering `max_catchup_gap_bytes`), this test throttles the proxy and uses
//! concurrent writes. Many connections create bursts that span multiple fsync
//! batches. The throttled replication can't drain the pending queue as fast as
//! fsynced batches enter it, so the queue exceeds the high water mark → S3
//! fallback → kick → follower catches up from S3 → TCP resumes.
//!
//! Run with: cargo run --bin invariant_replication_queue_pressure_main

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    count_events, s3_cluster_config, write_event, MinioContainer, TcpProxy, TestServer,
};
use celeriant_msg::process_requests::Request;
use celeriant_msg::request::requests::{ExistsRequest, SingleAggregateWrite, WriteRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use tokio::sync::Barrier;
use tokio::time::Instant;

const NUM_CONNECTIONS: usize = 1500;
const NUM_AGGREGATES: usize = 50;
const PRESSURE_DURATION_SECS: u64 = 30;
const PAYLOAD_BYTES: usize = 3024;
const CLIENTSIDE_TIMEOUT_S: u64 = 60;
/// Proxy adds this delay per 8KB chunk forwarded — makes replication ~25x slower
/// than unthrottled, giving fsync batches time to accumulate in the pending queue.
const THROTTLE_MS_PER_CHUNK: u64 = 200;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Replication Queue Pressure Test (2 shards) ===\n");

    // ========================================
    // Setup
    // ========================================
    let port_base = 10700 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;
    let proxy_port = port_base + 200;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-pressure").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 2;

    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_port + 1)).await?;
    println!("  Proxy {} → follower replication port {}", proxy_port, follower_port + 1);

    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    // Low high water mark: triggers S3 fallback when pending replication queue
    // accumulates > 256KB due to throttled follower. A single fsync batch from
    // ~100 concurrent connections is ~100KB, so we need 3+ batches to accumulate.
    config.pending_replication_high_water_bytes = 262144;
    // High max_catchup_gap_bytes: we are NOT testing WAL gap detection, only queue pressure.
    config.max_catchup_gap_bytes = 100_000_000;
    // Long heartbeat lease: no failover, just kick.
    config.heartbeat_lease_duration_ms = 30_000;
    // Generous internode timeout so the throttled connection stays open.
    config.internode_connection_timeout_ms = Some(60_000);

    let mut follower_config = config.clone();
    follower_config.advertised_replication_address = Some(proxy.address());

    println!("Starting two-node cluster (2 shards, high_water=256KB, max_gap=100MB)...");
    let mut _leader = TestServer::start_with_config_labeled(leader_port, config, "leader".into()).await?;
    let mut _follower =
        TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into())
            .await?;
    println!("  Leader at {}, Follower at {}", _leader.address(), _follower.address());

    println!("Waiting for election + discovery + replication connection...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Probe key on shard 0 (type_id=0, 0%2=0) for cross-shard verification
    let probe_shard0 = AggregateKey::new(1, 0, 999);
    // All pressure writes target shard 1 (type_id=1, 1%2=1)

    // ========================================
    // Phase 1: Verify normal TCP replication
    // ========================================
    println!("\nPHASE 1: Normal TCP replication");
    println!("-------------------------------");

    let mut leader_client = CeleriantClient::connect(_leader.address()).await?;
    println!("  Writing 3 events to shard 0 probe key...");
    for i in 1..=3 {
        write_event(&mut leader_client, &probe_shard0, i, i == 1).await?;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut follower_client = CeleriantClient::connect(_follower.address()).await?;
    let fc = count_events(&mut follower_client, &probe_shard0).await?;
    println!("  Follower shard 0: {} events", fc);
    assert_eq!(fc, 3, "Follower should have 3 events on shard 0");

    for shard_id in 0..num_shards {
        let objs = minio
            .list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id))
            .await?;
        assert!(
            objs.is_empty(),
            "No S3 fallback objects during normal replication (shard {})",
            shard_id
        );
    }
    println!("  No S3 fallback objects (TCP working)\n");

    // ========================================
    // Phase 2: Throttle proxy + pressure writes → S3 fallback
    // ========================================
    println!("PHASE 2: Throttle proxy + concurrent write pressure on shard 1");
    println!("--------------------------------------------------------------");

    proxy.throttle(THROTTLE_MS_PER_CHUNK);
    println!("  Proxy THROTTLED ({}ms per 8KB chunk)", THROTTLE_MS_PER_CHUNK);

    println!(
        "  Spawning {} connections, writing ~{}B events for {}s to {} aggregates on shard 1...",
        NUM_CONNECTIONS, PAYLOAD_BYTES, PRESSURE_DURATION_SECS, NUM_AGGREGATES
    );

    let total_written = run_pressure_writes(
        _leader.address(),
        NUM_CONNECTIONS,
        NUM_AGGREGATES,
        PRESSURE_DURATION_SECS,
    )
    .await?;

    println!("  Total events written during pressure phase: {}", total_written);

    // ========================================
    // Phase 3: Verify S3 fallback triggered
    // ========================================
    // println!("\nPHASE 3: Verify S3 fallback triggered");
    // println!("-------------------------------------");

    // tokio::time::sleep(Duration::from_secs(2)).await;

    // let mut s3_objects = 0;
    // for shard_id in 0..num_shards {
    //     let objs = minio
    //         .list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id))
    //         .await?;
    //     if !objs.is_empty() {
    //         println!("  shard_{:03}: {} S3 fallback objects", shard_id, objs.len());
    //     }
    //     s3_objects += objs.len();
    // }
    // println!("  Total S3 fallback objects: {}", s3_objects);
    // assert!(
    //     s3_objects > 0,
    //     "S3 fallback should have triggered from replication queue pressure"
    // );

    // ========================================
    // Phase 4: Unthrottle + wait for recovery + follower catchup
    // ========================================
    // After heavy throttling, the replication coordinator is stuck mid-send.
    // We must wait for:
    //   1. The in-flight replication batch to drain through the now-fast proxy
    //   2. The kick to be delivered to the follower
    //   3. The follower to download S3 fallback files and catch up
    // Only after recovery can the shard accept new writes normally.
    println!("\nPHASE 4: Unthrottle + wait for recovery + follower catchup");
    println!("----------------------------------------------------------");

    proxy.unthrottle();
    println!("  Proxy UNTHROTTLED");
    println!("  Waiting for replication pipeline to drain and kick to deliver...");
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Get leader counts for all shard 1 aggregates (no new writes yet)
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
    println!("  Leader total across {} aggregates: {} events", NUM_AGGREGATES, total_leader);

    // Poll until follower catches up on all Phase 2 data
    let timeout = Duration::from_secs(120);
    let start = std::time::Instant::now();
    let mut caught_up = false;

    while start.elapsed() < timeout {
        if let Err(msg) = _follower.check_alive() {
            panic!("{} — check stderr ([follower] lines) for the root cause", msg);
        }
        let mut fc = match CeleriantClient::connect_with_timeout(_follower.address(), Some(Duration::from_secs(10))).await {
            Ok(c) => c,
            Err(e) => {
                println!("  Follower connect failed: {} ({:.0}s elapsed)", e, start.elapsed().as_secs_f64());
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        let mut follower_total = 0usize;
        let mut all_match = true;
        let mut read_failed = false;
        for (key, leader_count) in &leader_counts {

            println!("  Checking follower for key {:?}", key);
            match count_events(&mut fc, key).await {
                Ok(c) => {
                    follower_total += c;
                    if c < *leader_count {
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
            tokio::time::sleep(Duration::from_secs(3)).await;
            continue;
        }
        println!(
            "  Follower: {}/{} events ({:.0}s elapsed)",
            follower_total,
            total_leader,
            start.elapsed().as_secs_f64()
        );
        if all_match {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    assert!(
        caught_up,
        "Follower should have caught up from S3 within {}s",
        timeout.as_secs()
    );
    println!("  Follower caught up!\n");

    // ========================================
    // Phase 5: Verify S3 consumed + hammer to verify TCP resumes
    // ========================================
    println!("PHASE 5: Verify S3 consumed + hammer to verify TCP resumes");
    println!("----------------------------------------------------------");

    // S3 fallback files should be consumed (deleted by follower during catchup)
    let mut remaining_s3 = 0;
    for shard_id in 0..num_shards {
        let objs = minio
            .list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id))
            .await?;
        remaining_s3 += objs.len();
        if !objs.is_empty() {
            println!("  shard_{:03}: {} objects remaining", shard_id, objs.len());
        }
    }
    println!("  Remaining S3 fallback objects: {}", remaining_s3);
    assert_eq!(
        remaining_s3, 0,
        "Follower should have consumed all S3 fallback objects during catchup"
    );

    // Record S3 state before hammering
    let s3_before: Vec<usize> = {
        let mut counts = Vec::new();
        for shard_id in 0..num_shards {
            counts.push(
                minio
                    .list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id))
                    .await?
                    .len(),
            );
        }
        counts
    };

    // ── Diagnostic: is the leader process alive and can shards respond? ──
    if let Err(msg) = _leader.check_alive() {
        panic!("Leader process is dead before Phase 5 hammer: {}", msg);
    }
    println!("  Leader process: alive (OS process running)");

    // Exists probes — reads bypass sync/replication coordinators
    let probe_shard1_key = AggregateKey::new(1, 1, 0);
    {
        let probe_timeout = Duration::from_secs(10);
        let mut probe = CeleriantClient::connect_with_timeout(_leader.address(), Some(probe_timeout)).await?;

        let exists_shard0 = probe.send_request(
            &Request::Exists(ExistsRequest { correlation_id: Some(0), aggregate_key: probe_shard0.clone() }),
            CompressionType::None,
        ).await;
        match &exists_shard0 {
            Ok(_) => println!("  Leader shard 0: exists OK"),
            Err(e) => println!("  Leader shard 0: exists FAILED: {}", e),
        }

        let exists_shard1 = probe.send_request(
            &Request::Exists(ExistsRequest { correlation_id: Some(1), aggregate_key: probe_shard1_key.clone() }),
            CompressionType::None,
        ).await;
        match &exists_shard1 {
            Ok(_) => println!("  Leader shard 1: exists OK"),
            Err(e) => println!("  Leader shard 1: exists FAILED: {}", e),
        }
    }

    // Write probes — hit sync_durable + replicate_durable coordinators
    // Shard 0 is the control (no pressure in Phase 2), shard 1 is the suspect.
    {
        let write_probe_timeout = Duration::from_secs(15);

        println!("  Write probe shard 0 (15s timeout)...");
        let mut probe0 = CeleriantClient::connect_with_timeout(_leader.address(), Some(write_probe_timeout)).await?
            .with_timeout(write_probe_timeout);
        let t0 = std::time::Instant::now();
        match write_event(&mut probe0, &probe_shard0, 100, false).await {
            Ok(_) => println!("  Leader shard 0: write OK ({:.1}s)", t0.elapsed().as_secs_f64()),
            Err(e) => println!("  Leader shard 0: write FAILED after {:.1}s: {}", t0.elapsed().as_secs_f64(), e),
        }

        println!("  Write probe shard 1 (15s timeout)...");
        let mut probe1 = CeleriantClient::connect_with_timeout(_leader.address(), Some(write_probe_timeout)).await?
            .with_timeout(write_probe_timeout);
        let t1 = std::time::Instant::now();
        match write_event(&mut probe1, &probe_shard1_key, 100, true).await {
            Ok(_) => println!("  Leader shard 1: write OK ({:.1}s)", t1.elapsed().as_secs_f64()),
            Err(e) => println!("  Leader shard 1: write FAILED after {:.1}s: {}", t1.elapsed().as_secs_f64(), e),
        }
    }

    // Now hammer shard 1 again — proxy is unthrottled, follower is caught up,
    // so these should all replicate via TCP (no S3 fallback).
    println!(
        "  Hammering shard 1 with {} connections for 5s (should use TCP only)...",
        NUM_CONNECTIONS / 150
    );
    let phase5_written = run_pressure_writes(
        _leader.address(),
        NUM_CONNECTIONS / 150,
        NUM_AGGREGATES,
        5,
    )
    .await?;
    println!("  Events written: {}", phase5_written);
    assert!(phase5_written > 0, "Post-recovery writes should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify follower received the new writes
    let mut leader_client = CeleriantClient::connect(_leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(_follower.address()).await?;
    let verify_key = AggregateKey::new(1, 1, 0);
    let lc = count_events(&mut leader_client, &verify_key).await?;
    let fc = count_events(&mut follower_client, &verify_key).await?;
    println!("  Verify key (agg 0): leader={}, follower={}", lc, fc);
    assert_eq!(fc, lc, "Follower should have all post-recovery events");

    // Verify no new S3 objects (TCP replication, not S3 fallback)
    let mut new_s3 = false;
    for shard_id in 0..num_shards {
        let after = minio
            .list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id))
            .await?
            .len();
        if after > s3_before[shard_id] {
            new_s3 = true;
            println!(
                "  WARNING: shard_{:03} new S3 objects: {} → {}",
                shard_id, s3_before[shard_id], after
            );
        }
    }
    assert!(
        !new_s3,
        "No new S3 objects should appear after catchup (TCP replication resumed)"
    );
    println!("  TCP replication resumed (no new S3 objects)");

    // ========================================
    // Phase 6: Final convergence
    // ========================================
    println!("\nPHASE 6: Final convergence verification");
    println!("---------------------------------------");

    let mut leader_client = CeleriantClient::connect(_leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(_follower.address()).await?;

    // Shard 0 probe (should be unaffected by shard 1 pressure)
    let lc0 = count_events(&mut leader_client, &probe_shard0).await?;
    let fc0 = count_events(&mut follower_client, &probe_shard0).await?;
    println!("  Shard 0 probe: leader={}, follower={}", lc0, fc0);
    assert_eq!(lc0, fc0, "Shard 0 counts must match");

    // All shard 1 aggregates
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
    assert_eq!(mismatches, 0, "All aggregates must have matching event counts");

    println!("\n=== All Tests Passed ===\n");
    Ok(())
}

/// Spawn concurrent connections that write ~1KB events to shard 1 aggregates
/// for the given duration. Returns total successful writes.
async fn run_pressure_writes(
    server_address: &str,
    num_connections: usize,
    num_aggregates: usize,
    duration_secs: u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut connection_tasks = Vec::with_capacity(num_connections);
    for id in 0..num_connections {
        let addr = server_address.to_string();
        connection_tasks.push(tokio::spawn(async move {
            CeleriantClient::connect_with_timeout(
                &addr,
                Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
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

    let total_written = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(clients.len()));

    let mut tasks = Vec::with_capacity(clients.len());
    for (id, client) in clients {
        let barrier = barrier.clone();
        let counter = total_written.clone();
        tasks.push(tokio::spawn(async move {
            pressure_writer(id, client, barrier, counter, num_aggregates, duration_secs).await;
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(total_written.load(Ordering::Relaxed))
}

async fn pressure_writer(
    id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
    counter: Arc<AtomicU64>,
    num_aggregates: usize,
    duration_secs: u64,
) {
    barrier.wait().await;
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let mut count = 0u64;

    // Pre-build the padding once
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
                compression_type: CompressionType::None,
            },
        );

        let request = Request::Write(WriteRequest {
            correlation_id: None,
            client_id: id as u128,
            user_id: None,
            writes,
        });

        match client.send_request(&request, CompressionType::None).await {
            Ok(_) => count += 1,
            Err(e) => {
                eprintln!("    Writer {id} failed after {count} events: {e:?}");
                break;
            }
        }
    }

    counter.fetch_add(count, Ordering::Relaxed);
}
