//! Debug Follower Pressure Test — Follower launched externally via VSCode debugger
//!
//! Identical scenario to `invariant_replication_queue_pressure_main`, but the
//! follower is NOT spawned as a subprocess. Instead the test waits for you to
//! launch the follower from the VSCode "Debug follower (pressure test)" config
//! so you can set breakpoints in the shard read path.
//!
//! Fixed ports (no PID randomness):
//!   Leader:   11000 (client) / 11001 (replication)
//!   Follower: 11100 (client) / 11101 (replication)  ← YOU launch this
//!   MinIO:    11010
//!   Proxy:    11200 → 11101
//!
//! Run with: cargo run --bin debug_follower_pressure_main

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, s3_cluster_config, write_event, MinioContainer, TcpProxy, TestServer,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
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
const THROTTLE_MS_PER_CHUNK: u64 = 200;

const LEADER_PORT: u16 = 11000;
const FOLLOWER_PORT: u16 = 11100;
const MINIO_PORT: u16 = 11010;
const PROXY_PORT: u16 = 11200;

const FOLLOWER_ADDRESS: &str = "127.0.0.1:11100";


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Debug Follower Pressure Test (follower launched externally) ===\n");

    // ========================================
    // Setup
    // ========================================
    // Wipe follower data from previous runs so the follower starts fresh.
    // The follower uses a fixed directory (not a tempdir) because it's
    // launched externally from VSCode.
    let follower_data = std::path::Path::new("data_debug_follower");
    if follower_data.exists() {
        println!("Cleaning stale follower data directory...");
        std::fs::remove_dir_all(follower_data)?;
    }

    println!("Starting MinIO on port {}...", MINIO_PORT);
    let minio = MinioContainer::start_with_bucket(MINIO_PORT, "test-pressure").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 2;

    let proxy = TcpProxy::start(PROXY_PORT, format!("127.0.0.1:{}", FOLLOWER_PORT + 1)).await?;
    println!("  Proxy {} → follower replication port {}", PROXY_PORT, FOLLOWER_PORT + 1);

    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.pending_replication_high_water_bytes = Some(262144);
    config.max_catchup_gap_bytes = Some(100_000_000);
    // 10-minute lease: plenty of time for debugging, prevents follower takeover
    config.heartbeat_lease_duration_ms = 600_000;
    config.internode_connection_timeout_ms = 60_000;

    println!("Starting leader on port {}...", LEADER_PORT);
    let _leader = TestServer::start_with_config_labeled(LEADER_PORT, config, "leader".into()).await?;
    println!("  Leader ready at {}", _leader.address());

    // ========================================
    // Wait for follower (launched externally)
    // ========================================
    println!("\n============================================================");
    println!("  WAITING FOR FOLLOWER on port {}", FOLLOWER_PORT);
    println!("  Launch 'Debug follower (pressure test)' from VSCode now.");
    println!("  Set breakpoints in the shard read path before launching.");
    println!("============================================================\n");

    let start = std::time::Instant::now();
    loop {
        match tokio::net::TcpStream::connect(FOLLOWER_ADDRESS).await {
            Ok(_) => {
                println!("  Follower detected on {} (waited {:.0}s)", FOLLOWER_ADDRESS, start.elapsed().as_secs_f64());
                break;
            }
            Err(_) => {
                if start.elapsed() > Duration::from_secs(600) {
                    return Err("Timed out waiting for follower (10 minutes)".into());
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    // Give the leader time to discover the follower via S3 membership polling.
    // Without this, the first probe write hits S3 fallback because the leader
    // hasn't connected to the follower yet.
    println!("Waiting 15s for leader to discover follower via S3 membership...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    let probe_shard0 = AggregateKey::new(1, 0, 999);

    // Now poll: write on leader, check if follower receives it via TCP.
    println!("Waiting for replication to be established (polling)...");
    let repl_start = std::time::Instant::now();
    let mut replication_ok = false;
    let mut probe_event = 1u64;
    while repl_start.elapsed() < Duration::from_secs(120) {
        // Write a probe event on the leader
        let mut lc = CeleriantClient::connect(_leader.address()).await?;
        if write_event(&mut lc, &probe_shard0, probe_event, probe_event == 1).await.is_err() {
            tokio::time::sleep(Duration::from_secs(2)).await;
            probe_event += 1;
            continue;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Check if follower has it
        if let Ok(mut fc) = CeleriantClient::connect(FOLLOWER_ADDRESS).await {
            if let Ok(c) = count_events(&mut fc, &probe_shard0).await {
                if c >= probe_event as usize {
                    println!(
                        "  Replication confirmed: follower has {} events ({:.0}s)",
                        c, repl_start.elapsed().as_secs_f64()
                    );
                    replication_ok = true;
                    break;
                }
                println!(
                    "  Follower has {}/{} events, waiting... ({:.0}s)",
                    c, probe_event, repl_start.elapsed().as_secs_f64()
                );
            }
        }
        probe_event += 1;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(replication_ok, "Replication was not established within 120s");

    // ========================================
    // Phase 1: Verify normal TCP replication
    // ========================================
    println!("\nPHASE 1: Normal TCP replication");
    println!("-------------------------------");

    let mut leader_client = CeleriantClient::connect(_leader.address()).await?;
    // Write a few more events to confirm steady-state replication
    let base = probe_event;
    println!("  Writing 3 more events to shard 0 probe key...");
    for i in 1..=3 {
        write_event(&mut leader_client, &probe_shard0, base + i, false).await?;
    }

    let mut follower_client = CeleriantClient::connect(FOLLOWER_ADDRESS).await?;
    let fc = count_events(&mut follower_client, &probe_shard0).await?;
    let expected = (base + 3) as usize;
    println!("  Follower shard 0: {} events (expected {})", fc, expected);
    assert_eq!(fc, expected, "Follower should have all probe events on shard 0");
    println!("  TCP replication confirmed\n");

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
    // Phase 3: Unthrottle + wait for recovery + follower catchup
    // ========================================
    println!("\nPHASE 3: Unthrottle + wait for recovery + follower catchup");
    println!("----------------------------------------------------------");

    proxy.unthrottle();
    println!("  Proxy UNTHROTTLED");
    println!("  Waiting for replication pipeline to drain and kick to deliver...");
    tokio::time::sleep(Duration::from_secs(30)).await;

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

    // Poll until follower catches up
    let timeout = Duration::from_secs(120);
    let start = std::time::Instant::now();
    let mut caught_up = false;

    while start.elapsed() < timeout {
        let mut fc = match CeleriantClient::connect_with_timeout(FOLLOWER_ADDRESS, Some(Duration::from_secs(10)), None).await {
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
            follower_total, total_leader, start.elapsed().as_secs_f64()
        );
        if all_match {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    if caught_up {
        println!("  Follower caught up!\n");
    } else {
        println!("  Follower did NOT catch up within {}s — check debugger for panic", timeout.as_secs());
    }

    println!("\n=== Test Complete (check debugger if follower panicked) ===\n");
    println!("Press Ctrl+C to stop the orchestrator and clean up.");

    // Keep orchestrator alive so MinIO/leader/proxy stay running for debugging
    tokio::signal::ctrl_c().await?;
    Ok(())
}

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
                compression_type_id: 0,
                compression_level: None,
            },
        );

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: id as u128,
            user_id: None,
            writes,
        });

        match client.send_request(&request, CompressionType::None).await {
            Ok(_) => count += 1,
            Err(_) => break,
        }
    }

    counter.fetch_add(count, Ordering::Relaxed);
}
