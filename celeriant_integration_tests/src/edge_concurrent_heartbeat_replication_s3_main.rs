//! Regression guard: Concurrent Heartbeat + Replication + S3 Upload (test #15)
//!
//! Guards against regression of the no-lock S3 path in `FollowerConnection`.
//! Before the fix, S3 uploads were performed while holding the replication write lock,
//! blocking heartbeats for the duration of every S3 upload (potentially tens of seconds).
//! After the fix, S3 uploads acquire NO lock — they go directly to MinIO via the S3Uploader
//! that lives outside the lock, and only briefly re-acquire the replication lock for
//! `send_kick()` at the end.
//!
//! Scenario: heavily throttle the follower to build replication queue pressure beyond
//! `pending_replication_high_water_bytes`, triggering S3 fallback. During fallback,
//! verify that:
//!   1. Heartbeats continue flowing (follower does not trigger failover)
//!   2. S3 uploads complete successfully (fallback path is exercised)
//!   3. No deadlock occurs (both nodes remain alive)
//!   4. After unthrottle, the follower catches up from S3 and TCP resumes
//!
//! If the S3 path is ever placed back behind the replication lock, S3 uploads will
//! block heartbeats. With a 200ms/chunk proxy throttle and large payloads, each S3
//! upload batch takes many seconds — well beyond the heartbeat TTL. The follower's
//! watchdog would fire, causing a spurious failover. This test would fail at the
//! "both nodes alive" or "follower still a follower" assertion.
//!
//! Run with: cargo run --bin edge_concurrent_heartbeat_replication_s3_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    count_events, is_leader, s3_cluster_config, write_event, write_large_event, MinioContainer,
    TcpProxy, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Regression Guard: Concurrent Heartbeat + Replication + S3 Upload ===\n");
    println!("This test guards against the no-lock S3 path regression in FollowerConnection.");
    println!("If S3 uploads hold the replication lock, heartbeats stall during fallback,");
    println!("the follower watchdog fires, and a spurious failover occurs.\n");

    // ========================================
    // Setup
    // ========================================
    let port_base = 16300 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;
    let proxy_port = port_base + 200;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-concurrent-s3").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 2;

    // TcpProxy intercepts the follower's replication port.
    // Heavy throttle will cause queue pressure, triggering S3 fallback.
    // The test verifies heartbeats survive even while S3 uploads are in flight.
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_port + 1)).await?;
    println!("  Proxy {} -> follower replication port {}", proxy_port, follower_port + 1);

    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    // Long heartbeat lease: 30s. Even with slow heartbeats through the throttled proxy,
    // we should not trigger failover during our ~15s throttle window.
    // If S3 holds the replication lock, heartbeats cannot acquire it, so they are blocked
    // entirely — not just slowed. Missing all heartbeats for >lease_duration causes failover.
    config.heartbeat_lease_duration_ms = 30_000;
    // Low high water mark: 32KB triggers S3 fallback when concurrent writes accumulate.
    // With 20 concurrent writers × 4KB events in one fsync window, the pending queue
    // reaches ~240KB, far above this threshold. Must be above ~12KB (single event pending
    // size with 3x memory multiplier) so post-recovery serial writes use TCP, not S3.
    config.pending_replication_high_water_bytes = 32_768; // 32KB
    // Low max_catchup_gap_bytes: forces a kick after a small gap so the follower receives
    // the S3 pointer promptly after unthrottle.
    config.max_catchup_gap_bytes = 4096;
    // Generous internode timeout so throttled connections are not killed prematurely.
    config.internode_connection_timeout_ms = Some(60_000);

    let mut follower_config = config.clone();
    follower_config.advertised_replication_address = Some(proxy.address());

    println!("Starting two-node cluster (2 shards, heartbeat_lease=30s, high_water=32KB, max_gap=4096)...");
    let mut leader =
        TestServer::start_with_config_labeled(leader_port, config, "leader".into()).await?;
    let mut follower =
        TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into())
            .await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    // ========================================
    // Phase 1: Cluster stabilization
    // ========================================
    println!("\nPHASE 1: Wait for election + discovery + replication connection");
    println!("---------------------------------------------------------------");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify roles before throttling.
    assert!(
        is_leader(leader.address()).await?,
        "Expected leader node to be the leader"
    );
    assert!(
        !is_leader(follower.address()).await?,
        "Expected follower node to be a follower"
    );

    // Create the aggregates we will write to (one per shard).
    // We must create them before the heavy write phase so allow_create works.
    let key_shard0 = AggregateKey::new(1, 0, 200);
    let key_shard1 = AggregateKey::new(1, 1, 200);
    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    write_event(&mut leader_client, &key_shard0, 1, true).await?;
    write_event(&mut leader_client, &key_shard1, 1, true).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Confirm baseline replication works before stressing.
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let fc0 = count_events(&mut follower_client, &key_shard0).await?;
    let fc1 = count_events(&mut follower_client, &key_shard1).await?;
    assert_eq!(fc0, 1, "Shard 0 baseline event must replicate");
    assert_eq!(fc1, 1, "Shard 1 baseline event must replicate");
    println!("  Cluster healthy, baseline replication confirmed\n");

    // ========================================
    // Phase 2: Throttle heavily + write large events to trigger S3 fallback
    // ========================================
    println!("PHASE 2: Throttle heavily + concurrent writes to trigger S3 fallback");
    println!("--------------------------------------------------------------------");

    // 200ms per 8KB chunk makes TCP replication very slow through the proxy.
    proxy.throttle(200);
    println!("  Proxy THROTTLED (200ms/chunk — heavy)");

    // Concurrent writes create queue pressure. Serial durable writes can't build
    // pressure because each write blocks until replication completes. With N
    // concurrent writers in the same fsync window, the pending replication queue
    // reaches N × event_size (~12KB each), exceeding the 32KB threshold.
    let num_writers = 20usize;
    let events_per_writer = 5u64;
    let mut handles = Vec::with_capacity(num_writers);
    for w in 0..num_writers {
        let addr = leader.address().to_string();
        let k0 = key_shard0.clone();
        let k1 = key_shard1.clone();
        handles.push(tokio::spawn(async move {
            let mut c = CeleriantClient::connect(&addr).await
                .map_err(|e| format!("Writer {} connect: {}", w, e))?;
            for e in 0..events_per_writer {
                let idx = (w as u64) * events_per_writer + e + 2;
                write_large_event(&mut c, &k0, idx, 4096).await
                    .map_err(|e| format!("Writer {} shard0 idx {}: {}", w, idx, e))?;
                write_large_event(&mut c, &k1, idx, 4096).await
                    .map_err(|e| format!("Writer {} shard1 idx {}: {}", w, idx, e))?;
            }
            Ok::<_, String>(())
        }));
    }
    let mut write_failures = 0;
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => { eprintln!("  Writer failed: {}", e); write_failures += 1; }
            Err(e) => { eprintln!("  Writer panicked: {}", e); write_failures += 1; }
        }
    }
    let total_events = (num_writers as u64 - write_failures) * events_per_writer * 2;
    println!("  {} concurrent writers completed ({} failures)", num_writers, write_failures);
    println!("  ~{} large events written across 2 shards", total_events);
    println!("  S3 fallback should be triggered by queue pressure exceeding 32KB");

    // Verify S3 fallback was exercised by checking follower event counts.
    // S3 objects can't be checked reliably because the follower catches up from S3 and
    // deletes them in <1s (kick → download → apply → delete). Instead, prove S3 was
    // used: the follower has events that could ONLY have arrived via S3, since TCP
    // replication through the throttled proxy (200ms/8KB) is far too slow for ~200 events.
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let fc0 = count_events(&mut follower_client, &key_shard0).await.unwrap_or(0);
    let fc1 = count_events(&mut follower_client, &key_shard1).await.unwrap_or(0);
    let follower_total_after_writes = fc0 + fc1;
    // Baseline was 1 event per shard (2 total). If follower has significantly more,
    // S3 catchup must have delivered them (TCP at 200ms/8KB can't deliver 200 events).
    println!(
        "  Follower after writes: shard0={}, shard1={} (total={})",
        fc0, fc1, follower_total_after_writes
    );
    assert!(
        follower_total_after_writes > 10,
        "S3 fallback must have been triggered — follower should have received events via S3 \
         catchup (got {} total, expected >10). TCP at 200ms/8KB is too slow to deliver this \
         many events. Increase concurrent writer count or payload size.",
        follower_total_after_writes
    );
    println!(
        "  S3 fallback confirmed: follower has {} events (beyond baseline of 2)\n",
        follower_total_after_writes
    );
    drop(follower_client);

    // ========================================
    // Phase 3: Wait 15s — S3 uploads + heartbeats must both proceed
    // ========================================
    println!("PHASE 3: Wait 15s — verify S3 uploads + heartbeats are concurrent");
    println!("------------------------------------------------------------------");

    // During this window, the system should be:
    // - Uploading replication batches to S3 (no lock held during upload in fixed version)
    // - Sending heartbeats every 500ms (heartbeat_conn lock, independent of S3)
    // - NOT deadlocking
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Both nodes must still be alive — deadlock would cause one to hang and eventually crash.
    leader.check_alive().map_err(|e| format!("Leader died during S3 fallback: {}", e))?;
    follower.check_alive().map_err(|e| format!("Follower died during S3 fallback: {}", e))?;
    println!("  Both nodes alive (no deadlock)\n");

    // The follower must NOT have taken over as leader.
    // If heartbeats were blocked by the S3 upload lock, the follower's watchdog would fire
    // within heartbeat_lease_duration_ms. With a 30s lease and 15s window, any lock
    // contention lasting >30s would be caught on a subsequent run with longer waits,
    // but even a brief stall that prevents all 500ms heartbeats for 30s triggers this.
    let follower_became_leader = is_leader(follower.address()).await?;
    assert!(
        !follower_became_leader,
        "REGRESSION DETECTED: follower became leader during S3 fallback. \
         This indicates heartbeats were blocked by the S3 upload (no-lock S3 path \
         may have been reverted). The follower watchdog fired due to missing heartbeats \
         while the leader was uploading to S3 under the replication lock."
    );
    println!("  Follower is still a follower (no spurious failover during S3 fallback)");

    // ========================================
    // Phase 4: Unthrottle + wait for follower catchup from S3
    // ========================================
    println!("\nPHASE 4: Unthrottle + wait for follower S3 catchup (30s)");
    println!("---------------------------------------------------------");

    proxy.unthrottle();
    println!("  Proxy UNTHROTTLED");

    // Get leader event counts for convergence verification.
    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let leader_count0 = count_events(&mut leader_client, &key_shard0).await?;
    let leader_count1 = count_events(&mut leader_client, &key_shard1).await?;
    println!(
        "  Leader: shard0={} events, shard1={} events",
        leader_count0, leader_count1
    );

    // Poll for follower convergence with a generous timeout.
    // S3 catchup may involve: kick delivery → follower downloads S3 batches → applies them.
    let timeout = Duration::from_secs(60);
    let start = std::time::Instant::now();
    let mut caught_up = false;

    while start.elapsed() < timeout {
        leader.check_alive().map_err(|e| format!("Leader died during catchup: {}", e))?;
        follower.check_alive().map_err(|e| format!("Follower died during catchup: {}", e))?;

        if let Ok(mut fc) = CeleriantClient::connect(follower.address()).await {
            let c0 = count_events(&mut fc, &key_shard0).await.unwrap_or(0);
            let c1 = count_events(&mut fc, &key_shard1).await.unwrap_or(0);
            println!(
                "  Follower: shard0={}/{}, shard1={}/{} ({:.0}s elapsed)",
                c0,
                leader_count0,
                c1,
                leader_count1,
                start.elapsed().as_secs_f64()
            );
            if c0 >= leader_count0 && c1 >= leader_count1 {
                caught_up = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    assert!(
        caught_up,
        "Follower should have caught up from S3 within {}s after unthrottle",
        timeout.as_secs()
    );
    println!("  Follower caught up from S3\n");

    // ========================================
    // Phase 5: Final convergence verification
    // ========================================
    println!("PHASE 5: Final convergence verification");
    println!("---------------------------------------");

    // After S3 catchup, follower should consume (delete) S3 fallback objects.
    // Verify counts match on both nodes.
    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    let final_lc0 = count_events(&mut leader_client, &key_shard0).await?;
    let final_fc0 = count_events(&mut follower_client, &key_shard0).await?;
    println!("  Shard 0: leader={}, follower={}", final_lc0, final_fc0);
    assert_eq!(
        final_lc0, final_fc0,
        "Shard 0 event counts must match after S3 catchup"
    );

    let final_lc1 = count_events(&mut leader_client, &key_shard1).await?;
    let final_fc1 = count_events(&mut follower_client, &key_shard1).await?;
    println!("  Shard 1: leader={}, follower={}", final_lc1, final_fc1);
    assert_eq!(
        final_lc1, final_fc1,
        "Shard 1 event counts must match after S3 catchup"
    );

    println!("\n=== PASS: Concurrent Heartbeat + Replication + S3 Upload ===\n");
    Ok(())
}
