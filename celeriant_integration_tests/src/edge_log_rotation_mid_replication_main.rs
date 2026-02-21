//! Edge Case: Log Rotation While Replication Batch Mid-Flight
//!
//! Regression guard for the risk where the leader rotates to a new log file
//! while constructing a replication batch that spans the rotation boundary.
//! The old log file may be evicted from the LRU cache before the batch is
//! fully collected.
//!
//! Scenario:
//! 1. Start 4-shard cluster with minimum preallocate (1.5MB, ~512KB usable per
//!    file after dual headers) and a throttled TcpProxy.
//! 2. Throttle proxy — writes will outpace replication, creating replication
//!    batches that may span log rotation boundaries.
//! 3. Write 200 large events (32KB each) round-robin across 4 aggregates on
//!    different shards. Each shard gets 50 events × 32KB ≈ 1.6MB, forcing ~3
//!    rotations per shard.
//! 4. Unthrottle and wait for convergence (45s).
//! 5. Verify: all events replicated correctly despite log rotation.
//!
//! This is test #7 in the integration test coverage report.
//!
//! Run with: cargo run --bin edge_log_rotation_mid_replication_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    count_events, s3_cluster_config, write_event, write_large_event, MinioContainer, TcpProxy,
    TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: Log Rotation While Replication Batch Mid-Flight ===\n");

    let port_base = 15100 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;
    let proxy_port = port_base + 200;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-log-rotation-replication").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    // 4 shards, one aggregate per shard for write distribution.
    // Routing: AggregateTypeId, so aggregate_type_id % num_shards determines shard.
    let num_shards = 4;
    let keys = [
        AggregateKey::new(1, 1, 1), // shard = 1 % 4 = 1
        AggregateKey::new(2, 1, 1), // shard = 2 % 4 = 2
        AggregateKey::new(3, 1, 1), // shard = 3 % 4 = 3
        AggregateKey::new(4, 1, 1), // shard = 4 % 4 = 0
    ];

    // Proxy forwards to follower replication port (follower_port + 1).
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_port + 1)).await?;
    println!("  Proxy {} -> follower replication port {}", proxy_port, follower_port + 1);

    let base_config = s3_cluster_config(
        num_shards,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );

    let config = celeriant_integration_tests::ServerConfig {
        // Minimum valid preallocate (3 × 512KB headers). Usable space per file ≈ 512KB.
        // 50 events/shard × 32KB = 1.6MB/shard → ~3 rotations per shard.
        shard_log_preallocate_bytes: 3 * 512 * 1024,
        // Prevent failover during test; we want to stress rotation, not leadership change.
        heartbeat_lease_duration_ms: 30_000,
        ..base_config
    };

    let mut follower_config = config.clone();
    follower_config.advertised_replication_address = Some(proxy.address());

    println!("Starting two-node cluster (4 shards, 1.5MB preallocate)...");
    let leader =
        TestServer::start_with_config_labeled(leader_port, config, "leader".into()).await?;
    let follower =
        TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into())
            .await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    // Extra wait: proxy adds latency to initial replication connection.
    println!("Waiting for cluster stabilization (10s)...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // ========================================
    // Phase 1: Create aggregates (1 event each, allow_create = true)
    // ========================================
    println!("\nPHASE 1: Creating 4 aggregates (one per shard)");
    println!("-----------------------------------------------");

    for key in &keys {
        write_event(&mut leader_client, key, 1, true).await?;
    }
    println!("  4 aggregates created");

    // ========================================
    // Phase 2: Throttle proxy, rapid writes to force rotation mid-replication
    // ========================================
    println!("\nPHASE 2: Throttle proxy and write large events across all 4 shards");
    println!("---------------------------------------------------------------------");

    // 100ms per 8KB chunk: replication ~25x slower than writes.
    // This ensures the replication batch collector is working on old data while
    // the leader rotates to a new log file.
    proxy.throttle(100);

    let events_per_key = 200u64;
    let mut leader_counts = [1usize; 4];

    for i in 2u64..=events_per_key + 1 {
        let key_idx = ((i - 2) % 4) as usize;
        let key = &keys[key_idx];
        write_large_event(&mut leader_client, key, i, 32768)
            .await
            .map_err(|e| format!("ERROR: write {} to key {} failed: {}", i, key_idx, e))?;
        leader_counts[key_idx] += 1;

        if i % 50 == 0 {
            println!("  {} events written (distributed across 4 shards)...", i - 1);
        }
    }

    println!("  Write phase complete:");
    for (idx, key) in keys.iter().enumerate() {
        println!("    key[{}] (agg_type={}): {} events", idx, key.aggregate_type_id, leader_counts[idx]);
    }

    // ========================================
    // Phase 3: Unthrottle, wait for replication convergence
    // ========================================
    println!("\nPHASE 3: Unthrottle proxy, wait for replication convergence (45s)");
    println!("-------------------------------------------------------------------");

    proxy.unthrottle();

    // Generous timeout: throttled backlog of ~6.4MB needs to drain.
    let timeout = Duration::from_secs(45);
    let start = std::time::Instant::now();
    let mut caught_up = false;

    while start.elapsed() < timeout {
        if let Ok(mut fc) = CeleriantClient::connect(follower.address()).await {
            let mut all_match = true;
            for (idx, key) in keys.iter().enumerate() {
                let follower_count = count_events(&mut fc, key).await.unwrap_or(0);
                if follower_count < leader_counts[idx] {
                    all_match = false;
                    break;
                }
            }
            if all_match {
                caught_up = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // ========================================
    // Phase 4: Verify all event counts match
    // ========================================
    println!("\nPHASE 4: Verify event counts on follower");
    println!("-----------------------------------------");

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let mut all_pass = true;

    for (idx, key) in keys.iter().enumerate() {
        let follower_count = count_events(&mut follower_client, key).await?;
        let expected = leader_counts[idx];
        println!(
            "  key[{}] (agg_type={}): follower={} / leader={}  {}",
            idx,
            key.aggregate_type_id,
            follower_count,
            expected,
            if follower_count == expected { "OK" } else { "FAIL" }
        );
        if follower_count != expected {
            all_pass = false;
        }
    }

    assert!(
        caught_up && all_pass,
        "Follower did not replicate all events within {}s despite log rotation",
        timeout.as_secs()
    );

    println!("\n=== PASS ===\n");

    Ok(())
}
