//! Invariant: Replication Convergence Test
//!
//! Tests Hypothesis 2: After replication settles, leader and follower have
//! IDENTICAL event counts per aggregate (no duplicates, no loss).
//!
//! Test phases:
//! 1. Sequential writes: 50 events each to 4 aggregate keys (one per shard)
//! 2. Concurrent writes: 100 connections, 10 events each to random shards
//! 3. Final convergence check: Verify leader_count == follower_count for all aggregates
//!
//! Key assertion: For every aggregate, `leader_count == follower_count` (strict equality)
//!
//! Follower visibility trails the leader's commit by design, so every follower
//! count polls to the leader's count within `FOLLOWER_CONVERGENCE_TIMEOUT`.
//! Equality after the poll stays strict.
//!
//! Run with: cargo run --bin invariant_replication_convergence_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, poll_converged_count, s3_cluster_config, write_event, MinioContainer,
    TestServer, FOLLOWER_CONVERGENCE_TIMEOUT,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::collections::HashMap;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Invariant: Replication Convergence Test ===\n");

    // ========================================
    // Setup
    // ========================================
    let port_base = 10900 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;
    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 30_000;
    config.s3_lease_duration_ms = 30_000;

    println!("Starting two-node cluster (4 shards, S3 election)...");
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("Waiting for election + replication connection...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    // ========================================
    // Phase 1: Sequential writes
    // ========================================
    println!("\nPHASE 1: Sequential writes (50 events × 4 shards)");
    println!("------------------------------------------------");

    // Create 4 aggregate keys, one per shard (using type_id % 4)
    let keys: Vec<AggregateKey> = (0..4)
        .map(|shard_id| AggregateKey::new(1, shard_id, 1))
        .collect();

    println!("  Writing 50 events to each of 4 aggregates (one per shard)...");
    for (shard_id, key) in keys.iter().enumerate() {
        for event_num in 1..=50 {
            write_event(&mut leader_client, key, event_num, event_num == 1).await?;
        }
        println!("    Shard {}: 50 events written", shard_id);
    }

    println!("  Verifying counts on leader and follower...");
    for (shard_id, key) in keys.iter().enumerate() {
        let leader_count = count_events(&mut leader_client, key).await?;
        let follower_count =
            poll_converged_count(&mut follower_client, key, leader_count, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
        println!("    Shard {}: leader={}, follower={}", shard_id, leader_count, follower_count);
        assert_eq!(
            leader_count, follower_count,
            "Shard {} counts must match: leader={}, follower={}",
            shard_id, leader_count, follower_count
        );
    }
    println!("  Phase 1: All counts match\n");

    // ========================================
    // Phase 2: Concurrent writes
    // ========================================
    println!("PHASE 2: Concurrent writes (100 connections, 10 events each)");
    println!("-----------------------------------------------------------");

    let num_connections = 100;
    let events_per_connection = 10;

    println!("  Establishing {} connections to leader...", num_connections);
    let mut connection_tasks = Vec::with_capacity(num_connections);
    let addr = leader.address().to_string();
    for conn_id in 0..num_connections {
        let addr = addr.clone();
        connection_tasks.push(tokio::spawn(async move {
            CeleriantClient::connect(&addr)
                .await
                .map(|c| (conn_id, c))
                .map_err(|e| format!("conn {}: {}", conn_id, e))
        }));
    }

    let mut clients = Vec::with_capacity(num_connections);
    let mut failed = 0usize;
    for task in connection_tasks {
        match task.await {
            Ok(Ok(pair)) => clients.push(pair),
            _ => failed += 1,
        }
    }
    println!("  {} connected, {} failed", clients.len(), failed);

    // Track successful writes per aggregate
    let mut expected_counts: HashMap<AggregateKey, usize> = HashMap::new();

    println!("  Writing {} events from each connection...", events_per_connection);
    let mut write_tasks = Vec::new();
    for (conn_id, mut client) in clients {
        write_tasks.push(tokio::spawn(async move {
            // Each connection writes to a unique aggregate to avoid contention
            // Distribute across shards using type_id % 4
            let shard_id = (conn_id % 4) as u128;
            let aggregate_id = (conn_id / 4) as u128 + 100; // Start at 100 to avoid overlap with Phase 1
            let key = AggregateKey::new(1, shard_id, aggregate_id);

            let mut success_count = 0;
            for event_num in 1..=events_per_connection {
                match write_event(&mut client, &key, event_num as u64, event_num == 1).await {
                    Ok(_) => success_count += 1,
                    Err(_) => break,
                }
            }
            (key, success_count)
        }));
    }

    println!("  Collecting results...");
    for task in write_tasks {
        if let Ok((key, count)) = task.await {
            *expected_counts.entry(key).or_insert(0) += count;
        }
    }

    let total_writes: usize = expected_counts.values().sum();
    println!("  Total successful writes: {} across {} aggregates", total_writes, expected_counts.len());

    println!("  Verifying counts for all {} aggregates...", expected_counts.len());
    let mut mismatches = 0;
    for (key, expected) in &expected_counts {
        let leader_count = count_events(&mut leader_client, key).await?;
        let follower_count =
            poll_converged_count(&mut follower_client, key, leader_count, FOLLOWER_CONVERGENCE_TIMEOUT).await?;

        if leader_count != follower_count {
            println!(
                "    MISMATCH: aggregate({},{},{}): leader={}, follower={}, expected={}",
                key.org_id, key.aggregate_type_id, key.aggregate_id,
                leader_count, follower_count, expected
            );
            mismatches += 1;
        }
    }

    if mismatches > 0 {
        return Err(format!("{} aggregates have mismatched counts", mismatches).into());
    }
    println!("  Phase 2: All {} aggregates have matching counts\n", expected_counts.len());

    // ========================================
    // Phase 3: Final convergence check
    // ========================================
    println!("PHASE 3: Final convergence check (subset verification)");
    println!("------------------------------------------------------");

    // Sample 20 aggregates from phase 2 for detailed verification
    let sample_size = 20.min(expected_counts.len());
    let sample_keys: Vec<&AggregateKey> = expected_counts.keys().take(sample_size).collect();

    println!("  Verifying {} sampled aggregates...", sample_size);
    for key in &sample_keys {
        let leader_count = count_events(&mut leader_client, key).await?;
        let follower_count =
            poll_converged_count(&mut follower_client, key, leader_count, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
        let expected = expected_counts.get(key).unwrap();

        println!(
            "    aggregate({},{},{}): leader={}, follower={}, expected={}",
            key.org_id, key.aggregate_type_id, key.aggregate_id,
            leader_count, follower_count, expected
        );

        assert_eq!(
            leader_count, follower_count,
            "Counts must converge: leader={}, follower={}",
            leader_count, follower_count
        );
        assert_eq!(
            leader_count, *expected,
            "Leader count must match expected: leader={}, expected={}",
            leader_count, expected
        );
    }

    println!("  Phase 3: All sampled aggregates converged\n");

    println!("=== All Tests Passed: Replication Convergence Verified ===\n");

    Ok(())
}
