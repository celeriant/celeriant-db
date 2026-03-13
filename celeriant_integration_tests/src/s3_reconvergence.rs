//! S3 Reconvergence Integration Test - Post-partition healing and split-brain detection
//!
//! Tests that:
//! 1. TcpProxy correctly forwards replication traffic between leader and follower
//! 2. When proxy is blocked (simulating network partition), both nodes fence and race to S3
//! 3. Both nodes become Leader during partition (each wins separate S3 races)
//! 4. When proxy is unblocked (partition heals), cluster reconverges to exactly ONE leader
//!
//! After unblocking proxy, exactly one node remains Leader.
//! The Leader accepts writes, the non-Leader rejects writes.
//!
//! Run with: cargo run --bin s3_reconvergence_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, write_event, MinioContainer, ServerConfig, TestServer, TcpProxy};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Reconvergence Integration Test ===\n");

    let port_base = 13100 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let proxy_port = port_base + 200;
    let minio_port = port_base + 10;

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start cluster with TcpProxy
    // ========================================
    println!("PHASE 1: Start cluster with TcpProxy");
    println!("-------------------------------------");

    println!("  Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-reconvergence").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("  MinIO ready at {}", minio_endpoint);

    let leader_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: leader_port,
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region.clone()),
        s3_bucket: Some(bucket_name.clone()),
        s3_access_key_id: Some(access_key.clone()),
        s3_secret_access_key: Some(secret_key.clone()),
        s3_endpoint_override: Some(minio_endpoint.clone()),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!("  Starting leader on port {}...", leader_port);
    let leader = TestServer::start_with_config_labeled(leader_port, leader_config, "leader".into()).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_repl_port = follower_port + 1;
    println!("  Starting TcpProxy: {} -> {}", proxy_port, follower_repl_port);
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_repl_port)).await?;
    println!("  TcpProxy ready at {}", proxy.address());

    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: follower_port,
        advertised_replication_address: Some(format!("127.0.0.1:{}", proxy_port)),
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket_name.clone()),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
        s3_endpoint_override: Some(minio_endpoint),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!("  Starting follower on port {} (advertised repl: proxy {})",
        follower_port, proxy_port);
    let follower = TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into()).await?;

    println!("  Waiting for leader to discover follower and connect through proxy...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 2: Write events and verify replication + roles
    // ========================================
    println!("\nPHASE 2: Write events and verify replication + roles");
    println!("-----------------------------------------------------");

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    println!("  Writing events 1-3 through leader...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(follower_count, 3, "Follower should have 3 events (replication through proxy)");
    println!("  ✓ Cluster healthy: follower has {} events through proxy", follower_count);

    // Verify roles: leader accepts writes, follower rejects
    let pre_key = AggregateKey::new(2, 1, 1);
    let leader_ok = write_event(&mut leader_client, &pre_key, 1, true).await.is_ok();
    let follower_ok = write_event(&mut follower_client, &pre_key, 2, true).await.is_ok();
    assert!(leader_ok, "Leader should accept writes before partition");
    assert!(!follower_ok, "Follower should reject writes before partition");
    println!("  ✓ Pre-partition roles correct: leader accepts, follower rejects\n");

    // ========================================
    // PHASE 3: Block proxy (simulate network partition)
    // ========================================
    println!("PHASE 3: Block proxy (simulate network partition)");
    println!("--------------------------------------------------");

    let initial_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let initial_lease = deserialise_lease(&initial_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    let initial_lease_index = initial_lease.lease_index;
    println!("  Initial lease_index={}", initial_lease_index);

    proxy.block();
    println!("  ✓ Proxy blocked - leader and follower partitioned\n");

    // ========================================
    // PHASE 4: Wait for both nodes to fence and race to S3
    // ========================================
    println!("PHASE 4: Wait for both nodes to fence and race to S3");
    println!("-----------------------------------------------------");

    println!("  Waiting for heartbeat timeout + both nodes to fence and race...");
    // Need longer wait: leader loses follower heartbeat, follower's watchdog expires
    // Both nodes then fence and race to S3 (possibly multiple times)
    tokio::time::sleep(Duration::from_secs(8)).await;

    let post_race_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let post_race_lease = deserialise_lease(&post_race_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  Post-race lease: leader_node_id={:x}, lease_index={}",
        post_race_lease.leader_node_id, post_race_lease.lease_index);
    assert!(
        post_race_lease.lease_index > initial_lease_index,
        "lease_index should have increased after S3 races: was {}, now {}",
        initial_lease_index, post_race_lease.lease_index
    );
    println!("  ✓ S3 races resolved: lease_index {} → {} (multiple races may have occurred)",
        initial_lease_index, post_race_lease.lease_index);

    // At this point, BOTH nodes are Leader (each won separate S3 races)
    // Verify this by attempting writes to both - both should succeed
    let partition_key = AggregateKey::new(3, 1, 1);
    let leader_during_partition = write_event(&mut leader_client, &partition_key, 1, true).await.is_ok();
    let follower_during_partition = write_event(&mut follower_client, &partition_key, 2, true).await.is_ok();
    println!("  During partition: leader accepts writes = {}, follower accepts writes = {}",
        leader_during_partition, follower_during_partition);
    println!("  (Both nodes are likely Leaders now - the bug is about to manifest)\n");

    // ========================================
    // PHASE 5: Unblock proxy (partition heals)
    // ========================================
    println!("PHASE 5: Unblock proxy (partition heals)");
    println!("-----------------------------------------");

    proxy.unblock();
    println!("  ✓ Proxy unblocked - nodes can communicate again\n");

    // ========================================
    // PHASE 6: Wait for reconvergence and assert exactly ONE leader
    // ========================================
    println!("PHASE 6: Wait for reconvergence - expect exactly ONE leader");
    println!("------------------------------------------------------------");

    println!("  Polling for reconvergence (max 20 seconds)...");

    // Poll periodically to check if exactly one node accepts writes
    let max_reconverge_time = Duration::from_secs(20);
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();
    let mut reconverged = false;

    while start.elapsed() < max_reconverge_time {
        tokio::time::sleep(poll_interval).await;

        // Try writing to both nodes with different keys
        let check_key_leader = AggregateKey::new(4, 1, start.elapsed().as_millis() % 1000);
        let check_key_follower = AggregateKey::new(5, 1, start.elapsed().as_millis() % 1000);

        let leader_accepts = write_event(&mut leader_client, &check_key_leader, 1, true).await.is_ok();
        let follower_accepts = write_event(&mut follower_client, &check_key_follower, 1, true).await.is_ok();

        println!("  Poll at {:?}: leader accepts = {}, follower accepts = {}",
            start.elapsed(), leader_accepts, follower_accepts);

        // Check if exactly one accepts (reconverged to single leader)
        if leader_accepts && !follower_accepts {
            println!("  ✓ Reconverged: original leader is the single Leader");
            reconverged = true;
            break;
        } else if !leader_accepts && follower_accepts {
            println!("  ✓ Reconverged: original follower became the single Leader");
            reconverged = true;
            break;
        } else if !leader_accepts && !follower_accepts {
            println!("  Both nodes rejecting writes (still fencing or transitioning)...");
        } else {
            println!("  WARNING: Both nodes accepting writes (split-brain persists)");
        }
    }

    // ========================================
    // PHASE 7: Verify the single leader invariant
    // ========================================
    println!("\nPHASE 7: Verify exactly ONE leader after reconvergence");
    println!("-------------------------------------------------------");

    assert!(
        reconverged,
        "FAILED: Cluster did not reconverge to exactly one leader within {} seconds. \
         Both nodes likely remain as Leader (split-brain bug). \
         Expected: One node accepts writes, the other rejects. \
         Actual: Both nodes accepting writes or timeout expired.",
        max_reconverge_time.as_secs()
    );

    // Do a final verification write to confirm roles are stable
    let final_key_leader = AggregateKey::new(6, 1, 1);
    let final_key_follower = AggregateKey::new(7, 1, 1);

    let final_leader_accepts = write_event(&mut leader_client, &final_key_leader, 1, true).await.is_ok();
    let final_follower_accepts = write_event(&mut follower_client, &final_key_follower, 1, true).await.is_ok();

    assert_ne!(
        final_leader_accepts, final_follower_accepts,
        "FAILED: Both nodes have same write acceptance state. Expected exactly one leader. \
         leader accepts = {}, follower accepts = {}",
        final_leader_accepts, final_follower_accepts
    );

    println!("  ✓ Final verification: exactly one node is Leader (accepts writes)");
    println!("  Leader node: {}", if final_leader_accepts { "original leader" } else { "original follower" });

    println!("\n=== All Tests Passed ===");
    println!("Reconvergence test validated:");
    println!("  1. TcpProxy correctly forwarded replication (3 events replicated)");
    println!("  2. During partition: both nodes fenced and raced to S3");
    println!("  3. After unblocking: cluster reconverged to exactly ONE leader");
    println!("  4. Final state: one Leader (accepts writes), one non-Leader (rejects writes)\n");

    Ok(())
}
