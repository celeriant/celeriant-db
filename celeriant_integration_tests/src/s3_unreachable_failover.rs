//! S3 Unreachable Failover Integration Test
//!
//! Tests the "S3 unreachable + network partition" failure mode. Both nodes are alive
//! but partitioned, and S3 is also down — the hardest failure case.
//! Absorbs s3_writes_during_fencing (both-nodes-reject assertion).
//!
//! Scenario:
//! 1. Start two-node cluster with TcpProxy, verify replication
//! 2. Pause MinIO (prevent S3 lease renewal/race)
//! 3. Block proxy (simulate network partition)
//! 4. Wait for both nodes to fence
//! 5. Verify BOTH nodes reject writes while fenced (invariant 3)
//! 6. Verify write rejection is consistent across multiple attempts
//! 7. Unpause MinIO + unblock proxy
//! 8. Wait for S3 race to resolve
//! 9. Verify exactly one leader emerges
//!
//! Invariants tested: 1 (single leader), 3 (write gating), 4 (asymmetric fencing)

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{poll_converged_count, write_event, MinioContainer, ServerConfig, TestServer, TcpProxy, FOLLOWER_CONVERGENCE_TIMEOUT};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Unreachable Failover Integration Test ===\n");

    let port_base = 12700 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let proxy_port = port_base + 200;
    let minio_port = port_base + 10;

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start cluster with TcpProxy, verify healthy
    // ========================================
    println!("PHASE 1: Start cluster with TcpProxy");
    println!("-------------------------------------");

    println!("  Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-s3-unreachable").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("  MinIO ready at {}", minio_endpoint);

    let leader_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: leader_port,
        routing_rule: RoutingRule::AggregateTypeId,
        s3_lease_duration_ms: 10_000,
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
    let _leader = TestServer::start_with_config_labeled(leader_port, leader_config, "leader".into()).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_repl_port = follower_port + 1;
    println!("  Starting TcpProxy: {} -> {}", proxy_port, follower_repl_port);
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_repl_port)).await?;

    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: follower_port,
        advertised_replication_address: Some(format!("127.0.0.1:{}", proxy_port)),
        routing_rule: RoutingRule::AggregateTypeId,
        s3_lease_duration_ms: 10_000,
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket_name),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
        s3_endpoint_override: Some(minio_endpoint),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!("  Starting follower on port {} (advertised repl: proxy {})",
        follower_port, proxy_port);
    let _follower = TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into()).await?;

    println!("  Waiting for election, heartbeat, and S3 lease expiry...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut leader_client = CeleriantClient::connect(&format!("127.0.0.1:{}", leader_port)).await?;
    let mut follower_client = CeleriantClient::connect(&format!("127.0.0.1:{}", follower_port)).await?;

    println!("  Writing events 1-3 to verify cluster health...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let follower_count =
        poll_converged_count(&mut follower_client, &aggregate_key, 3, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(follower_count, 3, "Follower should have 3 events");
    println!("  Cluster healthy: follower has {} events\n", follower_count);

    // ========================================
    // PHASE 2: Pause MinIO, then block proxy
    // ========================================
    println!("PHASE 2: Pause MinIO + block proxy (dual failure)");
    println!("--------------------------------------------------");

    let initial_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let initial_lease = deserialise_lease(&initial_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    let initial_lease_epoch = initial_lease.lease_epoch;
    println!("  Initial lease_epoch={}", initial_lease_epoch);

    // Pause MinIO FIRST to prevent S3 lease renewal on first heartbeat miss
    println!("  Pausing MinIO (S3 unreachable)...");
    minio.pause()?;

    proxy.block();
    println!("  Proxy blocked - both replication paths down\n");

    // ========================================
    // PHASE 3: Verify BOTH nodes reject writes while fenced
    // ========================================
    println!("PHASE 3: Verify BOTH nodes reject writes while fenced");
    println!("------------------------------------------------------");

    println!("  Waiting for heartbeat timeout + fencing...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let fenced_key_a = AggregateKey::new(2, 1, 1);
    let fenced_key_b = AggregateKey::new(3, 1, 1);

    let leader_accepts = write_event(&mut leader_client, &fenced_key_a, 1, true).await.is_ok();
    let follower_accepts = write_event(&mut follower_client, &fenced_key_b, 1, true).await.is_ok();

    assert!(!leader_accepts, "Leader accepted writes while Fenced");
    assert!(!follower_accepts, "Follower accepted writes while Fenced");
    println!("  Both nodes correctly reject writes while fenced");

    // Verify consistency across multiple attempts
    for i in 1u64..=3 {
        let retry_key = AggregateKey::new(4, 1, i as u128);
        let a_ok = write_event(&mut leader_client, &retry_key, i, true).await.is_ok();
        let b_ok = write_event(&mut follower_client, &retry_key, i, true).await.is_ok();
        assert!(!a_ok, "Leader accepted write attempt {} while Fenced", i);
        assert!(!b_ok, "Follower accepted write attempt {} while Fenced", i);
    }
    println!("  Write rejection consistent across 3 additional attempts\n");

    // ========================================
    // PHASE 4: Restore S3 + network, verify recovery
    // ========================================
    println!("PHASE 4: Unpause MinIO + unblock proxy");
    println!("----------------------------------------");

    minio.unpause()?;
    println!("  MinIO unpaused");
    proxy.unblock();
    println!("  Proxy unblocked");

    println!("  Waiting for S3 race + reconvergence + S3 lease expiry...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    // ========================================
    // PHASE 5: Verify exactly one leader emerges
    // ========================================
    println!("\nPHASE 5: Verify exactly one leader after recovery");
    println!("--------------------------------------------------");

    let post_key_a = AggregateKey::new(5, 1, 1);
    let post_key_b = AggregateKey::new(6, 1, 1);

    let leader_accepts_after = write_event(&mut leader_client, &post_key_a, 1, true).await.is_ok();
    let follower_accepts_after = write_event(&mut follower_client, &post_key_b, 1, true).await.is_ok();

    println!("  Leader accepts writes after recovery: {}", leader_accepts_after);
    println!("  Follower accepts writes after recovery: {}", follower_accepts_after);

    assert_ne!(
        leader_accepts_after, follower_accepts_after,
        "Both nodes have same write acceptance state. Expected exactly one leader."
    );

    let final_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let final_lease = deserialise_lease(&final_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    assert!(
        final_lease.lease_epoch >= initial_lease_epoch,
        "lease_epoch should not regress: was {}, now {}",
        initial_lease_epoch, final_lease.lease_epoch
    );
    println!("  lease_epoch {} -> {} (monotonic)", initial_lease_epoch, final_lease.lease_epoch);

    println!("\n=== All Tests Passed ===");
    println!("S3 unreachable failover validated:");
    println!("  1. Both nodes correctly rejected writes while Fenced (S3 + TCP down)");
    println!("  2. Write rejection consistent across multiple attempts");
    println!("  3. After recovery: exactly one leader emerged");
    println!("  4. lease_epoch did not regress (bumps only if a different node won the race)\n");

    Ok(())
}
