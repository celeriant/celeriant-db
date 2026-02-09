//! S3 Writes During Fencing Integration Test
//!
//! Tests Invariant 9: "No writes while Fenced"
//!
//! Scenario:
//! 1. Start two-node cluster with TcpProxy
//! 2. Write some data and verify replication
//! 3. Create network partition (block proxy)
//! 4. Wait for both nodes to fence
//! 5. Send write requests to BOTH nodes - ALL should be rejected
//! 6. Unblock network
//! 7. Wait for one node to win S3 race
//! 8. Verify winner accepts writes, loser rejects writes
//!
//! Run with: cargo run --bin s3_writes_during_fencing_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{count_events, write_event, MinioContainer, ServerConfig, TestServer, TcpProxy};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Writes During Fencing Integration Test ===\n");

    let port_base = 13500 + (std::process::id() % 100) as u16;
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
    let minio = MinioContainer::start_with_bucket(minio_port, "test-writes-fencing").await?;
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
    let _leader = TestServer::start_with_config_labeled(leader_port, leader_config, "leader".into()).await?;

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
    let _follower = TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into()).await?;

    println!("  Waiting for leader to discover follower through proxy...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 2: Write events and verify replication
    // ========================================
    println!("\nPHASE 2: Write events and verify replication");
    println!("---------------------------------------------");

    let mut leader_client = CeleriantClient::connect(&format!("127.0.0.1:{}", leader_port)).await?;
    let mut follower_client = CeleriantClient::connect(&format!("127.0.0.1:{}", follower_port)).await?;

    println!("  Writing events 1-3 through leader...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    println!("  Waiting for replication through proxy...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(follower_count, 3, "Follower should have 3 events");
    println!("  ✓ Initial replication verified: follower has {} events\n", follower_count);

    // ========================================
    // PHASE 3: Block proxy (simulate network partition)
    // ========================================
    println!("PHASE 3: Block proxy (simulate network partition)");
    println!("--------------------------------------------------");

    let initial_lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let initial_lease = deserialise_lease(&initial_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    let initial_lease_index = initial_lease.lease_index;
    println!("  Initial lease_index={}", initial_lease_index);

    proxy.block();
    println!("  ✓ Proxy blocked - nodes partitioned");

    // Pause MinIO so nodes can't complete S3 race — they'll stay Fenced
    println!("  Pausing MinIO so nodes stay Fenced (can't re-elect)...");
    tokio::time::sleep(Duration::from_secs(1)).await;
    minio.pause()?;
    println!("  ✓ MinIO paused\n");

    // ========================================
    // PHASE 4: Wait for both nodes to fence
    // ========================================
    println!("PHASE 4: Wait for both nodes to fence");
    println!("--------------------------------------");

    println!("  Waiting for heartbeat timeout and fencing...");
    // With proxy blocked and MinIO paused, both nodes will detect heartbeat loss,
    // fence, attempt S3 race (which hangs), and remain Fenced.
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("  ✓ Both nodes should now be Fenced\n");

    // ========================================
    // PHASE 5: Verify BOTH nodes reject writes while Fenced
    // ========================================
    println!("PHASE 5: Verify BOTH nodes reject writes while Fenced");
    println!("------------------------------------------------------");

    println!("  Testing writes while fenced...");
    let fenced_key_leader = AggregateKey::new(2, 1, 1);
    let fenced_key_follower = AggregateKey::new(3, 1, 1);

    let leader_accepts_while_fenced = write_event(&mut leader_client, &fenced_key_leader, 1, true).await.is_ok();
    let follower_accepts_while_fenced = write_event(&mut follower_client, &fenced_key_follower, 1, true).await.is_ok();

    println!("  Leader accepts writes while fenced: {}", leader_accepts_while_fenced);
    println!("  Follower accepts writes while fenced: {}", follower_accepts_while_fenced);

    assert!(
        !leader_accepts_while_fenced,
        "FAILED: Leader accepted writes while Fenced (violates Invariant 9)"
    );
    assert!(
        !follower_accepts_while_fenced,
        "FAILED: Follower accepted writes while Fenced (violates Invariant 9)"
    );
    println!("  ✓ Both nodes correctly reject writes while Fenced\n");

    // Try multiple writes to ensure consistent rejection
    println!("  Testing write rejection consistency (3 attempts each)...");
    for i in 1u64..=3 {
        let retry_key = AggregateKey::new(4, 1, i as u128);
        let leader_accepts = write_event(&mut leader_client, &retry_key, i, true).await.is_ok();
        let follower_accepts = write_event(&mut follower_client, &retry_key, i, true).await.is_ok();

        assert!(!leader_accepts, "Leader accepted write attempt {} while Fenced", i);
        assert!(!follower_accepts, "Follower accepted write attempt {} while Fenced", i);

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("  ✓ All write attempts consistently rejected while Fenced\n");

    // ========================================
    // PHASE 6: Unpause MinIO, unblock proxy — let S3 race resolve
    // ========================================
    println!("PHASE 6: Unpause MinIO and unblock proxy for S3 race");
    println!("------------------------------------------------------");

    minio.unpause()?;
    println!("  ✓ MinIO unpaused");
    proxy.unblock();
    println!("  ✓ Proxy unblocked - nodes can race and reconnect\n");

    println!("  Waiting for S3 race and reconvergence...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let post_race_lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let post_race_lease = deserialise_lease(&post_race_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  Post-race lease: leader_node_id={:x}, lease_index={}",
        post_race_lease.leader_node_id, post_race_lease.lease_index);
    assert!(
        post_race_lease.lease_index > initial_lease_index,
        "lease_index should have increased after S3 race"
    );
    println!("  ✓ S3 race completed: lease_index {} → {}\n",
        initial_lease_index, post_race_lease.lease_index);

    // ========================================
    // PHASE 7: Verify exactly one node accepts writes after race
    // ========================================
    println!("PHASE 7: Verify exactly one node accepts writes after race");
    println!("------------------------------------------------------------");

    println!("  Waiting for reconvergence...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let post_race_key_leader = AggregateKey::new(5, 1, 1);
    let post_race_key_follower = AggregateKey::new(6, 1, 1);

    let leader_accepts_after = write_event(&mut leader_client, &post_race_key_leader, 1, true).await.is_ok();
    let follower_accepts_after = write_event(&mut follower_client, &post_race_key_follower, 1, true).await.is_ok();

    println!("  Leader accepts writes after race: {}", leader_accepts_after);
    println!("  Follower accepts writes after race: {}", follower_accepts_after);

    assert_ne!(
        leader_accepts_after, follower_accepts_after,
        "FAILED: Both nodes have same write acceptance state. Expected exactly one leader."
    );

    if leader_accepts_after && !follower_accepts_after {
        println!("  ✓ Original leader won S3 race (is Leader)");
    } else if !leader_accepts_after && follower_accepts_after {
        println!("  ✓ Original follower won S3 race (is Leader)");
    }

    println!("\n=== All Tests Passed ===");
    println!("Writes during fencing test validated:");
    println!("  1. Both nodes correctly rejected writes while Fenced");
    println!("  2. Write rejection was consistent across multiple attempts");
    println!("  3. After S3 race, exactly one node became Leader");
    println!("  4. Invariant 9 'No writes while Fenced' is enforced\n");

    Ok(())
}
