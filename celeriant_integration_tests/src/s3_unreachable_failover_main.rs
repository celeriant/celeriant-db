//! S3 Unreachable Failover Integration Test - MinIO down during leader crash
//!
//! Tests the "S3 unreachable, heartbeat lost" failure mode from the design spec.
//! This validates the critical liveness scenario where both nodes fence but cannot
//! complete the S3 race because MinIO is down.
//!
//! Scenario:
//! 1. Start MinIO, establish two-node cluster (leader + follower)
//! 2. Write events 1-3, verify cluster is healthy (replication works)
//! 3. Pause MinIO container (S3 becomes unreachable)
//! 4. Kill leader process (follower detects heartbeat loss)
//! 5. Verify follower stays fenced (can't complete S3 race, S3 unreachable)
//! 6. Unpause MinIO (S3 becomes reachable again)
//! 7. Verify follower recovers (wins S3 race, becomes leader)
//!
//! Expected behavior from design spec:
//! "S3 unreachable, heartbeat lost: Both sides fence. Both try S3 race — S3 unreachable.
//!  Both stay fenced, retry with backoff. No writes served by either node until S3 returns."
//!
//! Test results: S3 retry works! The follower successfully recovers after MinIO is unpaused,
//! completes the S3 race, and becomes leader. This validates the liveness guarantee.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{count_events, write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Unreachable Failover Integration Test ===\n");

    let port_base = 12700 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-s3-unreachable").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start cluster and verify healthy
    // ========================================
    println!("PHASE 1: Start cluster and verify healthy");
    println!("------------------------------------------");

    let leader_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        bootstrap_as_leader: true,
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
    let mut leader = TestServer::start_with_config(leader_port, leader_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        bootstrap_as_leader: false,
        routing_rule: RoutingRule::AggregateTypeId,
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
    println!("  Starting follower on port {}...", follower_port);
    let follower = TestServer::start_with_config(follower_port, follower_config).await?;

    println!("  Waiting for election and heartbeat establishment...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    println!("  Writing events 1-3 to verify cluster health...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(follower_count, 3, "Follower should have 3 events");
    println!("  ✓ Cluster healthy: follower has {} events", follower_count);

    let initial_lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let initial_lease = deserialise_lease(&initial_lease_bytes)
        .map_err(|e| format!("Failed to deserialise initial lease: {:?}", e))?;
    println!("  ✓ Initial lease: leader_node_id={:x}, lease_index={}\n",
        initial_lease.leader_node_id, initial_lease.lease_index);
    assert_eq!(initial_lease.lease_index, 1, "Initial lease_index should be 1");

    // ========================================
    // PHASE 2: Pause MinIO (S3 unreachable)
    // ========================================
    println!("PHASE 2: Pause MinIO (S3 unreachable)");
    println!("-------------------------------------");

    println!("  Pausing MinIO container...");
    minio.pause()?;
    println!("  ✓ S3 is now unreachable (all S3 requests will timeout)\n");

    // ========================================
    // PHASE 3: Kill leader
    // ========================================
    println!("PHASE 3: Kill leader");
    println!("--------------------");

    println!("  Stopping leader process...");
    drop(leader_client);
    leader.stop();
    println!("  ✓ Leader stopped (follower will detect heartbeat loss)\n");

    // ========================================
    // PHASE 4: Verify follower is fenced (S3 unreachable)
    // ========================================
    println!("PHASE 4: Verify follower is fenced (S3 unreachable)");
    println!("---------------------------------------------------");

    println!("  Waiting for heartbeat timeout (~2s) + S3 race attempts...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("  Attempting write to follower (should fail - fenced or can't complete S3 race)...");
    let write_result_1 = write_event(&mut follower_client, &aggregate_key, 4, false).await;

    if write_result_1.is_err() {
        println!("  ✓ First write rejected (follower fenced or still Follower)");
    } else {
        return Err("Follower accepted write but should be fenced (S3 unreachable)!".into());
    }

    println!("  Waiting another 3s to ensure follower stays fenced...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("  Attempting second write to follower (should still fail)...");
    let write_result_2 = write_event(&mut follower_client, &aggregate_key, 5, false).await;

    if write_result_2.is_err() {
        println!("  ✓ Second write rejected (follower still fenced, S3 unreachable)");
    } else {
        return Err("Follower accepted write but should stay fenced (S3 unreachable)!".into());
    }
    println!("  ✓ Verified: follower stays fenced when S3 is unreachable\n");

    // ========================================
    // PHASE 5: Unpause MinIO (S3 becomes reachable)
    // ========================================
    println!("PHASE 5: Unpause MinIO (S3 becomes reachable)");
    println!("---------------------------------------------");

    println!("  Unpausing MinIO container...");
    minio.unpause()?;
    println!("  ✓ S3 is now reachable (requests can succeed)\n");

    // ========================================
    // PHASE 6: Verify follower recovers
    // ========================================
    println!("PHASE 6: Verify follower recovers");
    println!("---------------------------------");

    println!("  Waiting for S3 race retry to complete (~5s)...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("  Attempting write to follower (should succeed now that S3 is reachable)...");
    write_event(&mut follower_client, &aggregate_key, 6, false).await?;
    println!("  ✓ Follower recovered and became leader (S3 retry works!)");

    let final_lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let final_lease = deserialise_lease(&final_lease_bytes)
        .map_err(|e| format!("Failed to deserialise final lease: {:?}", e))?;

    println!("  ✓ Final lease: leader_node_id={:x}, lease_index={}",
        final_lease.leader_node_id, final_lease.lease_index);
    assert_eq!(final_lease.lease_index, 2, "lease_index should be 2 after takeover");

    println!("  ✓ Verifying replication of event 6...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let final_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(final_count, 4, "Should have 4 events (1-3 + 6)");
    println!("  ✓ Recovery complete: follower became leader after S3 returned");

    println!("\n=== Test Complete ===\n");

    Ok(())
}
