//! S3 Lease Integration Test - Follower crash + leader self-heal
//!
//! Tests the follower crash failure mode: leader detects heartbeat loss, fences,
//! races to S3, wins (follower dead), unfences with new lease, continues operation.
//! Then follower restarts and rejoins.
//!
//! Scenario:
//! 1. Start MinIO, establish two-node cluster (leader + follower)
//! 2. Write events 1-3, verify cluster is healthy (replication works)
//! 3. Read initial lease from S3, record lease_index (should be 1)
//! 4. Kill follower process (simulate crash)
//! 5. Wait for leader to detect heartbeat loss and self-heal via S3
//! 6. Verify leader still accepts writes, lease_index incremented to 2
//! 7. Restart follower process
//! 8. Wait for follower to re-register and rejoin cluster
//! 9. Verify follower receives replicated data from leader
//!
//! Run with: cargo test --test s3_follower_crash_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{count_events, write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Lease Follower Crash Integration Test ===\n");

    let port_base = 11900 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-follower-crash").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start cluster and establish leader/follower
    // ========================================
    println!("PHASE 1: Start cluster and establish leader/follower");
    println!("-----------------------------------------------------");

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
    let leader = TestServer::start_with_config(leader_port, leader_config).await?;

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
    let mut follower = TestServer::start_with_config(follower_port, follower_config).await?;

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
    println!("  ✓ Cluster healthy: follower has {} events\n", follower_count);

    // ========================================
    // PHASE 2: Record initial lease state
    // ========================================
    println!("PHASE 2: Record initial lease state");
    println!("-----------------------------------");

    let initial_lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let initial_lease = deserialise_lease(&initial_lease_bytes)
        .map_err(|e| format!("Failed to deserialise initial lease: {:?}", e))?;

    println!("  Initial lease: leader_node_id={:x}, lease_index={}",
        initial_lease.leader_node_id, initial_lease.lease_index);

    assert_eq!(initial_lease.lease_index, 1, "Initial lease_index should be 1");
    println!("  ✓ Recorded initial state\n");

    // ========================================
    // PHASE 3: Kill follower (simulate crash)
    // ========================================
    println!("PHASE 3: Kill follower (simulate crash)");
    println!("----------------------------------------");

    println!("  Stopping follower process...");
    drop(follower_client);
    follower.stop();
    println!("  ✓ Follower stopped\n");

    // ========================================
    // PHASE 4: Wait for leader self-heal
    // ========================================
    println!("PHASE 4: Wait for leader self-heal");
    println!("----------------------------------");

    println!("  Waiting for leader to detect heartbeat loss and self-heal...");
    println!("  (heartbeat timeout ~2s + S3 race ~1s = ~5s total)");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 5: Verify leader recovered and accepts writes
    // ========================================
    println!("\nPHASE 5: Verify leader recovered and accepts writes");
    println!("----------------------------------------------------");

    println!("  Writing events 4-5 to leader (should still work)...");
    write_event(&mut leader_client, &aggregate_key, 4, false).await?;
    write_event(&mut leader_client, &aggregate_key, 5, false).await?;
    println!("  ✓ Leader accepted writes after self-heal");

    let new_lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let new_lease = deserialise_lease(&new_lease_bytes)
        .map_err(|e| format!("Failed to deserialise new lease: {:?}", e))?;

    println!("  New lease: leader_node_id={:x}, lease_index={}",
        new_lease.leader_node_id, new_lease.lease_index);

    assert!(new_lease.lease_index > initial_lease.lease_index,
        "lease_index should have increased after self-heal: was {}, now {}",
        initial_lease.lease_index, new_lease.lease_index);
    assert_eq!(
        new_lease.leader_node_id, initial_lease.leader_node_id,
        "leader_node_id should NOT have changed (same leader self-healed)"
    );
    println!("  ✓ Lease updated: lease_index={}, same leader\n", new_lease.lease_index);

    // ========================================
    // PHASE 6: Restart follower
    // ========================================
    println!("PHASE 6: Restart follower");
    println!("-------------------------");

    println!("  Restarting follower...");
    follower.restart().await?;
    println!("  Follower process restarted");

    println!("  Waiting for follower to re-register and rejoin...");
    println!("  (startup + registration + discovery + catch-up = ~8s)");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // ========================================
    // PHASE 7: Verify follower rejoined and receives new writes
    // ========================================
    println!("\nPHASE 7: Verify follower rejoined and receives new writes");
    println!("----------------------------------------------------------");

    let mut restarted_follower_client = CeleriantClient::connect(follower.address()).await?;

    // The follower's persisted WAL has events 1-3 from before the crash.
    // Events 4-5 were written while follower was down — catching up on those
    // requires the follower kick/rejoin protocol (not yet implemented).
    // Verify that NEW writes after follower restart DO replicate.
    println!("  Writing events 6-7 to leader (after follower restart)...");
    write_event(&mut leader_client, &aggregate_key, 6, false).await?;
    write_event(&mut leader_client, &aggregate_key, 7, false).await?;

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let final_follower_count = count_events(&mut restarted_follower_client, &aggregate_key).await?;
    println!("  Restarted follower has {} events", final_follower_count);

    // Follower should have at least the original 3 + the 2 new ones.
    // It may or may not have events 4-5 (depends on kick/rejoin protocol).
    assert!(
        final_follower_count >= 5,
        "Restarted follower should have at least 5 events (3 original + 2 new), got {}",
        final_follower_count
    );
    println!("  ✓ Follower rejoined and receives new replication");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
