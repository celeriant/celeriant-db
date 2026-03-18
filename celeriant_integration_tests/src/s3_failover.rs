//! S3 Lease Failover Integration Test - Leader crash + follower takeover + old leader recovery
//!
//! Tests the complete failover cycle: leader crashes, follower detects and takes over,
//! old leader restarts and becomes follower.
//!
//! Scenario:
//! 1. Start MinIO, establish two-node cluster (leader + follower)
//! 2. Write events 1-3, verify cluster is healthy (replication works)
//! 3. Read initial lease from S3, record lease_index (should be 1)
//! 4. Kill leader process (simulate crash)
//! 5. Wait for follower to detect heartbeat loss and win S3 race to become new leader
//! 6. Verify follower is now leader: writes succeed, lease_index incremented to 2
//! 7. Restart old leader process
//! 8. Wait for old leader to complete election and discover it should be follower
//! 9. Verify old leader is now follower: writes rejected
//! 10. Write events to new leader, verify old leader (now follower) replicates them
//!
//! Run with: cargo test --test s3_failover_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Lease Failover Integration Test ===\n");

    let port_base = 11500 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-failover").await?;
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
    println!("  Starting initial leader on port {}...", leader_port);
    let mut leader = TestServer::start_with_config(leader_port, leader_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
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
    println!("  Starting follower on port {}...", follower_port);
    let follower = TestServer::start_with_config(follower_port, follower_config).await?;

    println!("  Waiting for election, heartbeat establishment, and S3 lease expiry...");
    println!("  (S3 lease TTL = 10s; must expire so failover is gated only by heartbeat TTL)");
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    println!("  Writing events 1-3 to verify cluster health...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(follower_count, 3, "Follower should have 3 events");
    println!("  ✓ Cluster healthy: follower has {} events\n", follower_count);

    // ========================================
    // PHASE 2: Record initial lease state
    // ========================================
    println!("PHASE 2: Record initial lease state");
    println!("-----------------------------------");

    let initial_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let initial_lease = deserialise_lease(&initial_lease_bytes)
        .map_err(|e| format!("Failed to deserialise initial lease: {:?}", e))?;

    println!("  Initial lease: leader_node_id={:x}, lease_index={}",
        initial_lease.leader_node_id, initial_lease.lease_index);

    let initial_lease_index = initial_lease.lease_index;
    let original_leader_node_id = initial_lease.leader_node_id;
    println!("  ✓ Recorded initial state\n");

    // ========================================
    // PHASE 3: Kill leader (simulate crash)
    // ========================================
    println!("PHASE 3: Kill leader (simulate crash)");
    println!("--------------------------------------");

    println!("  Stopping leader process...");
    drop(leader_client);
    leader.stop();
    println!("  ✓ Leader stopped\n");

    // ========================================
    // PHASE 4: Wait for follower takeover
    // ========================================
    println!("PHASE 4: Wait for follower takeover");
    println!("-----------------------------------");

    println!("  Waiting for follower to detect heartbeat loss and take over...");
    println!("  (S3 lease already expired; heartbeat timeout ~2s + S3 CAS ~1s)");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 5: Verify follower is now leader
    // ========================================
    println!("\nPHASE 5: Verify follower is now leader");
    println!("--------------------------------------");

    println!("  Attempting write to former follower (should be new leader)...");
    write_event(&mut follower_client, &aggregate_key, 4, false).await?;
    println!("  ✓ Former follower accepted write (is now leader)");

    let new_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let new_lease = deserialise_lease(&new_lease_bytes)
        .map_err(|e| format!("Failed to deserialise new lease: {:?}", e))?;

    println!("  New lease: leader_node_id={:x}, lease_index={}",
        new_lease.leader_node_id, new_lease.lease_index);

    assert!(
        new_lease.lease_index > initial_lease_index,
        "lease_index should have increased after failover: was {}, now {}",
        initial_lease_index, new_lease.lease_index
    );
    assert_ne!(
        new_lease.leader_node_id, original_leader_node_id,
        "leader_node_id should have changed"
    );
    println!("  ✓ Lease updated: lease_index {} → {}, new leader\n",
        initial_lease_index, new_lease.lease_index);

    // ========================================
    // PHASE 6: Restart old leader
    // ========================================
    println!("PHASE 6: Restart old leader");
    println!("---------------------------");

    println!("  Restarting old leader...");
    leader.restart().await?;
    println!("  Old leader process restarted");

    println!("  Waiting for old leader to complete election and become follower...");
    println!("  (startup + election + heartbeat = ~5s)");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 7: Verify old leader is now follower
    // ========================================
    println!("\nPHASE 7: Verify old leader is now follower");
    println!("-------------------------------------------");

    let mut old_leader_client = CeleriantClient::connect(leader.address()).await?;

    println!("  Attempting write to old leader (should be follower now)...");
    let write_result = write_event(&mut old_leader_client, &aggregate_key, 99, false).await;

    if write_result.is_err() {
        println!("  ✓ Old leader rejected write (is now follower)");
    } else {
        return Err("Old leader accepted write but should be follower!".into());
    }

    // ========================================
    // PHASE 8: Verify replication to old leader (now follower)
    // ========================================
    println!("\nPHASE 8: Verify replication to old leader (now follower)");
    println!("---------------------------------------------------------");

    println!("  Writing events 5-6 to new leader...");
    for i in 5..=6 {
        write_event(&mut follower_client, &aggregate_key, i, false).await?;
    }

    let old_leader_count = count_events(&mut old_leader_client, &aggregate_key).await?;
    println!("  Old leader (now follower) has {} events", old_leader_count);

    assert_eq!(
        old_leader_count, 6,
        "Old leader (now follower) should have all 6 events"
    );
    println!("  ✓ Old leader successfully replicating as follower");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
