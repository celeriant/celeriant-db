//! S3 Lease Monotonicity Integration Test - Multiple consecutive failovers
//!
//! Tests that lease_index is strictly monotonically increasing across multiple failovers.
//! Design spec (docs/s3-lease-high-level-design.md, invariant 1):
//! "lease_index is monotonically increasing. Every Lease::promote() increments by 1.
//!  Never decremented, never reused."
//!
//! Scenario:
//! 1. Start cluster: leader A + follower B, verify healthy (lease_index=1)
//! 2. Failover 1: Kill leader A, follower B takes over (lease_index=2)
//! 3. Write to new leader B, verify accepted
//! 4. Restart old leader A as follower
//! 5. Failover 2: Kill leader B, follower A takes over (lease_index=3)
//! 6. Verify final state: lease_index=3, leader A accepts writes
//!
//! This chains: Leader A → Leader B → Leader A, with lease_index going 1 → 2 → 3.
//!
//! Run with: cargo run --bin s3_lease_monotonicity_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Lease Monotonicity Integration Test ===\n");

    let port_base = 12500 + (std::process::id() % 100) as u16;
    let leader_a_port = port_base;
    let leader_b_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-lease-monotonicity").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start cluster (leader A + follower B) and verify initial state
    // ========================================
    println!("PHASE 1: Start cluster and verify initial state");
    println!("-----------------------------------------------");

    let leader_a_config = ServerConfig {
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
    println!("  Starting leader A on port {}...", leader_a_port);
    let mut leader_a = TestServer::start_with_config(leader_a_port, leader_a_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader_b_config = ServerConfig {
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
    println!("  Starting follower B on port {}...", leader_b_port);
    let mut leader_b = TestServer::start_with_config(leader_b_port, leader_b_config).await?;

    println!("  Waiting for election, heartbeat establishment, and S3 lease expiry...");
    println!("  (S3 lease TTL = 10s; must expire so failover is gated only by heartbeat TTL)");
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut leader_a_client = CeleriantClient::connect(leader_a.address()).await?;

    println!("  Writing events 1-3 to verify cluster health...");
    for i in 1..=3 {
        write_event(&mut leader_a_client, &aggregate_key, i, i == 1).await?;
    }

    let mut leader_b_client = CeleriantClient::connect(leader_b.address()).await?;
    let follower_b_count = count_events(&mut leader_b_client, &aggregate_key).await?;
    assert_eq!(follower_b_count, 3, "Follower B should have 3 events");
    println!("  ✓ Cluster healthy: follower B has {} events", follower_b_count);

    let lease_1_bytes = minio.get_object("cluster/lease.json").await?;
    let lease_1 = deserialise_lease(&lease_1_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  Initial lease: leader_node_id={:x}, lease_index={}",
        lease_1.leader_node_id, lease_1.lease_index);
    let initial_lease_index = lease_1.lease_index;
    let node_a_id = lease_1.leader_node_id;
    println!("  ✓ Initial state verified: lease_index={}\n", initial_lease_index);

    // ========================================
    // PHASE 2: First failover - Kill leader A
    // ========================================
    println!("PHASE 2: First failover - Kill leader A");
    println!("---------------------------------------");

    println!("  Stopping leader A...");
    drop(leader_a_client);
    leader_a.stop();
    println!("  ✓ Leader A stopped");

    println!("  Waiting for follower B to detect heartbeat loss and win S3 race...");
    println!("  (heartbeat timeout ~2s + S3 race ~1s = ~5s total)");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 3: Verify follower B is now leader with lease_index=2
    // ========================================
    println!("\nPHASE 3: Verify follower B is now leader (lease_index=2)");
    println!("--------------------------------------------------------");

    let lease_2_bytes = minio.get_object("cluster/lease.json").await?;
    let lease_2 = deserialise_lease(&lease_2_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  Lease after failover 1: leader_node_id={:x}, lease_index={}",
        lease_2.leader_node_id, lease_2.lease_index);
    assert!(
        lease_2.lease_index > initial_lease_index,
        "lease_index should have increased after first failover: was {}, now {}",
        initial_lease_index, lease_2.lease_index
    );
    assert_ne!(lease_2.leader_node_id, node_a_id, "leader_node_id should have changed");
    let node_b_id = lease_2.leader_node_id;
    let lease_index_after_failover_1 = lease_2.lease_index;
    println!("  ✓ Lease updated: lease_index {} → {}, new leader B",
        initial_lease_index, lease_2.lease_index);

    println!("  Writing events 4-5 to new leader B...");
    for i in 4..=5 {
        write_event(&mut leader_b_client, &aggregate_key, i, false).await?;
    }
    println!("  ✓ New leader B accepted writes\n");

    // ========================================
    // PHASE 4: Restart old leader A as follower
    // ========================================
    println!("PHASE 4: Restart old leader A as follower");
    println!("-----------------------------------------");

    println!("  Restarting leader A...");
    leader_a.restart().await?;
    println!("  Leader A process restarted");

    println!("  Waiting for A to become follower and B's S3 lease to expire...");
    println!("  (startup + election + heartbeat + S3 lease expiry = ~12s)");
    tokio::time::sleep(Duration::from_secs(12)).await;

    leader_a_client = CeleriantClient::connect(leader_a.address()).await?;

    println!("  Attempting write to A (should be follower now)...");
    let write_result = write_event(&mut leader_a_client, &aggregate_key, 99, false).await;
    if write_result.is_err() {
        println!("  ✓ A rejected write (is now follower)\n");
    } else {
        return Err("A accepted write but should be follower!".into());
    }

    // ========================================
    // PHASE 5: Second failover - Kill leader B
    // ========================================
    println!("PHASE 5: Second failover - Kill leader B");
    println!("----------------------------------------");

    println!("  Stopping leader B...");
    drop(leader_b_client);
    leader_b.stop();
    println!("  ✓ Leader B stopped");

    println!("  Waiting for follower A to detect heartbeat loss and win S3 race...");
    println!("  (heartbeat timeout ~2s + S3 race ~1s = ~5s total)");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 6: Verify follower A is now leader again with lease_index=3
    // ========================================
    println!("\nPHASE 6: Verify A is now leader again (lease_index=3)");
    println!("-----------------------------------------------------");

    let lease_3_bytes = minio.get_object("cluster/lease.json").await?;
    let lease_3 = deserialise_lease(&lease_3_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  Lease after failover 2: leader_node_id={:x}, lease_index={}",
        lease_3.leader_node_id, lease_3.lease_index);
    assert!(
        lease_3.lease_index > lease_index_after_failover_1,
        "lease_index should have increased after second failover: was {}, now {}",
        lease_index_after_failover_1, lease_3.lease_index
    );
    assert_eq!(lease_3.leader_node_id, node_a_id, "leader should be A again");
    assert_ne!(lease_3.leader_node_id, node_b_id, "leader should not be B");
    println!("  ✓ Lease updated: lease_index {} → {}, leader is A again",
        lease_index_after_failover_1, lease_3.lease_index);

    println!("  Writing events 6-7 to re-promoted leader A...");
    for i in 6..=7 {
        write_event(&mut leader_a_client, &aggregate_key, i, false).await?;
    }
    println!("  ✓ Re-promoted leader A accepted writes");

    let final_count_a = count_events(&mut leader_a_client, &aggregate_key).await?;
    println!("  Leader A has {} events", final_count_a);
    // A has events 1-3 from its first term + events 6-7 from re-promoted term = 5 minimum.
    // Events 4-5 (written to B) may or may not have replicated to A before B was killed.
    assert!(final_count_a >= 5, "Leader A should have at least 5 events (3 original + 2 new)");
    println!("  ✓ Final state verified (leader A has {} events)\n", final_count_a);

    // ========================================
    // Summary
    // ========================================
    println!("=== All Tests Passed ===\n");
    println!("Summary: lease_index monotonicity verified across multiple failovers:");
    println!("  - Initial state (Leader A): lease_index={}", initial_lease_index);
    println!("  - After failover 1 (A→B):   lease_index={}", lease_index_after_failover_1);
    println!("  - After failover 2 (B→A):   lease_index={}", lease_3.lease_index);
    println!("  - Invariant satisfied: {} < {} < {} (strictly monotonically increasing)",
        initial_lease_index, lease_index_after_failover_1, lease_3.lease_index);

    Ok(())
}
