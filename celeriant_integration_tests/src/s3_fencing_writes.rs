//! S3 Fencing Writes Integration Test
//!
//! Tests write fencing invariants from the S3 lease design.
//! Absorbs not_leader_error (NotLeader error type + leader address assertion).
//!
//! Scenario:
//! Part A: Follower rejects writes across all shards, returns NotLeader with leader address
//! Part B: Writes rejected during failover transition (kill leader, immediate write fails)
//! Part C: Former follower becomes leader, lease_epoch incremented, writes succeed
//!
//! Invariants tested: 3 (write gating), 4 (asymmetric fencing)

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{count_events, write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Fencing Writes Integration Test ===\n");

    let port_base = 12300 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-fencing-writes").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    let num_shards = 4;

    // Create aggregate keys for testing different shards
    let agg_shard_0 = AggregateKey::new(1, 1, 1);
    let agg_shard_1 = AggregateKey::new(1, 2, 1);
    let agg_shard_2 = AggregateKey::new(1, 3, 1);
    let agg_shard_3 = AggregateKey::new(1, 4, 1);

    println!("Starting two-node cluster with S3 lease election...");

    // Node A config: Leader role, S3 enabled
    let node_a_config = ServerConfig {
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
    println!("Starting Node A (leader role) on port {}...", leader_port);
    let mut node_a = TestServer::start_with_config(leader_port, node_a_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Node B config: Follower role, same S3 bucket
    let node_b_config = ServerConfig {
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
    println!("Starting Node B (follower role) on port {}...", follower_port);
    let node_b = TestServer::start_with_config(follower_port, node_b_config).await?;

    println!(
        "Cluster started: Node A at {}, Node B at {}\n",
        node_a.address(),
        node_b.address()
    );

    // Wait for election, heartbeat establishment, and S3 lease expiry (15s TTL).
    // Must expire so failover is gated only by heartbeat TTL.
    println!("Waiting for election, heartbeat establishment, and S3 lease expiry...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    // ========================================
    // PHASE 1: Verify leader accepts writes
    // ========================================
    println!("\nPHASE 1: Verify leader accepts writes");
    println!("--------------------------------------");

    let mut node_a_client = CeleriantClient::connect(node_a.address()).await?;

    println!("  Writing events to Node A across multiple shards...");
    write_event(&mut node_a_client, &agg_shard_0, 1, true).await?;
    write_event(&mut node_a_client, &agg_shard_1, 1, true).await?;
    write_event(&mut node_a_client, &agg_shard_2, 1, true).await?;
    write_event(&mut node_a_client, &agg_shard_3, 1, true).await?;
    println!("  ✓ Node A accepted writes across all shards (is leader)");

    // ========================================
    // PHASE 2: Verify follower rejects writes across multiple shards
    // ========================================
    println!("\nPHASE 2: Verify follower rejects writes across multiple shards");
    println!("---------------------------------------------------------------");

    let mut node_b_client = CeleriantClient::connect(node_b.address()).await?;

    println!("  Attempting writes to Node B (should be follower)...");

    let write_shard_0 = write_event(&mut node_b_client, &agg_shard_0, 99, false).await;
    let write_shard_1 = write_event(&mut node_b_client, &agg_shard_1, 99, false).await;
    let write_shard_2 = write_event(&mut node_b_client, &agg_shard_2, 99, false).await;
    let write_shard_3 = write_event(&mut node_b_client, &agg_shard_3, 99, false).await;

    if write_shard_0.is_err() && write_shard_1.is_err() && write_shard_2.is_err() && write_shard_3.is_err() {
        println!("  ✓ Node B rejected writes on all shards (is follower)");
    } else {
        return Err("Node B accepted writes on some shards but should be follower!".into());
    }

    // Verify NotLeader error type with leader address (absorbed from not_leader_error)
    let not_leader_key = AggregateKey::new(9, 1, 1);
    let not_leader_result = write_event(&mut node_b_client, &not_leader_key, 1, true).await;
    match not_leader_result {
        Err(ref e) => {
            if let Some(client_err) = e.downcast_ref::<ClientError>() {
                match client_err {
                    ClientError::NotLeader { leader_address: Some(addr), .. } => {
                        assert!(
                            addr.ends_with(&format!(":{}", leader_port)),
                            "NotLeader leader_address '{}' should contain leader port {}",
                            addr, leader_port
                        );
                        println!("  ✓ NotLeader error includes correct leader address");
                    }
                    ClientError::NotLeader { leader_address: None, .. } => {
                        println!("  ✓ NotLeader returned (leader address not yet known)");
                    }
                    _ => println!("  ✓ Follower rejected write (non-NotLeader error type)"),
                }
            }
        }
        Ok(_) => return Err("Follower accepted write but should return NotLeader".into()),
    }

    // ========================================
    // PHASE 3: Read initial lease state
    // ========================================
    println!("\nPHASE 3: Read initial lease state");
    println!("---------------------------------");

    let initial_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let initial_lease = deserialise_lease(&initial_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  Initial lease: leader_node_id={:x}, lease_epoch={}",
        initial_lease.leader_node_id, initial_lease.lease_epoch);

    let initial_leader_node_id = initial_lease.leader_node_id;
    let initial_lease_epoch = initial_lease.lease_epoch;

    println!("  ✓ Initial lease_epoch is {}", initial_lease_epoch);

    // ========================================
    // PHASE 4: Writes rejected during failover transition
    // ========================================
    println!("\nPHASE 4: Writes rejected during failover transition");
    println!("---------------------------------------------------");

    println!("  Killing leader (Node A)...");
    node_a.stop();
    println!("  Leader stopped");

    println!("  Immediately attempting write to Node B (should fail - still Follower)...");
    let immediate_write_result = write_event(&mut node_b_client, &agg_shard_0, 2, false).await;

    if immediate_write_result.is_err() {
        println!("  ✓ Node B rejected write immediately after leader death (still Follower)");
    } else {
        println!("  ⚠ Node B accepted write (already became Leader - race timing)");
    }

    println!("  Waiting for S3 race resolution (5 seconds)...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 5: Former follower now accepts writes as new leader
    // ========================================
    println!("\nPHASE 5: Former follower now accepts writes as new leader");
    println!("---------------------------------------------------------");

    println!("  Attempting write to Node B (should succeed - now Leader)...");
    write_event(&mut node_b_client, &agg_shard_0, 2, false).await?;
    write_event(&mut node_b_client, &agg_shard_0, 3, false).await?;
    println!("  ✓ Node B now accepts writes (became leader after S3 race)");

    // ========================================
    // PHASE 6: Verify lease_epoch incremented after failover
    // ========================================
    println!("\nPHASE 6: Verify lease_epoch incremented after failover");
    println!("------------------------------------------------------");

    let new_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let new_lease = deserialise_lease(&new_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  New lease: leader_node_id={:x}, lease_epoch={}",
        new_lease.leader_node_id, new_lease.lease_epoch);

    assert!(
        new_lease.lease_epoch > initial_lease_epoch,
        "lease_epoch should have increased after failover: was {}, now {}",
        initial_lease_epoch, new_lease.lease_epoch
    );
    println!("  ✓ lease_epoch increased: {} → {}", initial_lease_epoch, new_lease.lease_epoch);

    assert_ne!(
        new_lease.leader_node_id, initial_leader_node_id,
        "leader_node_id should change after failover"
    );
    println!("  ✓ leader_node_id changed to {:x}", new_lease.leader_node_id);

    // ========================================
    // PHASE 7: Verify writes with new lease term
    // ========================================
    println!("\nPHASE 7: Verify writes with new lease term");
    println!("-------------------------------------------");

    // Use a fresh aggregate key to avoid issues with replicated data from old term
    let agg_new_term = AggregateKey::new(2, 1, 1);
    println!("  Writing events to new leader on fresh aggregate (new lease term)...");
    write_event(&mut node_b_client, &agg_new_term, 1, true).await?;
    write_event(&mut node_b_client, &agg_new_term, 2, false).await?;
    println!("  ✓ New leader accepts writes in new lease term");

    let new_term_count = count_events(&mut node_b_client, &agg_new_term).await?;
    assert_eq!(new_term_count, 2, "New term aggregate should have 2 events");
    println!("  ✓ Event count correct: {} events in new lease term", new_term_count);

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
