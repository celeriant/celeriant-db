//! S3 Lease Election Integration Test - Cold-start + Steady State
//!
//! Tests that two nodes can perform a cold-start election via S3 and establish
//! a stable leader-follower relationship.
//!
//! Scenario:
//! 1. Start MinIO container, create bucket for cluster metadata
//! 2. Start Node A with cluster_role=Leader (bootstrap_as_leader=true), S3 config
//! 3. Start Node B with cluster_role=Follower (bootstrap_as_leader=false), same S3 bucket
//! 4. Wait for election to complete and heartbeat to establish
//! 5. Verify writes to leader succeed, writes to follower rejected
//! 6. Verify S3 lease.bin shows correct leader_node_id and lease_index=1
//! 7. Verify S3 membership.bin shows both nodes registered
//! 8. Verify replication works (read from follower shows leader's writes)
//!
//! Run with: cargo test --test s3_election_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{count_events, write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::{deserialise_lease, deserialise_membership};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Lease Election Integration Test - Cold-start + Steady State ===\n");

    let port_base = 11300 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-election").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    println!("Starting two-node cluster with S3 lease election...");

    // Node A config: bootstrap as leader, S3 enabled (discovery via S3)
    let node_a_config = ServerConfig {
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
    println!("Starting Node A (leader role) on port {}...", leader_port);
    let node_a = TestServer::start_with_config(leader_port, node_a_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Node B config: Follower role, same S3 bucket
    let node_b_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        bootstrap_as_leader: false,
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket_name.clone()),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
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

    // Wait for election to complete and heartbeat to establish
    println!("Waiting for election and heartbeat establishment (3 seconds)...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ========================================
    // Verify Leader Accepts Writes
    // ========================================
    println!("\nPHASE 1: Verify leader accepts writes");
    println!("--------------------------------------");

    let mut node_a_client = CeleriantClient::connect(node_a.address()).await?;

    println!("  Writing events 1-3 to Node A (should be leader)...");
    for i in 1..=3 {
        write_event(&mut node_a_client, &aggregate_key, i, i == 1).await?;
    }
    println!("  ✓ Node A accepted writes (is leader)");

    // ========================================
    // Verify Follower Rejects Writes
    // ========================================
    println!("\nPHASE 2: Verify follower rejects writes");
    println!("----------------------------------------");

    let mut node_b_client = CeleriantClient::connect(node_b.address()).await?;

    println!("  Attempting write to Node B (should be follower)...");
    let write_result = write_event(&mut node_b_client, &aggregate_key, 99, false).await;

    if write_result.is_err() {
        println!("  ✓ Node B rejected write (is follower)");
    } else {
        return Err("Node B accepted write but should be follower!".into());
    }

    // ========================================
    // Verify S3 Lease State
    // ========================================
    println!("\nPHASE 3: Verify S3 lease state");
    println!("------------------------------");

    let lease_bytes = minio.get_object("cluster/lease.bin").await?;
    println!("  Read lease.bin from S3 ({} bytes)", lease_bytes.len());

    let lease = deserialise_lease(&lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  Lease: leader_node_id={}, lease_index={}", lease.leader_node_id, lease.lease_index);

    assert_eq!(
        lease.lease_index, 1,
        "lease_index should be 1 for initial election"
    );
    println!("  ✓ lease_index is 1 (initial election)");

    // We can't know which specific node_id, but it should be non-zero
    assert_ne!(lease.leader_node_id, 0, "leader_node_id should be set");
    println!("  ✓ leader_node_id is set ({:x})", lease.leader_node_id);

    // ========================================
    // Verify S3 Membership State
    // ========================================
    println!("\nPHASE 4: Verify S3 membership state");
    println!("-----------------------------------");

    let membership_bytes = minio.get_object("cluster/membership.bin").await?;
    println!("  Read membership.bin from S3 ({} bytes)", membership_bytes.len());

    let membership = deserialise_membership(&membership_bytes)
        .map_err(|e| format!("Failed to deserialise membership: {:?}", e))?;

    println!("  Membership: node_count={}", membership.node_count());

    assert!(
        membership.is_fully_replicated(),
        "membership should have both nodes registered"
    );

    println!("  ✓ Both nodes registered in membership");

    // ========================================
    // Verify Replication Works
    // ========================================
    println!("\nPHASE 5: Verify replication works");
    println!("---------------------------------");

    println!("  Waiting for replication to propagate...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_count = count_events(&mut node_b_client, &aggregate_key).await?;
    assert_eq!(
        follower_count, 3,
        "Follower should have replicated 3 events from leader"
    );
    println!("  ✓ Follower has {} events (replication working)", follower_count);

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
