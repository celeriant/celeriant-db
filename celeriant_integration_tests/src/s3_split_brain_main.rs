//! S3 Split-Brain Prevention Test - Both nodes bootstrap as leader
//!
//! Tests that when both nodes start with cluster_role=Leader, the S3 CreateOnly
//! race ensures exactly one becomes leader and the other becomes follower.
//!
//! Scenario:
//! 1. Start MinIO
//! 2. Start Node A with cluster_role=Leader (bootstrap_as_leader=true)
//! 3. Start Node B with cluster_role=Leader (bootstrap_as_leader=true) — BOTH claim leader
//! 4. Wait for election to settle via S3 CreateOnly race
//! 5. Verify exactly one leader emerged:
//!    - S3 lease shows single leader_node_id, lease_index=1
//!    - Exactly one node accepts writes, the other rejects
//! 6. Verify the loser of the CreateOnly race became Follower
//!
//! Run with: cargo test --test s3_split_brain_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_msg::process_requests::Request;
use celeriant_runtimes::RoutingRule;
use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType};
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Split-Brain Prevention Test ===\n");

    let port_base = 11700 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-split-brain").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start BOTH nodes as Leader (split-brain scenario)
    // ========================================
    println!("PHASE 1: Start BOTH nodes as Leader (split-brain scenario)");
    println!("-----------------------------------------------------------");

    let node_a_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
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
    println!("  Starting Node A with cluster_role=Leader on port {}...", node_a_port);
    let node_a = TestServer::start_with_config(node_a_port, node_a_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let node_b_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
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
    println!("  Starting Node B with cluster_role=Leader on port {}...", node_b_port);
    let node_b = TestServer::start_with_config(node_b_port, node_b_config).await?;

    println!("\n  Both nodes started claiming to be leader!");
    println!("  Waiting for S3 CreateOnly race to resolve (5 seconds)...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 2: Verify exactly one leader emerged from S3 lease
    // ========================================
    println!("\nPHASE 2: Verify exactly one leader emerged from S3 lease");
    println!("---------------------------------------------------------");

    let lease_bytes = minio.get_object("cluster/lease.bin").await?;
    println!("  Read lease.bin from S3 ({} bytes)", lease_bytes.len());

    let lease = deserialise_lease(&lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  Lease: leader_node_id={:x}, lease_index={}",
        lease.leader_node_id, lease.lease_index);

    assert_eq!(lease.lease_index, 1, "lease_index should be 1 (single leader elected)");
    assert_ne!(lease.leader_node_id, 0, "leader_node_id should be set");
    println!("  ✓ S3 lease shows single leader with lease_index=1");

    // ========================================
    // PHASE 3: Verify exactly one node accepts writes
    // ========================================
    println!("\nPHASE 3: Verify exactly one node accepts writes");
    println!("------------------------------------------------");

    let mut node_a_client = CeleriantClient::connect(node_a.address()).await?;
    let mut node_b_client = CeleriantClient::connect(node_b.address()).await?;

    println!("  Attempting write to Node A...");
    let a_write_result = write_event(&mut node_a_client, &aggregate_key, 1, true).await;

    println!("  Attempting write to Node B...");
    let b_write_result = write_event(&mut node_b_client, &aggregate_key, 2, true).await;

    let a_is_leader = a_write_result.is_ok();
    let b_is_leader = b_write_result.is_ok();

    println!("  Node A write result: {}", if a_is_leader { "SUCCESS (is leader)" } else { "REJECTED (is follower)" });
    println!("  Node B write result: {}", if b_is_leader { "SUCCESS (is leader)" } else { "REJECTED (is follower)" });

    // Exactly one should be leader
    assert!(
        a_is_leader != b_is_leader,
        "Exactly one node should accept writes (one leader, one follower)"
    );

    if a_is_leader {
        println!("  ✓ Node A is leader, Node B is follower");
    } else {
        println!("  ✓ Node B is leader, Node A is follower");
    }

    // ========================================
    // PHASE 4: Verify the loser became Follower (additional validation)
    // ========================================
    println!("\nPHASE 4: Verify loser of CreateOnly race became Follower");
    println!("---------------------------------------------------------");

    let (leader_client, follower_client, leader_name, follower_name) = if a_is_leader {
        (&mut node_a_client, &mut node_b_client, "Node A", "Node B")
    } else {
        (&mut node_b_client, &mut node_a_client, "Node B", "Node A")
    };

    println!("  Writing events 10-12 to {} (leader)...", leader_name);
    for i in 10..=12 {
        write_event(leader_client, &aggregate_key, i, i == 10).await?;
    }
    println!("  ✓ Leader accepted writes");

    println!("  Waiting for replication to {} (follower)...", follower_name);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Try to read from follower to verify it's replicating
    use celeriant_msg::request::read_filters::ReadFilters;

    let read_req = Request::Read(celeriant_msg::request::requests::ReadRequest {
        correlation_id: Some(999),
        aggregate_key: aggregate_key.clone(),
        filters: ReadFilters::new(1),
    });

    let response = follower_client
        .send_request(&read_req, CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Read(read_resp) => {
            let total: usize = read_resp
                .event_batches
                .iter()
                .map(|b| b.events.len())
                .sum();
            println!("  {} (follower) has {} events", follower_name, total);
            assert!(total >= 3, "Follower should have replicated events from leader");
            println!("  ✓ {} successfully replicating as follower", follower_name);
        }
        other => {
            return Err(format!("Unexpected read response from follower: {:?}", other).into());
        }
    }

    println!("\n=== All Tests Passed ===");
    println!("Split-brain prevented: S3 CreateOnly race resolved to single leader\n");

    Ok(())
}
