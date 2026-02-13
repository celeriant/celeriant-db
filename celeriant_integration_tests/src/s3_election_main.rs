//! S3 Lease Election Integration Test
//!
//! Tests cold-start S3 election, follower discovery, and TCP replication.
//!
//! 1. Start MinIO + two nodes concurrently — one wins CreateOnly race
//! 2. Probe both to discover who is leader (who accepts writes)
//! 3. Write events via leader, verify follower has them via TCP replication
//! 4. Verify lease.bin and membership.bin in S3
//! 5. Verify NO S3 fallback data (follower is active, TCP replication only)
//!
//! Run with: cargo run --bin s3_election_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{count_events, write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::{deserialise_lease, deserialise_membership};
use std::time::Duration;

fn s3_cluster_config(
    num_shards: usize,
    region: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    endpoint: &str,
    allow_http: bool,
) -> ServerConfig {
    ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        routing_rule: RoutingRule::AggregateTypeId,
        // 1hr TTL — no heartbeat yet, status must not self-fence during test
        heartbeat_lease_duration_ms: 3_600_000,
        s3_enabled: true,
        s3_region: Some(region.to_string()),
        s3_bucket: Some(bucket.to_string()),
        s3_access_key_id: Some(access_key.to_string()),
        s3_secret_access_key: Some(secret_key.to_string()),
        s3_endpoint_override: Some(endpoint.to_string()),
        s3_allow_http: allow_http,
        ..Default::default()
    }
}

/// Try writing a probe event to determine if this node is the leader.
/// Returns true if the write was accepted.
async fn is_leader(address: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let probe_key = AggregateKey::new(999, 999, 999);
    let mut client = CeleriantClient::connect(address).await?;
    match write_event(&mut client, &probe_key, 1, true).await {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Lease Election Integration Test ===\n");

    let port_base = 11300 + (std::process::id() % 100) as u16;
    let port_a = port_base;
    let port_b = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-election").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;
    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    // Start both nodes — one will win the CreateOnly race
    println!("Starting two nodes concurrently...");
    let node_a = TestServer::start_with_config_labeled(port_a, config.clone(), "node-a".into()).await?;
    let node_b = TestServer::start_with_config_labeled(port_b, config, "node-b".into()).await?;
    println!("  Node A at {}, Node B at {}", node_a.address(), node_b.address());

    // Wait for: S3 catchup (no-op) + election + discovery loop + TCP connection
    println!("Waiting for election + discovery + replication connection...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // ========================================
    // Phase 1: Discover who won the election
    // ========================================
    println!("\nPHASE 1: Discover leader");
    println!("------------------------");

    let a_is_leader = is_leader(node_a.address()).await?;
    let b_is_leader = is_leader(node_b.address()).await?;

    println!("  Node A is_leader: {}", a_is_leader);
    println!("  Node B is_leader: {}", b_is_leader);

    assert!(
        a_is_leader ^ b_is_leader,
        "Exactly one node should be leader (a={}, b={})",
        a_is_leader, b_is_leader
    );

    let (leader_addr, follower_addr) = if a_is_leader {
        println!("  Node A won election");
        (node_a.address(), node_b.address())
    } else {
        println!("  Node B won election");
        (node_b.address(), node_a.address())
    };

    // ========================================
    // Phase 2: Write events via leader
    // ========================================
    println!("\nPHASE 2: Write events to leader");
    println!("-------------------------------");

    let aggregate_key = AggregateKey::new(1, 1, 1);
    let mut leader_client = CeleriantClient::connect(leader_addr).await?;

    for i in 1..=5 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }
    println!("  Wrote 5 events to leader");

    // ========================================
    // Phase 3: Verify follower rejects writes
    // ========================================
    println!("\nPHASE 3: Verify follower rejects writes");
    println!("----------------------------------------");

    let mut follower_client = CeleriantClient::connect(follower_addr).await?;
    let write_result = write_event(&mut follower_client, &aggregate_key, 99, false).await;
    assert!(write_result.is_err(), "Follower must reject writes");
    println!("  Follower rejected write");

    // ========================================
    // Phase 4: Verify replication (data readable on both nodes)
    // ========================================
    println!("\nPHASE 4: Verify replication");
    println!("---------------------------");

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    assert_eq!(leader_count, 5, "Leader should have 5 events");
    println!("  Leader has {} events", leader_count);

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(follower_count, 5, "Follower should have 5 events via TCP replication");
    println!("  Follower has {} events", follower_count);

    // ========================================
    // Phase 5: Verify S3 lease and membership
    // ========================================
    println!("\nPHASE 5: Verify S3 control plane state");
    println!("---------------------------------------");

    let lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let lease = deserialise_lease(&lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  lease_index={}, leader_node_id={:x}", lease.lease_index, lease.leader_node_id);
    assert_eq!(lease.lease_index, 1, "Initial election should produce lease_index=1");
    assert_ne!(lease.leader_node_id, 0, "leader_node_id must be set");

    let membership_bytes = minio.get_object("cluster/membership.bin").await?;
    let membership = deserialise_membership(&membership_bytes)
        .map_err(|e| format!("Failed to deserialise membership: {:?}", e))?;

    println!("  membership node_count={}", membership.node_count());
    assert!(membership.is_fully_replicated(), "Both nodes should be registered in membership");

    // ========================================
    // Phase 6: No S3 fallback data (TCP replication only)
    // ========================================
    println!("\nPHASE 6: Verify no S3 fallback data");
    println!("------------------------------------");

    let fallback_objects = minio.list_objects("cluster/fallback/").await?;
    assert!(
        fallback_objects.is_empty(),
        "No S3 fallback objects should exist — follower is active, TCP replication only. Found: {:?}",
        fallback_objects
    );
    println!("  No S3 fallback objects (TCP replication only)");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
