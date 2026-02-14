//! S3 Lease Election Integration Test
//!
//! Tests cold-start S3 election, split-brain prevention, multi-shard replication,
//! and S3 control plane state.
//!
//! 1. Start MinIO + two nodes concurrently — one wins CreateOnly race
//! 2. Probe both to discover who is leader (who accepts writes)
//! 3. Verify exactly one leader (split-brain prevention via S3 CreateOnly)
//! 4. Write events via leader to multiple shards, verify follower has them
//! 5. Verify follower rejects writes
//! 6. Verify lease.bin and membership.bin in S3
//! 7. Verify NO S3 fallback data (follower is active, TCP replication only)
//!
//! Run with: cargo run --bin s3_election_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{count_events, is_leader, s3_cluster_config, write_event, MinioContainer, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::{deserialise_lease, deserialise_membership};
use std::time::Duration;

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
    // Phase 1: Discover who won the election (split-brain prevention)
    // ========================================
    println!("\nPHASE 1: Discover leader (split-brain prevention)");
    println!("-------------------------------------------------");

    let a_is_leader = is_leader(node_a.address()).await?;
    let b_is_leader = is_leader(node_b.address()).await?;

    println!("  Node A is_leader: {}", a_is_leader);
    println!("  Node B is_leader: {}", b_is_leader);

    assert!(
        a_is_leader ^ b_is_leader,
        "Exactly one node should be leader — S3 CreateOnly prevents split-brain (a={}, b={})",
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
    // Phase 2: Write events to multiple shards via leader
    // ========================================
    println!("\nPHASE 2: Write events to multiple shards");
    println!("-----------------------------------------");

    // Two aggregate keys that route to different shards (AggregateTypeId routing: shard = type_id % num_shards)
    let key_shard_1 = AggregateKey::new(1, 1, 1); // 1 % 4 = shard 1
    let key_shard_2 = AggregateKey::new(1, 2, 1); // 2 % 4 = shard 2

    let mut leader_client = CeleriantClient::connect(leader_addr).await?;

    for i in 1..=3 {
        write_event(&mut leader_client, &key_shard_1, i, i == 1).await?;
        write_event(&mut leader_client, &key_shard_2, i, i == 1).await?;
    }
    println!("  Wrote 3 events to shard 1, 3 events to shard 2");

    // ========================================
    // Phase 3: Verify follower rejects writes
    // ========================================
    println!("\nPHASE 3: Verify follower rejects writes");
    println!("----------------------------------------");

    let mut follower_client = CeleriantClient::connect(follower_addr).await?;
    let write_result = write_event(&mut follower_client, &key_shard_1, 99, false).await;
    assert!(write_result.is_err(), "Follower must reject writes");
    println!("  Follower rejected write");

    // ========================================
    // Phase 4: Verify multi-shard replication
    // ========================================
    println!("\nPHASE 4: Verify multi-shard replication");
    println!("---------------------------------------");

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let leader_count_s1 = count_events(&mut leader_client, &key_shard_1).await?;
    let leader_count_s2 = count_events(&mut leader_client, &key_shard_2).await?;
    assert_eq!(leader_count_s1, 3, "Leader shard 1 should have 3 events");
    assert_eq!(leader_count_s2, 3, "Leader shard 2 should have 3 events");
    println!("  Leader: shard 1 = {} events, shard 2 = {} events", leader_count_s1, leader_count_s2);

    let follower_count_s1 = count_events(&mut follower_client, &key_shard_1).await?;
    let follower_count_s2 = count_events(&mut follower_client, &key_shard_2).await?;
    assert_eq!(follower_count_s1, 3, "Follower shard 1 should have 3 events via TCP replication");
    assert_eq!(follower_count_s2, 3, "Follower shard 2 should have 3 events via TCP replication");
    println!("  Follower: shard 1 = {} events, shard 2 = {} events", follower_count_s1, follower_count_s2);

    // ========================================
    // Phase 5: Verify S3 lease and membership
    // ========================================
    println!("\nPHASE 5: Verify S3 control plane state");
    println!("---------------------------------------");

    let lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let lease = deserialise_lease(&lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  lease_index={}, leader_node_id={:x}", lease.lease_index, lease.leader_node_id);
    assert!(lease.lease_index >= 1, "Election should produce lease_index >= 1 (got {})", lease.lease_index);
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
