//! S3 Leader Solo Integration Test - Follower slow to start
//!
//! Tests that a leader can operate alone, extending its S3 lease during the
//! discovery loop, writing to S3 fallback, and then a late-joining follower
//! catches up and resumes normal TCP replication.
//!
//! Scenario:
//! 1. Start MinIO, start ONLY the leader (no follower)
//! 2. Wait for election (leader wins CreateOnly on empty cluster)
//! 3. Write events 1-3 (S3 fallback since no follower)
//! 4. Verify S3 fallback objects exist
//! 5. Verify lease has been renewed (lease_index > 1, leader extended during discovery)
//! 6. Start follower (late start)
//! 7. Wait for follower registration + leader discovery + boot catchup + heartbeat
//! 8. Verify follower caught up from S3 (has events 1-3)
//! 9. Write events 4-6 (TCP replication, follower is connected)
//! 10. Verify both nodes have all 6 events, no new S3 fallback objects
//!
//! Run with: cargo run --bin s3_leader_solo_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{count_events, s3_cluster_config, write_event, MinioContainer, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Leader Solo Integration Test ===\n");

    let port_base = 12100 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-leader-solo").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);
    let expected_shard = (aggregate_key.aggregate_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", expected_shard);

    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    // ========================================
    // PHASE 1: Start ONLY the leader (no follower)
    // ========================================
    println!("PHASE 1: Start leader only (no follower)");
    println!("-----------------------------------------");

    println!("  Starting leader on port {}...", leader_port);
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;

    println!("  Waiting for election (leader wins CreateOnly on empty cluster)...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 2: Write events while solo (S3 fallback)
    // ========================================
    println!("\nPHASE 2: Write events while leader is solo");
    println!("-------------------------------------------");

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    println!("  Writing events 1-3 to leader (no follower, S3 fallback)...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }
    println!("  Leader accepted all 3 writes");

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // ========================================
    // PHASE 3: Verify S3 fallback objects
    // ========================================
    println!("\nPHASE 3: Verify S3 fallback objects exist");
    println!("------------------------------------------");

    let fallback_objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 fallback objects: {}", fallback_objects.len());
    for obj in &fallback_objects {
        println!("    - {}", obj);
    }
    assert!(
        !fallback_objects.is_empty(),
        "Expected S3 fallback objects (no follower to replicate to)"
    );
    println!("  S3 fallback working while leader is solo");

    // ========================================
    // PHASE 4: Verify lease has been renewed (discovery loop extends it)
    // ========================================
    println!("\nPHASE 4: Verify lease extended during discovery loop");
    println!("----------------------------------------------------");

    let lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let lease = deserialise_lease(&lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  lease_index={}, leader_node_id={:x}", lease.lease_index, lease.leader_node_id);
    assert!(
        lease.lease_index > 1,
        "lease_index should be > 1 (leader renews during discovery loop), got {}",
        lease.lease_index
    );
    println!("  Leader extended its lease during follower discovery loop");

    // ========================================
    // PHASE 5: Start follower (late join)
    // ========================================
    println!("\nPHASE 5: Start follower (late join)");
    println!("------------------------------------");

    println!("  Starting follower on port {}...", follower_port);
    let _follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("  Waiting for follower registration + leader discovery + boot catchup...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // ========================================
    // PHASE 6: Verify follower caught up from S3
    // ========================================
    println!("\nPHASE 6: Verify follower caught up from S3");
    println!("-------------------------------------------");

    let mut follower_client = CeleriantClient::connect(_follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower has {} events after boot catchup", follower_count);
    assert_eq!(
        follower_count, 3,
        "Follower should have all 3 events from S3 boot catchup, got {}",
        follower_count
    );
    println!("  Follower caught up from S3 fallback batches");

    // ========================================
    // PHASE 7: Write events via TCP replication (follower is connected)
    // ========================================
    println!("\nPHASE 7: Write events via TCP replication");
    println!("------------------------------------------");

    let objects_before = minio.list_objects(&shard_prefix).await?;
    let count_before = objects_before.len();

    println!("  Writing events 4-6 to leader (follower now connected)...");
    for i in 4..=6 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ========================================
    // PHASE 8: Verify both nodes have all events, no new S3 fallback
    // ========================================
    println!("\nPHASE 8: Verify complete sync, no new S3 fallback");
    println!("---------------------------------------------------");

    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Leader has {} events, follower has {} events", leader_count, follower_count);

    assert_eq!(leader_count, 6, "Leader should have 6 events");
    assert_eq!(follower_count, 6, "Follower should have 6 events");

    let objects_after = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects: {} before, {} after TCP replication", count_before, objects_after.len());
    assert_eq!(
        objects_after.len(), count_before,
        "No new S3 fallback objects should appear after follower joined"
    );
    println!("  Normal TCP replication resumed (zero S3 during normal operation)");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
