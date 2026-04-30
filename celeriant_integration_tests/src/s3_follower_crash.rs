//! S3 Lease Integration Test - Follower crash + leader self-heal
//!
//! Tests the follower crash failure mode: leader detects heartbeat loss,
//! pre-renews its S3 lease without fencing (asymmetric behavior), continues
//! serving writes. Same-leader renewal keeps lease_index unchanged but
//! advances expires_at_ms. Then follower restarts and rejoins.
//!
//! Scenario:
//! 1. Start MinIO, establish two-node cluster (leader + follower)
//! 2. Write events 1-3, verify cluster is healthy (replication works)
//! 3. Read initial lease from S3, record lease_index
//! 4. Kill follower process (simulate crash)
//! 5. Wait for leader to detect heartbeat loss and self-heal via S3 pre-renewal
//! 6. Verify leader still accepts writes, lease_index unchanged (same leader)
//! 7. Verify S3 fallback batches carry the same lease_index
//! 8. Restart follower process
//! 9. Wait for follower to re-register and rejoin cluster
//! 10. Verify follower receives replicated data from leader
//!
//! Run with: cargo run --bin s3_follower_crash_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, poll_event_count, s3_cluster_config, write_event, MinioContainer, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::{deserialise_fallback_batch, deserialise_lease};
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Lease Follower Crash Integration Test ===\n");

    let port_base = 11900 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-follower-crash").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);
    let expected_shard = (aggregate_key.aggregate_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", expected_shard);

    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    // ========================================
    // PHASE 1: Start cluster and establish leader/follower
    // ========================================
    println!("PHASE 1: Start cluster and establish leader/follower");
    println!("-----------------------------------------------------");

    // Leader starts first — wins CreateOnly election race
    println!("  Starting leader on port {}...", leader_port);
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    println!("  Starting follower on port {}...", follower_port);
    let mut follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("  Waiting for election + discovery + heartbeat establishment...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    println!("  Writing events 1-3 to verify cluster health...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(follower_count, 3, "Follower should have 3 events");
    println!("  Cluster healthy: follower has {} events\n", follower_count);

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

    assert!(initial_lease.lease_index >= 1, "Initial lease_index should be >= 1");
    println!("  Recorded initial state\n");

    // ========================================
    // PHASE 3: Kill follower (simulate crash)
    // ========================================
    println!("PHASE 3: Kill follower (simulate crash)");
    println!("----------------------------------------");

    println!("  Stopping follower process...");
    drop(follower_client);
    follower.stop();
    println!("  Follower stopped\n");

    // ========================================
    // PHASE 4: Wait for leader self-heal
    // ========================================
    println!("PHASE 4: Wait for leader self-heal");
    println!("----------------------------------");

    println!("  Waiting for leader to detect heartbeat loss and pre-renew S3 lease...");
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
    println!("  Leader accepted writes after self-heal");

    let new_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let new_lease = deserialise_lease(&new_lease_bytes)
        .map_err(|e| format!("Failed to deserialise new lease: {:?}", e))?;

    println!("  New lease: leader_node_id={:x}, lease_index={}",
        new_lease.leader_node_id, new_lease.lease_index);

    assert_eq!(
        new_lease.lease_index, initial_lease.lease_index,
        "lease_index should NOT change on same-leader self-heal: was {}, now {}",
        initial_lease.lease_index, new_lease.lease_index
    );
    assert_eq!(
        new_lease.leader_node_id, initial_lease.leader_node_id,
        "leader_node_id should NOT have changed (same leader self-healed)"
    );
    assert!(
        new_lease.expires_at_ms > initial_lease.expires_at_ms,
        "expires_at_ms should advance on self-heal: was {}, now {}",
        initial_lease.expires_at_ms, new_lease.expires_at_ms
    );
    println!("  Lease renewed: lease_index unchanged at {}, expires_at_ms advanced", new_lease.lease_index);

    // ========================================
    // PHASE 5.5: Verify S3 fallback batches carry new lease_index
    // ========================================
    println!("\nPHASE 5.5: Verify S3 fallback batches carry new lease_index");
    println!("------------------------------------------------------------");

    let fallback_objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 fallback objects: {}", fallback_objects.len());
    assert!(!fallback_objects.is_empty(), "Expected S3 fallback objects after follower crash");

    let last_object = &fallback_objects[fallback_objects.len() - 1];
    let batch_bytes = minio.get_object(last_object).await?;
    let batch = deserialise_fallback_batch(&batch_bytes)
        .map_err(|e| format!("Failed to deserialise fallback batch: {:?}", e))?;

    let batch_lease_index = batch.items[0].metablock.lease_index;
    println!("  Fallback batch lease_index={}, initial lease_index={}",
        batch_lease_index, initial_lease.lease_index);
    assert_eq!(
        batch_lease_index, initial_lease.lease_index,
        "S3 fallback batch lease_index ({}) should equal initial ({}) — same leader, same term",
        batch_lease_index, initial_lease.lease_index
    );
    println!("  lease_index correctly stamped on S3 fallback batches\n");

    // ========================================
    // PHASE 6: Restart follower
    // ========================================
    println!("PHASE 6: Restart follower");
    println!("-------------------------");

    println!("  Restarting follower...");
    follower.restart().await?;
    println!("  Follower process restarted");

    // Wait for follower to catch up with events 1-5 (3 persisted + 2 from S3 catchup)
    println!("  Polling for follower catchup (events 1-5)...");
    poll_event_count(follower.address(), &aggregate_key, 5, Duration::from_secs(30)).await;

    // ========================================
    // PHASE 7: Verify follower rejoined and receives new writes
    // ========================================
    println!("\nPHASE 7: Verify follower rejoined and receives new writes");
    println!("----------------------------------------------------------");

    println!("  Writing events 6-7 to leader (after follower restart)...");
    write_event(&mut leader_client, &aggregate_key, 6, false).await?;
    write_event(&mut leader_client, &aggregate_key, 7, false).await?;

    let final_follower_count = poll_event_count(
        follower.address(), &aggregate_key, 7, Duration::from_secs(15),
    ).await;
    println!("  Restarted follower has {} events", final_follower_count);
    println!("  Follower rejoined and has all events");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
