//! P2-4 S3 Degraded-Mode Capacity Integration Test
//!
//! Tests S3 fallback handling of large volumes and follower catchup correctness.
//!
//! Scenario:
//! 1. Start 2-node cluster with S3
//! 2. Write events 1-5, verify normal replication
//! 3. Stop follower
//! 4. Write 100-200 events with 4-8KB payloads (sustained S3 fallback)
//! 5. Verify S3 has multiple batch objects
//! 6. Restart follower
//! 7. Wait for catchup (up to 30s)
//! 8. Verify follower has ALL events
//! 9. Write post-catchup event, verify normal replication
//!
//! Scaled down from spec's 100k+ events for CI performance.
//! 100-200 events × 4-8KB = 400KB-1.6MB, sufficient to create multiple S3 batches.
//!
//! Run with: cargo run --bin p2_4_s3_capacity_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, poll_event_count, s3_cluster_config, write_event, write_large_event, MinioContainer, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_fallback_batch;
use std::time::Duration;

const PORT_BASE: u16 = 19900;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P2-4 S3 Degraded-Mode Capacity Integration Test ===\n");

    let leader_port = PORT_BASE;
    let follower_port = PORT_BASE + 100;
    let minio_port = PORT_BASE + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);
    let expected_shard = (aggregate_key.aggregate_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", expected_shard);
    println!("  Expected shard: {} (prefix: {})", expected_shard, shard_prefix);

    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    // Leader starts first — wins CreateOnly election race
    println!("Starting two-node cluster...");
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let mut follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("Waiting for election + discovery + replication connection...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // ========================================
    // Phase 1: Normal replication (follower is up)
    // ========================================
    println!("PHASE 1: Normal replication with follower online");
    println!("------------------------------------------------");

    println!("  Writing events 1-5 to leader...");
    for i in 1..=5 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(
        follower_count, 5,
        "Follower should have 5 events after normal replication"
    );
    println!("  Follower has {} events\n", follower_count);

    // ========================================
    // Phase 2: Follower goes down, large-volume S3 fallback
    // ========================================
    println!("PHASE 2: Follower down, sustained S3 fallback (100-200 events × 4-8KB)");
    println!("----------------------------------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let num_fallback_events = 100u64;
    let payload_size = 4 * 1024; // 4KB per event

    println!(
        "  Writing {} events with {}KB payloads (total ~{}MB)...",
        num_fallback_events,
        payload_size / 1024,
        (num_fallback_events as usize * payload_size) / (1024 * 1024)
    );

    for i in 6..=(5 + num_fallback_events) {
        write_large_event(&mut leader_client, &aggregate_key, i, payload_size as usize).await?;
        if i % 50 == 0 {
            println!("    Written {} events...", i - 5);
        }
    }
    println!("  Leader writes succeeded (S3 fallback active)\n");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // ========================================
    // Phase 3: Verify S3 has multiple batch objects
    // ========================================
    println!("PHASE 3: Verify S3 fallback batches");
    println!("-----------------------------------");

    let objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects found: {}", objects.len());
    for obj in &objects {
        println!("    - {}", obj);
    }

    assert!(
        objects.len() >= 2,
        "Expected at least 2 S3 fallback batches for {}KB total, got {}",
        (num_fallback_events as usize * payload_size) / 1024,
        objects.len()
    );
    println!("  Multiple S3 batches created: {}", objects.len());

    // Verify first batch is valid
    let first_object_path = &objects[0];
    let object_bytes = minio.get_object(first_object_path).await?;
    let fallback_batch = deserialise_fallback_batch(&object_bytes)
        .map_err(|e| format!("deserialise fallback batch: {:?}", e))?;
    println!(
        "  Valid FallbackBatch: shard_id={}, fallback_index={}, items={}",
        fallback_batch.shard_id,
        fallback_batch.fallback_index,
        fallback_batch.items.len()
    );

    // ========================================
    // Phase 4: Follower restarts — boot catchup from S3
    // ========================================
    println!("\nPHASE 4: Follower restarts, boot catchup from S3");
    println!("-------------------------------------------------");

    println!("  Restarting follower...");
    follower.restart().await?;

    let expected_after_fallback = (5 + num_fallback_events) as usize;

    // Poll for boot catchup — 100 events × 4KB from S3 can take a while.
    println!("  Polling for boot catchup ({} events)...", expected_after_fallback);
    poll_event_count(
        follower.address(), &aggregate_key, expected_after_fallback, Duration::from_secs(60),
    ).await;

    // Reconnect leader client (may have timed out during long catchup)
    leader_client = CeleriantClient::connect(leader.address()).await?;

    // Write a few post-catchup events. First write may go via S3 (stale replication
    // connection), subsequent writes use a fresh TCP connection.
    println!("  Writing post-catchup events {} and {} to leader...",
        expected_after_fallback + 1, expected_after_fallback + 2);
    write_event(&mut leader_client, &aggregate_key, (expected_after_fallback + 1) as u64, false).await?;
    write_event(&mut leader_client, &aggregate_key, (expected_after_fallback + 2) as u64, false).await?;

    // ========================================
    // Phase 5: Verify follower caught up completely
    // ========================================
    println!("\nPHASE 5: Verify follower caught up with all events");
    println!("--------------------------------------------------");

    let follower_count = poll_event_count(
        follower.address(), &aggregate_key, expected_after_fallback + 2, Duration::from_secs(45),
    ).await;
    println!("  Follower now has {} events", follower_count);
    println!("  Catchup successful!");

    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    assert_eq!(
        leader_count,
        expected_after_fallback + 2,
        "Leader should have {} events",
        expected_after_fallback + 2
    );
    println!("  Leader has {} events", leader_count);

    // ========================================
    // Phase 6: Normal replication resumes
    // ========================================
    println!("\nPHASE 6: Normal replication resumes");
    println!("-----------------------------------");

    println!("  Writing final 3 events to leader (follower is online)...");
    for i in (expected_after_fallback + 3)..=(expected_after_fallback + 5) {
        write_event(&mut leader_client, &aggregate_key, i as u64, false).await?;
    }

    let follower_count = poll_event_count(
        follower.address(), &aggregate_key, expected_after_fallback + 5, Duration::from_secs(30),
    ).await;
    println!("  Follower has {} events", follower_count);

    println!("\n=== P2-4 Test Passed ===");
    println!("  - {} events with {}KB payloads written during fallback", num_fallback_events, payload_size / 1024);
    println!("  - {} S3 fallback batches created", objects.len());
    println!("  - Follower catchup successful");
    println!("  - Normal replication resumed\n");

    Ok(())
}
