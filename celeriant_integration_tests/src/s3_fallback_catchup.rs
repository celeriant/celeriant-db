//! S3 Fallback + Follower Catchup Integration Test
//!
//! Tests the full cycle: normal replication, follower goes down, S3 fallback,
//! follower comes back via boot catchup from S3, and continued normal replication.
//! Absorbs s3_fallback (S3 object verification + ordering assertions).
//!
//! Scenario:
//! 1. Start MinIO + two-node cluster via S3 election
//! 2. Write events 1-3. Verify follower has 3 events
//! 3. Stop follower
//! 4. Write events 4-8 to leader (S3 fallback for these)
//! 5. Verify S3 objects: correct paths, batch content, lexicographic ordering
//! 6. Restart follower — boot catchup reads S3 fallback batches
//! 7. Write event 9. Verify follower has all 9 events
//! 8. Write events 10-12. Verify follower gets them via normal replication
//!    (no new S3 objects)
//!
//! Invariants tested: 10 (post-election S3 catchup), 11 (S3 fallback)

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, s3_cluster_config, write_event, MinioContainer, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_fallback_batch;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Fallback + Follower Catchup Integration Test ===\n");

    let port_base = 10500 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

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

    println!("  Writing events 1-3 to leader...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(
        follower_count, 3,
        "Follower should have 3 events after normal replication"
    );
    println!("  Follower has {} events\n", follower_count);

    // ========================================
    // Phase 2: Follower goes down, S3 fallback
    // ========================================
    println!("PHASE 2: Follower goes down, leader falls back to S3");
    println!("----------------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing events 4-8 to leader (follower is down, S3 fallback)...");
    for i in 4..=8 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }
    println!("  Leader writes succeeded (S3 fallback active)\n");

    // ========================================
    // Phase 3: Verify S3 objects exist from fallback period (before follower consumes them)
    // ========================================
    println!("PHASE 3: Verify S3 fallback objects");
    println!("-----------------------------------");

    let objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects found: {}", objects.len());
    for obj in &objects {
        println!("    - {}", obj);
    }

    assert!(
        !objects.is_empty(),
        "Expected S3 objects from fallback period"
    );

    // Verify lexicographic ordering (from s3_fallback)
    let mut sorted = objects.clone();
    sorted.sort();
    assert_eq!(objects, sorted, "S3 objects should be in lexicographic (temporal) order");
    println!("  S3 objects are lexicographically ordered");

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

    assert_eq!(
        fallback_batch.shard_id, expected_shard,
        "FallbackBatch shard_id should match expected_shard"
    );
    assert!(
        !fallback_batch.items.is_empty(),
        "FallbackBatch should contain items"
    );
    let first_wal_index = fallback_batch.items[0].metablock.wal_index;
    assert_eq!(
        fallback_batch.fallback_index, first_wal_index,
        "fallback_index should match first item's WAL index"
    );
    println!("  Batch content verified: shard_id, item count, WAL index alignment");

    // ========================================
    // Phase 4: Follower restarts — boot catchup reads S3 fallback batches
    // ========================================
    println!("\nPHASE 4: Follower restarts, boot catchup from S3");
    println!("-------------------------------------------------");

    println!("  Restarting follower...");
    follower.restart().await?;

    // Wait for boot catchup + election + leader discovery + heartbeat
    println!("  Waiting for boot catchup + cluster rejoin...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    // After restart, follower's boot catchup applies S3 fallback batches.
    // WAL has events 1-3 from before crash. Boot catchup applies events 4-8 from S3.
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower has {} events after restart + boot catchup", follower_count);
    assert!(
        follower_count >= 3,
        "Follower should have at least 3 events (persisted WAL) after restart, got {}",
        follower_count
    );

    println!("  Writing event 9 to leader...");
    write_event(&mut leader_client, &aggregate_key, 9, false).await?;

    // ========================================
    // Phase 5: Verify follower caught up
    // ========================================
    println!("\nPHASE 5: Verify follower caught up");
    println!("----------------------------------");

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower now has {} events", follower_count);

    assert_eq!(
        follower_count, 9,
        "Follower should have all 9 events after catchup + replication"
    );
    println!("  Catchup successful! Follower has all 9 events");

    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    assert_eq!(leader_count, 9, "Leader should have 9 events");
    println!("  Leader has {} events", leader_count);

    // ========================================
    // Phase 6: Normal replication resumes (no new S3 objects)
    // ========================================
    println!("\nPHASE 6: Normal replication resumes (no new S3 objects)");
    println!("-------------------------------------------------------");

    // Check no new S3 fallback objects were created (TCP replication handled events 10-12)
    let objects_before_phase6 = minio.list_objects(&shard_prefix).await?;
    // Note: boot catchup may have consumed the original fallback objects, so count could be 0
    let count_before = objects_before_phase6.len();

    println!("  Writing events 10-12 to leader (follower is online)...");
    for i in 10..=12 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(
        follower_count, 12,
        "Follower should have 12 events after normal replication"
    );
    println!("  Follower has {} events", follower_count);

    let objects_after = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects: {} before, {} after", count_before, objects_after.len());

    assert_eq!(
        objects_after.len(),
        count_before,
        "No new S3 objects should appear after follower is back online"
    );
    println!("  No new S3 objects created (normal replication resumed)");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
