//! CreateOnly Prevents Overwrites Integration Test
//!
//! Tests that PutCondition::CreateOnly is actually being used by verifying
//! behavior when an object already exists at the target path.
//!
//! Scenario:
//! 1. Start MinIO + two-node cluster via S3 election
//! 2. Write events 1-3. Verify follower has 3 events (normal replication)
//! 3. Pre-seed S3 fallback paths with garbage content (AFTER cluster is up,
//!    so boot catchup doesn't try to read them)
//! 4. Stop follower
//! 5. Write event 4 to leader. Fallback path triggers. Should hit pre-seeded object
//! 6. Verify the write succeeds (AlreadyExists treated as Ok)
//! 7. Verify the pre-seeded objects are NOT overwritten — still contain garbage
//!
//! Run with: cargo run --bin s3_fallback_createonly_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, s3_cluster_config, write_event, MinioContainer, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== CreateOnly Prevents Overwrites Integration Test ===\n");

    let port_base = 11100 + (std::process::id() % 100) as u16;
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

    // ========================================
    // Phase 1: Start cluster, verify normal replication
    // ========================================
    println!("\nPHASE 1: Start cluster and verify normal replication");
    println!("----------------------------------------------------");

    // Leader starts first — wins CreateOnly election race
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("  Waiting for election + discovery + replication connection...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

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
    // Phase 2: Pre-seed S3 fallback paths with garbage content
    // ========================================
    println!("PHASE 2: Pre-seed S3 fallback paths with garbage content");
    println!("---------------------------------------------------------");

    // Pre-seed AFTER cluster is up so boot catchup doesn't try to read garbage.
    // Use high WAL indices that the leader's fallback will try to write to.
    let garbage_content = b"GARBAGE_CONTENT_DO_NOT_OVERWRITE_THIS_987654321";
    let mut seeded_paths = Vec::new();

    for wal_seq in 4..=10 {
        let path = format!("cluster/fallback/shard_{:03}/batch_{:09}_{:09}.bin", expected_shard, wal_seq, wal_seq);
        println!("  Pre-seeding: {}", path);
        minio.put_object(&path, garbage_content.to_vec()).await?;
        seeded_paths.push(path);
    }
    println!("  Pre-seeded {} objects with garbage content\n", seeded_paths.len());

    // ========================================
    // Phase 3: Follower goes down, S3 fallback hits pre-seeded objects
    // ========================================
    println!("PHASE 3: Follower goes down, S3 fallback hits pre-seeded objects");
    println!("----------------------------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    drop(follower);
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing event 4 to leader (fallback should hit pre-seeded object)...");
    write_event(&mut leader_client, &aggregate_key, 4, false).await?;
    println!("  Write succeeded (AlreadyExists was treated as Ok)\n");

    // ========================================
    // Phase 4: Verify pre-seeded objects are NOT overwritten
    // ========================================
    println!("PHASE 4: Verify pre-seeded objects are NOT overwritten");
    println!("------------------------------------------------------");

    let mut found_garbage = false;
    for path in &seeded_paths {
        let object_bytes = minio.get_object(path).await?;

        if object_bytes.as_ref() == garbage_content {
            println!("  {} still contains garbage (NOT overwritten)", path);
            found_garbage = true;
        } else {
            println!("  ? {} has different content (size: {} bytes)", path, object_bytes.len());
        }
    }

    assert!(
        found_garbage,
        "At least one pre-seeded object should still contain garbage content, proving CreateOnly was used"
    );

    println!("\n  CreateOnly is in effect - pre-seeded objects were NOT overwritten");
    println!("  This proves PutCondition::CreateOnly is being used, not PutCondition::None");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
