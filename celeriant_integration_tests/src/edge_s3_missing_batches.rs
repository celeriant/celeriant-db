//! Edge Case: Missing S3 Batches / Gap Detection
//!
//! Tests that the follower's S3 catchup correctly detects a gap when one or more
//! fallback batches are absent from S3.
//!
//! Scenario:
//! 1. Start MinIO + two-node cluster (S3 enabled, small batch size to generate many batches)
//! 2. Verify roles
//! 3. Stop follower
//! 4. Write ~50 large events to leader — enough to produce 5+ S3 fallback batches
//! 5. Wait for S3 fallback writes to complete
//! 6. Delete the middle batch from S3, creating a gap
//! 7. Restart follower — catchup will encounter the gap
//! 8. Verify: follower exits cleanly (status 0) — WalIndexGap is a fatal
//!    non-retriable error that triggers graceful shutdown, not a crash
//!
//! This is test #5 in the integration test coverage report.
//!
//! Run with: cargo run --bin edge_s3_missing_batches_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, is_leader, s3_cluster_config, write_event, write_large_event, MinioContainer,
    TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: Missing S3 Batches / Gap Detection ===\n");

    let port_base = 14300 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-missing-batches").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 2;
    // Route writes to a known shard: aggregate_type_id=1, shard = 1 % 2 = 1
    let aggregate_key = AggregateKey::new(1, 1, 1);
    let expected_shard = (aggregate_key.aggregate_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", expected_shard);

    let base_config = s3_cluster_config(
        num_shards,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );

    // Small preallocate (2MB) avoids large WAL files in CI.
    let config = crate::ServerConfig {
        shard_log_preallocate_bytes: 2 * 1024 * 1024,
        ..base_config
    };

    println!("Starting leader on port {}...", leader_port);
    let leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into())
            .await?;
    println!("Starting follower on port {}...", follower_port);
    let mut follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into())
            .await?;

    println!(
        "  Leader at {}, Follower at {}",
        leader.address(),
        follower.address()
    );

    println!("Waiting for cluster stabilization (8s)...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Verify expected roles — leader started first, wins CreateOnly election.
    let leader_is_leader = is_leader(leader.address()).await?;
    let follower_is_leader = is_leader(follower.address()).await?;
    assert!(
        leader_is_leader && !follower_is_leader,
        "Unexpected role assignment: leader_is_leader={}, follower_is_leader={}",
        leader_is_leader,
        follower_is_leader
    );
    println!("  Role check passed: leader accepted writes, follower rejected writes\n");

    // ========================================
    // Phase 1: Normal replication (follower up) — create the aggregate
    // ========================================
    println!("PHASE 1: Create aggregate with follower online");
    println!("----------------------------------------------");

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    write_event(&mut leader_client, &aggregate_key, 1, true).await?;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let initial_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(
        initial_count, 1,
        "Follower should have 1 event after initial replication, got {}",
        initial_count
    );
    println!("  Follower has {} event(s) — replication working\n", initial_count);

    // ========================================
    // Phase 2: Stop follower, write many large events to trigger S3 fallback
    // ========================================
    println!("PHASE 2: Stop follower, write large events to generate multiple S3 batches");
    println!("---------------------------------------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // With 32KB batch limit: ~8 events × 4KB = 32KB per batch.
    // Write 50 events => ~6 batches. Keep first event (already written) in mind.
    println!("  Writing 50 large events (4KB each) to leader...");
    for i in 2u64..=51 {
        write_large_event(&mut leader_client, &aggregate_key, i, 4096).await?;
        if i % 10 == 0 {
            println!("    {} events written...", i - 1);
        }
    }
    println!("  Writes complete (50 events)\n");

    // Wait for S3 fallback writes to land.
    println!("  Waiting 5s for S3 fallback writes to complete...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // Phase 3: Verify S3 batches exist, then delete a middle batch
    // ========================================
    println!("PHASE 3: Inspect S3 batches and delete a middle batch");
    println!("------------------------------------------------------");

    let mut objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 fallback objects found: {}", objects.len());
    for obj in &objects {
        println!("    - {}", obj);
    }

    assert!(
        objects.len() >= 3,
        "Expected at least 3 S3 batches to create a meaningful gap, got {}. \
         The 32KB batch limit may need adjustment or more events.",
        objects.len()
    );

    // Sort by path — lexicographic order matches WAL index order for zero-padded names.
    objects.sort();

    // Delete multiple consecutive batches from the middle to create a multi-batch gap.
    let mid = objects.len() / 2;
    let delete_start = mid;
    let delete_end = (mid + 2).min(objects.len() - 1); // keep at least the last batch
    for i in delete_start..delete_end {
        println!("  Deleting batch: {}", objects[i]);
        minio.delete_object(&objects[i]).await?;
    }
    println!("  Deleted {} batch(es) to create gap", delete_end - delete_start);

    let remaining = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects remaining after deletion: {}", remaining.len());
    assert!(
        remaining.len() < objects.len(),
        "Deletion did not reduce object count"
    );
    println!();

    // ========================================
    // Phase 4: Restart follower — S3 catchup encounters the gap
    // ========================================
    println!("PHASE 4: Restart follower — S3 catchup should encounter gap");
    println!("-------------------------------------------------------------");

    println!("  Restarting follower...");
    follower.restart().await?;

    // Give the follower time to attempt catchup (and encounter the gap).
    println!("  Waiting 15s for catchup attempt...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    // ========================================
    // Phase 5: Verify gap was detected — follower shuts down gracefully
    // ========================================
    println!("PHASE 5: Verify gap was detected");
    println!("---------------------------------");

    // WalIndexGap is a fatal (non-retriable) S3 catchup error. The server
    // treats it as a data integrity concern and performs a graceful shutdown
    // (exit status 0), NOT a panic.
    match follower.check_alive() {
        Ok(()) => panic!(
            "Follower should have exited after encountering WAL index gap, but it is still running"
        ),
        Err(e) => {
            println!("  Follower exited as expected: {}", e);
            assert!(
                e.contains("exit status: 0") || e.contains("status: 0"),
                "Follower should exit cleanly (status 0), not crash: {}",
                e
            );
        }
    }
    println!("  Follower exited cleanly (status 0) — gap detected, graceful shutdown");

    // Leader must still have all events (unaffected by follower's gap).
    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    println!("  Leader has {} total events (unaffected by follower gap)", leader_count);
    assert_eq!(leader_count, 51, "Leader should have all 51 events (1 initial + 50 large)");

    println!("\n=== PASS ===\n");

    Ok(())
}
