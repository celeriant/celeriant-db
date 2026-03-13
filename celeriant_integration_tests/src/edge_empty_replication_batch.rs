//! Edge Case: Empty Replication Batch
//!
//! Tests that multiple heartbeat + replication cycles with no writes do not
//! produce any state inconsistency. Verifies the `NoCaptureRaceButOk` code
//! path in the replication loop handles empty batches cleanly.
//!
//! Scenario:
//! 1. Start two-node cluster (leader + follower)
//! 2. Wait 10s with NO writes (multiple empty replication cycles)
//! 3. Write 1 event to leader
//! 4. Verify follower receives it
//! 5. Verify no S3 fallback objects created (empty cycles must not generate fallback)
//!
//! This is a regression guard for test #18 in the integration test coverage report.
//!
//! Run with: cargo run --bin edge_empty_replication_batch_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, is_leader, s3_cluster_config, write_event, MinioContainer, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: Empty Replication Batch ===\n");

    let port_base = 13900 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-empty-repl").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let config = s3_cluster_config(
        2,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );

    println!("Starting leader on port {}...", leader_port);
    let leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into())
            .await?;
    println!("Starting follower on port {}...", follower_port);
    let follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!(
        "  Leader at {}, Follower at {}",
        leader.address(),
        follower.address()
    );

    println!("Waiting for cluster stabilization (8s)...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Confirm expected roles — leader starts first and wins the CreateOnly race.
    let leader_is_leader = is_leader(leader.address()).await?;
    let follower_is_leader = is_leader(follower.address()).await?;

    assert!(
        leader_is_leader && !follower_is_leader,
        "Unexpected role assignment. leader_is_leader={}, follower_is_leader={}",
        leader_is_leader,
        follower_is_leader
    );
    println!("  Role check passed: leader accepted writes, follower rejected writes");

    // ========================================
    // Phase 1: Let empty replication cycles run
    // ========================================
    println!("\nPHASE 1: Waiting 10s with no writes (multiple empty replication cycles)");
    println!("-------------------------------------------------------------------------");
    tokio::time::sleep(Duration::from_secs(10)).await;
    println!("  Idle period complete — no writes issued");

    // ========================================
    // Phase 2: Write 1 event to leader
    // ========================================
    println!("\nPHASE 2: Write 1 event to leader");
    println!("----------------------------------");

    // Use type_id=1, category_id=1 — routes to shard 1 % 2 = shard 1
    let key = AggregateKey::new(1, 1, 1);
    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    write_event(&mut leader_client, &key, 1, true).await.map_err(|e| {
        format!("ERROR: Failed to write event to leader: {}", e)
    })?;
    println!("  Write succeeded");

    // ========================================
    // Phase 3: Verify follower received the event
    // ========================================
    println!("\nPHASE 3: Verify follower received the event");
    println!("--------------------------------------------");

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let count = count_events(&mut follower_client, &key).await.map_err(|e| {
        format!("ERROR: Failed to count events on follower: {}", e)
    })?;

    assert_eq!(count, 1, "Follower should have 1 event, but has {}", count);
    println!("  Follower has {} event(s) — correct", count);

    // Verify leader also has the event
    let leader_count = count_events(&mut leader_client, &key).await.map_err(|e| {
        format!("ERROR: Failed to count events on leader: {}", e)
    })?;
    assert_eq!(leader_count, 1, "Leader should have 1 event, but has {}", leader_count);
    println!("  Leader has {} event(s) — correct", leader_count);

    // ========================================
    // Phase 4: Verify no S3 fallback objects created
    // ========================================
    println!("\nPHASE 4: Verify no S3 fallback objects created");
    println!("------------------------------------------------");

    let fallback_objects = minio.list_objects("cluster/fallback/").await?;
    assert!(
        fallback_objects.is_empty(),
        "S3 fallback objects found when follower was always active: {:?}",
        fallback_objects
    );
    println!("  No S3 fallback objects — empty cycles did not trigger fallback");

    println!("\n=== PASS ===\n");

    Ok(())
}
