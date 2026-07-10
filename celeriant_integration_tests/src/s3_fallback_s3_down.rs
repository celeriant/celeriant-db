//! S3 Goes Down Integration Test
//!
//! Tests that when S3 is initially available (for election), then goes down,
//! and then the follower goes down, writes are rolled back.
//!
//! Scenario:
//! 1. Start MinIO + two-node cluster via S3 election
//! 2. Write events 1-3. Verify follower has 3 events (normal replication)
//! 3. Pause MinIO (S3 becomes unreachable)
//! 4. Stop follower
//! 5. Write event 4 to leader. Follower down, S3 unreachable — fallback fails
//! 6. Verify the write is rejected — client receives error
//!
//! Run with: cargo run --bin s3_fallback_s3_down_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{poll_converged_count, s3_cluster_config, write_event, MinioContainer, TestServer, FOLLOWER_CONVERGENCE_TIMEOUT};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Goes Down Integration Test ===\n");

    let port_base = 10900 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    let num_shards = 4;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-s3down").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    // Leader starts first — wins CreateOnly election race
    println!("Starting two-node cluster...");
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("Waiting for election + discovery + replication connection...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let aggregate_key = AggregateKey::new(1, 1, 1);

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
    let follower_count =
        poll_converged_count(&mut follower_client, &aggregate_key, 3, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(
        follower_count, 3,
        "Follower should have 3 events after normal replication"
    );
    println!("  Follower has {} events\n", follower_count);

    // ========================================
    // Phase 2: S3 goes down
    // ========================================
    println!("PHASE 2: S3 goes down (pause MinIO)");
    println!("-----------------------------------");

    println!("  Pausing MinIO container...");
    minio.pause()?;
    println!("  MinIO paused (S3 now unreachable)\n");

    // ========================================
    // Phase 3: Follower goes down, S3 unreachable
    // ========================================
    println!("PHASE 3: Follower goes down, S3 unreachable");
    println!("-------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    drop(follower);
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing event 4 to leader (should be REJECTED - no follower, S3 unreachable)...");
    let write_result = write_event(&mut leader_client, &aggregate_key, 4, false).await;

    match write_result {
        Ok(_) => {
            return Err("Write should have failed but succeeded!".into());
        }
        Err(e) => {
            println!("  Write was rejected as expected: {}", e);
        }
    }

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
