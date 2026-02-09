//! Replication Catchup Integration Tests
//!
//! Tests the flexible replication catchup feature that allows the leader to
//! send missing WAL entries when a follower falls behind.
//!
//! Scenario:
//! 1. Leader and follower start together, initial writes replicate normally
//! 2. Follower is stopped
//! 3. Leader continues writing (falls back to S3, continues without follower)
//! 4. Follower restarts
//! 5. Next write triggers WalIndexMismatch, leader catches up follower
//! 6. Verify follower has all events
//!
//! Requires Docker for MinIO.
//! Run with: cargo run --bin replication_catchup_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    count_events, write_event, MinioContainer, ServerConfig, TestServer,
};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Replication Catchup Integration Tests ===\n");

    let port = 10300 + (std::process::id() % 100) as u16;
    let follower_port = port + 100;
    let minio_port = port + 10;

    // Start MinIO for S3 fallback replication
    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-catchup").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) =
        minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    // Start follower first
    let follower_config = ServerConfig {
        log_level: "info".to_string(),
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region.clone()),
        s3_bucket: Some(bucket_name.clone()),
        s3_access_key_id: Some(access_key.clone()),
        s3_secret_access_key: Some(secret_key.clone()),
        s3_endpoint_override: Some(minio_endpoint.clone()),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!("Starting follower on port {}...", follower_port);
    let mut follower = TestServer::start_with_config(follower_port, follower_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start leader with follower address
    let follower_replication_port = follower_port + 1;
    let leader_config = ServerConfig {
        log_level: "info".to_string(),
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket_name),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
        s3_endpoint_override: Some(minio_endpoint),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!(
        "Starting leader on port {} (replicating to 127.0.0.1:{})...",
        port, follower_replication_port
    );
    let leader = TestServer::start_with_config(port, leader_config).await?;

    println!(
        "Cluster started: leader at {}, follower at {}\n",
        leader.address(),
        follower.address()
    );

    // Wait for S3 election to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

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

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(
        follower_count, 3,
        "Follower should have 3 events after normal replication"
    );
    println!("  ✓ Follower has {} events\n", follower_count);

    // ========================================
    // Phase 2: Follower goes down, leader continues (S3 fallback)
    // ========================================
    println!("PHASE 2: Follower goes down, leader continues writing");
    println!("------------------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing events 4-6 to leader (follower is down, S3 fallback)...");
    for i in 4..=6 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }
    println!("  ✓ Leader continued writing while follower was down\n");

    // ========================================
    // Phase 3: Follower restarts, catchup happens
    // ========================================
    println!("PHASE 3: Follower restarts, catchup on next write");
    println!("-------------------------------------------------");

    println!("  Restarting follower...");
    follower.restart().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!(
        "  Follower currently has {} events (behind leader)",
        follower_count
    );
    assert_eq!(
        follower_count, 3,
        "Follower should still have only 3 events after restart"
    );

    println!("  Writing event 7 to leader (should trigger catchup)...");
    write_event(&mut leader_client, &aggregate_key, 7, false).await?;

    println!("  Waiting for catchup replication...");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // ========================================
    // Phase 4: Verify catchup worked
    // ========================================
    println!("\nPHASE 4: Verify follower caught up");
    println!("----------------------------------");

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower now has {} events", follower_count);

    if follower_count == 7 {
        println!("  ✓ Catchup successful! Follower has all 7 events");
    } else {
        println!(
            "  ✗ Catchup failed! Expected 7 events, got {}",
            follower_count
        );
        return Err(
            format!("Catchup verification failed: expected 7, got {}", follower_count).into(),
        );
    }

    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    assert_eq!(leader_count, 7, "Leader should have 7 events");
    println!("  ✓ Leader has {} events", leader_count);

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
