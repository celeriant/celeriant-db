//! Invariant 21: Clock Drift Validation on Replication
//!
//! NOTE: This invariant cannot be meaningfully tested at the integration level
//! because both nodes run on the same machine with the same system clock. Even with
//! max_clock_drift_ms=0, the follower-leader timestamp difference on loopback is
//! always <1ms (same clock, negligible network RTT).
//!
//! Testing this properly requires either:
//! - libfaketime or similar clock injection
//! - Running on separate machines with actual clock skew (Pi cluster)
//!
//! The invariant IS verified by a unit test in celeriant_shard/src/shard_wal.rs
//! (test_follower_rejects_stale_lease_and_clock_drift) which injects a timestamp
//! directly into the replication request.
//!
//! This integration test verifies the basic property: with normal clock drift
//! tolerance (500ms), replication works correctly on loopback.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{poll_converged_count, s3_cluster_config, write_event, MinioContainer, TestServer, FOLLOWER_CONVERGENCE_TIMEOUT};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Invariant: Clock Drift Validation ===\n");
    println!("NOTE: Full TimeDriftTooHigh rejection requires actual clock skew.");
    println!("      This test verifies normal replication works with default drift.\n");

    let port_base = 15500 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    let num_shards = 1;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    let minio = MinioContainer::start_with_bucket(minio_port, "test-clock-drift").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    println!("  Starting leader on port {}...", leader_port);
    let _leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("  Starting follower on port {}...", follower_port);
    let _follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("  Waiting for election + discovery...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut leader_client = CeleriantClient::connect(&format!("127.0.0.1:{}", leader_port)).await?;
    let mut follower_client = CeleriantClient::connect(&format!("127.0.0.1:{}", follower_port)).await?;

    println!("  Writing events 1-5...");
    for i in 1..=5 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }
    let follower_count =
        poll_converged_count(&mut follower_client, &aggregate_key, 5, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(follower_count, 5, "Follower should have 5 events via TCP");
    println!("  TCP replication with default drift tolerance: {} events", follower_count);

    println!("\n=== Test Passed ===");
    println!("Clock drift validation:");
    println!("  - Normal replication works with max_clock_drift_ms=500 (default)");
    println!("  - Full TimeDriftTooHigh rejection tested by unit test:");
    println!("    celeriant_shard/src/shard_wal.rs:test_follower_rejects_stale_lease_and_clock_drift\n");

    Ok(())
}
