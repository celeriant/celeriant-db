//! S3 Failover Latency Test - Measures time from leader crash to follower accepting writes
//!
//! With production defaults (heartbeat_lease_duration_ms=1500, max_clock_drift=500ms),
//! failover gap = heartbeat_lease_duration + max_clock_drift + S3 CAS ≈ 2s + S3 round-trip.
//! This test asserts the total is under 3 seconds.
//!
//! Uses the production default heartbeat_lease_duration_ms (1500ms) rather than the
//! inflated value (10s) in s3_cluster_config. The initial S3 lease written at election
//! has the same TTL, so we wait long enough for it to go stale and for the system to
//! be running purely on heartbeat-refreshed TTLs before killing the leader.
//!
//! Run with: cargo run --bin s3_failover_latency_main -p celeriant_integration_tests --release

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, write_event, MinioContainer, ServerConfig, TestServer,
};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::{Duration, Instant};

const MAX_FAILOVER: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const POLL_TIMEOUT: Duration = Duration::from_secs(15);


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Failover Latency Test ===\n");

    let port_base = 11500 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    let minio = MinioContainer::start_with_bucket(minio_port, "test-failover-latency").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // Production-realistic defaults: heartbeat_lease_duration=1500ms, heartbeat_interval=500ms,
    // max_clock_drift=500ms. Failover gap = 1500 + 500 + S3 CAS ≈ 2s.
    let config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        routing_rule: RoutingRule::AggregateTypeId,
        heartbeat_lease_duration_ms: 1500,
        heartbeat_interval_ms: 500,
        max_clock_drift_ms: 500,
        s3_lease_duration_ms: 10_000,
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
        s3_endpoint_override: Some(endpoint),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };

    println!("Starting leader on port {}...", leader_port);
    let mut leader = TestServer::start_with_config(leader_port, config.clone()).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Starting follower on port {}...", follower_port);
    let follower = TestServer::start_with_config(follower_port, config).await?;

    // Wait for: election (S3 round-trip) + initial S3 lease to expire (10s) +
    // heartbeat establishment + safety margin. After this window the follower's
    // ValidatedNodeStatus is refreshed purely by heartbeats with status_ttl_ms (2s).
    println!("Waiting for election, initial S3 lease expiry, and heartbeat establishment...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    // --- Verify cluster health ---
    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(count, 3, "Follower should have replicated 3 events");
    println!("Cluster healthy: follower has {} events\n", count);

    // --- Kill leader and measure failover ---
    println!("Killing leader...");
    drop(leader_client);
    leader.stop();

    let start = Instant::now();
    println!("Polling follower until it accepts writes...");

    let mut failover_time = None;
    while start.elapsed() < POLL_TIMEOUT {
        if let Ok(mut client) = CeleriantClient::connect(follower.address()).await {
            if write_event(&mut client, &aggregate_key, 100, false).await.is_ok() {
                failover_time = Some(start.elapsed());
                break;
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    let elapsed = failover_time.ok_or("Follower never accepted writes within timeout")?;
    println!("\nFailover completed in {:.2?}", elapsed);

    assert!(
        elapsed < MAX_FAILOVER,
        "Failover took {:.2?}, expected under {:.2?}",
        elapsed,
        MAX_FAILOVER,
    );
    println!("PASS: {:.2?} < {:.2?}\n", elapsed, MAX_FAILOVER);

    println!("=== All Tests Passed ===");
    Ok(())
}
