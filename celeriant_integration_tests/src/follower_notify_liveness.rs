//! Integration test: Follower Notify Liveness (idle-tail convergence bound)
//!
//! Proves the commit-notify fast path, not the 5s periodic replication probe.
//! A burst-tail commit and a lone single write must both propagate to the
//! follower within the recency-notify window (RECENCY_WINDOW_BATCHES ×
//! replication_delay), well before the 5s probe would fire.
//!
//! With `replication_delay_us = 5000` (5ms) the recency window is ~80ms, so a
//! 1.5s convergence bound is tight yet comfortably clear of the 5s probe. Each
//! phase writes to the leader, STOPS, then times follower convergence with its
//! own Instant loop and asserts the measured elapsed is under that bound.
//!
//! Run with: cargo run --release -p celeriant_integration_tests -- --test follower_notify_liveness

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, s3_cluster_config, write_event, MinioContainer, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::{Duration, Instant};

/// Convergence bound for the notify fast path: comfortably below the 5s probe,
/// far above the ~80ms recency window at replication_delay_us=5000.
const NOTIFY_CONVERGENCE_BOUND: Duration = Duration::from_millis(1500);

/// Poll the follower until its event count for `key` reaches `expected`,
/// returning the elapsed time to converge. Errors if it does not converge
/// within `NOTIFY_CONVERGENCE_BOUND` (caller asserts on the returned elapsed,
/// but this guards against an unbounded hang on regression).
async fn time_convergence(
    follower: &mut CeleriantClient,
    key: &AggregateKey,
    expected: usize,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let start = Instant::now();
    // Hard ceiling so a lost-notify regression fails fast rather than hanging.
    let ceiling = Duration::from_secs(8);
    loop {
        let count = count_events(follower, key).await?;
        if count == expected {
            return Ok(start.elapsed());
        }
        if start.elapsed() >= ceiling {
            return Err(format!(
                "follower stuck at {}/{} after {:.3}s",
                count,
                expected,
                start.elapsed().as_secs_f64()
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Follower Notify Liveness Test ===\n");

    let port_base = 11600 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO...");
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = 1;
    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 30_000;
    config.s3_lease_duration_ms = 30_000;
    // 5ms replication delay → ~80ms recency-notify window. Tight, well-separated
    // from the 5s periodic probe so this test isolates the notify fast path.
    config.replication_delay_us = 5000;

    println!("Starting cluster (replication_delay_us={})...", config.replication_delay_us);
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("Waiting for election + replication...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    // ── Phase burst: burst of writes, then stop ──
    println!("\nPHASE burst: 20-event burst to one aggregate, stop, time follower convergence");
    let key_burst = AggregateKey::new(1, 0, 1);
    for i in 1..=20 {
        write_event(&mut leader_client, &key_burst, i, i == 1).await?;
    }
    let leader_count = count_events(&mut leader_client, &key_burst).await?;
    assert_eq!(leader_count, 20, "leader should have 20 events");
    // No further writes: the tail commit must ride the notify window, not a probe.
    let burst_elapsed = time_convergence(&mut follower_client, &key_burst, 20).await?;
    println!("  burst converged in {:.3}s", burst_elapsed.as_secs_f64());
    assert!(
        burst_elapsed < NOTIFY_CONVERGENCE_BOUND,
        "burst tail must converge within notify window ({:?}), took {:.3}s",
        NOTIFY_CONVERGENCE_BOUND,
        burst_elapsed.as_secs_f64()
    );
    println!("  Phase burst PASSED\n");

    // ── Phase lone-write: single write to a fresh aggregate, then stop ──
    println!("PHASE lone-write: single write to fresh aggregate, stop, time follower convergence");
    let key_lone = AggregateKey::new(1, 0, 2);
    write_event(&mut leader_client, &key_lone, 1, true).await?;
    let leader_count = count_events(&mut leader_client, &key_lone).await?;
    assert_eq!(leader_count, 1, "leader should have 1 event");
    let lone_elapsed = time_convergence(&mut follower_client, &key_lone, 1).await?;
    println!("  lone-write converged in {:.3}s", lone_elapsed.as_secs_f64());
    assert!(
        lone_elapsed < NOTIFY_CONVERGENCE_BOUND,
        "lone write must converge within notify window ({:?}), took {:.3}s",
        NOTIFY_CONVERGENCE_BOUND,
        lone_elapsed.as_secs_f64()
    );
    println!("  Phase lone-write PASSED\n");

    println!(
        "=== All Phases Passed (burst={:.3}s, lone={:.3}s) ===",
        burst_elapsed.as_secs_f64(),
        lone_elapsed.as_secs_f64()
    );
    Ok(())
}
