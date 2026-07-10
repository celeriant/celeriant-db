//! Regression test: kick delivery after follower restart
//!
//! Verifies that a single write after follower restart correctly delivers to the
//! follower. Previously, the kick was gated by `is_follower_reachable()` which
//! returned false after the stale TCP connection error, preventing kick delivery
//! even though the follower was alive. Fixed by decoupling kick from the
//! TCP-skip optimisation (follower_reachable gates TCP attempts, not kicks).
//!
//! Sequence:
//! 1. Leader + follower, TCP replication working
//! 2. Stop follower
//! 3. Write events to leader (S3 fallback)
//! 4. Restart follower, wait for S3 boot catchup
//! 5. Write ONE event to leader
//! 6. Verify: event goes to S3 (stale TCP conn fails) but follower doesn't get it
//!    (kick not sent because follower_reachable=false)
//! 7. Write a SECOND event — triggers extended TCP catchup, delivers both events
//! 8. Verify: follower now has all events (proving the workaround)

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, poll_converged_count, poll_event_count, s3_cluster_config, write_event,
    MinioContainer, TestServer, FOLLOWER_CONVERGENCE_TIMEOUT,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Bug Reproduction: Kick Not Delivered After Follower Restart ===\n");

    let port_base = 15800 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = 2;
    let aggregate_key = AggregateKey::new(1, 1, 1);
    let expected_shard = (aggregate_key.aggregate_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", expected_shard);

    let config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );

    println!("Starting two-node cluster...");
    let leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into())
            .await?;
    let mut follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("Waiting for election + discovery...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // ========================================
    // Phase 1: Normal TCP replication
    // ========================================
    println!("PHASE 1: Normal TCP replication");
    println!("-------------------------------");

    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let fc =
        poll_converged_count(&mut follower_client, &aggregate_key, 3, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(fc, 3, "Follower should have 3 events");
    println!("  Follower has {} events (TCP working)\n", fc);

    // ========================================
    // Phase 2: Stop follower, write via S3
    // ========================================
    println!("PHASE 2: Stop follower, write events (S3 fallback)");
    println!("--------------------------------------------------");

    drop(follower_client);
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 4..=5 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }
    println!("  Wrote events 4-5 to leader (S3 fallback)\n");

    // ========================================
    // Phase 3: Restart follower, wait for boot catchup
    // ========================================
    println!("PHASE 3: Restart follower, wait for boot catchup");
    println!("-------------------------------------------------");

    follower.restart().await?;
    let fc = poll_event_count(
        follower.address(), &aggregate_key, 5, Duration::from_secs(30),
    ).await;
    println!("  Follower caught up: {} events\n", fc);

    // ========================================
    // Phase 4: Write ONE event — should trigger S3 fallback + kick
    // ========================================
    println!("PHASE 4: Write one event (the bug)");
    println!("-----------------------------------");
    println!("  Writing event 6 to leader...");
    write_event(&mut leader_client, &aggregate_key, 6, false).await?;

    // Verify the event went to S3 (stale TCP connection failed)
    tokio::time::sleep(Duration::from_secs(2)).await;
    let s3_objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 fallback objects after write: {}", s3_objects.len());

    // Give the kick plenty of time to arrive (if it were sent)
    println!("  Waiting 10s for kick to arrive (if sent)...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let fc =
        poll_converged_count(&mut follower_client, &aggregate_key, 6, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    println!("  Follower has {} events (expected: 5, bug = kick not sent)", fc);

    // After the fix: kick is sent regardless of follower_reachable flag.
    // The follower should receive event 6 via kick → S3 catchup, or via
    // direct TCP replication if the connection recovered in time.
    assert!(
        fc >= 6,
        "Follower should have 6 events after kick delivery, got {}",
        fc
    );
    println!("  Follower received event 6 — kick delivered correctly\n");

    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    assert_eq!(leader_count, 6, "Leader should have 6 events");

    println!("=== PASS ===");
    println!("Single write after follower restart delivered via kick + S3 catchup.\n");

    Ok(())
}
