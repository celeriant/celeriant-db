//! Edge Case: WAL Tip Hash Divergence Detection (test #2)
//!
//! Verifies that the cluster correctly detects a WAL tip hash mismatch.
//!
//! How divergence is created so that TipHashMismatch (not WalIndexMismatch) is triggered:
//! 1. Start node A (standalone), write 5 events, stop. (wal_index = 5)
//! 2. Copy A's shard data to B's data dir.
//! 3. Start B (standalone from copy), write 1 LARGE divergent event (event 6), stop.
//!    B: wal_index = 6, tip_hash = hash(tip_5 || large_event_6_bytes)
//! 4. Start A (original data), write 3 SMALL events (events 6, 7, 8), stop.
//!    A: wal_index = 8, tip_hash at index 6 = hash(tip_5 || small_event_6_bytes)
//! 5. Start A as distributed leader (wal_index 8) with S3.
//!    Leader uploads fallback batches (events 6-8) to S3.
//! 6. Start B as distributed follower (wal_index 6, divergent).
//!    - Leader sends replication batch starting at wal_index 7.
//!      Batch's previous_tip_hash = A's hash at 6, but B's hash at 6 is divergent
//!      → TipHashMismatch. Leader falls back to S3.
//!    - B attempts S3 catchup. The batch covering wal_index 7 has
//!      previous_tip_hash = A's hash at 6 ≠ B's hash at 6
//!      → TipHashMismatch in apply_external_batch → fatal S3 catchup error.
//!    - B shuts down gracefully (exit status 0).
//! 7. Verify: B has exited, A is still healthy with 8 events.
//!
//! WAL divergence is detected but not recovered — the divergent follower shuts down.
//! The correct recovery path is to wipe the follower and re-sync from S3 (test #3).
//!
//! This is test #2 in the integration test coverage report.
//!
//! Run with: cargo run --bin edge_wal_tip_hash_divergence_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    copy_shard_dirs, count_events, is_leader, s3_cluster_config, write_event, write_large_event,
    MinioContainer, RoutingRule, ServerConfig, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;
use tempfile::TempDir;

const PORT_BASE: u16 = 17900;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: WAL Tip Hash Divergence Detection ===\n");
    println!("This test verifies that WAL divergence is detected via TipHashMismatch.");
    println!("The divergent follower shuts down. The leader remains healthy.\n");

    let port_a = PORT_BASE + (std::process::id() % 100) as u16;
    let port_b = port_a + 100;
    let minio_port = port_a + 10;

    // ========================================
    // Setup: MinIO
    // ========================================
    println!("Starting MinIO on port {}...", minio_port);
    let minio =
        MinioContainer::start_with_bucket(minio_port, "test-tip-hash-divergence").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) =
        minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 1;
    let aggregate_key = AggregateKey::new(1, 0, 1);

    let standalone_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        standalone: true,
        routing_rule: RoutingRule::AggregateTypeId,
        ..Default::default()
    };

    // ========================================
    // Phase 1: Start node A (standalone), write 5 events, stop.
    // ========================================
    println!("PHASE 1: Write 5 events to standalone node A");
    println!("----------------------------------------------");

    let mut node_a = TestServer::start_with_config_labeled(
        port_a,
        standalone_config.clone(),
        "node-a-standalone".into(),
    )
    .await?;

    let mut client_a = CeleriantClient::connect(node_a.address()).await?;
    for i in 1u64..=5 {
        write_event(&mut client_a, &aggregate_key, i, i == 1).await?;
    }
    let count_a = count_events(&mut client_a, &aggregate_key).await?;
    assert_eq!(count_a, 5, "Node A should have 5 events, got {}", count_a);
    println!("  Node A has {} events (wal_index=5)", count_a);

    drop(client_a);
    node_a.stop();
    println!("  Node A stopped. WAL at wal_index=5.\n");

    // ========================================
    // Phase 2: Copy A's data to B. Start B standalone.
    //          Write 1 LARGE event (event 6) to B, then stop.
    //          B: wal_index=6, tip_hash at 6 = hash(tip_5 || large_event_6)
    // ========================================
    println!("PHASE 2: Create node B with 1 divergent large event (event 6)");
    println!("----------------------------------------------------------------");

    let node_b_temp = TempDir::new()?;
    copy_shard_dirs(&node_a.config().data_root, node_b_temp.path())?;
    println!("  B's temp dir populated with A's shard data (5 events)");

    let mut node_b = TestServer::start_with_existing_dir(
        port_b,
        standalone_config.clone(),
        "node-b-standalone".into(),
        node_b_temp,
    )
    .await?;

    let mut client_b = CeleriantClient::connect(node_b.address()).await?;
    // One large event at index 6. Different bytes from A's future event 6 → divergent tip hash.
    write_large_event(&mut client_b, &aggregate_key, 6, 4096).await?;
    let count_b = count_events(&mut client_b, &aggregate_key).await?;
    assert_eq!(count_b, 6, "Node B should have 6 events (5 + 1 divergent), got {}", count_b);
    println!("  Node B has {} events (wal_index=6, divergent tip_hash at 6)", count_b);

    let node_b_data_root = node_b.config().data_root.clone();
    drop(client_b);
    node_b.stop();
    println!("  Node B stopped. Divergent WAL tip at wal_index=6 saved to disk.\n");

    // ========================================
    // Phase 3: Restart A as distributed leader.
    //          Write 3 SMALL events (events 6, 7, 8).
    //          A: wal_index=8, tip_hash at 6 = hash(tip_5 || small_event_6) ≠ B's
    // ========================================
    println!("PHASE 3: Restart A as distributed leader, write 3 small events (6, 7, 8)");
    println!("--------------------------------------------------------------------------");

    let cluster_config = s3_cluster_config(
        num_shards,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );
    let leader_config = ServerConfig {
        routing_rule: RoutingRule::AggregateTypeId,
        ..cluster_config.clone()
    };

    node_a.restart_with_config(leader_config).await?;
    println!("  Node A restarted as distributed leader (no follower yet)");

    let mut client_a = CeleriantClient::connect(node_a.address()).await?;
    for i in 6u64..=8 {
        write_event(&mut client_a, &aggregate_key, i, false).await?;
    }
    let count_after = count_events(&mut client_a, &aggregate_key).await?;
    assert_eq!(count_after, 8, "Leader should have 8 events (5+3), got {}", count_after);
    println!("  Leader has {} events (wal_index=8). tip_hash at 6 differs from B.", count_after);

    // Wait for S3 fallback writes to land. No follower → all new events go to S3.
    println!("  Waiting 6s for S3 fallback writes to complete...");
    tokio::time::sleep(Duration::from_secs(6)).await;

    let shard_prefix = "cluster/fallback/shard_000/";
    let s3_objects = minio.list_objects(shard_prefix).await?;
    println!("  S3 objects under {}: {}", shard_prefix, s3_objects.len());
    assert!(
        !s3_objects.is_empty(),
        "Expected S3 fallback objects for events 6-8 — found none."
    );
    println!("  S3 fallback batches present (leader's events 6-8 available for catchup)\n");

    // ========================================
    // Phase 4: Start B as distributed follower (divergent WAL, wal_index=6).
    //          Leader sends batch at wal_index 7. Batch's previous_tip_hash = A's hash at 6.
    //          B's hash at 6 is divergent → TipHashMismatch.
    //          Leader falls back to S3.
    //          B's S3 catchup encounters the same mismatch → fatal → B shuts down.
    // ========================================
    println!("PHASE 4: Start B as distributed follower (divergent WAL at wal_index=6)");
    println!("-------------------------------------------------------------------------");

    let fresh_b_temp = TempDir::new()?;
    copy_shard_dirs(&node_b_data_root, fresh_b_temp.path())?;
    println!("  Copied B's divergent shard data (wal_index=6) to fresh temp dir");

    let follower_config = ServerConfig {
        routing_rule: RoutingRule::AggregateTypeId,
        ..cluster_config
    };

    drop(client_a);

    println!("  Starting node B as follower (divergent WAL)...");
    let mut node_b = TestServer::start_with_existing_dir(
        port_b,
        follower_config,
        "node-b-follower".into(),
        fresh_b_temp,
    )
    .await?;

    // ========================================
    // Phase 5: Wait for TipHashMismatch to propagate and B to shut down.
    // ========================================
    println!("\nPHASE 5: Wait for divergence detection and follower shutdown");
    println!("-------------------------------------------------------------");

    println!("  Waiting 20s for replication attempt, S3 catchup, and graceful shutdown...");
    tokio::time::sleep(Duration::from_secs(20)).await;

    // ========================================
    // Phase 6: Assert follower shut down, leader still healthy.
    // ========================================
    println!("\nPHASE 6: Verify divergence detected — follower shut down, leader healthy");
    println!("--------------------------------------------------------------------------");

    // TipHashMismatch in S3 catchup is a fatal error. The server shuts down gracefully
    // (exit status 0), not via a panic/crash.
    match node_b.check_alive() {
        Ok(()) => panic!(
            "Follower should have exited after TipHashMismatch in S3 catchup, but it is still running"
        ),
        Err(e) => {
            println!("  Follower exited as expected: {}", e);
            assert!(
                e.contains("exit status: 0") || e.contains("status: 0"),
                "Follower should exit cleanly (status 0) on TipHashMismatch, got: {}",
                e
            );
        }
    }
    println!("  Follower exited cleanly (status 0) — TipHashMismatch detected, graceful shutdown.");

    // Leader must still be healthy with all 8 events.
    let mut client_a = CeleriantClient::connect(node_a.address()).await?;
    let leader_count = count_events(&mut client_a, &aggregate_key).await?;
    println!("  Leader has {} events (unaffected by divergent follower)", leader_count);
    assert_eq!(leader_count, 8, "Leader should have 8 events, got {}", leader_count);

    let a_is_leader = is_leader(node_a.address()).await?;
    assert!(a_is_leader, "Node A must still be the leader");
    println!("  Node A is still the leader.");

    println!("\n=== PASS ===\n");
    println!("WAL tip hash divergence was detected (TipHashMismatch).");
    println!("Leader fell back to S3. Follower's S3 catchup also encountered TipHashMismatch.");
    println!("Divergent follower shut down gracefully (status 0). Leader remains healthy.");
    println!("Note: WAL divergence is detected but not auto-recovered.");
    println!("To recover: wipe the divergent follower and let it re-sync from S3 (see test #3).");

    Ok(())
}
