//! Edge Case: WAL Divergence Recovery (test #3)
//!
//! Verifies the operational recovery path after WAL tip hash divergence:
//! wipe the divergent node and let it re-sync from S3 as a fresh follower.
//!
//! Setup (same divergence scenario as test #2):
//! 1. Start node A as distributed leader (with S3), write 5 events, stop.
//!    S3 fallback batches created for events 1-5 (no follower).
//! 2. Copy A's shard data to B.
//! 3. Start B (standalone from copy), write 1 LARGE divergent event (event 6), stop.
//!    B: wal_index = 6, tip_hash at 6 = hash(tip_5 || large_event_6)
//! 4. Restart A as distributed leader, write 3 SMALL events (events 6, 7, 8).
//!    A: wal_index = 8, tip_hash at 6 = hash(tip_5 || small_event_6) ≠ B's
//!    S3 fallback batches for events 6-8 also land in S3.
//! 5. Start B as distributed follower (divergent wal_index=6).
//!    - TipHashMismatch → follower shuts down (same as test #2).
//!
//! Recovery:
//! 6. Start a FRESH follower (clean data dir, no divergent state).
//!    - Fresh follower has no WAL data → S3 catchup applies events 1-8.
//!    - Replication resumes normally.
//! 7. Verify: fresh follower has all 8 events. New writes replicate correctly.
//!
//! The correct recovery for WAL divergence is to wipe the divergent follower
//! and let it re-join from scratch via S3 catchup.
//!
//! This is test #3 in the integration test coverage report.
//!
//! Run with: cargo run --bin edge_wal_divergence_recovery_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    copy_shard_dirs, count_events, is_leader, s3_cluster_config, write_event, write_large_event,
    MinioContainer, RoutingRule, ServerConfig, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;
use tempfile::TempDir;

const PORT_BASE: u16 = 18300;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: WAL Divergence Recovery ===\n");
    println!("This test verifies the operational recovery path for WAL divergence:");
    println!("wipe the divergent node and let it re-sync from S3 as a fresh follower.\n");

    let port_a = PORT_BASE + (std::process::id() % 100) as u16;
    let port_b = port_a + 100;
    let port_fresh = port_a + 200;
    let minio_port = port_a + 10;

    // ========================================
    // Setup: MinIO
    // ========================================
    println!("Starting MinIO on port {}...", minio_port);
    let minio =
        MinioContainer::start_with_bucket(minio_port, "test-wal-divergence-recovery").await?;
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

    let cluster_config = s3_cluster_config(
        num_shards,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );

    // ========================================
    // Phase 1: Start node A as distributed leader (with S3), write 5 events, stop.
    //          S3 fallback batches are created for events 1-5 (no follower present).
    // ========================================
    println!("PHASE 1: Write 5 events to distributed node A (with S3)");
    println!("--------------------------------------------------------");

    let leader_config = ServerConfig {
        routing_rule: RoutingRule::AggregateTypeId,
        ..cluster_config.clone()
    };
    let mut node_a = TestServer::start_with_config_labeled(
        port_a,
        leader_config,
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

    println!("  Waiting 4s for S3 fallback writes for events 1-5...");
    tokio::time::sleep(Duration::from_secs(4)).await;

    drop(client_a);
    node_a.stop();
    println!("  Node A stopped.\n");

    // ========================================
    // Phase 2: Copy A's data to B. Start B standalone.
    //          Write 1 LARGE event (event 6) to B, then stop.
    //          B: wal_index=6, tip_hash at 6 = hash(tip_5 || large_event_6) — divergent
    // ========================================
    println!("PHASE 2: Create divergent node B (1 large event 6, wal_index=6)");
    println!("------------------------------------------------------------------");

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

    let restart_config = ServerConfig {
        routing_rule: RoutingRule::AggregateTypeId,
        ..cluster_config.clone()
    };

    node_a.restart_with_config(restart_config).await?;
    println!("  Node A restarted as distributed leader (no follower yet)");

    let mut client_a = CeleriantClient::connect(node_a.address()).await?;
    for i in 6u64..=8 {
        write_event(&mut client_a, &aggregate_key, i, false).await?;
    }
    let count_after = count_events(&mut client_a, &aggregate_key).await?;
    assert_eq!(count_after, 8, "Leader should have 8 events (5+3), got {}", count_after);
    println!("  Leader has {} events (wal_index=8). tip_hash at 6 differs from B.", count_after);

    println!("  Waiting 6s for S3 fallback writes to complete...");
    tokio::time::sleep(Duration::from_secs(6)).await;

    let shard_prefix = "cluster/fallback/shard_000/";
    let s3_objects = minio.list_objects(shard_prefix).await?;
    println!("  S3 objects under {}: {}", shard_prefix, s3_objects.len());
    assert!(
        !s3_objects.is_empty(),
        "Expected S3 fallback objects for events 6-8 — found none."
    );
    println!("  S3 fallback batches present (events 6-8)\n");

    // ========================================
    // Phase 4: Start divergent B as follower.
    //          It will detect TipHashMismatch and shut down.
    // ========================================
    println!("PHASE 4: Start divergent B as follower — expect TipHashMismatch + shutdown");
    println!("----------------------------------------------------------------------------");

    let fresh_b_temp = TempDir::new()?;
    copy_shard_dirs(&node_b_data_root, fresh_b_temp.path())?;
    println!("  Copied B's divergent shard data (wal_index=6) to fresh temp dir");

    let follower_config = ServerConfig {
        routing_rule: RoutingRule::AggregateTypeId,
        ..cluster_config.clone()
    };

    drop(client_a);

    println!("  Starting divergent node B as follower...");
    let mut node_b = TestServer::start_with_existing_dir(
        port_b,
        follower_config,
        "node-b-follower-divergent".into(),
        fresh_b_temp,
    )
    .await?;

    println!("  Waiting 20s for TipHashMismatch detection and graceful shutdown...");
    tokio::time::sleep(Duration::from_secs(20)).await;

    // Divergent follower must have exited with status 0.
    match node_b.check_alive() {
        Ok(()) => panic!(
            "Divergent follower should have exited after TipHashMismatch, but is still running"
        ),
        Err(e) => {
            println!("  Divergent follower exited as expected: {}", e);
            assert!(
                e.contains("exit status: 0") || e.contains("status: 0"),
                "Divergent follower should exit cleanly (status 0), got: {}",
                e
            );
        }
    }
    println!("  Divergent follower shut down gracefully (status 0).\n");

    // ========================================
    // Phase 5: Start a FRESH follower (clean data dir).
    //          No divergent state → S3 catchup applies events 6-8 cleanly.
    //          Replication resumes normally.
    // ========================================
    println!("PHASE 5: Start a fresh follower (clean data dir) — operational recovery");
    println!("--------------------------------------------------------------------------");

    let fresh_config = ServerConfig {
        routing_rule: RoutingRule::AggregateTypeId,
        ..cluster_config
    };

    println!("  Starting fresh follower on port {}...", port_fresh);
    let mut fresh_follower = TestServer::start_with_config_labeled(
        port_fresh,
        fresh_config,
        "node-fresh-follower".into(),
    )
    .await?;

    println!("  Waiting 15s for S3 catchup and cluster formation...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    // ========================================
    // Phase 6: Verify the fresh follower has caught up and the cluster is healthy.
    // ========================================
    println!("\nPHASE 6: Verify fresh follower caught up and cluster is healthy");
    println!("------------------------------------------------------------------");

    let mut client_a = CeleriantClient::connect(node_a.address()).await?;

    node_a
        .check_alive()
        .map_err(|e| format!("Leader crashed: {}", e))?;
    fresh_follower
        .check_alive()
        .map_err(|e| format!("Fresh follower crashed: {}", e))?;

    let a_is_leader = is_leader(node_a.address()).await?;
    let fresh_is_leader = is_leader(fresh_follower.address()).await?;
    println!("  A is_leader={}, fresh_follower is_leader={}", a_is_leader, fresh_is_leader);
    assert!(a_is_leader, "Node A must still be the leader");
    assert!(!fresh_is_leader, "Fresh follower must be follower");

    let leader_count = count_events(&mut client_a, &aggregate_key).await?;
    let mut fresh_client = CeleriantClient::connect(fresh_follower.address()).await?;
    let fresh_count = count_events(&mut fresh_client, &aggregate_key).await?;

    println!("  Leader event count: {}", leader_count);
    println!("  Fresh follower event count: {}", fresh_count);

    assert_eq!(leader_count, 8, "Leader should have 8 events, got {}", leader_count);
    assert_eq!(
        fresh_count, 8,
        "Fresh follower should have 8 events after S3 catchup, got {}",
        fresh_count
    );
    println!("  PASS: Fresh follower caught up to 8 events via S3 catchup.");

    // Verify replication is healthy after the recovery.
    println!("\n  Writing events 9-10 to verify healthy replication...");
    write_event(&mut client_a, &aggregate_key, 9, false).await?;
    write_event(&mut client_a, &aggregate_key, 10, false).await?;

    tokio::time::sleep(Duration::from_secs(4)).await;

    let leader_final = count_events(&mut client_a, &aggregate_key).await?;
    let fresh_final = count_events(&mut fresh_client, &aggregate_key).await?;
    println!("  Leader final: {} events", leader_final);
    println!("  Fresh follower final: {} events", fresh_final);

    assert_eq!(leader_final, 10, "Leader should have 10 events, got {}", leader_final);
    assert_eq!(
        fresh_final, 10,
        "Fresh follower should have 10 events after replication, got {}",
        fresh_final
    );

    println!("  Both nodes have {} events — replication healthy after recovery.", leader_final);

    println!("\n=== PASS ===\n");
    println!("WAL divergence operational recovery verified:");
    println!("  - Divergent follower (wal_index=6, wrong tip_hash) detected TipHashMismatch.");
    println!("  - Divergent follower shut down gracefully (status 0).");
    println!("  - Fresh follower (clean data dir) joined and caught up via S3 catchup.");
    println!("  - Replication resumed successfully after recovery.");
    println!("  - Recovery path: wipe divergent node, let it re-sync from S3.");

    Ok(())
}
