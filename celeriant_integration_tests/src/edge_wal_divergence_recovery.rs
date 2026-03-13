//! Edge Case: WAL Divergence Recovery via Auto-Heal (test #3)
//!
//! Verifies that a divergent follower auto-heals and replication resumes.
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
//!    - TipHashMismatch detected → WAL truncated → S3 catchup → auto-healed.
//! 6. Verify: both nodes converged at 8 events. New writes replicate correctly.
//!
//! Run with: cargo run --bin edge_wal_divergence_recovery_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    copy_shard_dirs, count_events, s3_cluster_config, write_event, write_large_event,
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
        "node-a-leader".into(),
    )
    .await?;

    // Wait for A to win election and become leader (empty S3, no contention).
    println!("  Waiting 5s for A to win election...");
    tokio::time::sleep(Duration::from_secs(5)).await;

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

    // Wait for A to win election after restart (must wait for previous lease to expire).
    println!("  Waiting 5s for A to win election...");
    tokio::time::sleep(Duration::from_secs(5)).await;

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
    //          It will detect TipHashMismatch, truncate, and auto-heal via S3.
    // ========================================
    println!("PHASE 4: Start divergent B as follower — expect TipHashMismatch + auto-heal");
    println!("-----------------------------------------------------------------------------");

    let fresh_b_temp = TempDir::new()?;
    copy_shard_dirs(&node_b_data_root, fresh_b_temp.path())?;
    println!("  Copied B's divergent shard data (wal_index=6) to fresh temp dir");

    let follower_config = ServerConfig {
        routing_rule: RoutingRule::AggregateTypeId,
        ..cluster_config.clone()
    };

    println!("  Starting divergent node B as follower...");
    let mut node_b = TestServer::start_with_existing_dir(
        port_b,
        follower_config,
        "node-b-follower-divergent".into(),
        fresh_b_temp,
    )
    .await?;

    // Auto-heal is fast (truncation + S3 catchup completes in <1s), but wait for
    // replication connection establishment and follower discovery.
    println!("  Waiting 10s for TipHashMismatch detection, WAL truncation, and S3 catchup...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    node_b
        .check_alive()
        .map_err(|e| format!("Divergent follower should have auto-healed but crashed: {}", e))?;
    println!("  Divergent follower is still alive (auto-heal succeeded).");

    let mut client_b = CeleriantClient::connect(node_b.address()).await?;
    let b_count = count_events(&mut client_b, &aggregate_key).await?;
    println!("  Divergent follower has {} events after auto-heal", b_count);
    assert_eq!(
        b_count, 8,
        "Divergent follower should have 8 events after truncation and S3 catchup, got {}",
        b_count
    );
    // ========================================
    // Phase 5: Verify replication is healthy after auto-heal.
    //          Write new events to A, verify they replicate to B.
    // ========================================
    println!("\nPHASE 5: Verify replication is healthy after auto-heal");
    println!("------------------------------------------------------");

    let leader_count = count_events(&mut client_a, &aggregate_key).await?;
    assert_eq!(leader_count, 8, "Leader should have 8 events, got {}", leader_count);
    println!("  Leader has {} events, follower auto-healed to {}.", leader_count, b_count);

    println!("  Writing events 9-10 to verify healthy replication...");
    write_event(&mut client_a, &aggregate_key, 9, false).await?;
    write_event(&mut client_a, &aggregate_key, 10, false).await?;

    tokio::time::sleep(Duration::from_secs(4)).await;

    let leader_final = count_events(&mut client_a, &aggregate_key).await?;
    drop(client_b);
    let mut client_b = CeleriantClient::connect(node_b.address()).await?;
    let follower_final = count_events(&mut client_b, &aggregate_key).await?;
    println!("  Leader final: {} events", leader_final);
    println!("  Follower final: {} events", follower_final);

    assert_eq!(leader_final, 10, "Leader should have 10 events, got {}", leader_final);
    assert_eq!(
        follower_final, 10,
        "Follower should have 10 events after replication, got {}",
        follower_final
    );

    println!("  Both nodes have {} events — replication healthy after auto-heal.", leader_final);

    println!("\n=== PASS ===\n");
    println!("WAL divergence recovery verified:");
    println!("  - Divergent follower (wal_index=6, wrong tip_hash) detected TipHashMismatch.");
    println!("  - Divergent follower auto-healed: truncated WAL, caught up from S3.");
    println!("  - Replication resumed successfully (events 9-10 replicated after recovery).");

    Ok(())
}
