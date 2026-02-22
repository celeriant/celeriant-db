//! Edge Case: Leader Crash Before Replication — Divergent WAL Auto-Heal
//!
//! Tests the only split-brain scenario in a two-node cluster that fsyncs before
//! replication: the leader fsyncs a write to local disk but crashes before
//! replicating to the follower or uploading to S3. The unreplicated write stays
//! on disk. The follower takes over as leader and writes its own divergent events.
//! When the old leader restarts as follower, it has a divergent WAL and must
//! auto-heal: truncate back to the last common point, catch up from S3, and
//! rejoin the cluster as a healthy follower.
//!
//! Scenario:
//! 1. Start A as distributed leader (with S3, no follower), write events 1-5.
//!    S3 fallback batches land for events 1-5 (no follower to replicate to).
//! 2. Copy A's shard data to B (simulates B being a synced follower via replication).
//! 3. Start A standalone, write event 6 (large, divergent).
//!    This simulates the unreplicated fsync that was on A's disk when it crashed.
//!    A: wal_index=6, tip_hash at 6 = hash(tip_5 || large_event_6)
//! 4. Start B as distributed leader. B writes events 6-8 (small, different bytes).
//!    B: wal_index=8, tip_hash at 6 = hash(tip_5 || small_event_6) != A's
//!    S3 has events 1-5 (from Phase 1) + events 6-8 (from Phase 4).
//! 5. Start A as distributed follower (divergent wal_index=6).
//!    A does S3 catchup. The batch covering events 6-8 partially overlaps:
//!    - Skips entry 6 (already have it, wal_index <= current)
//!    - Tries entry 7: previous_tip_hash = B's hash at 6 != A's hash at 6
//!    - TipHashMismatch detected -> A truncates WAL back to wal_index=5
//!    - A re-applies events 6-8 from S3 -> wal_index=8, converged with B
//! 6. Verify A is alive with 8 events, B still healthy with 8 events.
//! 7. Write events 9-10 to B, verify they replicate to A.
//!
//! This is the only split-brain scenario possible when fsync precedes replication:
//! the crash window between fsync and replication is the only moment where data
//! can exist on one node but nowhere else.
//!
//! Run with: cargo run --bin edge_leader_crash_divergent_wal_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    copy_shard_dirs, count_events, s3_cluster_config, write_event, write_large_event,
    MinioContainer, RoutingRule, ServerConfig, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;
use tempfile::TempDir;

const PORT_BASE: u16 = 18700;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: Leader Crash Before Replication — Divergent WAL Recovery ===\n");
    println!("Tests the split-brain scenario where a leader fsyncs a write but crashes");
    println!("before replication. The old leader restarts with a divergent WAL.\n");

    let port_a = PORT_BASE + (std::process::id() % 100) as u16;
    let port_b = port_a + 100;
    let minio_port = port_a + 10;

    // ========================================
    // Setup: MinIO
    // ========================================
    println!("Starting MinIO on port {}...", minio_port);
    let minio =
        MinioContainer::start_with_bucket(minio_port, "test-leader-crash-divergent").await?;
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
    // Phase 1: Start A as distributed leader (with S3, no follower).
    //          Write events 1-5. Wait for S3 fallback writes.
    //          S3 has events 1-5 as fallback batches.
    // ========================================
    println!("PHASE 1: Write 5 events to distributed leader A (with S3, no follower)");
    println!("-----------------------------------------------------------------------");

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
    // Don't use is_leader() here — it writes a probe aggregate that would get
    // copied to B in Phase 2, breaking later is_leader() checks on B.
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

    let shard_prefix = "cluster/fallback/shard_000/";
    let s3_objects = minio.list_objects(shard_prefix).await?;
    println!("  S3 objects under {}: {}", shard_prefix, s3_objects.len());
    assert!(
        !s3_objects.is_empty(),
        "Expected S3 fallback batches for events 1-5 — found none."
    );
    println!("  S3 fallback batches present for events 1-5");

    drop(client_a);
    node_a.stop();
    // Wait for the port to be fully released before restarting
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("  Node A stopped.\n");

    // ========================================
    // Phase 2: Copy A's shard data to B.
    //          B represents the follower that had events 1-5 via replication
    //          at the moment the leader crashed.
    // ========================================
    println!("PHASE 2: Copy A's shard data to B (simulates synced follower)");
    println!("--------------------------------------------------------------");

    let node_b_temp = TempDir::new()?;
    copy_shard_dirs(&node_a.config().data_root, node_b_temp.path())?;
    println!("  B's data dir populated with A's shard data (events 1-5)\n");

    // ========================================
    // Phase 3: Start A standalone, write event 6 (large, divergent).
    //          This simulates the unreplicated fsync that was on A's disk
    //          when the leader process crashed.
    // ========================================
    println!("PHASE 3: Write divergent event 6 to A standalone (simulates unreplicated fsync)");
    println!("--------------------------------------------------------------------------------");

    node_a
        .restart_with_config(standalone_config.clone())
        .await?;
    let mut client_a = CeleriantClient::connect(node_a.address()).await?;

    // Large event ensures different bytes (and therefore different tip_hash) from B's event 6
    write_large_event(&mut client_a, &aggregate_key, 6, 4096).await?;
    let count_a = count_events(&mut client_a, &aggregate_key).await?;
    assert_eq!(
        count_a, 6,
        "A should have 6 events (5 + 1 divergent), got {}",
        count_a
    );
    println!(
        "  A has {} events (wal_index=6, divergent tip_hash at 6)",
        count_a
    );

    drop(client_a);
    node_a.stop();
    println!("  A stopped. Divergent event 6 saved to disk.\n");

    // ========================================
    // Phase 4: Start B as distributed leader (with S3).
    //          B writes events 6-8 (small, different from A's large event 6).
    //          Wait for S3 fallback writes.
    // ========================================
    println!("PHASE 4: Start B as distributed leader, write events 6-8 (divergent from A)");
    println!("----------------------------------------------------------------------------");

    let b_leader_config = ServerConfig {
        routing_rule: RoutingRule::AggregateTypeId,
        ..cluster_config.clone()
    };
    let mut node_b = TestServer::start_with_existing_dir(
        port_b,
        b_leader_config,
        "node-b-leader".into(),
        node_b_temp,
    )
    .await?;

    // Wait for B to win S3 CAS race and become leader.
    // A's lease from Phase 1 has a 10s duration. B must wait for it to expire.
    println!("  Waiting 15s for A's lease to expire and B to win S3 CAS race...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    let mut client_b = CeleriantClient::connect(node_b.address()).await?;

    // B's events 6-8: small payload, different bytes from A's large event 6
    // Retry with backoff in case B hasn't fully transitioned to leader yet.
    for i in 6u64..=8 {
        for retry in 0..10 {
            match write_event(&mut client_b, &aggregate_key, i, false).await {
                Ok(_) => break,
                Err(e) if retry < 9 => {
                    println!("  Write {} retry {}: {}", i, retry + 1, e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
    let count_b = count_events(&mut client_b, &aggregate_key).await?;
    assert_eq!(
        count_b, 8,
        "B should have 8 events (5 + 3), got {}",
        count_b
    );
    println!(
        "  B has {} events (wal_index=8). tip_hash at 6 differs from A.",
        count_b
    );

    println!("  Waiting 6s for S3 fallback writes for events 6-8...");
    tokio::time::sleep(Duration::from_secs(6)).await;

    let s3_objects = minio.list_objects(shard_prefix).await?;
    println!("  S3 objects under {}: {}", shard_prefix, s3_objects.len());
    assert!(
        !s3_objects.is_empty(),
        "Expected S3 fallback batches — found none."
    );
    println!("  S3 fallback batches present (events 1-5 from Phase 1, events 6-8 from Phase 4)\n");

    // ========================================
    // Phase 5: Start A as distributed follower (divergent wal_index=6).
    //          A does S3 catchup:
    //          - S3 batch for events 6-8: skips entry 6, tries entry 7
    //          - entry 7's previous_tip_hash = B's hash at 6 != A's hash at 6
    //          - TipHashMismatch detected -> A truncates WAL to last common point
    //          - A re-applies events 6-8 from S3 -> converged with B
    // ========================================
    println!("PHASE 5: Start A as distributed follower (divergent WAL at wal_index=6)");
    println!("-----------------------------------------------------------------------");

    drop(client_b);

    let follower_config = ServerConfig {
        routing_rule: RoutingRule::AggregateTypeId,
        ..cluster_config
    };

    println!("  Starting A as follower (divergent WAL)...");
    node_a.restart_with_config(follower_config).await?;

    println!("  Waiting 20s for auto-heal: TipHashMismatch -> truncate -> S3 catchup...");
    tokio::time::sleep(Duration::from_secs(20)).await;

    // ========================================
    // Phase 6: Verify A auto-healed and both nodes converged
    // ========================================
    println!("\nPHASE 6: Verify auto-heal — A alive with 8 events, B healthy");
    println!("-------------------------------------------------------------");

    node_a
        .check_alive()
        .map_err(|e| format!("A should have auto-healed but crashed: {}", e))?;
    println!("  A is still alive (auto-heal succeeded).");

    let mut client_a = CeleriantClient::connect(node_a.address()).await?;
    let a_count = count_events(&mut client_a, &aggregate_key).await?;
    println!("  A has {} events after auto-heal", a_count);
    assert_eq!(
        a_count, 8,
        "A should have 8 events after truncating divergent event 6 and catching up from S3, got {}",
        a_count
    );

    node_b
        .check_alive()
        .map_err(|e| format!("B crashed: {}", e))?;
    let mut client_b = CeleriantClient::connect(node_b.address()).await?;
    let b_count = count_events(&mut client_b, &aggregate_key).await?;
    assert_eq!(b_count, 8, "B should have 8 events, got {}", b_count);
    println!("  B is healthy with {} events.", b_count);
    println!("  Both nodes converged at 8 events.\n");

    // ========================================
    // Phase 7: Write events 9-10 to B, verify replication to A
    // ========================================
    println!("PHASE 7: Verify replication B -> A works after auto-heal");
    println!("--------------------------------------------------------");

    write_event(&mut client_b, &aggregate_key, 9, false).await?;
    write_event(&mut client_b, &aggregate_key, 10, false).await?;
    println!("  Wrote events 9-10 to leader B.");

    println!("  Waiting 10s for replication B -> A...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let b_final = count_events(&mut client_b, &aggregate_key).await?;
    assert_eq!(b_final, 10, "B should have 10 events, got {}", b_final);

    drop(client_a);
    let mut client_a = CeleriantClient::connect(node_a.address()).await?;
    let a_final = count_events(&mut client_a, &aggregate_key).await?;
    println!("  B: {} events, A: {} events", b_final, a_final);
    assert_eq!(
        a_final, 10,
        "A should have 10 events after replication, got {}",
        a_final
    );
    println!("  Replication B -> A working after auto-heal.");

    println!("\n=== PASS ===\n");
    println!("Leader crash before replication — divergent WAL auto-heal verified:");
    println!("  - Leader A wrote events 1-5 (replicated to S3, copied to B).");
    println!("  - Leader A 'crashed' with unreplicated event 6 on disk.");
    println!("  - Follower B took over as leader, wrote divergent events 6-8.");
    println!("  - Old leader A restarted as follower, detected TipHashMismatch.");
    println!("  - A auto-healed: truncated divergent WAL, caught up from S3 (8 events).");
    println!("  - Replication B -> A resumed: events 9-10 replicated successfully.");

    Ok(())
}
