//! Standalone-to-Distributed Transition Integration Test
//!
//! Tests migrating a standalone node to a two-node distributed cluster with
//! WAL data seeded by copying shard files. Runs two scenarios:
//!
//! - **LeaderFirst**: A stays running as leader, B joins as follower
//! - **FollowerFirst**: A stops, B starts first (catches up from S3, becomes leader),
//!   then A restarts as follower
//!
//! Both scenarios must produce identical results: all events present on both nodes.
//!
//! Flow per scenario:
//! 1. Start node A in standalone mode, write mixed-size events across 4 shards
//! 2. Stop A, restart in distributed mode (leader, no follower yet)
//! 3. Copy shard WAL files from A to a new data directory for B
//! 4. Write more events to A (go to S3 fallback since no follower)
//! 5. Start B (join order varies by scenario)
//! 6. Verify both nodes have all events, new writes replicate
//!
//! Run with: cargo run --release --bin standalone_to_distributed_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    copy_shard_dirs, count_events, is_leader, s3_cluster_config, write_event, write_large_event,
    MinioContainer, RoutingRule, ServerConfig, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;
use tempfile::TempDir;

const NUM_SHARDS: usize = 4;
const SMALL_PREALLOCATE: u64 = 2 * 1024 * 1024; // 2MB (must be > 2 * HEADER_BLOCK_SIZE_BYTES and aligned to 512KB)
const LARGE_PAYLOAD: usize = 1200; // ~1.2KB events

#[derive(Debug, Clone, Copy)]
enum JoinOrder {
    LeaderFirst,
    FollowerFirst,
}


fn aggregate_keys() -> [AggregateKey; NUM_SHARDS] {
    [
        AggregateKey::new(1, 0, 1), // shard 0
        AggregateKey::new(1, 1, 1), // shard 1
        AggregateKey::new(1, 2, 1), // shard 2
        AggregateKey::new(1, 3, 1), // shard 3
    ]
}

async fn assert_event_counts(
    client: &mut CeleriantClient,
    keys: &[AggregateKey],
    expected: usize,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for (i, key) in keys.iter().enumerate() {
        let count = count_events(client, key).await?;
        assert_eq!(
            count, expected,
            "{} shard {}: expected {} events, got {}",
            label, i, expected, count
        );
    }
    Ok(())
}

async fn run_scenario(
    join_order: JoinOrder,
    port_base: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Scenario: {:?} ===\n", join_order);

    let port_a = port_base;
    let port_b = port_base + 100;
    let minio_port = port_base + 10;
    let keys = aggregate_keys();

    // ========================================
    // Phase 1: Standalone writes (mixed sizes)
    // ========================================
    println!("PHASE 1: Standalone writes (mixed sizes, 4 shards)");

    let standalone_config = ServerConfig {
        num_shards: Some(NUM_SHARDS),
        log_level: "info".to_string(),
        standalone: true,
        routing_rule: RoutingRule::AggregateTypeId,
        shard_log_preallocate_bytes: SMALL_PREALLOCATE,
        ..Default::default()
    };

    let mut node_a = TestServer::start_with_config_labeled(
        port_a,
        standalone_config,
        "node-a".into(),
    )
    .await?;

    let mut client_a = CeleriantClient::connect(node_a.address()).await?;

    for key in &keys {
        // 3 small events
        for i in 1..=3 {
            write_event(&mut client_a, key, i, i == 1).await?;
        }
        // 2 large events (~1.2KB each)
        for i in 4..=5 {
            write_large_event(&mut client_a, key, i, LARGE_PAYLOAD).await?;
        }
    }

    assert_event_counts(&mut client_a, &keys, 5, "standalone").await?;
    println!("  All 4 shards have 5 events each (20 total)\n");

    // ========================================
    // Phase 2: Transition A to distributed mode
    // ========================================
    println!("PHASE 2: Transition node A to distributed mode");

    drop(client_a);
    node_a.stop();

    let minio = MinioContainer::start_with_bucket(
        minio_port,
        &format!("test-transition-{:?}", join_order).to_lowercase(),
    )
    .await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let mut distributed_config = s3_cluster_config(
        NUM_SHARDS,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );
    distributed_config.shard_log_preallocate_bytes = SMALL_PREALLOCATE;

    node_a.restart_with_config(distributed_config.clone()).await?;
    println!("  Node A restarted in distributed mode (leader, no follower)\n");

    // ========================================
    // Phase 3: Copy WAL files to node B's data directory
    // ========================================
    println!("PHASE 3: Copy shard files from A to B");

    let node_b_temp = TempDir::new()?;
    copy_shard_dirs(&node_a.config().data_root, node_b_temp.path())?;
    println!("  B's data directory seeded with 20 events\n");

    // ========================================
    // Phase 4: Write more events to A (S3 fallback, no follower)
    // ========================================
    println!("PHASE 4: Write more events to A (leader, no follower -> S3 fallback)");

    let mut client_a = CeleriantClient::connect(node_a.address()).await?;

    for key in &keys {
        write_event(&mut client_a, key, 6, false).await?; // small
        write_large_event(&mut client_a, key, 7, LARGE_PAYLOAD).await?; // large
    }

    assert_event_counts(&mut client_a, &keys, 7, "leader-solo").await?;
    println!("  Node A has 7 events per shard (28 total), B's copy has 5\n");

    // ========================================
    // Phase 5: Join second node (varies by scenario)
    // ========================================
    println!("PHASE 5: Join nodes ({:?})", join_order);

    drop(client_a);

    match join_order {
        JoinOrder::LeaderFirst => {
            // A stays running, B joins as follower
            let node_b = TestServer::start_with_existing_dir(
                port_b,
                distributed_config,
                "node-b".into(),
                node_b_temp,
            )
            .await?;

            println!("  Waiting for cluster formation and S3 lease expiry...");
            tokio::time::sleep(Duration::from_secs(12)).await;

            let a_leader = is_leader(node_a.address()).await?;
            let b_leader = is_leader(node_b.address()).await?;
            println!("  A is_leader={}, B is_leader={}", a_leader, b_leader);
            assert!(a_leader, "Node A must be leader (started first)");
            assert!(!b_leader, "Node B must be follower");

            verify_cluster(node_a.address(), node_b.address(), &keys).await?;
        }
        JoinOrder::FollowerFirst => {
            // Stop A, start B first
            node_a.stop();
            println!("  Node A stopped");

            let node_b = TestServer::start_with_existing_dir(
                port_b,
                distributed_config.clone(),
                "node-b".into(),
                node_b_temp,
            )
            .await?;

            // B catches up from S3, waits for A's S3 lease to expire, then becomes leader
            println!("  Waiting for B to catch up from S3, S3 lease expiry, and become leader...");
            tokio::time::sleep(Duration::from_secs(12)).await;

            assert!(
                is_leader(node_b.address()).await?,
                "Node B must be leader (only node running)"
            );

            // Restart A, it joins as follower
            node_a.restart_with_config(distributed_config).await?;
            println!("  Node A restarted, joining as follower...");
            tokio::time::sleep(Duration::from_secs(10)).await;

            let a_leader = is_leader(node_a.address()).await?;
            println!("  A is_leader={}", a_leader);
            assert!(!a_leader, "Node A must be follower");

            verify_cluster(node_b.address(), node_a.address(), &keys).await?;
        }
    }

    Ok(())
}

async fn verify_cluster(
    leader_addr: &str,
    follower_addr: &str,
    keys: &[AggregateKey],
) -> Result<(), Box<dyn std::error::Error>> {
    // ========================================
    // Phase 6: Verify data integrity and replication
    // ========================================
    println!("\nPHASE 6: Verify data integrity");

    println!("  Waiting for replication to settle...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut leader_client = CeleriantClient::connect(leader_addr).await?;
    let mut follower_client = CeleriantClient::connect(follower_addr).await?;

    assert_event_counts(&mut leader_client, keys, 7, "leader").await?;
    assert_event_counts(&mut follower_client, keys, 7, "follower").await?;
    println!("  Both nodes have 7 events per shard (28 total)");

    // Write new events in distributed mode
    println!("  Writing event 8 per shard to leader...");
    for key in keys {
        write_event(&mut leader_client, key, 8, false).await?;
    }

    assert_event_counts(&mut leader_client, keys, 8, "leader-final").await?;
    assert_event_counts(&mut follower_client, keys, 8, "follower-final").await?;
    println!("  Both nodes have 8 events per shard (32 total)");

    // Verify follower rejects writes
    let reject = write_event(&mut follower_client, &keys[0], 99, false).await;
    assert!(reject.is_err(), "Follower must reject writes");
    println!("  Follower correctly rejects writes");

    println!("\n=== Scenario Passed ===\n");
    Ok(())
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Standalone-to-Distributed Transition Test ===\n");

    let base = 13500 + (std::process::id() % 50) as u16;

    run_scenario(JoinOrder::LeaderFirst, base).await?;
    run_scenario(JoinOrder::FollowerFirst, base + 200).await?;

    println!("=== All Scenarios Passed ===");
    println!("Standalone-to-distributed transition validated for both join orders.\n");
    Ok(())
}
