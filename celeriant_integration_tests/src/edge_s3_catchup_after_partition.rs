//! Edge Case: S3 Catchup After Network Partition (WAL Sequence Gap Bug)
//!
//! Reproduces the real-world bug from docs/wal-mismatch-pi-cluster.md where
//! S3 catchup crashes with WalSeqGap after a network partition + failover.
//!
//! Bug scenario:
//! 1. Leader (A) writes events, some replicated via TCP, some via S3 fallback
//! 2. Network partition: A goes offline
//! 3. Follower (B) takes over as leader, writes more events (S3 fallback, no follower)
//! 4. A comes back, demotes to follower, runs S3 catchup
//! 5. BUG: S3 batches from B don't align with A's local WAL position → WalSeqGap
//!
//! The test verifies that the catchup correctly handles the transition point
//! between A's existing WAL and B's S3 batches.
//!
//! Invariants tested: 7 (WAL sequence continuity), 10 (post-election S3 catchup),
//!   13 (WAL divergence recovery)

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, s3_cluster_config, write_event, MinioContainer, TestServer, TcpProxy};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: S3 Catchup After Network Partition ===\n");
    println!("Reproduces WAL sequence gap bug from Pi cluster.\n");

    let port_base = 16100 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 100;
    let proxy_port = port_base + 200;
    let minio_port = port_base + 10;

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    println!("  Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-catchup-partition").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    // ========================================
    // PHASE 1: Start cluster with TcpProxy, write and replicate events
    // ========================================
    println!("PHASE 1: Start cluster, write events with normal replication");
    println!("-------------------------------------------------------------");

    let mut a_config = config.clone();
    a_config.client_port = node_a_port;
    println!("  Starting node A (leader) on port {}...", node_a_port);
    let mut node_a = TestServer::start_with_config_labeled(node_a_port, a_config, "node-a".into()).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let b_repl_port = node_b_port + 1;
    println!("  Starting TcpProxy: {} -> {}", proxy_port, b_repl_port);
    let _proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", b_repl_port)).await?;

    let mut b_config = config.clone();
    b_config.client_port = node_b_port;
    b_config.advertised_replication_address = Some(format!("127.0.0.1:{}", proxy_port));
    println!("  Starting node B (follower) on port {}...", node_b_port);
    let _node_b = TestServer::start_with_config_labeled(node_b_port, b_config, "node-b".into()).await?;

    println!("  Waiting for election + discovery + S3 lease expiry...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut client_a = CeleriantClient::connect(&format!("127.0.0.1:{}", node_a_port)).await?;
    let mut client_b = CeleriantClient::connect(&format!("127.0.0.1:{}", node_b_port)).await?;

    // Write a batch of events while both nodes are healthy
    println!("  Writing events 1-10 through node A...");
    for i in 1..=10 {
        write_event(&mut client_a, &aggregate_key, i, i == 1).await?;
    }

    let b_count = count_events(&mut client_b, &aggregate_key).await?;
    assert_eq!(b_count, 10, "Node B should have 10 events via TCP replication");
    println!("  Node B has {} events via TCP replication\n", b_count);

    // ========================================
    // PHASE 2: Simulate network partition (kill node A)
    // ========================================
    println!("PHASE 2: Kill node A (simulate network partition)");
    println!("--------------------------------------------------");

    drop(client_a);
    node_a.stop();
    println!("  Node A stopped");

    println!("  Waiting for node B to detect heartbeat loss and take over...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 3: Write events on new leader (B) — S3 fallback (no follower)
    // ========================================
    println!("\nPHASE 3: Write events on new leader (B) — S3 fallback");
    println!("------------------------------------------------------");

    println!("  Writing events 11-30 through node B (new leader)...");
    for i in 11..=30 {
        write_event(&mut client_b, &aggregate_key, i, false).await?;
    }
    let b_count = count_events(&mut client_b, &aggregate_key).await?;
    assert_eq!(b_count, 30, "Node B should have 30 events");
    println!("  Node B has {} events. S3 fallback active (no follower).", b_count);

    // Wait for S3 fallback writes to complete
    println!("  Waiting for S3 fallback writes...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify S3 fallback objects exist
    let mut total_s3_objects = 0;
    for shard_id in 0..num_shards {
        let prefix = format!("cluster/fallback/shard_{:03}/", shard_id);
        let objs = minio.list_objects(&prefix).await?;
        total_s3_objects += objs.len();
    }
    println!("  Total S3 fallback objects across all shards: {}", total_s3_objects);
    assert!(total_s3_objects > 0, "Expected S3 fallback objects from B's writes");

    // ========================================
    // PHASE 4: Restart node A — should catch up from S3 without WalSeqGap
    // ========================================
    println!("\nPHASE 4: Restart node A (catches up from S3)");
    println!("---------------------------------------------");

    println!("  Restarting node A...");
    node_a.restart().await?;

    // This is the critical phase: A's WAL has events 1-10. B uploaded events 11-30
    // to S3. A must catch up from S3 without WalSeqGap.
    println!("  Waiting for boot catchup + S3 catchup + cluster rejoin...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Verify A is alive (didn't crash with WalSeqGap)
    node_a.check_alive()
        .map_err(|e| format!("Node A crashed during S3 catchup (WalSeqGap bug?): {}", e))?;
    println!("  Node A is alive (no WalSeqGap crash)");

    let mut client_a = CeleriantClient::connect(&format!("127.0.0.1:{}", node_a_port)).await?;
    let a_count = count_events(&mut client_a, &aggregate_key).await?;
    println!("  Node A has {} events after catchup", a_count);
    assert_eq!(a_count, 30, "Node A should have all 30 events after S3 catchup");

    // ========================================
    // PHASE 5: Verify continued replication works
    // ========================================
    println!("\nPHASE 5: Verify continued replication B -> A");
    println!("----------------------------------------------");

    for i in 31..=33 {
        write_event(&mut client_b, &aggregate_key, i, false).await?;
    }

    drop(client_a);
    let mut client_a = CeleriantClient::connect(&format!("127.0.0.1:{}", node_a_port)).await?;
    let a_final = count_events(&mut client_a, &aggregate_key).await?;
    assert_eq!(a_final, 33, "Node A should have 33 events after replication");
    println!("  Replication B -> A working: node A has {} events", a_final);

    println!("\n=== All Tests Passed ===");
    println!("S3 catchup after partition validated:");
    println!("  1. TCP replication worked for events 1-10");
    println!("  2. Node A killed, node B took over");
    println!("  3. Node B wrote events 11-30 via S3 fallback");
    println!("  4. Node A restarted, caught up from S3 (no WalSeqGap)");
    println!("  5. Continued replication works (events 31-33)\n");

    Ok(())
}
