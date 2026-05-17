//! Edge Case: Overlapping S3 Batches After Network Partition
//!
//! Reproduces the WalSeqGap bug from docs/wal-mismatch-pi-cluster.md where
//! S3 has fallback batches from two different leadership terms with overlapping
//! WAL sequence ranges.
//!
//! Bug scenario (from Pi cluster):
//! 1. Leader A writes events, follower B replicates via TCP
//! 2. B goes offline — A falls back to S3 for new writes
//! 3. A goes offline (cable unplugged) — S3 has A's fallback batches
//! 4. B comes back, takes over as leader, writes new events via S3 fallback
//! 5. S3 now has batches from BOTH leaders with overlapping WAL indices
//! 6. A restarts, runs S3 catchup — must handle the overlap without WalSeqGap
//!
//! The overlap occurs because A uploaded S3 batches starting at WAL sequence N,
//! then B took over and started uploading from its own WAL position (which may
//! be < N since B didn't receive A's last S3 fallback writes via TCP).
//!
//! Invariants tested: 7 (WAL continuity), 10 (S3 catchup), 13 (divergence recovery)

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, s3_cluster_config, write_event, MinioContainer, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: Overlapping S3 Batches After Partition ===\n");

    let port_base = 16300 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 100;
    let minio_port = port_base + 10;

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    println!("  Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-s3-overlap").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    // ========================================
    // PHASE 1: Start cluster, write events with TCP replication
    // ========================================
    println!("PHASE 1: Normal cluster, TCP replication");
    println!("-----------------------------------------");

    let mut node_a = TestServer::start_with_config_labeled(node_a_port, config.clone(), "node-a".into()).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut node_b = TestServer::start_with_config_labeled(node_b_port, config.clone(), "node-b".into()).await?;

    println!("  Waiting for election + discovery + S3 lease expiry...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut client_a = CeleriantClient::connect(&format!("127.0.0.1:{}", node_a_port)).await?;
    let mut client_b = CeleriantClient::connect(&format!("127.0.0.1:{}", node_b_port)).await?;

    println!("  Writing events 1-10...");
    for i in 1..=10 {
        write_event(&mut client_a, &aggregate_key, i, i == 1).await?;
    }
    let b_count = count_events(&mut client_b, &aggregate_key).await?;
    assert_eq!(b_count, 10, "B should have 10 events");
    println!("  Both nodes have 10 events via TCP\n");

    // ========================================
    // PHASE 2: Stop B, A writes via S3 fallback
    // ========================================
    println!("PHASE 2: Stop B — A writes via S3 fallback");
    println!("--------------------------------------------");

    drop(client_b);
    node_b.stop();
    println!("  B stopped");

    println!("  Writing events 11-20 on A (S3 fallback, no follower)...");
    for i in 11..=20 {
        write_event(&mut client_a, &aggregate_key, i, false).await?;
    }
    println!("  A has 20 events");

    // Wait for S3 fallback uploads to complete
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut a_s3_objects = 0;
    for shard_id in 0..num_shards {
        let prefix = format!("cluster/fallback/shard_{:03}/", shard_id);
        let objs = minio.list_objects(&prefix).await?;
        if !objs.is_empty() {
            println!("  shard_{:03}: {} S3 objects from A", shard_id, objs.len());
        }
        a_s3_objects += objs.len();
    }
    println!("  Total S3 objects from A's leadership: {}\n", a_s3_objects);

    // ========================================
    // PHASE 3: Stop A, restart B — B takes over, writes via S3 fallback
    // ========================================
    println!("PHASE 3: Stop A, restart B — B takes over as leader");
    println!("----------------------------------------------------");

    drop(client_a);
    node_a.stop();
    println!("  A stopped (S3 still has A's fallback batches)");

    node_b.restart().await?;
    println!("  B restarted");

    println!("  Waiting for B to win election + S3 catchup...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut client_b = CeleriantClient::connect(&format!("127.0.0.1:{}", node_b_port)).await?;

    // B catches up from A's S3 batches first, then becomes leader
    let b_count = count_events(&mut client_b, &aggregate_key).await?;
    println!("  B has {} events after catchup", b_count);

    // B writes MORE events as leader — these also go to S3 (no follower)
    println!("  Writing events 21-30 on B (S3 fallback, no follower)...");
    for i in (b_count as u64 + 1)..=(b_count as u64 + 10) {
        write_event(&mut client_b, &aggregate_key, i, false).await?;
    }
    let b_final = count_events(&mut client_b, &aggregate_key).await?;
    println!("  B has {} events", b_final);

    // Wait for B's S3 fallback uploads
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut total_s3_objects = 0;
    for shard_id in 0..num_shards {
        let prefix = format!("cluster/fallback/shard_{:03}/", shard_id);
        let objs = minio.list_objects(&prefix).await?;
        if !objs.is_empty() {
            println!("  shard_{:03}: {} S3 objects (from both A and B)", shard_id, objs.len());
        }
        total_s3_objects += objs.len();
    }
    println!("  Total S3 objects from both leaders: {} (was {} from A alone)\n",
        total_s3_objects, a_s3_objects);

    // ========================================
    // PHASE 4: Restart A — must catch up from S3 with overlapping batches
    // ========================================
    println!("PHASE 4: Restart A — S3 catchup with overlapping batches");
    println!("----------------------------------------------------------");

    node_a.restart().await?;
    println!("  A restarted (WAL has events 1-20, S3 has batches from both A and B)");

    println!("  Waiting for S3 catchup (the critical moment — WalSeqGap bug)...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Verify A is alive (didn't crash with WalSeqGap)
    node_a.check_alive()
        .map_err(|e| format!("Node A crashed during S3 catchup (WalSeqGap bug): {}", e))?;
    println!("  Node A is alive (no WalSeqGap crash)");

    let mut client_a = CeleriantClient::connect(&format!("127.0.0.1:{}", node_a_port)).await?;
    let a_count = count_events(&mut client_a, &aggregate_key).await?;
    println!("  Node A has {} events after catchup", a_count);
    assert_eq!(a_count, b_final,
        "A should match B's count after catchup. A={}, B={}", a_count, b_final);

    // ========================================
    // PHASE 5: Verify continued replication
    // ========================================
    println!("\nPHASE 5: Verify replication B -> A");
    println!("------------------------------------");

    let next_event = b_final as u64 + 1;
    for i in next_event..next_event + 3 {
        write_event(&mut client_b, &aggregate_key, i, false).await?;
    }

    drop(client_a);
    let mut client_a = CeleriantClient::connect(&format!("127.0.0.1:{}", node_a_port)).await?;
    let a_final = count_events(&mut client_a, &aggregate_key).await?;
    let b_count = count_events(&mut client_b, &aggregate_key).await?;
    assert_eq!(a_final, b_count, "Both should converge. A={}, B={}", a_final, b_count);
    println!("  Replication working: A={}, B={}", a_final, b_count);

    println!("\n=== All Tests Passed ===");
    println!("Overlapping S3 batches handled correctly:");
    println!("  1. A wrote events 11-20 to S3 as leader");
    println!("  2. B caught up from A's batches, then wrote events 21-30 to S3");
    println!("  3. A restarted with overlapping S3 batches — no WalSeqGap crash");
    println!("  4. Both nodes converged, replication resumed\n");

    Ok(())
}
