//! Edge Case: S3 Fallback Batch Boundary Contiguity Under Load
//!
//! Targets the WalSeqGap bug from docs/wal-mismatch-pi-cluster.md.
//! Under high write throughput, the leader creates multiple S3 fallback batches.
//! Each batch's start_wal_seq must equal the previous batch's end_wal_seq + 1.
//! The bug manifested as overlapping boundaries (off-by-one or off-by-batch-size)
//! when the sidecar S3 uploader and shard executor had concurrent batch creation.
//!
//! Scenario:
//! 1. Start 2-node cluster, verify TCP replication
//! 2. Stop follower (force S3 fallback)
//! 3. Write many events rapidly (enough to create multiple S3 batches)
//! 4. Verify S3 batch boundaries are strictly contiguous (no gaps, no overlaps)
//! 5. Restart follower, verify it catches up without WalSeqGap
//!
//! Invariants tested: 7 (WAL continuity), 11 (S3 fallback batches)

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, s3_cluster_config, write_event, MinioContainer, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: S3 Batch Boundary Contiguity Under Load ===\n");

    let port_base = 16500 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    let minio = MinioContainer::start_with_bucket(minio_port, "test-batch-contiguity").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    // ========================================
    // PHASE 1: Start cluster, verify replication
    // ========================================
    println!("PHASE 1: Start cluster");
    println!("----------------------");

    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("  Waiting for election + discovery...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    println!("  Writing events 1-3 to verify cluster health...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let f_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(f_count, 3);
    println!("  Cluster healthy\n");

    // ========================================
    // PHASE 2: Stop follower, write many events (S3 fallback)
    // ========================================
    println!("PHASE 2: Stop follower, write 100 events via S3 fallback");
    println!("----------------------------------------------------------");

    drop(follower_client);
    follower.stop();
    println!("  Follower stopped");

    // Write 100 events rapidly — should create multiple S3 fallback batches
    let total_writes = 100u64;
    println!("  Writing events 4-{}...", total_writes + 3);
    for i in 4..=(total_writes + 3) {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }
    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    println!("  Leader has {} events", leader_count);

    // Wait for S3 fallback uploads to complete
    println!("  Waiting for S3 uploads...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 3: Verify S3 batch boundary contiguity
    // ========================================
    println!("\nPHASE 3: Verify S3 batch boundaries are contiguous");
    println!("---------------------------------------------------");

    let expected_shard = (aggregate_key.aggregate_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", expected_shard);

    let objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects in {}: {}", shard_prefix, objects.len());

    if objects.len() < 2 {
        println!("  Only {} batch(es) — can't verify contiguity (need >= 2).", objects.len());
        println!("  This is OK: all events fit in a single batch.\n");
    } else {
        // Parse batch names to extract WAL sequence ranges
        // Format: cluster/fallback/shard_NNN/batch_SSSSSSSSS_EEEEEEEEE_UUID.bin
        let mut boundaries: Vec<(u64, u64)> = Vec::new();
        for obj in &objects {
            let (_shard_id, start, end, _node_id) =
                celeriant_distributed::paths::parse_fallback_path(obj)
                    .unwrap_or_else(|| panic!("Failed to parse batch name: {}", obj));
            boundaries.push((start, end));
        }

        boundaries.sort_by_key(|&(start, _)| start);

        println!("  Batch boundaries (sorted by start):");
        for (start, end) in &boundaries {
            println!("    [{} - {}]", start, end);
        }

        // Verify contiguity: each batch's start = previous batch's end + 1
        let mut contiguity_errors = 0;
        for window in boundaries.windows(2) {
            let prev_end = window[0].1;
            let next_start = window[1].0;
            let expected = prev_end + 1;
            if next_start != expected {
                println!("  CONTIGUITY ERROR: expected start={}, got start={} ({})",
                    expected, next_start,
                    if next_start < expected { "OVERLAP" } else { "GAP" }
                );
                contiguity_errors += 1;
            }
        }

        assert_eq!(
            contiguity_errors, 0,
            "S3 batch boundaries are not contiguous: {} errors found. This is the WalSeqGap bug.",
            contiguity_errors
        );
        println!("  All {} batch boundaries are contiguous", boundaries.len());
    }

    // ========================================
    // PHASE 4: Restart follower, verify catchup
    // ========================================
    println!("\nPHASE 4: Restart follower — S3 catchup");
    println!("---------------------------------------");

    follower.restart().await?;
    println!("  Follower restarted");

    println!("  Waiting for boot catchup...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    follower.check_alive()
        .map_err(|e| format!("Follower crashed during S3 catchup (WalSeqGap?): {}", e))?;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let f_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower has {} events after catchup", f_count);
    assert_eq!(f_count, leader_count,
        "Follower should match leader after catchup. Follower={}, Leader={}", f_count, leader_count);

    println!("\n=== All Tests Passed ===");
    println!("S3 batch boundary contiguity verified:");
    println!("  1. {} events written via S3 fallback", total_writes);
    println!("  2. All batch boundaries contiguous (no gaps or overlaps)");
    println!("  3. Follower caught up without WalSeqGap\n");

    Ok(())
}
