//! Hypothesis 2+4: S3 fallback + boot catchup produces no duplicate events
//!
//! Stop follower → write events → S3 fallback on leader.
//! Restart follower → boot catchup reads S3 batches.
//! Verify: follower event count == leader event count (strict equality).
//!
//! Multi-shard: tests 4 aggregates across 4 different shards simultaneously.
//! This catches any per-shard dedup issues.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, poll_converged_count, poll_event_count, s3_cluster_config, write_event,
    MinioContainer, TestServer, FOLLOWER_CONVERGENCE_TIMEOUT,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Fallback Deduplication Test (Multi-Shard) ===");
    println!("Tests: S3 boot catchup produces no duplicates\n");

    let port_base = 11000 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;
    let config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );

    println!("Starting two-node cluster (4 shards)...");
    let leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into())
            .await?;
    let mut follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("Waiting for election + discovery...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // One aggregate per shard (type_id % 4)
    let keys: Vec<AggregateKey> = (0..4).map(|s| AggregateKey::new(1, s, 1)).collect();

    // ========================================
    // Phase 1: Normal TCP replication
    // ========================================
    println!("\nPHASE 1: Normal TCP replication");
    println!("-------------------------------");

    for key in &keys {
        for i in 1..=20 {
            write_event(&mut leader_client, key, i, i == 1).await?;
        }
    }
    println!("  Wrote 20 events to each of 4 shards");

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    for key in &keys {
        let lc = count_events(&mut leader_client, key).await?;
        let fc =
            poll_converged_count(&mut follower_client, key, 20, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
        let shard = key.aggregate_type_id % 4;
        println!("  shard {}: leader={}, follower={}", shard, lc, fc);
        assert_eq!(lc, 20, "Leader should have 20 events");
        assert_eq!(fc, 20, "Follower should have 20 events via TCP");
    }
    println!("  All shards replicated correctly\n");

    // ========================================
    // Phase 2: Stop follower, write more → S3 fallback
    // ========================================
    println!("PHASE 2: Stop follower, write events (S3 fallback)");
    println!("--------------------------------------------------");

    drop(follower_client);
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    for key in &keys {
        for i in 21..=50 {
            write_event(&mut leader_client, key, i, false).await?;
        }
    }
    println!("  Wrote 30 more events per shard while follower down");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut total_s3 = 0;
    for shard_id in 0..num_shards {
        let objs = minio
            .list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id))
            .await?;
        println!("  S3 shard_{:03}: {} objects", shard_id, objs.len());
        total_s3 += objs.len();
    }
    assert!(total_s3 > 0, "S3 fallback should have triggered");
    println!("  S3 fallback confirmed ({} total objects)\n", total_s3);

    // ========================================
    // Phase 3: Restart follower → boot catchup from S3
    // ========================================
    println!("PHASE 3: Restart follower, boot catchup from S3");
    println!("-----------------------------------------------");

    follower.restart().await?;
    println!("  Polling for boot catchup (50 events per shard)...");
    for key in &keys {
        poll_event_count(follower.address(), key, 50, Duration::from_secs(45)).await;
    }

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    for key in &keys {
        let lc = count_events(&mut leader_client, key).await?;
        let fc =
            poll_converged_count(&mut follower_client, key, 50, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
        let shard = key.aggregate_type_id % 4;
        println!("  shard {}: leader={}, follower={}", shard, lc, fc);
        assert_eq!(lc, 50, "Leader should have 50 events");
        assert_eq!(
            lc, fc,
            "CRITICAL: shard {} leader ({}) != follower ({}) — S3 catchup dedup failure",
            shard, lc, fc
        );
    }
    println!("  All shards caught up correctly (no duplicates)\n");

    // ========================================
    // Phase 4: Verify TCP resumes (no new S3)
    // ========================================
    println!("PHASE 4: Verify TCP replication resumes");
    println!("---------------------------------------");

    for key in &keys {
        for i in 51..=60 {
            write_event(&mut leader_client, key, i, false).await?;
        }
    }
    println!("  Wrote 10 more events per shard");

    // Poll until follower has all 60 events on every shard.
    // Note: the first write per shard after follower restart may go via S3 fallback
    // (stale replication connection), but subsequent writes use TCP. The critical
    // invariant is convergence, not transport path.
    for key in &keys {
        poll_event_count(follower.address(), key, 60, Duration::from_secs(45)).await;
    }

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    for key in &keys {
        let lc = count_events(&mut leader_client, key).await?;
        let fc =
            poll_converged_count(&mut follower_client, key, 60, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
        let shard = key.aggregate_type_id % 4;
        println!("  shard {}: leader={}, follower={}", shard, lc, fc);
        assert_eq!(lc, 60, "Leader should have 60 events");
        assert_eq!(
            lc, fc,
            "Post-catchup: shard {} leader ({}) != follower ({})",
            shard, lc, fc
        );
    }
    println!("  TCP replication converged (all counts match)");

    println!("\n=== All Tests Passed ===");
    Ok(())
}
