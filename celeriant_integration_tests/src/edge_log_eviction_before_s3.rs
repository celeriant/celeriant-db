//! Edge Case: Log File Evicted Before S3 Upload
//!
//! Regression guard for the risk where the log rotates, the old file is evicted
//! from the open-file LRU cache (max_open_files = 2), and then the S3 upload
//! needs data from the evicted file.  The shard must re-open the evicted file
//! from disk transparently.
//!
//! Scenario:
//! 1. Start a 2-shard cluster with max_open_files=2 and shard_log_preallocate=2MB.
//! 2. Stop the follower so the leader must fall back to S3 for durability.
//! 3. Write ~100 large events (32KB each) = ~3.2MB, causing 3+ log rotations.
//!    With max_open_files=2, earlier log files are evicted before S3 can upload them.
//! 4. Verify S3 objects exist.
//! 5. Restart follower, wait for S3 catchup.
//! 6. Verify: follower event count matches leader.
//!
//! This is test #8 in the integration test coverage report.
//!
//! Run with: cargo run --bin edge_log_eviction_before_s3_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, s3_cluster_config, write_event, write_large_event, MinioContainer, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: Log File Evicted Before S3 Upload ===\n");

    let port_base = 15500 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-log-eviction-s3").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 2;

    // Focus all writes on a single shard for predictable rotation counting.
    // AggregateTypeId routing: agg_type_id % num_shards.
    // agg_type_id=1 -> shard 1 % 2 = 1.
    let agg_type_id: u128 = 1;
    let category_id: u128 = 1;
    let target_shard = (agg_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", target_shard);

    let base_config = s3_cluster_config(
        num_shards,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );

    let config = crate::ServerConfig {
        // 2MB preallocate — 10 aggs × 10 events × 32KB = 3.2MB forces 3+ rotations.
        shard_log_preallocate_bytes: 2 * 1024 * 1024,
        // max_open_files=2: with 3+ log files, older ones are evicted before S3 upload.
        max_open_files: 2,
        // Very high water mark: prevent unintended S3 fallback during the write phase
        // while the follower is stopped. The test relies on S3 uploads triggered by the
        // stopped follower, not by queue pressure.
        internode_max_request_size: 100_000_000,
        ..base_config
    };

    println!("Starting two-node cluster (2 shards, 2MB preallocate, max_open_files=2)...");
    let leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into())
            .await?;
    let mut follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("Waiting for cluster stabilization (8s)...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // ========================================
    // Phase 1: Normal replication — verify cluster is up
    // ========================================
    println!("\nPHASE 1: Verify initial replication");
    println!("------------------------------------");

    let probe_key = AggregateKey::new(agg_type_id, category_id, 1);
    write_event(&mut leader_client, &probe_key, 1, true).await?;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &probe_key).await?;
    assert_eq!(follower_count, 1, "Initial replication should deliver 1 event to follower");
    drop(follower_client);
    println!("  Initial replication OK (1 event replicated)");

    // ========================================
    // Phase 2: Stop follower — leader falls back to S3
    // ========================================
    println!("\nPHASE 2: Stop follower, leader switches to S3 fallback");
    println!("-------------------------------------------------------");

    println!("  Stopping follower...");
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ========================================
    // Phase 3: Write 100 large events to force 3+ log rotations
    // ========================================
    println!("\nPHASE 3: Writing ~100 large events (32KB each) to shard {} to force 3+ rotations", target_shard);
    println!("-----------------------------------------------------------------------");

    // Use multiple aggregates to spread writes, all on the same shard.
    // Aggregates 1-10 on shard 1 (agg_type_id=1, category_id=1, aggregate_id=1..10).
    // 10 aggs × 10 events × 32KB = 3.2MB → 3+ rotations with 2MB preallocate.
    let events_per_agg = 10u64; // 10 aggs × 10 events = 100 total
    let num_aggs = 10u128;

    // Aggregate 1 already has event 1 from Phase 1.
    for agg_id in 2u128..=num_aggs {
        let key = AggregateKey::new(agg_type_id, category_id, agg_id);
        write_event(&mut leader_client, &key, 1, true).await?;
    }

    for i in 2u64..=events_per_agg {
        for agg_id in 1u128..=num_aggs {
            let key = AggregateKey::new(agg_type_id, category_id, agg_id);
            write_large_event(&mut leader_client, &key, i, 32768)
                .await
                .map_err(|e| format!("ERROR: large write {} to agg {} failed: {}", i, agg_id, e))?;
        }

        if i % 5 == 0 {
            let written = (i - 1) * num_aggs as u64 + num_aggs as u64;
            println!("  ~{} total events written...", written);
        }
    }

    let total_written = events_per_agg * num_aggs as u64;
    let approx_bytes = total_written * 32768;
    println!(
        "  Write phase complete: ~{} events, ~{}MB on shard {}",
        total_written,
        approx_bytes / (1024 * 1024),
        target_shard
    );

    // ========================================
    // Phase 4: Verify S3 fallback uploads (completed inline during durable writes)
    // ========================================
    println!("\nPHASE 4: Verify S3 fallback uploads");
    println!("-------------------------------------");

    let s3_objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects in {}: {}", shard_prefix, s3_objects.len());
    for obj in s3_objects.iter().take(5) {
        println!("    - {}", obj);
    }
    if s3_objects.len() > 5 {
        println!("    ... and {} more", s3_objects.len() - 5);
    }

    assert!(
        !s3_objects.is_empty(),
        "Expected S3 fallback objects after follower stop and large writes"
    );
    println!("  S3 fallback uploads confirmed");

    // ========================================
    // Phase 5: Restart follower and wait for S3 catchup
    // ========================================
    println!("\nPHASE 5: Restart follower and wait for S3 catchup (30s)");
    println!("---------------------------------------------------------");

    follower.restart().await?;

    // Write a triggering event to force the leader's replication path to fire.
    // Without this, the leader has no new writes, never detects the follower is behind,
    // and never sends kicks to trigger further S3 catchup beyond the initial boot rounds.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let trigger_key = AggregateKey::new(agg_type_id, category_id, num_aggs + 1);
    write_event(&mut leader_client, &trigger_key, 1, true).await?;
    println!("  Wrote trigger event to force leader replication kick");

    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();
    let mut caught_up = false;

    let expected_per_agg = events_per_agg as usize;

    while start.elapsed() < timeout {
        if let Ok(mut fc) = CeleriantClient::connect(follower.address()).await {
            let count = count_events(&mut fc, &probe_key).await.unwrap_or(0);
            println!(
                "  agg[1] follower={}/{} ({:.0}s elapsed)",
                count,
                expected_per_agg,
                start.elapsed().as_secs_f64()
            );
            if count >= expected_per_agg {
                caught_up = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    assert!(
        caught_up,
        "Follower did not catch up from S3 within {}s (log file re-open from disk failed?)",
        timeout.as_secs()
    );

    // ========================================
    // Phase 6: Verify all aggregate counts
    // ========================================
    println!("\nPHASE 6: Verify event counts on follower");
    println!("-----------------------------------------");

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let mut all_pass = true;

    for agg_id in 1u128..=num_aggs {
        let key = AggregateKey::new(agg_type_id, category_id, agg_id);
        let follower_count = count_events(&mut follower_client, &key).await?;
        let leader_count = count_events(&mut leader_client, &key).await?;
        let ok = follower_count == leader_count;
        println!(
            "  agg[{}]: follower={} / leader={}  {}",
            agg_id,
            follower_count,
            leader_count,
            if ok { "OK" } else { "FAIL" }
        );
        if !ok {
            all_pass = false;
        }
    }

    assert!(
        caught_up && all_pass,
        "Follower did not catch up from S3 within {}s (log file re-open from disk failed?)",
        timeout.as_secs()
    );

    println!("\n=== PASS ===\n");

    Ok(())
}
