//! Edge Case: Stale Cache Entry After Log Rotation + Eviction
//!
//! Regression guard for the bug fixed in commit 29756fe where `log_id` and
//! `metablock_absolute_pos` in `MemSnapshotAggregate` were not updated on LRU
//! eviction + log rotation.
//!
//! Scenario:
//! 1. Start two-node cluster with tiny aggregate snapshot cache (64KB) and
//!    2MB log preallocate size.
//! 2. Phase A: Create 300 aggregates — fills and overflows the 64KB LRU cache.
//! 3. Phase B: Write large events to force log rotation (~2.4MB of WAL writes).
//! 4. Phase C: Write a second event to the first 100 of the original aggregates
//!    (these were evicted from LRU; stale log_id pointers trigger the bug).
//! 5. Phase D: Read back all 100 aggregates and assert each has exactly 2 events.
//! 6. Wait for replication, verify follower has identical counts.
//!
//! This is test #9 in the integration test coverage report.
//!
//! Run with: cargo run --bin edge_stale_cache_rotation_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    poll_converged_count, s3_cluster_config, write_event, write_large_event, MinioContainer,
    TestServer, FOLLOWER_CONVERGENCE_TIMEOUT,
};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

async fn verify_aggregate_counts(
    client: &mut CeleriantClient,
    label: &str,
    range: std::ops::RangeInclusive<u128>,
    agg_type_id: u128,
    category_id: u128,
    expected_count: usize,
) -> Vec<String> {
    let mut failures = Vec::new();
    for i in range {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        let count = poll_converged_count(client, &key, expected_count, FOLLOWER_CONVERGENCE_TIMEOUT)
            .await
            .unwrap_or(0);
        if count != expected_count {
            failures.push(format!(
                "{} aggregate {}: expected {} events, got {}",
                label, i, expected_count, count
            ));
        }
    }
    failures
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: Stale Cache After Log Rotation + Eviction ===\n");

    let port_base = 14100 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-stale-cache").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    // Build config: small memory budget (forces LRU eviction) + small log preallocate (forces rotation).
    let base_config = s3_cluster_config(
        2,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );
    let config = crate::ServerConfig {
        // 2MB log preallocate — 50 × 32KB events (~1.6MB) cause rotation.
        shard_log_preallocate_bytes: 2 * 1024 * 1024,
        // Small memory budget per shard — forces cache eviction
        memory_budget_bytes: Some(512 * 1024),
        routing_rule: RoutingRule::AggregateTypeId,
        // Very high water mark: this test is NOT about S3 fallback. Prevent the
        // replication queue from triggering S3 during the rapid Phase B writes.
        internode_max_request_size: 100_000_000,
        ..base_config
    };

    println!("Starting leader on port {}...", leader_port);
    let leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into())
            .await?;
    println!("Starting follower on port {}...", follower_port);
    let follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!(
        "  Leader at {}, Follower at {}",
        leader.address(),
        follower.address()
    );

    println!("Waiting for cluster stabilization (8s)...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // agg_type_id=1, category_id=1 → shard = 1 % 2 = 1. All 500 aggregates go to the same shard.
    let agg_type_id: u128 = 1;
    let category_id: u128 = 1;

    // ========================================
    // Phase A: Create 300 aggregates (fills + overflows LRU cache)
    // ========================================
    println!("\nPHASE A: Creating 300 aggregates to overflow LRU cache");
    println!("--------------------------------------------------------");

    for i in 1u128..=300 {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        write_event(&mut leader_client, &key, 1, true)
            .await
            .map_err(|e| format!("ERROR: Phase A write failed for aggregate {}: {}", i, e))?;

        if i % 100 == 0 {
            println!("  Created {} aggregates...", i);
        }
    }
    println!("  Phase A complete: 300 aggregates created");

    // ========================================
    // Phase B: Force log rotation via large writes (~1.6MB)
    // ========================================
    println!("\nPHASE B: Forcing log rotation via large writes (~1.6MB)");
    println!("----------------------------------------------------------");

    // Write to aggregate 301 (a fresh one). 50 writes × 32KB = ~1.6MB > 1.5MB data space.
    let rotation_key = AggregateKey::new(agg_type_id, category_id, 301u128);
    write_event(&mut leader_client, &rotation_key, 1, true)
        .await
        .map_err(|e| format!("ERROR: Phase B initial write failed: {}", e))?;

    for i in 2u64..=50 {
        write_large_event(&mut leader_client, &rotation_key, i, 32768)
            .await
            .map_err(|e| format!("ERROR: Phase B large write {} failed: {}", i, e))?;

        if i % 25 == 0 {
            println!("  {} large events written...", i);
        }
    }
    println!("  Phase B complete: log rotation triggered");

    // ========================================
    // Phase C: Write event 2 to the first 100 aggregates (post-eviction)
    // ========================================
    println!("\nPHASE C: Writing second event to 100 evicted aggregates");
    println!("---------------------------------------------------------");

    for i in 1u128..=100 {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        write_event(&mut leader_client, &key, 2, false)
            .await
            .map_err(|e| format!("ERROR: Phase C write failed for aggregate {}: {}", i, e))?;

        if i % 50 == 0 {
            println!("  Wrote second event to {} aggregates...", i);
        }
    }
    println!("  Phase C complete: second event written to 100 aggregates");

    // ========================================
    // Phase D: Verify each of the 200 aggregates has exactly 2 events
    // ========================================
    println!("\nPHASE D: Verifying event counts on leader");
    println!("-----------------------------------------");

    let leader_failures =
        verify_aggregate_counts(&mut leader_client, "Leader", 1..=100, agg_type_id, category_id, 2).await;

    println!("  All 100 aggregates have exactly 2 events on leader");

    // ========================================
    // Phase D continued: Verify on follower after replication
    // ========================================
    println!("Verifying event counts on follower");
    println!("-----------------------------------");

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_failures =
        verify_aggregate_counts(&mut follower_client, "Follower", 1..=100, agg_type_id, category_id, 2).await;

    let all_failures: Vec<_> = leader_failures.into_iter().chain(follower_failures).collect();
    assert!(
        all_failures.is_empty(),
        "Verification failed for {} aggregates:\n{}",
        all_failures.len(),
        all_failures.join("\n")
    );
    println!("  All 100 aggregates have exactly 2 events on follower");

    println!("\n=== PASS ===\n");

    Ok(())
}
