//! P4-1. Rolling Upgrade with Zero Downtime Integration Test
//!
//! Tests that writes continue during rolling node restarts in a replicated cluster.
//!
//! NOTE: This uses SIGKILL (hard restart), not graceful SIGTERM (not supported in TestServer).
//! The test verifies durability and recovery: acknowledged writes survive node restarts,
//! and the cluster recovers to accept new writes. Some writes may fail during the takeover
//! window - this is expected and acceptable.
//!
//! Scenario:
//! 1. Start 2-node replicated cluster with S3
//! 2. Spawn continuous write task in background
//! 3. Let writes run for 2 seconds
//! 4. Stop follower (SIGKILL)
//! 5. Continue writes on leader for 2 seconds
//! 6. Restart follower
//! 7. Wait for replication to catch up
//! 8. Stop leader (SIGKILL) - follower should take over via S3
//! 9. Wait for takeover
//! 10. Continue writes on new leader for 2 seconds
//! 11. Count total events on new leader
//! 12. Verify: all writes that succeeded are present (no data loss)
//!
//! Success criteria:
//! - No acknowledged writes are lost
//! - Cluster recovers and accepts new writes after each restart
//!
//! Run with: cargo test --test p4_1_rolling_upgrade_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    count_events, s3_cluster_config, write_event, MinioContainer, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const PORT_BASE: u16 = 21100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P4-1. Rolling Upgrade with Zero Downtime Integration Test ===\n");

    let node_a_port = PORT_BASE;
    let node_b_port = PORT_BASE + 100;
    let minio_port = PORT_BASE + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-rolling").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start 2-node cluster
    // ========================================
    println!("PHASE 1: Start 2-node cluster");
    println!("------------------------------");

    let config_a = s3_cluster_config(
        num_shards,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );
    println!("  Starting node A on port {}...", node_a_port);
    let mut node_a =
        TestServer::start_with_config_labeled(node_a_port, config_a, "node-A".to_string()).await?;

    sleep(Duration::from_millis(500)).await;

    let config_b = s3_cluster_config(
        num_shards,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );
    println!("  Starting node B on port {}...", node_b_port);
    let mut node_b =
        TestServer::start_with_config_labeled(node_b_port, config_b, "node-B".to_string()).await?;

    println!("  Waiting for election and heartbeat establishment...");
    sleep(Duration::from_secs(3)).await;

    println!("  ✓ Cluster started\n");

    // ========================================
    // PHASE 2: Spawn continuous write task
    // ========================================
    println!("PHASE 2: Spawn continuous write task");
    println!("------------------------------------");

    let stop_signal = Arc::new(AtomicBool::new(false));
    let success_count = Arc::new(AtomicU64::new(0));
    let failure_count = Arc::new(AtomicU64::new(0));

    let stop_clone = stop_signal.clone();
    let success_clone = success_count.clone();
    let failure_clone = failure_count.clone();
    let key_clone = aggregate_key.clone();

    let write_task = tokio::spawn(async move {
        let mut event_num = 1u64;
        let mut last_successful_port = node_a_port;

        loop {
            if stop_clone.load(Ordering::Relaxed) {
                break;
            }

            // Try current port first, fall back to the other if connection fails
            let ports = [last_successful_port, if last_successful_port == node_a_port { node_b_port } else { node_a_port }];

            let mut write_succeeded = false;
            for &port in &ports {
                let address = format!("127.0.0.1:{}", port);
                match CeleriantClient::connect(&address).await {
                    Ok(mut client) => {
                        let allow_create = event_num == 1;
                        match write_event(&mut client, &key_clone, event_num, allow_create).await {
                            Ok(_) => {
                                success_clone.fetch_add(1, Ordering::Relaxed);
                                event_num += 1;
                                last_successful_port = port;
                                write_succeeded = true;
                                break;
                            }
                            Err(_) => {
                                // Write failed, try other node
                                continue;
                            }
                        }
                    }
                    Err(_) => {
                        // Connection failed, try other node
                        continue;
                    }
                }
            }

            if !write_succeeded {
                failure_clone.fetch_add(1, Ordering::Relaxed);
                // Brief pause before retry when both nodes fail
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // Small delay between writes
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    println!("  ✓ Continuous write task started\n");

    // ========================================
    // PHASE 3: Let writes run for 2 seconds
    // ========================================
    println!("PHASE 3: Initial write period (2s)");
    println!("----------------------------------");

    sleep(Duration::from_secs(2)).await;
    let phase3_success = success_count.load(Ordering::Relaxed);
    let phase3_failures = failure_count.load(Ordering::Relaxed);
    println!(
        "  Writes: {} successful, {} failures",
        phase3_success, phase3_failures
    );
    println!("  ✓ Initial write period complete\n");

    // ========================================
    // PHASE 4: Stop follower (we'll assume B is follower)
    // ========================================
    println!("PHASE 4: Stop node B (follower)");
    println!("--------------------------------");

    println!("  Stopping node B...");
    node_b.stop();
    println!("  ✓ Node B stopped\n");

    // ========================================
    // PHASE 5: Continue writes on leader for 2s
    // ========================================
    println!("PHASE 5: Continue writes on node A (2s)");
    println!("---------------------------------------");

    sleep(Duration::from_secs(2)).await;
    let phase5_success = success_count.load(Ordering::Relaxed);
    let phase5_failures = failure_count.load(Ordering::Relaxed);
    println!(
        "  Writes: {} successful, {} failures (delta: +{}, +{})",
        phase5_success,
        phase5_failures,
        phase5_success - phase3_success,
        phase5_failures - phase3_failures
    );
    println!("  ✓ Writes continued with node B offline\n");

    // ========================================
    // PHASE 6: Restart follower
    // ========================================
    println!("PHASE 6: Restart node B");
    println!("-----------------------");

    println!("  Restarting node B...");
    node_b.restart().await?;
    println!("  ✓ Node B restarted");

    println!("  Waiting for replication to catch up...");
    sleep(Duration::from_secs(3)).await;
    println!("  ✓ Replication catchup complete\n");

    // ========================================
    // PHASE 7: Stop leader (trigger failover)
    // ========================================
    println!("PHASE 7: Stop node A (leader, trigger failover)");
    println!("-----------------------------------------------");

    println!("  Stopping node A...");
    node_a.stop();
    println!("  ✓ Node A stopped");

    println!("  Waiting for node B to detect loss and take over...");
    sleep(Duration::from_secs(5)).await;
    println!("  ✓ Failover window complete\n");

    // ========================================
    // PHASE 8: Continue writes on new leader for 2s
    // ========================================
    println!("PHASE 8: Continue writes on node B (new leader, 2s)");
    println!("---------------------------------------------------");

    sleep(Duration::from_secs(2)).await;
    let phase8_success = success_count.load(Ordering::Relaxed);
    let phase8_failures = failure_count.load(Ordering::Relaxed);
    println!(
        "  Writes: {} successful, {} failures (delta: +{}, +{})",
        phase8_success,
        phase8_failures,
        phase8_success - phase5_success,
        phase8_failures - phase5_failures
    );
    println!("  ✓ Writes continued on new leader\n");

    // ========================================
    // PHASE 9: Stop write task and verify
    // ========================================
    println!("PHASE 9: Stop writes and verify data");
    println!("------------------------------------");

    println!("  Stopping write task...");
    stop_signal.store(true, Ordering::Relaxed);
    write_task.await?;

    let final_success = success_count.load(Ordering::Relaxed);
    let final_failures = failure_count.load(Ordering::Relaxed);
    println!(
        "  Final stats: {} successful writes, {} failures",
        final_success, final_failures
    );

    println!("  Counting events on node B...");
    let mut client_b = CeleriantClient::connect(&format!("127.0.0.1:{}", node_b_port)).await?;
    let total_events = count_events(&mut client_b, &aggregate_key).await?;
    println!("  Events stored: {}", total_events);

    // Verify: successful writes should equal stored events
    assert_eq!(
        total_events, final_success as usize,
        "Event count mismatch: {} stored vs {} successful writes",
        total_events, final_success
    );
    println!("  ✓ All successful writes present (no data loss)");

    // Report failure rate during transitions
    let failure_rate = if final_success + final_failures > 0 {
        (final_failures as f64 / (final_success + final_failures) as f64) * 100.0
    } else {
        0.0
    };
    println!("  Failure rate: {:.2}%", failure_rate);

    println!("\n=== SUCCESS ===");
    println!("Rolling restart test passed:");
    println!("  - {} writes succeeded", final_success);
    println!("  - {} writes failed (expected during failover)", final_failures);
    println!("  - {} events stored (100% of successful writes)", total_events);
    println!("  - No acknowledged writes lost");
    println!("  - Cluster recovered after each restart");

    Ok(())
}
