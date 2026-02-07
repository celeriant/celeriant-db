//! S3 Old Leader Recovery Integration Test
//!
//! Tests the "Old Leader Returns" scenario from the S3 lease design:
//! 1. Two-node cluster (A=leader, B=follower)
//! 2. Leader (A) crashes
//! 3. Follower (B) takes over via S3 race, becomes new leader
//! 4. Old leader (A) restarts
//! 5. Old leader (A) discovers it's no longer leader (via S3 lease read)
//! 6. Old leader (A) becomes follower
//! 7. New leader (B) discovers old leader's registration, connects
//! 8. Heartbeat resumes between B (leader) and A (follower)
//! 9. Replication works from B to A
//!
//! Run with: cargo run --bin s3_old_leader_recovery_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{count_events, write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Old Leader Recovery Integration Test ===\n");

    let port_base = 13300 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 100;
    let minio_port = port_base + 10;

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start two-node cluster (A=leader, B=follower)
    // ========================================
    println!("PHASE 1: Start two-node cluster (A=leader, B=follower)");
    println!("-------------------------------------------------------");

    println!("  Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-old-leader-recovery").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("  MinIO ready at {}", minio_endpoint);

    let node_a_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: node_a_port,
        bootstrap_as_leader: true,
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region.clone()),
        s3_bucket: Some(bucket_name.clone()),
        s3_access_key_id: Some(access_key.clone()),
        s3_secret_access_key: Some(secret_key.clone()),
        s3_endpoint_override: Some(minio_endpoint.clone()),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!("  Starting node A (leader) on port {}...", node_a_port);
    let mut node_a = TestServer::start_with_config_labeled(node_a_port, node_a_config, "node-a-leader".into()).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let node_b_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: node_b_port,
        bootstrap_as_leader: false,
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region.clone()),
        s3_bucket: Some(bucket_name.clone()),
        s3_access_key_id: Some(access_key.clone()),
        s3_secret_access_key: Some(secret_key.clone()),
        s3_endpoint_override: Some(minio_endpoint.clone()),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!("  Starting node B (follower) on port {}...", node_b_port);
    let _node_b = TestServer::start_with_config_labeled(node_b_port, node_b_config, "node-b-follower".into()).await?;

    println!("  Waiting for leader to discover follower...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 2: Write data on leader (node A)
    // ========================================
    println!("\nPHASE 2: Write data on leader (node A)");
    println!("---------------------------------------");

    let mut node_a_client = CeleriantClient::connect(&format!("127.0.0.1:{}", node_a_port)).await?;
    let mut node_b_client = CeleriantClient::connect(&format!("127.0.0.1:{}", node_b_port)).await?;

    println!("  Writing events 1-5 through node A...");
    for i in 1..=5 {
        write_event(&mut node_a_client, &aggregate_key, i, i == 1).await?;
    }

    println!("  Waiting for replication to node B...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let node_b_count = count_events(&mut node_b_client, &aggregate_key).await?;
    assert_eq!(node_b_count, 5, "Node B should have 5 events replicated");
    println!("  ✓ Initial replication verified: node B has {} events\n", node_b_count);

    // ========================================
    // PHASE 3: Kill leader (node A)
    // ========================================
    println!("PHASE 3: Kill leader (node A)");
    println!("------------------------------");

    let initial_lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let initial_lease = deserialise_lease(&initial_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    let initial_lease_index = initial_lease.lease_index;
    println!("  Initial lease_index={}, leader_node_id={:x}",
        initial_lease_index, initial_lease.leader_node_id);

    node_a.stop();
    println!("  ✓ Node A (old leader) stopped\n");

    // ========================================
    // PHASE 4: Wait for follower (node B) to take over via S3 race
    // ========================================
    println!("PHASE 4: Wait for follower (node B) to take over via S3 race");
    println!("-------------------------------------------------------------");

    println!("  Waiting for node B's watchdog to expire and win S3 race...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let post_takeover_lease_bytes = minio.get_object("cluster/lease.bin").await?;
    let post_takeover_lease = deserialise_lease(&post_takeover_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  Post-takeover lease: leader_node_id={:x}, lease_index={}",
        post_takeover_lease.leader_node_id, post_takeover_lease.lease_index);
    assert!(
        post_takeover_lease.lease_index > initial_lease_index,
        "lease_index should have increased after takeover: was {}, now {}",
        initial_lease_index, post_takeover_lease.lease_index
    );
    println!("  ✓ Node B took over: lease_index {} → {}\n",
        initial_lease_index, post_takeover_lease.lease_index);

    // ========================================
    // PHASE 5: Verify node B is now leader (can accept writes)
    // ========================================
    println!("PHASE 5: Verify node B is now leader (can accept writes)");
    println!("---------------------------------------------------------");

    let takeover_key = AggregateKey::new(2, 1, 1);
    println!("  Writing event through node B...");
    write_event(&mut node_b_client, &takeover_key, 1, true).await?;
    println!("  ✓ Node B accepts writes (is now Leader)\n");

    // ========================================
    // PHASE 6: Restart old leader (node A)
    // ========================================
    println!("PHASE 6: Restart old leader (node A)");
    println!("-------------------------------------");

    println!("  Restarting node A...");
    node_a.restart().await?;
    println!("  ✓ Node A restarted\n");

    // ========================================
    // PHASE 7: Wait for old leader (node A) to discover it's now a follower
    // ========================================
    println!("PHASE 7: Wait for old leader (node A) to discover it's now a follower");
    println!("----------------------------------------------------------------------");

    println!("  Waiting for node A to read S3 lease and become follower...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Reconnect to node A — old connection from before kill/restart is stale
    drop(node_a_client);
    let mut node_a_client = CeleriantClient::connect(&format!("127.0.0.1:{}", node_a_port)).await?;

    // Verify node A rejects writes (is follower)
    let follower_test_key = AggregateKey::new(3, 1, 1);
    let node_a_accepts = write_event(&mut node_a_client, &follower_test_key, 1, true).await.is_ok();
    assert!(!node_a_accepts, "Node A should reject writes (is now Follower)");
    println!("  ✓ Node A rejects writes (is now Follower)\n");

    // ========================================
    // PHASE 8: Verify new leader (B) discovers old leader's (A) registration
    // ========================================
    println!("PHASE 8: Verify new leader (B) discovers old leader's (A) registration");
    println!("-----------------------------------------------------------------------");

    println!("  Waiting for node B to discover node A's registration...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("  ✓ Discovery period complete (node B should have connected to node A)\n");

    // ========================================
    // PHASE 9: Verify heartbeat and replication from B (leader) to A (follower)
    // ========================================
    println!("PHASE 9: Verify heartbeat and replication from B (leader) to A (follower)");
    println!("--------------------------------------------------------------------------");

    // Reconnect clients — old connections may be stale after role transitions
    drop(node_b_client);
    drop(node_a_client);
    let mut node_b_client = CeleriantClient::connect(&format!("127.0.0.1:{}", node_b_port)).await?;
    let mut node_a_client = CeleriantClient::connect(&format!("127.0.0.1:{}", node_a_port)).await?;

    println!("  Writing events 6-10 through node B (new leader)...");
    for i in 6..=10 {
        write_event(&mut node_b_client, &aggregate_key, i, false).await?;
    }

    println!("  Waiting for replication from B to A...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let node_a_final_count = count_events(&mut node_a_client, &aggregate_key).await?;
    assert_eq!(node_a_final_count, 10,
        "Node A should have 10 events (5 from old session + 5 replicated)");
    println!("  ✓ Replication verified: node A has {} events\n", node_a_final_count);

    println!("=== All Tests Passed ===");
    println!("Old leader recovery test validated:");
    println!("  1. Old leader (A) crashed");
    println!("  2. Follower (B) took over via S3 race");
    println!("  3. Old leader (A) restarted and became follower");
    println!("  4. New leader (B) discovered old leader (A)");
    println!("  5. Heartbeat and replication resumed from B to A\n");

    Ok(())
}
