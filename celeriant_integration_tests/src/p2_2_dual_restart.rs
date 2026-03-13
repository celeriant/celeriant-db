//! P2-2: Both Nodes Restart Simultaneously Integration Test
//!
//! Tests that both nodes can restart simultaneously and cleanly race for S3 lease.
//! Design spec: docs/integration-test-coverage-report.md, section "P2-2. Both Nodes Restart Simultaneously"
//!
//! Scenario:
//! 1. Start 2-node cluster with S3
//! 2. Write events to verify cluster health
//! 3. Note initial lease_index from S3
//! 4. Stop BOTH nodes simultaneously
//! 5. Start both nodes nearly simultaneously (100-500ms apart)
//! 6. Wait for election (~8s)
//! 7. Verify exactly one leader via is_leader() on both nodes
//! 8. Verify lease_index incremented in S3
//! 9. Verify cluster is functional (write + read succeeds)
//!
//! Run with: cargo run --bin p2_2_dual_restart_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, is_leader, write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P2-2: Both Nodes Restart Simultaneously ===\n");

    let port_base = 19700;
    let node_a_port = port_base;
    let node_b_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-dual-restart").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start 2-node cluster and verify health
    // ========================================
    println!("PHASE 1: Start 2-node cluster and verify health");
    println!("------------------------------------------------");

    let node_a_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
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
    println!("  Starting node A on port {}...", node_a_port);
    let mut node_a = TestServer::start_with_config_labeled(node_a_port, node_a_config, "node-A".to_string()).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let node_b_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket_name),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
        s3_endpoint_override: Some(minio_endpoint),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!("  Starting node B on port {}...", node_b_port);
    let mut node_b = TestServer::start_with_config_labeled(node_b_port, node_b_config, "node-B".to_string()).await?;

    println!("  Waiting for election and heartbeat establishment...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut node_a_client = CeleriantClient::connect(node_a.address()).await?;

    println!("  Writing events 1-5 to verify cluster health...");
    for i in 1..=5 {
        write_event(&mut node_a_client, &aggregate_key, i, i == 1).await?;
    }

    let mut node_b_client = CeleriantClient::connect(node_b.address()).await?;
    let node_b_count = count_events(&mut node_b_client, &aggregate_key).await?;
    assert_eq!(node_b_count, 5, "Node B should have 5 events");
    println!("  ✓ Cluster healthy: node B has {} events", node_b_count);

    let lease_initial_bytes = minio.get_object("cluster/lease.json").await?;
    let lease_initial = deserialise_lease(&lease_initial_bytes)
        .map_err(|e| format!("Failed to deserialise initial lease: {:?}", e))?;

    println!("  Initial lease: leader_node_id={:x}, lease_index={}",
        lease_initial.leader_node_id, lease_initial.lease_index);
    let initial_lease_index = lease_initial.lease_index;
    println!("  ✓ Initial state verified: lease_index={}\n", initial_lease_index);

    // ========================================
    // PHASE 2: Stop BOTH nodes
    // ========================================
    println!("PHASE 2: Stop both nodes simultaneously");
    println!("----------------------------------------");

    println!("  Stopping node A...");
    drop(node_a_client);
    node_a.stop();
    println!("  ✓ Node A stopped");

    println!("  Stopping node B...");
    drop(node_b_client);
    node_b.stop();
    println!("  ✓ Node B stopped");
    println!("  Both nodes are now offline\n");

    // ========================================
    // PHASE 3: Restart both nodes nearly simultaneously
    // ========================================
    println!("PHASE 3: Restart both nodes nearly simultaneously");
    println!("--------------------------------------------------");

    println!("  Starting node A...");
    let restart_a = node_a.restart();
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("  Starting node B (300ms after A)...");
    let restart_b = node_b.restart();

    println!("  Waiting for both restarts to complete...");
    let (result_a, result_b) = tokio::join!(restart_a, restart_b);
    result_a?;
    result_b?;
    println!("  ✓ Both nodes restarted");

    println!("  Waiting for S3 CAS race and election (~8s)...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // ========================================
    // PHASE 4: Verify exactly one leader via is_leader()
    // ========================================
    println!("\nPHASE 4: Verify exactly one leader emerged");
    println!("--------------------------------------------");

    let node_a_is_leader = is_leader(node_a.address()).await?;
    let node_b_is_leader = is_leader(node_b.address()).await?;

    println!("  Node A is_leader: {}", node_a_is_leader);
    println!("  Node B is_leader: {}", node_b_is_leader);

    if node_a_is_leader && node_b_is_leader {
        return Err("SPLIT BRAIN: Both nodes claim to be leader!".into());
    }
    if !node_a_is_leader && !node_b_is_leader {
        return Err("NO LEADER: Both nodes are fenced or followers!".into());
    }

    println!("  ✓ Exactly one leader elected\n");

    // ========================================
    // PHASE 5: Verify lease_index incremented in S3
    // ========================================
    println!("PHASE 5: Verify lease_index incremented in S3");
    println!("-----------------------------------------------");

    let lease_final_bytes = minio.get_object("cluster/lease.json").await?;
    let lease_final = deserialise_lease(&lease_final_bytes)
        .map_err(|e| format!("Failed to deserialise final lease: {:?}", e))?;

    println!("  Final lease: leader_node_id={:x}, lease_index={}",
        lease_final.leader_node_id, lease_final.lease_index);

    assert!(
        lease_final.lease_index > initial_lease_index,
        "lease_index should have incremented after dual restart: was {}, now {}",
        initial_lease_index, lease_final.lease_index
    );
    println!("  ✓ Lease incremented: {} → {}\n", initial_lease_index, lease_final.lease_index);

    // ========================================
    // PHASE 6: Verify cluster is functional
    // ========================================
    println!("PHASE 6: Verify cluster is functional");
    println!("--------------------------------------");

    let leader_address = if node_a_is_leader {
        node_a.address()
    } else {
        node_b.address()
    };

    let mut leader_client = CeleriantClient::connect(leader_address).await?;

    println!("  Writing events 6-8 to leader...");
    for i in 6..=8 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }

    let final_count = count_events(&mut leader_client, &aggregate_key).await?;
    println!("  Leader has {} events total", final_count);

    // We wrote 1-5 initially, then 6-8 after restart = 8 events minimum.
    // (Some events may have been replicated to follower, that's fine)
    assert!(final_count >= 8, "Leader should have at least 8 events (5 initial + 3 new)");
    println!("  ✓ Cluster is functional (leader has {} events)\n", final_count);

    // ========================================
    // Summary
    // ========================================
    println!("=== All Tests Passed ===\n");
    println!("Summary:");
    println!("  - Both nodes stopped simultaneously");
    println!("  - Both nodes restarted nearly simultaneously");
    println!("  - Exactly one leader elected via S3 CAS race");
    println!("  - lease_index incremented: {} → {}", initial_lease_index, lease_final.lease_index);
    println!("  - Cluster accepted writes post-restart");
    println!("  - Dual restart resolves cleanly without split-brain\n");

    Ok(())
}
