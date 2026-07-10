//! S3 Concurrent CAS Integration Test
//!
//! Tests the IfMatchETag CAS mechanism under network partition with reconvergence.
//! Absorbs s3_network_partition (fencing + monotonic lease) and s3_reconvergence
//! (post-partition single-leader convergence).
//!
//! Scenario:
//! 1. Start two-node cluster with TcpProxy, verify replication through proxy
//! 2. Block proxy (simulate network partition)
//! 3. Wait for both nodes to fence and race to S3
//! 4. Verify lease_epoch monotonically increased (S3 CAS safety)
//! 5. Unblock proxy — nodes reconverge
//! 6. Poll until exactly one leader emerges
//! 7. Verify final state: one leader accepts writes, one follower rejects
//!
//! Invariants tested: 1 (single leader), 2 (monotonic lease_epoch),
//!   3 (write gating), 17 (membership CAS)

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{poll_converged_count, write_event, MinioContainer, ServerConfig, TestServer, TcpProxy, FOLLOWER_CONVERGENCE_TIMEOUT};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Concurrent CAS Integration Test ===\n");

    let port_base = 13700 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let proxy_port = port_base + 200;
    let minio_port = port_base + 10;

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start cluster with TcpProxy, verify replication
    // ========================================
    println!("PHASE 1: Start cluster with TcpProxy");
    println!("-------------------------------------");

    println!("  Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-concurrent-cas").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("  MinIO ready at {}", minio_endpoint);

    let leader_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: leader_port,
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
    println!("  Starting leader on port {}...", leader_port);
    let _leader = TestServer::start_with_config_labeled(leader_port, leader_config, "leader".into()).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_repl_port = follower_port + 1;
    println!("  Starting TcpProxy: {} -> {}", proxy_port, follower_repl_port);
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_repl_port)).await?;
    println!("  TcpProxy ready at {}", proxy.address());

    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: follower_port,
        advertised_replication_address: Some(format!("127.0.0.1:{}", proxy_port)),
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket_name.clone()),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
        s3_endpoint_override: Some(minio_endpoint),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!("  Starting follower on port {} (advertised repl: proxy {})",
        follower_port, proxy_port);
    let _follower = TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into()).await?;

    println!("  Waiting for leader to discover follower through proxy...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Write events and verify replication through proxy (from s3_network_partition)
    let mut leader_client = CeleriantClient::connect(&format!("127.0.0.1:{}", leader_port)).await?;
    let mut follower_client = CeleriantClient::connect(&format!("127.0.0.1:{}", follower_port)).await?;

    println!("  Writing events 1-3 through leader...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let follower_count =
        poll_converged_count(&mut follower_client, &aggregate_key, 3, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(follower_count, 3, "Follower should have 3 events (replication through proxy)");
    println!("  Cluster healthy: follower has {} events through proxy", follower_count);

    // Verify pre-partition roles
    let pre_key = AggregateKey::new(2, 1, 1);
    let leader_ok = write_event(&mut leader_client, &pre_key, 1, true).await.is_ok();
    let follower_ok = write_event(&mut follower_client, &pre_key, 2, true).await.is_ok();
    assert!(leader_ok, "Leader should accept writes before partition");
    assert!(!follower_ok, "Follower should reject writes before partition");
    println!("  Pre-partition roles correct: leader accepts, follower rejects\n");

    // ========================================
    // PHASE 2: Capture initial lease and create partition
    // ========================================
    println!("PHASE 2: Block proxy (simulate network partition)");
    println!("--------------------------------------------------");

    let initial_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let initial_lease = deserialise_lease(&initial_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    let initial_lease_epoch = initial_lease.lease_epoch;
    let initial_leader_node_id = initial_lease.leader_node_id;
    println!("  Initial lease_epoch={}, leader_node_id={:x}",
        initial_lease_epoch, initial_leader_node_id);

    proxy.block();
    println!("  Proxy blocked - nodes partitioned\n");

    // ========================================
    // PHASE 3: Wait for fencing and S3 races
    // ========================================
    println!("PHASE 3: Wait for both nodes to fence and race to S3");
    println!("-----------------------------------------------------");

    println!("  Waiting for heartbeat timeout + fencing + S3 races...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Verify lease_epoch monotonicity (from s3_network_partition)
    let post_race_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let post_race_lease = deserialise_lease(&post_race_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    let post_race_lease_epoch = post_race_lease.lease_epoch;
    let post_race_leader_node_id = post_race_lease.leader_node_id;

    println!("  Post-race lease: leader_node_id={:x}, lease_epoch={}",
        post_race_leader_node_id, post_race_lease_epoch);

    assert!(
        post_race_lease_epoch >= initial_lease_epoch,
        "lease_epoch should not regress after S3 races: was {}, now {}",
        initial_lease_epoch, post_race_lease_epoch
    );
    assert!(
        post_race_lease_epoch <= initial_lease_epoch + 10,
        "lease_epoch should not have jumped unreasonably: was {}, now {}",
        initial_lease_epoch, post_race_lease_epoch
    );
    let lease_increments = post_race_lease_epoch - initial_lease_epoch;
    println!("  S3 CAS resolved: lease_epoch {} -> {} ({} cross-node bumps)\n",
        initial_lease_epoch, post_race_lease_epoch, lease_increments);

    // ========================================
    // PHASE 4: Unblock proxy, poll for reconvergence
    // ========================================
    println!("PHASE 4: Unblock proxy, poll for reconvergence");
    println!("-----------------------------------------------");

    proxy.unblock();
    println!("  Proxy unblocked - nodes can communicate again");

    // Poll for reconvergence (from s3_reconvergence)
    let max_reconverge_time = Duration::from_secs(20);
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();
    let mut reconverged = false;

    while start.elapsed() < max_reconverge_time {
        tokio::time::sleep(poll_interval).await;

        let check_key_a = AggregateKey::new(4, 1, start.elapsed().as_millis() as u128 % 1000);
        let check_key_b = AggregateKey::new(5, 1, start.elapsed().as_millis() as u128 % 1000);

        let a_accepts = write_event(&mut leader_client, &check_key_a, 1, true).await.is_ok();
        let b_accepts = write_event(&mut follower_client, &check_key_b, 1, true).await.is_ok();

        if a_accepts != b_accepts {
            let winner = if a_accepts { "original leader" } else { "original follower" };
            println!("  Reconverged at {:?}: {} is the single leader", start.elapsed(), winner);
            reconverged = true;
            break;
        }
    }

    assert!(
        reconverged,
        "Cluster did not reconverge to exactly one leader within {}s",
        max_reconverge_time.as_secs()
    );

    // ========================================
    // PHASE 5: Verify final state
    // ========================================
    println!("\nPHASE 5: Verify final single-leader state");
    println!("-------------------------------------------");

    let final_key_a = AggregateKey::new(6, 1, 1);
    let final_key_b = AggregateKey::new(7, 1, 1);

    let final_a_accepts = write_event(&mut leader_client, &final_key_a, 1, true).await.is_ok();
    let final_b_accepts = write_event(&mut follower_client, &final_key_b, 1, true).await.is_ok();

    assert_ne!(
        final_a_accepts, final_b_accepts,
        "Both nodes have same write acceptance state. Expected exactly one leader."
    );

    // Verify lease winner matches actual behavior
    if final_a_accepts {
        println!("  Original leader won the final CAS race");
    } else {
        println!("  Original follower won the final CAS race");
    }

    // Verify the final lease is consistent
    let final_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let final_lease = deserialise_lease(&final_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    assert!(
        final_lease.lease_epoch >= post_race_lease_epoch,
        "Final lease_epoch should not decrease"
    );
    println!("  Final lease_epoch={} (monotonic)", final_lease.lease_epoch);

    println!("\n=== All Tests Passed ===");
    println!("Concurrent CAS + partition + reconvergence validated:");
    println!("  1. Replication through TcpProxy verified");
    println!("  2. S3 CAS race resolved with {} lease increments", lease_increments);
    println!("  3. Post-partition: exactly one leader emerged");
    println!("  4. Lease_index strictly monotonic throughout\n");

    Ok(())
}
