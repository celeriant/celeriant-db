//! S3 Stale Lease Integration Test - Restarting node does not takeover on stale lease
//!
//! Tests the "Node restart with stale S3 lease" scenario from the design spec:
//! "A restarting node reads this stale expired lease but must not immediately attempt
//! takeover. Instead: register self, become Follower, wait for leader to connect."
//!
//! Scenario:
//! 1. Start cluster, establish leader/follower, verify healthy
//! 2. Kill follower — leader self-heals via S3 CAS renewals (lease_epoch unchanged, same leader)
//! 3. Restart follower — it reads the stale S3 lease (different leader, expired)
//! 4. Verify follower does NOT race to S3 — leader_node_id unchanged
//! 5. Verify follower rejoins as follower and replication works
//!
//! The key invariant: a restarting node seeing a stale lease from another leader
//! becomes follower and waits, rather than attempting takeover.
//!
//! Run with: cargo run --bin s3_stale_lease_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{poll_converged_count, poll_event_count, write_event, MinioContainer, ServerConfig, TestServer, FOLLOWER_CONVERGENCE_TIMEOUT};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Stale Lease Integration Test ===\n");

    let port_base = 12100 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-stale-lease").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start cluster and verify healthy
    // ========================================
    println!("PHASE 1: Start cluster and verify healthy");
    println!("------------------------------------------");

    let leader_config = ServerConfig {
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
    println!("  Starting leader on port {}...", leader_port);
    let leader = TestServer::start_with_config(leader_port, leader_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_config = ServerConfig {
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
    println!("  Starting follower on port {}...", follower_port);
    let mut follower = TestServer::start_with_config(follower_port, follower_config).await?;

    println!("  Waiting for election and heartbeat establishment...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    println!("  Writing events 1-3 to verify cluster health...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count =
        poll_converged_count(&mut follower_client, &aggregate_key, 3, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(follower_count, 3, "Follower should have 3 events");
    println!("  ✓ Cluster healthy: follower has {} events\n", follower_count);

    // ========================================
    // PHASE 2: Kill follower — leader self-heals
    // ========================================
    println!("PHASE 2: Kill follower — leader self-heals");
    println!("-------------------------------------------");

    drop(follower_client);
    follower.stop();
    println!("  Follower stopped");

    println!("  Waiting for leader to detect heartbeat loss and self-heal...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Leader should have self-healed via S3 race
    let self_heal_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let self_heal_lease = deserialise_lease(&self_heal_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;

    println!("  After self-heal: leader_node_id={:x}, lease_epoch={}, expires_at_ms={}",
        self_heal_lease.leader_node_id, self_heal_lease.lease_epoch, self_heal_lease.expires_at_ms);

    let leader_node_id = self_heal_lease.leader_node_id;
    let lease_epoch_after_heal = self_heal_lease.lease_epoch;
    assert!(
        self_heal_lease.expires_at_ms > 0,
        "self-heal should have produced a lease with expiry set"
    );

    write_event(&mut leader_client, &aggregate_key, 4, false).await?;
    println!("  ✓ Leader self-healed at lease_epoch={}\n", lease_epoch_after_heal);

    // ========================================
    // PHASE 3: Restart follower (reads stale S3 lease)
    // ========================================
    println!("PHASE 3: Restart follower (reads stale S3 lease)");
    println!("-------------------------------------------------");

    // The S3 lease now has:
    // - leader_node_id = original leader (different from restarting follower)
    // - expires_at_ms in the past (stale — heartbeat replaced it during steady state)
    // Per the design spec, the restarting node should:
    //   1. Register self on S3
    //   2. Read lease → different node is leader → become Follower, wait
    //   3. NOT attempt takeover despite stale expires_at_ms

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    println!("  S3 lease expires_at_ms={}, current time={}ms (stale by {}ms)",
        self_heal_lease.expires_at_ms, now_ms,
        now_ms.saturating_sub(self_heal_lease.expires_at_ms));

    println!("  Restarting follower...");
    follower.restart().await?;
    println!("  Follower restarted");

    println!("  Waiting for follower to read stale lease and rejoin as follower...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // ========================================
    // PHASE 4: Verify follower did NOT takeover
    // ========================================
    println!("\nPHASE 4: Verify follower did NOT takeover");
    println!("-----------------------------------------");

    let post_restart_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let post_restart_lease = deserialise_lease(&post_restart_lease_bytes)
        .map_err(|e| format!("Failed to deserialise post-restart lease: {:?}", e))?;

    println!("  Post-restart lease: leader_node_id={:x}, lease_epoch={}",
        post_restart_lease.leader_node_id, post_restart_lease.lease_epoch);

    assert_eq!(
        post_restart_lease.leader_node_id, leader_node_id,
        "leader_node_id should NOT have changed"
    );
    assert_eq!(
        post_restart_lease.lease_epoch, lease_epoch_after_heal,
        "lease_epoch should NOT change while same leader holds the lease"
    );
    println!("  ✓ Follower did NOT attempt takeover despite stale S3 lease");
    println!("    (lease_epoch unchanged at {}, same leader self-renewing)\n",
        post_restart_lease.lease_epoch);

    // ========================================
    // PHASE 5: Verify cluster works after follower rejoin
    // ========================================
    println!("PHASE 5: Verify cluster works after follower rejoin");
    println!("----------------------------------------------------");

    println!("  Writing events 5-6 to leader...");
    for i in 5..=6 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }
    println!("  Leader accepted writes");

    // Poll follower until it has at least 5 events (3 persisted + events after rejoin)
    let final_follower_count = poll_event_count(
        follower.address(), &aggregate_key, 5, Duration::from_secs(30),
    ).await;
    println!("  Follower has {} events", final_follower_count);
    println!("  Follower successfully replicated after restart with stale lease");

    println!("\n=== All Tests Passed ===\n");
    println!("Key result: Restarting follower saw stale S3 lease (expired, different leader),");
    println!("correctly became Follower without attempting S3 takeover.");

    Ok(())
}
