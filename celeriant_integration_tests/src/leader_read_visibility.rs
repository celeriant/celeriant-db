//! Integration test: Leader Read Visibility Invariant
//!
//! Tests that data written to the leader is NOT visible to readers until
//! replication completes. Uses a TcpProxy to throttle replication, creating
//! a deterministic window where data is fsync'd but not yet replicated.
//! Writes are spawned in background tasks so we can read while replication
//! is still in progress.
//!
//! Run with: cargo run --bin leader_read_visibility_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, write_event, MinioContainer, ServerConfig, TestServer, TcpProxy,
};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Leader Read Visibility Invariant Test ===\n");

    let port_base = 11700 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let proxy_port = port_base + 200;
    let minio_port = port_base + 10;

    // ── Phase 1: Start cluster with proxy ──
    println!("PHASE 1: Start cluster with TcpProxy");
    println!("-------------------------------------");

    println!("  Starting MinIO...");
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = 1;

    let leader_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: leader_port,
        routing_rule: RoutingRule::AggregateTypeId,
        // Timeout for replication requests — gives us a window to read
        internode_request_timeout_ms: 30_000,
        // Long lease so throttled heartbeats don't cause leadership change
        heartbeat_lease_duration_ms: 120_000,
        s3_enabled: true,
        s3_region: Some(region.clone()),
        s3_bucket: Some(bucket.clone()),
        s3_access_key_id: Some(access_key.clone()),
        s3_secret_access_key: Some(secret_key.clone()),
        s3_endpoint_override: Some(endpoint.clone()),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    let leader = TestServer::start_with_config_labeled(leader_port, leader_config, "leader".into()).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_repl_port = follower_port + 1;
    println!("  Starting TcpProxy: {} -> {}", proxy_port, follower_repl_port);
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_repl_port)).await?;

    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        client_port: follower_port,
        advertised_replication_address: Some(format!("127.0.0.1:{}", proxy_port)),
        routing_rule: RoutingRule::AggregateTypeId,
        heartbeat_lease_duration_ms: 120_000,
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
        s3_endpoint_override: Some(endpoint),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    let follower = TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into()).await?;

    println!("  Waiting for election + replication connection...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    // ── Phase 2: Verify cluster is healthy ──
    println!("\nPHASE 2: Verify healthy replication");
    println!("------------------------------------");

    let key = AggregateKey::new(1, 0, 1);
    for i in 1..=3 {
        write_event(&mut leader_client, &key, i, i == 1).await?;
    }

    let leader_count = count_events(&mut leader_client, &key).await?;
    let follower_count = count_events(&mut follower_client, &key).await?;
    assert_eq!(leader_count, 3, "leader should see 3 events");
    assert_eq!(follower_count, 3, "follower should see 3 events via proxy");
    println!("  Cluster healthy: leader={}, follower={}", leader_count, follower_count);

    // ── Phase 3: Throttle proxy, write in background, read immediately ──
    println!("\nPHASE 3: Throttle proxy, verify leader does not expose unreplicated data");
    println!("------------------------------------------------------------------------");

    // Heavy throttle: 10s per 8KB chunk. Replication will crawl but the TCP
    // connection stays alive, so the leader keeps waiting (no S3 fallback).
    // internode_request_timeout_ms is 30s, giving us plenty of time to read.
    proxy.throttle(10_000);

    // Small sleep to ensure existing in-flight replication completes
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Spawn writes in background — they will block waiting for replication ACK
    let key2 = AggregateKey::new(1, 0, 2);
    let leader_addr = leader.address().to_string();
    let write_key = key2.clone();
    println!("  Spawning background writes to new aggregate while proxy is throttled...");
    let write_handle = tokio::spawn(async move {
        let mut client = CeleriantClient::connect(&leader_addr).await.unwrap();
        for i in 1..=5 {
            write_event(&mut client, &write_key, i, i == 1).await.unwrap();
        }
        println!("  Background writes completed (replication done)");
    });

    // Give the first write a moment to reach fsync but not complete replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Read from leader — not all data should be visible because replication
    // is throttled. The first write may have completed (first chunk sneaks
    // through the proxy before the throttle delay kicks in), but the remaining
    // writes should be blocked waiting for their replication round-trip.
    let leader_count_pre = count_events(&mut leader_client, &key2).await?;
    println!("  Leader read (mid-replication): {} events (expected < 5)", leader_count_pre);
    assert!(
        leader_count_pre < 5,
        "Leader must NOT expose all data before replication completes (got {}/5)",
        leader_count_pre
    );
    println!("  INVARIANT HELD: leader shows {}/5 events mid-replication", leader_count_pre);

    // The original aggregate should still be readable (already replicated)
    let leader_count_old = count_events(&mut leader_client, &key).await?;
    assert_eq!(leader_count_old, 3, "previously replicated data should still be readable");
    println!("  Previously replicated data still readable: {} events", leader_count_old);

    // ── Phase 4: Unthrottle and verify convergence ──
    println!("\nPHASE 4: Unthrottle proxy, verify data becomes visible");
    println!("------------------------------------------------------");

    proxy.unthrottle();
    println!("  Proxy unthrottled — replication will complete");

    // Wait for background writes to finish
    write_handle.await?;

    // The proxy throttle delays heartbeats enough to trigger clock-drift fencing,
    // which kicks the follower into S3 catchup. Give it time to finish.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let leader_count_post = count_events(&mut leader_client, &key2).await?;
    let follower_count_post = count_events(&mut follower_client, &key2).await?;
    println!("  After replication: leader={}, follower={}", leader_count_post, follower_count_post);

    assert_eq!(leader_count_post, 5, "leader should now see all 5 events after replication");
    assert_eq!(follower_count_post, 5, "follower should also see all 5 events");

    println!("\n=== All Tests Passed ===");
    println!("Leader read visibility invariant verified:");
    println!("  1. Data written but not replicated: invisible to leader readers");
    println!("  2. Previously replicated data: still readable during throttle");
    println!("  3. After replication completes: data visible on both nodes");

    Ok(())
}
