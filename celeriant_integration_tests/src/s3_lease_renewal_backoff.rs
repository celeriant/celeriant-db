//! S3 Lease Renewal Backoff Test
//!
//! Verifies that when the follower is unreachable, the leader backs off S3
//! lease renewals to a rate proportional to the S3 lease duration — NOT the
//! heartbeat interval.
//!
//! Setup:
//! 1. Start MinIO + 2-node cluster with TcpProxy on the replication path
//! 2. Write events to verify the cluster is healthy
//! 3. Block the proxy (follower unreachable)
//! 4. Wait a fixed observation window (15s)
//! 5. Count the lease_index delta in S3
//!
//! With the bug (no backoff): heartbeat_interval=500ms → ~20-30 renewals in 15s
//! With the fix (s3_lease_duration/3 backoff): ~2-3 renewals in 15s
//!
//! The test asserts lease_index delta ≤ 5 over the window.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{write_event, MinioContainer, ServerConfig, TestServer, TcpProxy};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::disk::versioned_block::deserialise_lease;
use std::time::Duration;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Lease Renewal Backoff Test ===\n");

    let port_base = 13400 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let proxy_port = port_base + 200;
    let minio_port = port_base + 10;

    let num_shards = 1;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start cluster with TcpProxy
    // ========================================
    println!("PHASE 1: Start cluster with TcpProxy");
    println!("-------------------------------------");

    let minio = MinioContainer::start_with_bucket(minio_port, "test-lease-backoff").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) =
        minio.s3_config_fields();

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
        ..Default::default()
    };
    let leader =
        TestServer::start_with_config_labeled(leader_port, leader_config, "leader".into()).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_repl_port = follower_port + 1;
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_repl_port)).await?;

    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        routing_rule: RoutingRule::AggregateTypeId,
        advertised_replication_address: Some(format!("127.0.0.1:{}", proxy_port)),
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket_name),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
        s3_endpoint_override: Some(minio_endpoint),
        s3_allow_http: allow_http,
        ..Default::default()
    };
    let _follower =
        TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into())
            .await?;

    println!("  Waiting for election and heartbeat establishment...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    write_event(&mut leader_client, &aggregate_key, 1, true).await?;
    println!("  Cluster healthy: leader accepted write\n");

    // ========================================
    // PHASE 2: Record lease_index, block proxy
    // ========================================
    println!("PHASE 2: Block proxy and observe S3 renewal rate");
    println!("-------------------------------------------------");

    let pre_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let pre_lease = deserialise_lease(&pre_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    let pre_index = pre_lease.lease_index;
    println!("  Pre-block lease_index = {}", pre_index);

    proxy.block();
    println!("  Proxy blocked — follower unreachable");

    // ========================================
    // PHASE 3: Observation window
    // ========================================
    println!("\n  Waiting 15s observation window...");
    let observation_secs = 15;
    tokio::time::sleep(Duration::from_secs(observation_secs)).await;

    // ========================================
    // PHASE 4: Measure lease_index delta
    // ========================================
    println!("\nPHASE 3: Measure S3 lease renewal rate");
    println!("--------------------------------------");

    let post_lease_bytes = minio.get_object("cluster/lease.json").await?;
    let post_lease = deserialise_lease(&post_lease_bytes)
        .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
    let post_index = post_lease.lease_index;
    let delta = post_index - pre_index;

    println!("  Post-block lease_index = {}", post_index);
    println!("  Delta over {}s = {} renewals", observation_secs, delta);
    println!(
        "  Rate = {:.1} renewals/sec",
        delta as f64 / observation_secs as f64
    );

    // With heartbeat_interval=500ms and no backoff, we'd see ~20-30 renewals.
    // With proper s3_lease_duration/3 backoff (5s cycles), we'd see ~3.
    // Threshold of 5 gives margin while catching the ~20-30 case.
    let max_allowed_renewals = 5;
    assert!(
        delta <= max_allowed_renewals,
        "S3 lease renewed {} times in {}s (>{} allowed). \
         Leader is hammering S3 at heartbeat rate instead of backing off. \
         Expected ≤{} renewals with s3_lease_duration/3 backoff.",
        delta,
        observation_secs,
        max_allowed_renewals,
        max_allowed_renewals,
    );

    println!(
        "  {} renewals in {}s — backed off correctly (≤{})",
        delta, observation_secs, max_allowed_renewals
    );

    println!("\n=== S3 Lease Renewal Backoff Test Passed ===");
    Ok(())
}
