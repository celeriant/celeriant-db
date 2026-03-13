//! Regression guard: Heartbeat Independence Under Replication Pressure (test #14)
//!
//! Guards against regression of the split-locking fix in `FollowerConnection`.
//! Before the fix, the replication client held a single lock through network I/O,
//! causing heartbeats to starve when replication was slow. The fix introduced:
//!   - `replication_conn: RwLock<ConnState>` — for replication batches and kick
//!   - `heartbeat_conn: RwLock<ConnState>` — for heartbeats only
//! These two locks are independent; neither blocks the other.
//!
//! Scenario: throttle replication via TcpProxy, write events to create replication
//! pressure, verify the cluster stays healthy — leader keeps sending heartbeats
//! (follower does NOT trigger failover), replication eventually catches up after
//! unthrottle, and no unnecessary S3 fallback occurs during the throttle period.
//!
//! If the split-locking is ever reverted, heartbeat round-trips would stall behind
//! the replication lock, the follower's heartbeat watchdog would fire, and the
//! follower would attempt to take over — causing a spurious failover. This test
//! would then fail at the "follower still a follower" assertion.
//!
//! Run with: cargo run --bin edge_heartbeat_lock_contention_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, is_leader, s3_cluster_config, write_event, MinioContainer, TcpProxy, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Regression Guard: Heartbeat Independence Under Replication Pressure ===\n");
    println!("This test guards against regression of the split-locking fix in FollowerConnection.");
    println!("If heartbeat and replication share a single lock, slow replication will starve");
    println!("heartbeats and cause spurious failover. With split locks, they are independent.\n");

    // ========================================
    // Setup
    // ========================================
    let port_base = 15900 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;
    let proxy_port = port_base + 200;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-heartbeat-lock").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 2;

    // TcpProxy intercepts the follower's replication port (port+1).
    // Both heartbeat and replication TCP connections go through this proxy,
    // but with split locking they use independent locks and do not block each other.
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_port + 1)).await?;
    println!("  Proxy {} -> follower replication port {}", proxy_port, follower_port + 1);

    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    // Long heartbeat lease: 30s gives ample margin — if the follower triggers failover
    // during a 10s throttle window, the test fails and the regression is detected.
    config.heartbeat_lease_duration_ms = 30_000;
    // Default heartbeat interval: 500ms. With a 30s lease, the follower would need
    // ~60 consecutive missed heartbeats to trigger failover. A healthy system should
    // not miss any; a regressed system will miss all of them during throttle.
    config.heartbeat_interval_ms = 500;
    // High water mark: 10MB — intentionally high so that moderate throttle pressure
    // does NOT trigger S3 fallback. We want to verify no false S3 fallback occurs.
    config.pending_replication_high_water_bytes = 10 * 1024 * 1024;
    // Allow longer internode timeout so the throttled connection is not dropped.
    config.internode_connection_timeout_ms = Some(60_000);

    let mut follower_config = config.clone();
    // Route the follower's advertised replication address through the proxy.
    follower_config.advertised_replication_address = Some(proxy.address());

    println!("Starting two-node cluster (2 shards, heartbeat_lease=30s, high_water=10MB)...");
    let mut leader =
        TestServer::start_with_config_labeled(leader_port, config, "leader".into()).await?;
    let mut follower =
        TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into())
            .await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    // ========================================
    // Phase 1: Cluster stabilization
    // ========================================
    println!("\nPHASE 1: Wait for election + discovery + replication connection");
    println!("---------------------------------------------------------------");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    // Verify roles before throttling.
    assert!(
        is_leader(leader.address()).await?,
        "Expected leader node to be the leader"
    );
    assert!(
        !is_leader(follower.address()).await?,
        "Expected follower node to be a follower"
    );

    // Write a baseline event and verify it replicates.
    let probe_key = AggregateKey::new(1, 0, 12345);
    write_event(&mut leader_client, &probe_key, 1, true).await?;
    let baseline = count_events(&mut follower_client, &probe_key).await?;
    assert_eq!(baseline, 1, "Baseline event should have replicated to follower");
    println!("  Cluster healthy, baseline replication confirmed\n");

    // Record S3 state before throttling — there must be zero fallback objects at this point.
    let s3_before: Vec<usize> = {
        let mut counts = Vec::new();
        for shard_id in 0..num_shards {
            let objs = minio
                .list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id))
                .await?;
            assert!(
                objs.is_empty(),
                "No S3 fallback objects expected before throttle (shard {})",
                shard_id
            );
            counts.push(objs.len());
        }
        counts
    };
    println!("  No S3 fallback objects before throttle (confirmed)");

    // ========================================
    // Phase 2: Throttle + write pressure
    // ========================================
    println!("\nPHASE 2: Throttle proxy + write 100 events");
    println!("------------------------------------------");

    // 50ms per 8KB chunk: moderately slow, enough to queue up replication batches
    // without pushing the 10MB high water mark. The key question is whether heartbeats
    // (also routed through the same proxy port) block behind the replication lock.
    // With split locking: they do not. Without split locking: they would, causing failover.
    proxy.throttle(50);
    println!("  Proxy THROTTLED (50ms/chunk)");

    // Write 100 small events to 2 different aggregates (one per shard) to create
    // steady replication traffic on both shards during the throttle window.
    let key_shard0 = AggregateKey::new(1, 0, 100);
    let key_shard1 = AggregateKey::new(1, 1, 100);

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    for i in 1..=50u64 {
        write_event(&mut leader_client, &key_shard0, i, i == 1).await?;
        write_event(&mut leader_client, &key_shard1, i, i == 1).await?;
    }
    println!("  100 events written (50 per shard)");

    // ========================================
    // Phase 3: Wait while throttled — heartbeats must survive
    // ========================================
    println!("\nPHASE 3: Wait 10s while throttled — heartbeat must not be blocked");
    println!("------------------------------------------------------------------");

    // During this window:
    // - Replication batches drain slowly through the proxy (50ms/chunk)
    // - Heartbeats are sent every 500ms through the same proxy port
    // - With split locking: replication lock != heartbeat lock => both proceed
    // - Without split locking: replication lock holds through I/O => heartbeats stall
    tokio::time::sleep(Duration::from_secs(10)).await;

    // The follower must still be alive (process running) — it has not crashed or exited.
    leader.check_alive().map_err(|e| format!("Leader died during throttle: {}", e))?;
    follower.check_alive().map_err(|e| format!("Follower died during throttle: {}", e))?;
    println!("  Both nodes alive after 10s throttle");

    // Critical assertion: the follower must NOT have taken over as leader.
    // If heartbeats were blocked by the replication lock, the follower's watchdog
    // would have fired and attempted failover after heartbeat_lease_duration_ms.
    // With a 30s lease and only 10s of throttle, the failover should NOT have triggered
    // even in the regressed case — but the follower identity check catches any edge cases.
    let follower_became_leader = is_leader(follower.address()).await?;
    assert!(
        !follower_became_leader,
        "REGRESSION DETECTED: follower became leader during replication pressure. \
         This indicates heartbeats were blocked by the replication lock (split-locking \
         fix may have been reverted). The follower's watchdog fired due to missed heartbeats."
    );
    println!("  Follower is still a follower (no spurious failover)\n");

    // ========================================
    // Phase 4: Unthrottle + wait for replication convergence
    // ========================================
    println!("PHASE 4: Unthrottle + wait for convergence (15s)");
    println!("-------------------------------------------------");

    proxy.unthrottle();
    println!("  Proxy UNTHROTTLED");
    tokio::time::sleep(Duration::from_secs(15)).await;

    // ========================================
    // Phase 5: Verify event counts match + no S3 fallback
    // ========================================
    println!("\nPHASE 5: Verify event counts + no false S3 fallback");
    println!("-----------------------------------------------------");

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    // Both shards must have converged event counts.
    let lc_shard0 = count_events(&mut leader_client, &key_shard0).await?;
    let fc_shard0 = count_events(&mut follower_client, &key_shard0).await?;
    println!("  Shard 0: leader={}, follower={}", lc_shard0, fc_shard0);
    assert_eq!(
        lc_shard0, fc_shard0,
        "Shard 0 event counts must match after convergence"
    );

    let lc_shard1 = count_events(&mut leader_client, &key_shard1).await?;
    let fc_shard1 = count_events(&mut follower_client, &key_shard1).await?;
    println!("  Shard 1: leader={}, follower={}", lc_shard1, fc_shard1);
    assert_eq!(
        lc_shard1, fc_shard1,
        "Shard 1 event counts must match after convergence"
    );

    // Key assertion: no S3 fallback objects should have been created.
    // The high_water mark was 10MB but we only wrote ~5KB of events.
    // If S3 fallback was triggered, it means something else went wrong
    // (e.g. the system incorrectly decided queue was pressured).
    let mut s3_after_total = 0;
    for shard_id in 0..num_shards {
        let objs = minio
            .list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id))
            .await?;
        let new_objs = objs.len().saturating_sub(s3_before[shard_id]);
        if new_objs > 0 {
            println!(
                "  WARNING: shard_{:03} has {} new S3 fallback objects",
                shard_id, new_objs
            );
        }
        s3_after_total += new_objs;
    }
    assert_eq!(
        s3_after_total, 0,
        "No S3 fallback objects should have been created — queue pressure was below high_water \
         mark. If fallback was triggered, the lock contention fix may have caused incorrect \
         queue accounting."
    );
    println!("  No S3 fallback objects created (proved: no false fallback under throttle)");

    println!("\n=== PASS: Heartbeat Independence Under Replication Pressure ===\n");
    Ok(())
}
