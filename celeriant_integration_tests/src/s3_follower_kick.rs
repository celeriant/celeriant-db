//! S3 Follower Kick Integration Test
//!
//! Tests the full coordinated kick cycle with 4 shards:
//! 1. Normal TCP replication works
//! 2. Block proxy → write events → S3 fallback (kick can't reach follower)
//! 3. Unblock proxy → write more → follower rejects (WAL gap) → S3 again → kick delivered
//! 4. Follower receives kick → FollowerCatchingUp → catches up from S3
//! 5. TCP replication resumes
//!
//! Key insight: the write path is synchronous through replication, so throttle
//! alone can't build queue pressure. Instead we block → build WAL gap → unblock →
//! the gap triggers FollowerTooFarBehind → S3 path → kick over now-open TCP.
//!
//! Run with: cargo run --bin s3_follower_kick_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{
    count_events, s3_cluster_config, write_event, MinioContainer, TcpProxy, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Follower Kick Integration Test (4 shards) ===\n");

    // ========================================
    // Setup
    // ========================================
    let port_base = 10600 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;
    let proxy_port = port_base + 200;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;
    let key_shard1 = AggregateKey::new(1, 1, 999);
    let key_shard2 = AggregateKey::new(1, 2, 999);

    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", follower_port + 1)).await?;
    println!("  Proxy {} → follower replication port {}", proxy_port, follower_port + 1);

    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    // Low max_catchup_gap_bytes: when leader tries to bridge WAL gap for follower
    // and the gap exceeds this, it triggers FollowerTooFarBehind → S3 fallback + kick.
    // FIXED_BLOCK_SIZE_BYTES=1024, so ~5 WAL entries exceed 4096.
    config.max_catchup_gap_bytes = 4096;
    // Long heartbeat timeout — we don't want failover, just kick
    config.heartbeat_lease_duration_ms = 30_000;

    let mut follower_config = config.clone();
    follower_config.advertised_replication_address = Some(proxy.address());

    println!("Starting two-node cluster (4 shards, max_catchup_gap=4096)...");
    config.log_level = "info".to_string();
    let leader = TestServer::start_with_config_labeled(leader_port, config, "leader".into()).await?;
    let follower =
        TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into())
            .await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("Waiting for election + discovery + replication connection...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // ========================================
    // Phase 1: Normal TCP replication (proxy open)
    // ========================================
    println!("\nPHASE 1: Normal TCP replication");
    println!("-------------------------------");

    println!("  Writing 3 events to each probe key...");
    for i in 1..=3 {
        write_event(&mut leader_client, &key_shard1, i, i == 1).await?;
        write_event(&mut leader_client, &key_shard2, i, i == 1).await?;
    }

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let count1 = count_events(&mut follower_client, &key_shard1).await?;
    let count2 = count_events(&mut follower_client, &key_shard2).await?;
    println!("  Follower: shard1={} events, shard2={} events", count1, count2);
    assert_eq!(count1, 3, "Follower shard 1 should have 3 events");
    assert_eq!(count2, 3, "Follower shard 2 should have 3 events");

    for shard_id in 0..num_shards {
        let objs = minio.list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id)).await?;
        assert!(objs.is_empty(), "No S3 fallback objects during normal replication (shard {})", shard_id);
    }
    println!("  No S3 fallback objects (TCP working)\n");

    // ========================================
    // Phase 2: Block proxy → write events → S3 fallback
    // ========================================
    println!("PHASE 2: Block proxy, write events (S3 fallback)");
    println!("------------------------------------------------");

    proxy.block();
    println!("  Proxy BLOCKED");

    // Write enough events to create a WAL gap > max_catchup_gap_bytes (4096).
    // Each WAL entry ~1024+ bytes in the catchup size estimate,
    // so 10 events per key = 20 entries >> 4096 bytes.
    println!("  Writing 10 events to each probe key while proxy blocked...");
    for i in 4..=13 {
        write_event(&mut leader_client, &key_shard1, i, false).await?;
        write_event(&mut leader_client, &key_shard2, i, false).await?;
    }
    println!("  Writes succeeded (leader fell back to S3)");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify S3 fallback happened during the block
    let mut s3_objects_phase2 = 0;
    for shard_id in 0..num_shards {
        let objs = minio.list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id)).await?;
        if !objs.is_empty() {
            println!("  shard_{:03}: {} S3 objects", shard_id, objs.len());
        }
        s3_objects_phase2 += objs.len();
    }
    println!("  Total S3 objects from block phase: {}", s3_objects_phase2);
    assert!(s3_objects_phase2 > 0, "S3 fallback should have triggered during proxy block");

    // Leader has 13 events per key, follower still has 3 (missed the blocked phase)
    let leader_count1 = count_events(&mut leader_client, &key_shard1).await?;
    let leader_count2 = count_events(&mut leader_client, &key_shard2).await?;
    println!("  Leader: shard1={}, shard2={}", leader_count1, leader_count2);
    assert_eq!(leader_count1, 13, "Leader should have 13 events on shard 1");
    assert_eq!(leader_count2, 13, "Leader should have 13 events on shard 2");

    // ========================================
    // Phase 3: Unblock proxy → write event → kick delivered
    // ========================================
    println!("\nPHASE 3: Unblock proxy, trigger kick via WAL gap");
    println!("------------------------------------------------");

    proxy.unblock();
    println!("  Proxy UNBLOCKED");

    // Give TCP connection time to recover
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Write one more event. The replication pipeline will:
    // 1. Reconnect to follower via proxy
    // 2. Send batch → follower rejects with WalIndexMismatch (it's 10 entries behind)
    // 3. Leader tries fetch_catchup_entries → gap > 4096 → FollowerTooFarBehind
    // 4. Falls back to S3 → sends kick (proxy is open now!)
    // 5. Follower receives kick → transitions to FollowerCatchingUp
    println!("  Writing event 14 to trigger kick...");
    write_event(&mut leader_client, &key_shard1, 14, false).await?;
    write_event(&mut leader_client, &key_shard2, 14, false).await?;
    println!("  Writes succeeded (kick should have been sent)");

    // ========================================
    // Phase 4: Wait for follower to catch up from S3
    // ========================================
    println!("\nPHASE 4: Wait for follower to catch up from S3");
    println!("----------------------------------------------");

    let leader_count1 = count_events(&mut leader_client, &key_shard1).await?;
    let leader_count2 = count_events(&mut leader_client, &key_shard2).await?;
    println!("  Leader: shard1={}, shard2={}", leader_count1, leader_count2);

    let timeout = Duration::from_secs(60);
    let start = std::time::Instant::now();
    let mut caught_up = false;

    while start.elapsed() < timeout {
        if let Ok(mut fc) = CeleriantClient::connect(follower.address()).await {
            let c1 = count_events(&mut fc, &key_shard1).await.unwrap_or(0);
            let c2 = count_events(&mut fc, &key_shard2).await.unwrap_or(0);
            println!(
                "  Follower: shard1={}/{}, shard2={}/{} ({:.0}s elapsed)",
                c1, leader_count1, c2, leader_count2, start.elapsed().as_secs_f64()
            );
            if c1 >= leader_count1 && c2 >= leader_count2 {
                caught_up = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    assert!(
        caught_up,
        "Follower should have caught up from S3 within {}s",
        timeout.as_secs()
    );
    println!("  Follower caught up from S3\n");

    // ========================================
    // Phase 5: Verify TCP replication resumes
    // ========================================
    println!("PHASE 5: Verify TCP replication resumes");
    println!("---------------------------------------");

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Record S3 counts before new writes
    let s3_before: Vec<usize> = {
        let mut counts = Vec::new();
        for shard_id in 0..num_shards {
            counts.push(minio.list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id)).await?.len());
        }
        counts
    };

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let base1 = count_events(&mut leader_client, &key_shard1).await?;
    let base2 = count_events(&mut leader_client, &key_shard2).await?;

    println!("  Writing 3 more events to each probe key...");
    for i in 0..3u64 {
        write_event(&mut leader_client, &key_shard1, base1 as u64 + i + 1, false).await?;
        write_event(&mut leader_client, &key_shard2, base2 as u64 + i + 1, false).await?;
    }

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let final1 = count_events(&mut follower_client, &key_shard1).await?;
    let final2 = count_events(&mut follower_client, &key_shard2).await?;
    let expected1 = base1 + 3;
    let expected2 = base2 + 3;
    println!("  Follower: shard1={}/{}, shard2={}/{}", final1, expected1, final2, expected2);
    assert_eq!(final1, expected1, "Follower shard 1 should have all events after re-join");
    assert_eq!(final2, expected2, "Follower shard 2 should have all events after re-join");

    // Verify no new S3 objects (TCP replication resumed)
    let mut new_s3 = false;
    for shard_id in 0..num_shards {
        let after = minio.list_objects(&format!("cluster/fallback/shard_{:03}/", shard_id)).await?.len();
        if after > s3_before[shard_id] {
            new_s3 = true;
            println!("  WARNING: shard_{:03} new S3 objects: {} → {}", shard_id, s3_before[shard_id], after);
        }
    }
    assert!(!new_s3, "No new S3 objects should appear after re-join (TCP replication resumed)");
    println!("  TCP replication resumed (no new S3 objects)");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
