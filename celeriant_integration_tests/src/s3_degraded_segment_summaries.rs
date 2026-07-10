//! Sealed Segment Summary Sidecar Test (TCP + S3 paths)
//!
//! Verifies that segment summaries land as `log_<id>.summary` sidecars on local NVMe
//! across rotation, in both replication paths:
//!
//! - **TCP path (follower alive):** the leader's per-cycle sweep at the end of
//!   `replicate` takes any sealed-and-fully-replicated summary out of memcache and
//!   writes the sidecar. The follower writes its own sidecar synchronously inside
//!   fsync (non-leader branch in `shard_wal_sync.rs`).
//! - **S3 fallback path (follower offline):** same leader sweep runs after
//!   `run_s3_fallback` drains the snapshot. Without this, summaries staged at
//!   rotation time stay orphaned in memcache forever — the read cursor heals on
//!   reload via the WAL header, but the precomputed summary used by list/pagination
//!   cursors does not.
//!
//! Phases:
//!   1. Start cluster, sanity-check TCP replication and zero S3 fallback objects.
//!   2. TCP burst: force ≥2 rotations while follower is alive.
//!   3. Verify zero new S3 fallback objects, both leader and follower have sidecars.
//!   4. Kill follower → leader degrades to S3 fallback.
//!   5. S3 burst: force more rotations under degraded mode.
//!   6. Verify S3 fallback objects exist and leader has sidecars for every sealed
//!      segment (TCP-phase + S3-phase combined).
//!
//! Run with: cargo run --bin s3_degraded_segment_summaries_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    poll_converged_count, s3_cluster_config, write_event, write_large_event, MinioContainer,
    ServerConfig, TestServer, FOLLOWER_CONVERGENCE_TIMEOUT,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::path::Path;
use std::time::Duration;

/// Read a shard's data dir and return `(wal_log_ids, summary_log_ids)`, both sorted ascending.
fn scan_log_files(shard_dir: &Path) -> std::io::Result<(Vec<u64>, Vec<u64>)> {
    let mut wal_ids: Vec<u64> = Vec::new();
    let mut summary_ids: Vec<u64> = Vec::new();
    for entry in std::fs::read_dir(shard_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(stem) = name_str.strip_prefix("log_") {
            if let Some(id) = stem.strip_suffix(".wal").and_then(|s| s.parse::<u64>().ok()) {
                wal_ids.push(id);
            } else if let Some(id) = stem.strip_suffix(".summary").and_then(|s| s.parse::<u64>().ok()) {
                summary_ids.push(id);
            }
        }
    }
    wal_ids.sort();
    summary_ids.sort();
    Ok((wal_ids, summary_ids))
}

/// Assert that every sealed (non-active, i.e. not the highest log_id) WAL has a matching
/// `.summary` sidecar, and that at least one rotation occurred.
fn assert_sealed_summaries(label: &str, wal_ids: &[u64], summary_ids: &[u64]) {
    assert!(
        wal_ids.len() >= 2,
        "{}: expected ≥2 WAL segments (proving ≥1 rotation); got {} ({:?})",
        label, wal_ids.len(), wal_ids
    );
    let active_id = *wal_ids.last().unwrap();
    let sealed_ids: Vec<u64> = wal_ids.iter().copied().filter(|id| *id != active_id).collect();
    let missing: Vec<u64> = sealed_ids
        .iter()
        .copied()
        .filter(|id| !summary_ids.contains(id))
        .collect();
    assert!(
        missing.is_empty(),
        "{}: sealed segments missing .summary sidecar files: {:?} (sealed={:?}, summaries={:?})",
        label, missing, sealed_ids, summary_ids
    );
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Sealed Segment Summary Sidecar Test (TCP + S3 paths) ===\n");

    let port_base = 17800 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-summary-sidecars").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 1;
    let aggregate_key = AggregateKey::new(1, 1, 1);
    let shard_id = (aggregate_key.aggregate_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", shard_id);

    let base = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    let config = ServerConfig {
        // 15MB segments force multiple rotations under a modest write burst (default is 1GB).
        shard_log_preallocate_bytes: 15 * 1024 * 1024,
        ..base
    };

    // ========================================
    // PHASE 1: Start cluster, sanity-check TCP replication
    // ========================================
    println!("PHASE 1: Start cluster, verify TCP replication");
    println!("-----------------------------------------------");

    let leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let mut follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Waiting for election + replication connection...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    println!("  Writing 5 sanity events...");
    for i in 1..=5u64 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count =
        poll_converged_count(&mut follower_client, &aggregate_key, 5, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(follower_count, 5, "Follower should have 5 events via TCP");

    let pre = minio.list_objects(&shard_prefix).await?;
    assert!(
        pre.is_empty(),
        "Expected zero S3 fallback objects on healthy TCP, got {}",
        pre.len()
    );
    println!("  Cluster healthy: follower has 5 events, no S3 fallback objects\n");

    // ========================================
    // PHASE 2: Force rotations via the TCP path (follower alive)
    // ========================================
    println!("PHASE 2: TCP burst — force ≥2 rotations while follower is alive");
    println!("---------------------------------------------------------------");

    let event_size = 64 * 1024;
    // 600 × 64KB ≈ 37.5MB against 15MB segments forces ≥2 rotations with margin.
    let tcp_burst: u64 = 600;
    println!(
        "  Writing {} events × {}B (~{:.1}MB total)...",
        tcp_burst,
        event_size,
        (tcp_burst as f64 * event_size as f64) / (1024.0 * 1024.0)
    );
    for i in 6..6 + tcp_burst {
        write_large_event(&mut leader_client, &aggregate_key, i, event_size).await?;
        if (i - 5) % 50 == 0 {
            println!("    {}/{} written", i - 5, tcp_burst);
        }
    }
    // Defensive: write_event returns after replicate completes, but allow any tail-end
    // sweep + fsync on either node to fully settle before scanning data dirs.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ========================================
    // PHASE 3: Verify TCP path produced sidecars on both nodes, no S3 fallback used
    // ========================================
    println!("\nPHASE 3: Verify TCP path produced sidecars on both nodes");
    println!("--------------------------------------------------------");

    let post_tcp_fallback = minio.list_objects(&shard_prefix).await?;
    assert!(
        post_tcp_fallback.is_empty(),
        "Expected zero S3 fallback objects after TCP burst (follower alive); got {}",
        post_tcp_fallback.len()
    );
    println!("  S3 fallback objects: 0 (TCP path was used end-to-end)");

    let leader_dir = leader.config().data_root.join(format!("shard_{}", shard_id));
    let follower_dir = follower.config().data_root.join(format!("shard_{}", shard_id));

    let (leader_wal_tcp, leader_summaries_tcp) = scan_log_files(&leader_dir)?;
    let (follower_wal_tcp, follower_summaries_tcp) = scan_log_files(&follower_dir)?;
    println!("  Leader   wal={:?} summaries={:?}", leader_wal_tcp, leader_summaries_tcp);
    println!("  Follower wal={:?} summaries={:?}", follower_wal_tcp, follower_summaries_tcp);

    assert_sealed_summaries("leader (TCP phase)", &leader_wal_tcp, &leader_summaries_tcp);
    assert_sealed_summaries("follower (TCP phase)", &follower_wal_tcp, &follower_summaries_tcp);

    let tcp_sealed_count = leader_wal_tcp.len() - 1;
    println!(
        "  TCP path covered: {} sealed segments have sidecars on leader and follower",
        tcp_sealed_count
    );

    // ========================================
    // PHASE 4: Kill follower → leader degrades to S3 fallback
    // ========================================
    println!("\nPHASE 4: Kill follower, leader enters degraded mode");
    println!("---------------------------------------------------");

    drop(follower_client);
    follower.stop();
    println!("  Follower stopped; waiting for heartbeat-loss + S3 lease pre-renewal...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // PHASE 5: Force more rotations under S3 fallback
    // ========================================
    println!("\nPHASE 5: S3 burst — force more rotations under S3 fallback");
    println!("-----------------------------------------------------------");

    // 400 × 64KB ≈ 25MB ensures at least one more rotation while in S3 fallback.
    let s3_burst: u64 = 400;
    let s3_start = 6 + tcp_burst;
    println!(
        "  Writing {} events × {}B (~{:.1}MB total)...",
        s3_burst,
        event_size,
        (s3_burst as f64 * event_size as f64) / (1024.0 * 1024.0)
    );
    for i in s3_start..s3_start + s3_burst {
        write_large_event(&mut leader_client, &aggregate_key, i, event_size).await?;
        if (i - s3_start + 1) % 50 == 0 {
            println!("    {}/{} written", i - s3_start + 1, s3_burst);
        }
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ========================================
    // PHASE 6: Verify S3 fallback used and leader has sidecars for ALL sealed segments
    // ========================================
    println!("\nPHASE 6: Verify S3 fallback path also produced sidecars");
    println!("-------------------------------------------------------");

    let s3_fallback = minio.list_objects(&shard_prefix).await?;
    assert!(
        !s3_fallback.is_empty(),
        "Expected S3 fallback objects after killing follower; got 0"
    );
    println!(
        "  S3 fallback objects: {} (degraded mode engaged)",
        s3_fallback.len()
    );

    let (leader_wal_final, leader_summaries_final) = scan_log_files(&leader_dir)?;
    println!(
        "  Leader wal={:?} summaries={:?}",
        leader_wal_final, leader_summaries_final
    );

    assert!(
        leader_wal_final.len() > leader_wal_tcp.len(),
        "Expected new sealed segments after S3 burst; before={:?} after={:?}",
        leader_wal_tcp, leader_wal_final
    );
    assert_sealed_summaries(
        "leader (TCP+S3 combined)",
        &leader_wal_final,
        &leader_summaries_final,
    );

    let s3_sealed_count = (leader_wal_final.len() - 1) - tcp_sealed_count;
    println!(
        "  S3 path covered: {} additional sealed segments + all {} TCP-phase sidecars intact",
        s3_sealed_count, tcp_sealed_count
    );

    println!("\n=== All Tests Passed ===\n");
    Ok(())
}
