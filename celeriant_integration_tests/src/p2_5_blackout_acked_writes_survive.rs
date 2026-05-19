//! P2-5. Acknowledged Write Survival Across an S3 Blackout
//!
//! Concurrent writers across N aggregates push events as fast as they can.
//! Mid-bench we SIGKILL the follower AND pause MinIO ("both replication
//! paths dead") long enough for the leader's S3 lease to lapse. We then
//! unpause MinIO and restart the follower; the cluster heals via S3 CAS
//! and S3 catchup. The invariant: **every `client_seq` that the bench got
//! `Ok` for must be readable on BOTH nodes after heal**.
//!
//! This isolates the chaos-suite false-ack class at integration-test speed:
//! ~90s, no rpi cluster, deterministic seed. The chaos suite at 8000 tasks
//! on real hardware finds the same bug shape but iterates slowly.
//!
//! The originally-failing chaos scenario (`idempotency_audit_fast_blackout`)
//! is a 4-shard 8000-task version of this. We use 2 nodes, 4 shards, 50
//! concurrent writers — enough to put the replication coordinators under
//! load without overwhelming a local laptop.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_wal::aggregate_key::AggregateKey;
use crate::{read_all_batches, s3_cluster_config, wait_for_election_and_replication, MinioContainer, TestServer};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const PORT_BASE: u16 = 20500;
const NUM_AGGREGATES: u128 = 50;
const WRITERS_PER_AGG: u64 = 1; // one writer = one client_id = one aggregate
const BENCH_BUDGET: Duration = Duration::from_secs(60);
const BLACKOUT_DURATION: Duration = Duration::from_secs(15);
const HEAL_SETTLE: Duration = Duration::from_secs(25);

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P2-5: Acknowledged Write Survival Across S3 Blackout ===\n");

    let leader_port = PORT_BASE;
    let follower_port = PORT_BASE + 100;
    let minio_port = PORT_BASE + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-blackout-survival").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;

    // Short S3 lease so the blackout actually lapses leadership in test budget.
    let mut leader_config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);
    leader_config.heartbeat_lease_duration_ms = 5_000;
    leader_config.s3_lease_duration_ms = 5_000;

    let mut follower_config = leader_config.clone();
    follower_config.client_port = follower_port;
    follower_config.replication_port = follower_port + 1;

    println!("Starting two-node cluster (num_shards={num_shards}, s3_lease=5s)");
    let leader = TestServer::start_with_config(leader_port, leader_config).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut follower = TestServer::start_with_config(follower_port, follower_config).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    wait_for_election_and_replication().await;
    println!("  ✓ Cluster ready\n");

    // ── Phase 1: ramp up concurrent writers ────────────────────────────────
    println!("Phase 1: spawn {NUM_AGGREGATES} concurrent writers (each owns one aggregate)");
    let stop = Arc::new(AtomicU64::new(0)); // 0 = running, 1 = stop
    let acked: Arc<Vec<AtomicU64>> = Arc::new((0..NUM_AGGREGATES).map(|_| AtomicU64::new(0)).collect());
    let attempted: Arc<Vec<AtomicU64>> = Arc::new((0..NUM_AGGREGATES).map(|_| AtomicU64::new(0)).collect());

    let leader_addr = leader.address().to_string();
    let mut writer_handles = Vec::with_capacity(NUM_AGGREGATES as usize);

    for agg_idx in 0..NUM_AGGREGATES {
        let stop = Arc::clone(&stop);
        let acked = Arc::clone(&acked);
        let attempted = Arc::clone(&attempted);
        let addr = leader_addr.clone();
        writer_handles.push(tokio::spawn(async move {
            let mut client = match CeleriantClient::connect(&addr).await {
                Ok(c) => c,
                Err(_) => return,
            };
            let key = AggregateKey::new(1, 1, agg_idx);
            let mut seq: u64 = 0;
            let _ = WRITERS_PER_AGG; // single writer per aggregate
            while stop.load(Ordering::Relaxed) == 0 {
                seq += 1;
                attempted[agg_idx as usize].fetch_add(1, Ordering::Relaxed);
                let succeeded = crate::write_event(&mut client, &key, seq, seq == 1).await.is_ok();
                if succeeded {
                    acked[agg_idx as usize].store(seq, Ordering::Relaxed);
                } else {
                    // retry same seq (idempotent on server side)
                    seq -= 1;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }));
    }

    // ── Phase 2: warm up, then blackout (kill follower + pause minio) ──────
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("Phase 2: SIGKILL follower + pause MinIO for {:?}", BLACKOUT_DURATION);
    follower.stop();
    minio.pause()?;

    tokio::time::sleep(BLACKOUT_DURATION).await;

    println!("Phase 3: unpause MinIO + restart follower; heal {:?}", HEAL_SETTLE);
    minio.unpause()?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    follower.restart().await?;

    tokio::time::sleep(HEAL_SETTLE).await;

    // ── Phase 4: keep writing a bit more after heal, then stop ─────────────
    println!("Phase 4: continue writes for 10s post-heal, then stop");
    tokio::time::sleep(Duration::from_secs(10)).await;
    stop.store(1, Ordering::Relaxed);

    // Drain writers (some may be stuck mid-request — bound by bench budget)
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    for h in writer_handles {
        let remaining = drain_deadline.saturating_duration_since(tokio::time::Instant::now());
        let _ = tokio::time::timeout(remaining.max(Duration::from_millis(100)), h).await;
    }

    let total_acked: u64 = acked.iter().map(|a| a.load(Ordering::Relaxed)).sum();
    let total_attempted: u64 = attempted.iter().map(|a| a.load(Ordering::Relaxed)).sum();
    println!("  Bench: {total_acked} acked / {total_attempted} attempted across {NUM_AGGREGATES} aggregates");

    if BENCH_BUDGET.as_secs() > 0 && total_acked == 0 {
        return Err("bench acked zero writes — cluster never accepted any traffic".into());
    }

    // ── Phase 5: settle, then audit each aggregate on BOTH nodes ───────────
    // Long-ish settle so periodic heartbeat-probes can converge the
    // follower's tail before the audit. If a gap remains after this, it's a
    // real convergence bug (replication is write-driven and writers stopped).
    tokio::time::sleep(Duration::from_secs(60)).await;
    println!("Phase 5: audit every acked seq on both nodes");

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    let mut mismatches: Vec<String> = Vec::new();
    let mut agg_missing_count = 0usize;
    for agg_idx in 0..NUM_AGGREGATES {
        let max_acked = acked[agg_idx as usize].load(Ordering::Relaxed);
        if max_acked == 0 {
            continue;
        }
        let key = AggregateKey::new(1, 1, agg_idx);
        let leader_seqs = read_all_client_seqs(&mut leader_client, &key).await.unwrap_or_default();
        let follower_seqs = read_all_client_seqs(&mut follower_client, &key).await.unwrap_or_default();

        let leader_missing: Vec<u64> = (1..=max_acked).filter(|s| !leader_seqs.contains(s)).collect();
        let follower_missing: Vec<u64> = (1..=max_acked).filter(|s| !follower_seqs.contains(s)).collect();

        if !leader_missing.is_empty() || !follower_missing.is_empty() {
            agg_missing_count += 1;
            if mismatches.len() < 10 {
                mismatches.push(format!(
                    "agg={agg_idx} max_acked={max_acked} leader_present={} follower_present={} leader_missing={:?} follower_missing={:?}",
                    leader_seqs.len(), follower_seqs.len(),
                    &leader_missing[..leader_missing.len().min(8)],
                    &follower_missing[..follower_missing.len().min(8)]
                ));
            }
        }
    }

    if agg_missing_count > 0 {
        for m in &mismatches {
            println!("  MISMATCH: {m}");
        }
        return Err(format!(
            "p2_5: {agg_missing_count} of {NUM_AGGREGATES} aggregates lost at least one acked write across the blackout"
        ).into());
    }

    println!("\n=== PASS: every acked write survived the blackout on BOTH nodes ===");
    Ok(())
}

async fn read_all_client_seqs(
    client: &mut CeleriantClient,
    key: &AggregateKey,
) -> Result<std::collections::BTreeSet<u64>, Box<dyn std::error::Error>> {
    let batches = read_all_batches(client, key).await?;
    let mut seqs = std::collections::BTreeSet::new();
    for batch in batches {
        for ev in &batch.events {
            if ev.client_seq > 0 {
                seqs.insert(ev.client_seq);
            }
        }
    }
    Ok(seqs)
}
