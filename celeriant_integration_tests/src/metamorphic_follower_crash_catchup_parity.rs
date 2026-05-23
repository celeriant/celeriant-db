//! Metamorphic oracle: follower crash + boot-catchup payload parity.
//!
//! Phase A writes a baseline workload with both nodes healthy (TCP
//! replication). The follower is SIGKILLed; phase B keeps writing to the
//! leader, which falls back to S3 uploads per batch. The follower is
//! restarted against the same data dir, performs S3 boot catchup, and
//! rejoins. Phase D then drives further writes through the resumed TCP
//! replication path — a distinct code path from boot catchup. Finally the
//! full event list of every aggregate is read from both nodes and diffed
//! byte-for-byte.
//!
//! Catches divergence introduced by the S3-catchup path: wrong ordering,
//! missed events, double-applied events, hash chain breakage. Count-based
//! coverage lives in `s3_fallback_catchup`; this is the payload-level
//! upgrade.
//!
//! Scope: clean boot catchup only. The follower never wrote while down, so
//! its WAL is a strict prefix of what S3 has — truncation must not fire.
//! The divergence-recovery case (follower had its own writes before
//! leaving) is Scenario C, not this test.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    metamorphic_common::{diff_aggregate, format_key, response_digest, DiffMode},
    poll_event_count, read_all_batches, s3_cluster_config,
    wait_for_election_and_replication, write_event, MinioContainer, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

const AGGREGATE_COUNT: u128 = 4;
const PHASE_A_EVENTS: u64 = 20;
const PHASE_B_EVENTS: u64 = 20;
const PHASE_D_EVENTS: u64 = 10;
const TOTAL_EVENTS: u64 = PHASE_A_EVENTS + PHASE_B_EVENTS + PHASE_D_EVENTS;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Metamorphic: Follower Crash + Catchup Parity ===\n");

    let port_base = 18600 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;
    // Metrics sidecar port. Both nodes get the same value; only the leader
    // (started first) wins the bind, and that's the one we want to scrape.
    let metrics_port = port_base + 20;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = 4;
    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    // Long heartbeat/lease: no failover in this scenario. Keep the leader
    // stable while the follower is down so the whole run stays inside the
    // `Leader` state on the survivor.
    config.heartbeat_lease_duration_ms = 30_000;
    config.s3_lease_duration_ms = 30_000;
    config.metrics_port = metrics_port;

    println!("Starting two-node cluster ({} shards, S3 election)...", num_shards);
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let mut follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    let keys: Vec<AggregateKey> = (0..AGGREGATE_COUNT)
        .map(|shard_id| AggregateKey::new(1, shard_id, 1))
        .collect();

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // ── Phase A: baseline writes (TCP replication, both nodes healthy) ────
    println!(
        "\nPhase A: writing events 1..={} × {} aggregates round-robin (healthy cluster, TCP)...",
        PHASE_A_EVENTS, keys.len()
    );
    for event_num in 1..=PHASE_A_EVENTS {
        for key in &keys {
            write_event(&mut leader_client, key, event_num, event_num == 1).await?;
        }
    }
    println!("  Phase A complete ({} writes acknowledged).", PHASE_A_EVENTS * keys.len() as u64);

    // ── SIGKILL follower; leader continues via S3 fallback ────────────────
    println!("\nSIGKILLing follower...");
    follower.stop();

    println!(
        "Phase B: writing events {}..={} × {} aggregates round-robin (S3 fallback)...",
        PHASE_A_EVENTS + 1, PHASE_A_EVENTS + PHASE_B_EVENTS, keys.len()
    );
    for event_num in (PHASE_A_EVENTS + 1)..=(PHASE_A_EVENTS + PHASE_B_EVENTS) {
        for key in &keys {
            write_event(&mut leader_client, key, event_num, false).await?;
        }
    }
    println!("  Phase B complete ({} writes acknowledged via S3 fallback).",
        PHASE_B_EVENTS * keys.len() as u64);

    // ── Restart follower; wait for boot catchup ───────────────────────────
    println!("\nRestarting follower (same data dir — boot catchup from S3)...");
    follower.restart().await?;

    let target_post_catchup = PHASE_A_EVENTS + PHASE_B_EVENTS;
    println!("  Waiting for follower to catch up to {} events on every aggregate...",
        target_post_catchup);
    for key in &keys {
        let count = poll_event_count(
            follower.address(),
            key,
            target_post_catchup as usize,
            Duration::from_secs(30),
        ).await;
        println!("    aggregate {}: {} events", format_key(key), count);
    }

    // ── Phase D: new writes exercise resumed TCP replication ──────────────
    println!(
        "\nPhase D: writing events {}..={} × {} aggregates round-robin (post-heal)...",
        target_post_catchup + 1, TOTAL_EVENTS, keys.len()
    );
    for event_num in (target_post_catchup + 1)..=TOTAL_EVENTS {
        for key in &keys {
            write_event(&mut leader_client, key, event_num, false).await?;
        }
    }
    println!("  Phase D complete ({} writes acknowledged).", PHASE_D_EVENTS * keys.len() as u64);

    // The first post-heal writes may still take the S3 fallback path if the
    // leader has not yet flipped the follower back from `FollowerCatchingUp`.
    // Those ACK after S3 upload, not after the follower applies them — so
    // poll the follower until it has all TOTAL_EVENTS before diffing.
    println!("  Waiting for follower to reach {} events...", TOTAL_EVENTS);
    for key in &keys {
        let count = poll_event_count(
            follower.address(),
            key,
            TOTAL_EVENTS as usize,
            Duration::from_secs(30),
        ).await;
        println!("    aggregate {}: {} events", format_key(key), count);
    }

    // ── Read both nodes and diff ──────────────────────────────────────────
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let mut mismatches = Vec::<String>::new();
    for key in &keys {
        let leader_batches = read_all_batches(&mut leader_client, key).await?;
        let follower_batches = read_all_batches(&mut follower_client, key).await?;
        let lh = response_digest(&leader_batches);
        let fh = response_digest(&follower_batches);
        println!(
            "  aggregate {}: leader_batches={}, follower_batches={}, response_digest leader={:016x} follower={:016x}",
            format_key(key), leader_batches.len(), follower_batches.len(), lh, fh,
        );
        if (leader_batches.len() as u64) != TOTAL_EVENTS {
            mismatches.push(format!(
                "aggregate {}: leader returned {} batches, expected {}",
                format_key(key), leader_batches.len(), TOTAL_EVENTS
            ));
        }
        if (follower_batches.len() as u64) != TOTAL_EVENTS {
            mismatches.push(format!(
                "aggregate {}: follower returned {} batches, expected {}",
                format_key(key), follower_batches.len(), TOTAL_EVENTS
            ));
        }
        if let Err(msg) = diff_aggregate(key, &leader_batches, &follower_batches, DiffMode::SameRun) {
            mismatches.push(msg);
        }
    }

    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("  MISMATCH: {}", m);
        }
        return Err(format!(
            "metamorphic follower-crash + catchup parity failed on {} aggregate(s)",
            mismatches.len()
        ).into());
    }

    println!("\n=== PASS: leader and follower are byte-identical after S3 boot catchup + TCP resume ===");
    Ok(())
}
