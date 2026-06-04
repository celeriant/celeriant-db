//! Metamorphic oracle: post-failover leader/follower payload parity.
//!
//! Phase A writes a baseline workload to leader A. A is SIGKILLed; follower B
//! takes over via S3 lease race. Phase B writes more events to the new leader
//! B. A then restarts and rejoins as follower. Once A has caught up, both
//! nodes' full event lists are read and diffed byte-for-byte.
//!
//! Catches divergence introduced by the failover path itself: the lease
//! handover, B's promotion-batch upload (`invariants.md` "Replication
//! Protocol", line 54), and A's S3 catchup on rejoin. The leader/follower
//! parity test covers stable-state replication; the standalone-vs-cluster
//! test covers mode-vs-mode drift; this one covers the failover-cycle drift
//! that neither sees.
//!
//! Scope: clean-handoff failover only. All Phase-A writes are ACKed before
//! the kill, so per `invariants.md` "Durability" they are durable on both
//! nodes — A has no unreplicated writes to lose. The
//! crash-before-replication divergence-recovery case is covered by
//! `edge_wal_divergence_and_recovery`; not duplicated here.
//!
//! No S3-fallback assertion: B uploads a promotion batch on election
//! (`invariants.md` line 54), so `cluster/fallback/` will be non-empty.
//! That assertion belongs in `metamorphic_leader_follower_parity` only.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    metamorphic_common::{diff_aggregate, format_key, response_digest, wait_for_promotion, DiffMode},
    poll_event_count, read_all_batches, s3_cluster_config, wait_for_election_and_replication,
    write_event, MinioContainer, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

const AGGREGATE_COUNT: u128 = 4;
const EVENTS_PER_PHASE: u64 = 20;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Metamorphic: Post-Failover Parity ===\n");

    let port_base = 18500 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = 4;
    // Shorter heartbeat + S3 lease than the stable-state metamorphic tests:
    // failover requires both to expire on the survivor before it challenges
    // for the lease via S3 CAS. 10s mirrors `s3_failover_and_recovery` and
    // keeps the whole test inside the 60s estimate.
    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 10_000;
    config.s3_lease_duration_ms = 10_000;

    println!("Starting two-node cluster ({} shards, S3 election)...", num_shards);
    let mut node_a = TestServer::start_with_config_labeled(node_a_port, config.clone(), "node-a".into()).await?;
    let node_b = TestServer::start_with_config_labeled(node_b_port, config, "node-b".into()).await?;
    println!("  Node A at {}, Node B at {}", node_a.address(), node_b.address());

    println!("Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    let keys: Vec<AggregateKey> = (0..AGGREGATE_COUNT)
        .map(|shard_id| AggregateKey::new(1, shard_id, 1))
        .collect();

    // ── Phase A: baseline writes against initial leader (A) ───────────────
    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    println!(
        "\nPhase A: writing events 1..={} × {} aggregates round-robin to leader A...",
        EVENTS_PER_PHASE, keys.len()
    );
    for event_num in 1..=EVENTS_PER_PHASE {
        for key in &keys {
            write_event(&mut a_client, key, event_num, event_num == 1).await?;
        }
    }
    println!("  Phase A complete ({} writes acknowledged).", EVENTS_PER_PHASE * keys.len() as u64);

    // ── Kill leader, wait for B promotion ─────────────────────────────────
    drop(a_client);
    node_a.stop();
    println!("\nLeader A killed (SIGKILL).");

    println!("Waiting for B to win S3 lease and serve writes...");
    let mut b_client = wait_for_promotion(node_b.address()).await?;
    println!("  Node B promoted to leader.");

    // ── Phase B: writes against new leader (B) ────────────────────────────
    println!(
        "\nPhase B: writing events {}..={} × {} aggregates round-robin to leader B...",
        EVENTS_PER_PHASE + 1, 2 * EVENTS_PER_PHASE, keys.len()
    );
    for event_num in (EVENTS_PER_PHASE + 1)..=(2 * EVENTS_PER_PHASE) {
        for key in &keys {
            write_event(&mut b_client, key, event_num, false).await?;
        }
    }
    println!("  Phase B complete ({} writes acknowledged).", EVENTS_PER_PHASE * keys.len() as u64);

    // ── Restart old leader A, wait for catchup ────────────────────────────
    println!("\nRestarting node A — rejoins as follower...");
    node_a.restart().await?;

    println!("  Waiting for A to catch up to {} events on every aggregate...", 2 * EVENTS_PER_PHASE);
    for key in &keys {
        let count = poll_event_count(
            node_a.address(),
            key,
            (2 * EVENTS_PER_PHASE) as usize,
            Duration::from_secs(30),
        ).await;
        println!("    aggregate {}: {} events", format_key(key), count);
    }

    // ── Read both nodes and diff ──────────────────────────────────────────
    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    let mut mismatches = Vec::<String>::new();
    for key in &keys {
        let a_batches = read_all_batches(&mut a_client, key).await?;
        let b_batches = read_all_batches(&mut b_client, key).await?;
        let ah = response_digest(&a_batches);
        let bh = response_digest(&b_batches);
        println!(
            "  aggregate {}: A_batches={}, B_batches={}, response_digest A={:016x} B={:016x}",
            format_key(key), a_batches.len(), b_batches.len(), ah, bh,
        );
        if (a_batches.len() as u64) != 2 * EVENTS_PER_PHASE {
            mismatches.push(format!(
                "aggregate {}: A returned {} batches, expected {}",
                format_key(key), a_batches.len(), 2 * EVENTS_PER_PHASE
            ));
        }
        if (b_batches.len() as u64) != 2 * EVENTS_PER_PHASE {
            mismatches.push(format!(
                "aggregate {}: B returned {} batches, expected {}",
                format_key(key), b_batches.len(), 2 * EVENTS_PER_PHASE
            ));
        }
        if let Err(msg) = diff_aggregate(key, &a_batches, &b_batches, DiffMode::SameRun) {
            mismatches.push(msg);
        }
    }

    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("  MISMATCH: {}", m);
        }
        return Err(format!(
            "metamorphic post-failover parity failed on {} aggregate(s)",
            mismatches.len()
        ).into());
    }

    println!("\n=== PASS: A and B are byte-identical after failover + rejoin ===");
    Ok(())
}
