//! Metamorphic oracle: rollback payload parity.
//!
//! Drives the leader into the "both paths dead" corner so a rollback fires,
//! then heals both dependencies and asserts that once the cluster converges
//! the two nodes are byte-identical.
//!
//! Phase A writes a baseline (TCP replication). The follower is SIGKILLed,
//! driving Phase B writes through S3 fallback. MinIO is paused, driving a
//! single Phase C write on each aggregate with *both* replication paths
//! dead — the leader fsyncs to local disk, neither TCP nor S3 fallback
//! succeeds, rollback fires, and the client receives an error. MinIO is
//! unpaused and the follower is restarted. Either node can win the S3 CAS
//! race to become leader, so the test does not hard-code the post-heal
//! leader. After convergence the full event list of every aggregate is
//! read from both nodes and diffed byte-for-byte.
//!
//! Catches rollback bugs that only surface post-heal: the rolled-back
//! write left a disk artefact, the cursor rewind kept an extra entry, or
//! the ancestors differ across nodes by a single metablock.
//!
//! Scope: the final event set is whatever the client got `Ok` for — not a
//! hardcoded count. A Phase C write that races the `pause()` and actually
//! ACKs is tolerated; the metamorphic property (both nodes agree) is the
//! real assertion.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    metamorphic_common::{diff_aggregate, format_key, response_digest, DiffMode},
    poll_event_count, read_all_batches, s3_cluster_config, scrape_counter,
    wait_for_election_and_replication, write_event, MinioContainer, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

const AGGREGATE_COUNT: u128 = 4;
const PHASE_A_EVENTS: u64 = 20;
const PHASE_B_EVENTS: u64 = 5;
const PHASE_C_EVENT: u64 = PHASE_A_EVENTS + PHASE_B_EVENTS + 1;
const PHASE_C_WRITE_BUDGET: Duration = Duration::from_secs(30);

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Metamorphic: Rollback Parity ===\n");

    let port_base = 18800 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 100;
    let minio_port = port_base + 10;
    // Metrics sidecar port: derived from port_base so it's collision-free with
    // both MinIO (+10) and the sibling node's ports (+100..). Both nodes get
    // the same value; only node-a (started first) wins the bind. Scraping
    // node-a is sufficient for this scenario — rollback fires on the leader.
    let metrics_port = port_base + 20;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = 4;
    // Short leases so the leader self-fences soon after MinIO pauses — we
    // want the rollback path + lease expiry sequence to complete inside the
    // test budget. 10s mirrors `metamorphic_post_failover_parity`.
    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 10_000;
    config.s3_lease_duration_ms = 10_000;
    config.metrics_port = metrics_port;

    println!("Starting two-node cluster ({} shards, S3 election)...", num_shards);
    let node_a = TestServer::start_with_config_labeled(node_a_port, config.clone(), "node-a".into()).await?;
    let mut node_b = TestServer::start_with_config_labeled(node_b_port, config, "node-b".into()).await?;
    println!("  Node A at {}, Node B at {}", node_a.address(), node_b.address());

    println!("Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    // Baseline rollback counter on node-a (the initial leader where rollback
    // is expected to fire in Phase C). Healthy baseline should be 0.
    let rollbacks_before =
        scrape_counter("127.0.0.1", metrics_port, "celeriant_replication_rollbacks_total").await?;
    println!("  node-a rollbacks_total baseline = {}", rollbacks_before);

    let keys: Vec<AggregateKey> = (0..AGGREGATE_COUNT)
        .map(|shard_id| AggregateKey::new(1, shard_id, 1))
        .collect();

    let mut a_client = CeleriantClient::connect(node_a.address()).await?;

    // ── Phase A: baseline writes (TCP replication, both nodes healthy) ────
    println!(
        "\nPhase A: writing events 1..={} × {} aggregates round-robin (healthy cluster)...",
        PHASE_A_EVENTS, keys.len()
    );
    for event_num in 1..=PHASE_A_EVENTS {
        for key in &keys {
            write_event(&mut a_client, key, event_num, event_num == 1).await?;
        }
    }
    println!("  Phase A complete ({} writes acknowledged).", PHASE_A_EVENTS * keys.len() as u64);

    // ── SIGKILL follower; leader continues via S3 fallback ────────────────
    println!("\nSIGKILLing follower (node B)...");
    node_b.stop();

    println!(
        "Phase B: writing events {}..={} × {} aggregates (S3 fallback)...",
        PHASE_A_EVENTS + 1, PHASE_A_EVENTS + PHASE_B_EVENTS, keys.len()
    );
    for event_num in (PHASE_A_EVENTS + 1)..=(PHASE_A_EVENTS + PHASE_B_EVENTS) {
        for key in &keys {
            write_event(&mut a_client, key, event_num, false).await?;
        }
    }
    println!("  Phase B complete ({} writes acknowledged via S3 fallback).",
        PHASE_B_EVENTS * keys.len() as u64);

    // ── Pause MinIO; Phase C writes should fail (both paths dead) ─────────
    println!("\nPausing MinIO (S3 now unreachable)...");
    minio.pause()?;

    // Attempt Phase C on a single aggregate only. One rollback is enough to
    // exercise the code path; attempting on all four would serialise through
    // per-shard S3 timeouts and blow the test budget without adding coverage.
    // The other aggregates remain at PHASE_A + PHASE_B and must be
    // byte-identical on both nodes after heal — that's the broader oracle.
    let phase_c_key = keys[0].clone();
    println!(
        "Phase C: attempting event {} on aggregate {} — both paths dead, rollback expected...",
        PHASE_C_EVENT, format_key(&phase_c_key)
    );
    // Per-key expected post-heal event count. Baseline is all Phase A + B
    // writes; if the Phase C write actually ACKed the aggregate bumps by one.
    // A write that returned Err (or timed out past the budget) was rolled
    // back and must not appear on either node.
    let mut expected_count: Vec<u64> = keys
        .iter()
        .map(|_| PHASE_A_EVENTS + PHASE_B_EVENTS)
        .collect();

    let write_fut = write_event(&mut a_client, &phase_c_key, PHASE_C_EVENT, false);
    match tokio::time::timeout(PHASE_C_WRITE_BUDGET, write_fut).await {
        Ok(Ok(())) => {
            expected_count[0] += 1;
            println!("  Phase C ACKed (unexpected but tolerated — S3 pause raced)");
        }
        Ok(Err(e)) => {
            println!("  Phase C rejected (expected): {}", e);
        }
        Err(_) => {
            println!(
                "  Phase C timed out after {:?} — treating as rolled back",
                PHASE_C_WRITE_BUDGET
            );
        }
    }
    drop(a_client);

    // Give the leader time to self-fence via S3 lease expiry before we
    // unpause. Without this the old leader may still hold a valid lease when
    // MinIO comes back, and the S3 CAS race is a no-op.
    println!("\nWaiting 12s for leader to self-fence (s3_lease_duration=10s)...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    // ── Heal: unpause MinIO, restart follower ─────────────────────────────
    println!("Unpausing MinIO...");
    minio.unpause()?;

    println!("Restarting node B (same data dir)...");
    node_b.restart().await?;

    // S3 CAS race + catchup on whichever node wins. Either outcome is valid.
    println!("Waiting 20s for S3 CAS race + catchup...");
    tokio::time::sleep(Duration::from_secs(20)).await;

    // ── Wait for convergence on every aggregate on both nodes ─────────────
    for (i, key) in keys.iter().enumerate() {
        let target = expected_count[i] as usize;
        println!("  aggregate {}: expected {} events", format_key(key), target);
        let a_count = poll_event_count(node_a.address(), key, target, Duration::from_secs(60)).await;
        let b_count = poll_event_count(node_b.address(), key, target, Duration::from_secs(60)).await;
        println!("    A={} B={}", a_count, b_count);
    }

    // ── Read both nodes and diff ──────────────────────────────────────────
    let mut a_read_client = CeleriantClient::connect(node_a.address()).await?;
    let mut b_read_client = CeleriantClient::connect(node_b.address()).await?;
    let mut mismatches = Vec::<String>::new();
    for (i, key) in keys.iter().enumerate() {
        let a_batches = read_all_batches(&mut a_read_client, key).await?;
        let b_batches = read_all_batches(&mut b_read_client, key).await?;
        let ah = response_digest(&a_batches);
        let bh = response_digest(&b_batches);
        let target = expected_count[i];
        println!(
            "  aggregate {}: A_batches={}, B_batches={}, expected={}, digest A={:016x} B={:016x}",
            format_key(key), a_batches.len(), b_batches.len(), target, ah, bh,
        );
        if (a_batches.len() as u64) != target {
            mismatches.push(format!(
                "aggregate {}: A returned {} batches, expected {} (from client ACK tally)",
                format_key(key), a_batches.len(), target
            ));
        }
        if (b_batches.len() as u64) != target {
            mismatches.push(format!(
                "aggregate {}: B returned {} batches, expected {} (from client ACK tally)",
                format_key(key), b_batches.len(), target
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
            "metamorphic rollback parity failed on {} aggregate(s)",
            mismatches.len()
        ).into());
    }

    // The test name is "rollback parity" — the core value comes from actually
    // exercising the rollback path. Byte-parity on its own would still pass if
    // a regression made Phase C fail earlier (e.g. at the client transport
    // layer) without ever reaching the rollback code. Assert the server-side
    // counter bumped by at least one to prove the rollback actually fired.
    let rollbacks_after =
        scrape_counter("127.0.0.1", metrics_port, "celeriant_replication_rollbacks_total").await?;
    let rollback_delta = rollbacks_after.saturating_sub(rollbacks_before);
    println!(
        "  node-a rollbacks_total: {} -> {} (delta = {})",
        rollbacks_before, rollbacks_after, rollback_delta
    );
    if rollback_delta == 0 {
        return Err(
            "rollback counter did not increment — Phase C must exercise the rollback path but didn't. \
             Byte-parity passed, but the test is no longer validating what its name claims."
                .into(),
        );
    }

    println!("\n=== PASS: both nodes byte-identical after rollback + heal; rollback path confirmed fired ===");
    Ok(())
}
