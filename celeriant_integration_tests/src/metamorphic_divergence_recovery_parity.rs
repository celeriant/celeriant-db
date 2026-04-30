//! Metamorphic oracle: divergence recovery payload parity.
//!
//! Exercises the only split-brain case possible when fsync precedes
//! replication: the leader fsynces writes that never replicate, crashes,
//! the follower promotes and writes its own events, and the old leader
//! rejoins with a divergent WAL. S3 catchup must detect `TipHashMismatch`,
//! truncate back to the last common ancestor, and replay the replacement
//! chain from S3. After TCP replication resumes and more writes land the
//! two nodes must be byte-identical.
//!
//! Scope: this is the scenario `edge_wal_divergence_and_recovery` exercises
//! count-wise; this test is the payload-level upgrade. The bug class is the
//! single highest-stakes one in Celeriant — the truncation keeping or
//! losing the wrong events, the hash chain breaking across the cut, or the
//! replay producing different bytes than the new leader wrote.
//!
//! Oracle layering:
//! - `poll_event_count` + a strict `batches == TOTAL_EVENTS` check catches
//!   truncation failures in both directions (kept too much → counts match
//!   16 or 23; kept too little → counts stall below 20).
//! - `diff_aggregate(DiffMode::SameRun)` catches byte-level drift once
//!   counts line up.
//! - A distinct 4 KB payload marker on A's divergent Phase-2 events makes
//!   their post-heal absence provable: nothing on either node should ever
//!   carry a large payload, since B only ever writes tiny ones.
//! - `celeriant_s3_catchup_rounds_total` on A's fresh rejoin process must
//!   be non-zero — otherwise byte-parity could pass vacuously without the
//!   S3 catchup path actually running (e.g. if A never detected divergence
//!   and silently served stale divergent data).
//!
//! Metrics-port split: A and B take turns being the first-started node
//! (A leads phase 1–2, B leads 3–4, A rejoins in 4). The "single metrics
//! port, first node wins bind" trick used in scenarios A and B doesn't
//! work here — A would lose the bind to B in phase 3+. Each node gets its
//! own metrics port so A remains scrapeable after it rejoins.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    copy_shard_dirs,
    metamorphic_common::{diff_aggregate, format_key, response_digest, DiffMode},
    poll_event_count, read_all_batches, s3_cluster_config, scrape_counter,
    write_event, write_large_event, MinioContainer, RoutingRule, ServerConfig, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;
use tempfile::TempDir;

const AGGREGATE_COUNT: u128 = 4;
const PHASE_1_EVENTS: u64 = 10;  // events 1..10 — A solo, replicated to S3
const PHASE_2_EVENTS: u64 = 3;   // events 11..13 — A standalone, divergent
const PHASE_3_EVENTS: u64 = 5;   // events 11..15 — B's post-promotion writes
const PHASE_5_EVENTS: u64 = 5;   // events 16..20 — post-heal via TCP
const TOTAL_EVENTS: u64 = PHASE_1_EVENTS + PHASE_3_EVENTS + PHASE_5_EVENTS; // 20
// Large enough to dwarf any legitimate payload (B's `write_event` produces
// ~15 bytes). Small enough that a 4-shard x 3-event round is negligible.
const DIVERGENT_PAYLOAD_BYTES: usize = 4096;
const MAX_LEGIT_PAYLOAD_BYTES: usize = 200;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Metamorphic: Divergence Recovery Parity ===\n");

    let port_base = 18900 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 100;
    let minio_port = port_base + 10;
    // Distinct metrics ports: A and B alternate as "the running node",
    // so a shared port would mean A loses the bind once B starts. See
    // module doc-comment "Metrics-port split".
    let metrics_port_a = port_base + 20;
    let metrics_port_b = port_base + 30;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = AGGREGATE_COUNT as usize;

    // Cluster config for A — 10s leases so A's Phase-1 lease expires fast
    // enough for B to win CAS in Phase 4 without blowing the test budget.
    let mut cluster_config_a = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    cluster_config_a.heartbeat_lease_duration_ms = 10_000;
    cluster_config_a.s3_lease_duration_ms = 10_000;
    cluster_config_a.metrics_port = metrics_port_a;

    // Standalone config for A's Phase 2 — same data dir, no replication.
    let standalone_config_a = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        standalone: true,
        routing_rule: RoutingRule::AggregateTypeId,
        metrics_port: metrics_port_a,
        ..Default::default()
    };

    // Cluster config for B — same lease tuning, separate metrics port.
    let mut cluster_config_b = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    cluster_config_b.heartbeat_lease_duration_ms = 10_000;
    cluster_config_b.s3_lease_duration_ms = 10_000;
    cluster_config_b.metrics_port = metrics_port_b;

    let keys: Vec<AggregateKey> = (0..AGGREGATE_COUNT)
        .map(|shard_id| AggregateKey::new(1, shard_id, 1))
        .collect();

    // ── Phase 1: A as distributed leader, solo, writes 1..10 via S3 fallback ──
    println!(
        "\nPhase 1: A as distributed leader (no follower), write events 1..={} × {} aggregates...",
        PHASE_1_EVENTS, keys.len()
    );
    let mut node_a = TestServer::start_with_config_labeled(
        node_a_port, cluster_config_a.clone(), "node-a".into(),
    ).await?;

    // Empty S3 + no contender → A wins first CAS within a few seconds.
    // Don't use is_leader() probes here — the probe aggregate would get
    // copied into B's dir and pollute Phase 3.
    println!("  Waiting 5s for A to win initial S3 CAS...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    for event_num in 1..=PHASE_1_EVENTS {
        for key in &keys {
            write_event(&mut a_client, key, event_num, event_num == 1).await?;
        }
    }
    println!("  Phase 1 complete ({} writes).", PHASE_1_EVENTS * keys.len() as u64);

    println!("  Waiting 4s for S3 fallback batches to land...");
    tokio::time::sleep(Duration::from_secs(4)).await;

    drop(a_client);
    node_a.stop();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ── Copy A's shard dirs to a new temp dir for B ──
    println!("\nCopying A's shard data → B's dir (simulates B as synced follower at T={})",
        PHASE_1_EVENTS);
    let b_temp = TempDir::new()?;
    copy_shard_dirs(&node_a.config().data_root, b_temp.path())?;

    // ── Phase 2: A standalone, writes divergent events 11..13 (4 KB marker) ──
    println!(
        "\nPhase 2: A standalone, write divergent events {}..={} × {} aggregates ({} B marker)...",
        PHASE_1_EVENTS + 1, PHASE_1_EVENTS + PHASE_2_EVENTS, keys.len(), DIVERGENT_PAYLOAD_BYTES
    );
    node_a.restart_with_config(standalone_config_a).await?;
    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    for event_num in (PHASE_1_EVENTS + 1)..=(PHASE_1_EVENTS + PHASE_2_EVENTS) {
        for key in &keys {
            write_large_event(&mut a_client, key, event_num, DIVERGENT_PAYLOAD_BYTES).await?;
        }
    }
    drop(a_client);
    node_a.stop();
    println!("  Phase 2 complete. A's disk has divergent events {}..={} with {}-byte payloads.",
        PHASE_1_EVENTS + 1, PHASE_1_EVENTS + PHASE_2_EVENTS, DIVERGENT_PAYLOAD_BYTES);

    // ── Phase 3: B as distributed leader, writes events 11..15 (distinct bytes) ──
    println!(
        "\nPhase 3: B as distributed leader (with A's copied data), write events {}..={} × {} aggregates...",
        PHASE_1_EVENTS + 1, PHASE_1_EVENTS + PHASE_3_EVENTS, keys.len()
    );
    let node_b = TestServer::start_with_existing_dir(
        node_b_port, cluster_config_b, "node-b".into(), b_temp,
    ).await?;

    println!("  Waiting 15s for A's old lease to expire and B to win S3 CAS...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    let mut b_client = CeleriantClient::connect(node_b.address()).await?;
    for event_num in (PHASE_1_EVENTS + 1)..=(PHASE_1_EVENTS + PHASE_3_EVENTS) {
        for key in &keys {
            // Retry for transitions: B may still be fencing/catching-up right
            // after winning the CAS race.
            for retry in 0..10 {
                match write_event(&mut b_client, key, event_num, false).await {
                    Ok(_) => break,
                    Err(e) if retry < 9 => {
                        println!("  event {} aggregate {} retry {}: {}",
                            event_num, format_key(key), retry + 1, e);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }
    println!("  Phase 3 complete. B has events 1..={} per aggregate.",
        PHASE_1_EVENTS + PHASE_3_EVENTS);

    println!("  Waiting 4s for S3 fallback batches covering events 11..15 to land...");
    tokio::time::sleep(Duration::from_secs(4)).await;

    // ── Phase 4: A rejoins as distributed follower — truncate + S3 catchup ──
    println!("\nPhase 4: Restart A as distributed follower (divergent WAL at wal_index={})...",
        PHASE_1_EVENTS + PHASE_2_EVENTS);
    node_a.restart_with_config(cluster_config_a).await?;

    let target_post_catchup = PHASE_1_EVENTS + PHASE_3_EVENTS;
    println!("  Waiting for A to detect TipHashMismatch, truncate to {}, catch up to {} events...",
        PHASE_1_EVENTS, target_post_catchup);
    for key in &keys {
        let count = poll_event_count(
            node_a.address(),
            key,
            target_post_catchup as usize,
            Duration::from_secs(60),
        ).await;
        println!("    aggregate {}: {} events", format_key(key), count);
    }

    // ── Phase 5: B writes events 16..20; TCP replicates to A ──
    println!(
        "\nPhase 5: B writes events {}..={} — TCP replication to A...",
        target_post_catchup + 1, TOTAL_EVENTS
    );
    for event_num in (target_post_catchup + 1)..=TOTAL_EVENTS {
        for key in &keys {
            write_event(&mut b_client, key, event_num, false).await?;
        }
    }
    println!("  Phase 5 complete ({} writes).", PHASE_5_EVENTS * keys.len() as u64);

    println!("  Waiting for A to reach {} events...", TOTAL_EVENTS);
    for key in &keys {
        let count = poll_event_count(
            node_a.address(),
            key,
            TOTAL_EVENTS as usize,
            Duration::from_secs(30),
        ).await;
        println!("    aggregate {}: {} events", format_key(key), count);
    }

    // ── Phase 6: read both nodes and diff byte-for-byte ──
    let mut a_read_client = CeleriantClient::connect(node_a.address()).await?;
    let mut mismatches = Vec::<String>::new();
    for key in &keys {
        let a_batches = read_all_batches(&mut a_read_client, key).await?;
        let b_batches = read_all_batches(&mut b_client, key).await?;
        let ah = response_digest(&a_batches);
        let bh = response_digest(&b_batches);
        println!(
            "  aggregate {}: A_batches={}, B_batches={}, digest A={:016x} B={:016x}",
            format_key(key), a_batches.len(), b_batches.len(), ah, bh,
        );
        if (a_batches.len() as u64) != TOTAL_EVENTS {
            mismatches.push(format!(
                "aggregate {}: A returned {} batches, expected {} — truncation may have kept \
                 divergent Phase-2 events (would land on 13 or 23) or stalled before applying \
                 B's replacement chain (would land below 15)",
                format_key(key), a_batches.len(), TOTAL_EVENTS
            ));
        }
        if (b_batches.len() as u64) != TOTAL_EVENTS {
            mismatches.push(format!(
                "aggregate {}: B returned {} batches, expected {}",
                format_key(key), b_batches.len(), TOTAL_EVENTS
            ));
        }

        // A's Phase-2 divergent events carry a {DIVERGENT_PAYLOAD_BYTES}-byte
        // payload. Everything else is a tiny JSON blob (~15 B). Any event
        // above MAX_LEGIT_PAYLOAD_BYTES is a divergent event that leaked
        // past the truncation.
        for (bi, batch) in a_batches.iter().enumerate() {
            for (ei, ev) in batch.events.iter().enumerate() {
                if ev.event_value.len() > MAX_LEGIT_PAYLOAD_BYTES {
                    mismatches.push(format!(
                        "aggregate {} A batch[{}] event[{}]: {}-byte payload — divergent \
                         Phase-2 event survived the truncation",
                        format_key(key), bi, ei, ev.event_value.len()
                    ));
                }
            }
        }
        for (bi, batch) in b_batches.iter().enumerate() {
            for (ei, ev) in batch.events.iter().enumerate() {
                if ev.event_value.len() > MAX_LEGIT_PAYLOAD_BYTES {
                    mismatches.push(format!(
                        "aggregate {} B batch[{}] event[{}]: {}-byte payload on B — \
                         should be impossible, B never wrote large events",
                        format_key(key), bi, ei, ev.event_value.len()
                    ));
                }
            }
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
            "metamorphic divergence recovery parity failed on {} aggregate(s)",
            mismatches.len()
        ).into());
    }

    // Catchup oracle: A's rejoin process starts fresh, so the counter is
    // zero-based. At least one round must have run to detect the mismatch
    // and apply the replacement chain. If this is zero, byte-parity passed
    // without A ever running S3 catchup — the test is no longer validating
    // what its name claims.
    let catchup_rounds = scrape_counter(
        "127.0.0.1", metrics_port_a, "celeriant_s3_catchup_rounds_total",
    ).await?;
    println!("  node-a s3_catchup_rounds_total = {} (fresh rejoin process)", catchup_rounds);
    if catchup_rounds == 0 {
        return Err(
            "S3 catchup rounds counter is 0 on A's rejoin process — expected at least one round \
             to detect TipHashMismatch and apply B's replacement chain. Byte-parity passed but \
             the divergence recovery path was not exercised."
                .into(),
        );
    }

    println!(
        "\n=== PASS: A and B byte-identical after TipHashMismatch + truncation + S3 catchup; \
         divergent payloads absent ==="
    );
    Ok(())
}
