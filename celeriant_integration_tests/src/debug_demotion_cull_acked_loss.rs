//! Reproducer: demotion cull with a stale leader-era ack barrier destroys
//! follower-replicated, client-ACKed entries.
//!
//! Found by `metamorphic_cull_parity` phase F (intermittent there): a node
//! that once led carries its old `last_self_acked_wal_seq` forever. If it
//! later serves as follower, receives TCP-replicated entries beyond that
//! barrier, then bounces Fenced→Follower (heartbeat gap > TTL, then heal),
//! the demotion cull (`reconcile_durable_tail(RewindToAckBarrier)`,
//! `BothToAckBarrier` arm) fires with `read == write > last_self_acked` and
//! rewinds both cursors to the stale barrier — destroying entries the
//! *current* leader already ACKed to clients. If the leader then dies before
//! re-replicating and S3 never held the range (healthy-cluster TCP-only
//! norm), the acked data exists nowhere.
//!
//! This test forces each precondition deterministically and asserts the
//! correct behavior: every client-ACKed event survives on the promoted
//! survivor.
//!
//! Empirical result (2026-06-04): the bounce-cull fires 2/2 and DOES rewind
//! A past B's acked entries — but the leader's reconciliation probe detects
//! the behind-follower ~11ms later and TCP extended-catchup re-supplies the
//! range within ~1s, so the final read heals and this test passes. Green
//! here therefore pins the probe as the safety net for the bounce-cull; if
//! the probe ever breaks, this goes red. The residual durability hole — the
//! leader dying inside the ~1s probe window with the range absent from S3 —
//! has no deterministic external orchestration (would need a server-side
//! probe-delay knob) and is tracked as a finding, not asserted here. The
//! failure output names exactly which acked events were destroyed and
//! whether S3 ever held them (the forensic distinction between "healed" and
//! "gone").
//!
//! Traces to `invariants.md` Durability ("Client ACK is withheld until …
//! replication succeeds") and the truncate-barrier rationale
//! (`last_self_acked` protects "data owed to whoever wrote it" — but only
//! the data *this node* acked; it says nothing about data the peer acked
//! that lives here as the only surviving copy).

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events,
    metamorphic_common::{format_key, wait_for_promotion},
    poll_event_count, read_all_batches, s3_cluster_config,
    wait_for_election_and_replication, write_event, MinioContainer, TcpProxy, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

const NUM_SHARDS: usize = 2;
const A_ERA_EVENTS: u64 = 10; // acked by A as leader: sets A's ack barrier
const B_ERA_EVENTS: u64 = 5; // acked by B as leader, TCP-replicated to A: the at-risk range
const FINAL_EVENTS: u64 = A_ERA_EVENTS + B_ERA_EVENTS;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Reproducer: demotion cull drops peer-acked entries (stale ack barrier) ===\n");

    let port_base = 16800 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 60;
    let proxy_a_port = port_base + 120; // fronts A's replication port: B→A heartbeats + replication
    let proxy_s3_a_port = port_base + 125; // A's private S3 path
    let minio_port = port_base + 15;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let mut config = s3_cluster_config(
        NUM_SHARDS, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 10_000;
    config.s3_lease_duration_ms = 10_000;

    let proxy_a = TcpProxy::start(proxy_a_port, format!("127.0.0.1:{}", node_a_port + 1)).await?;
    let proxy_s3_a = TcpProxy::start(proxy_s3_a_port, format!("127.0.0.1:{}", minio_port)).await?;

    let mut a_config = config.clone();
    a_config.client_port = node_a_port;
    a_config.advertised_replication_address = Some(format!("127.0.0.1:{}", proxy_a_port));
    a_config.s3_endpoint_override = Some(format!("http://127.0.0.1:{}", proxy_s3_a_port));
    println!("Starting node A (initial leader) on port {}...", node_a_port);
    let mut node_a =
        TestServer::start_with_config_labeled(node_a_port, a_config, "node-a".into()).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut b_config = config.clone();
    b_config.client_port = node_b_port;
    println!("Starting node B (follower) on port {}...", node_b_port);
    let mut node_b =
        TestServer::start_with_config_labeled(node_b_port, b_config, "node-b".into()).await?;

    println!("Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    let keys: Vec<AggregateKey> =
        (1..=NUM_SHARDS as u128).map(|id| AggregateKey::new(1, id, id)).collect();

    // ── A-era: acked writes set A's ack barrier (last_self_acked = 10) ────
    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    println!("\nA-era: writing events 1..={} × {} aggregates to leader A...", A_ERA_EVENTS, keys.len());
    for event_num in 1..=A_ERA_EVENTS {
        for key in &keys {
            write_event(&mut a_client, key, event_num, event_num == 1).await?;
        }
    }
    let mut b_client = CeleriantClient::connect(node_b.address()).await?;
    for key in &keys {
        let n = count_events(&mut b_client, key).await?;
        if n as u64 != A_ERA_EVENTS {
            return Err(format!("baseline not on B: {} has {}", format_key(key), n).into());
        }
    }

    // ── Hand leadership to B; A rejoins as follower with the stale barrier ─
    println!("\nSIGKILL A; B promotes...");
    drop(a_client);
    node_a.stop();
    let mut b_client = wait_for_promotion(node_b.address()).await?;
    println!("  B promoted. Restarting A as follower...");
    node_a.restart().await?;
    for key in &keys {
        poll_event_count(node_a.address(), key, A_ERA_EVENTS as usize, Duration::from_secs(60)).await;
    }
    println!("  A rejoined as follower at {} events.", A_ERA_EVENTS);

    // ── B-era: client-ACKed writes, TCP-replicated to A ───────────────────
    // After these, A holds read == write == 15 with last_self_acked == 10.
    // B goes idle so nothing re-sends the range later.
    println!("\nB-era: writing events {}..={} × {} aggregates to leader B...", A_ERA_EVENTS + 1, FINAL_EVENTS, keys.len());
    for event_num in (A_ERA_EVENTS + 1)..=FINAL_EVENTS {
        for key in &keys {
            write_event(&mut b_client, key, event_num, false).await?;
        }
    }
    for key in &keys {
        poll_event_count(node_a.address(), key, FINAL_EVENTS as usize, Duration::from_secs(30)).await;
    }
    println!("  B-era events ACKed and TCP-replicated to A.");

    // Forensic baseline: which S3 fallback objects exist before the bounce.
    let s3_objects_before = list_fallback_objects(&minio).await?;
    println!("  S3 fallback objects before bounce: {}", s3_objects_before.len());

    // ── Force the Fenced bounce ────────────────────────────────────────────
    // Severing proxy_a starves A of B's heartbeats; A's TTL decays to
    // effective-Fenced. A's S3 is also severed so A cannot observe the lease
    // or challenge. B stays leader via its direct S3 path.
    println!("\nBlocking B→A heartbeats and A's S3; waiting for A's TTL to decay (~10s)...");
    proxy_a.block();
    proxy_s3_a.block();
    tokio::time::sleep(Duration::from_secs(13)).await;

    let s3_objects_blocked = list_fallback_objects(&minio).await?;
    println!(
        "  S3 fallback objects during block: {} ({} new — retro-upload of the TCP-only range would close the loss window)",
        s3_objects_blocked.len(),
        s3_objects_blocked.len().saturating_sub(s3_objects_before.len())
    );

    // Heal heartbeats only. B's epoch-2 heartbeat reaches fenced A: the
    // heartbeat-demotion path culls to the stale ack barrier.
    println!("Unblocking heartbeats — B's heartbeat demotes fenced A (cull fires here)...");
    proxy_a.unblock();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── Kill B before it can re-replicate; A must promote with what it has ─
    println!("SIGKILL B; unblocking A's S3; A challenges once B's lease expires...");
    drop(b_client);
    node_b.stop();
    proxy_s3_a.unblock();

    let mut a_client = wait_for_promotion(node_a.address()).await?;
    println!("  A promoted.\n");

    // ── The durability assertion: every client-ACKed event must survive ───
    let mut lost = Vec::<String>::new();
    for key in &keys {
        let batches = read_all_batches(&mut a_client, key).await?;
        let present: std::collections::HashSet<u64> =
            batches.iter().flat_map(|b| b.events.iter().map(|e| e.client_seq)).collect();
        for event_num in 1..=FINAL_EVENTS {
            if !present.contains(&event_num) {
                lost.push(format!(
                    "aggregate {}: client-ACKed event {} missing from survivor A",
                    format_key(key), event_num
                ));
            }
        }
        println!("  aggregate {}: {} events on A (expected {})", format_key(key), batches.len(), FINAL_EVENTS);
    }

    if !lost.is_empty() {
        let s3_objects_after = list_fallback_objects(&minio).await?;
        eprintln!("\nACKED DATA LOST ({} events):", lost.len());
        for l in &lost {
            eprintln!("  {}", l);
        }
        eprintln!(
            "\nForensics: S3 fallback objects before/during/after: {}/{}/{}",
            s3_objects_before.len(), s3_objects_blocked.len(), s3_objects_after.len()
        );
        eprintln!("(If no new objects appeared during the block, the destroyed range never existed outside A and the dead B.)");
        return Err(format!(
            "demotion cull destroyed {} client-ACKed events (stale ack barrier)", lost.len()
        ).into());
    }

    println!("\n=== PASS: all client-ACKed events survived the fence bounce + leader death ===");
    Ok(())
}

async fn list_fallback_objects(minio: &MinioContainer) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut all = Vec::new();
    for shard_id in 0..NUM_SHARDS {
        let prefix = format!("cluster/fallback/shard_{:03}/", shard_id);
        all.extend(minio.list_objects(&prefix).await?);
    }
    Ok(all)
}
