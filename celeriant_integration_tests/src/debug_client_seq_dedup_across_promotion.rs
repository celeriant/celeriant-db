//! client_seq deduplication state is not carried across a leader handover.
//!
//! Field shape, confirmed on disk by three chaos reproductions of
//! `idempotency_audit_partition_then_kill_minio`: a client write whose ack is
//! lost is retried with the same (client_id, client_seq). The old leader
//! correctly rejects the retry (`InflightDuplicateWrite`). When the other node
//! promotes it accepts the same retry as a NEW write — even though the original
//! is already durably committed in its own WAL — and assigns a fresh
//! aggregate_version. Fingerprint: final version == acks + 1, byte-identical on
//! both nodes, the duplicate landing at the next lease epoch.
//!
//! Deterministic reproduction here, no chaos rig:
//!   1. Client C writes idempotent seqs 1..=3 to the leader; both nodes converge.
//!   2. The follower RESTARTS — boot warmup seeds its dedup cache from its local
//!      WAL, so it now holds a live entry for (C, seq 3). This is the field's
//!      restarted-node precondition, and the reason the write path's cache-miss
//!      disk scan (which reads the truth) never runs on the retry.
//!   3. Client C writes idempotent seqs 4..=5; the follower applies them via
//!      replication, which does NOT refresh that dedup entry — it is now stale
//!      at 3 while the WAL holds 5.
//!   4. The leader is killed; the follower promotes.
//!   5. Client C retries seq 5 (the write whose ack it never saw).
//!
//! CONTRACT: the retry must be rejected as a duplicate and the aggregate must
//! stay at version 5. EXPECTED RED today: accepted, version 6.
//!
//! Opt-in only (`debug` category) — see registry.rs.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{AggregateDetailsRequest, SingleAggregateWrite, WriteRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    count_events, metamorphic_common::wait_for_promotion, poll_event_count, s3_cluster_config,
    wait_for_leader, MinioContainer, TestServer,
};

const NUM_SHARDS: usize = 1;
const CLIENT_ID: u128 = 424_242;
/// Seqs written before the follower restarts: what its boot warmup caches.
const PRE_RESTART_SEQ: u64 = 3;
/// Total acked seqs. The last one is the write whose ack the client loses.
const ACKED_SEQ: u64 = 5;

fn agg() -> AggregateKey {
    AggregateKey::new(1, 0, 1)
}

/// One idempotent event carrying exactly `client_seq`. No `expected_version`:
/// an OCC check would reject the replay first and mask the idempotency gate
/// (see `invariant_occ_before_idempotency`).
fn idem_write_request(client_seq: u64) -> ClientRequest {
    let event = DatablockAggregateEvent {
        client_seq,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + client_seq,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"seq\":{client_seq}}}").into_bytes()),
        iv: None,
    };
    let mut writes = HashMap::new();
    writes.insert(
        agg(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: true,
        },
    );
    ClientRequest::Write(WriteRequest {
        correlation_id: Some(client_seq as u128),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    })
}

async fn write_acked(client: &mut CeleriantClient, client_seq: u64) -> Result<(), Box<dyn std::error::Error>> {
    match client.send_request(&idem_write_request(client_seq)).await? {
        ClientResponse::Write(w) => {
            println!("  seq {client_seq} acked at version {:?}", w.max_aggregate_version);
            Ok(())
        }
        other => Err(format!("seq {client_seq} must be acked by the leader, got {other:?}").into()),
    }
}

/// The replay's outcome, as the client sees it. A rejection arrives as a typed
/// server error, not a response variant.
enum Replay {
    Rejected(String),
    Committed(Option<u64>),
    Other(String),
}

async fn replay_last_seq(client: &mut CeleriantClient) -> Replay {
    match client.send_request(&idem_write_request(ACKED_SEQ)).await {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::ClientIdempotencyViolation { .. } | WriteError::InflightDuplicateWrite { .. },
            error_message,
        })) => Replay::Rejected(error_message),
        Ok(ClientResponse::Write(w)) => Replay::Committed(w.max_aggregate_version),
        Ok(other) => Replay::Other(format!("{other:?}")),
        Err(e) => Replay::Other(format!("{e:?}")),
    }
}

async fn aggregate_version(client: &mut CeleriantClient) -> Result<u64, Box<dyn std::error::Error>> {
    let req = ClientRequest::AggregateDetails(AggregateDetailsRequest {
        correlation_id: Some(0xD1),
        aggregate_key: agg(),
    });
    match client.send_request(&req).await? {
        ClientResponse::AggregateDetails(d) => Ok(d.max_aggregate_version),
        other => Err(format!("aggregate details must be readable, got {other:?}").into()),
    }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== client_seq dedup across leader handover ===\n");

    let port_base = 19900 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 150;
    let minio_port = port_base + 10;

    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    let config = s3_cluster_config(NUM_SHARDS, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    let mut leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let mut follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("Leader at {}, follower at {}", leader.address(), follower.address());

    wait_for_leader(leader.address(), Duration::from_secs(45)).await?;
    println!("Election settled\n");

    // ── Phase 1: acked idempotent writes, then converge ──
    println!("PHASE 1: client {CLIENT_ID} writes idempotent seqs 1..={PRE_RESTART_SEQ}");
    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    for seq in 1..=PRE_RESTART_SEQ {
        write_acked(&mut leader_client, seq).await?;
    }
    poll_event_count(follower.address(), &agg(), PRE_RESTART_SEQ as usize, Duration::from_secs(45)).await;
    println!("  follower converged at {PRE_RESTART_SEQ}\n");

    // ── Phase 2: the follower restarts — boot warmup caches (C, seq 3) ──
    println!("PHASE 2: restart the follower (boot warmup seeds its dedup state)");
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;
    follower.restart().await?;
    poll_event_count(follower.address(), &agg(), PRE_RESTART_SEQ as usize, Duration::from_secs(45)).await;
    println!("  follower back with {PRE_RESTART_SEQ} events\n");

    // ── Phase 3: more acked writes land on the restarted follower ──
    println!("PHASE 3: client writes idempotent seqs {}..={ACKED_SEQ}", PRE_RESTART_SEQ + 1);
    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    for seq in PRE_RESTART_SEQ + 1..=ACKED_SEQ {
        write_acked(&mut leader_client, seq).await?;
    }
    poll_event_count(follower.address(), &agg(), ACKED_SEQ as usize, Duration::from_secs(45)).await;
    println!("  follower converged at {ACKED_SEQ} — its WAL holds seq {ACKED_SEQ}, its dedup cache still says {PRE_RESTART_SEQ}\n");

    // The old leader itself rejects the replay: the behavior the survivor owes.
    match replay_last_seq(&mut leader_client).await {
        Replay::Rejected(msg) => println!("  old leader rejects the replay of seq {ACKED_SEQ}: {msg}\n"),
        Replay::Committed(v) => {
            return Err(format!("scaffolding: the authoring leader accepted the replay at version {v:?}").into())
        }
        Replay::Other(what) => {
            return Err(format!("scaffolding: the authoring leader must reject the replay, got {what}").into())
        }
    }

    // ── Phase 4: kill the leader, let the follower promote ──
    println!("PHASE 4: kill the leader, await promotion");
    drop(leader_client);
    leader.stop();
    let mut new_leader = wait_for_promotion(follower.address()).await?;
    println!("  {} promoted\n", follower.address());

    let version_before = aggregate_version(&mut new_leader).await?;
    assert_eq!(
        version_before, ACKED_SEQ,
        "scaffolding: the survivor must hold every acked write before the replay"
    );

    // ── Phase 5: the client retries the write whose ack it never saw ──
    println!("PHASE 5: replay (client {CLIENT_ID}, seq {ACKED_SEQ}) against the new leader");
    match replay_last_seq(&mut new_leader).await {
        Replay::Rejected(msg) => println!("  rejected as duplicate: {msg}"),
        Replay::Committed(version) => {
            let version_after = aggregate_version(&mut new_leader).await?;
            let events = count_events(&mut new_leader, &agg()).await?;
            panic!(
                "the promoted node COMMITTED the replay of (client {CLIENT_ID}, seq {ACKED_SEQ}) \
                 at version {version:?} — {ACKED_SEQ} acks, version now {version_after}, {events} event batches. \
                 The same (client_id, client_seq) is committed twice."
            );
        }
        Replay::Other(what) => return Err(format!("replay must be rejected as a duplicate, got {what}").into()),
    }

    let version_after = aggregate_version(&mut new_leader).await?;
    let events = count_events(&mut new_leader, &agg()).await?;
    assert_eq!(
        version_after, ACKED_SEQ,
        "a rejected replay must leave the aggregate at version {ACKED_SEQ}, not {version_after}"
    );
    assert_eq!(events, ACKED_SEQ as usize, "no duplicate event batch may exist");

    println!("\n=== dedup survived the handover ===");
    Ok(())
}
