//! Metamorphic oracle: standalone vs cluster parity for delete/trim decisions.
//!
//! Same deterministic delete/trim workload against a standalone server and a
//! 2-node S3 cluster; every accept/reject decision is recorded as a label and
//! the two decision logs must match exactly, then surviving aggregates are
//! payload-diffed. Catches the delete/trim durability-gap class of bug
//! (false-ack across rollback, stale-tombstone enqueue, regressed versions
//! feeding sequence-continuation recreates) behaving differently with
//! replication enabled vs disabled.
//!
//! The workload covers: delete accept via correct expected_version, delete of
//! an already-deleted aggregate, recreate-not-allowed write reject, delete OCC
//! reject, trim accept, trim out-of-range reject, and a no-OCC delete +
//! sequence-continuation recreate — the recreate's returned version is the
//! duplicate-version probe; both modes must continue at exactly 6.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{
    metamorphic_common::{diff_aggregate, format_key, DiffMode},
    s3_cluster_config, wait_for_election_and_replication, write_event,
    MinioContainer, RoutingRule, ServerConfig, TestServer,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{
    DeleteRequest, ReadRequest, SingleAggregateDelete, SingleAggregateWrite, TrimStartRequest, WriteRequest,
};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::collections::HashMap;
use std::sync::Arc;

// One aggregate per shard so the decisions exercise all four shard threads.
fn keys() -> [AggregateKey; 4] {
    [
        AggregateKey::new(1, 0, 10), // A: delete accept, delete-again reject, recreate-not-allowed reject
        AggregateKey::new(1, 1, 11), // B: delete OCC reject, survives intact
        AggregateKey::new(1, 2, 12), // C: trim accept + trim out-of-range reject
        AggregateKey::new(1, 3, 13), // D: no-OCC delete + sequence-continuation recreate
    ]
}

/// Stable accept/reject label. Server rejections are decisions and compare
/// across modes; transport failures are environmental and fail the run.
fn label(result: Result<ClientResponse, ClientError>) -> Result<String, Box<dyn std::error::Error>> {
    match result {
        Ok(ClientResponse::Write(w)) => Ok(format!("write_ok(max_v={:?})", w.max_aggregate_version)),
        Ok(ClientResponse::Delete(_)) => Ok("delete_ok".into()),
        Ok(ClientResponse::TrimStart(_)) => Ok("trim_ok".into()),
        Ok(other) => Ok(format!("unexpected_ok({other:?})")),
        Err(ClientError::Server(e)) => Ok(format!("rejected({e:?})")),
        Err(e) => Err(format!("transport error: {e}").into()),
    }
}

async fn write_one(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    event_num: u64,
    allow_create: bool,
) -> Result<ClientResponse, ClientError> {
    let event = DatablockAggregateEvent {
        client_seq: event_num,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"event\":{}}}", event_num).into_bytes()),
        iv: None,
    };
    let mut writes = HashMap::new();
    writes.insert(key.clone(), SingleAggregateWrite {
        events: vec![event],
        allow_create,
        expected_version: None,
        enforce_client_idempotency: false,
    });
    client.send_request(&ClientRequest::Write(WriteRequest {
        correlation_id: None,
        client_id: 999,
        user_id: Some(888),
        writes,
    })).await
}

async fn delete_one(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    expected_version: Option<u64>,
    allow_recreate: bool,
    allow_sequence_continuation: bool,
) -> Result<ClientResponse, ClientError> {
    let mut deletes = HashMap::new();
    deletes.insert(key.clone(), SingleAggregateDelete {
        allow_recreate,
        allow_sequence_continuation,
        expected_version,
    });
    client.send_request(&ClientRequest::Delete(DeleteRequest {
        correlation_id: None,
        client_id: 999,
        user_id: None,
        deletes,
    })).await
}

async fn trim_one(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    keep_from: u64,
) -> Result<ClientResponse, ClientError> {
    client.send_request(&ClientRequest::TrimStart(TrimStartRequest {
        correlation_id: None,
        aggregate_key: key.clone(),
        keep_from_aggregate_version: keep_from,
        client_id: 999,
        user_id: None,
    })).await
}

async fn read_from(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    from: u64,
) -> Result<Vec<AggregateEventBatch>, Box<dyn std::error::Error>> {
    let result = client.send_request(&ClientRequest::Read(ReadRequest {
        correlation_id: None,
        aggregate_key: key.clone(),
        filters: ReadFilters::new(from),
    })).await;
    match result {
        Ok(ClientResponse::Read(r)) => Ok(r.event_batches),
        other => Err(format!("expected read response for {}, got {other:?}", format_key(key)).into()),
    }
}

struct WorkloadResult {
    decisions: Vec<String>,
    b_batches: Vec<AggregateEventBatch>,
    c_batches: Vec<AggregateEventBatch>,
    d_batches: Vec<AggregateEventBatch>,
}

async fn run_workload(client: &mut CeleriantClient) -> Result<WorkloadResult, Box<dyn std::error::Error>> {
    let [a, b, c, d] = keys();

    for event_num in 1..=5 {
        for key in [&a, &b, &c, &d] {
            write_event(client, key, event_num, event_num == 1).await?;
        }
    }

    let mut decisions = Vec::new();
    let mut record = |name: &str, l: String| decisions.push(format!("{name}: {l}"));

    record("delete_a_v5", label(delete_one(client, &a, Some(5), false, false).await)?);
    record("delete_a_again", label(delete_one(client, &a, None, false, false).await)?);
    record("recreate_a_denied", label(write_one(client, &a, 6, true).await)?);
    record("delete_b_stale_occ", label(delete_one(client, &b, Some(3), false, false).await)?);
    record("trim_c_keep_from_3", label(trim_one(client, &c, 3).await)?);
    record("trim_c_out_of_range", label(trim_one(client, &c, 99).await)?);
    record("delete_d_no_occ", label(delete_one(client, &d, None, true, true).await)?);
    // Duplicate-version probe: continuation must resume at exactly v6 in both modes.
    record("recreate_d_continuation", label(write_one(client, &d, 6, true).await)?);

    Ok(WorkloadResult {
        decisions,
        b_batches: read_from(client, &b, 1).await?,
        c_batches: read_from(client, &c, 3).await?,
        d_batches: read_from(client, &d, 1).await?,
    })
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Metamorphic: Delete/Trim Decision Parity (standalone vs cluster) ===\n");

    let port_base = 18700 + (std::process::id() % 100) as u16;
    let standalone_port = port_base;
    let leader_port = port_base + 50;
    let follower_port = port_base + 150;
    let minio_port = port_base + 10;
    let num_shards = 4;

    // ── Run A: standalone ─────────────────────────────────────────────────
    println!("Run A: starting standalone server on port {}...", standalone_port);
    let standalone_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        routing_rule: RoutingRule::AggregateTypeId,
        standalone: true,
        ..Default::default()
    };
    let standalone = TestServer::start_with_config_labeled(
        standalone_port,
        standalone_config,
        "standalone".into(),
    ).await?;
    let mut standalone_client = CeleriantClient::connect(standalone.address()).await?;
    let run_a = run_workload(&mut standalone_client).await?;
    drop(standalone_client);
    drop(standalone);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // ── Run B: 2-node cluster with MinIO ──────────────────────────────────
    println!("\nRun B: starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 30_000;
    config.s3_lease_duration_ms = 30_000;

    println!("  Starting two-node cluster ({} shards, S3 election)...", num_shards);
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let _follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let run_b = run_workload(&mut leader_client).await?;

    // ── Compare decisions ─────────────────────────────────────────────────
    let mut mismatches = Vec::<String>::new();
    for (da, db) in run_a.decisions.iter().zip(run_b.decisions.iter()) {
        let marker = if da == db { "  " } else { "✗ " };
        println!("  {marker}standalone: {da}");
        if da != db {
            println!("  {marker}cluster:    {db}");
            mismatches.push(format!("decision divergence: standalone={da} cluster={db}"));
        }
    }

    // The continuation recreate must be v6 in BOTH modes — equality alone
    // would let a shared duplicate-version bug through.
    for (mode, r) in [("standalone", &run_a), ("cluster", &run_b)] {
        let got = r.decisions.last().expect("decision log is never empty");
        if got != "recreate_d_continuation: write_ok(max_v=Some(6))" {
            mismatches.push(format!("{mode}: sequence continuation must resume at v6, got {got:?}"));
        }
    }

    // ── Compare surviving payloads ────────────────────────────────────────
    let [_, b, c, d] = keys();
    for (key, a_batches, b_batches) in [
        (&b, &run_a.b_batches, &run_b.b_batches),
        (&c, &run_a.c_batches, &run_b.c_batches),
        (&d, &run_a.d_batches, &run_b.d_batches),
    ] {
        if let Err(msg) = diff_aggregate(key, a_batches, b_batches, DiffMode::CrossRun) {
            mismatches.push(msg);
        }
    }

    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("  MISMATCH: {}", m);
        }
        return Err(format!("delete/trim parity failed with {} mismatch(es)", mismatches.len()).into());
    }

    println!("\n=== PASS: delete/trim decisions and surviving payloads identical across modes ===");
    Ok(())
}
