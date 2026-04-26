//! Metamorphic oracle: leader vs follower payload-level parity.
//!
//! Writes N events across M aggregates to the leader, then immediately reads
//! every aggregate's full event list from BOTH leader and follower via the
//! client API. Fails if any aggregate's batches differ in any field exposed by
//! the read response (batch index, client/user id, server timestamp, or any
//! per-event field including the raw payload bytes).
//!
//! No quiesce wait is required: `invariants.md` guarantees that by the time a
//! client sees `ok` for a write, fsync AND replication have both completed and
//! both read cursors have advanced. If an immediate read flakes, that is a bug.
//!
//! Post-run: asserts no S3 fallback objects were uploaded. In a healthy cluster
//! all replication flows over TCP (`invariants.md`, "Replication Protocol").
//! Any object under `cluster/fallback/` means fallback fired — the test would
//! still pass after a follower catches up via S3, masking a bring-up regression.
//! Failing on fallback surfaces that directly.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    metamorphic_common::{diff_aggregate, format_key, response_digest, DiffMode},
    read_all_batches, s3_cluster_config, wait_for_election_and_replication, write_event,
    MinioContainer, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;

const AGGREGATE_COUNT: u128 = 4;
const EVENTS_PER_AGGREGATE: u64 = 50;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Metamorphic: Leader/Follower Parity ===\n");

    let port_base = 18300 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = 4;
    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 30_000;
    config.s3_lease_duration_ms = 30_000;

    println!("Starting two-node cluster ({} shards, S3 election)...", num_shards);
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    // One aggregate per shard via aggregate_type_id routing.
    let keys: Vec<AggregateKey> = (0..AGGREGATE_COUNT)
        .map(|shard_id| AggregateKey::new(1, shard_id, 1))
        .collect();

    // Interleave writes round-robin across aggregates to exercise ordering.
    println!(
        "\nWriting {} events to each of {} aggregates ({} total) round-robin...",
        EVENTS_PER_AGGREGATE, keys.len(), EVENTS_PER_AGGREGATE * keys.len() as u64
    );
    for event_num in 1..=EVENTS_PER_AGGREGATE {
        for key in &keys {
            write_event(&mut leader_client, key, event_num, event_num == 1).await?;
        }
    }
    println!("  Writes complete. Reading immediately (no quiesce wait).");

    // Read each aggregate from both nodes and diff.
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
        if leader_batches.len() as u64 != EVENTS_PER_AGGREGATE {
            mismatches.push(format!(
                "aggregate {}: leader returned {} batches, expected {}",
                format_key(key), leader_batches.len(), EVENTS_PER_AGGREGATE
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
            "metamorphic parity failed on {} aggregate(s)",
            mismatches.len()
        ).into());
    }

    // Healthy 2-node cluster must use TCP replication only; any S3 fallback
    // object means bring-up regressed. Per invariants.md "Replication Protocol":
    // "S3 is never touched" in a healthy cluster.
    let fallback_objects = minio.list_objects("cluster/fallback/").await?;
    if !fallback_objects.is_empty() {
        eprintln!("  S3 fallback objects uploaded ({}):", fallback_objects.len());
        for obj in &fallback_objects {
            eprintln!("    {}", obj);
        }
        return Err(
            "S3 fallback fired during the test — healthy cluster should use TCP replication only. \
             Parity held only because the follower caught up via S3, masking a bring-up regression."
                .into(),
        );
    }
    println!("  S3 fallback check: 0 objects under cluster/fallback/ (TCP-only confirmed)");

    println!("\n=== PASS: leader and follower are byte-identical on every aggregate ===");
    Ok(())
}
