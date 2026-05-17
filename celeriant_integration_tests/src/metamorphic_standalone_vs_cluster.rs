//! Metamorphic oracle: standalone vs distributed-cluster payload parity.
//!
//! Runs the same workload twice — once against a standalone server, once
//! against a 2-node S3 cluster — then compares the per-aggregate event lists.
//! Catches bugs where the shard's write path behaves differently with
//! replication enabled vs disabled (cache only populated on one path, off-by-one
//! aggregate version between modes, different compression decision, etc.); such bugs
//! would escape a leader/follower parity test because both distributed nodes
//! could agree and both drift relative to standalone.
//!
//! Cross-run fields that legitimately differ (`event_id`, `server_timestamp`,
//! `event_timestamp`) are excluded via `DiffMode::CrossRun`. The workload is
//! otherwise deterministic: `write_event` builds payloads from `event_num`
//! only, so the two runs emit byte-identical event bytes for every compared
//! field.
//!
//! Post Run B: asserts no S3 fallback objects were uploaded. Same rationale as
//! in `metamorphic_leader_follower_parity` — a healthy 2-node cluster under a
//! ~200-write workload must use TCP exclusively.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    metamorphic_common::{diff_aggregate, format_key, response_digest, DiffMode},
    read_all_batches, s3_cluster_config, wait_for_election_and_replication, write_event,
    MinioContainer, RoutingRule, ServerConfig, TestServer,
};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_wal::aggregate_key::AggregateKey;

const AGGREGATE_COUNT: u128 = 4;
const EVENTS_PER_AGGREGATE: u64 = 50;

fn workload_keys() -> Vec<AggregateKey> {
    (0..AGGREGATE_COUNT)
        .map(|shard_id| AggregateKey::new(1, shard_id, 1))
        .collect()
}

async fn run_workload(
    client: &mut CeleriantClient,
    keys: &[AggregateKey],
) -> Result<(), Box<dyn std::error::Error>> {
    for event_num in 1..=EVENTS_PER_AGGREGATE {
        for key in keys {
            write_event(client, key, event_num, event_num == 1).await?;
        }
    }
    Ok(())
}

async fn collect_reads(
    client: &mut CeleriantClient,
    keys: &[AggregateKey],
) -> Result<Vec<Vec<AggregateEventBatch>>, Box<dyn std::error::Error>> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        out.push(read_all_batches(client, key).await?);
    }
    Ok(out)
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Metamorphic: Standalone vs Cluster ===\n");

    let port_base = 18400 + (std::process::id() % 100) as u16;
    let standalone_port = port_base;
    let leader_port = port_base + 50;
    let follower_port = port_base + 150;
    let minio_port = port_base + 10;

    let keys = workload_keys();
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

    println!(
        "  Writing {} events × {} aggregates round-robin...",
        EVENTS_PER_AGGREGATE, keys.len()
    );
    run_workload(&mut standalone_client, &keys).await?;

    println!("  Reading all aggregates...");
    let run_a = collect_reads(&mut standalone_client, &keys).await?;

    drop(standalone_client);
    drop(standalone);
    // Let the standalone subprocess exit before bringing up MinIO + cluster
    // (port reuse isn't a concern here, but leaves clean logs).
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
    let follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {}", leader.address(), follower.address());

    println!("  Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    println!(
        "  Writing {} events × {} aggregates round-robin...",
        EVENTS_PER_AGGREGATE, keys.len()
    );
    run_workload(&mut leader_client, &keys).await?;

    // S3 fallback assertion is a bring-up check — evaluate before the payload
    // diff so a fallback-hit is reported as a cluster-configuration failure
    // rather than a payload mismatch.
    let fallback_objects = minio.list_objects("cluster/fallback/").await?;
    if !fallback_objects.is_empty() {
        eprintln!("  S3 fallback objects uploaded ({}):", fallback_objects.len());
        for obj in &fallback_objects {
            eprintln!("    {}", obj);
        }
        return Err(
            "S3 fallback fired during the cluster run — healthy 2-node cluster should use TCP \
             replication only. This is a bring-up / configuration regression, not a payload \
             divergence."
                .into(),
        );
    }
    println!("  S3 fallback check: 0 objects under cluster/fallback/ (TCP-only confirmed)");

    println!("  Reading all aggregates from leader...");
    let run_b = collect_reads(&mut leader_client, &keys).await?;

    // ── Diff ──────────────────────────────────────────────────────────────
    let mut mismatches = Vec::<String>::new();
    for (i, key) in keys.iter().enumerate() {
        let a = &run_a[i];
        let b = &run_b[i];
        let ah = response_digest(a);
        let bh = response_digest(b);
        println!(
            "  aggregate {}: standalone_batches={}, cluster_batches={}, response_digest standalone={:016x} cluster={:016x}",
            format_key(key), a.len(), b.len(), ah, bh,
        );
        if (a.len() as u64) != EVENTS_PER_AGGREGATE {
            mismatches.push(format!(
                "aggregate {}: standalone returned {} batches, expected {}",
                format_key(key), a.len(), EVENTS_PER_AGGREGATE
            ));
        }
        if (b.len() as u64) != EVENTS_PER_AGGREGATE {
            mismatches.push(format!(
                "aggregate {}: cluster returned {} batches, expected {}",
                format_key(key), b.len(), EVENTS_PER_AGGREGATE
            ));
        }
        if let Err(msg) = diff_aggregate(key, a, b, DiffMode::CrossRun) {
            mismatches.push(msg);
        }
    }

    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("  MISMATCH: {}", m);
        }
        return Err(format!(
            "metamorphic standalone-vs-cluster failed on {} aggregate(s)",
            mismatches.len()
        ).into());
    }

    println!("\n=== PASS: standalone and cluster runs are identical modulo cross-run artefacts ===");
    Ok(())
}
