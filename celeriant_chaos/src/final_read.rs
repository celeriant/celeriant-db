//! Client-API final-read phase.
//!
//! After heal + settle, read every bench-written aggregate from BOTH nodes —
//! each through a pool pinned to that node (seed == primary, so failover
//! can't silently redirect the read to the other node). Disk-truth catches
//! WAL-level divergence via wal-inspect; this catches read-path issues
//! (stale caches, visibility cursors) that disk bytes can't show.
//!
//! Reads are never rejected by node status (a follower serves stale reads
//! silently), so both slots answer regardless of who currently leads.
//! Records are appended to the scenario's history file and consumed by
//! `checkers::check_wal_monotonicity` / `check_final_read_parity`.

use crate::config::ClusterConfig;
use celeriant_bench::history::FinalReadRecord;
use celeriant_bench::{read_max_aggregate_version, Pool, PoolBuilder, TaskAckSummary};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

const READ_ATTEMPTS: u32 = 4;
const READ_BACKOFF_MS: u64 = 250;
const READ_CONCURRENCY: usize = 32;

/// Read `acks`' aggregates from both nodes. Node labels are the config-slot
/// hostnames (roles may have changed hands mid-scenario; the parity check
/// only needs two distinct, stable labels).
pub async fn run_final_read_phase(
    scen: &str,
    cfg: &ClusterConfig,
    acks: &[TaskAckSummary],
) -> Result<Vec<FinalReadRecord>, String> {
    let mut all = Vec::new();
    for (host, addr) in [
        (cfg.leader_host.clone(), cfg.leader_addr()),
        (cfg.follower_host.clone(), cfg.follower_addr()),
    ] {
        let pool = PoolBuilder {
            address1: &addr,
            address2: &addr, // pinned: no failover escape to the other node
            server_name: Some(&host),
            ca_cert: cfg.ca_cert.to_str().unwrap(),
            client_cert: cfg.client_cert.to_str().unwrap(),
            client_key: cfg.client_key.to_str().unwrap(),
            plaintext: false,
            max_connections: READ_CONCURRENCY,
        }
        .build()
        .await
        .map_err(|e| format!("{scen}: final-read pool for {host}: {e}"))?;

        let records = read_node(&host, &pool, acks).await;
        let errors = records.iter().filter(|r| r.error.is_some()).count();
        println!(
            "[{scen}] final-read {}: {} aggregates, {} unreadable",
            host,
            records.len(),
            errors
        );
        all.extend(records);
    }
    Ok(all)
}

async fn read_node(host: &str, pool: &Arc<Pool>, acks: &[TaskAckSummary]) -> Vec<FinalReadRecord> {
    let semaphore = Arc::new(Semaphore::new(READ_CONCURRENCY));
    let mut handles = Vec::new();
    for ack in acks {
        if ack.max_acked_client_seq == 0 {
            continue;
        }
        let pool = Arc::clone(pool);
        let ack = ack.clone();
        let host = host.to_string();
        let permit = Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            let _p = permit.acquire_owned().await.expect("semaphore closed");
            let mut last_err = String::new();
            for attempt in 1..=READ_ATTEMPTS {
                match read_max_aggregate_version(&pool, &ack.aggregate_key).await {
                    Ok(version) => {
                        return FinalReadRecord {
                            node: host,
                            org_id: ack.aggregate_key.org_id,
                            type_id: ack.aggregate_key.aggregate_type_id,
                            agg_id: ack.aggregate_key.aggregate_id,
                            client_id: ack.client_id,
                            max_aggregate_version: Some(version),
                            error: None,
                        };
                    }
                    Err(e) => {
                        last_err = format!("{e}");
                        if attempt < READ_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(READ_BACKOFF_MS * attempt as u64)).await;
                        }
                    }
                }
            }
            FinalReadRecord {
                node: host,
                org_id: ack.aggregate_key.org_id,
                type_id: ack.aggregate_key.aggregate_type_id,
                agg_id: ack.aggregate_key.aggregate_id,
                client_id: ack.client_id,
                max_aggregate_version: None,
                error: Some(last_err),
            }
        }));
    }

    let mut records = Vec::with_capacity(handles.len());
    for handle in handles {
        if let Ok(rec) = handle.await {
            records.push(rec);
        }
    }
    records
}
