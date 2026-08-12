//! Restart-then-immediate-fan-in: a herd of producers all replaying at once.
//!
//! Many clients fan in on few aggregates (the BFF shape: producers >> streams).
//! The server is killed and restarted with data preserved, and then EVERY client
//! immediately retries its last write with its last client_seq — concurrently,
//! before the server has had time to warm anything. Every retry must be rejected
//! as a duplicate (2002 ClientIdempotencyViolation, or the in-flight variant):
//! accepting even one would be an exactly-once violation. A NEW client writing
//! to an existing aggregate right after the burst must still succeed.
//!
//! `idempotency_cold_reconstruction` pins the single-client cold reverse-scan;
//! this pins the burst: all cold lookups at once, per-client floors recovered
//! correctly for every client, verified by exact per-client batch counts.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use celeriant_wal::aggregate_key::AggregateKey;
use std::collections::HashMap;
use std::time::Duration;

use crate::common::{event, port_for, read_all};
use crate::{ServerConfig, TestServer};

const TYPE: u64 = 100;
const N_AGGS: u64 = 4;
/// Producers per aggregate — 32 clients total fanning in on 4 aggregates.
const CLIENTS_PER_AGG: u64 = 8;
/// Writes per client; the retry replays the last of these.
const SEQS: u64 = 3;

fn agg_key(a: u64) -> AggregateKey {
    AggregateKey::new(1, 1, (a + 1) as u128)
}

fn client_id(a: u64, c: u64) -> u128 {
    (1000 + a * 100 + c) as u128
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Idempotency: restart then immediate fan-in retry burst ===\n");

    // Single shard: all aggregates share one WAL, so every cold lookup contends
    // on the same recovery state.
    let config = ServerConfig { num_shards: Some(1), standalone: true, ..Default::default() };
    let mut server = TestServer::start_with_config(port_for("idempotency_restart_fanin"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    println!("Writing {} clients x {SEQS} seqs fanned in on {N_AGGS} aggregates", N_AGGS * CLIENTS_PER_AGG);
    // Round-robin over (seq, agg, client) so each client's blocks interleave with
    // every other client's — the worst chain shape for per-client floor recovery.
    for seq in 1..=SEQS {
        for a in 0..N_AGGS {
            for cl in 0..CLIENTS_PER_AGG {
                let opts = WriteEventsOptions {
                    allow_create: seq == 1 && cl == 0,
                    enforce_client_idempotency: true,
                    ..Default::default()
                };
                c.write_events_with(
                    agg_key(a),
                    vec![event(seq, TYPE, 1000 + seq, r#"{"fanin":true}"#)],
                    client_id(a, cl),
                    opts,
                )
                .await?;
            }
        }
    }

    let per_agg = (CLIENTS_PER_AGG * SEQS) as usize;
    for a in 0..N_AGGS {
        let n = read_all(&mut c, &agg_key(a)).await?.len();
        if n != per_agg {
            return Err(format!("setup: aggregate {a} has {n} batches, expected {per_agg}").into());
        }
    }
    drop(c);

    // Kill (not graceful) then restart on the same data dir: caches cold, WAL intact.
    println!("Killing then restarting server (data preserved, caches cold)");
    server.stop();
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.restart().await?;

    // The burst: every client reconnects and replays its LAST write concurrently,
    // immediately after the port opens. Every one must be deduped.
    println!("All {} clients concurrently retry their last seq", N_AGGS * CLIENTS_PER_AGG);
    let addr = server.address().to_string();
    let mut tasks = Vec::new();
    for a in 0..N_AGGS {
        for cl in 0..CLIENTS_PER_AGG {
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut c = CeleriantClient::connect(&addr).await
                    .map_err(|e| format!("client ({a},{cl}) reconnect: {e}"))?;
                let opts = WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() };
                let res = c
                    .write_events_with(
                        agg_key(a),
                        vec![event(SEQS, TYPE, 1000 + SEQS, r#"{"fanin":true}"#)],
                        client_id(a, cl),
                        opts,
                    )
                    .await;
                match res {
                    Err(ClientError::Server(ServerError::Write {
                        kind: WriteError::ClientIdempotencyViolation { last_client_seq, attempted_client_seq }, ..
                    }))
                    | Err(ClientError::Server(ServerError::Write {
                        kind: WriteError::InflightDuplicateWrite { last_client_seq, attempted_client_seq }, ..
                    })) => {
                        if last_client_seq != Some(SEQS) || attempted_client_seq != Some(SEQS) {
                            return Err(format!(
                                "client ({a},{cl}): duplicate rejected but wrong floor: last={last_client_seq:?} attempted={attempted_client_seq:?}, expected both Some({SEQS})"
                            ));
                        }
                        Ok(())
                    }
                    Ok(_) => Err(format!(
                        "client ({a},{cl}): retry of seq {SEQS} was ACCEPTED — exactly-once violation (write applied twice)"
                    )),
                    Err(other) => Err(format!("client ({a},{cl}): expected duplicate rejection, got {other:?}")),
                }
            }));
        }
    }
    let mut failures = Vec::new();
    for t in tasks {
        if let Err(msg) = t.await? {
            failures.push(msg);
        }
    }
    if !failures.is_empty() {
        for f in &failures {
            eprintln!("  FAIL: {f}");
        }
        return Err(format!("{} of {} retries mishandled after restart", failures.len(), N_AGGS * CLIENTS_PER_AGG).into());
    }

    // A brand-new client must not be caught in the dedup net.
    println!("New client writes to an existing aggregate (must be accepted)");
    let mut c = CeleriantClient::connect(server.address()).await?;
    let opts = WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() };
    c.write_events_with(agg_key(0), vec![event(1, TYPE, 2000, r#"{"new":true}"#)], 99_999, opts).await?;

    // Exact per-client accounting: nothing accepted twice, nothing lost.
    for a in 0..N_AGGS {
        let batches = read_all(&mut c, &agg_key(a)).await?;
        let expected = per_agg + if a == 0 { 1 } else { 0 };
        if batches.len() != expected {
            return Err(format!(
                "aggregate {a}: {} batches after burst, expected {expected} — a retry double-wrote or a write was lost",
                batches.len()
            ).into());
        }
        let mut per_client: HashMap<u128, usize> = HashMap::new();
        for b in &batches {
            *per_client.entry(b.client_id).or_default() += 1;
        }
        for cl in 0..CLIENTS_PER_AGG {
            let id = client_id(a, cl);
            let n = per_client.get(&id).copied().unwrap_or(0);
            if n != SEQS as usize {
                return Err(format!("aggregate {a}: client {id} has {n} batches, expected {SEQS}").into());
            }
        }
        if a == 0 && per_client.get(&99_999).copied().unwrap_or(0) != 1 {
            return Err("aggregate 0: new client's write missing after the burst".into());
        }
    }

    println!("\n=== PASS: every retry deduped, new client accepted, per-client counts exact ===");
    Ok(())
}
