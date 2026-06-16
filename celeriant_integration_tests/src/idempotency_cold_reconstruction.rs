//! Cold client-seq reverse-WAL reconstruction after a restart.
//!
//! Idempotency dedup needs a client's last `client_seq`. While the server is warm
//! that comes from cache, but after a restart the cache is cold and the shard must
//! reconstruct it by reverse-scanning the WAL (`cache_aggregate_client`), walking
//! THIS aggregate's per-client versions past interleaved foreign aggregates.
//!
//! No other test exercises that cold path — the warm idempotency tests never miss
//! the cache. This one writes one aggregate by many distinct clients interleaved
//! with foreign aggregates, restarts (cold caches, same WAL on disk), then replays
//! the OLDEST client's seq. A correct reconstruction must walk back past every
//! other client's block and the foreign blocks, find that client's seq=1, and
//! reject the replay — guarding the chain-follow scan used by the reconstruction.
//!
//! The oldest client is reliably cold by construction: warmup caches only the
//! newest writer per aggregate (it stops at the first block it sees per aggregate),
//! so every older client of a multi-client aggregate misses the cache regardless of
//! warmup tuning.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

use crate::common::{event, port_for, read_all};
use crate::{ServerConfig, TestServer};

const TYPE: u64 = 100;
/// Distinct clients writing the target aggregate. The oldest sits at the bottom of
/// the chain, so reconstructing it walks the whole thing.
const N_CLIENTS: u64 = 24;
/// Foreign aggregates between each target write, so the target's chain is spread
/// across the WAL and the reconstruction must skip them.
const FOREIGN_PER: u64 = 16;
const OLDEST_CLIENT: u128 = 1000;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Idempotency cold reverse-WAL reconstruction after restart ===\n");

    // Single shard so target + foreign interleave in one WAL.
    let config = ServerConfig { num_shards: Some(1), standalone: true, ..Default::default() };
    let mut server = TestServer::start_with_config(port_for("idempotency_cold_reconstruction"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    let target = AggregateKey::new(1, 1, 1);

    println!("Writing target by {N_CLIENTS} clients, interleaved with {FOREIGN_PER}x foreign aggregates");
    let mut foreign_id = 1_000_000u128;
    for i in 0..N_CLIENTS {
        let client_id = OLDEST_CLIENT + i as u128;
        // Each (client, seq=1) is a distinct dedup key, so all land.
        let opts = WriteEventsOptions { allow_create: i == 0, enforce_client_idempotency: true, ..Default::default() };
        c.write_events_with(target.clone(), vec![event(1, TYPE, 1000, r#"{"v":1}"#)], client_id, opts).await?;
        for _ in 0..FOREIGN_PER {
            let fk = AggregateKey::new(1, 1, foreign_id);
            foreign_id += 1;
            let opts = WriteEventsOptions { allow_create: true, enforce_client_idempotency: false, ..Default::default() };
            c.write_events_with(fk, vec![event(1, TYPE, 1000, "{}")], foreign_id, opts).await?;
        }
    }

    let before = total_events(&mut c, &target).await?;
    if before != N_CLIENTS as usize {
        return Err(format!("setup: expected {N_CLIENTS} target events, got {before}").into());
    }

    // Kill first: restart() alone only spawns a new process; the old (warm) one would
    // keep the port and serve from cache, hiding the cold path. Pause lets the port free.
    println!("Stopping then restarting server (cold caches, same WAL on disk)");
    server.stop();
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.restart().await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    // Replay the OLDEST client's seq=1. Cold cache → forces the reverse-WAL scan to
    // reconstruct that client's last seq, walking past every newer client + foreign block.
    println!("Replaying oldest client's seq=1 (must trigger cold reconstruction and dedupe)");
    let opts = WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() };
    let res = c
        .write_events_with(target.clone(), vec![event(1, TYPE, 1000, r#"{"v":1}"#)], OLDEST_CLIENT, opts.clone())
        .await;
    match res {
        Err(ClientError::Server(ServerError::Write { kind: WriteError::ClientIdempotencyViolation { .. }, .. })) => {}
        other => return Err(format!("expected ClientIdempotencyViolation from cold reconstruction, got {other:?}").into()),
    }

    // A genuinely new seq for the same client must still be accepted (reconstruction
    // recovered the right floor, not an over-broad block).
    println!("Writing oldest client's seq=2 (must be accepted)");
    c.write_events_with(target.clone(), vec![event(2, TYPE, 1001, r#"{"v":2}"#)], OLDEST_CLIENT, opts).await?;

    let after = total_events(&mut c, &target).await?;
    if after != N_CLIENTS as usize + 1 {
        return Err(format!("expected {} target events after replay+new write, got {after} (replay double-wrote?)", N_CLIENTS + 1).into());
    }

    println!("\n=== PASS: cold reconstruction deduped the replay and accepted the new seq ===");
    Ok(())
}

async fn total_events(c: &mut CeleriantClient, key: &AggregateKey) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(read_all(c, key).await?.iter().map(|b| b.events.len()).sum())
}
