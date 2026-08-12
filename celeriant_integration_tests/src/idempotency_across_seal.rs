//! Idempotency floors must survive segment rotation (seal), warm and cold.
//!
//! Early clients write seq=1 to two aggregates, then bulk writers push enough
//! large payloads through the shard that the log rotates several times
//! (preallocate is 2 MiB; the bulk volume is ~8 MiB), sealing the segments
//! holding the early clients' blocks. The contract pinned here:
//!
//!   - retrying an early client's old seq is rejected even though its block now
//!     lives in a SEALED segment (the dedup lookup must cross the seal),
//!   - a new client writing to the same deep aggregate is still accepted,
//!   - after a kill+restart the same holds cold: sealed-segment lookups still
//!     find the floor, and post-restart retries of pre-seal seqs are rejected.
//!
//! `idempotency_negative_scan_present_client` measures the cost of this shape;
//! nothing before this pinned its correctness.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

use crate::common::{event, port_for, read_all};
use crate::{ServerConfig, TestServer};

const TYPE: u64 = 100;
const EARLY_1: u128 = 71;
const EARLY_2: u128 = 72;
const NEW_CLIENT: u128 = 81;
/// Bulk writes per aggregate. 120 x 64 KiB of random hex x 2 aggregates: hex
/// compresses ~2x under the server's zstd dictionary, leaving ~7.5 MiB on disk
/// against a 2 MiB preallocate — several rotations, so the early blocks are
/// sealed away. (A constant payload compresses to nothing and never rotates.)
const BULK_PER_AGG: u64 = 120;
const BULK_PAYLOAD_BYTES: usize = 64 * 1024;

/// Incompressible-enough payload: random hex, ~4 bits of entropy per byte.
fn random_payload() -> String {
    let mut s = String::with_capacity(BULK_PAYLOAD_BYTES);
    while s.len() < BULK_PAYLOAD_BYTES {
        s.push_str(&format!("{:016x}", rand::random::<u64>()));
    }
    s
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Idempotency: floors survive segment seal (rotation), warm and cold ===\n");

    // Small segments so the write volume below rotates the log several times.
    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        shard_log_preallocate_bytes: 2 * 1024 * 1024,
        ..Default::default()
    };
    let mut server = TestServer::start_with_config(port_for("idempotency_across_seal"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    let key1 = AggregateKey::new(1, 1, 1);
    let key2 = AggregateKey::new(1, 1, 2);

    println!("Early clients write seq=1 to each aggregate");
    let opts_first = WriteEventsOptions { allow_create: true, enforce_client_idempotency: true, ..Default::default() };
    c.write_events_with(key1.clone(), vec![event(1, TYPE, 1000, r#"{"early":1}"#)], EARLY_1, opts_first.clone()).await?;
    c.write_events_with(key2.clone(), vec![event(1, TYPE, 1000, r#"{"early":2}"#)], EARLY_2, opts_first).await?;

    println!("Bulk: {BULK_PER_AGG} x {BULK_PAYLOAD_BYTES}B random writes per aggregate (rotates the 2MiB segment repeatedly)");
    let mut bulk_client = 10_000u128;
    for b in 0..BULK_PER_AGG {
        for key in [&key1, &key2] {
            // Distinct client per write, idempotency OFF: setup must not depend on
            // the very lookup path under test.
            let opts = WriteEventsOptions { allow_create: false, enforce_client_idempotency: false, ..Default::default() };
            c.write_events_with(key.clone(), vec![event(1, TYPE, 2000 + b, &random_payload())], bulk_client, opts).await?;
            bulk_client += 1;
        }
    }

    println!("Retrying early seqs across the seal (must be rejected)");
    expect_duplicate(&mut c, &key1, EARLY_1, 1, "warm retry across seal, aggregate 1").await?;
    expect_duplicate(&mut c, &key2, EARLY_2, 1, "warm retry across seal, aggregate 2").await?;

    println!("New client to the deep aggregate (must be accepted)");
    let opts_idem = WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() };
    c.write_events_with(key1.clone(), vec![event(1, TYPE, 3000, r#"{"new":1}"#)], NEW_CLIENT, opts_idem.clone()).await?;

    println!("Killing then restarting server (data preserved, caches cold)");
    server.stop();
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.restart().await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    println!("Cold retries of pre-seal seqs (must still be rejected)");
    expect_duplicate(&mut c, &key1, EARLY_1, 1, "cold retry across seal, aggregate 1").await?;
    expect_duplicate(&mut c, &key2, EARLY_2, 1, "cold retry across seal, aggregate 2").await?;
    expect_duplicate(&mut c, &key1, NEW_CLIENT, 1, "cold retry of pre-restart write").await?;

    println!("Genuinely new seqs still accepted cold");
    c.write_events_with(key1.clone(), vec![event(2, TYPE, 4000, r#"{"new":2}"#)], NEW_CLIENT, opts_idem.clone()).await?;
    c.write_events_with(key2.clone(), vec![event(2, TYPE, 4000, r#"{"early":2}"#)], EARLY_2, opts_idem).await?;

    // Exact accounting: early + bulk + new-client writes, nothing doubled.
    let n1 = read_all(&mut c, &key1).await?.len();
    let n2 = read_all(&mut c, &key2).await?.len();
    let want1 = 1 + BULK_PER_AGG as usize + 2; // early + bulk + new client's seq 1 and 2
    let want2 = 1 + BULK_PER_AGG as usize + 1; // early (seq 1 and 2) + bulk
    if n1 != want1 || n2 != want2 {
        return Err(format!(
            "batch counts after seal+restart: agg1={n1} (want {want1}), agg2={n2} (want {want2}) — a retry double-wrote or a write was lost"
        ).into());
    }

    println!("\n=== PASS: sealed-segment floors held warm and cold, counts exact ===");
    Ok(())
}

async fn expect_duplicate(
    c: &mut CeleriantClient,
    key: &AggregateKey,
    client: u128,
    seq: u64,
    what: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let opts = WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() };
    let res = c.write_events_with(key.clone(), vec![event(seq, TYPE, 9000, "{}")], client, opts).await;
    match res {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::ClientIdempotencyViolation { last_client_seq, attempted_client_seq }, ..
        }))
        | Err(ClientError::Server(ServerError::Write {
            kind: WriteError::InflightDuplicateWrite { last_client_seq, attempted_client_seq }, ..
        })) => {
            if attempted_client_seq != Some(seq) || last_client_seq < Some(seq) {
                return Err(format!(
                    "{what}: rejected but wrong floor: last={last_client_seq:?} attempted={attempted_client_seq:?}"
                ).into());
            }
            Ok(())
        }
        Ok(_) => Err(format!("{what}: ACCEPTED — accepting this retry is an exactly-once violation").into()),
        Err(other) => Err(format!("{what}: expected duplicate rejection, got {other:?}").into()),
    }
}
