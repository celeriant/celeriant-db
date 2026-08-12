//! Trim must not forget client idempotency state.
//!
//! Clients A and B write one aggregate (A owns versions 1-3, B owns 4-6), then
//! TrimStart drops everything below version 5 — including ALL of A's batches.
//! The contract pinned here:
//!
//!   - a NEW client C's first write still succeeds, and version numbering
//!     continues at 7 (trim never rewinds the version counter),
//!   - A retrying an OLD client_seq is still rejected as a duplicate, both warm
//!     and after a kill+restart (cold reconstruction must recover A's floor even
//!     though every batch A ever wrote is below the trim point),
//!   - a genuinely new seq from A is still accepted afterwards.
//!
//! If trim forgot A's state, the retry would be re-applied — an exactly-once
//! violation laundered through retention.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use celeriant_msg::request::requests::TrimStartRequest;
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

use crate::common::{event, port_for, read_all_from};
use crate::{ServerConfig, TestServer};

const TYPE: u64 = 100;
const CLIENT_A: u128 = 111;
const CLIENT_B: u128 = 222;
const CLIENT_C: u128 = 333;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Idempotency: trim must not forget client state ===\n");

    let config = ServerConfig { num_shards: Some(1), standalone: true, ..Default::default() };
    let mut server = TestServer::start_with_config(port_for("idempotency_trim_client_state"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    let key = AggregateKey::new(1, 1, 1);

    println!("A writes seqs 1..=3 (versions 1-3), B writes seqs 1..=3 (versions 4-6)");
    for (client, first) in [(CLIENT_A, true), (CLIENT_B, false)] {
        for seq in 1..=3u64 {
            let opts = WriteEventsOptions {
                allow_create: first && seq == 1,
                enforce_client_idempotency: true,
                ..Default::default()
            };
            c.write_events_with(key.clone(), vec![event(seq, TYPE, 1000 + seq, "{}")], client, opts).await?;
        }
    }

    println!("Trimming: keep from version 5 (drops ALL of A's batches and B's first)");
    c.trim_start(TrimStartRequest {
        correlation_id: None,
        aggregate_key: key.clone(),
        keep_from_aggregate_version: 5,
        client_id: 1,
        user_id: None,
    })
    .await?;

    // New client after trim: accepted, and versions continue — trim is not a rewind.
    println!("New client C's first write (must succeed and land at version 7)");
    let opts_idem = WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() };
    let resp = c.write_events_with(key.clone(), vec![event(1, TYPE, 2000, "{}")], CLIENT_C, opts_idem.clone()).await?;
    if resp.max_aggregate_version != Some(7) {
        return Err(format!(
            "C's post-trim write landed at {:?}, expected Some(7) — trim broke version continuity",
            resp.max_aggregate_version
        ).into());
    }

    // Warm retries of trimmed-away history: still duplicates.
    println!("A retries seq 3 and seq 2 warm (both must be rejected)");
    expect_duplicate(&mut c, &key, CLIENT_A, 3, Some(3), "warm retry of A's last seq").await?;
    expect_duplicate(&mut c, &key, CLIENT_A, 2, Some(3), "warm retry of an older A seq").await?;

    // Cold: the restart drops caches; A's floor must be reconstructable even
    // though every batch A wrote sits below the trim point.
    println!("Killing then restarting server (data preserved, caches cold)");
    server.stop();
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.restart().await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    println!("A and B retry old seqs cold (both must be rejected)");
    expect_duplicate(&mut c, &key, CLIENT_A, 3, Some(3), "cold retry of A's last seq after trim").await?;
    expect_duplicate(&mut c, &key, CLIENT_B, 3, Some(3), "cold retry of B's last seq after trim").await?;

    println!("A's genuinely new seq 4 and C's seq 2 (both must be accepted)");
    c.write_events_with(key.clone(), vec![event(4, TYPE, 3000, "{}")], CLIENT_A, opts_idem.clone()).await?;
    c.write_events_with(key.clone(), vec![event(2, TYPE, 3001, "{}")], CLIENT_C, opts_idem).await?;

    // Surviving stream: versions 5..=9, owned by B,B,C,A,C in write order.
    let batches = read_all_from(&mut c, &key, 5).await?;
    let got: Vec<(u64, u128)> = batches.iter().map(|b| (b.aggregate_version, b.client_id)).collect();
    let expected = vec![(5, CLIENT_B), (6, CLIENT_B), (7, CLIENT_C), (8, CLIENT_A), (9, CLIENT_C)];
    if got != expected {
        return Err(format!(
            "surviving stream mismatch: got {got:?}, expected {expected:?} — a duplicate landed or a write was lost"
        ).into());
    }

    println!("\n=== PASS: trim kept every client's idempotency floor, versions continuous ===");
    Ok(())
}

async fn expect_duplicate(
    c: &mut CeleriantClient,
    key: &AggregateKey,
    client: u128,
    seq: u64,
    expect_last: Option<u64>,
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
            if last_client_seq != expect_last || attempted_client_seq != Some(seq) {
                return Err(format!(
                    "{what}: rejected but wrong floor: last={last_client_seq:?} attempted={attempted_client_seq:?}, expected last={expect_last:?} attempted=Some({seq})"
                ).into());
            }
            Ok(())
        }
        Ok(_) => Err(format!("{what}: ACCEPTED — accepting this retry is an exactly-once violation").into()),
        Err(other) => Err(format!("{what}: expected duplicate rejection, got {other:?}").into()),
    }
}
