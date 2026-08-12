//! Delete + recreate vs client idempotency state (characterization).
//!
//! Client A writes an aggregate with idempotency ON, the aggregate is
//! soft-deleted with `allow_recreate: true` (both `allow_sequence_continuation`
//! modes), then A retries an OLD client_seq while a NEW client C also writes.
//!
//! Observed rule, pinned here so the storage refactor cannot silently change it:
//!
//!   - Delete NEVER clears per-client idempotency floors: A retrying seq 3 is
//!     rejected 2002 in BOTH modes, warm and after a kill+restart.
//!   - Continuation mode: the version counter continues (C's first write lands
//!     at v4 after a 3-event stream) and the pre-delete history becomes
//!     readable again once the stream is recreated (v1-3 resurface).
//!   - Fresh mode: the version counter resets for a new client (C lands at v1),
//!     but a RETURNING client's next write lands past its own old high-water
//!     mark (A's seq 4 lands at v4), leaving a version gap (v2, v3 absent).

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use celeriant_msg::request::requests::{DeleteRequest, SingleAggregateDelete};
use celeriant_wal::aggregate_key::AggregateKey;
use std::collections::HashMap;
use std::time::Duration;

use crate::common::{event, port_for, read_all};
use crate::{ServerConfig, TestServer};

const TYPE: u64 = 100;
const CLIENT_A: u128 = 444;
const CLIENT_C: u128 = 555;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Idempotency: delete/recreate keeps client seq floors (characterization) ===\n");

    let config = ServerConfig { num_shards: Some(1), standalone: true, ..Default::default() };
    let mut server = TestServer::start_with_config(port_for("idempotency_delete_recreate"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    let key_cont = AggregateKey::new(1, 1, 1);
    let key_fresh = AggregateKey::new(1, 1, 2);

    // (variant, key, continuation, expected C version, expected A-seq4 version,
    //  expected read-back (version, client, seq) after recreate)
    let variants: [(&str, &AggregateKey, bool, u64, u64, Vec<(u64, u128, u64)>); 2] = [
        (
            "continuation",
            &key_cont,
            true,
            4,
            5,
            // Pre-delete history resurfaces; versions continue unbroken.
            vec![(1, CLIENT_A, 1), (2, CLIENT_A, 2), (3, CLIENT_A, 3), (4, CLIENT_C, 1), (5, CLIENT_A, 4)],
        ),
        (
            "fresh",
            &key_fresh,
            false,
            1,
            4,
            // Counter reset for C, but A's write lands past its OLD high-water
            // mark: versions 2-3 are a gap. Pinned as observed.
            vec![(1, CLIENT_C, 1), (4, CLIENT_A, 4)],
        ),
    ];

    for (name, key, continuation, c_version, a4_version, expected_state) in &variants {
        let key: &AggregateKey = key;
        println!("--- variant: allow_sequence_continuation = {continuation} ---");
        for seq in 1..=3u64 {
            let opts = WriteEventsOptions { allow_create: seq == 1, enforce_client_idempotency: true, ..Default::default() };
            c.write_events_with(key.clone(), vec![event(seq, TYPE, 1000 + seq, "{}")], CLIENT_A, opts).await?;
        }
        let mut deletes = HashMap::new();
        deletes.insert(key.clone(), SingleAggregateDelete {
            allow_recreate: true,
            allow_sequence_continuation: *continuation,
            expected_version: None,
        });
        c.delete(DeleteRequest { correlation_id: None, client_id: 1, user_id: None, deletes }).await?;

        println!("A retries seq 3 after delete (must be rejected — floors survive delete)");
        expect_duplicate(&mut c, key, CLIENT_A, 3, Some(3), &format!("{name}: warm A retry after delete")).await?;

        println!("New client C's first write (must be accepted at v{c_version})");
        let opts = WriteEventsOptions { allow_create: true, enforce_client_idempotency: true, ..Default::default() };
        let resp = c.write_events_with(key.clone(), vec![event(1, TYPE, 2000, "{}")], CLIENT_C, opts.clone()).await?;
        if resp.max_aggregate_version != Some(*c_version) {
            return Err(format!(
                "{name}: C's recreate write landed at {:?}, expected Some({c_version})",
                resp.max_aggregate_version
            ).into());
        }

        println!("A's genuinely new seq 4 (must be accepted at v{a4_version})");
        let resp = c.write_events_with(key.clone(), vec![event(4, TYPE, 2001, "{}")], CLIENT_A, opts).await?;
        if resp.max_aggregate_version != Some(*a4_version) {
            return Err(format!(
                "{name}: A's post-recreate seq 4 landed at {:?}, expected Some({a4_version})",
                resp.max_aggregate_version
            ).into());
        }

        let got: Vec<(u64, u128, u64)> = read_all(&mut c, key).await?
            .iter()
            .map(|b| (b.aggregate_version, b.client_id, b.events[0].client_seq))
            .collect();
        if got != *expected_state {
            return Err(format!(
                "{name}: recreated stream mismatch: got {got:?}, expected {expected_state:?}"
            ).into());
        }
    }

    // Cold: floors must survive the delete/recreate boundary through a restart too.
    println!("Killing then restarting server (data preserved, caches cold)");
    server.stop();
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.restart().await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    println!("Cold retries of A's pre-delete seq 3 (must be rejected in both variants)");
    // A's floor is now 4 (its post-recreate write).
    expect_duplicate(&mut c, &key_cont, CLIENT_A, 3, Some(4), "continuation: cold A retry").await?;
    expect_duplicate(&mut c, &key_fresh, CLIENT_A, 3, Some(4), "fresh: cold A retry").await?;

    println!("\n=== PASS: delete/recreate preserved every client floor; versions pinned as observed ===");
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
    let opts = WriteEventsOptions { allow_create: true, enforce_client_idempotency: true, ..Default::default() };
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
        Ok(_) => Err(format!("{what}: ACCEPTED — the delete/recreate boundary dropped the client's floor (exactly-once violation)").into()),
        Err(other) => Err(format!("{what}: expected duplicate rejection, got {other:?}").into()),
    }
}
