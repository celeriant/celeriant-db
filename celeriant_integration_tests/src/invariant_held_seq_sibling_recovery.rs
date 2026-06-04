//! Invariant: a held client_seq consumed by a sibling must not lose the event.
//!
//! The BFF case: concurrent requests share one client_id, so two can derive the
//! same client_seq against the same expected_version. The winner lands. If the
//! loser's OCC rejection is lost to a timeout, its retry (held seq, refreshed
//! expected_version) gets ClientIdempotencyViolation (2002) — about the WINNER's
//! event. Treating that 2002 as success silently drops the loser's write.
//!
//! The protocol pinned here: stamp every event with an event_id, and on 2002
//! check who owns the held seq. Sibling's: re-derive and write again. Yours:
//! done. This test drives both verdicts against a real server and asserts no
//! event is lost and none is doubled.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use crate::common::{flatten, read_all};
use crate::{write_event, ServerConfig, TestServer};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::collections::HashMap;
use std::sync::Arc;

const CLIENT_ID: u128 = 5454; // shared by both "green threads", as on a BFF

const EID_A: u128 = 0xA; // winner's request id
const EID_B: u128 = 0xB; // loser's request id
const EID_C: u128 = 0xC; // timed-out-but-landed request id

async fn write(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    client_seq: u64,
    expected_version: u64,
    event_id: u128,
    payload: &str,
) -> Result<(), ClientError> {
    let event = DatablockAggregateEvent {
        client_seq,
        event_seq: 0,
        event_id: Some(event_id),
        event_timestamp: 1000 + client_seq,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(payload.as_bytes().to_vec()),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: false,
            expected_version: Some(expected_version),
            enforce_client_idempotency: true,
        },
    );

    client
        .send_request(&ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: CLIENT_ID,
            user_id: None,
            writes,
        }))
        .await?;
    Ok(())
}

fn expect_2002(result: Result<(), ClientError>, context: &str) -> Result<(), Box<dyn std::error::Error>> {
    match result {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::ClientIdempotencyViolation { .. },
            ..
        })) => Ok(()),
        other => Err(format!("{context}: expected ClientIdempotencyViolation, got {other:?}").into()),
    }
}

/// Who owns this client_seq in the durable log? The BFF answers this from its
/// seq-owner cache; the test answers it from the stream, which is what the
/// cache is warmed from.
async fn seq_owner(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    client_seq: u64,
) -> Result<Option<u128>, Box<dyn std::error::Error>> {
    let batches = read_all(client, key).await?;
    Ok(flatten(&batches)
        .iter()
        .find(|e| e.client_seq == client_seq)
        .and_then(|e| e.event_id))
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Invariant: Held client_seq consumed by a sibling ===\n");

    let port = 15810 + (std::process::id() % 100) as u16;
    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let _server = TestServer::start_with_config(port, config).await?;
    let addr = format!("127.0.0.1:{}", port);
    let key = AggregateKey::new(2, 2, 2);

    let mut client = CeleriantClient::connect(&addr).await?;

    // Setup: aggregate at version 1, max client_seq 1. Both siblings catch up
    // here and derive client_seq=2, expected_version=1.
    write_event(&mut client, &key, 1, true).await?;
    println!("Setup: aggregate_version=1, client_seq high-water=1\n");

    // ========================================
    // TEST 1: sibling takes the seq, loser's 2003 is lost, retry sees 2002
    // ========================================
    println!("TEST 1: A and B both derive seq=2/expected=1; A lands; B's rejection is lost");
    println!("----------------------------------------------------------------------------");

    write(&mut client, &key, 2, 1, EID_A, "a-deposit").await?;
    println!("  A: write(seq=2, expected=1) landed (version now 2)");

    // B's first attempt would have returned 2003; the response was lost. B
    // follows the timeout recipe: hold seq=2, refresh expected_version to 2.
    let result = write(&mut client, &key, 2, 2, EID_B, "b-deposit").await;
    expect_2002(result, "B retry with held seq")?;
    println!("  B: retry(seq=2, expected=2) -> 2002, as the protocol predicts");

    // The pin: that 2002 is about A's event, not B's. Blind success here
    // loses b-deposit.
    let owner = seq_owner(&mut client, &key, 2).await?;
    assert_eq!(owner, Some(EID_A), "seq 2 must be owned by A's event_id");
    println!("  B: seq 2 is owned by EID_A — not mine. Re-derive instead of claiming success");

    // B re-derives and writes its own event.
    write(&mut client, &key, 3, 2, EID_B, "b-deposit").await?;
    println!("  B: write(seq=3, expected=2) landed — b-deposit recovered\n");

    // ========================================
    // TEST 2: the other verdict — the held seq is yours, do not write again
    // ========================================
    println!("TEST 2: C's write lands but the ack is lost; retry must verify 'mine' and stop");
    println!("------------------------------------------------------------------------------");

    write(&mut client, &key, 4, 3, EID_C, "c-deposit").await?;
    println!("  C: write(seq=4, expected=3) landed; ack lost");

    let result = write(&mut client, &key, 4, 4, EID_C, "c-deposit").await;
    expect_2002(result, "C retry with held seq")?;

    let owner = seq_owner(&mut client, &key, 4).await?;
    assert_eq!(owner, Some(EID_C), "seq 4 must be owned by C's event_id");
    println!("  C: retry -> 2002, seq 4 owned by EID_C — mine. Success, no re-derive\n");

    // ========================================
    // Final: nothing lost, nothing doubled
    // ========================================
    let batches = read_all(&mut client, &key).await?;
    let events = flatten(&batches);
    let payloads: Vec<String> = events
        .iter()
        .map(|e| String::from_utf8_lossy(&e.event_value).into_owned())
        .collect();

    assert_eq!(events.len(), 4, "setup + a + b + c, exactly once each: {payloads:?}");
    assert_eq!(payloads.iter().filter(|p| *p == "a-deposit").count(), 1);
    assert_eq!(payloads.iter().filter(|p| *p == "b-deposit").count(), 1);
    assert_eq!(payloads.iter().filter(|p| *p == "c-deposit").count(), 1);
    println!("Final log: {payloads:?}");

    println!("\n=== All Tests Passed ===");
    println!("Held-seq verification pinned:");
    println!("  1. Sibling owns the seq -> 2002 is not yours; re-derive, event recovered");
    println!("  2. You own the seq -> 2002 is success; no double-write\n");

    Ok(())
}
