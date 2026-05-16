//! Invariant 16: OCC Validation Before Client Idempotency
//!
//! When both OCC and idempotency checks would fail, OCC fires first.
//! A concurrent writer with a stale read should get OptimisticConcurrencyViolation,
//! not ClientIdempotencyViolation.
//!
//! Invariants tested: 16 (OCC before idempotency ordering)

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use crate::{count_events, write_event, ServerConfig, TestServer};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::collections::HashMap;
use std::sync::Arc;


const CLIENT_ID: u128 = 7777;

async fn write_occ(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    client_event_index: u64,
    expected_event_batch_index: Option<u64>,
    enforce_idempotency: bool,
) -> Result<(), ClientError> {
    let event = DatablockAggregateEvent {
        client_event_index,
        event_index: 0,
        event_id: None,
        event_timestamp: 1000 + client_event_index,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("occ_{}", client_event_index).into_bytes()),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: false,
            expected_event_batch_index,
            enforce_client_idempotency: enforce_idempotency,
        },
    );

    client
        .send_request(
            &ClientRequest::Write(WriteRequest {
                correlation_id: None,
                client_id: CLIENT_ID,
                user_id: None,
                writes,
            })
        )
        .await?;
    Ok(())
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Invariant: OCC Before Idempotency ===\n");

    let port = 15800 + (std::process::id() % 100) as u16;
    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let _server = TestServer::start_with_config(port, config).await?;
    let addr = format!("127.0.0.1:{}", port);
    let key = AggregateKey::new(1, 1, 1);

    let mut client = CeleriantClient::connect(&addr).await?;

    // ========================================
    // Setup: Create aggregate and establish known state using CLIENT_ID 7777
    // ========================================
    println!("Setup: Create aggregate and write initial events using CLIENT_ID 7777");
    println!("---------------------------------------------------------------------");

    // Create aggregate
    write_event(&mut client, &key, 1, true).await?;
    println!("  Aggregate created (event_batch_index=1)");

    // Write with OCC to discover the current batch index
    // Try expected=1 first (FIRST_EVENT_BATCH_INDEX)
    write_occ(&mut client, &key, 1, Some(1), false).await?;
    println!("  OCC write succeeded with expected=1 (event_batch_index now 2)");

    // Write another to bump it to 3
    write_occ(&mut client, &key, 2, Some(2), false).await?;
    println!("  OCC write succeeded with expected=2 (event_batch_index now 3)");

    let count = count_events(&mut client, &key).await?;
    assert_eq!(count, 3, "Should have 3 events");
    println!("  Verified: {} events total\n", count);

    // At this point: event_batch_index=3, client 7777 has client_event_index 1 and 2

    // ========================================
    // TEST 1: Stale OCC + duplicate idempotency → OCC fires first
    // ========================================
    println!("TEST 1: Stale OCC (expected=1) + duplicate idempotency (client_event_index=1)");
    println!("------------------------------------------------------------------------------");

    let result = write_occ(&mut client, &key, 1, Some(1), true).await;
    match &result {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { expected_event_batch_index, current_event_batch_index },
            ..
        })) => {
            println!("  Got OCC violation (expected={:?}, current={:?})", expected_event_batch_index, current_event_batch_index);
            println!("  CORRECT: OCC fires before idempotency\n");
        }
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::ClientIdempotencyViolation { .. }, ..
        })) => {
            return Err("Got ClientIdempotencyViolation but expected OCC first (invariant 16 violated)".into());
        }
        other => return Err(format!("Expected OCC violation, got: {:?}", other).into()),
    }

    // ========================================
    // TEST 2: Correct OCC + duplicate idempotency → idempotency fires
    // ========================================
    println!("TEST 2: Correct OCC (expected=3) + duplicate idempotency (client_event_index=1)");
    println!("---------------------------------------------------------------------------------");

    let result = write_occ(&mut client, &key, 1, Some(3), true).await;
    match &result {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::ClientIdempotencyViolation { last_client_event_index, attempted_client_event_index },
            ..
        })) => {
            println!("  Got idempotency violation (last={:?}, attempted={:?})", last_client_event_index, attempted_client_event_index);
            println!("  CORRECT: OCC passed, idempotency caught the duplicate\n");
        }
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
        })) => {
            return Err("Got OCC violation but expected idempotency (OCC should have passed with correct expected)".into());
        }
        other => return Err(format!("Expected idempotency violation, got: {:?}", other).into()),
    }

    // ========================================
    // TEST 3: Correct OCC + fresh idempotency → success
    // ========================================
    println!("TEST 3: Correct OCC (expected=3) + fresh client_event_index=3");
    println!("--------------------------------------------------------------");

    write_occ(&mut client, &key, 3, Some(3), true).await?;
    println!("  Write succeeded (both OCC and idempotency passed)");

    let final_count = count_events(&mut client, &key).await?;
    assert_eq!(final_count, 4, "Should have 4 events total");
    println!("  Final event count: {}\n", final_count);

    println!("=== All Tests Passed ===");
    println!("OCC before idempotency ordering (invariant 16) verified:");
    println!("  1. Stale OCC + duplicate idempotency -> OCC fires first");
    println!("  2. Correct OCC + duplicate idempotency -> idempotency fires");
    println!("  3. Correct OCC + fresh idempotency -> success\n");

    Ok(())
}
