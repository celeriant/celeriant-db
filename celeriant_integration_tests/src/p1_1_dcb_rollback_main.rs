//! P1-1: Multi-Aggregate Atomic Write (DCB) Rollback on Partial OCC Failure
//!
//! Proves that multi-aggregate atomic writes roll back completely when one aggregate
//! fails its OCC check. This is the core correctness claim for distributed coordinated
//! batching (DCB): all aggregates commit or none do.
//!
//! Scenario:
//! 1. Write to aggregate A (version 0->1) and aggregate B (version 0->1)
//! 2. Concurrently, write to aggregate B (version 1->2) from another client
//! 3. Issue a DCB write: A at version 1->2, B at version 1->2 (but B is now stale at version 2)
//! 4. Verify: both A and B are rejected, A's event count is unchanged at 1
//!
//! Run with: cargo run --bin p1_1_dcb_rollback_main

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_integration_tests::{count_events, ServerConfig, TestServer};
use celeriant_msg::{
    process_requests::Request,
    process_responses::Response,
    request::requests::{SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};

const PORT_BASE: u16 = 18500;

fn create_event(client_event_index: u64, message: String) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_event_index,
        event_index: 0, // Server will assign
        event_id: Some(rand::random()),
        event_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(message.into_bytes()),
        iv: None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P1-1: DCB Rollback on Partial OCC Failure ===\n");

    // Start standalone server (simpler, faster for this test)
    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };

    let server = TestServer::start_with_config(PORT_BASE, config).await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    let client_id = rand::random();
    let aggregate_a = AggregateKey::new(1, 100, 1);
    let aggregate_b = AggregateKey::new(1, 100, 2);

    // Phase 1: Initial writes to both aggregates (version 0 -> 1)
    println!("Phase 1: Initial write to aggregate A and B (version 0 -> 1)");

    let event_a1 = create_event(1, "Initial event for aggregate A".to_string());
    let event_b1 = create_event(1, "Initial event for aggregate B".to_string());

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_a.clone(),
        SingleAggregateWrite {
            events: vec![event_a1],
            allow_create: true,
            expected_event_batch_index: Some(0),
            enforce_client_idempotency: true,
            compression_type: CompressionType::None,
        },
    );
    writes.insert(
        aggregate_b.clone(),
        SingleAggregateWrite {
            events: vec![event_b1],
            allow_create: true,
            expected_event_batch_index: Some(0),
            enforce_client_idempotency: true,
            compression_type: CompressionType::None,
        },
    );

    let request = Request::Write(WriteRequest {
        correlation_id: Some(1),
        client_id,
        user_id: None,
        writes,
    });

    let response = client
        .send_request(&request, CompressionType::None)
        .await?;

    match response {
        Response::Write(_) => println!("  ✓ Initial write succeeded"),
        _ => panic!("Initial write failed: {:?}", response),
    }

    // Verify both aggregates are at version 1
    let count_a = count_events(&mut client, &aggregate_a).await?;
    let count_b = count_events(&mut client, &aggregate_b).await?;
    assert_eq!(count_a, 1, "Aggregate A should have 1 event after initial write");
    assert_eq!(count_b, 1, "Aggregate B should have 1 event after initial write");
    println!("  ✓ Both aggregates at version 1\n");

    // Phase 2: Concurrent write to aggregate B (version 1 -> 2)
    println!("Phase 2: Concurrent write to aggregate B (version 1 -> 2)");

    let event_b2 = create_event(2, "Concurrent event for aggregate B".to_string());

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_b.clone(),
        SingleAggregateWrite {
            events: vec![event_b2],
            allow_create: false,
            expected_event_batch_index: Some(1),
            enforce_client_idempotency: true,
            compression_type: CompressionType::None,
        },
    );

    let request = Request::Write(WriteRequest {
        correlation_id: Some(2),
        client_id,
        user_id: None,
        writes,
    });

    let response = client
        .send_request(&request, CompressionType::None)
        .await?;

    match response {
        Response::Write(_) => println!("  ✓ Concurrent write to B succeeded"),
        _ => panic!("Concurrent write to B failed: {:?}", response),
    }

    // Verify aggregate B is now at version 2, A unchanged at 1
    let count_a = count_events(&mut client, &aggregate_a).await?;
    let count_b = count_events(&mut client, &aggregate_b).await?;
    assert_eq!(count_a, 1, "Aggregate A should still have 1 event");
    assert_eq!(count_b, 2, "Aggregate B should have 2 events after concurrent write");
    println!("  ✓ Aggregate B now at version 2\n");

    // Phase 3: DCB write with stale OCC on B (should rollback both A and B)
    println!("Phase 3: DCB write - A at version 1, B at stale version 1 (should fail)");

    let event_a2 = create_event(3, "DCB event for aggregate A".to_string());
    let event_b3 = create_event(4, "DCB event for aggregate B (stale)".to_string());

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_a.clone(),
        SingleAggregateWrite {
            events: vec![event_a2],
            allow_create: false,
            expected_event_batch_index: Some(1), // Expects version 1
            enforce_client_idempotency: true,
            compression_type: CompressionType::None,
        },
    );
    writes.insert(
        aggregate_b.clone(),
        SingleAggregateWrite {
            events: vec![event_b3],
            allow_create: false,
            expected_event_batch_index: Some(1), // Stale! B is actually at version 2
            enforce_client_idempotency: true,
            compression_type: CompressionType::None,
        },
    );

    let request = Request::Write(WriteRequest {
        correlation_id: Some(3),
        client_id,
        user_id: None,
        writes,
    });

    let result = client
        .send_request(&request, CompressionType::None)
        .await;

    // The server returns OCC violation as a CeleriantError, not a Response
    match result {
        Err(ClientError::CeleriantError(error)) => {
            println!("  ✓ DCB write rejected with error: {}", error.error_message);
            assert_eq!(
                error.error_code, 2003,
                "Expected OCC violation error code 2003, got {}",
                error.error_code
            );
            println!("  ✓ Error code is 2003 (WRITE_OPTIMISTIC_CONCURRENCY_VIOLATION)");
        }
        Ok(resp) => panic!("Expected OCC error, got success: {:?}", resp),
        Err(e) => panic!("Expected OCC error (code 2003), got: {:?}", e),
    }

    // Phase 4: Verify rollback - both A and B unchanged
    println!("\nPhase 4: Verify complete rollback");

    let count_a = count_events(&mut client, &aggregate_a).await?;
    let count_b = count_events(&mut client, &aggregate_b).await?;

    assert_eq!(
        count_a, 1,
        "Aggregate A should still have 1 event (rollback - A's write was not committed)"
    );
    assert_eq!(
        count_b, 2,
        "Aggregate B should still have 2 events (rollback - B's write was rejected)"
    );

    println!("  ✓ Aggregate A unchanged at version 1");
    println!("  ✓ Aggregate B unchanged at version 2");
    println!("  ✓ Complete rollback verified\n");

    println!("=== TEST PASSED: DCB Rollback on Partial OCC Failure ===");
    Ok(())
}
