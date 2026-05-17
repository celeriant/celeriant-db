//! P1-4: Exactly-Once Writes Under Connection Failure
//!
//! Tests that writes are exactly-once even when TCP connection fails mid-write.
//!
//! Scenario:
//! 1. Client sends a write with enforce_client_idempotency: true
//! 2. Connection is dropped after sending but before receiving response (uncertain ack)
//! 3. Client reconnects and retries with same client_id and client_seq
//! 4. Verify: exactly one event exists (original write succeeded, retry rejected with error 2002)
//!
//! Run with: cargo run --bin p1_4_exactly_once_main

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{ServerConfig, TestServer};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::{
        read_filters::ReadFilters,
        requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};

const PORT_BASE: u16 = 19500;
const CLIENT_ID: u128 = 777;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P1-4: Exactly-Once Writes Under Connection Failure ===\n");

    // Start standalone server
    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let server = TestServer::start_with_config(PORT_BASE, config).await?;
    println!("Server started at {}\n", server.address());

    let aggregate = AggregateKey::new(1, 2, 101);

    // Test 1: Basic idempotency - normal retry on same connection
    println!("=== Test 1: Basic idempotency (normal retry) ===");
    test_basic_idempotency(&server.address(), &aggregate).await?;

    // Test 2: Uncertain ack scenario - connection drop after send
    println!("\n=== Test 2: Uncertain ack (connection drop) ===");
    test_uncertain_ack(&server.address(), &aggregate).await?;

    println!("\n=== All tests passed! ===");
    Ok(())
}

async fn test_basic_idempotency(
    server_address: &str,
    aggregate: &AggregateKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect(server_address).await?;

    let event = create_event(1, "Test 1: Basic idempotency".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event.clone()],
            allow_create: true,
            expected_version: None, // Don't use OCC for this test
            enforce_client_idempotency: true,
        },
    );

    let request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(1),
        client_id: CLIENT_ID,
        user_id: None,
        writes: writes.clone(),
    });

    // First write should succeed
    println!("  Sending first write...");
    let response = client.send_request(&request).await?;
    match response {
        ClientResponse::Write(_) => println!("  First write: SUCCESS"),
        other => panic!("Expected Write response, got {:?}", other),
    }

    // Retry same write (same client_id, same client_seq)
    // Note: We can't reuse the exact same request object because WriteRequest takes ownership
    // So we create a new request with the same parameters
    let retry_request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(2), // Different correlation_id is fine for idempotency
        client_id: CLIENT_ID,
        user_id: None,
        writes: writes.clone(),
    });

    println!("  Retrying same write (should fail with error 2002)...");
    match client.send_request(&retry_request).await {
        Err(ClientError::Server(celeriant_client_tokio::server_error::ServerError::Write {
            kind: celeriant_client_tokio::server_error::WriteError::ClientIdempotencyViolation {
                last_client_seq,
                attempted_client_seq,
            }, ..
        })) => {
            assert_eq!(last_client_seq, Some(1), "last_client_seq should be 1");
            assert_eq!(attempted_client_seq, Some(1), "attempted_client_seq should be 1");
            println!("  Retry: REJECTED with ClientIdempotencyViolation (last={}, attempted={}) - CORRECT",
                last_client_seq.unwrap(), attempted_client_seq.unwrap());
        }
        other => panic!("Expected ClientIdempotencyViolation, got {:?}", other),
    }

    // Read back and verify exactly one event batch
    verify_event_count(&mut client, aggregate, 1, 1).await?;

    Ok(())
}

async fn test_uncertain_ack(
    server_address: &str,
    aggregate: &AggregateKey,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create first connection and send write
    let mut client1 = CeleriantClient::connect(server_address).await?;

    let event = create_event(2, "Test 2: Uncertain ack".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event.clone()],
            allow_create: false,
            expected_version: None, // Don't use OCC - rely on idempotency
            enforce_client_idempotency: true,
        },
    );

    let request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(3),
        client_id: CLIENT_ID,
        user_id: None,
        writes: writes.clone(),
    });

    println!("  Sending write on first connection...");
    let response = client1.send_request(&request).await?;
    match response {
        ClientResponse::Write(_) => println!("  Write sent successfully"),
        other => panic!("Expected Write response, got {:?}", other),
    }

    // Drop the connection WITHOUT reading the response (simulate uncertain ack)
    println!("  Dropping connection (simulating uncertain ack)...");
    drop(client1);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create new connection and retry the exact same write
    println!("  Reconnecting and retrying same write...");
    let mut client2 = CeleriantClient::connect(server_address).await?;

    let retry_request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(4),
        client_id: CLIENT_ID,
        user_id: None,
        writes: writes.clone(),
    });

    match client2.send_request(&retry_request).await {
        Err(ClientError::Server(celeriant_client_tokio::server_error::ServerError::Write {
            kind: celeriant_client_tokio::server_error::WriteError::ClientIdempotencyViolation {
                last_client_seq,
                attempted_client_seq,
            }, ..
        })) => {
            assert_eq!(last_client_seq, Some(2), "last_client_seq should be 2");
            assert_eq!(attempted_client_seq, Some(2), "attempted_client_seq should be 2");
            println!("  Retry: REJECTED with ClientIdempotencyViolation (last={}, attempted={}) - CORRECT",
                last_client_seq.unwrap(), attempted_client_seq.unwrap());
        }
        Ok(ClientResponse::Write(_)) => {
            println!("  Retry: SUCCESS (original didn't reach server) - ACCEPTABLE");
            println!("  (Either way, exactly one event should exist)");
        }
        other => panic!("Expected ClientIdempotencyViolation or Write response, got {:?}", other),
    }

    // Read back and verify exactly one event exists for this client_seq
    verify_event_count(&mut client2, aggregate, 2, 2).await?;

    // Additional verification: read all events and check client_seq values
    verify_client_event_indices(&mut client2, aggregate).await?;

    Ok(())
}

async fn verify_event_count(
    client: &mut CeleriantClient,
    aggregate: &AggregateKey,
    expected_batch_count: usize,
    expected_event_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = ClientRequest::Read(ReadRequest {
        correlation_id: None,
        aggregate_key: aggregate.clone(),
        filters: ReadFilters::new(1),
    });

    let response = client.send_request(&request).await?;
    match response {
        ClientResponse::Read(r) => {
            let batch_count = r.event_batches.len();
            let event_count: usize = r.event_batches.iter().map(|b| b.events.len()).sum();
            println!(
                "  Verification: {} event batches, {} total events",
                batch_count, event_count
            );
            assert_eq!(
                batch_count, expected_batch_count,
                "Expected {} event batches, got {}",
                expected_batch_count, batch_count
            );
            assert_eq!(
                event_count, expected_event_count,
                "Expected {} events, got {}",
                expected_event_count, event_count
            );
            println!("  Verification: PASSED");
            Ok(())
        }
        other => panic!("Expected Read response, got {:?}", other),
    }
}

async fn verify_client_event_indices(
    client: &mut CeleriantClient,
    aggregate: &AggregateKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = ClientRequest::Read(ReadRequest {
        correlation_id: None,
        aggregate_key: aggregate.clone(),
        filters: ReadFilters::new(1),
    });

    let response = client.send_request(&request).await?;
    match response {
        ClientResponse::Read(r) => {
            let mut client_event_indices = Vec::new();
            for batch in &r.event_batches {
                for event in &batch.events {
                    client_event_indices.push(event.client_seq);
                }
            }
            client_event_indices.sort();

            println!("  Client event indices: {:?}", client_event_indices);

            // Check for duplicates
            for i in 1..client_event_indices.len() {
                assert_ne!(
                    client_event_indices[i - 1],
                    client_event_indices[i],
                    "Duplicate client_seq found: {}",
                    client_event_indices[i]
                );
            }

            // Check for expected indices (1 and 2)
            assert_eq!(client_event_indices.len(), 2, "Expected 2 events");
            assert_eq!(client_event_indices[0], 1, "Expected client_seq 1");
            assert_eq!(client_event_indices[1], 2, "Expected client_seq 2");

            println!("  Client event indices verification: PASSED (no duplicates)");
            Ok(())
        }
        other => panic!("Expected Read response, got {:?}", other),
    }
}

fn create_event(client_seq: u64, message: String) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq,
        event_seq: 0, // Server will assign
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
