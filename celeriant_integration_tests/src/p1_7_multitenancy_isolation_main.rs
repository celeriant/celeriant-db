//! P1-7: Multi-Tenancy Isolation Integration Test
//!
//! Tests that org A cannot see org B's data.
//!
//! Scenario:
//! 1. Write events under org_id = 1
//! 2. List aggregates filtering by org_id = 2 - should return empty
//! 3. Read a specific aggregate key belonging to org 1 but from a request context
//!    expecting org 2 - should return not found
//!
//! Run with: cargo run --bin p1_7_multitenancy_isolation_main

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_integration_tests::TestServer;
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::list_operations::{ListAggregatesIterator, ListOptions};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::{
        read_filters::ReadFilters,
        requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};

const PORT_BASE: u16 = 18700;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P1-7: Multi-Tenancy Isolation Test ===\n");

    // Start standalone server
    println!("Starting test server...");
    let server = TestServer::start_with_port(PORT_BASE).await?;
    println!("Server started at {}\n", server.address());

    let mut client = CeleriantClient::connect(server.address()).await?;

    // Phase 1: Write events to org_id = 1
    println!("=== Phase 1: Write events to org_id = 1 ===");
    let org1_agg1 = AggregateKey::new(1, 100, 501);
    let org1_agg2 = AggregateKey::new(1, 100, 502);
    let org1_agg3 = AggregateKey::new(1, 101, 601);
    let client_id: u128 = 777;

    for (i, agg) in [&org1_agg1, &org1_agg2, &org1_agg3].iter().enumerate() {
        let event = create_event(0, format!("Org 1 event for aggregate {}", i + 1));
        let mut writes = HashMap::new();
        writes.insert(
            (*agg).clone(),
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_event_batch_index: Some(0),
                enforce_client_idempotency: true,
                compression_type: CompressionType::None,
            },
        );

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: Some(i as u128),
            client_id,
            user_id: None,
            writes,
        });
        client
            .send_request(&request, CompressionType::None)
            .await?;
        println!("  Written event to aggregate {:?}", agg);
    }

    // Phase 2: List aggregates filtering by org_id = 2 (should be empty)
    println!("\n=== Phase 2: List aggregates for org_id = 2 (expect empty) ===");
    let options = ListOptions::default();
    let aggs_iter = ListAggregatesIterator::new(&mut client, Some(2), None, options);
    let org2_aggregates = aggs_iter.collect().await?;
    println!("Found {} aggregates for org_id = 2", org2_aggregates.len());
    assert!(
        org2_aggregates.is_empty(),
        "Expected org_id = 2 to have zero aggregates, found {}",
        org2_aggregates.len()
    );
    println!("  PASS: org_id = 2 has zero aggregates");

    // Phase 3: List aggregates for org_id = 1 (sanity check)
    println!("\n=== Phase 3: List aggregates for org_id = 1 (sanity check) ===");
    let options = ListOptions::default();
    let aggs_iter = ListAggregatesIterator::new(&mut client, Some(1), None, options);
    let org1_aggregates = aggs_iter.collect().await?;
    println!("Found {} aggregates for org_id = 1", org1_aggregates.len());
    assert!(
        org1_aggregates.len() == 3,
        "Expected org_id = 1 to have 3 aggregates, found {}",
        org1_aggregates.len()
    );
    println!("  PASS: org_id = 1 has 3 aggregates as expected");

    // Phase 4: Attempt to read an org=1 aggregate with org=2 key (should fail)
    println!("\n=== Phase 4: Read org=1 aggregate using org=2 key (expect not found) ===");
    // Construct a key that has org_id=2 but same type/id as org1_agg1
    let org2_key_for_org1_data = AggregateKey::new(2, 100, 501);
    let read_request = ClientRequest::Read(ReadRequest {
        correlation_id: None,
        aggregate_key: org2_key_for_org1_data.clone(),
        filters: ReadFilters::new(1),
    });
    let read_result = client
        .send_request(&read_request, CompressionType::None)
        .await;

    match read_result {
        Ok(response) => {
            panic!("Expected read to fail with not found, but got success: {:?}", response);
        }
        Err(e) => {
            // Expect error code 1001 (AggregateDoesNotExist)
            match &e {
                celeriant_client_tokio::client_error::ClientError::CeleriantError(error_response) => {
                    println!("  Received error code: {}", error_response.error_code);
                    assert!(
                        error_response.error_code == 1001,
                        "Expected error code 1001 (AggregateDoesNotExist), got {}",
                        error_response.error_code
                    );
                    println!("  PASS: org=2 key for org=1 data returns AggregateDoesNotExist");
                }
                _ => {
                    panic!("Expected CeleriantError with code 1001, got: {:?}", e);
                }
            }
        }
    }

    // Phase 5: Verify org=1 key can still read the data (sanity check)
    println!("\n=== Phase 5: Read org=1 aggregate using org=1 key (sanity check) ===");
    let read_request = ClientRequest::Read(ReadRequest {
        correlation_id: None,
        aggregate_key: org1_agg1.clone(),
        filters: ReadFilters::new(1),
    });
    let read_result = client
        .send_request(&read_request, CompressionType::None)
        .await?;

    match read_result {
        ClientResponse::Read(read_resp) => {
            let event_count: usize = read_resp
                .event_batches
                .iter()
                .map(|b| b.events.len())
                .sum();
            println!("  Read {} events from org=1 aggregate", event_count);
            assert!(
                event_count == 1,
                "Expected 1 event for org=1 aggregate, found {}",
                event_count
            );
            println!("  PASS: org=1 key reads org=1 data successfully");
        }
        other => {
            panic!("Expected Read response, got: {:?}", other);
        }
    }

    println!("\n=== All multi-tenancy isolation tests passed! ===");

    Ok(())
}

fn create_event(client_event_index: u64, message: String) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_event_index,
        event_index: 0,
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
