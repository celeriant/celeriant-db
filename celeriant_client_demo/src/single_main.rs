use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::{
    process_requests::Request,
    request::{read_filters::ReadFilters, requests::{ExistsRequest, ReadRequest, SingleAggregateWrite, WriteRequest}},
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect("0.0.0.0:10000").await?;

    let aggregate_1 = AggregateKey::new(1, 2, 101);
    let aggregate_2 = AggregateKey::new(1, 2+32, 201);
    let client_id: u128 = 999;

    // Check if aggregates exist
    println!("=== Checking if aggregates exist ===");
    for agg in [&aggregate_1, &aggregate_2] {
        let request = Request::Exists(ExistsRequest {
            aggregate_key: agg.clone(),
            correlation_id: None,
        });
        match client.send_request(&request, CompressionType::None).await {
            Ok(response) => println!("Aggregate {:?}: {:?}", agg, response),
            Err(e) => println!("Aggregate {:?}: Error - {:?}", agg, e),
        }
    }

    // Create initial events for both aggregates
    println!("\n=== Creating aggregates with initial writes ===");
    for (i, agg) in [&aggregate_1, &aggregate_2].iter().enumerate() {
        let event = create_event(0, format!("Initial event for aggregate {}", i + 1));
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

        let request = Request::Write(WriteRequest {
            correlation_id: Some(i as u128),
            client_id,
            user_id: None,
            writes,
        });
        match client.send_request(&request, CompressionType::None).await {
            Ok(response) => println!("Initial write to aggregate {}: {:?}", i + 1, response),
            Err(e) => println!("Initial write to aggregate {} failed: {:?}", i + 1, e),
        }
    }

    // Atomic multi-aggregate write
    println!("\n=== Performing atomic multi-aggregate write ===");
    let event_1 = create_event(1, "Atomic write event for aggregate 1".to_string());
    let event_2 = create_event(1, "Atomic write event for aggregate 2".to_string());

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_1.clone(),
        SingleAggregateWrite {
            events: vec![event_1],
            allow_create: false,
            expected_event_batch_index: Some(1),
            enforce_client_idempotency: true,
            compression_type: CompressionType::None,
        },
    );
    writes.insert(
        aggregate_2.clone(),
        SingleAggregateWrite {
            events: vec![event_2],
            allow_create: false,
            expected_event_batch_index: Some(1),
            enforce_client_idempotency: true,
            compression_type: CompressionType::None,
        },
    );

    let atomic_request = Request::Write(WriteRequest {
        correlation_id: Some(1000),
        client_id,
        user_id: Some(42),
        writes,
    });
    match client.send_request(&atomic_request, CompressionType::None).await {
        Ok(response) => println!("Atomic multi-aggregate write: {:?}", response),
        Err(e) => println!("Atomic multi-aggregate write failed: {:?}", e),
    }

    // Test idempotency - retry same write
    println!("\n=== Testing idempotency (retry same write) ===");
    match client.send_request(&atomic_request, CompressionType::None).await {
        Ok(response) => println!("Idempotent retry succeeded: {:?}", response),
        Err(e) => println!("Idempotent retry result: {:?}", e),
    }

    // Read back events from both aggregates
    println!("\n=== Reading events from both aggregates ===");
    for (i, agg) in [&aggregate_1, &aggregate_2].iter().enumerate() {
        let request = Request::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: (*agg).clone(),
            filters: ReadFilters::new(1),
        });
        match client.send_request(&request, CompressionType::None).await {
            Ok(response) => println!("Aggregate {} events: {:?}", i + 1, response),
            Err(e) => println!("Aggregate {} read failed: {:?}", i + 1, e),
        }
    }

    // Test expected_event_batch_index conflict
    println!("\n=== Testing expected_event_batch_index conflict ===");
    let conflict_event = create_event(2, "This should fail".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_1.clone(),
        SingleAggregateWrite {
            events: vec![conflict_event],
            allow_create: false,
            expected_event_batch_index: Some(0), // Wrong! Should be 2 now
            enforce_client_idempotency: true,
            compression_type: CompressionType::None,
        },
    );

    let request = Request::Write(WriteRequest {
        correlation_id: Some(2000),
        client_id,
        user_id: None,
        writes,
    });
    match client.send_request(&request, CompressionType::None).await {
        Ok(response) => println!("Unexpected success: {:?}", response),
        Err(e) => println!("Expected conflict error: {:?}", e),
    }

    Ok(())
}


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