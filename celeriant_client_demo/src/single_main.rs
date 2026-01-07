use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::list_operations::{ListOptions, ListOrgsIterator, ListAggregateTypesIterator, ListAggregatesIterator};
use celeriant_msg::{
    process_requests::Request,
    request::{read_filters::ReadFilters, requests::{DeleteRequest, ExistsRequest, ReadRequest, SingleAggregateDelete, SingleAggregateWrite, WriteRequest}},
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

    // === List Operations ===
    println!("\n=== Listing Organizations ===");
    let options = ListOptions::default();
    let mut orgs_iter = ListOrgsIterator::new(&mut client, options);
    let orgs = orgs_iter.collect().await?;
    println!("Found {} organizations:", orgs.len());
    for org in &orgs {
        println!("  - Org ID: {}", org.org_id);
    }
    // Verify org_id 1 exists (both aggregates use org_id 1)
    assert!(orgs.iter().any(|o| o.org_id == 1), "Expected org_id 1 to exist");
    println!("✓ Verified org_id 1 exists");

    println!("\n=== Listing Aggregate Types ===");
    let options = ListOptions::default();
    let mut types_iter = ListAggregateTypesIterator::new(&mut client, Some(1), options);
    let agg_types = types_iter.collect().await?;
    println!("Found {} aggregate types for org 1:", agg_types.len());
    for agg_type in &agg_types {
        println!("  - Org: {}, Type ID: {}", agg_type.org_id, agg_type.aggregate_type_id);
    }
    // Verify both aggregate types exist (2 and 34)
    assert!(agg_types.iter().any(|t| t.aggregate_type_id == 2), "Expected aggregate_type_id 2 to exist");
    assert!(agg_types.iter().any(|t| t.aggregate_type_id == 34), "Expected aggregate_type_id 34 to exist");
    println!("✓ Verified aggregate types 2 and 34 exist");

    println!("\n=== Listing Aggregates (before delete) ===");
    let options = ListOptions::default();
    let mut aggs_iter = ListAggregatesIterator::new(&mut client, Some(1), None, options);
    let aggregates = aggs_iter.collect().await?;
    println!("Found {} aggregates for org 1:", aggregates.len());
    for agg in &aggregates {
        println!("  - Org: {}, Type: {}, ID: {}, Deleted: {}", 
            agg.org_id, agg.aggregate_type_id, agg.aggregate_id, agg.is_deleted);
    }
    // Verify both aggregates exist and are not deleted
    assert!(aggregates.iter().any(|a| a.aggregate_id == 101 && !a.is_deleted), 
        "Expected aggregate 101 to exist and not be deleted");
    assert!(aggregates.iter().any(|a| a.aggregate_id == 201 && !a.is_deleted), 
        "Expected aggregate 201 to exist and not be deleted");
    println!("✓ Verified aggregates 101 and 201 exist and are not deleted");

    // === Delete aggregate_1 ===
    println!("\n=== Deleting aggregate 1 ===");
    let mut deletes = HashMap::new();
    deletes.insert(
        aggregate_1.clone(),
        SingleAggregateDelete {
            allow_recreate: false,
            allow_index_continuation: false,
            expected_event_batch_index: Some(2), // We have 2 event batches now (0 and 1)
        },
    );
    let delete_request = Request::Delete(DeleteRequest {
        correlation_id: Some(3000),
        client_id,
        user_id: Some(42),
        deletes,
    });
    match client.send_request(&delete_request, CompressionType::None).await {
        Ok(response) => println!("Delete aggregate 1: {:?}", response),
        Err(e) => println!("Delete aggregate 1 failed: {:?}", e),
    }

    // === List Aggregates again to verify delete ===
    println!("\n=== Listing Aggregates (after delete, excluding deleted) ===");
    let options = ListOptions::default();
    let mut aggs_iter = ListAggregatesIterator::new(&mut client, Some(1), None, options);
    let aggregates = aggs_iter.collect().await?;
    println!("Found {} non-deleted aggregates for org 1:", aggregates.len());
    for agg in &aggregates {
        println!("  - Org: {}, Type: {}, ID: {}, Deleted: {}", 
            agg.org_id, agg.aggregate_type_id, agg.aggregate_id, agg.is_deleted);
    }
    // Verify aggregate 101 is no longer in the list (filtered out as deleted)
    assert!(!aggregates.iter().any(|a| a.aggregate_id == 101), 
        "Expected aggregate 101 to be filtered out (deleted)");
    assert!(aggregates.iter().any(|a| a.aggregate_id == 201 && !a.is_deleted), 
        "Expected aggregate 201 to still exist and not be deleted");
    println!("✓ Verified aggregate 101 is filtered out, aggregate 201 still exists");

    println!("\n=== Listing Aggregates (after delete, including deleted) ===");
    let options = ListOptions {
        include_deleted: true,
        ..Default::default()
    };
    let mut aggs_iter = ListAggregatesIterator::new(&mut client, Some(1), None, options);
    let aggregates = aggs_iter.collect().await?;
    println!("Found {} total aggregates for org 1 (including deleted):", aggregates.len());
    for agg in &aggregates {
        println!("  - Org: {}, Type: {}, ID: {}, Deleted: {}", 
            agg.org_id, agg.aggregate_type_id, agg.aggregate_id, agg.is_deleted);
    }
    // Verify aggregate 101 shows as deleted
    assert!(aggregates.iter().any(|a| a.aggregate_id == 101 && a.is_deleted), 
        "Expected aggregate 101 to be marked as deleted");
    assert!(aggregates.iter().any(|a| a.aggregate_id == 201 && !a.is_deleted), 
        "Expected aggregate 201 to still exist and not be deleted");
    println!("✓ Verified aggregate 101 is marked as deleted, aggregate 201 is not deleted");

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

    println!("\n=== All tests completed successfully! ===");

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