//! Simple example of using the EventPlaneDB client

use eventplanedb_client::{ClientConfig, EventPlaneDBClient};
use eventplanedb_structures::{
    compression_type::CompressionType,
    event_item::EventItem,
    read_filters::ReadFilters,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client with custom configuration
    let config = ClientConfig::new("127.0.0.1:10000".to_string())
        .with_timeout(5000)
        .with_retries(3, 100)
        .with_compression(CompressionType::None);

    let client = EventPlaneDBClient::new(config).await?;

    // Define aggregate identifiers
    let org_id = 1u128;
    let aggregate_type_id = 100u128;
    let aggregate_id = 12345u128;
    let client_id = 999u128;

    // Check if aggregate exists
    let exists_response = client
        .exists(org_id, aggregate_type_id, aggregate_id, None)
        .await?;

    if let Some(err) = exists_response.error {
        eprintln!("Error checking existence: {:?}", err);
        return Ok(());
    }

    println!("Aggregate exists: {}", exists_response.exists);

    // Create some events
    let events = vec![
        EventItem::new(
            1,                           // client_event_index
            0,                           // event_index (server will assign)
            Some(1001),                  // event_id
            chrono::Utc::now().timestamp_millis() as u64,
            1,                           // event_type_major
            0,                           // event_type_minor
            b"Hello, EventPlaneDB!".to_vec(),
        ),
        EventItem::new(
            2,
            0,
            Some(1002),
            chrono::Utc::now().timestamp_millis() as u64,
            1,
            0,
            b"Second event".to_vec(),
        ),
    ];

    // Write events
    let write_response = client
        .write(
            org_id,
            aggregate_type_id,
            aggregate_id,
            client_id,
            None,                        // user_id
            events,
            true,                        // allow_create
            None,                        // expected_event_batch_index (OCC)
            false,                        // enforce_client_idempotency
            None,                        // durable_write_with_delay_us
            CompressionType::None,
            None,                        // correlation_id
        )
        .await?;

    if let Some(err) = write_response.error {
        eprintln!("Error writing events: {:?}", err);
        return Ok(());
    }

    if let Some(result) = write_response.result {
        println!("Events written. Next batch index: {}", result.next_event_batch_index);
    }

    // Read events back
    let filters = ReadFilters::new(1); // Start from batch index 1
    let read_response = client
        .read(org_id, aggregate_type_id, aggregate_id, filters, None)
        .await?;

    if let Some(err) = read_response.error {
        eprintln!("Error reading events: {:?}", err);
        return Ok(());
    }

    if let Some(result) = read_response.result {
        println!("Read {} event batches", result.event_batches.len());
        for batch in result.event_batches {
            println!("  Batch {}: {} events", batch.event_batch_index, batch.events.len());
            for event in batch.events {
                println!(
                    "    Event {}: type={}, value={:?}",
                    event.event_index,
                    event.event_type_major,
                    String::from_utf8_lossy(&event.event_value)
                );
            }
        }
    }

    // Get client statistics
    let stats = client.stats().await;
    println!("\nClient Statistics:");
    println!("  Active connections: {}", stats.active_connections);
    println!("  Idle connections: {}", stats.idle_connections);
    println!("  Total requests: {}", stats.total_requests);
    println!("  Failed requests: {}", stats.failed_requests);

    // Gracefully close
    client.close().await;

    Ok(())
}