use std::time::Duration;

use eventplanedb_client::{ClientError, EventPlaneDBClient};
use eventplanedb_structures::{
    compression_type::CompressionType, event_item::EventItem, read_filters::ReadFilters, request::{ReadRequest, Request, WriteRequest}
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = EventPlaneDBClient::connect("127.0.0.1:10000")
        .await?
        .with_timeout(Duration::from_secs(5));

    let request = Request::Write(WriteRequest { 
        correlation_id: Some(999), 
        org_id: 1, 
        aggregate_type_id: 3, 
        aggregate_id: 4, 
        client_id: 123, 
        user_id: None, 
        events: vec![
            // Fill with EventItems
            EventItem::new(
                0,                           // client_event_index
                0,                           // event_index (server will assign)
                None,                       // event_id
                0,                          // client timestamp
                1,                           // event_type_major
                0,                           // event_type_minor
                b"Welcome to a large chunk of TEXT!".to_vec(),
            ),
        ], 
        allow_create: true, 
        expected_event_batch_index: None, 
        enforce_client_idempotency: false, 
        durable_write_with_delay_us: Some(20), 
        compression_type: CompressionType::None });
    
    match client.send_request(&request, CompressionType::None).await {
        Ok(response) => println!("Success: {:?}", response),
        Err(ClientError::EventPlaneDBError(e)) => {
            // Handle specific EventPlaneDB errors
            println!("DB Error: {:?}", e);
        }
        Err(e) => println!("Other error: {}", e),
    }

    let request = Request::Read(ReadRequest {
        correlation_id:Some(1000),
        org_id:1,
        aggregate_type_id:3,
        aggregate_id:4,
        filters: ReadFilters::new(1) 
    });
    
    match client.send_request(&request, CompressionType::Zstd { level: 6 }).await {
        Ok(response) => println!("Success: {:?}", response),
        Err(ClientError::EventPlaneDBError(e)) => {
            // Handle specific EventPlaneDB errors
            println!("DB Error: {:?}", e);
        }
        Err(e) => println!("Other error: {}", e),
    }

    Ok(())
}