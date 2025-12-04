use eventplanedb_client::{ClientError, EventPlaneDBClient};
use eventplanedb_structures::{
    compression_type::CompressionType, event_item::EventItem, read_filters::ReadFilters, request::{ReadRequest, Request, WriteRequest}
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = EventPlaneDBClient::connect("0.0.0.0:9000")
        .await?;
        // .with_timeout(Duration::from_secs(25));

    let request = Request::Write(WriteRequest { 
        correlation_id: Some(999), 
        org_id: 1, 
        aggregate_type_id: 2, 
        aggregate_id: 2, 
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
        aggregate_type_id:2,
        aggregate_id:2,
        filters: ReadFilters::new(1) 
    });
    
    match client.send_request(&request, CompressionType::Zstd { level: 6 }).await {
        Ok(response) => {
            // println!("Success: {:?}", response);
            match  response {
                eventplanedb_structures::response::Response::ListOrganisations(_list_organisations_response) => todo!(),
                eventplanedb_structures::response::Response::ListAggregates(_list_aggregates_response) => todo!(),
                eventplanedb_structures::response::Response::Exists(_exists_response) => todo!(),
                eventplanedb_structures::response::Response::Read(read_response) => {
                    println!("Batch count: {}", read_response.result.as_ref().unwrap().event_batches.len());
                    println!("Last lease index: {}", read_response.result.as_ref().unwrap().event_batches.last().unwrap().lease_index);
                },
                eventplanedb_structures::response::Response::Write(_write_response) => todo!(),
                eventplanedb_structures::response::Response::WriteBatches(_write_batches_response) => todo!(),
                eventplanedb_structures::response::Response::TrimStart(_trim_start_response) => todo!(),
                eventplanedb_structures::response::Response::Delete(_delete_response) => todo!(),
                eventplanedb_structures::response::Response::ProtocolError(_protocol_error_response) => todo!(),
                eventplanedb_structures::response::Response::UpdateCacheLimits(_update_cache_limits_response) => todo!(),
            }
        },
        Err(ClientError::EventPlaneDBError(e)) => {
            // Handle specific EventPlaneDB errors
            println!("DB Error: {:?}", e);
        }
        Err(e) => println!("Other error: {}", e),
    }

    Ok(())
}