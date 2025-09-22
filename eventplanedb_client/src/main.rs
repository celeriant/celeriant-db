use std::net::TcpStream;
use std::io::{self};
use eventplanedb_storage_structures::event_item::EventItem;
use serde::{Serialize, Deserialize};
use thiserror::Error;
use std::os::unix::io::{AsRawFd, FromRawFd};
use tokio::runtime::Runtime;

// Import protocol types and functions
mod protocol;
use protocol::{Request, Response, ProtocolError, write_message, read_message};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use Tokio runtime for async protocol functions
    let rt = Runtime::new()?;

    let server_address = "127.0.0.1:8080"; // Adjust as necessary

    // Create a sample EventItem
    let event = EventItem::new(
        2,                // client_event_index
        0,                // event_index (server will likely overwrite)
        1_700_000_000_000, // event_timestamp (ms)
        1,                // event_type_major
        0,                // event_type_minor
        b"hello world".to_vec(), // event_value
    );

    let request = Request::AppendEvents {
        org_id: 1,
        aggregate_type_id: 1,
        aggregate_id: 1,
        client_id: 1,
        user_id: None,
        events: vec![event],
        expected_event_batch_index: None,
    };

    rt.block_on(async {
        let mut stream = tokio::net::TcpStream::connect(server_address).await.expect("Failed to connect");
        write_message(&mut stream, &request).await?;
        let response: Response = read_message(&mut stream).await?;
        match response {
            Response::AppendEventsResult(result) => {
                match result {
                    Ok(_) => println!("Success: Event appended"),
                    Err(error_message) => eprintln!("Error: {}", error_message),
                }
            }
        }
        Ok::<(), ProtocolError>(())
    })?;

    Ok(())
}