use std::net::TcpStream;
use std::io::{self};
use eventplanedb_storage_structures::event_item::EventItem;
use serde::{Serialize, Deserialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use tokio::runtime::Runtime;
use tokio::time::Instant;

// Import protocol types and functions
mod protocol;
use protocol::{Request, Response, ProtocolError, write_message, read_message};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use Tokio runtime for async protocol functions
    let rt = Runtime::new()?;

    let server_address = "127.0.0.1:8080"; // Adjust as necessary

    rt.block_on(async {
        let mut client_event_index = 0;
        let mut total_events = 0;
        let mut last_report = Instant::now();
        let report_interval = 100; // Print stats every 100 events

        let mut aggregate_id_offset = 0;
        let mut stream = tokio::net::TcpStream::connect(server_address).await.expect("Failed to connect");
        loop {
            // Create a sample EventItem
            let event = EventItem::new(
                client_event_index,                // client_event_index
                0,                // event_index (server will likely overwrite)
                0, // event_timestamp (ms)
                1,                // event_type_major
                0,                // event_type_minor
                b"hello world".to_vec(), // event_value
            );

            let request = Request::AppendEvents {
                org_id: 43,
                aggregate_type_id: 23,
                aggregate_id: 122 + aggregate_id_offset,
                client_id: 44,
                user_id: None,
                events: vec![event],
                expected_event_batch_index: None,
            };
            // let request = Request::Exists { org_id: 43, aggregate_type_id: 23, aggregate_id: 12 + aggregate_id_offset };
            aggregate_id_offset += 1;
            if aggregate_id_offset > 5 {
                aggregate_id_offset = 0;
            }

            write_message(&mut stream, &request).await?;
            let response: Response = read_message(&mut stream).await?;
            match response {
                Response::AppendEventsResult(result) => {
                    match result {
                        Ok(_) => (), // Success
                        Err(error_message) => eprintln!("Error: {}", error_message),
                    }
                },
                Response::ExistsResult(result) => {
                    match result {
                        Ok(exists) => (), // Success
                        Err(error_message) => eprintln!("Error: {}", error_message),
                    }
                },
                _ => eprintln!("Unexpected response type"),
            }
            // stream.shutdown().await;
            client_event_index += 3;
            total_events += 1;

            if total_events % report_interval == 0 {
                let elapsed = last_report.elapsed().as_secs_f64();
                let rate = report_interval as f64 / elapsed;
                println!("Wrote {} events in {:.2} seconds ({:.2} events/sec)", report_interval, elapsed, rate);
                last_report = Instant::now();
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), ProtocolError>(())
    })?;

    Ok(())
}