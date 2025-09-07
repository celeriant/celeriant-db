use std::{net::SocketAddr, time::Instant};

use eventplanedb_storage_glommio::{protocol::*, GlommioResult};
use eventplanedb_storage_structures::{
    event_item::EventItem, read_filters::ReadFilters,
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{net::TcpStream, LocalExecutor};

struct SimpleClient {
    stream: TcpStream,
}

impl SimpleClient {
    async fn connect(addr: SocketAddr) -> GlommioResult<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    async fn send_request(&mut self, request: Request) -> GlommioResult<Response> {
        write_message(&mut self.stream, &request).await?;
        let response = read_message(&mut self.stream).await?;
        Ok(response)
    }
}

fn create_test_events(start_index: u64, count: usize) -> Vec<EventItem> {
    (0..count)
        .map(|i| {
            EventItem::new(
                start_index + i as u64,
                start_index + i as u64,
                1000 + i as u64,
                42,
                1,
                format!("test event {}", i).into_bytes(),
            )
        })
        .collect()
}

fn main() -> GlommioResult<()> {
    let local_ex = LocalExecutor::default();
    local_ex.run(async {
        let mut client = SimpleClient::connect("127.0.0.1:8080".parse().unwrap()).await?;

        println!("Connected to server");

        // Test append events
        let events = create_test_events(1, 100);
        let request = Request::AppendEvents {
            aggregate_id: "test_aggregate".to_string(),
            client_id: 100,
            user_id: None,
            events,
            expected_event_batch_index: None,
        };

        let start = Instant::now();
        let response = client.send_request(request).await?;
        let duration = start.elapsed();

        match response {
            Response::AppendEventsResult(Ok(metadata)) => {
                println!("Append successful: batch_index={}, took {:?}", 
                         metadata.event_batch_index, duration);
            }
            Response::AppendEventsResult(Err(e)) => {
                println!("Append failed: {}", e);
            }
            _ => {
                println!("Unexpected response: {:?}", response);
            }
        }

        // Test read events
        let request = Request::ReadFiltered {
            aggregate_id: "test_aggregate".to_string(),
            filters: ReadFilters::new(0),
        };

        let start = Instant::now();
        let response = client.send_request(request).await?;
        let duration = start.elapsed();

        match response {
            Response::ReadFilteredResult(Ok(result)) => {
                println!("Read successful: {} batches, {} events total, took {:?}", 
                         result.event_batches.len(),
                         result.event_batches.iter().map(|b| b.events.len()).sum::<usize>(),
                         duration);
            }
            Response::ReadFilteredResult(Err(e)) => {
                println!("Read failed: {}", e);
            }
            _ => {
                println!("Unexpected response: {:?}", response);
            }
        }

        // Test exists
        let request = Request::Exists {
            aggregate_id: "test_aggregate".to_string(),
        };

        let response = client.send_request(request).await?;
        match response {
            Response::ExistsResult(Ok(exists)) => {
                println!("Aggregate exists: {}", exists);
            }
            Response::ExistsResult(Err(e)) => {
                println!("Exists check failed: {}", e);
            }
            _ => {
                println!("Unexpected response: {:?}", response);
            }
        }

        Ok(())
    })
}