//! Hypothesis 3: Concurrent writes to the same aggregate produce correct event count
//!
//! Many connections write simultaneously to one aggregate. The invariant:
//! stored event count == initial count + total successful write responses.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_integration_tests::{count_events, write_event, ServerConfig, TestServer};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Invariant Test: Concurrent Writes to Same Aggregate ===\n");

    let port = 10800 + (std::process::id() % 100) as u16;
    let num_connections: usize = 50;
    let writes_per_connection: u64 = 20;

    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let _server = TestServer::start_with_config(port, config).await?;

    let key = AggregateKey::new(1, 1, 1);
    let addr = format!("127.0.0.1:{}", port);

    println!("Creating aggregate with initial write...");
    let mut init_client = CeleriantClient::connect(&addr).await?;
    write_event(&mut init_client, &key, 1, true).await?;
    let initial_count = count_events(&mut init_client, &key).await?;
    assert_eq!(initial_count, 1, "Initial write should produce 1 event");
    println!("  Initial count: {}", initial_count);

    println!("\nEstablishing {} connections...", num_connections);
    let mut connect_tasks = Vec::with_capacity(num_connections);
    for conn_id in 0..num_connections {
        let addr = addr.clone();
        connect_tasks.push(tokio::spawn(async move {
            CeleriantClient::connect(&addr)
                .await
                .map(|c| (conn_id, c))
                .map_err(|e| format!("conn {}: {}", conn_id, e))
        }));
    }
    let mut clients = Vec::with_capacity(num_connections);
    for task in connect_tasks {
        match task.await {
            Ok(Ok(pair)) => clients.push(pair),
            _ => {}
        }
    }
    println!("  {} connected", clients.len());

    let barrier = Arc::new(Barrier::new(clients.len()));
    let success_count = Arc::new(AtomicU64::new(0));
    let failure_count = Arc::new(AtomicU64::new(0));

    println!("Writing {} events per connection...", writes_per_connection);
    let mut tasks = Vec::with_capacity(clients.len());
    #[allow(unused_mut)]
    for (conn_id, mut client) in clients {
        let bar = barrier.clone();
        let successes = success_count.clone();
        let failures = failure_count.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            bar.wait().await;
            for seq in 0..writes_per_connection {
                let event = DatablockAggregateEvent {
                    client_event_index: seq,
                    event_index: 0,
                    event_id: None,
                    event_timestamp: 1000 + seq,
                    event_type_major: 100,
                    event_type_minor: 0,
                    event_value: Arc::new(
                        format!("conn={},seq={}", conn_id, seq).into_bytes(),
                    ),
                    iv: None,
                };
                let mut writes = HashMap::new();
                writes.insert(
                    key.clone(),
                    SingleAggregateWrite {
                        events: vec![event],
                        allow_create: false,
                        expected_event_batch_index: None,
                        enforce_client_idempotency: false,
                        compression_type: CompressionType::None,
                    },
                );
                let request = ClientRequest::Write(WriteRequest {
                    correlation_id: None,
                    client_id: conn_id as u128,
                    user_id: None,
                    writes,
                });
                match client.send_request(&request, CompressionType::None).await {
                    Ok(ClientResponse::Write(_)) => {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(ClientResponse::GenericError(_)) => {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(ClientError::CeleriantError(_)) => {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(_) => {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => break,
                }
            }
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    let successful = success_count.load(Ordering::Relaxed);
    let failed = failure_count.load(Ordering::Relaxed);
    println!("  Successful: {}, Failed: {}", successful, failed);

    let mut read_client = CeleriantClient::connect(&addr).await?;
    let final_count = count_events(&mut read_client, &key).await?;
    let expected = initial_count as u64 + successful;

    println!("\n  Expected: {} (initial={} + successful={})", expected, initial_count, successful);
    println!("  Actual:   {}", final_count);

    assert_eq!(
        final_count as u64, expected,
        "Event count mismatch: stored={}, expected={}",
        final_count, expected
    );

    println!("\n=== All Tests Passed ===");
    Ok(())
}
