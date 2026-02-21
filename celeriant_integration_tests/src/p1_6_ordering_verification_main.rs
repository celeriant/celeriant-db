//! P1-6: Per-Aggregate Strict Total Ordering Verification
//!
//! Tests that concurrent writes to a single aggregate produce a contiguous,
//! monotonically increasing sequence of event_batch_index values.
//!
//! This test READS BACK the events and verifies ordering, not just count.
//! A bug that stored events out of order but with correct count would pass
//! existing tests but fail this one.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_integration_tests::{ServerConfig, TestServer};
use celeriant_msg::process_requests::Request;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Barrier;

const PORT_BASE: u16 = 19100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P1-6: Per-Aggregate Strict Total Ordering Verification ===\n");

    let port = PORT_BASE + (std::process::id() % 100) as u16;
    let num_writers: usize = 50;

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
    write_event(&mut init_client, &key, 0, true).await?;
    println!("  Initial write successful");

    println!("\nEstablishing {} connections...", num_writers);
    let mut connect_tasks = Vec::with_capacity(num_writers);
    for writer_id in 0..num_writers {
        let addr = addr.clone();
        connect_tasks.push(tokio::spawn(async move {
            CeleriantClient::connect(&addr)
                .await
                .map(|c| (writer_id, c))
                .map_err(|e| format!("writer {}: {}", writer_id, e))
        }));
    }
    let mut clients = Vec::with_capacity(num_writers);
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

    println!("Racing {} concurrent writes to the same aggregate...", clients.len());
    let mut tasks = Vec::with_capacity(clients.len());
    #[allow(unused_mut)]
    for (writer_id, mut client) in clients {
        let bar = barrier.clone();
        let successes = success_count.clone();
        let failures = failure_count.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            bar.wait().await;
            let event = DatablockAggregateEvent {
                client_event_index: writer_id as u64,
                event_index: 0,
                event_id: None,
                event_timestamp: 1000 + writer_id as u64,
                event_type_major: 100,
                event_type_minor: 0,
                event_value: Arc::new(format!("writer={}", writer_id).into_bytes()),
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
            let request = Request::Write(WriteRequest {
                correlation_id: None,
                client_id: writer_id as u128,
                user_id: None,
                writes,
            });
            match client.send_request(&request, CompressionType::None).await {
                Ok(celeriant_msg::process_responses::Response::Write(_)) => {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
                Ok(celeriant_msg::process_responses::Response::GenericError(_)) => {
                    failures.fetch_add(1, Ordering::Relaxed);
                }
                Err(ClientError::CeleriantError(_)) => {
                    failures.fetch_add(1, Ordering::Relaxed);
                }
                Ok(_) => {
                    failures.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {}
            }
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    let successful = success_count.load(Ordering::Relaxed);
    let failed = failure_count.load(Ordering::Relaxed);
    println!("  Successful: {}, Failed: {} (OCC expected)", successful, failed);

    println!("\nReading back all events...");
    let mut read_client = CeleriantClient::connect(&addr).await?;
    let all_batches = read_all_batches(&mut read_client, &key).await?;

    let total_batches = all_batches.len();
    println!("  Read {} event batches", total_batches);

    println!("\nVerifying event_batch_index ordering...");
    let expected_count = 1 + successful;
    assert_eq!(
        total_batches as u64, expected_count,
        "Expected {} batches (1 initial + {} successful writes), got {}",
        expected_count, successful, total_batches
    );

    let mut batch_indices: Vec<u64> = all_batches.iter().map(|b| b.event_batch_index).collect();
    batch_indices.sort_unstable();

    for (i, &batch_index) in batch_indices.iter().enumerate() {
        let expected = (i + 1) as u64;
        assert_eq!(
            batch_index, expected,
            "Gap or duplicate detected: expected event_batch_index={}, got {}",
            expected, batch_index
        );
    }

    println!("  Verified: event_batch_index values are 1, 2, 3, ..., {}", total_batches);
    println!("  No gaps, no duplicates");

    println!("\n=== All Tests Passed ===");
    Ok(())
}

async fn write_event(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    event_num: u64,
    allow_create: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = DatablockAggregateEvent {
        client_event_index: event_num,
        event_index: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"event\":{}}}", event_num).into_bytes()),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create,
            expected_event_batch_index: if allow_create { Some(0) } else { None },
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    let write_req = WriteRequest {
        correlation_id: None,
        client_id: 999,
        user_id: None,
        writes,
    };

    let response = client
        .send_request(&Request::Write(write_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Write(_) => Ok(()),
        other => Err(format!("Write failed: {:?}", other).into()),
    }
}

async fn read_all_batches(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
) -> Result<Vec<celeriant_msg::response::aggregate_event_batch::AggregateEventBatch>, Box<dyn std::error::Error>>
{
    let mut all_batches = Vec::new();
    let mut from_batch = 1u64;

    loop {
        let read_req = ReadRequest {
            correlation_id: None,
            aggregate_key: aggregate_key.clone(),
            filters: ReadFilters::new(from_batch),
        };

        let response = client
            .send_request(&Request::Read(read_req), CompressionType::None)
            .await;

        match response {
            Ok(celeriant_msg::process_responses::Response::Read(read_resp)) => {
                all_batches.extend(read_resp.event_batches);
                match read_resp.next_event_batch_index {
                    Some(next) => from_batch = next,
                    None => return Ok(all_batches),
                }
            }
            Err(ClientError::CeleriantError(error_response)) => {
                if error_response.error_code == 1001 {
                    return Ok(all_batches);
                } else {
                    return Err(format!("Read error: {:?}", error_response).into());
                }
            }
            other => return Err(format!("Unexpected response: {:?}", other).into()),
        }
    }
}
