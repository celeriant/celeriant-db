use std::{collections::HashMap, sync::Arc, time::Duration};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_msg::{process_requests::Request, request::requests::SingleAggregateWrite};
use celeriant_msg::request::requests::WriteRequest;
use celeriant_wal::{
    aggregate_key::AggregateKey,
    compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use tokio::sync::Barrier;
use tokio::time::Instant;

const NUM_CONNECTIONS: usize = 12*1024; // 28k max source port limit ~25000;
const TEST_DURATION_SECS: u64 = 30;
const NUM_AGGREGATES: usize = NUM_CONNECTIONS / 64;
const USE_MICRO_PAYLOAD: bool = true;
const SERVER_ADDR: &str = "0.0.0.0:10000";
const CLIENTSIDE_TIMEOUT_S: u64 = 5;

struct TaskStats {
    request_count: u64,
    latencies_us: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Establishing {} connections...",
        NUM_CONNECTIONS
    );

    let connect_start = Instant::now();

    // Establish all connections first
    let mut connection_tasks = Vec::with_capacity(NUM_CONNECTIONS);
    for connection_id in 0..NUM_CONNECTIONS {
        let task = tokio::spawn(async move {
            let client = CeleriantClient::connect_with_timeout(
                SERVER_ADDR,
                Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
            )
            .await
            .map_err(|e| format!("Connection {} error: {}", connection_id, e))?
            .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));
            Ok::<_, String>((connection_id, client))
        });
        connection_tasks.push(task);
    }

    // Collect all established connections
    let mut clients = Vec::with_capacity(NUM_CONNECTIONS);
    let mut failed_connections = 0;
    for task in connection_tasks {
        match task.await {
            Ok(Ok((connection_id, client))) => {
                clients.push((connection_id, client));
            }
            Ok(Err(e)) => {
                eprintln!("{}", e);
                failed_connections += 1;
            }
            Err(e) => {
                eprintln!("Join error: {}", e);
                failed_connections += 1;
            }
        }
    }

    let connect_duration = connect_start.elapsed();
    println!(
        "Established {} connections in {:.2}s ({} failed)",
        clients.len(),
        connect_duration.as_secs_f64(),
        failed_connections
    );

    if clients.is_empty() {
        return Err("No connections established".into());
    }

    // Create a barrier to synchronize all tasks to start at the same time
    let barrier = Arc::new(Barrier::new(clients.len()));

    println!(
        "Starting benchmark with {} concurrent connections for {} seconds...",
        clients.len(),
        TEST_DURATION_SECS
    );

    let start_time = Instant::now();

    // Spawn benchmark tasks with pre-established connections
    let mut tasks = Vec::with_capacity(clients.len());
    for (connection_id, client) in clients {
        let barrier = Arc::clone(&barrier);
        let task = tokio::spawn(async move {
            run_connection_benchmark(connection_id, client, barrier).await
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut all_stats = Vec::with_capacity(NUM_CONNECTIONS);
    for task in tasks {
        match task.await {
            Ok(Ok(stats)) => all_stats.push(stats),
            Ok(Err(e)) => eprintln!("Task error: {}", e),
            Err(e) => eprintln!("Join error: {}", e),
        }
    }

    let total_duration = start_time.elapsed();

    // Aggregate results
    let total_requests: u64 = all_stats.iter().map(|s| s.request_count).sum();
    let mut all_latencies: Vec<u64> = all_stats.into_iter().flat_map(|s| s.latencies_us).collect();

    all_latencies.sort_unstable();

    // Calculate statistics
    let throughput = total_requests as f64 / total_duration.as_secs_f64();

    println!("\n=== Benchmark Results ===");
    println!("Total Duration: {:.2}s", total_duration.as_secs_f64());
    println!("Total Requests: {}", total_requests);
    println!("Throughput: {:.2} req/s", throughput);

    if !all_latencies.is_empty() {
        let avg_latency =
            all_latencies.iter().sum::<u64>() as f64 / all_latencies.len() as f64;
        let p50 = all_latencies[all_latencies.len() * 50 / 100];
        let p95 = all_latencies[all_latencies.len() * 95 / 100];
        let p99 = all_latencies[all_latencies.len() * 99 / 100];
        let p999 = all_latencies[all_latencies.len() * 999 / 1000];
        let max_latency = all_latencies[all_latencies.len() - 1];
        let min_latency = all_latencies[0];

        println!("\n=== Latency Statistics (milliseconds) ===");
        println!("Average: {:.2}ms", avg_latency);
        println!("P50: {}ms", p50);
        println!("P95: {}ms", p95);
        println!("P99: {}ms", p99);
        println!("P99.9: {}ms", p999);
        println!("Max: {}ms", max_latency);
        println!("Min: {}ms", min_latency);
    }

    Ok(())
}

async fn run_connection_benchmark(
    connection_id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
) -> Result<TaskStats, String> {
    let mut request_count = 0u64;
    let mut latencies = Vec::new();

    // Wait for all connections to be ready before starting the benchmark
    barrier.wait().await;

    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    // Send requests until deadline
    while Instant::now() < deadline {
        let prefix = format!("[connection-{}-event-{}] ", connection_id, request_count);

        let event_1 = DatablockAggregateEvent {
            client_event_index: 3,
            event_index: 0,
            event_id: Some(1234567890),
            event_timestamp: 0,
            event_type_major: 2,
            event_type_minor: 3,
            event_value: std::sync::Arc::new(format!("{}Hello World!", prefix).into_bytes()),
            iv: None,
        };

        let event_2 = DatablockAggregateEvent {
            client_event_index: 3,
            event_index: 0,
            event_id: Some(1234567890),
            event_timestamp: 0,
            event_type_major: 2,
            event_type_minor: 3,
            event_value: std::sync::Arc::new(format!("{}She should have died hereafter;
            There would have been a time for such a word.
            Tomorrow, and tomorrow, and tomorrow,
            Creeps in this petty pace from day to day
            To the last syllable of recorded time,
            And all our yesterdays have lighted fools
            The way to dusty death. Out, out, brief candle!
            Life's but a walking shadow, a poor player
            That struts and frets his hour upon the stage
            And then is heard no more. It is a tale
            Told by an idiot, full of sound and fury, Signifying nothing. ", prefix).into_bytes()),
            iv: None,
        };

        let mut writes = HashMap::new();
        writes.insert(
            AggregateKey::new(
                1,
                1,
                (connection_id % NUM_AGGREGATES) as u128,
            ),
            SingleAggregateWrite {
                events: if USE_MICRO_PAYLOAD {
                    vec![event_1]
                } else {
                    vec![event_1, event_2]
                },
                allow_create: true,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
            },
        );

        let request = Request::Write(WriteRequest {
            correlation_id: None,
            client_id: connection_id as u128,
            user_id: None,
            writes,
        });

        let req_start = Instant::now();

        match client.send_request(&request, CompressionType::None).await {
            Ok(_) => {
                let latency_us = req_start.elapsed().as_millis() as u64;
                latencies.push(latency_us);
                request_count += 1;
            }
            Err(ClientError::CeleriantError(err_resp)) => {
                eprintln!(
                    "Connection {} server error: {} ({})",
                    connection_id, err_resp.error_message, err_resp.error_code
                );
            }
            Err(e) => {
                match e {
                    ClientError::RequestTimeout => eprintln!("Connection {} Timeout: {}", connection_id, e),
                    _ => {
                        eprintln!("Connection {} Error: {}", connection_id, e);
                        break; // Exit on connection errors
                    },
                }
            }
        }
    }

    Ok(TaskStats {
        request_count,
        latencies_us: latencies,
    })
}