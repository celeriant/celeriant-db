use std::{sync::Arc, time::Duration};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_msg::{process_requests::Request};
use celeriant_msg::request::requests::{ExistsRequest};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    compression_type::CompressionType,
};
use tokio::sync::Barrier;
use tokio::time::Instant;

const NUM_CONNECTIONS: usize = 12*1024; // 28k max source port limit ~25000;
const TEST_DURATION_SECS: u64 = 10;
const NUM_AGGREGATES: usize = 250000;
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
        let aggregate_id = (connection_id + request_count as usize) % NUM_AGGREGATES;
        let aggregate_key = AggregateKey::new(
            2, //INTENTIONALLY DIFFERENT FROM batch_main writes org
            1,
            aggregate_id as u128,
        );
        
        let request = Request::Exists(ExistsRequest {
            correlation_id: None,
            aggregate_key
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