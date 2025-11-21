use std::time::Duration;

use eventplanedb_client::{ClientError, EventPlaneDBClient};
use eventplanedb_structures::{
    compression_type::CompressionType, event_item::EventItem, request::{Request, WriteRequest}
};
use tokio::time::Instant;

const NUM_CONNECTIONS: usize = 8000;
const TEST_DURATION_SECS: u64 = 30;

struct TaskStats {
    request_count: u64,
    latencies_us: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting benchmark with {} concurrent connections for {} seconds...", 
             NUM_CONNECTIONS, TEST_DURATION_SECS);
    
    let start_time = Instant::now();
    
    // Spawn all tasks
    let mut tasks = Vec::with_capacity(NUM_CONNECTIONS);
    
    for connection_id in 0..NUM_CONNECTIONS {
        let task = tokio::spawn(async move {
            run_connection_benchmark(connection_id).await
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
    let mut all_latencies: Vec<u64> = all_stats
        .into_iter()
        .flat_map(|s| s.latencies_us)
        .collect();
    
    all_latencies.sort_unstable();
    
    // Calculate statistics
    let throughput = total_requests as f64 / total_duration.as_secs_f64();
    
    println!("\n=== Benchmark Results ===");
    println!("Total Duration: {:.2}s", total_duration.as_secs_f64());
    println!("Total Requests: {}", total_requests);
    println!("Throughput: {:.2} req/s", throughput);
    
    if !all_latencies.is_empty() {
        let avg_latency = all_latencies.iter().sum::<u64>() as f64 / all_latencies.len() as f64;
        let p50 = all_latencies[all_latencies.len() * 50 / 100];
        let p95 = all_latencies[all_latencies.len() * 95 / 100];
        let p99 = all_latencies[all_latencies.len() * 99 / 100];
        let p999 = all_latencies[all_latencies.len() * 999 / 1000];
        let max_latency = all_latencies[all_latencies.len() - 1];
        
        println!("\n=== Latency Statistics (microseconds) ===");
        println!("Average: {:.2}μs", avg_latency);
        println!("P50: {}μs", p50);
        println!("P95: {}μs", p95);
        println!("P99: {}μs", p99);
        println!("P99.9: {}μs", p999);
        println!("Max: {}μs", max_latency);
    }
    
    Ok(())
}

async fn run_connection_benchmark(connection_id: usize) -> Result<TaskStats, String> {
    // Connect to the server
    let mut client = EventPlaneDBClient::connect("127.0.0.1:10000")
        .await
        .map_err(|e| format!("Connection error: {}", e))?
        .with_timeout(Duration::from_secs(5));
    
    // Prepare the write request (reused for all requests)
    let request = Request::Write(WriteRequest { 
        correlation_id: Some(connection_id as u128), 
        org_id: 1, 
        aggregate_type_id: 1, 
        aggregate_id: (connection_id % 16) as u128, // Spread across 1000 aggregates
        client_id: connection_id as u128, 
        user_id: None, 
        events: vec![
            EventItem::new(
                0,                           // client_event_index
                0,                           // event_index (server will assign)
                None,                        // event_id
                0,                           // client timestamp
                1,                           // event_type_major
                0,                           // event_type_minor
                b"Benchmark data".to_vec(),
            ),
        ], 
        allow_create: true, 
        expected_event_batch_index: None, 
        enforce_client_idempotency: false, 
        durable_write_with_delay_us: Some(20), 
        compression_type: CompressionType::None 
    });
    
    let mut request_count = 0u64;
    let mut latencies = Vec::new();
    
    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);
    
    // Send requests until deadline
    while Instant::now() < deadline {
        let req_start = Instant::now();
        
        match client.send_request(&request, CompressionType::Zstd { level: 6 }).await {
            Ok(_) => {
                let latency_us = req_start.elapsed().as_micros() as u64;
                latencies.push(latency_us);
                request_count += 1;
            }
            Err(ClientError::EventPlaneDBError(e)) => {
                eprintln!("Connection {} DB Error: {:?}", connection_id, e);
            }
            Err(e) => {
                eprintln!("Connection {} Error: {}", connection_id, e);
                break; // Exit on connection errors
            }
        }
    }
    
    Ok(TaskStats {
        request_count,
        latencies_us: latencies,
    })
}