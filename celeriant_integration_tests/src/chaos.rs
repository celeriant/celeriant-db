//! Chaos Testing with Large Payloads
//!
//! Concurrent read/write testing with variable payload sizes.
//! Creates a temporary data directory and spawns the server automatically.
//!
//! Run with: cargo run --bin chaos_main

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::TestServer;
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::{
        read_filters::ReadFilters,
        requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Barrier;
use tokio::time::Instant;

const NUM_AGGREGATES: usize = 10;
const TEST_DURATION_SECS: u64 = 30;
const CLIENTSIDE_TIMEOUT_S: u64 = 30; // Longer timeout for large payloads

const MIN_PAYLOAD_SIZE: usize = 1;
const MAX_PAYLOAD_SIZE: usize = 5 * 1024 * 1024; // 5MB

// Shared state between reader and writer for an aggregate
struct AggregateState {
    latest_event_batch_index: AtomicU64,
    write_count: AtomicU64,
    read_count: AtomicU64,
    write_errors: AtomicU64,
    read_errors: AtomicU64,
}

impl AggregateState {
    fn new() -> Self {
        Self {
            latest_event_batch_index: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
            read_count: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            read_errors: AtomicU64::new(0),
        }
    }
}

struct ChaosStats {
    aggregate_id: usize,
    write_count: u64,
    read_count: u64,
    write_errors: u64,
    read_errors: u64,
    total_bytes_written: u64,
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Chaos Testing Mode ===\n");

    println!("Starting test server...");
    let server = TestServer::start().await?;
    let server_addr = server.address();
    println!("Server started at {}\n", server_addr);

    println!(
        "Aggregates: {}, Duration: {}s, Payload range: {} bytes - {} bytes",
        NUM_AGGREGATES, TEST_DURATION_SECS, MIN_PAYLOAD_SIZE, MAX_PAYLOAD_SIZE
    );

    let connect_start = Instant::now();

    // Create shared state for each aggregate
    let aggregate_states: Vec<Arc<AggregateState>> = (0..NUM_AGGREGATES)
        .map(|_| Arc::new(AggregateState::new()))
        .collect();

    // Establish connections for writers and readers (2 per aggregate)
    println!("Establishing {} connections...", NUM_AGGREGATES * 2);

    let mut writer_clients = Vec::with_capacity(NUM_AGGREGATES);
    let mut reader_clients = Vec::with_capacity(NUM_AGGREGATES);

    for aggregate_id in 0..NUM_AGGREGATES {
        // Writer connection
        let writer = CeleriantClient::connect_with_timeout(
            server_addr,
            Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
            None,
        )
        .await
        .map_err(|e| format!("Writer {} connection error: {}", aggregate_id, e))?
        .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));

        // Reader connection — responses can contain multiple large event batches,
        // so max_request_size must match the server's --max-response-size (64MB)
        let reader = CeleriantClient::connect_with_timeout(
            server_addr,
            Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
            None,
        )
        .await
        .map_err(|e| format!("Reader {} connection error: {}", aggregate_id, e))?
        .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S))
        .with_max_request_size(67_108_864);

        writer_clients.push((aggregate_id, writer));
        reader_clients.push((aggregate_id, reader));
    }

    let connect_duration = connect_start.elapsed();
    println!(
        "Established {} connections in {:.2}s",
        NUM_AGGREGATES * 2,
        connect_duration.as_secs_f64()
    );

    // Create barrier for synchronization (writers + readers)
    let barrier = Arc::new(Barrier::new(NUM_AGGREGATES * 2));

    println!("Starting chaos test...\n");
    let start_time = Instant::now();

    // Spawn writer tasks
    let mut writer_tasks = Vec::with_capacity(NUM_AGGREGATES);
    let mut reader_tasks = Vec::with_capacity(NUM_AGGREGATES);

    for (aggregate_id, client) in writer_clients {
        let barrier = Arc::clone(&barrier);
        let state = Arc::clone(&aggregate_states[aggregate_id]);
        let task =
            tokio::spawn(async move { run_writer_task(aggregate_id, client, barrier, state).await });
        writer_tasks.push((aggregate_id, task));
    }

    // Spawn reader tasks
    for (aggregate_id, client) in reader_clients {
        let barrier = Arc::clone(&barrier);
        let state = Arc::clone(&aggregate_states[aggregate_id]);
        let task =
            tokio::spawn(async move { run_reader_task(aggregate_id, client, barrier, state).await });
        reader_tasks.push((aggregate_id, task));
    }

    // Wait for all writer tasks and collect stats
    let mut all_stats = Vec::new();
    for (aggregate_id, task) in writer_tasks {
        match task.await {
            Ok(Ok(stats)) => all_stats.push(stats),
            Ok(Err(e)) => eprintln!("Writer {} error: {}", aggregate_id, e),
            Err(e) => eprintln!("Writer {} join error: {}", aggregate_id, e),
        }
    }

    // Wait for all reader tasks
    for (aggregate_id, task) in reader_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("Reader {} error: {}", aggregate_id, e),
            Err(e) => eprintln!("Reader {} join error: {}", aggregate_id, e),
        }
    }

    let total_duration = start_time.elapsed();

    // Print results
    println!("\n=== Chaos Test Results ===");
    println!("Total Duration: {:.2}s\n", total_duration.as_secs_f64());

    let mut total_writes = 0u64;
    let mut total_reads = 0u64;
    let mut total_write_errors = 0u64;
    let mut total_read_errors = 0u64;
    let mut total_bytes = 0u64;

    println!(
        "{:<12} {:>12} {:>12} {:>12} {:>12} {:>15}",
        "Aggregate", "Writes", "Reads", "Write Errs", "Read Errs", "Bytes Written"
    );
    println!("{}", "-".repeat(75));

    for stats in &all_stats {
        println!(
            "{:<12} {:>12} {:>12} {:>12} {:>12} {:>15}",
            stats.aggregate_id,
            stats.write_count,
            stats.read_count,
            stats.write_errors,
            stats.read_errors,
            format_bytes(stats.total_bytes_written)
        );
        total_writes += stats.write_count;
        total_reads += stats.read_count;
        total_write_errors += stats.write_errors;
        total_read_errors += stats.read_errors;
        total_bytes += stats.total_bytes_written;
    }

    println!("{}", "-".repeat(75));
    println!(
        "{:<12} {:>12} {:>12} {:>12} {:>12} {:>15}",
        "TOTAL",
        total_writes,
        total_reads,
        total_write_errors,
        total_read_errors,
        format_bytes(total_bytes)
    );

    println!("\n=== Throughput ===");
    println!(
        "Write throughput: {:.2} req/s",
        total_writes as f64 / total_duration.as_secs_f64()
    );
    println!(
        "Read throughput: {:.2} req/s",
        total_reads as f64 / total_duration.as_secs_f64()
    );
    println!(
        "Data throughput: {}/s",
        format_bytes((total_bytes as f64 / total_duration.as_secs_f64()) as u64)
    );

    if total_write_errors > 0 || total_read_errors > 0 {
        println!("\n  ERRORS DETECTED - Check logs above for details");
    } else {
        println!("\n  No errors detected");
    }

    Ok(())
}

async fn run_writer_task(
    aggregate_id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
    state: Arc<AggregateState>,
) -> Result<ChaosStats, String> {
    let aggregate_key = AggregateKey::new(aggregate_id as u128, 1, aggregate_id as u128);

    let mut rng = StdRng::from_entropy();
    let mut total_bytes_written = 0u64;
    let mut event_index = 0u64;

    // Wait for all tasks to be ready
    barrier.wait().await;

    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        // Generate random payload size (log-distributed for more small payloads)
        let payload_size = generate_random_payload_size(&mut rng);
        let payload = generate_random_payload(&mut rng, payload_size);

        let event = DatablockAggregateEvent {
            client_event_index: event_index,
            event_index: 0,
            event_id: Some(rng.r#gen()),
            event_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            event_type_major: rng.gen_range(1..=10),
            event_type_minor: rng.gen_range(1..=100),
            event_value: Arc::new(payload),
            iv: None,
        };

        let mut writes = HashMap::new();
        writes.insert(
            aggregate_key.clone(),
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
                compression_type_id: 0,
                compression_level: None,
            },
        );

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: aggregate_id as u128,
            user_id: None,
            writes,
        });

        match client
            .send_request(&request, CompressionType::None)
            .await
        {
            Ok(_) => {
                state.write_count.fetch_add(1, Ordering::Relaxed);
                event_index += 1;
                total_bytes_written += payload_size as u64;

                // Update latest batch index (assume sequential)
                state
                    .latest_event_batch_index
                    .fetch_max(event_index, Ordering::Release);
            }
            Err(ClientError::Server(err)) => {
                eprintln!("[Writer {}] Server error: {}", aggregate_id, err);
                state.write_errors.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("[Writer {}] Error: {}", aggregate_id, e);
                state.write_errors.fetch_add(1, Ordering::Relaxed);
                if !matches!(e, ClientError::RequestTimeout) {
                    break;
                }
            }
        }
    }

    Ok(ChaosStats {
        aggregate_id,
        write_count: state.write_count.load(Ordering::Relaxed),
        read_count: state.read_count.load(Ordering::Relaxed),
        write_errors: state.write_errors.load(Ordering::Relaxed),
        read_errors: state.read_errors.load(Ordering::Relaxed),
        total_bytes_written,
    })
}

async fn run_reader_task(
    aggregate_id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
    state: Arc<AggregateState>,
) -> Result<(), String> {
    let aggregate_key = AggregateKey::new(aggregate_id as u128, 1, aggregate_id as u128);

    // Wait for all tasks to be ready
    barrier.wait().await;

    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    // Small delay to let writer get ahead
    tokio::time::sleep(Duration::from_millis(100)).await;

    while Instant::now() < deadline {
        let latest_batch = state.latest_event_batch_index.load(Ordering::Acquire);

        // Only read if there's something to read
        if latest_batch == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }

        // Read last 3 event batches
        let from_batch = latest_batch.saturating_sub(2).max(1);

        let request = ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: aggregate_key.clone(),
            filters: ReadFilters::new(from_batch).to_event_batch_index(latest_batch),
        });

        match client
            .send_request(&request, CompressionType::None)
            .await
        {
            Ok(_response) => {
                state.read_count.fetch_add(1, Ordering::Relaxed);
            }
            Err(ClientError::Server(err)) => {
                // Some errors are expected (e.g., aggregate not found yet)
                if !matches!(err, celeriant_client_tokio::server_error::ServerError::Read { kind: celeriant_client_tokio::server_error::ReadError::AggregateNotExists, .. }) {
                    state.read_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                eprintln!("[Reader {}] Error: {}", aggregate_id, e);
                state.read_errors.fetch_add(1, Ordering::Relaxed);
                if !matches!(e, ClientError::RequestTimeout) {
                    break;
                }
            }
        }

        // Small delay between reads
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    Ok(())
}

fn generate_random_payload_size(rng: &mut impl Rng) -> usize {
    // Use log-uniform distribution to get more small payloads but still test large ones
    let log_min = (MIN_PAYLOAD_SIZE as f64).ln();
    let log_max = (MAX_PAYLOAD_SIZE as f64).ln();
    let log_size = rng.gen_range(log_min..=log_max);
    log_size.exp() as usize
}

fn generate_random_payload(rng: &mut impl Rng, size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; size];
    rng.fill(&mut payload[..]);
    payload
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
