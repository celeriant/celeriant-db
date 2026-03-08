//! Read & List Benchmark
//!
//! Measures read and list performance with large WAL sizes and under load.
//! Standalone plaintext server, single shard.
//!
//! Phases:
//!   1. Setup: Write N aggregates × M batches to build a large WAL
//!   2. Read throughput: Concurrent reads of random aggregates
//!   3. List throughput: Concurrent list_aggregates with full pagination
//!   4. Mixed read+write: Reads while writes are ongoing
//!   5. List memory pressure: Many concurrent list operations
//!
//! Run with: cargo run --release --bin read_list_benchmark_main

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_error::ClientError;
use celeriant_crypto::{generate_api_key, hash_api_key, Crypto};
use celeriant_integration_tests::{ServerConfig, TestServer};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{
    ListAggregatesRequest, ReadRequest, SingleAggregateWrite, WriteRequest,
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Barrier;
use tokio::time::Instant;

// --- Configuration ---
const NUM_AGGREGATES: usize = 5_000;
const BATCHES_PER_AGGREGATE: usize = 10;
const WRITE_CONNECTIONS: usize = 100;
const READ_CONNECTIONS: usize = 200;
const LIST_CONNECTIONS: usize = 50;
const LIST_PRESSURE_CONNECTIONS: usize = 200;
const TEST_DURATION_SECS: u64 = 15;
const MIXED_WRITER_CONNECTIONS: usize = 50;
const MIXED_READER_CONNECTIONS: usize = 150;
const CLIENTSIDE_TIMEOUT_S: u64 = 30;

// --- API Key Setup (same pattern as batch_standalone_cleartext_main) ---

struct ApiKeySet {
    primary_rw: [u8; 32],
    primary_rw_hash: [u8; 32],
    secondary_rw_hash: [u8; 32],
    primary_ro_hash: [u8; 32],
    secondary_ro_hash: [u8; 32],
}

fn generate_key_set() -> ApiKeySet {
    let primary_rw = generate_api_key();
    ApiKeySet {
        primary_rw_hash: hash_api_key(&primary_rw),
        secondary_rw_hash: hash_api_key(&generate_api_key()),
        primary_ro_hash: hash_api_key(&generate_api_key()),
        secondary_ro_hash: hash_api_key(&generate_api_key()),
        primary_rw,
    }
}

fn create_api_keys_file(data_root: &Path, keys: &ApiKeySet) -> std::io::Result<()> {
    let content = format!(
        r#"[keys]
primary_rw = "{}"
secondary_rw = "{}"
primary_ro = "{}"
secondary_ro = "{}"
"#,
        hex::encode(keys.primary_rw_hash),
        hex::encode(keys.secondary_rw_hash),
        hex::encode(keys.primary_ro_hash),
        hex::encode(keys.secondary_ro_hash),
    );
    fs::write(data_root.join("api_keys.toml"), content)
}

// --- Stats ---

struct TaskStats {
    request_count: u64,
    error_count: u64,
    latencies_us: Vec<u64>,
}

#[derive(Debug, Clone)]
struct BenchmarkResult {
    label: String,
    num_connections: usize,
    total_requests: u64,
    total_errors: u64,
    throughput: f64,
    avg_latency_ms: f64,
    p50_ms: u64,
    p95_ms: u64,
    p99_ms: u64,
    p999_ms: u64,
}

fn compute_result(
    label: &str,
    num_connections: usize,
    all_stats: Vec<TaskStats>,
    duration: Duration,
) -> BenchmarkResult {
    let total_requests: u64 = all_stats.iter().map(|s| s.request_count).sum();
    let total_errors: u64 = all_stats.iter().map(|s| s.error_count).sum();
    let mut all_latencies: Vec<u64> = all_stats
        .into_iter()
        .flat_map(|s| s.latencies_us)
        .collect();
    all_latencies.sort_unstable();

    let throughput = total_requests as f64 / duration.as_secs_f64();

    let (avg, p50, p95, p99, p999) = if !all_latencies.is_empty() {
        let n = all_latencies.len();
        let avg = all_latencies.iter().sum::<u64>() as f64 / n as f64;
        (
            avg,
            all_latencies[n * 50 / 100],
            all_latencies[n * 95 / 100],
            all_latencies[n * 99 / 100],
            all_latencies[n * 999 / 1000],
        )
    } else {
        (0.0, 0, 0, 0, 0)
    };

    BenchmarkResult {
        label: label.to_string(),
        num_connections,
        total_requests,
        total_errors,
        throughput,
        avg_latency_ms: avg,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        p999_ms: p999,
    }
}

fn print_result(r: &BenchmarkResult) {
    println!(
        "  Throughput: {:.0} req/s | Avg: {:.1}ms | P50: {}ms | P95: {}ms | P99: {}ms | P99.9: {}ms | Errors: {}",
        r.throughput, r.avg_latency_ms, r.p50_ms, r.p95_ms, r.p99_ms, r.p999_ms, r.total_errors
    );
}

// --- Connection helpers ---

async fn connect(
    addr: &str,
    identity: &ClientIdentityConfig,
) -> Result<(CeleriantClient, u128), String> {
    let mut client = CeleriantClient::connect_with_timeout(
        addr,
        Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
        None,
    )
    .await
    .map_err(|e| format!("Connect error: {}", e))?
    .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));

    let verified_client_id = client
        .identify(identity)
        .await
        .map_err(|e| format!("Identify error: {}", e))?
        .unwrap_or(0);

    Ok((client, verified_client_id))
}

async fn connect_pool(
    addr: &str,
    identity: &ClientIdentityConfig,
    count: usize,
) -> (Vec<CeleriantClient>, u128) {
    let mut tasks = Vec::with_capacity(count);
    for _ in 0..count {
        let addr = addr.to_string();
        let identity = identity.clone();
        tasks.push(tokio::spawn(
            async move { connect(&addr, &identity).await },
        ));
    }

    let mut clients = Vec::with_capacity(count);
    let mut verified_client_id = 0u128;
    let mut failed = 0;
    for task in tasks {
        match task.await {
            Ok(Ok((c, cid))) => {
                verified_client_id = cid;
                clients.push(c);
            }
            _ => failed += 1,
        }
    }
    if failed > 0 {
        println!("  ({} connections failed)", failed);
    }
    println!("  Established {} connections", clients.len());
    (clients, verified_client_id)
}

// --- Write helper ---

fn make_write_request(aggregate_id: u128, client_id: u128) -> ClientRequest {
    let event = DatablockAggregateEvent {
        client_event_index: 0,
        event_index: 0,
        event_id: Some(1),
        event_timestamp: 0,
        event_type_major: 1,
        event_type_minor: 1,
        event_value: Arc::new(b"benchmark-payload-data-0123456789".to_vec()),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, aggregate_id),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_event_batch_index: None,
            enforce_client_idempotency: false,
            compression_type_id: 0,
            compression_level: None,
        },
    );

    ClientRequest::Write(WriteRequest {
        correlation_id: None,
        client_id,
        user_id: None,
        writes,
    })
}

// --- Main ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Read & List Benchmark ===\n");

    let base_port = 10100 + (std::process::id() % 100) as u16;
    let api_keys = generate_key_set();
    let keypair = Crypto::generate_keypair(None)?;
    let api_key_b64 = base64::engine::general_purpose::STANDARD.encode(&api_keys.primary_rw);
    let identity_config = ClientIdentityConfig {
        public_key: Some(keypair.public_key_base64.clone()),
        private_key: Some(keypair.private_key_base64.clone()),
        api_key: Some(api_key_b64),
    };

    let config = ServerConfig {
        log_level: "warn".to_string(),
        standalone: true,
        require_client_identity: true,
        insecure_allow_plaintext_auth: true,
        list_page_size: 500,
        list_max_duration_ms: 5000,
        ..Default::default()
    };
    let temp_dir = tempfile::TempDir::new()?;
    create_api_keys_file(temp_dir.path(), &api_keys)?;
    let server = TestServer::start_with_existing_dir(
        base_port,
        config,
        "read-list-bench".to_string(),
        temp_dir,
    )
    .await?;
    let addr = server.address().to_string();

    // ========================================
    // Phase 1: Setup — populate WAL
    // ========================================
    println!("\n--- Phase 1: Populating WAL ({} aggregates × {} batches) ---",
        NUM_AGGREGATES, BATCHES_PER_AGGREGATE);

    let setup_start = Instant::now();
    let writes_done = Arc::new(AtomicU64::new(0));
    let total_writes = (NUM_AGGREGATES * BATCHES_PER_AGGREGATE) as u64;

    let (mut setup_clients, verified_client_id) = connect_pool(&addr, &identity_config, WRITE_CONNECTIONS).await;

    let mut setup_tasks = Vec::new();
    let aggregates_per_conn = NUM_AGGREGATES / setup_clients.len();
    let mut agg_offset = 0usize;

    for mut client in setup_clients.drain(..) {
        let count = aggregates_per_conn;
        let offset = agg_offset;
        let writes_done = Arc::clone(&writes_done);
        agg_offset += count;

        setup_tasks.push(tokio::spawn(async move {
            for i in 0..count {
                let aggregate_id = (offset + i) as u128;
                for _batch in 0..BATCHES_PER_AGGREGATE {
                    let req = make_write_request(aggregate_id, verified_client_id);
                    if let Err(e) = client.send_request(&req, CompressionType::None).await {
                        eprintln!("  Setup write error: {}", e);
                        return;
                    }
                    writes_done.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // Progress reporter
    let writes_done_monitor = Arc::clone(&writes_done);
    let progress = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let done = writes_done_monitor.load(Ordering::Relaxed);
            println!("  Setup progress: {}/{} writes ({:.0}%)",
                done, total_writes, done as f64 / total_writes as f64 * 100.0);
            if done >= total_writes {
                break;
            }
        }
    });

    for task in setup_tasks {
        let _ = task.await;
    }
    progress.abort();

    let setup_duration = setup_start.elapsed();
    let actual_writes = writes_done.load(Ordering::Relaxed);
    println!("  Setup complete: {} writes in {:.1}s ({:.0} writes/s)",
        actual_writes, setup_duration.as_secs_f64(),
        actual_writes as f64 / setup_duration.as_secs_f64());

    tokio::time::sleep(Duration::from_secs(1)).await;

    // ========================================
    // Phase 2: Read throughput
    // ========================================
    println!("\n--- Phase 2: Read Throughput ({} connections, {}s) ---",
        READ_CONNECTIONS, TEST_DURATION_SECS);

    let read_result = run_read_benchmark(
        &addr, &identity_config, READ_CONNECTIONS, "Read throughput",
    ).await?;
    print_result(&read_result);

    tokio::time::sleep(Duration::from_secs(1)).await;

    // ========================================
    // Phase 3: List throughput
    // ========================================
    println!("\n--- Phase 3: List Throughput ({} connections, {}s) ---",
        LIST_CONNECTIONS, TEST_DURATION_SECS);

    let list_result = run_list_benchmark(
        &addr, &identity_config, LIST_CONNECTIONS, "List throughput",
    ).await?;
    print_result(&list_result);

    tokio::time::sleep(Duration::from_secs(1)).await;

    // ========================================
    // Phase 4: Mixed read + write
    // ========================================
    println!("\n--- Phase 4: Mixed Read+Write ({} readers + {} writers, {}s) ---",
        MIXED_READER_CONNECTIONS, MIXED_WRITER_CONNECTIONS, TEST_DURATION_SECS);

    let mixed_results = run_mixed_benchmark(
        &addr, &identity_config,
    ).await?;
    for r in &mixed_results {
        println!("  [{}]", r.label);
        print_result(r);
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    // ========================================
    // Phase 5: List memory pressure
    // ========================================
    println!("\n--- Phase 5: List Memory Pressure ({} concurrent connections, {}s) ---",
        LIST_PRESSURE_CONNECTIONS, TEST_DURATION_SECS);

    let pressure_result = run_list_benchmark(
        &addr, &identity_config, LIST_PRESSURE_CONNECTIONS, "List pressure",
    ).await?;
    print_result(&pressure_result);

    drop(server);

    // ========================================
    // Summary
    // ========================================
    println!("\n\n{}", "=".repeat(100));
    println!("  RESULTS");
    println!("{}\n", "=".repeat(100));

    println!(
        "{:<25} {:>6} {:>14} {:>10} {:>8} {:>8} {:>8} {:>8}",
        "Scenario", "Conns", "Throughput", "Avg (ms)", "P50", "P95", "P99", "Errors"
    );
    println!("{}", "-".repeat(100));

    let all_results: Vec<&BenchmarkResult> = [
        &read_result, &list_result, &pressure_result,
    ].into_iter().chain(mixed_results.iter()).collect();

    for r in &all_results {
        println!(
            "{:<25} {:>6} {:>11.0} /s {:>10.1} {:>8} {:>8} {:>8} {:>8}",
            r.label, r.num_connections, r.throughput, r.avg_latency_ms,
            r.p50_ms, r.p95_ms, r.p99_ms, r.total_errors,
        );
    }

    Ok(())
}

// --- Scenario: Read benchmark ---

async fn run_read_benchmark(
    addr: &str,
    identity: &ClientIdentityConfig,
    num_connections: usize,
    label: &str,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let (clients, _) = connect_pool(addr, identity, num_connections).await;
    let actual = clients.len();
    let barrier = Arc::new(Barrier::new(actual));

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(actual);

    for (id, client) in clients.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            read_worker(id, client, barrier).await
        }));
    }

    let stats = collect_stats(tasks).await;
    Ok(compute_result(label, actual, stats, start.elapsed()))
}

async fn read_worker(
    id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
) -> Result<TaskStats, String> {
    let mut count = 0u64;
    let mut errors = 0u64;
    let mut latencies = Vec::new();

    barrier.wait().await;
    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        let aggregate_id = ((id as u64 * 7919 + count * 6271) % NUM_AGGREGATES as u64) as u128;
        let req = ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(1, 1, aggregate_id),
            filters: ReadFilters::new(1),
        });

        let t = Instant::now();
        match client.send_request(&req, CompressionType::None).await {
            Ok(ClientResponse::Read(_)) => {
                latencies.push(t.elapsed().as_millis() as u64);
                count += 1;
            }
            Ok(_) => errors += 1,
            Err(ClientError::CeleriantError(_)) => errors += 1,
            Err(ClientError::RequestTimeout) => errors += 1,
            Err(_) => {
                errors += 1;
                break;
            }
        }
    }

    Ok(TaskStats { request_count: count, error_count: errors, latencies_us: latencies })
}

// --- Scenario: List benchmark ---

async fn run_list_benchmark(
    addr: &str,
    identity: &ClientIdentityConfig,
    num_connections: usize,
    label: &str,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let (clients, _) = connect_pool(addr, identity, num_connections).await;
    let actual = clients.len();
    let barrier = Arc::new(Barrier::new(actual));

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(actual);

    for client in clients.into_iter() {
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            list_worker(client, barrier).await
        }));
    }

    let stats = collect_stats(tasks).await;
    Ok(compute_result(label, actual, stats, start.elapsed()))
}

async fn list_worker(
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
) -> Result<TaskStats, String> {
    let mut count = 0u64;
    let mut errors = 0u64;
    let mut latencies = Vec::new();

    barrier.wait().await;
    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        // Full pagination cycle: fetch all pages
        let mut cursor: Option<u64> = None;
        loop {
            if Instant::now() >= deadline {
                break;
            }

            let req = ClientRequest::ListAggregates(ListAggregatesRequest {
                correlation_id: None,
                shard_id: 0,
                org_id: Some(1),
                aggregate_type_id: Some(1),
                cursor,
            });

            let t = Instant::now();
            match client.send_request(&req, CompressionType::None).await {
                Ok(ClientResponse::ListAggregates(r)) => {
                    latencies.push(t.elapsed().as_millis() as u64);
                    count += 1;
                    cursor = r.next_cursor;
                    if cursor.is_none() {
                        break; // End of pagination
                    }
                }
                Ok(_) => {
                    errors += 1;
                    break;
                }
                Err(ClientError::CeleriantError(_)) => {
                    errors += 1;
                    break;
                }
                Err(ClientError::RequestTimeout) => {
                    errors += 1;
                    // Retry from start on timeout
                    cursor = None;
                }
                Err(_) => {
                    errors += 1;
                    break;
                }
            }
        }
    }

    Ok(TaskStats { request_count: count, error_count: errors, latencies_us: latencies })
}

// --- Scenario: Mixed read + write ---

async fn run_mixed_benchmark(
    addr: &str,
    identity: &ClientIdentityConfig,
) -> Result<Vec<BenchmarkResult>, Box<dyn std::error::Error>> {
    let total = MIXED_READER_CONNECTIONS + MIXED_WRITER_CONNECTIONS;
    let (all_clients, verified_client_id) = connect_pool(addr, identity, total).await;
    let actual = all_clients.len();

    let writer_count = MIXED_WRITER_CONNECTIONS.min(actual);
    let reader_count = actual - writer_count;

    let barrier = Arc::new(Barrier::new(actual));
    let start = Instant::now();

    let mut writer_tasks = Vec::new();
    let mut reader_tasks = Vec::new();

    for (i, client) in all_clients.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        if i < writer_count {
            writer_tasks.push(tokio::spawn(async move {
                write_worker(i, client, barrier, verified_client_id).await
            }));
        } else {
            reader_tasks.push(tokio::spawn(async move {
                read_worker(i, client, barrier).await
            }));
        }
    }

    let writer_stats = collect_stats(writer_tasks).await;
    let reader_stats = collect_stats(reader_tasks).await;
    let elapsed = start.elapsed();

    Ok(vec![
        compute_result("Mixed-read", reader_count, reader_stats, elapsed),
        compute_result("Mixed-write", writer_count, writer_stats, elapsed),
    ])
}

async fn write_worker(
    id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
    verified_client_id: u128,
) -> Result<TaskStats, String> {
    let mut count = 0u64;
    let mut errors = 0u64;
    let mut latencies = Vec::new();

    barrier.wait().await;
    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        let aggregate_id = ((id as u64 * 7919 + count * 6271) % NUM_AGGREGATES as u64) as u128;
        let req = make_write_request(aggregate_id, verified_client_id);

        let t = Instant::now();
        match client.send_request(&req, CompressionType::None).await {
            Ok(_) => {
                latencies.push(t.elapsed().as_millis() as u64);
                count += 1;
            }
            Err(ClientError::CeleriantError(e)) => {
                eprintln!("  Writer {} error: {} ({})", id, e.error_message, e.error_code);
                errors += 1;
            }
            Err(ClientError::RequestTimeout) => errors += 1,
            Err(_) => {
                errors += 1;
                break;
            }
        }
    }

    Ok(TaskStats { request_count: count, error_count: errors, latencies_us: latencies })
}

// --- Helpers ---

async fn collect_stats(
    tasks: Vec<tokio::task::JoinHandle<Result<TaskStats, String>>>,
) -> Vec<TaskStats> {
    let mut stats = Vec::new();
    for task in tasks {
        if let Ok(Ok(s)) = task.await {
            stats.push(s);
        }
    }
    stats
}
