//! Read & List Benchmark
//!
//! Measures read, list_aggregates, list_orgs, and list_aggregate_types performance
//! with high-cardinality data across multiple log segment rotations.
//!
//! 300K+ unique aggregates across 50 orgs × 20 types × 300 aggregates.
//! 128MB segment files force multiple rotations during setup.
//! Mix of miniblock-inline (<512B) and datablock (>512B) event payloads.
//!
//! Phases:
//!   1. Setup: Populate WAL with 300K+ aggregates via batched multi-aggregate writes
//!   2. Read throughput: Concurrent reads of random aggregates
//!   3. list_aggregates throughput: Concurrent listing with org/type filters
//!   4. list_orgs throughput: Concurrent org listing
//!   5. list_aggregate_types throughput: Concurrent type listing
//!   6. Mixed read+write: Reads while large-payload writes force more rotations
//!   7. List memory pressure: Many concurrent list operations

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_error::ClientError;
use celeriant_crypto::{generate_api_key, hash_api_key, Crypto};
use crate::{bench_tuning, ServerConfig, TestServer};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{
    ListAggregatesRequest, ListAggregateTypesRequest, ListOrgsRequest,
    ReadRequest, SingleAggregateWrite, WriteRequest,
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Barrier;
use tokio::time::Instant;

// --- Configuration ---
const NUM_ORGS: usize = 50;
const TYPES_PER_ORG: usize = 20;
const AGGREGATES_PER_TYPE: usize = 300;
const TOTAL_AGGREGATES: usize = NUM_ORGS * TYPES_PER_ORG * AGGREGATES_PER_TYPE; // 300,000

// Multi-event writes: each aggregate gets multiple events per write.
// Combined with large payloads, this produces enough data to force segment rotations.
const EVENTS_PER_WRITE: usize = 10;

const WRITE_CONNECTIONS: usize = 200;
const READ_CONNECTIONS: usize = 200;
const LIST_CONNECTIONS: usize = 100;
const LIST_ORGS_CONNECTIONS: usize = 50;
const LIST_TYPES_CONNECTIONS: usize = 50;
const LIST_PRESSURE_CONNECTIONS: usize = 200;
const TEST_DURATION_SECS: u64 = 15;
const MIXED_WRITER_CONNECTIONS: usize = 50;
const MIXED_READER_CONNECTIONS: usize = 150;
const CLIENTSIDE_TIMEOUT_S: u64 = 60;

// Payload sizes: mix of inline (<512B) and datablock (>512B)
const SMALL_PAYLOAD_SIZE: usize = 64;
const LARGE_PAYLOAD_SIZE: usize = 2048;

// --- API Key Setup ---

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
        "  Requests: {} | Throughput: {:.0} req/s | Avg: {:.1}ms | P50: {}ms | P95: {}ms | P99: {}ms | P99.9: {}ms | Errors: {}",
        r.total_requests, r.throughput, r.avg_latency_ms, r.p50_ms, r.p95_ms, r.p99_ms, r.p999_ms, r.total_errors
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

// --- Write helpers ---

/// Map a flat aggregate index (0..TOTAL_AGGREGATES) to (org_id, type_id, agg_id).
fn index_to_key(flat_index: usize) -> (u128, u128, u128) {
    let agg_within_type = flat_index % AGGREGATES_PER_TYPE;
    let type_index = (flat_index / AGGREGATES_PER_TYPE) % TYPES_PER_ORG;
    let org_index = flat_index / (AGGREGATES_PER_TYPE * TYPES_PER_ORG);
    (
        (org_index + 1) as u128,
        (type_index + 1) as u128,
        agg_within_type as u128,
    )
}

fn make_event(payload_size: usize) -> DatablockAggregateEvent {
    // Unique seed per event so events in the same datablock don't deduplicate under zstd-dict,
    // otherwise 10 identical large events would compress to one and let the WAL fill too slowly
    // to force segment rotation.
    static EVENT_SEED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seed = EVENT_SEED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut payload = vec![0u8; payload_size];
    crate::fill_incompressible(&mut payload, seed);
    DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: Some(1),
        event_timestamp: 0,
        event_type_major: 1,
        event_type_minor: 1,
        event_value: Arc::new(payload),
        iv: None,
    }
}

/// Build a write request for a single aggregate with multiple events.
/// Alternates between small (inline) and large (datablock) payloads.
fn make_create_request(flat_index: usize, client_id: u128) -> ClientRequest {
    let (org_id, type_id, agg_id) = index_to_key(flat_index);
    // Mix payloads: even indices get small (inline), odd get large (datablock)
    let payload_size = if flat_index % 2 == 0 { SMALL_PAYLOAD_SIZE } else { LARGE_PAYLOAD_SIZE };
    let events: Vec<_> = (0..EVENTS_PER_WRITE).map(|_| make_event(payload_size)).collect();

    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(org_id, type_id, agg_id),
        SingleAggregateWrite {
            events,
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    ClientRequest::Write(WriteRequest {
        correlation_id: None,
        client_id,
        user_id: None,
        writes,
    })
}

/// Single-aggregate write for mixed-write phase (large payload to force rotations).
fn make_single_write_request(org_id: u128, type_id: u128, aggregate_id: u128, client_id: u128) -> ClientRequest {
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(org_id, type_id, aggregate_id),
        SingleAggregateWrite {
            events: vec![make_event(LARGE_PAYLOAD_SIZE)],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
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

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Read & List Benchmark ===");
    println!("  {} orgs × {} types × {} aggregates = {} total",
        NUM_ORGS, TYPES_PER_ORG, AGGREGATES_PER_TYPE, TOTAL_AGGREGATES);
    println!("  128MB segment files, mixed inline/datablock payloads\n");

    let base_port = 10100 + (std::process::id() % 100) as u16;
    let api_keys = generate_key_set();
    let keypair = Crypto::generate_keypair(None)?;
    let api_key_b64 = base64::engine::general_purpose::STANDARD.encode(&api_keys.primary_rw);
    let identity_config = ClientIdentityConfig {
        public_key: Some(keypair.public_key_base64.clone()),
        private_key: Some(keypair.private_key_base64.clone()),
        api_key: Some(api_key_b64),
    };

    let (fsync_delay, num_shards) = bench_tuning();

    let config = ServerConfig {
        log_level: "warn".to_string(),
        standalone: true,
        require_client_identity: true,
        insecure_allow_plaintext_auth: true,
        shard_log_preallocate_bytes: 128 * 1024 * 1024,
        fsync_delay_us: fsync_delay,
        num_shards,
        list_page_size: 1000,
        list_max_duration_ms: 10_000,
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
    println!("--- Phase 1: Populating WAL ({} aggregates × {} events each) ---",
        TOTAL_AGGREGATES, EVENTS_PER_WRITE);

    let setup_start = Instant::now();
    let writes_done = Arc::new(AtomicU64::new(0));

    let (mut setup_clients, verified_client_id) = connect_pool(&addr, &identity_config, WRITE_CONNECTIONS).await;

    let mut setup_tasks = Vec::new();
    let aggs_per_conn = TOTAL_AGGREGATES / setup_clients.len();
    let mut agg_offset = 0usize;

    for mut client in setup_clients.drain(..) {
        let count = aggs_per_conn;
        let offset = agg_offset;
        let writes_done = Arc::clone(&writes_done);
        agg_offset += count;

        setup_tasks.push(tokio::spawn(async move {
            for i in 0..count {
                let flat = offset + i;
                let req = make_create_request(flat, verified_client_id);
                if let Err(e) = client.send_request(&req).await {
                    if i == 0 {
                        eprintln!("  Setup write error (offset={}, i={}): {}", offset, i, e);
                    }
                    return;
                }
                writes_done.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let writes_done_monitor = Arc::clone(&writes_done);
    let total_u64 = TOTAL_AGGREGATES as u64;
    let progress = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let done = writes_done_monitor.load(Ordering::Relaxed);
            println!("  Setup progress: {}/{} aggregates ({:.0}%)",
                done, total_u64, done as f64 / total_u64 as f64 * 100.0);
            if done >= total_u64 {
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
    println!("  Setup complete: {} aggregates in {:.1}s ({:.0} writes/s)",
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
    // Phase 3: list_aggregates throughput
    // ========================================
    println!("\n--- Phase 3: List Aggregates Throughput ({} connections, {}s) ---",
        LIST_CONNECTIONS, TEST_DURATION_SECS);

    let list_result = run_list_aggregates_benchmark(
        &addr, &identity_config, LIST_CONNECTIONS, "List aggregates",
    ).await?;
    print_result(&list_result);

    tokio::time::sleep(Duration::from_secs(1)).await;

    // ========================================
    // Phase 4: list_orgs throughput
    // ========================================
    println!("\n--- Phase 4: List Orgs Throughput ({} connections, {}s) ---",
        LIST_ORGS_CONNECTIONS, TEST_DURATION_SECS);

    let list_orgs_result = run_list_orgs_benchmark(
        &addr, &identity_config, LIST_ORGS_CONNECTIONS, "List orgs",
    ).await?;
    print_result(&list_orgs_result);

    tokio::time::sleep(Duration::from_secs(1)).await;

    // ========================================
    // Phase 5: list_aggregate_types throughput
    // ========================================
    println!("\n--- Phase 5: List Types Throughput ({} connections, {}s) ---",
        LIST_TYPES_CONNECTIONS, TEST_DURATION_SECS);

    let list_types_result = run_list_types_benchmark(
        &addr, &identity_config, LIST_TYPES_CONNECTIONS, "List types",
    ).await?;
    print_result(&list_types_result);

    tokio::time::sleep(Duration::from_secs(1)).await;

    // ========================================
    // Phase 6: Mixed read + write
    // ========================================
    println!("\n--- Phase 6: Mixed Read+Write ({} readers + {} writers, {}s) ---",
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
    // Phase 7: List memory pressure
    // ========================================
    println!("\n--- Phase 7: List Memory Pressure ({} concurrent connections, {}s) ---",
        LIST_PRESSURE_CONNECTIONS, TEST_DURATION_SECS);

    let pressure_result = run_list_aggregates_benchmark(
        &addr, &identity_config, LIST_PRESSURE_CONNECTIONS, "List pressure",
    ).await?;
    print_result(&pressure_result);

    // ========================================
    // Verify log rotations occurred (before dropping server, which cleans up temp dir)
    // ========================================
    let data_root = server.config().data_root.clone();
    let mut total_segments = 0usize;
    let mut shards_checked = 0usize;
    for entry in fs::read_dir(&data_root)? {
        let entry = entry?;
        if !entry.file_name().to_string_lossy().starts_with("shard_") {
            continue;
        }
        let shard_dir = entry.path();
        let wal_files: Vec<_> = fs::read_dir(&shard_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
            .collect();
        total_segments += wal_files.len();
        shards_checked += 1;
    }
    let avg_segments = if shards_checked > 0 { total_segments as f64 / shards_checked as f64 } else { 0.0 };
    println!("\n  Log segments: {} total across {} shards ({:.1} avg per shard)",
        total_segments, shards_checked, avg_segments);
    assert!(
        total_segments > shards_checked,
        "Expected at least one rotation (total segments {} should exceed shard count {})",
        total_segments, shards_checked
    );

    drop(server);

    // ========================================
    // Summary
    // ========================================
    println!("\n\n{}", "=".repeat(100));
    println!("  RESULTS");
    println!("{}\n", "=".repeat(100));

    println!(
        "{:<25} {:>6} {:>10} {:>14} {:>10} {:>8} {:>8} {:>8} {:>8}",
        "Scenario", "Conns", "Requests", "Throughput", "Avg (ms)", "P50", "P95", "P99", "Errors"
    );
    println!("{}", "-".repeat(110));

    let all_results = [
        &read_result, &list_result, &list_orgs_result, &list_types_result,
        &pressure_result,
    ];

    for r in all_results.iter().chain(mixed_results.iter().collect::<Vec<_>>().iter()) {
        println!(
            "{:<25} {:>6} {:>10} {:>11.0} /s {:>10.1} {:>8} {:>8} {:>8} {:>8}",
            r.label, r.num_connections, r.total_requests, r.throughput, r.avg_latency_ms,
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
        let flat = ((id as u64 * 7919 + count * 6271) % TOTAL_AGGREGATES as u64) as usize;
        let (org_id, type_id, agg_id) = index_to_key(flat);
        let req = ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(org_id, type_id, agg_id),
            filters: ReadFilters::new(1),
        });

        let t = Instant::now();
        match client.send_request(&req).await {
            Ok(ClientResponse::Read(_)) => {
                latencies.push(t.elapsed().as_millis() as u64);
                count += 1;
            }
            Ok(_) => errors += 1,
            Err(ClientError::Server(_)) => errors += 1,
            Err(ClientError::RequestTimeout) => errors += 1,
            Err(_) => {
                errors += 1;
                break;
            }
        }
    }

    Ok(TaskStats { request_count: count, error_count: errors, latencies_us: latencies })
}

// --- Scenario: list_aggregates benchmark ---

async fn run_list_aggregates_benchmark(
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
            list_aggregates_worker(id, client, barrier).await
        }));
    }

    let stats = collect_stats(tasks).await;
    Ok(compute_result(label, actual, stats, start.elapsed()))
}

async fn list_aggregates_worker(
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
        // Each connection cycles through different org/type combinations
        let org_id = ((id as u64 * 31 + count * 17) % NUM_ORGS as u64 + 1) as u128;
        let type_id = ((id as u64 * 13 + count * 7) % TYPES_PER_ORG as u64 + 1) as u128;

        let mut cursor: Option<u64> = None;
        loop {
            if Instant::now() >= deadline {
                break;
            }

            let req = ClientRequest::ListAggregates(ListAggregatesRequest {
                correlation_id: None,
                shard_id: 0,
                org_id: Some(org_id),
                aggregate_type_id: Some(type_id),
                cursor,
            });

            let t = Instant::now();
            match client.send_request(&req).await {
                Ok(ClientResponse::ListAggregates(r)) => {
                    latencies.push(t.elapsed().as_millis() as u64);
                    count += 1;
                    cursor = r.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                Ok(_) => { errors += 1; break; }
                Err(ClientError::Server(_)) => { errors += 1; break; }
                Err(ClientError::RequestTimeout) => {
                    errors += 1;
                    cursor = None;
                }
                Err(_) => { errors += 1; break; }
            }
        }
    }

    Ok(TaskStats { request_count: count, error_count: errors, latencies_us: latencies })
}

// --- Scenario: list_orgs benchmark ---

async fn run_list_orgs_benchmark(
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
            list_orgs_worker(client, barrier).await
        }));
    }

    let stats = collect_stats(tasks).await;
    Ok(compute_result(label, actual, stats, start.elapsed()))
}

async fn list_orgs_worker(
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
) -> Result<TaskStats, String> {
    let mut count = 0u64;
    let mut errors = 0u64;
    let mut latencies = Vec::new();

    barrier.wait().await;
    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        let mut cursor: Option<u64> = None;
        loop {
            if Instant::now() >= deadline {
                break;
            }

            let req = ClientRequest::ListOrgs(ListOrgsRequest {
                correlation_id: None,
                shard_id: 0,
                cursor,
            });

            let t = Instant::now();
            match client.send_request(&req).await {
                Ok(ClientResponse::ListOrgs(r)) => {
                    latencies.push(t.elapsed().as_millis() as u64);
                    count += 1;
                    cursor = r.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                Ok(_) => { errors += 1; break; }
                Err(ClientError::Server(_)) => { errors += 1; break; }
                Err(ClientError::RequestTimeout) => {
                    errors += 1;
                    cursor = None;
                }
                Err(_) => { errors += 1; break; }
            }
        }
    }

    Ok(TaskStats { request_count: count, error_count: errors, latencies_us: latencies })
}

// --- Scenario: list_aggregate_types benchmark ---

async fn run_list_types_benchmark(
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
            list_types_worker(id, client, barrier).await
        }));
    }

    let stats = collect_stats(tasks).await;
    Ok(compute_result(label, actual, stats, start.elapsed()))
}

async fn list_types_worker(
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
        let org_id = ((id as u64 * 31 + count * 17) % NUM_ORGS as u64 + 1) as u128;

        let mut cursor: Option<u64> = None;
        loop {
            if Instant::now() >= deadline {
                break;
            }

            let req = ClientRequest::ListAggregateTypes(ListAggregateTypesRequest {
                correlation_id: None,
                shard_id: 0,
                org_id: Some(org_id),
                cursor,
            });

            let t = Instant::now();
            match client.send_request(&req).await {
                Ok(ClientResponse::ListAggregateTypes(r)) => {
                    latencies.push(t.elapsed().as_millis() as u64);
                    count += 1;
                    cursor = r.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                Ok(_) => { errors += 1; break; }
                Err(ClientError::Server(_)) => { errors += 1; break; }
                Err(ClientError::RequestTimeout) => {
                    errors += 1;
                    cursor = None;
                }
                Err(_) => { errors += 1; break; }
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
        let flat = ((id as u64 * 7919 + count * 6271) % TOTAL_AGGREGATES as u64) as usize;
        let (org_id, type_id, agg_id) = index_to_key(flat);
        let req = make_single_write_request(org_id, type_id, agg_id, verified_client_id);

        let t = Instant::now();
        match client.send_request(&req).await {
            Ok(_) => {
                latencies.push(t.elapsed().as_millis() as u64);
                count += 1;
            }
            Err(ClientError::Server(e)) => {
                eprintln!("  Writer {} error: {}", id, e);
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
