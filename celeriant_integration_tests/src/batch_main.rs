//! Batch Write Performance Test
//!
//! Stress tests write throughput with many concurrent connections.
//! Creates a temporary data directory and spawns the server automatically.
//!
//! Run with: cargo run --bin batch_main
//!
//! Set SWEEP_MODE=1 to run connection count sweep for optimal throughput discovery.

use std::{collections::HashMap, sync::Arc, time::Duration};

use celeriant_integration_tests::{MinioContainer, ServerConfig, TestServer};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_msg::request::requests::WriteRequest;
use celeriant_msg::{process_requests::Request, request::requests::SingleAggregateWrite};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use tokio::sync::Barrier;
use tokio::time::Instant;

const DEFAULT_NUM_CONNECTIONS: usize = 24000; // 28k max source port limit ~25000;
const TEST_DURATION_SECS: u64 = 15;
const NUM_AGGREGATES: usize = 1024;
const USE_MICRO_PAYLOAD: bool = true;
const CLIENTSIDE_TIMEOUT_S: u64 = 5;

/// Enable replicated mode: spins up a leader and follower, benchmarks writes to leader
const REPLICATED_MODE: bool = true;

// Connection counts to sweep through when SWEEP_MODE is enabled
const CONNECTION_SWEEP: &[usize] = &[512, 1024, 2048, 4096, 6144, 8192, 10240, 12288, 14336, 16384];

struct TaskStats {
    request_count: u64,
    latencies_us: Vec<u64>,
}

/// Holds both leader and follower servers for replicated mode
struct ReplicatedServers {
    leader: TestServer,
    _follower: TestServer,
    _minio: MinioContainer,
}

impl ReplicatedServers {
    async fn start(base_port: u16, log_level: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let minio_port = base_port + 10;

        println!("Starting MinIO on port {}...", minio_port);
        let minio = MinioContainer::start_with_bucket(minio_port, "test-batch").await?;
        let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
        println!("MinIO ready at {}\n", endpoint);

        // Start follower first (it needs to be listening before leader connects)
        let follower_port = base_port + 100;
        let follower_config = ServerConfig {
            log_level: log_level.to_string(),
            bootstrap_as_leader: false,
            s3_enabled: true,
            s3_region: Some(region.clone()),
            s3_bucket: Some(bucket.clone()),
            s3_access_key_id: Some(access_key.clone()),
            s3_secret_access_key: Some(secret_key.clone()),
            s3_endpoint_override: Some(endpoint.clone()),
            s3_allow_http: allow_http,
            ..Default::default()
        };
        println!("Starting follower on port {}...", follower_port);
        let follower = TestServer::start_with_config(follower_port, follower_config).await?;

        // Small delay to ensure follower is fully ready
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start leader with S3 config for election/discovery
        let leader_config = ServerConfig {
            log_level: log_level.to_string(),
            bootstrap_as_leader: true,
            s3_enabled: true,
            s3_region: Some(region),
            s3_bucket: Some(bucket),
            s3_access_key_id: Some(access_key),
            s3_secret_access_key: Some(secret_key),
            s3_endpoint_override: Some(endpoint),
            s3_allow_http: allow_http,
            ..Default::default()
        };
        println!("Starting leader on port {} (S3 election mode)...", base_port);
        let leader = TestServer::start_with_config(base_port, leader_config).await?;

        // Wait for S3 election to complete
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok(Self {
            leader,
            _follower: follower,
            _minio: minio,
        })
    }

    fn address(&self) -> &str {
        self.leader.address()
    }
}

#[derive(Debug, Clone)]
struct BenchmarkResult {
    num_connections: usize,
    total_requests: u64,
    throughput: f64,
    avg_latency_ms: f64,
    p50_ms: u64,
    p95_ms: u64,
    p99_ms: u64,
    p999_ms: u64,
    min_ms: u64,
    max_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sweep_mode = std::env::var("SWEEP_MODE").is_ok();

    if sweep_mode {
        run_sweep_benchmark().await
    } else {
        let num_connections = std::env::var("NUM_CONNECTIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_NUM_CONNECTIONS);
        run_single_benchmark(num_connections, true).await.map(|_| ())
    }
}

async fn run_sweep_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    let mode_str = if REPLICATED_MODE { "Replicated (Leader+Follower)" } else { "Standalone" };
    println!("=== Batch Write Performance Sweep Test ({}) ===\n", mode_str);
    println!("Testing connection counts: {:?}\n", CONNECTION_SWEEP);

    let port = 10100 + (std::process::id() % 100) as u16;

    // Start server(s)
    let (server_address, _standalone, _replicated) = if REPLICATED_MODE {
        println!("Starting replicated cluster...");
        let replicated = ReplicatedServers::start(port, "warn").await?;
        let addr = replicated.address().to_string();
        println!("Cluster started, leader at {}\n", addr);
        (addr, None, Some(replicated))
    } else {
        println!("Starting standalone test server...");
        let config = ServerConfig {
            log_level: "warn".to_string(),
            ..Default::default()
        };
        let server = TestServer::start_with_config(port, config).await?;
        let addr = server.address().to_string();
        println!("Server started at {}\n", addr);
        (addr, Some(server), None)
    };

    let mut results: Vec<BenchmarkResult> = Vec::new();

    for &num_connections in CONNECTION_SWEEP {
        println!("\n{}", "=".repeat(60));
        println!("Testing {} connections...", num_connections);
        println!("{}", "=".repeat(60));

        match run_benchmark_iteration(&server_address, num_connections).await {
            Ok(result) => {
                println!(
                    "  Throughput: {:.2} req/s | Avg latency: {:.2}ms | P99: {}ms",
                    result.throughput, result.avg_latency_ms, result.p99_ms
                );
                results.push(result);
            }
            Err(e) => {
                eprintln!("  Benchmark failed: {}", e);
            }
        }

        // Brief pause between tests to let things settle
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Print summary report
    print_summary_report(&results);

    Ok(())
}

fn print_summary_report(results: &[BenchmarkResult]) {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                              BATCH WRITE PERFORMANCE SWEEP REPORT                                    ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Connections │  Throughput   │ Total Reqs │ Avg (ms) │ P50 │ P95 │ P99 │ P99.9 │ Min │  Max  ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════════════════════════════╣");

    for r in results {
        println!(
            "║ {:>10}  │ {:>12.2} │ {:>10} │ {:>8.2} │ {:>3} │ {:>3} │ {:>3} │ {:>5} │ {:>3} │ {:>5} ║",
            r.num_connections,
            r.throughput,
            r.total_requests,
            r.avg_latency_ms,
            r.p50_ms,
            r.p95_ms,
            r.p99_ms,
            r.p999_ms,
            r.min_ms,
            r.max_ms
        );
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════════════════════════════╝");

    // Find optimal configuration
    if let Some(best) = results.iter().max_by(|a, b| {
        a.throughput.partial_cmp(&b.throughput).unwrap()
    }) {
        println!("\n=== OPTIMAL CONFIGURATION ===");
        println!("Best throughput: {:.2} req/s with {} connections", best.throughput, best.num_connections);
        println!("Latency at optimal: avg={:.2}ms, P99={}ms", best.avg_latency_ms, best.p99_ms);

        let target = 400_000.0;
        if best.throughput >= target {
            println!("\n✓ Target of {} req/s ACHIEVED!", target as u64);
        } else {
            let gap = target - best.throughput;
            let percentage = (best.throughput / target) * 100.0;
            println!("\n✗ Target of {} req/s NOT achieved", target as u64);
            println!("  Current: {:.2} req/s ({:.1}% of target)", best.throughput, percentage);
            println!("  Gap: {:.2} req/s", gap);
        }
    }

    // Throughput trend analysis
    println!("\n=== THROUGHPUT TREND ===");
    for (i, r) in results.iter().enumerate() {
        let bar_length = ((r.throughput / 500_000.0) * 50.0) as usize;
        let bar: String = "█".repeat(bar_length.min(50));
        let marker = if i > 0 && results[i-1].throughput < r.throughput { "↑" }
                     else if i > 0 && results[i-1].throughput > r.throughput { "↓" }
                     else { " " };
        println!("{:>6} conn: {:50} {:>12.0} {}", r.num_connections, bar, r.throughput, marker);
    }
}

async fn run_benchmark_iteration(
    server_address: &str,
    num_connections: usize,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let connect_start = Instant::now();

    // Establish all connections
    let mut connection_tasks = Vec::with_capacity(num_connections);
    for connection_id in 0..num_connections {
        let addr = server_address.to_string();
        let task = tokio::spawn(async move {
            let client = CeleriantClient::connect_with_timeout(
                &addr,
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
    let mut clients = Vec::with_capacity(num_connections);
    let mut failed_connections = 0;
    for task in connection_tasks {
        match task.await {
            Ok(Ok((connection_id, client))) => {
                clients.push((connection_id, client));
            }
            Ok(Err(_)) => failed_connections += 1,
            Err(_) => failed_connections += 1,
        }
    }

    let connect_duration = connect_start.elapsed();
    println!(
        "  Established {} connections in {:.2}s ({} failed)",
        clients.len(),
        connect_duration.as_secs_f64(),
        failed_connections
    );

    if clients.is_empty() {
        return Err("No connections established".into());
    }

    let actual_connections = clients.len();
    let barrier = Arc::new(Barrier::new(actual_connections));

    let start_time = Instant::now();

    // Spawn benchmark tasks
    let mut tasks = Vec::with_capacity(actual_connections);
    for (connection_id, client) in clients {
        let barrier = Arc::clone(&barrier);
        let task = tokio::spawn(async move {
            run_connection_benchmark(connection_id, client, barrier).await
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut all_stats = Vec::with_capacity(actual_connections);
    for task in tasks {
        match task.await {
            Ok(Ok(stats)) => all_stats.push(stats),
            _ => {}
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

    let throughput = total_requests as f64 / total_duration.as_secs_f64();

    let (avg_latency_ms, p50_ms, p95_ms, p99_ms, p999_ms, min_ms, max_ms) = if !all_latencies.is_empty() {
        let avg = all_latencies.iter().sum::<u64>() as f64 / all_latencies.len() as f64;
        let p50 = all_latencies[all_latencies.len() * 50 / 100];
        let p95 = all_latencies[all_latencies.len() * 95 / 100];
        let p99 = all_latencies[all_latencies.len() * 99 / 100];
        let p999 = all_latencies[all_latencies.len() * 999 / 1000];
        let max = all_latencies[all_latencies.len() - 1];
        let min = all_latencies[0];
        (avg, p50, p95, p99, p999, min, max)
    } else {
        (0.0, 0, 0, 0, 0, 0, 0)
    };

    Ok(BenchmarkResult {
        num_connections: actual_connections,
        total_requests,
        throughput,
        avg_latency_ms,
        p50_ms,
        p95_ms,
        p99_ms,
        p999_ms,
        min_ms,
        max_ms,
    })
}

async fn run_single_benchmark(num_connections: usize, verbose: bool) -> Result<Option<BenchmarkResult>, Box<dyn std::error::Error>> {
    let mode_str = if REPLICATED_MODE { "Replicated" } else { "Standalone" };
    if verbose {
        println!("=== Batch Write Performance Test ({}) ===\n", mode_str);
    }

    let port = 10100 + (std::process::id() % 100) as u16;

    // Start server(s)
    let (server_address, _standalone, _replicated) = if REPLICATED_MODE {
        if verbose {
            println!("Starting replicated cluster...");
        }
        let replicated = ReplicatedServers::start(port, "warn").await?;
        let addr = replicated.address().to_string();
        if verbose {
            println!("Cluster started, leader at {}\n", addr);
        }
        (addr, None, Some(replicated))
    } else {
        if verbose {
            println!("Starting standalone test server...");
        }
        let config = ServerConfig {
            log_level: "warn".to_string(),
            fsync_delay_us: 30000,
            ..Default::default()
        };
        let server = TestServer::start_with_config(port, config).await?;
        let addr = server.address().to_string();
        if verbose {
            println!("Server started at {}\n", addr);
        }
        (addr, Some(server), None)
    };

    if verbose {
        println!("Establishing {} connections...", num_connections);
    }

    let connect_start = Instant::now();

    // Establish all connections first
    let mut connection_tasks = Vec::with_capacity(num_connections);
    for connection_id in 0..num_connections {
        let addr = server_address.to_string();
        let task = tokio::spawn(async move {
            let client = CeleriantClient::connect_with_timeout(
                &addr,
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
    let mut clients = Vec::with_capacity(num_connections);
    let mut failed_connections = 0;
    for task in connection_tasks {
        match task.await {
            Ok(Ok((connection_id, client))) => {
                clients.push((connection_id, client));
            }
            Ok(Err(e)) => {
                if verbose { eprintln!("{}", e); }
                failed_connections += 1;
            }
            Err(e) => {
                if verbose { eprintln!("Join error: {}", e); }
                failed_connections += 1;
            }
        }
    }

    let connect_duration = connect_start.elapsed();
    if verbose {
        println!(
            "Established {} connections in {:.2}s ({} failed)",
            clients.len(),
            connect_duration.as_secs_f64(),
            failed_connections
        );
    }

    if clients.is_empty() {
        return Err("No connections established".into());
    }

    // Create a barrier to synchronize all tasks to start at the same time
    let barrier = Arc::new(Barrier::new(clients.len()));

    if verbose {
        println!(
            "Starting benchmark with {} concurrent connections for {} seconds...",
            clients.len(),
            TEST_DURATION_SECS
        );
    }

    let start_time = Instant::now();

    // Spawn benchmark tasks with pre-established connections
    let mut tasks = Vec::with_capacity(clients.len());
    for (connection_id, client) in clients {
        let barrier = Arc::clone(&barrier);
        let task =
            tokio::spawn(
                async move { run_connection_benchmark(connection_id, client, barrier).await },
            );
        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut all_stats = Vec::with_capacity(num_connections);
    for task in tasks {
        match task.await {
            Ok(Ok(stats)) => all_stats.push(stats),
            Ok(Err(e)) => { if verbose { eprintln!("Task error: {}", e); } }
            Err(e) => { if verbose { eprintln!("Join error: {}", e); } }
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

    if verbose {
        println!("\n=== Benchmark Results ===");
        println!("Total Duration: {:.2}s", total_duration.as_secs_f64());
        println!("Total Requests: {}", total_requests);
        println!("Throughput: {:.2} req/s", throughput);
    }

    if !all_latencies.is_empty() && verbose {
        let avg_latency = all_latencies.iter().sum::<u64>() as f64 / all_latencies.len() as f64;
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

    Ok(None)
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
            event_value: std::sync::Arc::new(
                format!(
                    "{}She should have died hereafter;
            There would have been a time for such a word.
            Tomorrow, and tomorrow, and tomorrow,
            Creeps in this petty pace from day to day
            To the last syllable of recorded time,
            And all our yesterdays have lighted fools
            The way to dusty death. Out, out, brief candle!
            Life's but a walking shadow, a poor player
            That struts and frets his hour upon the stage
            And then is heard no more. It is a tale
            Told by an idiot, full of sound and fury, Signifying nothing. ",
                    prefix
                )
                .into_bytes(),
            ),
            iv: None,
        };

        let aggregate_id = (connection_id + request_count as usize) % NUM_AGGREGATES;

        let mut writes = HashMap::new();
        writes.insert(
            AggregateKey::new(1, 1, aggregate_id as u128),
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

        match client
            .send_request(&request, CompressionType::None)
            .await
        {
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
            Err(e) => match e {
                ClientError::RequestTimeout => {
                    eprintln!("Connection {} Timeout: {}", connection_id, e)
                }
                _ => {
                    eprintln!("Connection {} Error: {}", connection_id, e);
                    break; // Exit on connection errors
                }
            },
        }
    }

    Ok(TaskStats {
        request_count,
        latencies_us: latencies,
    })
}
