//! Replicated Cleartext Batch Write Benchmark
//!
//! Stripped-down variant of batch::run that only runs:
//!   1. Replicated plaintext — throughput (24k connections)
//!   2. Replicated plaintext — latency   (1k connections)
//!
//! Starts a leader + follower + MinIO, no TLS, no client identity. Useful for
//! bisecting the replicated-path regression without paying for the other 6
//! scenarios `--test batch` runs (standalone throughput/latency, mTLS
//! throughput/latency x2) or starting MinIO twice.

use std::{sync::Arc, time::Duration};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::batch::ReplicatedServers;
use celeriant_msg::request::requests::WriteRequest;
use celeriant_msg::{process_client_requests::ClientRequest, request::requests::SingleAggregateWrite};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use std::collections::HashMap;
use tokio::sync::Barrier;
use tokio::time::Instant;

const THROUGHPUT_CONNECTIONS: usize = 24000;
const LATENCY_CONNECTIONS: usize = 1000;
const TEST_DURATION_SECS: u64 = 15;
const NUM_AGGREGATES: usize = 1024;
const USE_MICRO_PAYLOAD: bool = true;
const CLIENTSIDE_TIMEOUT_S: u64 = 5;

// Same tuned baseline as batch.rs — see that file for the full derivation.
const REPLICATED_THROUGHPUT_MIN: f64 = 203_000.0; // 239k * 0.85
const REPLICATED_LATENCY_AVG_MAX_MS: f64 = 72.5; // 63ms * 1.15
const REPLICATED_LATENCY_P99_MAX_MS: u64 = 103; // 90ms * 1.15

struct TaskStats {
    request_count: u64,
    failed_requests: u64,
    latencies_us: Vec<u64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BenchmarkResult {
    num_connections: usize,
    total_requests: u64,
    failed_requests: u64,
    throughput: f64,
    avg_latency_ms: f64,
    p50_ms: u64,
    p95_ms: u64,
    p99_ms: u64,
    p999_ms: u64,
    min_ms: u64,
    max_ms: u64,
}

struct Thresholds {
    min_throughput: Option<f64>,
    max_avg_latency_ms: Option<f64>,
    max_p99_latency_ms: Option<u64>,
}

fn check_thresholds(result: &BenchmarkResult, thresholds: &Thresholds) -> Vec<String> {
    let mut failures = Vec::new();
    if let Some(min) = thresholds.min_throughput {
        if result.throughput < min {
            failures.push(format!(
                "throughput {:.0} req/s < minimum {:.0} req/s",
                result.throughput, min
            ));
        }
    }
    if let Some(max) = thresholds.max_avg_latency_ms {
        if result.avg_latency_ms > max {
            failures.push(format!(
                "avg latency {:.1}ms > maximum {:.1}ms",
                result.avg_latency_ms, max
            ));
        }
    }
    if let Some(max) = thresholds.max_p99_latency_ms {
        if result.p99_ms > max {
            failures.push(format!(
                "p99 latency {}ms > maximum {}ms",
                result.p99_ms, max
            ));
        }
    }
    failures
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Replicated Cleartext Batch Write Benchmark ===\n");

    let base_port = 10300 + (std::process::id() % 100) as u16;

    let (fsync_delay, num_shards) = crate::bench_tuning();
    println!("  fsync_delay_us: {}, num_shards: {:?}", fsync_delay, num_shards);

    let replicated = ReplicatedServers::start(base_port, "warn").await?;
    let addr = replicated.address().to_string();

    // --- Throughput ---
    println!(
        "\n--- Throughput ({} connections) ---",
        THROUGHPUT_CONNECTIONS
    );
    let thru = run_benchmark_iteration(&addr, THROUGHPUT_CONNECTIONS).await?;
    print_result(&thru);
    print_confirm_loop_metrics(base_port + 2).await;
    let thru_failures = check_thresholds(
        &thru,
        &Thresholds {
            min_throughput: Some(REPLICATED_THROUGHPUT_MIN),
            max_avg_latency_ms: None,
            max_p99_latency_ms: None,
        },
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Latency ---
    println!("\n--- Latency ({} connections) ---", LATENCY_CONNECTIONS);
    let lat = run_benchmark_iteration(&addr, LATENCY_CONNECTIONS).await?;
    print_result(&lat);
    let lat_failures = check_thresholds(
        &lat,
        &Thresholds {
            min_throughput: None,
            max_avg_latency_ms: Some(REPLICATED_LATENCY_AVG_MAX_MS),
            max_p99_latency_ms: Some(REPLICATED_LATENCY_P99_MAX_MS),
        },
    );

    drop(replicated);

    // --- Report ---
    println!("\n\n{}", "=".repeat(90));
    println!("  RESULTS");
    println!("{}\n", "=".repeat(90));

    println!(
        "{:<20} {:>8} {:>14} {:>10} {:>8} {:>10} {:>8}",
        "Scenario", "Conns", "Throughput", "Avg (ms)", "P99 (ms)", "Busy-fails", "Result"
    );
    println!("{}", "-".repeat(88));

    for (label, result, failures) in [
        ("Throughput", &thru, &thru_failures),
        ("Latency", &lat, &lat_failures),
    ] {
        let status = if failures.is_empty() { "PASS" } else { "FAIL" };
        println!(
            "{:<20} {:>8} {:>11.0} /s {:>10.1} {:>8} {:>10} {:>8}",
            label, result.num_connections, result.throughput, result.avg_latency_ms,
            result.p99_ms, result.failed_requests, status,
        );
        for f in failures {
            println!("  >> {}", f);
        }
    }

    let has_failures = !thru_failures.is_empty() || !lat_failures.is_empty();
    if has_failures {
        return Err("Performance regression detected — thresholds breached".into());
    }

    Ok(())
}

// TEMP DIAGNOSTIC (perf regression investigation, session/progress.md): scrape the
// leader's confirm-loop iteration histogram to see the retry shape under real load.
// Remove once the investigation concludes.
async fn print_confirm_loop_metrics(metrics_port: u16) {
    let url = format!("http://127.0.0.1:{}/metrics", metrics_port);
    let client = match reqwest::Client::builder().timeout(Duration::from_secs(2)).build() {
        Ok(c) => c,
        Err(_) => return,
    };
    let body = match client.get(&url).send().await {
        Ok(resp) => match resp.text().await {
            Ok(b) => b,
            Err(_) => return,
        },
        Err(_) => return,
    };
    println!("  --- confirm-loop iteration histogram (leader) ---");
    for line in body.lines() {
        if line.contains("celeriant_replication_confirm_loop_iterations") && !line.starts_with('#') {
            println!("  {}", line);
        }
    }
}

fn print_result(r: &BenchmarkResult) {
    println!(
        "  Throughput: {:.0} req/s | Avg: {:.1}ms | P50: {}ms | P95: {}ms | P99: {}ms | P99.9: {}ms | Busy-fails: {}",
        r.throughput, r.avg_latency_ms, r.p50_ms, r.p95_ms, r.p99_ms, r.p999_ms, r.failed_requests
    );
}

async fn run_benchmark_iteration(
    server_address: &str,
    num_connections: usize,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let connect_start = Instant::now();

    let mut connection_tasks = Vec::with_capacity(num_connections);
    for connection_id in 0..num_connections {
        let addr = server_address.to_string();
        let task = tokio::spawn(async move {
            let client = CeleriantClient::connect_with_timeout(
                &addr,
                Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
                None,
            )
            .await
            .map_err(|e| format!("Connection {} error: {}", connection_id, e))?
            .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));

            Ok::<_, String>((connection_id, client))
        });
        connection_tasks.push(task);
    }

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

    let mut tasks = Vec::with_capacity(actual_connections);
    for (connection_id, client) in clients {
        let barrier = Arc::clone(&barrier);
        let task = tokio::spawn(async move {
            run_connection_benchmark(connection_id, client, barrier).await
        });
        tasks.push(task);
    }

    let mut all_stats = Vec::with_capacity(actual_connections);
    for task in tasks {
        match task.await {
            Ok(Ok(stats)) => all_stats.push(stats),
            _ => {}
        }
    }

    let total_duration = start_time.elapsed();

    let total_requests: u64 = all_stats.iter().map(|s| s.request_count).sum();
    let failed_requests: u64 = all_stats.iter().map(|s| s.failed_requests).sum();
    let mut all_latencies: Vec<u64> = all_stats
        .into_iter()
        .flat_map(|s| s.latencies_us)
        .collect();

    all_latencies.sort_unstable();

    let throughput = total_requests as f64 / total_duration.as_secs_f64();

    let (avg_latency_ms, p50_ms, p95_ms, p99_ms, p999_ms, min_ms, max_ms) =
        if !all_latencies.is_empty() {
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
        failed_requests,
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

async fn run_connection_benchmark(
    connection_id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
) -> Result<TaskStats, String> {
    let mut request_count = 0u64;
    let mut failed_requests = 0u64;
    let mut latencies = Vec::new();

    barrier.wait().await;

    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        let prefix = format!("[connection-{}-event-{}] ", connection_id, request_count);

        let event_1 = DatablockAggregateEvent {
            client_seq: 3,
            event_seq: 0,
            event_id: Some(1234567890),
            event_timestamp: 0,
            event_type_major: 2,
            event_type_minor: 3,
            event_value: std::sync::Arc::new(format!("{}Hello World!", prefix).into_bytes()),
            iv: None,
        };

        let event_2 = DatablockAggregateEvent {
            client_seq: 3,
            event_seq: 0,
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

        // One aggregate per connection — see batch_standalone_cleartext.rs for the full
        // explanation. Rotating per request advanced the target shard by 1 (mod
        // num_shards) every write, so the target was never the shard holding the
        // connection and check_client_redirect migrated the TCP stream across the mesh
        // on essentially every request.
        let aggregate_id = connection_id % NUM_AGGREGATES;

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
                expected_version: None,
                enforce_client_idempotency: false,
            },
        );

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: connection_id as u128,
            user_id: None,
            writes,
        });

        let req_start = Instant::now();

        match client
            .send_request(&request)
            .await
        {
            Ok(_) => {
                let latency_us = req_start.elapsed().as_millis() as u64;
                latencies.push(latency_us);
                request_count += 1;
            }
            Err(ClientError::Server(err)) => {
                eprintln!("Connection {} server error: {}", connection_id, err);
            }
            Err(ClientError::ServerBusy) => {
                failed_requests += 1;
            }
            Err(e) => match e {
                ClientError::RequestTimeout => {
                    eprintln!("Connection {} Timeout: {}", connection_id, e)
                }
                _ => {
                    eprintln!("Connection {} Error: {}", connection_id, e);
                    break;
                }
            },
        }
    }

    Ok(TaskStats {
        request_count,
        failed_requests,
        latencies_us: latencies,
    })
}
