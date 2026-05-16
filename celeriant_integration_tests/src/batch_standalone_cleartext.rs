//! Standalone Cleartext Batch Write Benchmark
//!
//! Stripped-down variant of batch_main that only runs:
//!   1. Standalone plaintext — throughput (24k connections)
//!   2. Standalone plaintext — latency   (1k connections)
//!
//! Useful for quick iteration without the mTLS/replication overhead.

use std::{collections::HashMap, sync::Arc, time::Duration};

use base64::Engine;
use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_error::ClientError;
use celeriant_crypto::{generate_api_key, hash_api_key, Crypto};
use crate::{ServerConfig, TestServer};
use celeriant_msg::request::requests::WriteRequest;
use celeriant_msg::{process_client_requests::ClientRequest, request::requests::SingleAggregateWrite};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use std::fs;
use std::path::Path;
use tokio::sync::Barrier;
use tokio::time::Instant;

const THROUGHPUT_CONNECTIONS: usize = 24000;
const LATENCY_CONNECTIONS: usize = 1000;
const TEST_DURATION_SECS: u64 = 15;
const NUM_AGGREGATES: usize = 1024;
const USE_MICRO_PAYLOAD: bool = true;
const CLIENTSIDE_TIMEOUT_S: u64 = 5;

const STANDALONE_THROUGHPUT_MIN: f64 = 361_000.0; // ~425k * 0.85
const STANDALONE_LATENCY_AVG_MAX_MS: f64 = 25.0; // ~21ms * 1.15
const STANDALONE_LATENCY_P99_MAX_MS: u64 = 33; // ~28ms * 1.15

struct ApiKeySet {
    primary_rw: [u8; 32],
    primary_rw_hash: [u8; 32],
    secondary_rw_hash: [u8; 32],
    primary_ro_hash: [u8; 32],
    secondary_ro_hash: [u8; 32],
}

fn generate_key_set() -> ApiKeySet {
    let primary_rw = generate_api_key();
    let primary_rw_hash = hash_api_key(&primary_rw);
    let secondary_rw_hash = hash_api_key(&generate_api_key());
    let primary_ro_hash = hash_api_key(&generate_api_key());
    let secondary_ro_hash = hash_api_key(&generate_api_key());

    ApiKeySet {
        primary_rw,
        primary_rw_hash,
        secondary_rw_hash,
        primary_ro_hash,
        secondary_ro_hash,
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

struct TaskStats {
    request_count: u64,
    latencies_us: Vec<u64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
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
    println!("=== Standalone Cleartext Batch Write Benchmark ===\n");

    let base_port = 10100 + (std::process::id() % 100) as u16;

    let api_keys = generate_key_set();
    let keypair = Crypto::generate_keypair(None)?;
    let api_key_b64 = base64::engine::general_purpose::STANDARD.encode(&api_keys.primary_rw);
    let identity_config = ClientIdentityConfig {
        public_key: Some(keypair.public_key_base64.clone()),
        private_key: Some(keypair.private_key_base64.clone()),
        api_key: Some(api_key_b64),
    };

    let (fsync_delay, num_shards) = crate::bench_tuning();
    println!("  fsync_delay_us: {}, num_shards: {:?}", fsync_delay, num_shards);

    let config = ServerConfig {
        log_level: "warn".to_string(),
        standalone: true,
        require_client_identity: true,
        insecure_allow_plaintext_auth: true,
        fsync_delay_us: fsync_delay,
        num_shards,
        ..Default::default()
    };
    let temp_dir = match std::env::var("CELERIANT_TEST_DATA_DIR") {
        Ok(dir) => tempfile::TempDir::new_in(dir)?,
        Err(_) => tempfile::TempDir::new()?,
    };
    create_api_keys_file(temp_dir.path(), &api_keys)?;
    let server =
        TestServer::start_with_existing_dir(base_port, config, "standalone-pt".to_string(), temp_dir)
            .await?;
    let addr = server.address().to_string();

    // --- Throughput ---
    println!(
        "\n--- Throughput ({} connections) ---",
        THROUGHPUT_CONNECTIONS
    );
    let thru =
        run_benchmark_iteration(&addr, THROUGHPUT_CONNECTIONS, Some(identity_config.clone()))
            .await?;
    print_result(&thru);
    let thru_failures = check_thresholds(
        &thru,
        &Thresholds {
            min_throughput: Some(STANDALONE_THROUGHPUT_MIN),
            max_avg_latency_ms: None,
            max_p99_latency_ms: None,
        },
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Latency ---
    println!("\n--- Latency ({} connections) ---", LATENCY_CONNECTIONS);
    let lat =
        run_benchmark_iteration(&addr, LATENCY_CONNECTIONS, Some(identity_config.clone())).await?;
    print_result(&lat);
    let lat_failures = check_thresholds(
        &lat,
        &Thresholds {
            min_throughput: None,
            max_avg_latency_ms: Some(STANDALONE_LATENCY_AVG_MAX_MS),
            max_p99_latency_ms: Some(STANDALONE_LATENCY_P99_MAX_MS),
        },
    );

    drop(server);

    // --- Report ---
    println!("\n\n{}", "=".repeat(80));
    println!("  RESULTS");
    println!("{}\n", "=".repeat(80));

    println!(
        "{:<20} {:>8} {:>14} {:>10} {:>8} {:>8}",
        "Scenario", "Conns", "Throughput", "Avg (ms)", "P99 (ms)", "Result"
    );
    println!("{}", "-".repeat(78));

    for (label, result, failures) in [
        ("Throughput", &thru, &thru_failures),
        ("Latency", &lat, &lat_failures),
    ] {
        let status = if failures.is_empty() { "PASS" } else { "FAIL" };
        println!(
            "{:<20} {:>8} {:>11.0} /s {:>10.1} {:>8} {:>8}",
            label, result.num_connections, result.throughput, result.avg_latency_ms,
            result.p99_ms, status,
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

fn print_result(r: &BenchmarkResult) {
    println!(
        "  Throughput: {:.0} req/s | Avg: {:.1}ms | P50: {}ms | P95: {}ms | P99: {}ms | P99.9: {}ms",
        r.throughput, r.avg_latency_ms, r.p50_ms, r.p95_ms, r.p99_ms, r.p999_ms
    );
}

async fn run_benchmark_iteration(
    server_address: &str,
    num_connections: usize,
    identity_config: Option<ClientIdentityConfig>,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let connect_start = Instant::now();

    let mut connection_tasks = Vec::with_capacity(num_connections);
    for connection_id in 0..num_connections {
        let addr = server_address.to_string();
        let identity = identity_config.clone();
        let task = tokio::spawn(async move {
            let mut client = CeleriantClient::connect_with_timeout(
                &addr,
                Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
                None,
            )
            .await
            .map_err(|e| format!("Connection {} error: {}", connection_id, e))?
            .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));

            let verified_client_id: Option<u128> = if let Some(ref id_config) = identity {
                client
                    .identify(id_config)
                    .await
                    .map_err(|e| format!("Connection {} identify error: {}", connection_id, e))?
            } else {
                None
            };

            Ok::<_, String>((connection_id, client, verified_client_id))
        });
        connection_tasks.push(task);
    }

    let mut clients = Vec::with_capacity(num_connections);
    let mut failed_connections = 0;
    for task in connection_tasks {
        match task.await {
            Ok(Ok((connection_id, client, verified_client_id))) => {
                clients.push((connection_id, client, verified_client_id));
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
    for (connection_id, client, verified_client_id) in clients {
        let barrier = Arc::clone(&barrier);
        let task = tokio::spawn(async move {
            run_connection_benchmark(connection_id, client, verified_client_id, barrier).await
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
    verified_client_id: Option<u128>,
    barrier: Arc<Barrier>,
) -> Result<TaskStats, String> {
    let mut request_count = 0u64;
    let mut latencies = Vec::new();

    barrier.wait().await;

    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

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
            },
        );

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: verified_client_id.unwrap_or(connection_id as u128),
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
        latencies_us: latencies,
    })
}
