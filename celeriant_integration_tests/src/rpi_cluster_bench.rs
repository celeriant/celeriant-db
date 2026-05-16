//! RPi Cluster Benchmark
//!
//! Runs write throughput and latency benchmarks against a remote Celeriant
//! cluster (e.g. the RPi kTLS testbed). Unlike the `batch` test, this does
//! NOT start any servers — it connects to an already-running cluster.
//!
//! Environment variables:
//!   CLUSTER_ADDRESS     — leader address (default: 10.0.0.50:10000)
//!   CLUSTER_CA_CERT     — CA cert for server verification (default: deploy/rpi-cluster/certs/client-ca.crt)
//!   CLUSTER_CLIENT_CERT — client cert for mTLS (default: deploy/rpi-cluster/certs/client.crt)
//!   CLUSTER_CLIENT_KEY  — client key for mTLS (default: deploy/rpi-cluster/certs/client.key)
//!   CLUSTER_SERVER_NAME — TLS SNI server name (default: 10.0.0.50)
//!   CLUSTER_THROUGHPUT_CONNECTIONS — throughput test connections (default: 850)
//!   CLUSTER_LATENCY_CONNECTIONS   — latency test connections (default: 125)
//!   CLUSTER_DURATION              — test duration in seconds (default: 15)
//!   CLUSTER_CONNECT_BATCH_SIZE    — concurrent connection batch size (default: 100)

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::ClientTlsConfig;
use celeriant_crypto::pki::PkiManager;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use rustls_pki_types::ServerName;
use tokio::sync::Barrier;
use tokio::time::Instant;

const CLIENTSIDE_TIMEOUT_S: u64 = 30;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn expand_home(path: &str) -> PathBuf {
    if path.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(&path[2..]);
        }
    }
    PathBuf::from(path)
}

fn build_tls_config(
    ca_cert: &str,
    client_cert: &str,
    client_key: &str,
    server_name: &str,
) -> Result<ClientTlsConfig, Box<dyn std::error::Error>> {
    let ca_path = expand_home(ca_cert);
    let cert_path = expand_home(client_cert);
    let key_path = expand_home(client_key);

    let ca_bundle = PkiManager::load_ca_bundle(&ca_path)?;
    let (cert_chain, key) = PkiManager::load_identity(&cert_path, &key_path)?;
    let client_config = PkiManager::build_client_config(&ca_bundle, cert_chain, key)?;
    let sni = ServerName::try_from(server_name.to_string())
        .map_err(|e| format!("Invalid server name '{}': {e}", server_name))?;
    Ok(ClientTlsConfig::new(client_config, sni))
}

struct TaskStats {
    request_count: u64,
    latencies_ms: Vec<u64>,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let address = env_or("CLUSTER_ADDRESS", "10.0.0.50:10000");
    let plaintext = std::env::var("CLUSTER_PLAINTEXT").is_ok();
    let server_name = env_or("CLUSTER_SERVER_NAME", "10.0.0.50");
    let throughput_conns: usize = env_or("CLUSTER_THROUGHPUT_CONNECTIONS", "8000").parse()?;
    let latency_conns: usize = env_or("CLUSTER_LATENCY_CONNECTIONS", "125").parse()?;
    let duration_secs: u64 = env_or("CLUSTER_DURATION", "15").parse()?;

    println!("=== RPi Cluster Benchmark ===\n");
    println!("  Target:          {}", address);
    println!("  TLS:             {}", if plaintext { "disabled" } else { "mTLS" });
    println!("  Throughput conns: {}", throughput_conns);
    println!("  Latency conns:   {}", latency_conns);
    println!("  Duration:        {}s", duration_secs);
    println!();

    let tls = if plaintext {
        None
    } else {
        let ca_cert = env_or("CLUSTER_CA_CERT", "deploy/rpi-cluster/certs/client-ca.crt");
        let client_cert = env_or("CLUSTER_CLIENT_CERT", "deploy/rpi-cluster/certs/client.crt");
        let client_key = env_or("CLUSTER_CLIENT_KEY", "deploy/rpi-cluster/certs/client.key");
        Some(build_tls_config(&ca_cert, &client_cert, &client_key, &server_name)?)
    };

    // Smoke test: single connection write + read
    println!("--- Smoke test ---");
    {
        let mut client = CeleriantClient::connect_with_timeout(
            &address,
            Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
            tls.clone(),
        )
        .await?;

        let key = AggregateKey::new(99, 99, 99);
        let event = DatablockAggregateEvent {
            client_event_index: 0,
            event_index: 0,
            event_id: None,
            event_timestamp: 0,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(b"smoke-test".to_vec()),
            iv: None,
        };

        let mut writes = HashMap::new();
        writes.insert(key, SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_event_batch_index: None,
            enforce_client_idempotency: false,
        });

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        });

        client.send_request(&request).await?;
        println!("  Write OK\n");
    }

    // Throughput benchmark
    println!("--- Throughput ({} connections, {}s) ---", throughput_conns, duration_secs);
    let result = run_benchmark(&address, throughput_conns, duration_secs, tls.clone()).await?;
    print_result(&result);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Latency benchmark
    println!("\n--- Latency ({} connections, {}s) ---", latency_conns, duration_secs);
    let lat_result = run_benchmark(&address, latency_conns, duration_secs, tls).await?;
    print_result(&lat_result);

    println!("\n=== Done ===");
    Ok(())
}

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

fn print_result(r: &BenchmarkResult) {
    println!(
        "  Connections: {} | Requests: {} | Throughput: {:.0} req/s",
        r.num_connections, r.total_requests, r.throughput
    );
    println!(
        "  Latency — Avg: {:.1}ms | P50: {}ms | P95: {}ms | P99: {}ms | P99.9: {}ms | Min: {}ms | Max: {}ms",
        r.avg_latency_ms, r.p50_ms, r.p95_ms, r.p99_ms, r.p999_ms, r.min_ms, r.max_ms
    );
}

async fn run_benchmark(
    address: &str,
    num_connections: usize,
    duration_secs: u64,
    tls: Option<ClientTlsConfig>,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let connect_start = Instant::now();
    let batch_size = std::env::var("CLUSTER_CONNECT_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100usize);

    let mut clients = Vec::with_capacity(num_connections);
    let mut failed = 0;

    for batch_start in (0..num_connections).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_connections);
        let mut batch_tasks = Vec::with_capacity(batch_end - batch_start);
        for id in batch_start..batch_end {
            let addr = address.to_string();
            let tls = tls.clone();
            batch_tasks.push(tokio::spawn(async move {
                let client = CeleriantClient::connect_with_timeout(
                    &addr,
                    Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
                    tls,
                )
                .await
                .map_err(|e| format!("Connection {} error: {}", id, e))?
                .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));
                Ok::<_, String>((id, client))
            }));
        }
        for task in batch_tasks {
            match task.await {
                Ok(Ok(pair)) => clients.push(pair),
                _ => failed += 1,
            }
        }
        if batch_end < num_connections {
            print!(
                "\r  Connecting... {}/{} ({} failed)",
                clients.len(),
                num_connections,
                failed
            );
        }
    }

    println!(
        "\r  Established {} connections in {:.2}s ({} failed)",
        clients.len(),
        connect_start.elapsed().as_secs_f64(),
        failed
    );

    if clients.is_empty() {
        return Err("No connections established".into());
    }

    let actual = clients.len();
    let barrier = Arc::new(Barrier::new(actual));
    let start = Instant::now();

    let mut tasks = Vec::with_capacity(actual);
    for (id, client) in clients {
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            run_connection(id, client, duration_secs, barrier).await
        }));
    }

    let mut all_stats = Vec::with_capacity(actual);
    for task in tasks {
        if let Ok(Ok(stats)) = task.await {
            all_stats.push(stats);
        }
    }

    let total_duration = start.elapsed();
    let total_requests: u64 = all_stats.iter().map(|s| s.request_count).sum();
    let mut all_latencies: Vec<u64> = all_stats.into_iter().flat_map(|s| s.latencies_ms).collect();
    all_latencies.sort_unstable();

    let throughput = total_requests as f64 / total_duration.as_secs_f64();

    let (avg, p50, p95, p99, p999, min, max) = if !all_latencies.is_empty() {
        let len = all_latencies.len();
        let avg = all_latencies.iter().sum::<u64>() as f64 / len as f64;
        (
            avg,
            all_latencies[len * 50 / 100],
            all_latencies[len * 95 / 100],
            all_latencies[len * 99 / 100],
            all_latencies[len * 999 / 1000],
            all_latencies[0],
            all_latencies[len - 1],
        )
    } else {
        (0.0, 0, 0, 0, 0, 0, 0)
    };

    Ok(BenchmarkResult {
        num_connections: actual,
        total_requests,
        throughput,
        avg_latency_ms: avg,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        p999_ms: p999,
        min_ms: min,
        max_ms: max,
    })
}

async fn run_connection(
    id: usize,
    mut client: CeleriantClient,
    duration_secs: u64,
    barrier: Arc<Barrier>,
) -> Result<TaskStats, String> {
    let mut request_count = 0u64;
    let mut latencies = Vec::new();

    barrier.wait().await;
    let deadline = Instant::now() + Duration::from_secs(duration_secs);

    while Instant::now() < deadline {
        let event = DatablockAggregateEvent {
            client_event_index: 0,
            event_index: 0,
            event_id: None,
            event_timestamp: 0,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(
                format!("[conn-{}-req-{}] hello", id, request_count).into_bytes(),
            ),
            iv: None,
        };

        let aggregate_id = id;
        let mut writes = HashMap::new();
        writes.insert(
            AggregateKey::new(1, 1, aggregate_id as u128),
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
            },
        );

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: id as u128,
            user_id: None,
            writes,
        });

        let req_start = Instant::now();
        match client.send_request(&request).await {
            Ok(_) => {
                latencies.push(req_start.elapsed().as_millis() as u64);
                request_count += 1;
            }
            Err(ClientError::Server(err)) => {
                eprintln!("Connection {} server error: {}", id, err);
            }
            Err(e) => {
                eprintln!("Connection {} error: {}", id, e);
                break;
            }
        }
    }

    Ok(TaskStats {
        request_count,
        latencies_ms: latencies,
    })
}
