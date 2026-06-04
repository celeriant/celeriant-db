//! RPi Cluster Pool Benchmark
//!
//! Write throughput and latency benchmark against a remote Celeriant cluster
//! using `CeleriantPool` for automatic leader failover and connection management.
//!
//! Environment variables:
//!   CLUSTER_ADDRESS_1   — primary node (default: 10.0.0.50:10000)
//!   CLUSTER_ADDRESS_2   — seed node (default: 10.0.0.51:10000)
//!   CLUSTER_CA_CERT     — CA cert for server verification (default: deploy/rpi-cluster/certs/client-ca.crt)
//!   CLUSTER_CLIENT_CERT — client cert for mTLS (default: deploy/rpi-cluster/certs/client.crt)
//!   CLUSTER_CLIENT_KEY  — client key for mTLS (default: deploy/rpi-cluster/certs/client.key)
//!   CLUSTER_SERVER_NAME — TLS SNI server name (default: 10.0.0.50)
//!   CLUSTER_CONNECTIONS — pool max connections per node (default: 500)
//!   CLUSTER_TASKS       — concurrent writer tasks (default: 2000)
//!   CLUSTER_DURATION    — test duration in seconds (default: 15)

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_client_tokio::ClientTlsConfig;
use celeriant_crypto::pki::PkiManager;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use rustls_pki_types::ServerName;
use tokio::sync::Barrier;
use tokio::time::Instant;

async fn resolve_to_ip(host_port: &str) -> Result<String, Box<dyn std::error::Error>> {
    let addrs: Vec<_> = tokio::net::lookup_host(host_port).await?.collect();
    let addr = addrs.first().ok_or_else(|| format!("DNS lookup failed for {host_port}"))?;
    Ok(addr.to_string())
}

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
    let ca_bundle = PkiManager::load_ca_bundle(&expand_home(ca_cert))?;
    let (cert_chain, key) = PkiManager::load_identity(&expand_home(client_cert), &expand_home(client_key))?;
    let client_config = PkiManager::build_client_config(&ca_bundle, cert_chain, key)?;
    let sni = ServerName::try_from(server_name.to_string())
        .map_err(|e| format!("Invalid server name '{}': {e}", server_name))?;
    Ok(ClientTlsConfig::new(client_config, sni))
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let addr1 = env_or("CLUSTER_ADDRESS_1", "10.0.0.50:10000");
    let addr2 = env_or("CLUSTER_ADDRESS_2", "10.0.0.51:10000");
    let plaintext = std::env::var("CLUSTER_PLAINTEXT").is_ok();
    let server_name = env_or("CLUSTER_SERVER_NAME", "10.0.0.50");
    let num_tasks: usize = env_or("CLUSTER_TASKS", "8000").parse()?;
    let max_conns: usize = env_or("CLUSTER_CONNECTIONS", &num_tasks.to_string()).parse()?;
    let duration_secs: u64 = env_or("CLUSTER_DURATION", "15").parse()?;

    // Resolve hostnames once up front to avoid overwhelming dnsmasq under high concurrency
    let resolved1 = resolve_to_ip(&addr1).await?;
    let resolved2 = resolve_to_ip(&addr2).await?;

    println!("=== RPi Cluster Pool Benchmark ===\n");
    println!("  Primary:    {addr1} ({resolved1})");
    println!("  Seed:       {addr2} ({resolved2})");
    println!("  TLS:        {}", if plaintext { "disabled" } else { "mTLS" });
    println!("  Pool conns: {max_conns}/node");
    println!("  Tasks:      {num_tasks}");
    println!("  Duration:   {duration_secs}s");
    println!();

    let mut opts = PoolOptions::new(&resolved1)
        .with_seed_addresses(vec![resolved2])
        .with_max_connections(max_conns)
        .with_connection_timeout(Duration::from_secs(30))
        .with_request_timeout(Duration::from_secs(5));

    if !plaintext {
        let ca = env_or("CLUSTER_CA_CERT", "deploy/rpi-cluster/certs/client-ca.crt");
        let cert = env_or("CLUSTER_CLIENT_CERT", "deploy/rpi-cluster/certs/client.crt");
        let key = env_or("CLUSTER_CLIENT_KEY", "deploy/rpi-cluster/certs/client.key");
        opts = opts.with_tls(build_tls_config(&ca, &cert, &key, &server_name)?);
    }

    let pool = Arc::new(CeleriantPool::new(opts));

    // Smoke test
    println!("--- Smoke test ---");
    let smoke_key = AggregateKey::new(99, 99, 99);
    let smoke_event = DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: None,
        event_timestamp: 0,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(b"smoke-test".to_vec()),
        iv: None,
    };
    pool.write_events(smoke_key, vec![smoke_event], 0).await?;
    println!("  Write OK\n");

    // Throughput benchmark
    println!("--- Throughput ({num_tasks} tasks, {duration_secs}s) ---");
    let result = run_benchmark(&pool, num_tasks, duration_secs).await;
    print_result(&result);

    println!("\n=== Done ===");
    Ok(())
}

struct BenchmarkResult {
    num_tasks: usize,
    total_requests: u64,
    errors: u64,
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
        "  Tasks: {} | Requests: {} | Errors: {} | Throughput: {:.0} req/s",
        r.num_tasks, r.total_requests, r.errors, r.throughput
    );
    println!(
        "  Latency — Avg: {:.1}ms | P50: {}ms | P95: {}ms | P99: {}ms | P99.9: {}ms | Min: {}ms | Max: {}ms",
        r.avg_latency_ms, r.p50_ms, r.p95_ms, r.p99_ms, r.p999_ms, r.min_ms, r.max_ms
    );
}

async fn run_benchmark(
    pool: &Arc<CeleriantPool>,
    num_tasks: usize,
    duration_secs: u64,
) -> BenchmarkResult {
    let barrier = Arc::new(Barrier::new(num_tasks));
    let total_ok = Arc::new(AtomicU64::new(0));
    let total_err = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::with_capacity(num_tasks);
    let start = Instant::now();

    for id in 0..num_tasks {
        let pool = Arc::clone(pool);
        let barrier = barrier.clone();
        let ok_counter = total_ok.clone();
        let err_counter = total_err.clone();

        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            barrier.wait().await;
            let deadline = Instant::now() + Duration::from_secs(duration_secs);
            let mut seq = 0u64;

            while Instant::now() < deadline {
                let event = DatablockAggregateEvent {
                    client_seq: 0,
                    event_seq: 0,
                    event_id: None,
                    event_timestamp: 0,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(
                        format!("[t-{id}-r-{seq}] hello").into_bytes(),
                    ),
                    iv: None,
                };

                let key = AggregateKey::new(1, 1, id as u128);
                let req_start = Instant::now();
                match pool.write_events(key, vec![event], 0).await {
                    Ok(_) => {
                        latencies.push(req_start.elapsed().as_millis() as u64);
                        ok_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        err_counter.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Task {id} error: {e}");
                    }
                }
                seq += 1;
            }
            latencies
        }));
    }

    let mut all_latencies = Vec::new();
    for task in tasks {
        if let Ok(lats) = task.await {
            all_latencies.extend(lats);
        }
    }

    let elapsed = start.elapsed();
    let ok = total_ok.load(Ordering::Relaxed);
    let errors = total_err.load(Ordering::Relaxed);
    all_latencies.sort_unstable();

    let throughput = ok as f64 / elapsed.as_secs_f64();
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

    BenchmarkResult {
        num_tasks,
        total_requests: ok,
        errors,
        throughput,
        avg_latency_ms: avg,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        p999_ms: p999,
        min_ms: min,
        max_ms: max,
    }
}
