use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_client_tokio::ClientTlsConfig;
use celeriant_crypto::pki::PkiManager;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use clap::Parser;
use rustls_pki_types::ServerName;
use tokio::sync::Barrier;
use tokio::time::Instant;

#[derive(Parser)]
#[command(name = "celeriant-bench", about = "Write throughput and latency benchmark against a remote Celeriant cluster")]
struct Args {
    /// Primary node address
    #[arg(long, env = "CLUSTER_ADDRESS_1")]
    address1: String,

    /// Seed node address
    #[arg(long, env = "CLUSTER_ADDRESS_2")]
    address2: String,

    /// TLS SNI server name (default: extracted from address1)
    #[arg(long, env = "CLUSTER_SERVER_NAME")]
    server_name: Option<String>,

    /// CA cert for server verification
    #[arg(long, env = "CLUSTER_CA_CERT", default_value = "deploy/rpi-cluster/certs/client-ca.crt")]
    ca_cert: String,

    /// Client cert for mTLS
    #[arg(long, env = "CLUSTER_CLIENT_CERT", default_value = "deploy/rpi-cluster/certs/client.crt")]
    client_cert: String,

    /// Client key for mTLS
    #[arg(long, env = "CLUSTER_CLIENT_KEY", default_value = "deploy/rpi-cluster/certs/client.key")]
    client_key: String,

    /// Disable TLS (plaintext mode)
    #[arg(long, env = "CLUSTER_PLAINTEXT")]
    plaintext: bool,

    /// Concurrent writer tasks
    #[arg(long, env = "CLUSTER_TASKS", default_value = "8000")]
    tasks: usize,

    /// Pool max connections per node (defaults to --tasks)
    #[arg(long, env = "CLUSTER_CONNECTIONS")]
    connections: Option<usize>,

    /// Test duration in seconds
    #[arg(long, env = "CLUSTER_DURATION", default_value = "15")]
    duration: u64,
}

async fn resolve_to_ip(host_port: &str) -> Result<String, Box<dyn std::error::Error>> {
    let addrs: Vec<_> = tokio::net::lookup_host(host_port).await?.collect();
    let addr = addrs.first().ok_or_else(|| format!("DNS lookup failed for {host_port}"))?;
    Ok(addr.to_string())
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
        .map_err(|e| format!("Invalid server name '{server_name}': {e}"))?;
    Ok(ClientTlsConfig::new(client_config, sni))
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
                    client_event_index: 0,
                    event_index: 0,
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
                match pool.write_events(key, vec![event]).await {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let max_conns = args.connections.unwrap_or(args.tasks);

    let resolved1 = resolve_to_ip(&args.address1).await?;
    let resolved2 = resolve_to_ip(&args.address2).await?;

    println!("=== RPi Cluster Pool Benchmark ===\n");
    println!("  Primary:    {} ({})", args.address1, resolved1);
    println!("  Seed:       {} ({})", args.address2, resolved2);
    println!("  TLS:        {}", if args.plaintext { "disabled" } else { "mTLS" });
    println!("  Pool conns: {max_conns}/node");
    println!("  Tasks:      {}", args.tasks);
    println!("  Duration:   {}s", args.duration);
    println!();

    let mut opts = PoolOptions::new(&resolved1)
        .with_seed_addresses(vec![resolved2])
        .with_max_connections(max_conns)
        .with_connection_timeout(Duration::from_secs(30))
        .with_request_timeout(Duration::from_secs(5));

    if !args.plaintext {
        let server_name = args.server_name.unwrap_or_else(|| {
            args.address1.split(':').next().unwrap_or(&args.address1).to_string()
        });
        opts = opts.with_tls(build_tls_config(&args.ca_cert, &args.client_cert, &args.client_key, &server_name)?);
    }

    let pool = Arc::new(CeleriantPool::new(opts));

    // Smoke test
    println!("--- Smoke test ---");
    let smoke_key = AggregateKey::new(99, 99, 99);
    let smoke_event = DatablockAggregateEvent {
        client_event_index: 0,
        event_index: 0,
        event_id: None,
        event_timestamp: 0,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(b"smoke-test".to_vec()),
        iv: None,
    };
    pool.write_events(smoke_key, vec![smoke_event]).await?;
    println!("  Write OK\n");

    // Throughput benchmark
    println!("--- Throughput ({} tasks, {}s) ---", args.tasks, args.duration);
    let result = run_benchmark(&pool, args.tasks, args.duration).await;
    print_result(&result);

    println!("\n=== Done ===");
    Ok(())
}
