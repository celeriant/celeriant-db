use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use celeriant_client_tokio::ClientTlsConfig;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};

/// Re-exported so consumers (the chaos runner) don't need a direct dependency
/// on `celeriant_client_tokio` just to name the pool type returned by
/// `PoolBuilder::build`.
pub use celeriant_client_tokio::pool::CeleriantPool as Pool;
use celeriant_crypto::pki::PkiManager;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use rustls_pki_types::ServerName;
use tokio::sync::Barrier;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub num_tasks: usize,
    pub total_requests: u64,
    pub errors: u64,
    pub throughput: f64,
    pub avg_latency_ms: f64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub p999_ms: u64,
    pub min_ms: u64,
    pub max_ms: u64,
}

pub fn expand_home(path: &str) -> PathBuf {
    if path.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(&path[2..]);
        }
    }
    PathBuf::from(path)
}

pub async fn resolve_to_ip(host_port: &str) -> Result<String, Box<dyn std::error::Error>> {
    let addrs: Vec<_> = tokio::net::lookup_host(host_port).await?.collect();
    let addr = addrs.first().ok_or_else(|| format!("DNS lookup failed for {host_port}"))?;
    Ok(addr.to_string())
}

pub fn build_tls_config(
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

pub struct PoolBuilder<'a> {
    pub address1: &'a str,
    pub address2: &'a str,
    pub server_name: Option<&'a str>,
    pub ca_cert: &'a str,
    pub client_cert: &'a str,
    pub client_key: &'a str,
    pub plaintext: bool,
    pub max_connections: usize,
}

impl<'a> PoolBuilder<'a> {
    pub async fn build(self) -> Result<Arc<CeleriantPool>, Box<dyn std::error::Error>> {
        let resolved1 = resolve_to_ip(self.address1).await?;
        let resolved2 = resolve_to_ip(self.address2).await?;

        let mut opts = PoolOptions::new(&resolved1)
            .with_seed_addresses(vec![resolved2])
            .with_max_connections(self.max_connections)
            .with_connection_timeout(Duration::from_secs(30))
            .with_request_timeout(Duration::from_secs(5));

        if !self.plaintext {
            let server_name = self
                .server_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| self.address1.split(':').next().unwrap_or(self.address1).to_string());
            opts = opts.with_tls(build_tls_config(self.ca_cert, self.client_cert, self.client_key, &server_name)?);
        }

        Ok(Arc::new(CeleriantPool::new(opts)))
    }
}

pub async fn smoke_test(pool: &Arc<CeleriantPool>) -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}

pub async fn run_benchmark(
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
            // Jittered exponential backoff on repeated errors. Without this,
            // 4000 concurrent tasks hammer a broken leader during the failover
            // window and generate millions of cheap errors per second, which
            // swamps the BenchErrorsBounded invariant even when the system is
            // actually behaving correctly. Reset to zero on every success so a
            // brief blip doesn't leak into steady-state throughput.
            let mut backoff_ms: u64 = 0;
            const BACKOFF_INITIAL_MS: u64 = 10;
            const BACKOFF_MAX_MS: u64 = 500;

            while Instant::now() < deadline {
                let event = DatablockAggregateEvent {
                    client_event_index: 0,
                    event_index: 0,
                    event_id: None,
                    event_timestamp: 0,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(format!("[t-{id}-r-{seq}] hello").into_bytes()),
                    iv: None,
                };

                let key = AggregateKey::new(1, 1, id as u128);
                let req_start = Instant::now();
                match pool.write_events(key, vec![event]).await {
                    Ok(_) => {
                        latencies.push(req_start.elapsed().as_millis() as u64);
                        ok_counter.fetch_add(1, Ordering::Relaxed);
                        backoff_ms = 0;
                    }
                    Err(e) => {
                        err_counter.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Task {id} error: {e}");
                        let next = if backoff_ms == 0 {
                            BACKOFF_INITIAL_MS
                        } else {
                            (backoff_ms * 2).min(BACKOFF_MAX_MS)
                        };
                        // 50-150% jitter on top of the base delay so tasks
                        // don't resync into lock-step retry waves.
                        let jitter_num = ((id as u64).wrapping_mul(2654435761).wrapping_add(seq)) % 1000;
                        let sleep_ms = next / 2 + (next * jitter_num) / 1000;
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                        backoff_ms = next;
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
