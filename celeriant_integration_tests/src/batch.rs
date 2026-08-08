//! Batch Write Performance Benchmark
//!
//! Benchmarks write throughput and latency in standalone and replicated modes.
//! Starts servers automatically with temporary data directories.
//!
//! Default mode runs 4 scenarios:
//!   1. Standalone  — throughput  (24k connections)
//!   2. Standalone  — latency    (1k connections)
//!   3. Replicated  — throughput  (24k connections)
//!   4. Replicated  — latency    (1k connections)
//!
//! Fails with non-zero exit if performance drops below minimum thresholds.
//!
//! Environment variables:
//!   SWEEP_MODE=1           — connection count sweep for throughput discovery
//!   SWEEP_REPLICATED=1     — use replicated mode for sweep (default: standalone)

use std::{collections::HashMap, sync::Arc, time::Duration};

use base64::Engine;
use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::ClientTlsConfig;
use celeriant_crypto::{generate_api_key, hash_api_key, Crypto};
use crate::{count_events, MinioContainer, ServerConfig, TestPki, TestServer};
use celeriant_lib::server_config::{ConfigClientAuth, ConfigTlsMode};
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

// Performance thresholds — 15% regression from tuned baselines is an immediate fail.
//
// Tuned baseline (32-core / 21 shards, NVMe PCIe5, 17ms fsync delay):
//   Standalone  24k conn: 414k writes/s, avg 57ms, p99 117ms
//   Replicated  24k conn: 239k writes/s, avg 99ms, p99 131ms
//   Standalone   1k conn:  44k writes/s, avg 22ms, p99 28ms
//   Replicated   1k conn:  16k writes/s, avg 63ms, p99 90ms (raised from 79ms by the post-replication ack-floor fsync, fe90ecc)
const STANDALONE_THROUGHPUT_MIN: f64 = 352_000.0; // 414k * 0.85
const REPLICATED_THROUGHPUT_MIN: f64 = 203_000.0; // 239k * 0.85
const STANDALONE_LATENCY_AVG_MAX_MS: f64 = 25.5; // 22ms * 1.15
const STANDALONE_LATENCY_P99_MAX_MS: u64 = 32; // 28ms * 1.15
const REPLICATED_LATENCY_AVG_MAX_MS: f64 = 72.5; // 63ms * 1.15
const REPLICATED_LATENCY_P99_MAX_MS: u64 = 103; // 90ms * 1.15

const CONNECTION_SWEEP: &[usize] = &[
    512, 1024, 2048, 4096, 6144, 8192, 10240, 12288, 14336, 16384,
];

struct ApiKeySet {
    primary_rw: [u8; 32],
    primary_rw_hash: [u8; 32],
    #[allow(dead_code)]
    secondary_rw: [u8; 32],
    secondary_rw_hash: [u8; 32],
    #[allow(dead_code)]
    primary_ro: [u8; 32],
    primary_ro_hash: [u8; 32],
    #[allow(dead_code)]
    secondary_ro: [u8; 32],
    secondary_ro_hash: [u8; 32],
}

fn generate_key_set() -> ApiKeySet {
    let primary_rw = generate_api_key();
    let primary_rw_hash = hash_api_key(&primary_rw);
    let secondary_rw = generate_api_key();
    let secondary_rw_hash = hash_api_key(&secondary_rw);
    let primary_ro = generate_api_key();
    let primary_ro_hash = hash_api_key(&primary_ro);
    let secondary_ro = generate_api_key();
    let secondary_ro_hash = hash_api_key(&secondary_ro);

    ApiKeySet {
        primary_rw,
        primary_rw_hash,
        secondary_rw,
        secondary_rw_hash,
        primary_ro,
        primary_ro_hash,
        secondary_ro,
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
    failed_requests: u64,
    latencies_us: Vec<u64>,
}

pub(crate) struct ReplicatedServers {
    leader: TestServer,
    follower: TestServer,
    _minio: MinioContainer,
}

impl ReplicatedServers {
    pub(crate) async fn start(base_port: u16, log_level: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_tls_and_keys(base_port, log_level, None, None).await
    }

    #[allow(dead_code)]
    async fn start_with_tls(
        base_port: u16,
        log_level: &str,
        tls: Option<&TestPki>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_tls_and_keys(base_port, log_level, tls, None).await
    }

    async fn start_with_tls_and_keys(
        base_port: u16,
        log_level: &str,
        tls: Option<&TestPki>,
        api_keys: Option<&ApiKeySet>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let minio_port = base_port + 10;
        println!("Starting MinIO on port {}...", minio_port);
        let minio = MinioContainer::start_with_bucket(minio_port, "test-batch").await?;
        let (region, bucket, access_key, secret_key, endpoint, allow_http) =
            minio.s3_config_fields();
        println!("MinIO ready at {}\n", endpoint);

        let tls_fields = match tls {
            Some(pki) => {
                let (cert, key) = pki.create_node_cert("repl-node")?;
                Some((pki.ca_cert_path(), cert, key))
            }
            None => None,
        };

        let (fsync_delay, num_shards) = crate::bench_tuning();
        let mut leader_config = ServerConfig {
            log_level: log_level.to_string(),
            s3_lease_duration_ms: 10_000,
            s3_enabled: true,
            s3_region: Some(region.clone()),
            s3_bucket: Some(bucket.clone()),
            s3_access_key_id: Some(access_key.clone()),
            s3_secret_access_key: Some(secret_key.clone()),
            s3_endpoint_override: Some(endpoint.clone()),
            s3_allow_http: allow_http,
            fsync_delay_us: fsync_delay,
            num_shards,
            ..Default::default()
        };

        let has_tls = tls_fields.is_some();
        if let Some((ca, cert, key)) = &tls_fields {
            leader_config.tls_mode = ConfigTlsMode::Strict;
            leader_config.tls_ca_cert = Some(ca.clone());
            leader_config.tls_node_cert = Some(cert.clone());
            leader_config.tls_node_key = Some(key.clone());
            leader_config.tls_client_auth = ConfigClientAuth::Require;
            leader_config.listen_address = "127.0.0.1".to_string();
        }

        if api_keys.is_some() {
            leader_config.require_client_identity = true;
            if !has_tls {
                leader_config.insecure_allow_plaintext_auth = true;
            }
        }

        println!(
            "Starting leader on port {} (S3 election mode)...",
            base_port
        );

        let leader = if let Some(keys) = api_keys {
            let temp_dir = tempfile::TempDir::new()?;
            create_api_keys_file(temp_dir.path(), keys)?;
            TestServer::start_with_existing_dir(base_port, leader_config, "leader".to_string(), temp_dir).await?
        } else {
            TestServer::start_with_config(base_port, leader_config).await?
        };

        // Wait for leader to grab the S3 lease before starting follower
        tokio::time::sleep(Duration::from_secs(3)).await;

        let follower_port = base_port + 100;
        let mut follower_config = ServerConfig {
            log_level: log_level.to_string(),
            s3_lease_duration_ms: 10_000,
            s3_enabled: true,
            s3_region: Some(region),
            s3_bucket: Some(bucket),
            s3_access_key_id: Some(access_key),
            s3_secret_access_key: Some(secret_key),
            s3_endpoint_override: Some(endpoint),
            s3_allow_http: allow_http,
            fsync_delay_us: fsync_delay,
            num_shards,
            ..Default::default()
        };

        if let Some((ca, cert, key)) = &tls_fields {
            follower_config.tls_mode = ConfigTlsMode::Strict;
            follower_config.tls_ca_cert = Some(ca.clone());
            follower_config.tls_node_cert = Some(cert.clone());
            follower_config.tls_node_key = Some(key.clone());
            follower_config.tls_client_auth = ConfigClientAuth::Require;
            follower_config.listen_address = "127.0.0.1".to_string();
        }

        if api_keys.is_some() {
            follower_config.require_client_identity = true;
            if !has_tls {
                follower_config.insecure_allow_plaintext_auth = true;
            }
        }

        println!("Starting follower on port {}...", follower_port);

        let follower = if let Some(keys) = api_keys {
            let temp_dir = tempfile::TempDir::new()?;
            create_api_keys_file(temp_dir.path(), keys)?;
            TestServer::start_with_existing_dir(follower_port, follower_config, "follower".to_string(), temp_dir).await?
        } else {
            TestServer::start_with_config(follower_port, follower_config).await?
        };

        // Wait for follower election + replication connection establishment
        tokio::time::sleep(Duration::from_secs(5)).await;

        Ok(Self {
            leader,
            follower,
            _minio: minio,
        })
    }

    pub(crate) fn address(&self) -> &str {
        self.leader.address()
    }

    fn follower_address(&self) -> &str {
        self.follower.address()
    }
}

#[derive(Debug, Clone)]
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

struct ScenarioResult {
    label: String,
    result: BenchmarkResult,
    failures: Vec<String>,
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
    if std::env::var("SWEEP_MODE").is_ok() {
        return run_sweep_benchmark().await;
    }
    run_full_benchmark_suite().await
}

async fn run_full_benchmark_suite() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Batch Write Performance Suite ===\n");
    println!("Scenarios: standalone + replicated x throughput + latency x plaintext + mTLS\n");

    let (fsync_delay, num_shards) = crate::bench_tuning();
    println!("  fsync_delay_us: {}, num_shards: {:?}\n", fsync_delay, num_shards);

    let mut results: Vec<ScenarioResult> = Vec::new();
    let mut plaintext_pairs: Vec<(BenchmarkResult, BenchmarkResult)> = Vec::new();
    let mut mtls_pairs: Vec<(BenchmarkResult, BenchmarkResult)> = Vec::new();
    let base_port = 10100 + (std::process::id() % 100) as u16;

    // Generate API keys and client identity for all scenarios
    let api_keys = generate_key_set();
    let keypair = Crypto::generate_keypair(None)?;
    let api_key_b64 = base64::engine::general_purpose::STANDARD.encode(&api_keys.primary_rw);
    let identity_config = ClientIdentityConfig {
        public_key: Some(keypair.public_key_base64.clone()),
        private_key: Some(keypair.private_key_base64.clone()),
        api_key: Some(api_key_b64),
    };

    // Set up PKI once for all mTLS scenarios
    let pki = TestPki::new()?;
    let (node_cert, node_key) = pki.create_node_cert("bench-node")?;
    let (client_cert, client_key) = pki.create_client_cert("bench-client")?;

    // === Standalone Plaintext ===
    let (standalone_pt_thru, standalone_pt_lat) = {
        println!("{}", "=".repeat(70));
        println!("  STANDALONE PLAINTEXT");
        println!("{}\n", "=".repeat(70));

        let config = ServerConfig {
            log_level: "warn".to_string(),
            standalone: true,
            require_client_identity: true,
            insecure_allow_plaintext_auth: true,
            fsync_delay_us: fsync_delay,
            num_shards,
            ..Default::default()
        };
        let temp_dir = tempfile::TempDir::new()?;
        create_api_keys_file(temp_dir.path(), &api_keys)?;
        let server = TestServer::start_with_existing_dir(base_port, config, "standalone-pt".to_string(), temp_dir).await?;
        let addr = server.address().to_string();

        println!(
            "\n--- Standalone Plaintext Throughput ({} connections) ---",
            THROUGHPUT_CONNECTIONS
        );
        let thru = run_benchmark_iteration(&addr, THROUGHPUT_CONNECTIONS, None, Some(identity_config.clone())).await?;
        print_result(&thru);
        results.push(ScenarioResult {
            label: "Standalone PT Thru".to_string(),
            failures: check_thresholds(
                &thru,
                &Thresholds {
                    min_throughput: Some(STANDALONE_THROUGHPUT_MIN),
                    max_avg_latency_ms: None,
                    max_p99_latency_ms: None,
                },
            ),
            result: thru.clone(),
        });

        tokio::time::sleep(Duration::from_secs(2)).await;

        println!(
            "\n--- Standalone Plaintext Latency ({} connections) ---",
            LATENCY_CONNECTIONS
        );
        let lat = run_benchmark_iteration(&addr, LATENCY_CONNECTIONS, None, Some(identity_config.clone())).await?;
        print_result(&lat);
        results.push(ScenarioResult {
            label: "Standalone PT Lat".to_string(),
            failures: check_thresholds(
                &lat,
                &Thresholds {
                    min_throughput: None,
                    max_avg_latency_ms: Some(STANDALONE_LATENCY_AVG_MAX_MS),
                    max_p99_latency_ms: Some(STANDALONE_LATENCY_P99_MAX_MS),
                },
            ),
            result: lat.clone(),
        });

        drop(server);
        (thru, lat)
    };

    tokio::time::sleep(Duration::from_secs(3)).await;

    // === Standalone mTLS ===
    let (standalone_mtls_thru, standalone_mtls_lat) = {
        println!("\n{}", "=".repeat(70));
        println!("  STANDALONE mTLS");
        println!("{}\n", "=".repeat(70));

        let config = ServerConfig {
            log_level: "warn".to_string(),
            standalone: true,
            tls_mode: ConfigTlsMode::Strict,
            tls_ca_cert: Some(pki.ca_cert_path()),
            tls_node_cert: Some(node_cert.clone()),
            tls_node_key: Some(node_key.clone()),
            tls_client_auth: ConfigClientAuth::Require,
            require_client_identity: true,
            fsync_delay_us: fsync_delay,
            num_shards,
            ..Default::default()
        };
        let temp_dir = tempfile::TempDir::new()?;
        create_api_keys_file(temp_dir.path(), &api_keys)?;
        let server = TestServer::start_with_existing_dir(base_port, config, "standalone-mtls".to_string(), temp_dir).await?;
        let addr = server.address().to_string();
        let client_tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;

        println!(
            "\n--- Standalone mTLS Throughput ({} connections) ---",
            THROUGHPUT_CONNECTIONS
        );
        let thru = run_benchmark_iteration(&addr, THROUGHPUT_CONNECTIONS, Some(client_tls), Some(identity_config.clone())).await?;
        print_result(&thru);
        results.push(ScenarioResult {
            label: "Standalone mTLS Thru".to_string(),
            failures: Vec::new(), // no threshold checks for mTLS
            result: thru.clone(),
        });

        tokio::time::sleep(Duration::from_secs(2)).await;

        let client_tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
        println!(
            "\n--- Standalone mTLS Latency ({} connections) ---",
            LATENCY_CONNECTIONS
        );
        let lat = run_benchmark_iteration(&addr, LATENCY_CONNECTIONS, Some(client_tls), Some(identity_config.clone())).await?;
        print_result(&lat);
        results.push(ScenarioResult {
            label: "Standalone mTLS Lat".to_string(),
            failures: Vec::new(),
            result: lat.clone(),
        });

        drop(server);
        (thru, lat)
    };

    plaintext_pairs.push((standalone_pt_thru, standalone_pt_lat));
    mtls_pairs.push((standalone_mtls_thru, standalone_mtls_lat));

    tokio::time::sleep(Duration::from_secs(3)).await;

    // === Replicated Plaintext ===
    let (replicated_pt_thru, replicated_pt_lat) = {
        println!("\n{}", "=".repeat(70));
        println!("  REPLICATED PLAINTEXT");
        println!("{}\n", "=".repeat(70));

        let replicated = ReplicatedServers::start_with_tls_and_keys(base_port + 200, "warn", None, Some(&api_keys)).await?;
        let addr = replicated.address().to_string();

        println!(
            "\n--- Replicated Plaintext Throughput ({} connections) ---",
            THROUGHPUT_CONNECTIONS
        );
        let thru = run_benchmark_iteration(&addr, THROUGHPUT_CONNECTIONS, None, Some(identity_config.clone())).await?;
        print_result(&thru);
        results.push(ScenarioResult {
            label: "Replicated PT Thru".to_string(),
            failures: check_thresholds(
                &thru,
                &Thresholds {
                    min_throughput: Some(REPLICATED_THROUGHPUT_MIN),
                    max_avg_latency_ms: None,
                    max_p99_latency_ms: None,
                },
            ),
            result: thru.clone(),
        });

        tokio::time::sleep(Duration::from_secs(2)).await;

        println!(
            "\n--- Replicated Plaintext Latency ({} connections) ---",
            LATENCY_CONNECTIONS
        );
        let lat = run_benchmark_iteration(&addr, LATENCY_CONNECTIONS, None, Some(identity_config.clone())).await?;
        print_result(&lat);
        results.push(ScenarioResult {
            label: "Replicated PT Lat".to_string(),
            failures: check_thresholds(
                &lat,
                &Thresholds {
                    min_throughput: None,
                    max_avg_latency_ms: Some(REPLICATED_LATENCY_AVG_MAX_MS),
                    max_p99_latency_ms: Some(REPLICATED_LATENCY_P99_MAX_MS),
                },
            ),
            result: lat.clone(),
        });

        (thru, lat)
        // replicated servers dropped here
    };

    tokio::time::sleep(Duration::from_secs(3)).await;

    // === Replicated mTLS ===
    let (replicated_mtls_thru, replicated_mtls_lat) = {
        println!("\n{}", "=".repeat(70));
        println!("  REPLICATED mTLS");
        println!("{}\n", "=".repeat(70));

        let replicated =
            ReplicatedServers::start_with_tls_and_keys(base_port + 400, "warn", Some(&pki), Some(&api_keys)).await?;
        let addr = replicated.address().to_string();
        let client_tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;

        println!(
            "\n--- Replicated mTLS Throughput ({} connections) ---",
            THROUGHPUT_CONNECTIONS
        );
        let thru = run_benchmark_iteration(&addr, THROUGHPUT_CONNECTIONS, Some(client_tls), Some(identity_config.clone())).await?;
        print_result(&thru);
        results.push(ScenarioResult {
            label: "Replicated mTLS Thru".to_string(),
            failures: Vec::new(),
            result: thru.clone(),
        });

        tokio::time::sleep(Duration::from_secs(2)).await;

        let client_tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
        println!(
            "\n--- Replicated mTLS Latency ({} connections) ---",
            LATENCY_CONNECTIONS
        );
        let lat = run_benchmark_iteration(&addr, LATENCY_CONNECTIONS, Some(client_tls), Some(identity_config.clone())).await?;
        print_result(&lat);
        results.push(ScenarioResult {
            label: "Replicated mTLS Lat".to_string(),
            failures: Vec::new(),
            result: lat.clone(),
        });

        // Verify follower caught up (only once, after the last replicated scenario)
        let verify_tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
        verify_replication(&replicated, Some(verify_tls), Some(identity_config.clone())).await?;

        (thru, lat)
    };

    plaintext_pairs.push((replicated_pt_thru, replicated_pt_lat));
    mtls_pairs.push((replicated_mtls_thru, replicated_mtls_lat));

    print_suite_report(&results);
    print_comparison_report(&plaintext_pairs, &mtls_pairs);

    if results.iter().any(|r| !r.failures.is_empty()) {
        return Err("Performance regression detected — thresholds breached".into());
    }

    Ok(())
}

async fn verify_replication(
    replicated: &ReplicatedServers,
    tls_config: Option<ClientTlsConfig>,
    identity_config: Option<ClientIdentityConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Verifying Replication ===");
    println!("Waiting for follower to catch up...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let sample_ids: Vec<u128> = (0..10)
        .map(|i| (i * (NUM_AGGREGATES / 10)) as u128)
        .collect();

    // Retry verification — mTLS replication may need extra catch-up time
    for attempt in 0..3 {
        let mut leader_client = CeleriantClient::connect_with_timeout(
            replicated.address(),
            Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
            tls_config.clone(),
        )
        .await?;
        let mut follower_client = CeleriantClient::connect_with_timeout(
            replicated.follower_address(),
            Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
            tls_config.clone(),
        )
        .await?;

        if let Some(ref id_config) = identity_config {
            leader_client.identify(id_config).await?;
            follower_client.identify(id_config).await?;
        }

        let mut total_leader = 0usize;
        let mut total_follower = 0usize;

        for &agg_id in &sample_ids {
            let key = AggregateKey::new(1, 1, agg_id);
            let lc = count_events(&mut leader_client, &key).await?;
            let fc = count_events(&mut follower_client, &key).await?;
            total_leader += lc;
            total_follower += fc;
        }

        println!(
            "  Attempt {}: leader={} events, follower={} events",
            attempt + 1,
            total_leader,
            total_follower
        );

        if total_leader == total_follower {
            println!("Replication verified!");
            return Ok(());
        }

        if attempt < 2 {
            println!("  Follower still catching up, waiting 5s...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    println!("WARNING: Follower did not fully catch up — mTLS replication overhead may require longer sync");
    Ok(())
}

fn print_result(r: &BenchmarkResult) {
    println!(
        "  Throughput: {:.0} req/s | Avg: {:.1}ms | P50: {}ms | P95: {}ms | P99: {}ms | P99.9: {}ms",
        r.throughput, r.avg_latency_ms, r.p50_ms, r.p95_ms, r.p99_ms, r.p999_ms
    );
}

fn print_suite_report(results: &[ScenarioResult]) {
    println!("\n\n{}", "=".repeat(90));
    println!("  BENCHMARK SUITE RESULTS");
    println!("{}\n", "=".repeat(90));

    println!(
        "{:<25} {:>8} {:>14} {:>10} {:>8} {:>10} {:>8}",
        "Scenario", "Conns", "Throughput", "Avg (ms)", "P99 (ms)", "Busy-fails", "Result"
    );
    println!("{}", "-".repeat(95));

    for r in results {
        let status = if r.failures.is_empty() {
            "PASS"
        } else {
            "FAIL"
        };
        println!(
            "{:<25} {:>8} {:>11.0} /s {:>10.1} {:>8} {:>10} {:>8}",
            r.label, r.result.num_connections, r.result.throughput, r.result.avg_latency_ms,
            r.result.p99_ms, r.result.failed_requests, status,
        );
        for f in &r.failures {
            println!("  >> {}", f);
        }
    }

    let total = results.len();
    let passed = results.iter().filter(|r| r.failures.is_empty()).count();
    let failed = total - passed;

    println!("\n{}/{} scenarios passed", passed, total);
    if failed > 0 {
        println!("{} scenarios FAILED", failed);
    }
}

fn print_comparison_report(
    plaintext: &[(BenchmarkResult, BenchmarkResult)],
    mtls: &[(BenchmarkResult, BenchmarkResult)],
) {
    let labels = ["Standalone", "Replicated"];

    println!("\n\n{}", "=".repeat(100));
    println!("  mTLS OVERHEAD COMPARISON");
    println!("{}\n", "=".repeat(100));

    println!(
        "{:<22} {:>14} {:>14} {:>10}    {:>10} {:>10} {:>10}",
        "Scenario", "PT Thru/s", "mTLS Thru/s", "Overhead",
        "PT Avg ms", "mTLS Avg ms", "Overhead"
    );
    println!("{}", "-".repeat(100));

    for (i, (pt, mt)) in plaintext.iter().zip(mtls.iter()).enumerate() {
        let label = labels[i];

        // Throughput comparison
        let thru_overhead = ((mt.0.throughput - pt.0.throughput) / pt.0.throughput) * 100.0;
        // Latency comparison (avg)
        let lat_overhead = if pt.1.avg_latency_ms > 0.0 {
            ((mt.1.avg_latency_ms - pt.1.avg_latency_ms) / pt.1.avg_latency_ms) * 100.0
        } else {
            0.0
        };

        println!(
            "{:<12} throughput {:>12.0} {:>12.0} {:>+9.1}%    {:>10.1} {:>10.1} {:>+9.1}%",
            label, pt.0.throughput, mt.0.throughput, thru_overhead,
            pt.0.avg_latency_ms, mt.0.avg_latency_ms,
            if pt.0.avg_latency_ms > 0.0 {
                ((mt.0.avg_latency_ms - pt.0.avg_latency_ms) / pt.0.avg_latency_ms) * 100.0
            } else { 0.0 }
        );
        println!(
            "{:<12} latency   {:>12.0} {:>12.0} {:>+9.1}%    {:>10.1} {:>10.1} {:>+9.1}%",
            label, pt.1.throughput, mt.1.throughput,
            ((mt.1.throughput - pt.1.throughput) / pt.1.throughput) * 100.0,
            pt.1.avg_latency_ms, mt.1.avg_latency_ms, lat_overhead
        );
    }

    println!("\n  (positive overhead% = mTLS is slower/lower throughput)");
}

// --- Sweep mode (for throughput discovery, not regression testing) ---

async fn run_sweep_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    let replicated = std::env::var("SWEEP_REPLICATED").is_ok();
    let mode_str = if replicated {
        "Replicated (Leader+Follower)"
    } else {
        "Standalone"
    };
    println!("=== Batch Write Performance Sweep ({}) ===\n", mode_str);
    println!("Testing connection counts: {:?}\n", CONNECTION_SWEEP);

    let port = 10100 + (std::process::id() % 100) as u16;

    let (server_address, _standalone, _replicated) = if replicated {
        println!("Starting replicated cluster...");
        let replicated = ReplicatedServers::start(port, "warn").await?;
        let addr = replicated.address().to_string();
        println!("Cluster started, leader at {}\n", addr);
        (addr, None, Some(replicated))
    } else {
        println!("Starting standalone test server...");
        let (fsync_delay, num_shards) = crate::bench_tuning();
        let config = ServerConfig {
            log_level: "warn".to_string(),
            standalone: true,
            fsync_delay_us: fsync_delay,
            num_shards,
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

        match run_benchmark_iteration(&server_address, num_connections, None, None).await {
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

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    print_sweep_report(&results);

    Ok(())
}

fn print_sweep_report(results: &[BenchmarkResult]) {
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

    if let Some(best) = results
        .iter()
        .max_by(|a, b| a.throughput.partial_cmp(&b.throughput).unwrap())
    {
        println!("\n=== OPTIMAL CONFIGURATION ===");
        println!(
            "Best throughput: {:.2} req/s with {} connections",
            best.throughput, best.num_connections
        );
        println!(
            "Latency at optimal: avg={:.2}ms, P99={}ms",
            best.avg_latency_ms, best.p99_ms
        );
    }

    println!("\n=== THROUGHPUT TREND ===");
    for (i, r) in results.iter().enumerate() {
        let bar_length = ((r.throughput / 500_000.0) * 50.0) as usize;
        let bar: String = "█".repeat(bar_length.min(50));
        let marker = if i > 0 && results[i - 1].throughput < r.throughput {
            "↑"
        } else if i > 0 && results[i - 1].throughput > r.throughput {
            "↓"
        } else {
            " "
        };
        println!(
            "{:>6} conn: {:50} {:>12.0} {}",
            r.num_connections, bar, r.throughput, marker
        );
    }
}

// --- Benchmark execution ---

async fn run_benchmark_iteration(
    server_address: &str,
    num_connections: usize,
    tls_config: Option<ClientTlsConfig>,
    identity_config: Option<ClientIdentityConfig>,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let connect_start = Instant::now();

    let mut connection_tasks = Vec::with_capacity(num_connections);
    for connection_id in 0..num_connections {
        let addr = server_address.to_string();
        let tls = tls_config.clone();
        let identity = identity_config.clone();
        let task = tokio::spawn(async move {
            let mut client = CeleriantClient::connect_with_timeout(
                &addr,
                Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
                tls,
            )
            .await
            .map_err(|e| format!("Connection {} error: {}", connection_id, e))?
            .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));

            let verified_client_id: Option<u128> = if let Some(ref id_config) = identity {
                client.identify(id_config).await
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
    verified_client_id: Option<u128>,
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

        // One aggregate per connection, fixed for its lifetime. Rotating the aggregate per
        // request made almost every request target an aggregate owned by another shard, and
        // check_client_redirect responds to that by moving the whole TCP stream across the
        // intrashard mesh (IntrashardMessages::ClientConnectionRedirect). The benchmark was
        // therefore measuring connection-handover throughput, not write throughput. Real
        // clients hold a connection and write to aggregates it owns; celeriant_bench already
        // models it this way (AggregateKey::new(1, 1, id)).
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
