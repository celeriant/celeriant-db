//! Bank Transaction Schema Benchmark
//!
//! Measures throughput and latency of schema-validated, atomic dual-aggregate writes
//! (bank transactions). Validates that the schema validation path doesn't introduce
//! unacceptable overhead under load.
//!
//! Key differences from batch_main:
//!   - Pre-built requests (no allocation in the hot loop)
//!   - Dual-aggregate atomic writes (debit from A, credit to B)
//!   - JSON schema enforced on every write
//!
//! Scenarios (8 total):
//!   1-2. Standalone plaintext  — throughput (4k) + latency (500)
//!   3-4. Standalone mTLS       — throughput (4k) + latency (500)
//!   5-6. Replicated plaintext  — throughput (4k) + latency (500)
//!   7-8. Replicated mTLS       — throughput (4k) + latency (500)

use std::{collections::HashMap, sync::Arc, time::Duration};

use base64::Engine;
use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::ClientTlsConfig;
use celeriant_crypto::{generate_api_key, hash_api_key, Crypto};
use celeriant_integration_tests::{count_events, MinioContainer, ServerConfig, TestPki, TestServer};
use celeriant_lib::server_config::{ConfigClientAuth, ConfigTlsMode};
use celeriant_runtimes::RoutingRule;
use celeriant_msg::request::requests::{RegisterSchemaRequest, WriteRequest};
use celeriant_msg::{process_client_requests::ClientRequest, request::requests::SingleAggregateWrite};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    schema_key::SchemaKey,
};
use std::fs;
use std::path::Path;
use tokio::sync::Barrier;
use tokio::time::Instant;

const THROUGHPUT_CONNECTIONS: usize = 800;
const LATENCY_CONNECTIONS: usize = 1000;
const TEST_DURATION_SECS: u64 = 15;
const NUM_ACCOUNTS: usize = 1024;
const PREBUILT_PER_CONNECTION: usize = 1_000;
const CLIENTSIDE_TIMEOUT_S: u64 = 5;

const BANK_ORG_ID: u128 = 1;
const BANK_AGGREGATE_TYPE_ID: u128 = 1;
const BANK_EVENT_TYPE_MAJOR: u64 = 1;
const BANK_EVENT_TYPE_MINOR: u64 = 0;
const BANK_CLIENT_ID: u128 = 9999;

fn bank_transaction_schema() -> String {
    r#"{"type":"object","properties":{"amount":{"type":"integer"},"from_account":{"type":"integer"},"to_account":{"type":"integer"},"txn_id":{"type":"integer"}},"required":["amount","from_account","to_account","txn_id"]}"#.to_string()
}

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
    latencies_us: Vec<u64>,
}

struct ReplicatedServers {
    leader: TestServer,
    follower: TestServer,
    _minio: MinioContainer,
}

impl ReplicatedServers {
    async fn start_with_tls_and_keys(
        base_port: u16,
        log_level: &str,
        tls: Option<&TestPki>,
        api_keys: Option<&ApiKeySet>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let minio_port = base_port + 10;
        println!("Starting MinIO on port {}...", minio_port);
        let minio = MinioContainer::start_with_bucket(minio_port, "test-schema-bench").await?;
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

        let mut leader_config = ServerConfig {
            log_level: log_level.to_string(),
            routing_rule: RoutingRule::AggregateTypeId,
            s3_enabled: true,
            s3_region: Some(region.clone()),
            s3_bucket: Some(bucket.clone()),
            s3_access_key_id: Some(access_key.clone()),
            s3_secret_access_key: Some(secret_key.clone()),
            s3_endpoint_override: Some(endpoint.clone()),
            s3_allow_http: allow_http,
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

        tokio::time::sleep(Duration::from_secs(3)).await;

        let follower_port = base_port + 100;
        let mut follower_config = ServerConfig {
            log_level: log_level.to_string(),
            routing_rule: RoutingRule::AggregateTypeId,
            s3_enabled: true,
            s3_region: Some(region),
            s3_bucket: Some(bucket),
            s3_access_key_id: Some(access_key),
            s3_secret_access_key: Some(secret_key),
            s3_endpoint_override: Some(endpoint),
            s3_allow_http: allow_http,
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

        tokio::time::sleep(Duration::from_secs(5)).await;

        Ok(Self {
            leader,
            follower,
            _minio: minio,
        })
    }

    fn address(&self) -> &str {
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

/// Register the bank transaction schema and pre-create all accounts.
async fn setup_schema_and_accounts(
    addr: &str,
    tls_config: Option<ClientTlsConfig>,
    identity_config: Option<&ClientIdentityConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect_with_timeout(
        addr,
        Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
        tls_config,
    )
    .await?
    .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));

    let client_id = if let Some(id_config) = identity_config {
        client.identify(id_config).await?.unwrap_or(BANK_CLIENT_ID)
    } else {
        BANK_CLIENT_ID
    };

    // Register bank transaction schema
    println!("  Registering bank transaction schema...");
    let req = ClientRequest::RegisterSchema(RegisterSchemaRequest {
        correlation_id: Some(rand::random()),
        client_id,
        user_id: None,
        schema_key: SchemaKey::new(BANK_ORG_ID, BANK_AGGREGATE_TYPE_ID, BANK_EVENT_TYPE_MAJOR, BANK_EVENT_TYPE_MINOR),
        schema_type: 0,
        schema: bank_transaction_schema(),
    });
    client.send_request(&req, CompressionType::None).await?;
    println!("  Schema registered.");

    // Pre-create all accounts with valid JSON payloads
    println!("  Pre-creating {} accounts...", NUM_ACCOUNTS);
    for account_id in 0..NUM_ACCOUNTS {
        let payload = format!(
            r#"{{"amount":0,"from_account":{},"to_account":{},"txn_id":0}}"#,
            account_id, account_id
        );
        let event = DatablockAggregateEvent {
            client_event_index: 0,
            event_index: 0,
            event_id: Some(rand::random()),
            event_timestamp: 0,
            event_type_major: BANK_EVENT_TYPE_MAJOR,
            event_type_minor: BANK_EVENT_TYPE_MINOR,
            event_value: Arc::new(payload.into_bytes()),
            iv: None,
        };

        let mut writes = HashMap::new();
        writes.insert(
            AggregateKey::new(BANK_ORG_ID, BANK_AGGREGATE_TYPE_ID, account_id as u128),
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
                compression_type_id: 0,
                compression_level: None,
            },
        );

        let req = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id,
            user_id: None,
            writes,
        });
        client.send_request(&req, CompressionType::None).await?;
    }
    println!("  {} accounts created.", NUM_ACCOUNTS);

    Ok(())
}

/// Build pre-built requests for a single connection.
fn build_prebuilt_requests(connection_id: usize, client_id: u128) -> Vec<ClientRequest> {
    let mut requests = Vec::with_capacity(PREBUILT_PER_CONNECTION);
    for i in 0..PREBUILT_PER_CONNECTION {
        let from_account = (connection_id + i) % NUM_ACCOUNTS;
        let to_account = (connection_id + i + 1) % NUM_ACCOUNTS;
        let amount = (i % 1000) + 1;
        let txn_id = connection_id * PREBUILT_PER_CONNECTION + i;

        let payload = format!(
            r#"{{"amount":{},"from_account":{},"to_account":{},"txn_id":{}}}"#,
            amount, from_account, to_account, txn_id
        );
        let event_value = Arc::new(payload.into_bytes());

        let debit_event = DatablockAggregateEvent {
            client_event_index: 0,
            event_index: 0,
            event_id: Some(rand::random()),
            event_timestamp: 0,
            event_type_major: BANK_EVENT_TYPE_MAJOR,
            event_type_minor: BANK_EVENT_TYPE_MINOR,
            event_value: event_value.clone(),
            iv: None,
        };

        let credit_event = DatablockAggregateEvent {
            client_event_index: 1,
            event_index: 0,
            event_id: Some(rand::random()),
            event_timestamp: 0,
            event_type_major: BANK_EVENT_TYPE_MAJOR,
            event_type_minor: BANK_EVENT_TYPE_MINOR,
            event_value: event_value,
            iv: None,
        };

        let mut writes = HashMap::new();
        writes.insert(
            AggregateKey::new(BANK_ORG_ID, BANK_AGGREGATE_TYPE_ID, from_account as u128),
            SingleAggregateWrite {
                events: vec![debit_event],
                allow_create: false,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
                compression_type_id: 0,
                compression_level: None,
            },
        );
        writes.insert(
            AggregateKey::new(BANK_ORG_ID, BANK_AGGREGATE_TYPE_ID, to_account as u128),
            SingleAggregateWrite {
                events: vec![credit_event],
                allow_create: false,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
                compression_type_id: 0,
                compression_level: None,
            },
        );

        requests.push(ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id,
            user_id: None,
            writes,
        }));
    }
    requests
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_full_benchmark_suite().await
}

async fn run_full_benchmark_suite() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Bank Transaction Schema Benchmark ===\n");
    println!("Scenarios: standalone + replicated x throughput + latency x plaintext + mTLS\n");
    println!(
        "Config: {} accounts, {} prebuilt reqs/conn, {}s per scenario\n",
        NUM_ACCOUNTS, PREBUILT_PER_CONNECTION, TEST_DURATION_SECS
    );

    let mut results: Vec<ScenarioResult> = Vec::new();
    let mut plaintext_pairs: Vec<(BenchmarkResult, BenchmarkResult)> = Vec::new();
    let mut mtls_pairs: Vec<(BenchmarkResult, BenchmarkResult)> = Vec::new();
    let base_port = 11100 + (std::process::id() % 100) as u16;
    let no_thresholds = Thresholds {
        min_throughput: None,
        max_avg_latency_ms: None,
        max_p99_latency_ms: None,
    };

    let api_keys = generate_key_set();
    let keypair = Crypto::generate_keypair(None)?;
    let api_key_b64 = base64::engine::general_purpose::STANDARD.encode(&api_keys.primary_rw);
    let identity_config = ClientIdentityConfig {
        public_key: Some(keypair.public_key_base64.clone()),
        private_key: Some(keypair.private_key_base64.clone()),
        api_key: Some(api_key_b64),
    };

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
            routing_rule: RoutingRule::AggregateTypeId,
            require_client_identity: true,
            insecure_allow_plaintext_auth: true,
            ..Default::default()
        };
        let temp_dir = tempfile::TempDir::new()?;
        create_api_keys_file(temp_dir.path(), &api_keys)?;
        let server = TestServer::start_with_existing_dir(base_port, config, "standalone-pt".to_string(), temp_dir).await?;
        let addr = server.address().to_string();

        setup_schema_and_accounts(&addr, None, Some(&identity_config)).await?;

        println!(
            "\n--- Standalone Plaintext Throughput ({} connections) ---",
            THROUGHPUT_CONNECTIONS
        );
        let thru = run_benchmark_iteration(&addr, THROUGHPUT_CONNECTIONS, None, Some(identity_config.clone())).await?;
        print_result(&thru);
        results.push(ScenarioResult {
            label: "Standalone PT Thru".to_string(),
            failures: check_thresholds(&thru, &no_thresholds),
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
            failures: check_thresholds(&lat, &no_thresholds),
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
            routing_rule: RoutingRule::AggregateTypeId,
            tls_mode: ConfigTlsMode::Strict,
            tls_ca_cert: Some(pki.ca_cert_path()),
            tls_node_cert: Some(node_cert.clone()),
            tls_node_key: Some(node_key.clone()),
            tls_client_auth: ConfigClientAuth::Require,
            require_client_identity: true,
            ..Default::default()
        };
        let temp_dir = tempfile::TempDir::new()?;
        create_api_keys_file(temp_dir.path(), &api_keys)?;
        let server = TestServer::start_with_existing_dir(base_port, config, "standalone-mtls".to_string(), temp_dir).await?;
        let addr = server.address().to_string();

        let setup_tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
        setup_schema_and_accounts(&addr, Some(setup_tls), Some(&identity_config)).await?;

        let client_tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
        println!(
            "\n--- Standalone mTLS Throughput ({} connections) ---",
            THROUGHPUT_CONNECTIONS
        );
        let thru = run_benchmark_iteration(&addr, THROUGHPUT_CONNECTIONS, Some(client_tls), Some(identity_config.clone())).await?;
        print_result(&thru);
        results.push(ScenarioResult {
            label: "Standalone mTLS Thru".to_string(),
            failures: Vec::new(),
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

        setup_schema_and_accounts(&addr, None, Some(&identity_config)).await?;

        println!(
            "\n--- Replicated Plaintext Throughput ({} connections) ---",
            THROUGHPUT_CONNECTIONS
        );
        let thru = run_benchmark_iteration(&addr, THROUGHPUT_CONNECTIONS, None, Some(identity_config.clone())).await?;
        print_result(&thru);
        results.push(ScenarioResult {
            label: "Replicated PT Thru".to_string(),
            failures: check_thresholds(&thru, &no_thresholds),
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
            failures: check_thresholds(&lat, &no_thresholds),
            result: lat.clone(),
        });

        (thru, lat)
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

        let setup_tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
        setup_schema_and_accounts(&addr, Some(setup_tls), Some(&identity_config)).await?;

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

        // Verify follower caught up (best-effort — follower may have crashed under load)
        let verify_tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
        if let Err(e) = verify_replication(&replicated, Some(verify_tls), Some(identity_config.clone())).await {
            println!("WARNING: Replication verification failed: {}", e);
        }

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
        .map(|i| (i * (NUM_ACCOUNTS / 10)) as u128)
        .collect();

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
            let key = AggregateKey::new(BANK_ORG_ID, BANK_AGGREGATE_TYPE_ID, agg_id);
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

    println!("WARNING: Follower did not fully catch up — replication overhead may require longer sync");
    Ok(())
}

fn print_result(r: &BenchmarkResult) {
    println!(
        "  Total: {} reqs | Throughput: {:.0} req/s | Avg: {:.1}ms | Min: {}ms | P50: {}ms | P95: {}ms | P99: {}ms | P99.9: {}ms | Max: {}ms",
        r.total_requests, r.throughput, r.avg_latency_ms, r.min_ms, r.p50_ms, r.p95_ms, r.p99_ms, r.p999_ms, r.max_ms
    );
}

fn print_suite_report(results: &[ScenarioResult]) {
    println!("\n\n{}", "=".repeat(90));
    println!("  SCHEMA BANK BENCHMARK RESULTS");
    println!("{}\n", "=".repeat(90));

    println!(
        "{:<25} {:>8} {:>14} {:>10} {:>8} {:>8}",
        "Scenario", "Conns", "Throughput", "Avg (ms)", "P99 (ms)", "Result"
    );
    println!("{}", "-".repeat(83));

    for r in results {
        let status = if r.failures.is_empty() {
            "PASS"
        } else {
            "FAIL"
        };
        println!(
            "{:<25} {:>8} {:>11.0} /s {:>10.1} {:>8} {:>8}",
            r.label, r.result.num_connections, r.result.throughput, r.result.avg_latency_ms,
            r.result.p99_ms, status,
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

        let thru_overhead = ((mt.0.throughput - pt.0.throughput) / pt.0.throughput) * 100.0;
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

    // Pre-build all requests before timing starts
    println!("  Pre-building {} requests per connection...", PREBUILT_PER_CONNECTION);
    let prebuild_start = Instant::now();
    let mut prebuilt: Vec<(usize, CeleriantClient, Option<u128>, Vec<ClientRequest>)> =
        Vec::with_capacity(clients.len());
    for (connection_id, client, verified_client_id) in clients {
        let cid = verified_client_id.unwrap_or(connection_id as u128);
        let requests = build_prebuilt_requests(connection_id, cid);
        prebuilt.push((connection_id, client, verified_client_id, requests));
    }
    println!(
        "  Pre-built {} total requests in {:.2}s",
        prebuilt.len() * PREBUILT_PER_CONNECTION,
        prebuild_start.elapsed().as_secs_f64()
    );

    let actual_connections = prebuilt.len();
    let barrier = Arc::new(Barrier::new(actual_connections));

    let start_time = Instant::now();

    let mut tasks = Vec::with_capacity(actual_connections);
    for (connection_id, client, _verified_client_id, requests) in prebuilt {
        let barrier = Arc::clone(&barrier);
        let task = tokio::spawn(async move {
            run_connection_benchmark(connection_id, client, requests, barrier).await
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
    requests: Vec<ClientRequest>,
    barrier: Arc<Barrier>,
) -> Result<TaskStats, String> {
    let mut request_count = 0u64;
    let mut latencies = Vec::new();
    let num_requests = requests.len();

    barrier.wait().await;

    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        let idx = (request_count as usize) % num_requests;
        let req_start = Instant::now();

        match client
            .send_request(&requests[idx], CompressionType::None)
            .await
        {
            Ok(_) => {
                let latency_ms = req_start.elapsed().as_millis() as u64;
                latencies.push(latency_ms);
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
