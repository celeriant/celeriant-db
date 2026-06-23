//! Standalone Cleartext Batch Write Benchmark — COMPRESSED payloads.
//!
//! Like `batch_standalone_cleartext`, but each write carries a ~3 KiB compressible payload, well
//! over the client's 1 KiB compression threshold, so every request exercises the zstd-dictionary
//! compression path on the client. Use to measure the throughput cost of the client's stateless
//! per-call dict compression on a write path.
//!
//! Throughput-only (no latency phase, no thresholds): prints req/s and the per-request payload.

use std::{collections::HashMap, sync::Arc, time::Duration};

use base64::Engine;
use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_error::ClientError;
use celeriant_crypto::{generate_api_key, hash_api_key, Crypto};
use crate::{ServerConfig, TestServer};
use celeriant_msg::request::requests::WriteRequest;
use celeriant_msg::{process_client_requests::ClientRequest, request::requests::SingleAggregateWrite};
use celeriant_wal::{aggregate_key::AggregateKey, datablocks::datablock_aggregate_event::DatablockAggregateEvent};
use std::fs;
use std::path::Path;
use tokio::sync::Barrier;
use tokio::time::Instant;

const THROUGHPUT_CONNECTIONS: usize = 8000;
const TEST_DURATION_SECS: u64 = 10;
const NUM_AGGREGATES: usize = 1024;
const CLIENTSIDE_TIMEOUT_S: u64 = 5;

/// A ~3 KiB compressible event value (repeated prose), built once and shared via `Arc`.
fn payload() -> Arc<Vec<u8>> {
    const BLOCK: &str = "Tomorrow, and tomorrow, and tomorrow, creeps in this petty pace from day \
to day, to the last syllable of recorded time; and all our yesterdays have lighted fools the way \
to dusty death. Out, out, brief candle! Life's but a walking shadow. ";
    let mut s = String::with_capacity(3072);
    while s.len() < 3072 {
        s.push_str(BLOCK);
    }
    Arc::new(s.into_bytes())
}

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
        primary_rw,
        primary_rw_hash: hash_api_key(&primary_rw),
        secondary_rw_hash: hash_api_key(&generate_api_key()),
        primary_ro_hash: hash_api_key(&generate_api_key()),
        secondary_ro_hash: hash_api_key(&generate_api_key()),
    }
}

fn create_api_keys_file(data_root: &Path, keys: &ApiKeySet) -> std::io::Result<()> {
    let content = format!(
        "[keys]\nprimary_rw = \"{}\"\nsecondary_rw = \"{}\"\nprimary_ro = \"{}\"\nsecondary_ro = \"{}\"\n",
        hex::encode(keys.primary_rw_hash),
        hex::encode(keys.secondary_rw_hash),
        hex::encode(keys.primary_ro_hash),
        hex::encode(keys.secondary_ro_hash),
    );
    fs::write(data_root.join("api_keys.toml"), content)
}

struct TaskStats {
    request_count: u64,
    latencies_ms: Vec<u64>,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let pl = payload();
    println!("=== Standalone Compressed Batch Write Benchmark ===");
    println!("  payload: {} bytes/event (> 1 KiB threshold → ZstdDict on every write)\n", pl.len());

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
        TestServer::start_with_existing_dir(base_port, config, "standalone-zstd".to_string(), temp_dir).await?;
    let addr = server.address().to_string();

    println!("--- Throughput ({} connections, {}s, compressed) ---", THROUGHPUT_CONNECTIONS, TEST_DURATION_SECS);
    let (throughput, total, avg_ms, p99_ms) =
        run_iteration(&addr, THROUGHPUT_CONNECTIONS, identity_config, pl).await?;
    println!(
        "\n  RESULT: {:.0} req/s | {} requests | avg {:.1}ms | p99 {}ms",
        throughput, total, avg_ms, p99_ms
    );

    drop(server);
    Ok(())
}

async fn run_iteration(
    server_address: &str,
    num_connections: usize,
    identity_config: ClientIdentityConfig,
    payload: Arc<Vec<u8>>,
) -> Result<(f64, u64, f64, u64), Box<dyn std::error::Error>> {
    let connect_start = Instant::now();
    let mut connection_tasks = Vec::with_capacity(num_connections);
    for connection_id in 0..num_connections {
        let addr = server_address.to_string();
        let identity = identity_config.clone();
        connection_tasks.push(tokio::spawn(async move {
            let mut client = CeleriantClient::connect_with_timeout(
                &addr,
                Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
                None,
            )
            .await
            .map_err(|e| format!("Connection {} error: {}", connection_id, e))?
            .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));

            let verified = client
                .identify(&identity)
                .await
                .map_err(|e| format!("Connection {} identify error: {}", connection_id, e))?;
            Ok::<_, String>((connection_id, client, verified))
        }));
    }

    let mut clients = Vec::with_capacity(num_connections);
    let mut failed = 0;
    for task in connection_tasks {
        match task.await {
            Ok(Ok(c)) => clients.push(c),
            _ => failed += 1,
        }
    }
    println!(
        "  Established {} connections in {:.2}s ({} failed)",
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
    for (connection_id, client, verified) in clients {
        let barrier = Arc::clone(&barrier);
        let payload = Arc::clone(&payload);
        tasks.push(tokio::spawn(async move {
            run_connection(connection_id, client, verified, barrier, payload).await
        }));
    }

    let mut all_stats = Vec::with_capacity(actual);
    for task in tasks {
        if let Ok(Ok(stats)) = task.await {
            all_stats.push(stats);
        }
    }
    let elapsed = start.elapsed();

    let total: u64 = all_stats.iter().map(|s| s.request_count).sum();
    let mut lat: Vec<u64> = all_stats.into_iter().flat_map(|s| s.latencies_ms).collect();
    lat.sort_unstable();
    let throughput = total as f64 / elapsed.as_secs_f64();
    let (avg, p99) = if lat.is_empty() {
        (0.0, 0)
    } else {
        (lat.iter().sum::<u64>() as f64 / lat.len() as f64, lat[lat.len() * 99 / 100])
    };
    Ok((throughput, total, avg, p99))
}

async fn run_connection(
    connection_id: usize,
    mut client: CeleriantClient,
    verified_client_id: Option<u128>,
    barrier: Arc<Barrier>,
    payload: Arc<Vec<u8>>,
) -> Result<TaskStats, String> {
    let mut request_count = 0u64;
    let mut latencies = Vec::new();
    barrier.wait().await;
    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        let event = DatablockAggregateEvent {
            client_seq: 3,
            event_seq: 0,
            event_id: Some(1234567890),
            event_timestamp: 0,
            event_type_major: 2,
            event_type_minor: 3,
            event_value: Arc::clone(&payload),
            iv: None,
        };
        let aggregate_id = (connection_id + request_count as usize) % NUM_AGGREGATES;
        let mut writes = HashMap::new();
        writes.insert(
            AggregateKey::new(1, 1, aggregate_id as u128),
            SingleAggregateWrite {
                events: vec![event],
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
        match client.send_request(&request).await {
            Ok(_) => {
                latencies.push(req_start.elapsed().as_millis() as u64);
                request_count += 1;
            }
            Err(ClientError::Server(err)) => eprintln!("Connection {} server error: {}", connection_id, err),
            Err(ClientError::RequestTimeout) => eprintln!("Connection {} timeout", connection_id),
            Err(e) => {
                eprintln!("Connection {} error: {}", connection_id, e);
                break;
            }
        }
    }

    Ok(TaskStats { request_count, latencies_ms: latencies })
}
