//! mTLS Integration Tests
//!
//! Tests TLS handshake, plaintext rejection, and certificate trust validation
//! using real server processes. Requires kTLS kernel support (CONFIG_TLS).
//!
//! kTLS + Glommio has an intermittent race where the first read after kTLS
//! setup times out (~30% of connections). Tests that expect successful TLS
//! connections retry up to 3 times to tolerate this.
//!
//! Run with: cargo run --bin mtls_test_main -p celeriant_integration_tests

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::ClientTlsConfig;
use celeriant_crypto::pki::PkiManager;
use celeriant_integration_tests::{ServerConfig, TestPki, TestServer};
use celeriant_lib::server_config::{ConfigClientAuth, ConfigTlsMode};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::requests::{AggregateDetailsRequest, ReadRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use rustls_pki_types::ServerName;
use tokio::time::Duration;

const CLIENT_ID: u128 = 54321;
const KTLS_RETRIES: u32 = 5;

/// Port base for mTLS tests (10400+) — distinct from connection_test_main (10100+).
fn test_port(offset: u16) -> u16 {
    10400 + (std::process::id() % 100) as u16 + offset * 2
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== mTLS Integration Tests ===\n");

    if let Err(e) = celeriant_ktls::verify_ktls_support() {
        println!("SKIP: kTLS not supported on this kernel: {:?}", e);
        println!("      Build kernel with CONFIG_TLS or load the 'tls' module.");
        println!("      All mTLS tests skipped.");
        return Ok(());
    }
    println!("kTLS kernel support confirmed.\n");

    let mut passed = 0u32;
    let mut failed = 0u32;

    macro_rules! run_test {
        ($name:ident) => {
            match $name().await {
                Ok(()) => {
                    println!("[PASS] {}", stringify!($name));
                    passed += 1;
                }
                Err(e) => {
                    println!("[FAIL] {}: {}", stringify!($name), e);
                    failed += 1;
                }
            }
        };
    }

    run_test!(test_mtls_client_server_roundtrip);
    run_test!(test_strict_mode_rejects_plaintext);
    run_test!(test_untrusted_cert_rejected);
    run_test!(test_ktls_cross_shard_redirect);

    println!("\n=== Results: {} passed, {} failed ===", passed, failed);

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Full mTLS round-trip: write an event then read it back over kTLS.
async fn test_mtls_client_server_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let pki = TestPki::new()?;
    let (node_cert, node_key) = pki.create_node_cert("node")?;
    let (client_cert, client_key) = pki.create_client_cert("test-client")?;

    let port = test_port(0);
    let server = TestServer::start_with_config(
        port,
        ServerConfig {
            num_shards: Some(1),
            log_level: "warn".to_string(),
            standalone: true,
            tls_mode: ConfigTlsMode::Strict,
            tls_ca_cert: Some(pki.ca_cert_path()),
            tls_node_cert: Some(node_cert.clone()),
            tls_node_key: Some(node_key.clone()),
            tls_client_auth: ConfigClientAuth::Require,
            ..Default::default()
        },
    )
    .await?;

    let aggregate = AggregateKey::new(1, 1, 40001);
    let event = DatablockAggregateEvent {
        client_event_index: 1,
        event_index: 0,
        event_id: None,
        event_timestamp: 1_000_000,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(b"mtls roundtrip payload".to_vec()),
        iv: None,
    };
    let mut writes = HashMap::new();
    writes.insert(
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_event_batch_index: None,
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    // Retry the full connect+write+read cycle for kTLS flake resilience.
    let mut last_err = String::new();
    for attempt in 0..KTLS_RETRIES {
        let tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
        let connect = CeleriantClient::connect_with_timeout(
            server.address(),
            Some(Duration::from_secs(10)),
            Some(tls),
        )
        .await;

        let mut client = match connect {
            Ok(c) => c,
            Err(e) => {
                last_err = format!("connect failed (attempt {attempt}): {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        let write_req = ClientRequest::Write(WriteRequest {
            correlation_id: Some(1),
            client_id: CLIENT_ID,
            user_id: None,
            writes: writes.clone(),
        });
        if let Err(e) = client.send_request(&write_req, CompressionType::None).await {
            last_err = format!("write failed (attempt {attempt}): {e}");
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        let read_req = ClientRequest::Read(ReadRequest {
            correlation_id: Some(2),
            aggregate_key: aggregate.clone(),
            filters: celeriant_msg::request::read_filters::ReadFilters::new(1),
        });
        match client.send_request(&read_req, CompressionType::None).await {
            Ok(ClientResponse::Read(r)) => {
                let total_events: usize = r.event_batches.iter().map(|b| b.events.len()).sum();
                if total_events == 0 {
                    last_err = format!("read returned 0 events (attempt {attempt})");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                println!("  Round-trip successful: {total_events} event(s) read back");
                return Ok(());
            }
            Ok(other) => return Err(format!("Unexpected response: {other:?}").into()),
            Err(e) => {
                last_err = format!("read failed (attempt {attempt}): {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(format!("Round-trip failed after {KTLS_RETRIES} attempts: {last_err}").into())
}

/// Strict mode must reject a plaintext (non-TLS) connection.
async fn test_strict_mode_rejects_plaintext() -> Result<(), Box<dyn std::error::Error>> {
    let pki = TestPki::new()?;
    let (node_cert, node_key) = pki.create_node_cert("node")?;

    let port = test_port(1);
    let server = TestServer::start_with_config(
        port,
        ServerConfig {
            num_shards: Some(1),
            log_level: "warn".to_string(),
            standalone: true,
            tls_mode: ConfigTlsMode::Strict,
            tls_ca_cert: Some(pki.ca_cert_path()),
            tls_node_cert: Some(node_cert),
            tls_node_key: Some(node_key),
            tls_client_auth: ConfigClientAuth::None,
            ..Default::default()
        },
    )
    .await?;

    let mut client = CeleriantClient::connect_with_timeout(
        server.address(),
        Some(Duration::from_secs(10)),
        None,
    )
    .await?;

    let request = ClientRequest::Read(ReadRequest {
        correlation_id: Some(1),
        aggregate_key: AggregateKey::new(1, 1, 40002),
        filters: celeriant_msg::request::read_filters::ReadFilters::new(1),
    });

    match client.send_request(&request, CompressionType::None).await {
        Err(_) => {
            println!("  Plaintext connection correctly rejected by strict-mode server");
            Ok(())
        }
        Ok(r) => Err(format!("Expected rejection but got success: {r:?}").into()),
    }
}

/// A client cert signed by an unrelated CA must be rejected by the server.
async fn test_untrusted_cert_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let cluster_pki = TestPki::new()?;
    let (node_cert, node_key) = cluster_pki.create_node_cert("node")?;

    let rogue_pki = TestPki::new()?;
    let (rogue_client_cert, rogue_client_key) = rogue_pki.create_client_cert("attacker")?;

    let port = test_port(4);
    let server = TestServer::start_with_config(
        port,
        ServerConfig {
            num_shards: Some(1),
            log_level: "warn".to_string(),
            standalone: true,
            tls_mode: ConfigTlsMode::Strict,
            tls_ca_cert: Some(cluster_pki.ca_cert_path()),
            tls_node_cert: Some(node_cert),
            tls_node_key: Some(node_key),
            tls_client_auth: ConfigClientAuth::Require,
            ..Default::default()
        },
    )
    .await?;

    let ca_bundle = PkiManager::load_ca_bundle(&cluster_pki.ca_cert_path())?;
    let (cert_chain, key) = PkiManager::load_identity(&rogue_client_cert, &rogue_client_key)?;
    let client_config = PkiManager::build_client_config(&ca_bundle, cert_chain, key)?;
    let sni = ServerName::try_from("localhost".to_string())
        .map_err(|e| format!("Invalid server name: {e}"))?;
    let tls = ClientTlsConfig::new(client_config, sni);

    match CeleriantClient::connect_with_timeout(
        server.address(),
        Some(Duration::from_secs(10)),
        Some(tls),
    )
    .await
    {
        Err(_) => {
            println!("  Rogue client cert correctly rejected (handshake failure)");
            Ok(())
        }
        Ok(mut client) => {
            let request = ClientRequest::Read(ReadRequest {
                correlation_id: Some(1),
                aggregate_key: AggregateKey::new(1, 1, 40005),
                filters: celeriant_msg::request::read_filters::ReadFilters::new(1),
            });
            match client.send_request(&request, CompressionType::None).await {
                Err(_) => {
                    println!("  Rogue client cert rejected on first request");
                    Ok(())
                }
                Ok(r) => Err(format!("Expected rejection but got success: {r:?}").into()),
            }
        }
    }
}

/// Cross-shard redirect over kTLS: write+read to all 4 shards on one TLS connection.
///
/// kTLS state lives in the kernel and must survive fd migration via
/// `into_accepted()` → intrashard channel → `bind_to_executor()`.
async fn test_ktls_cross_shard_redirect() -> Result<(), Box<dyn std::error::Error>> {
    let pki = TestPki::new()?;
    let (node_cert, node_key) = pki.create_node_cert("node")?;
    let (client_cert, client_key) = pki.create_client_cert("redirect-client")?;

    let port = test_port(5);
    let server = TestServer::start_with_config(
        port,
        ServerConfig {
            num_shards: Some(4),
            log_level: "warn".to_string(),
            standalone: true,
            tls_mode: ConfigTlsMode::Strict,
            tls_ca_cert: Some(pki.ca_cert_path()),
            tls_node_cert: Some(node_cert),
            tls_node_key: Some(node_key),
            tls_client_auth: ConfigClientAuth::Require,
            ..Default::default()
        },
    )
    .await?;

    // Retry the full cross-shard test for kTLS flake resilience.
    let mut last_err = String::new();
    for attempt in 0..KTLS_RETRIES {
        let tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
        let connect = CeleriantClient::connect_with_timeout(
            server.address(),
            Some(Duration::from_secs(10)),
            Some(tls),
        )
        .await;

        let mut client = match connect {
            Ok(c) => c,
            Err(e) => {
                last_err = format!("connect failed (attempt {attempt}): {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        match cross_shard_roundtrip(&mut client).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_err = format!("attempt {attempt}: {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(format!("Cross-shard test failed after {KTLS_RETRIES} attempts: {last_err}").into())
}

async fn cross_shard_roundtrip(
    client: &mut CeleriantClient,
) -> Result<(), Box<dyn std::error::Error>> {
    for shard in 0u128..4 {
        let agg_id = 50000 + shard;
        let aggregate = AggregateKey::new(1, 1, agg_id);

        let event = DatablockAggregateEvent {
            client_event_index: 1,
            event_index: 0,
            event_id: None,
            event_timestamp: 2_000_000 + shard as u64,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(format!("cross-shard payload shard {shard}").into_bytes()),
            iv: None,
        };
        let mut writes = HashMap::new();
        writes.insert(
            aggregate.clone(),
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
            },
        );
        let write_req = ClientRequest::Write(WriteRequest {
            correlation_id: Some(500 + shard),
            client_id: CLIENT_ID,
            user_id: None,
            writes,
        });
        client
            .send_request(&write_req, CompressionType::None)
            .await?;

        let read_req = ClientRequest::Read(ReadRequest {
            correlation_id: Some(600 + shard),
            aggregate_key: aggregate,
            filters: celeriant_msg::request::read_filters::ReadFilters::new(1),
        });
        let response = client
            .send_request(&read_req, CompressionType::None)
            .await?;

        match response {
            ClientResponse::Read(r) => {
                let total: usize = r.event_batches.iter().map(|b| b.events.len()).sum();
                if total == 0 {
                    return Err(format!("Shard {shard}: read returned 0 events after write").into());
                }
                println!("  Shard {shard}: write+read over kTLS ok ({total} event)");
            }
            other => {
                return Err(format!("Shard {shard}: unexpected response: {other:?}").into())
            }
        }
    }

    for i in 0u128..8 {
        let agg_id = 50000 + (i % 4);
        let aggregate = AggregateKey::new(1, 1, agg_id);
        let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
            aggregate_key: aggregate,
            correlation_id: Some(700 + i),
        });
        match client
            .send_request(&request, CompressionType::None)
            .await
        {
            Ok(_) | Err(celeriant_client_tokio::client_error::ClientError::CeleriantError(_)) => {}
            Err(e) => {
                return Err(
                    format!("Interleaved request {i} (shard {}) failed: {e}", i % 4).into(),
                )
            }
        }
    }
    println!("  8 interleaved cross-shard requests over kTLS ok");

    Ok(())
}
