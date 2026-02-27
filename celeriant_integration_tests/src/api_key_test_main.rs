//! API Key Authentication Integration Tests
//!
//! Tests the API key authentication flow with read-only and read-write permissions.
//! Covers six scenarios:
//! 1. Read-write key allows writes
//! 2. Read-only key blocks writes
//! 3. Invalid key rejected
//! 4. Missing key when required
//! 5. Secondary key works
//! 6. Backward compatibility (no API keys)
//!
//! Run with: cargo run --bin api_key_test

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use base64::Engine;
use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_error::ClientError;
use celeriant_crypto::{generate_api_key, hash_api_key};
use celeriant_integration_tests::{ServerConfig, TestServer};
use celeriant_msg::{
    process_requests::Request,
    request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};

struct ApiKeySet {
    primary_rw: [u8; 32],
    primary_rw_hash: [u8; 32],
    secondary_rw: [u8; 32],
    secondary_rw_hash: [u8; 32],
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== API Key Authentication Integration Tests ===\n");

    let base_port = 10200 + (std::process::id() % 100) as u16;

    // Generate API keys for tests
    let keys = generate_key_set();

    // Test 6 first: backward compatibility (no API keys)
    println!("Starting test server (no API keys) on port {}...", base_port);
    let server = TestServer::start_with_port(base_port).await?;
    println!("Server started at {}\n", server.address());

    test_no_api_keys_allows_all(server.address()).await?;

    drop(server);

    // Server WITH api_keys.toml configured
    let enforcing_port = base_port + 2;
    println!(
        "Starting test server (API keys enforced) on port {}...",
        enforcing_port
    );

    let enforcing_config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        insecure_allow_plaintext_auth: true,
        ..Default::default()
    };

    let temp_dir = tempfile::TempDir::new()?;
    create_api_keys_file(temp_dir.path(), &keys)?;

    let enforcing_server =
        TestServer::start_with_existing_dir(enforcing_port, enforcing_config, "api-key-server".to_string(), temp_dir).await?;
    println!("API key server started at {}\n", enforcing_server.address());

    // Test 1: Read-write key allows writes
    test_read_write_key_allows_writes(enforcing_server.address(), &keys.primary_rw).await?;

    // Test 2: Read-only key blocks writes
    test_read_only_key_blocks_writes(enforcing_server.address(), &keys.primary_ro).await?;

    // Test 3: Invalid key rejected
    test_invalid_key_rejected(enforcing_server.address()).await?;

    // Test 4: Missing key when required
    test_missing_key_when_required(enforcing_server.address()).await?;

    // Test 5: Secondary key works
    test_secondary_key_works(enforcing_server.address(), &keys.secondary_rw).await?;

    println!("\n=== All API Key Tests Passed ===");

    Ok(())
}

/// Test 1: Read-write key allows writes
async fn test_read_write_key_allows_writes(
    server_address: &str,
    rw_key: &[u8; 32],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 1: Read-Write Key Allows Writes ===");

    let api_key_b64 = base64::engine::general_purpose::STANDARD.encode(rw_key);
    let identity_config = ClientIdentityConfig {
        public_key: None,
        private_key: None,
        api_key: Some(api_key_b64),
    };

    let mut client = CeleriantClient::connect(server_address).await?;
    client.identify(&identity_config).await?;

    println!("  Authenticated with read-write key");

    // Send WriteRequest - should succeed
    let aggregate_key = AggregateKey::new(1, 2, 100);
    let event = create_test_event(1);
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_event_batch_index: Some(0),
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(1),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&Request::Write(write_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Write(_) => {
            println!("  ✓ Write with read-write key succeeded");
        }
        other => {
            return Err(format!("Expected WriteResponse, got {:?}", other).into());
        }
    }

    println!("  Test 1 PASSED\n");
    Ok(())
}

/// Test 2: Read-only key blocks writes
async fn test_read_only_key_blocks_writes(
    server_address: &str,
    ro_key: &[u8; 32],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 2: Read-Only Key Blocks Writes ===");

    let api_key_b64 = base64::engine::general_purpose::STANDARD.encode(ro_key);
    let identity_config = ClientIdentityConfig {
        public_key: None,
        private_key: None,
        api_key: Some(api_key_b64),
    };

    let mut client = CeleriantClient::connect(server_address).await?;
    client.identify(&identity_config).await?;

    println!("  Authenticated with read-only key");

    // First verify read succeeds
    let aggregate_key = AggregateKey::new(1, 2, 100);
    let read_req = ReadRequest {
        correlation_id: Some(2),
        aggregate_key: aggregate_key.clone(),
        filters: celeriant_msg::request::read_filters::ReadFilters::new(1),
    };

    let read_response = client
        .send_request(&Request::Read(read_req), CompressionType::None)
        .await?;

    match read_response {
        celeriant_msg::process_responses::Response::Read(_) => {
            println!("  ✓ Read with read-only key succeeded");
        }
        other => {
            return Err(format!("Expected ReadResponse, got {:?}", other).into());
        }
    }

    // Now verify write fails with AUTH_INSUFFICIENT_PERMISSIONS (1003)
    let event = create_test_event(2);
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: false,
            expected_event_batch_index: None,
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(3),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let result = client
        .send_request(&Request::Write(write_req), CompressionType::None)
        .await;

    match result {
        Err(ClientError::CeleriantError(err)) => {
            if err.error_code == 1003 {
                println!("  ✓ Received expected error code 1003 (AUTH_INSUFFICIENT_PERMISSIONS)");
                println!("  Error message: {}", err.error_message);
            } else {
                return Err(format!(
                    "Expected error code 1003, got {} - {}",
                    err.error_code, err.error_message
                )
                .into());
            }
        }
        Ok(response) => {
            return Err(format!(
                "Expected AUTH_INSUFFICIENT_PERMISSIONS error, got success: {:?}",
                response
            )
            .into());
        }
        Err(e) => {
            return Err(format!("Unexpected error type: {:?}", e).into());
        }
    }

    println!("  Test 2 PASSED\n");
    Ok(())
}

/// Test 3: Invalid key rejected
async fn test_invalid_key_rejected(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 3: Invalid Key Rejected ===");

    // Generate a random key that doesn't match any in api_keys.toml
    let wrong_key = generate_api_key();
    let api_key_b64 = base64::engine::general_purpose::STANDARD.encode(wrong_key);
    let identity_config = ClientIdentityConfig {
        public_key: None,
        private_key: None,
        api_key: Some(api_key_b64),
    };

    let mut client = CeleriantClient::connect(server_address).await?;

    println!("  Attempting to authenticate with invalid key");

    let result = client.identify(&identity_config).await;

    match result {
        Err(ClientError::CeleriantError(err)) => {
            if err.error_code == 1002 {
                println!("  ✓ Received expected error code 1002 (AUTH_INVALID_KEY)");
                println!("  Error message: {}", err.error_message);
            } else {
                return Err(format!(
                    "Expected error code 1002, got {} - {}",
                    err.error_code, err.error_message
                )
                .into());
            }
        }
        Ok(_) => {
            return Err("Expected AUTH_INVALID_KEY error, got success".into());
        }
        Err(e) => {
            return Err(format!("Unexpected error type: {:?}", e).into());
        }
    }

    println!("  Test 3 PASSED\n");
    Ok(())
}

/// Test 4: Missing key when required
async fn test_missing_key_when_required(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 4: Missing Key When Required ===");

    let identity_config = ClientIdentityConfig {
        public_key: None,
        private_key: None,
        api_key: None,
    };

    let mut client = CeleriantClient::connect(server_address).await?;

    println!("  Attempting to authenticate without API key");

    let result = client.identify(&identity_config).await;

    match result {
        Err(ClientError::CeleriantError(err)) => {
            if err.error_code == 1001 {
                println!("  ✓ Received expected error code 1001 (AUTH_REQUIRED)");
                println!("  Error message: {}", err.error_message);
            } else {
                return Err(format!(
                    "Expected error code 1001, got {} - {}",
                    err.error_code, err.error_message
                )
                .into());
            }
        }
        Ok(_) => {
            return Err("Expected AUTH_REQUIRED error, got success".into());
        }
        Err(e) => {
            return Err(format!("Unexpected error type: {:?}", e).into());
        }
    }

    println!("  Test 4 PASSED\n");
    Ok(())
}

/// Test 5: Secondary key works
async fn test_secondary_key_works(
    server_address: &str,
    secondary_key: &[u8; 32],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 5: Secondary Key Works ===");

    let api_key_b64 = base64::engine::general_purpose::STANDARD.encode(secondary_key);
    let identity_config = ClientIdentityConfig {
        public_key: None,
        private_key: None,
        api_key: Some(api_key_b64),
    };

    let mut client = CeleriantClient::connect(server_address).await?;
    client.identify(&identity_config).await?;

    println!("  Authenticated with secondary read-write key");

    // Send WriteRequest - should succeed
    let aggregate_key = AggregateKey::new(1, 2, 102);
    let event = create_test_event(5);
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_event_batch_index: Some(0),
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(5),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&Request::Write(write_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Write(_) => {
            println!("  ✓ Write with secondary key succeeded");
        }
        other => {
            return Err(format!("Expected WriteResponse, got {:?}", other).into());
        }
    }

    println!("  Test 5 PASSED\n");
    Ok(())
}

/// Test 6: Backward compatibility (no API keys)
async fn test_no_api_keys_allows_all(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 6: Backward Compatibility (No API Keys) ===");

    let mut client = CeleriantClient::connect(server_address).await?;

    println!("  Connected without API key authentication");

    // Send WriteRequest - should succeed (backward compatibility)
    let aggregate_key = AggregateKey::new(1, 2, 103);
    let event = create_test_event(6);
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_event_batch_index: Some(0),
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(6),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&Request::Write(write_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Write(_) => {
            println!("  ✓ Write succeeded without API key (backward compatible)");
        }
        other => {
            return Err(format!("Expected WriteResponse, got {:?}", other).into());
        }
    }

    println!("  Test 6 PASSED\n");
    Ok(())
}

fn create_test_event(event_num: u64) -> DatablockAggregateEvent {
    use std::sync::Arc;
    DatablockAggregateEvent {
        client_event_index: event_num,
        event_index: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"test_event\":{}}}", event_num).into_bytes()),
        iv: None,
    }
}
