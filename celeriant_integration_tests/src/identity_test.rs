//! Client Identity Verification Integration Tests
//!
//! Tests the identity verification handshake and enforcement.
//! Covers five scenarios:
//! 1. Successful identity verification
//! 2. Identity mismatch rejection
//! 3. Backward compatibility (no identity enforcement when not required)
//! 4. Enforcement mode: unidentified client rejected
//! 5. Enforcement mode: identified client succeeds
//!
//! Run with: cargo run --bin identity_test_main

use std::collections::HashMap;
use std::sync::Arc;

use crate::{ServerConfig, TestServer};
use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_error::ClientError;
use celeriant_crypto::Crypto;
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::requests::{SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Client Identity Verification Integration Tests ===\n");

    let base_port = 10100 + (std::process::id() % 100) as u16;

    // Server WITHOUT require_client_identity (default)
    println!("Starting test server (no enforcement) on port {}...", base_port);
    let server = TestServer::start_with_port(base_port).await?;
    println!("Server started at {}\n", server.address());

    // Test 1: Successful identity verification
    test_successful_identity_verification(server.address()).await?;

    // Test 2: Identity mismatch rejection
    test_identity_mismatch_rejection(server.address()).await?;

    // Test 3: Backward compatibility (no identity)
    test_backward_compatibility_no_identity(server.address()).await?;

    // Server WITH require_client_identity = true.
    // Skip past the first server's metrics port (`base_port + 2`).
    let enforcing_port = base_port + 4;
    println!("Starting test server (enforcement ON) on port {}...", enforcing_port);
    let enforcing_config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        require_client_identity: true,
        insecure_allow_plaintext_auth: true,
        ..Default::default()
    };
    let enforcing_server = TestServer::start_with_config(enforcing_port, enforcing_config).await?;
    println!("Enforcing server started at {}\n", enforcing_server.address());

    // Test 4: Enforcement rejects unidentified clients
    test_enforcement_rejects_unidentified(enforcing_server.address()).await?;

    // Test 5: Enforcement allows identified clients
    test_enforcement_allows_identified(enforcing_server.address()).await?;

    println!("\n=== All Identity Tests Passed ===");

    Ok(())
}

/// Test 1: Successful identity verification flow
async fn test_successful_identity_verification(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 1: Successful Identity Verification ===");

    // Generate keypair
    let keypair = Crypto::generate_keypair(None)?;
    let identity_config = ClientIdentityConfig {
        public_key: Some(keypair.public_key_base64.clone()),
        private_key: Some(keypair.private_key_base64.clone()),
        api_key: None,
    };

    // Derive expected client_id using validate_with_public_key
    // (we can use this since we have a valid nonce and signature)
    let nonce = Crypto::generate_nonce()?;
    let signature = Crypto::sign_nonce(identity_config.private_key.as_deref().unwrap(), &nonce)?;
    let expected_client_id = Crypto::validate_with_public_key(
        identity_config.public_key.as_deref().unwrap(),
        &nonce,
        &signature,
    )?;

    println!("  Generated keypair, expected client_id: {}", expected_client_id);

    // Connect and identify
    let mut client = CeleriantClient::connect(server_address).await?;
    let verified_client_id = client.identify(&identity_config).await?;

    println!("  Server verified client_id: {:?}", verified_client_id);

    assert_eq!(
        verified_client_id.unwrap(), expected_client_id,
        "Server-returned client_id should match derived client_id"
    );

    // Send WriteRequest with matching client_id - should succeed
    let aggregate_key = AggregateKey::new(1, 2, 100);
    let event = create_test_event(1);
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(1),
        client_id: verified_client_id.unwrap(),
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&ClientRequest::Write(write_req))
        .await?;

    match response {
        ClientResponse::Write(_) => {
            println!("  ✓ Write with matching client_id succeeded");
        }
        other => {
            return Err(format!("Expected WriteResponse, got {:?}", other).into());
        }
    }

    println!("  Test 1 PASSED\n");
    Ok(())
}

/// Test 2: Identity mismatch rejection
async fn test_identity_mismatch_rejection(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 2: Identity Mismatch Rejection ===");

    // Generate keypair and identify as client A
    let keypair = Crypto::generate_keypair(None)?;
    let identity_config = ClientIdentityConfig {
        public_key: Some(keypair.public_key_base64.clone()),
        private_key: Some(keypair.private_key_base64.clone()),
        api_key: None,
    };

    let mut client = CeleriantClient::connect(server_address).await?;
    let client_id_a = client.identify(&identity_config).await?;

    println!("  Verified as client_id: {:?}", client_id_a);

    // Generate a different client_id (client B)
    let client_id_b = client_id_a.unwrap().wrapping_add(1);

    println!("  Attempting write with different client_id: {}", client_id_b);

    // Send WriteRequest with client_id_b - should fail with error 1103
    let aggregate_key = AggregateKey::new(1, 2, 101);
    let event = create_test_event(2);
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(2),
        client_id: client_id_b,
        user_id: Some(888),
        writes,
    };

    let result = client
        .send_request(&ClientRequest::Write(write_req))
        .await;

    match result {
        Err(ClientError::Server(celeriant_client_tokio::server_error::ServerError::Auth { kind: celeriant_client_tokio::server_error::AuthError::Mismatch, ref error_message })) => {
            println!("  ✓ Received expected IDENTIFY_MISMATCH error");
            println!("  Error message: {}", error_message);
        }
        Err(ClientError::Server(ref err)) => {
            return Err(format!("Expected IDENTIFY_MISMATCH, got: {}", err).into());
        }
        Ok(response) => {
            return Err(format!(
                "Expected error for identity mismatch, got success: {:?}",
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

/// Test 3: Backward compatibility - no identity enforcement when not verified
async fn test_backward_compatibility_no_identity(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 3: Backward Compatibility (No Identity) ===");

    // Connect WITHOUT sending IdentifyRequest
    let mut client = CeleriantClient::connect(server_address).await?;

    println!("  Connected without identity verification");

    // Pick an arbitrary client_id
    let arbitrary_client_id: u128 = 42;

    println!("  Sending write with arbitrary client_id: {}", arbitrary_client_id);

    // Send WriteRequest - should succeed (no enforcement without verification)
    let aggregate_key = AggregateKey::new(1, 2, 102);
    let event = create_test_event(3);
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(3),
        client_id: arbitrary_client_id,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&ClientRequest::Write(write_req))
        .await?;

    match response {
        ClientResponse::Write(_) => {
            println!("  ✓ Write succeeded without identity verification");
        }
        other => {
            return Err(format!("Expected WriteResponse, got {:?}", other).into());
        }
    }

    println!("  Test 3 PASSED\n");
    Ok(())
}

/// Test 4: Enforcement mode rejects unidentified clients
async fn test_enforcement_rejects_unidentified(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 4: Enforcement Rejects Unidentified Client ===");

    let mut client = CeleriantClient::connect(server_address).await?;

    println!("  Connected without identity verification");

    // Send a write without identifying first
    let aggregate_key = AggregateKey::new(1, 2, 200);
    let event = create_test_event(4);
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(4),
        client_id: 42,
        user_id: Some(888),
        writes,
    };

    let result = client
        .send_request(&ClientRequest::Write(write_req))
        .await;

    match result {
        Err(ClientError::IdentityRequired) => {
            println!("  ✓ Received IdentityRequired error (10004)");
        }
        Ok(response) => {
            return Err(format!(
                "Expected IdentityRequired error, got success: {:?}",
                response
            )
            .into());
        }
        Err(e) => {
            return Err(format!("Expected IdentityRequired, got: {:?}", e).into());
        }
    }

    println!("  Test 4 PASSED\n");
    Ok(())
}

/// Test 5: Enforcement mode allows identified clients
async fn test_enforcement_allows_identified(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test 5: Enforcement Allows Identified Client ===");

    let keypair = Crypto::generate_keypair(None)?;
    let identity_config = ClientIdentityConfig {
        public_key: Some(keypair.public_key_base64.clone()),
        private_key: Some(keypair.private_key_base64.clone()),
        api_key: None,
    };

    let mut client = CeleriantClient::connect(server_address).await?;
    let verified_client_id = client.identify(&identity_config).await?;

    println!("  Identified as client_id: {:?}", verified_client_id);

    // Send a write with the verified client_id — should succeed
    let aggregate_key = AggregateKey::new(1, 2, 201);
    let event = create_test_event(5);
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(5),
        client_id: verified_client_id.unwrap(),
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&ClientRequest::Write(write_req))
        .await?;

    match response {
        ClientResponse::Write(_) => {
            println!("  ✓ Write succeeded after identity verification");
        }
        other => {
            return Err(format!("Expected WriteResponse, got {:?}", other).into());
        }
    }

    println!("  Test 5 PASSED\n");
    Ok(())
}

fn create_test_event(event_num: u64) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: event_num,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"test_event\":{}}}", event_num).into_bytes()),
        iv: None,
    }
}
