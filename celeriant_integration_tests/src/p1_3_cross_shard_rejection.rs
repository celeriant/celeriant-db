//! P1-3: Cross-Shard Write Rejection Integration Test
//!
//! Tests that multi-aggregate writes spanning different shards are properly rejected.
//! Uses a 4-shard standalone cluster with AggregateId routing.
//!
//! Run with: cargo run --bin p1_3_cross_shard_rejection_main

use std::collections::HashMap;
use std::sync::Arc;

use crate::{ServerConfig, TestServer};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::requests::{SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};

const CLIENT_ID: u128 = 99999;
const PORT_BASE: u16 = 19300;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P1-3: Cross-Shard Write Rejection Test ===\n");

    // Start server with 4 shards for cross-shard routing tests
    println!("Starting test server with 4 shards...");
    let config = ServerConfig {
        num_shards: Some(4),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let server = TestServer::start_with_config(PORT_BASE, config).await?;
    let server_address = server.address();
    println!("Server started at {}\n", server_address);

    let mut passed = 0;
    let mut failed = 0;

    // Test 1: Cross-shard write should be rejected
    match test_cross_shard_write_rejection(server_address).await {
        Ok(()) => {
            println!("[PASS] test_cross_shard_write_rejection");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_cross_shard_write_rejection: {}", e);
            failed += 1;
        }
    }

    // Test 2: Same-shard multi-aggregate write should succeed
    match test_same_shard_write_success(server_address).await {
        Ok(()) => {
            println!("[PASS] test_same_shard_write_success");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_same_shard_write_success: {}", e);
            failed += 1;
        }
    }

    // Test 3: Cross-shard write produces no partial writes
    match test_no_partial_writes(server_address).await {
        Ok(()) => {
            println!("[PASS] test_no_partial_writes");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_no_partial_writes: {}", e);
            failed += 1;
        }
    }

    println!("\n=== Results: {} passed, {} failed ===", passed, failed);

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Test that writing to aggregates on different shards is rejected with error code 9002
async fn test_cross_shard_write_rejection(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect(server_address).await?;

    // With 4 shards and aggregate_id routing:
    // aggregate_id % 4 determines the shard
    // 1000 % 4 = 0 (shard 0)
    // 1001 % 4 = 1 (shard 1)
    let agg_shard_0 = AggregateKey::new(1, 1, 1000);
    let agg_shard_1 = AggregateKey::new(1, 1, 1001);

    println!(
        "  Attempting cross-shard write (aggregate {} -> shard 0, aggregate {} -> shard 1)...",
        agg_shard_0.aggregate_id, agg_shard_1.aggregate_id
    );

    let event_0 = create_event(1, "Cross-shard event A".to_string());
    let event_1 = create_event(1, "Cross-shard event B".to_string());

    let mut writes = HashMap::new();
    writes.insert(
        agg_shard_0.clone(),
        SingleAggregateWrite {
            events: vec![event_0],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );
    writes.insert(
        agg_shard_1.clone(),
        SingleAggregateWrite {
            events: vec![event_1],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    let write_request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(1),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    });

    let response = client
        .send_request(&write_request)
        .await;

    match response {
        Err(celeriant_client_tokio::client_error::ClientError::Server(
            celeriant_client_tokio::server_error::ServerError::ShardRouting { error_code: 9002, ref error_message }
        )) => {
            println!("  Received ShardRouting::IncompatibleFilters error");
            println!("  Error message: {}", error_message);

            if !error_message.contains("spans multiple shards") {
                return Err(format!(
                    "Error message doesn't match expected pattern. Got: {}",
                    error_message
                )
                .into());
            }

            println!("  Cross-shard write correctly rejected");
            Ok(())
        }
        Ok(_) => Err("Expected error response, but write succeeded!".into()),
        Err(e) => Err(format!("Unexpected error type: {}", e).into()),
    }
}

/// Test that writing to multiple aggregates on the same shard succeeds
async fn test_same_shard_write_success(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect(server_address).await?;

    // Both route to shard 0: 2000 % 4 = 0, 2004 % 4 = 0
    let agg_a = AggregateKey::new(1, 1, 2000);
    let agg_b = AggregateKey::new(1, 1, 2004);

    println!(
        "  Attempting same-shard write (aggregates {} and {} both -> shard 0)...",
        agg_a.aggregate_id, agg_b.aggregate_id
    );

    let event_a = create_event(1, "Same-shard event A".to_string());
    let event_b = create_event(1, "Same-shard event B".to_string());

    let mut writes = HashMap::new();
    writes.insert(
        agg_a.clone(),
        SingleAggregateWrite {
            events: vec![event_a],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );
    writes.insert(
        agg_b.clone(),
        SingleAggregateWrite {
            events: vec![event_b],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    let write_request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(2),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    });

    let response = client
        .send_request(&write_request)
        .await?;

    match response {
        ClientResponse::Write(write_resp) => {
            println!("  Write response: {:?}", write_resp);
            println!("  Same-shard multi-aggregate write succeeded");
            Ok(())
        }
        other => Err(format!("Expected Write response, got: {:?}", other).into()),
    }
}

/// Test that a rejected cross-shard write doesn't partially write to either shard
async fn test_no_partial_writes(server_address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect(server_address).await?;

    // Use fresh aggregates: 3000 % 4 = 0, 3001 % 4 = 1
    let agg_shard_0 = AggregateKey::new(1, 1, 3000);
    let agg_shard_1 = AggregateKey::new(1, 1, 3001);

    println!(
        "  Testing no partial writes for cross-shard attempt (aggregates {} and {})...",
        agg_shard_0.aggregate_id, agg_shard_1.aggregate_id
    );

    // Verify aggregates don't exist yet (should return error 1001)
    let exists_req_0 = ClientRequest::AggregateDetails(celeriant_msg::request::requests::AggregateDetailsRequest {
        aggregate_key: agg_shard_0.clone(),
        correlation_id: Some(10),
    });
    let exists_req_1 = ClientRequest::AggregateDetails(celeriant_msg::request::requests::AggregateDetailsRequest {
        aggregate_key: agg_shard_1.clone(),
        correlation_id: Some(11),
    });

    match client
        .send_request(&exists_req_0)
        .await
    {
        Ok(_) => return Err("Aggregate on shard 0 already exists before test!".into()),
        Err(celeriant_client_tokio::client_error::ClientError::Server(
            celeriant_client_tokio::server_error::ServerError::Details {
                kind: celeriant_client_tokio::server_error::DetailsError::AggregateNotExists, ..
            }
        )) => {}
        Err(e) => return Err(format!("Unexpected error for Exists check: {}", e).into()),
    }

    match client
        .send_request(&exists_req_1)
        .await
    {
        Ok(resp) => return Err(format!("Aggregate on shard 1 already exists before test! Response: {:?}", resp).into()),
        Err(celeriant_client_tokio::client_error::ClientError::Server(
            celeriant_client_tokio::server_error::ServerError::Details {
                kind: celeriant_client_tokio::server_error::DetailsError::AggregateNotExists, ..
            }
        )) => {}
        Err(e) => return Err(format!("Unexpected error for Exists check: {}", e).into()),
    }

    println!("  Both aggregates confirmed non-existent before cross-shard write");

    // Attempt cross-shard write (should be rejected)
    let event_0 = create_event(1, "Partial write test A".to_string());
    let event_1 = create_event(1, "Partial write test B".to_string());

    let mut writes = HashMap::new();
    writes.insert(
        agg_shard_0.clone(),
        SingleAggregateWrite {
            events: vec![event_0],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );
    writes.insert(
        agg_shard_1.clone(),
        SingleAggregateWrite {
            events: vec![event_1],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    let write_request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(12),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    });

    let _ = client
        .send_request(&write_request)
        .await;

    println!("  Cross-shard write rejected (as expected)");

    // Verify neither aggregate was created (both should still return error 1001)
    match client
        .send_request(&exists_req_0)
        .await
    {
        Ok(_) => return Err("Aggregate on shard 0 was created despite rejection!".into()),
        Err(celeriant_client_tokio::client_error::ClientError::Server(
            celeriant_client_tokio::server_error::ServerError::Details {
                kind: celeriant_client_tokio::server_error::DetailsError::AggregateNotExists, ..
            }
        )) => {}
        Err(e) => {
            return Err(format!("Unexpected error for post-write Exists check: {}", e).into())
        }
    }

    match client
        .send_request(&exists_req_1)
        .await
    {
        Ok(_) => return Err("Aggregate on shard 1 was created despite rejection!".into()),
        Err(celeriant_client_tokio::client_error::ClientError::Server(
            celeriant_client_tokio::server_error::ServerError::Details {
                kind: celeriant_client_tokio::server_error::DetailsError::AggregateNotExists, ..
            }
        )) => {}
        Err(e) => {
            return Err(format!("Unexpected error for post-write Exists check: {}", e).into())
        }
    }

    println!("  Verified: neither aggregate was created (no partial writes)");
    Ok(())
}

fn create_event(client_seq: u64, message: String) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq,
        event_seq: 0,
        event_id: Some(rand::random()),
        event_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(message.into_bytes()),
        iv: None,
    }
}
