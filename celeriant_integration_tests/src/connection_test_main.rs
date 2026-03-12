//! Connection Handler Integration Tests
//!
//! Tests connection lifecycle, pipelining, and shard routing behavior.
//! Creates a temporary data directory and spawns the server automatically with 4 shards.
//!
//! Run with: cargo run --bin connection_test_main

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_integration_tests::{ServerConfig, TestServer};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::read_filters::ReadFilters,
    request::requests::{AggregateDetailsRequest, ReadRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use tokio::time::Duration;

use celeriant_client_tokio::client_error::ClientError;

const CLIENT_ID: u128 = 12345;

/// Send a request and treat server-level errors (like "aggregate not found") as success.
/// Only transport/protocol errors are propagated.
async fn send_probe(
    client: &mut CeleriantClient,
    request: &ClientRequest,
) -> Result<(), ClientError> {
    match client.send_request(request, CompressionType::None).await {
        Ok(_) | Err(ClientError::Server(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Connection Handler Integration Tests ===\n");

    // Start server with 4 shards for cross-shard routing tests
    println!("Starting test server with 4 shards...");
    let config = ServerConfig {
        num_shards: Some(4),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let port = 10100 + (std::process::id() % 100) as u16;
    let server = TestServer::start_with_config(port, config).await?;
    let server_address = server.address();
    println!("Server started at {}\n", server_address);

    let mut passed = 0;
    let mut failed = 0;

    // Test 1: Basic connection and single request
    match test_single_request(server_address).await {
        Ok(()) => {
            println!("[PASS] test_single_request");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_single_request: {}", e);
            failed += 1;
        }
    }

    // Test 2: Connection pipelining - multiple requests on same connection
    match test_pipelining(server_address).await {
        Ok(()) => {
            println!("[PASS] test_pipelining");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_pipelining: {}", e);
            failed += 1;
        }
    }

    // Test 3: Requests routing to different shards on same connection
    match test_cross_shard_routing(server_address).await {
        Ok(()) => {
            println!("[PASS] test_cross_shard_routing");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_cross_shard_routing: {}", e);
            failed += 1;
        }
    }

    // Test 4: Mixed read/write operations maintaining connection
    match test_mixed_operations(server_address).await {
        Ok(()) => {
            println!("[PASS] test_mixed_operations");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_mixed_operations: {}", e);
            failed += 1;
        }
    }

    // Test 5: Multiple connections in parallel
    match test_parallel_connections(server_address).await {
        Ok(()) => {
            println!("[PASS] test_parallel_connections");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_parallel_connections: {}", e);
            failed += 1;
        }
    }

    // Test 6: Rapid connection open/close cycles
    match test_connection_churn(server_address).await {
        Ok(()) => {
            println!("[PASS] test_connection_churn");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_connection_churn: {}", e);
            failed += 1;
        }
    }

    // Test 7: Request that routes to specific shards
    match test_shard_affinity(server_address).await {
        Ok(()) => {
            println!("[PASS] test_shard_affinity");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_shard_affinity: {}", e);
            failed += 1;
        }
    }

    // Test 8: Long-lived connection with many requests
    match test_long_lived_connection(server_address).await {
        Ok(()) => {
            println!("[PASS] test_long_lived_connection");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_long_lived_connection: {}", e);
            failed += 1;
        }
    }

    println!("\n=== Results: {} passed, {} failed ===", passed, failed);

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Test basic connection establishment and single request/response
async fn test_single_request(server_address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect(server_address).await?;

    let aggregate = AggregateKey::new(1, 1, 1000);
    let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
        aggregate_key: aggregate,
        correlation_id: Some(1),
    });

    send_probe(&mut client, &request).await?;
    println!("  Single request round-trip successful");

    Ok(())
}

/// Test that multiple requests work on the same connection (pipelining)
async fn test_pipelining(server_address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect(server_address).await?;

    // Send 10 requests on the same connection
    for i in 0..10 {
        // All the same shard as we route by aggregate
        let aggregate = AggregateKey::new(1, i, 2000);
        let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
            aggregate_key: aggregate.clone(),
            correlation_id: Some(i as u128),
        });

        send_probe(&mut client, &request).await?;
        println!("  Pipeline request {} ok", i);
    }

    println!("  Successfully sent 10 requests on single connection");
    Ok(())
}

/// Test requests that route to different shards (4 shards with aggregate_id routing)
async fn test_cross_shard_routing(server_address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect(server_address).await?;

    // With 4 shards and aggregate_id routing:
    // aggregate_id 0 -> shard 0
    // aggregate_id 1 -> shard 1
    // aggregate_id 2 -> shard 2
    // aggregate_id 3 -> shard 3
    // aggregate_id 4 -> shard 0 (wraps)

    let shard_targets = vec![
        (3000, 0), // aggregate_id 3000 % 4 = 0
        (3001, 1), // aggregate_id 3001 % 4 = 1
        (3002, 2), // aggregate_id 3002 % 4 = 2
        (3003, 3), // aggregate_id 3003 % 4 = 3
    ];

    for (agg_id, expected_shard) in shard_targets {
        let aggregate = AggregateKey::new(1, 1, agg_id);
        let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
            aggregate_key: aggregate.clone(),
            correlation_id: Some(agg_id as u128),
        });

        send_probe(&mut client, &request).await?;
        println!("  Aggregate {} (should route to shard {}) -> ok", agg_id, expected_shard);

        send_probe(&mut client, &request).await?;
        println!("  Aggregate {} (should route to shard {}) -> ok (repeat)", agg_id, expected_shard);
    }

    println!("  Cross-shard routing successful on single connection");
    Ok(())
}

/// Test mixed read/write operations on same connection
async fn test_mixed_operations(server_address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect(server_address).await?;

    let aggregate = AggregateKey::new(1, 1, 4000);

    // Write an event
    let event = create_event(0, "Mixed ops test event".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_event_batch_index: None, // Don't enforce version
            enforce_client_idempotency: false,
            compression_type_id: 0,
            compression_level: None,
        },
    );

    let write_request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(100),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    });

    let write_response = client
        .send_request(&write_request, CompressionType::None)
        .await?;
    println!("  Write response: {:?}", write_response);

    // Check exists on same connection
    let exists_request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
        aggregate_key: aggregate.clone(),
        correlation_id: Some(101),
    });

    let exists_response = client
        .send_request(&exists_request, CompressionType::None)
        .await?;
    println!("  Exists response: {:?}", exists_response);

    // Read on same connection
    let read_request = ClientRequest::Read(ReadRequest {
        correlation_id: Some(102),
        aggregate_key: aggregate.clone(),
        filters: ReadFilters::new(1),
    });

    let read_response = client
        .send_request(&read_request, CompressionType::None)
        .await?;
    println!("  Read response: {:?}", read_response);

    println!("  Mixed operations successful");
    Ok(())
}

/// Test multiple parallel connections
async fn test_parallel_connections(server_address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let num_connections = 10;
    let mut handles = Vec::new();

    for conn_id in 0..num_connections {
        let address = server_address.to_string();
        let handle = tokio::spawn(async move {
            let mut client = CeleriantClient::connect(&address).await?;

            for req_id in 0..5 {
                let aggregate = AggregateKey::new(1, 1, 5000 + conn_id * 100 + req_id);
                let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
                    aggregate_key: aggregate,
                    correlation_id: Some((conn_id * 100 + req_id) as u128),
                });

                send_probe(&mut client, &request).await?;
            }

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        handles.push(handle);
    }

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => println!("  Connection {} completed", i),
            Ok(Err(e)) => return Err(format!("Connection {} failed: {}", i, e).into()),
            Err(e) => return Err(format!("Connection {} panicked: {}", i, e).into()),
        }
    }

    println!("  {} parallel connections successful", num_connections);
    Ok(())
}

/// Test rapid connection open/close to stress connection handling
async fn test_connection_churn(server_address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let num_cycles = 20;

    for cycle in 0..num_cycles {
        let mut client = CeleriantClient::connect(server_address).await?;

        let aggregate = AggregateKey::new(1, 1, 6000 + cycle);
        let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
            aggregate_key: aggregate,
            correlation_id: Some(cycle as u128),
        });

        send_probe(&mut client, &request).await?;
        // Connection drops here when client goes out of scope
    }

    println!("  {} connection cycles completed", num_cycles);
    Ok(())
}

/// Test that requests consistently route to expected shards
async fn test_shard_affinity(server_address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut write_client = CeleriantClient::connect(server_address).await?;
    let mut read_client = CeleriantClient::connect(server_address).await?;

    // Create aggregates on specific shards and verify they can be read back
    // Using aggregate_id routing with 4 shards

    for shard in 0..4u128 {
        let aggregate_id = 7000 + shard; // 7000 % 4 = 0, 7001 % 4 = 1, etc.
        let aggregate = AggregateKey::new(1, 1, aggregate_id);

        // Write using first connection
        let event = create_event(0, format!("Shard {} affinity test", shard));
        let mut writes = HashMap::new();
        writes.insert(
            aggregate.clone(),
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
                compression_type_id: 0,
                compression_level: None,
            },
        );

        let write_request = ClientRequest::Write(WriteRequest {
            correlation_id: Some(200 + shard),
            client_id: CLIENT_ID,
            user_id: None,
            writes,
        });

        write_client
            .send_request(&write_request, CompressionType::None)
            .await?;

        // Read back using second connection
        let read_request = ClientRequest::Read(ReadRequest {
            correlation_id: Some(300 + shard),
            aggregate_key: aggregate.clone(),
            filters: ReadFilters::new(1),
        });

        let response = read_client
            .send_request(&read_request, CompressionType::None)
            .await?;
        println!(
            "  Shard {} write/read cycle successful: {:?}",
            shard, response
        );
    }

    println!("  Shard affinity test passed");
    Ok(())
}

/// Test a long-lived connection with many requests
async fn test_long_lived_connection(
    server_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client =
        CeleriantClient::connect_with_timeout(server_address, Some(Duration::from_secs(30)), None)
            .await?;

    let num_requests = 100;

    for i in 0..num_requests {
        let aggregate = AggregateKey::new(1, 1, 8000 + i);
        let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
            aggregate_key: aggregate,
            correlation_id: Some(i as u128),
        });

        send_probe(&mut client, &request).await?;

        if (i + 1) % 25 == 0 {
            println!("  Completed {} requests on long-lived connection", i + 1);
        }
    }

    println!(
        "  {} requests on single long-lived connection successful",
        num_requests
    );
    Ok(())
}

fn create_event(client_event_index: u64, message: String) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_event_index,
        event_index: 0, // Server will assign
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
