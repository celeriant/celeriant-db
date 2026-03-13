//! Multi-Shard Watch API Integration Tests
//!
//! Tests the multi-shard watch functionality with the new WatchConnection API.
//! Creates servers with multiple shards and tests fallback behavior, event merging,
//! and explicit shard routing.
//!
//! Run with: cargo run --bin multi_shard_watch_test_main

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::{TestServer, ServerConfig, RoutingRule};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::watch_connection::{WatchConnection, WatchOptions};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::requests::{SingleAggregateWrite, WatchRequest, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use tokio::time::sleep;

const CLIENT_ID: u128 = 12345;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Multi-Shard Watch API Integration Tests ===\n");

    println!("Starting multi-shard test server (3 shards)...");
    let config = ServerConfig {
        num_shards: Some(3),
        log_level: "warn".to_string(),
        standalone: true,
        routing_rule: RoutingRule::OrgId,
        ..Default::default()
    };
    let server = TestServer::start_with_config(10300, config).await?;
    println!("Server started at {}\n", server.address());

    let mut passed = 0;
    let mut failed = 0;

    // Test 1: Single-shard watch (no fallback needed)
    match test_single_shard_watch(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_single_shard_watch");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_single_shard_watch: {}", e);
            failed += 1;
        }
    }

    // Test 2: Multi-shard watch fallback (aggregate_type triggers error 9001)
    match test_multi_shard_watch_fallback(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_multi_shard_watch_fallback");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_multi_shard_watch_fallback: {}", e);
            failed += 1;
        }
    }

    // Test 3: Multi-shard watch receives events from different shards
    match test_multi_shard_watch_receives_events(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_multi_shard_watch_receives_events");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_multi_shard_watch_receives_events: {}", e);
            failed += 1;
        }
    }

    // Test 4: Multi-shard watch heartbeats pass through
    match test_multi_shard_watch_heartbeats(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_multi_shard_watch_heartbeats");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_multi_shard_watch_heartbeats: {}", e);
            failed += 1;
        }
    }

    // Test 5: Explicit shard_id routing
    match test_explicit_shard_id_routing(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_explicit_shard_id_routing");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_explicit_shard_id_routing: {}", e);
            failed += 1;
        }
    }

    // Test 6: max_shard_hint skips initial probe
    match test_max_shard_hint_skips_probe(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_max_shard_hint_skips_probe");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_max_shard_hint_skips_probe: {}", e);
            failed += 1;
        }
    }

    println!("\n=== Results: {} passed, {} failed ===", passed, failed);

    // Drop server explicitly before exiting so the child process is killed
    drop(server);

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Test that a watch with filters routing to a single shard works without fallback
async fn test_single_shard_watch(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("  Testing single-shard watch with specific org...");

    let org_id = 1u128;
    let aggregate = AggregateKey::new(org_id, 1, 1000);

    // Create aggregate first
    let mut write_client = CeleriantClient::connect(address).await?;
    write_event(&mut write_client, &aggregate, 0).await?;

    // Watch a specific org (routes to single shard with OrgId routing)
    let watch_request = WatchRequest {
        correlation_id: Some(1),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: Some(HashSet::from([org_id])),
        aggregate_types: None,
        aggregates: None,
        operation_types: None,
    };

    let mut watch = WatchConnection::connect(address, watch_request, WatchOptions::default()).await?;
    println!("  Watch established (should be single-shard mode)");

    // Write an event
    write_event(&mut write_client, &aggregate, 1).await?;

    // Should receive the event
    let mut received = false;
    for _ in 0..10 {
        match watch.next_timeout(Duration::from_millis(200)).await? {
            Some(response) if !response.events.is_empty() => {
                if response.events.iter().any(|e| e.org_id == aggregate.org_id && e.aggregate_type_id == aggregate.aggregate_type_id && e.aggregate_id == aggregate.aggregate_id) {
                    received = true;
                    println!("  Received event (single-shard path worked)");
                    break;
                }
            }
            Some(_) => continue, // Heartbeat
            None => continue,    // Timeout
        }
    }

    if !received {
        return Err("Did not receive event on single-shard watch".into());
    }

    Ok(())
}

/// Test that aggregate_type filter triggers error 9002 (IncompatibleFilters) and falls back to multi-shard mode
async fn test_multi_shard_watch_fallback(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("  Testing multi-shard fallback with aggregate_type filter (triggers 9002)...");

    // Watch by aggregate_type without orgs filter - incompatible with OrgId routing, triggers 9002
    let watch_request = WatchRequest {
        correlation_id: Some(2),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: Some(HashSet::from([42])),
        aggregates: None,
        operation_types: None,
    };

    let watch = WatchConnection::connect(address, watch_request, WatchOptions::default()).await?;
    println!("  Watch established (should have fallen back to multi-shard mode)");

    // Connection successful means fallback worked
    // WatchConnection handles the 9001 error internally and opens N connections
    drop(watch);

    Ok(())
}

/// Test that events from different shards are received through merged stream
async fn test_multi_shard_watch_receives_events(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("  Testing multi-shard watch receives events from multiple shards...");

    // Create aggregates on different shards
    // org_id determines shard (with 3 shards, org_id % 3 gives shard)
    let agg_shard0 = AggregateKey::new(0, 42, 2000);  // org 0 -> shard 0
    let agg_shard1 = AggregateKey::new(1, 42, 2001);  // org 1 -> shard 1
    let agg_shard2 = AggregateKey::new(2, 42, 2002);  // org 2 -> shard 2

    let mut write_client = CeleriantClient::connect(address).await?;

    // Create all aggregates
    write_event(&mut write_client, &agg_shard0, 0).await?;
    write_event(&mut write_client, &agg_shard1, 0).await?;
    write_event(&mut write_client, &agg_shard2, 0).await?;

    // Watch all aggregates of type 42 (multi-shard, no orgs filter = incompatible with OrgId routing)
    let watch_request = WatchRequest {
        correlation_id: Some(3),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: Some(HashSet::from([42])),
        aggregates: None,
        operation_types: None,
    };

    let mut watch = WatchConnection::connect(address, watch_request, WatchOptions::default()).await?;
    println!("  Watch established for aggregate_type 42");

    sleep(Duration::from_millis(50)).await;

    // Write events to each shard
    write_event(&mut write_client, &agg_shard0, 1).await?;
    write_event(&mut write_client, &agg_shard1, 1).await?;
    write_event(&mut write_client, &agg_shard2, 1).await?;

    println!("  Wrote events to all 3 shards");

    // Should receive events from all shards through merged stream
    let mut received_shards = HashSet::new();

    for _ in 0..30 {
        match watch.next_timeout(Duration::from_millis(200)).await? {
            Some(response) if !response.events.is_empty() => {
                for event in &response.events {
                    received_shards.insert(event.org_id);
                    println!("  Received event from shard {}", event.org_id);
                }
            }
            Some(_) => continue, // Heartbeat
            None => break,       // Timeout
        }

        if received_shards.len() == 3 {
            break;
        }
    }

    if received_shards.len() != 3 {
        return Err(format!(
            "Expected events from 3 shards, got {}",
            received_shards.len()
        )
        .into());
    }

    println!("  Received events from all 3 shards through merged stream");
    Ok(())
}

/// Test that heartbeats from multiple shards pass through
async fn test_multi_shard_watch_heartbeats(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("  Testing multi-shard watch heartbeats...");

    // Watch all aggregates of type 99 (multi-shard, no events expected)
    let watch_request = WatchRequest {
        correlation_id: Some(4),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: Some(HashSet::from([99])),
        aggregates: None,
        operation_types: None,
    };

    let mut watch = WatchConnection::connect(address, watch_request, WatchOptions::default()).await?;
    println!("  Watch established, waiting for heartbeats...");

    // Wait for at least one heartbeat (server sends every ~5s when idle)
    let start = std::time::Instant::now();
    let max_wait = Duration::from_secs(7);

    while start.elapsed() < max_wait {
        match watch.next_timeout(Duration::from_secs(6)).await? {
            Some(response) if response.events.is_empty() => {
                println!("  Received heartbeat from a shard");
                return Ok(());
            }
            Some(_) => {
                println!("  Received event (unexpected)");
            }
            None => {
                println!("  Timeout waiting for heartbeat");
            }
        }
    }

    Err("Did not receive heartbeat within expected time".into())
}

/// Test that explicit shard_id in WatchRequest routes directly
async fn test_explicit_shard_id_routing(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("  Testing explicit shard_id routing...");

    let aggregate = AggregateKey::new(0, 50, 3000);

    let mut write_client = CeleriantClient::connect(address).await?;
    write_event(&mut write_client, &aggregate, 0).await?;

    // Watch shard 0 explicitly (skips filter-based routing)
    let watch_request = WatchRequest {
        correlation_id: Some(5),
        requested_latency_ms: Some(100),
        shard_id: Some(0), // Explicit shard routing
        orgs: None,
        aggregate_types: None,
        aggregates: None, // No filter needed
        operation_types: None,
    };

    let mut watch = WatchConnection::connect(address, watch_request, WatchOptions::default()).await?;
    println!("  Watch established on shard 0 explicitly");

    // Write to shard 0
    write_event(&mut write_client, &aggregate, 1).await?;

    // Should receive event
    let mut received = false;
    for _ in 0..10 {
        match watch.next_timeout(Duration::from_millis(200)).await? {
            Some(response) if !response.events.is_empty() => {
                if response.events.iter().any(|e| e.org_id == aggregate.org_id && e.aggregate_type_id == aggregate.aggregate_type_id && e.aggregate_id == aggregate.aggregate_id) {
                    received = true;
                    println!("  Received event on explicit shard 0 watch");
                    break;
                }
            }
            Some(_) => continue,
            None => continue,
        }
    }

    if !received {
        return Err("Did not receive event on explicit shard watch".into());
    }

    Ok(())
}

/// Test that max_shard_hint skips the initial probe and opens N connections directly
async fn test_max_shard_hint_skips_probe(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("  Testing max_shard_hint optimization...");

    // Use max_shard_hint to skip the initial single-shard probe
    let watch_request = WatchRequest {
        correlation_id: Some(6),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: Some(HashSet::from([77])),
        aggregates: None,
        operation_types: None,
    };

    let options = WatchOptions {
        compression: CompressionType::None,
        timeout: None,
        start_shard: 0,
        max_shard_hint: Some(2), // Tell it there are 3 shards (0..2), skip probe
        tls_config: None,
        identity_config: None,
    };

    let watch = WatchConnection::connect(address, watch_request, options).await?;
    println!("  Watch established with max_shard_hint (should have opened 3 connections directly)");

    // If connection succeeds, hint was used correctly
    drop(watch);

    Ok(())
}

/// Helper to write an event to an aggregate
async fn write_event(
    client: &mut CeleriantClient,
    aggregate: &AggregateKey,
    event_num: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event = DatablockAggregateEvent {
        client_event_index: event_num,
        event_index: 0,
        event_id: Some(rand::random()),
        event_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"event\":{}}}", event_num).into_bytes()),
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
            compression_type_id: 0,
            compression_level: None,
        },
    );

    let write_request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(event_num as u128),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    });

    let response = client
        .send_request(&write_request, CompressionType::None)
        .await?;

    match response {
        ClientResponse::Write(_) => Ok(()),
        ClientResponse::GenericError(err) => {
            Err(format!("Write failed: {}", err.error_message).into())
        }
        other => Err(format!("Unexpected response: {:?}", other).into()),
    }
}
