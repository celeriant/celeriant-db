//! S3 Not Configured Integration Test
//!
//! Tests that when the follower is down and S3 is not configured,
//! writes are rolled back and the client sees an error.
//!
//! Scenario:
//! 1. Start follower (no S3 config), then leader (no S3 config, s3_enabled: false)
//! 2. Write events 1-3. Verify follower has 3 events
//! 3. Stop follower
//! 4. Write event 4 to leader. With no follower and no S3, leader must roll back
//! 5. Verify the write is rejected — client should receive an error response
//! 6. Restart follower
//! 7. Write event 4 again (now follower is back). Should succeed
//! 8. Verify follower has 4 events
//!
//! Run with: cargo run --bin s3_fallback_no_s3_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{ConfigClusterRole, ServerConfig, TestServer};
use celeriant_msg::{
    process_requests::Request,
    request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

async fn write_event(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    event_num: u64,
    allow_create: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = DatablockAggregateEvent {
        client_event_index: event_num,
        event_index: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"event\":{}}}", event_num).into_bytes()),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create,
            expected_event_batch_index: if event_num == 1 { Some(0) } else { None },
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(event_num as u128),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&Request::Write(write_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Write(_) => Ok(()),
        other => Err(format!("Write failed: {:?}", other).into()),
    }
}

async fn count_events(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
) -> Result<usize, Box<dyn std::error::Error>> {
    let read_req = ReadRequest {
        correlation_id: Some(999),
        aggregate_key: aggregate_key.clone(),
        filters: celeriant_msg::request::read_filters::ReadFilters::new(1),
    };

    let response = client
        .send_request(&Request::Read(read_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Read(read_resp) => {
            let total: usize = read_resp
                .event_batches
                .iter()
                .map(|b| b.events.len())
                .sum();
            Ok(total)
        }
        celeriant_msg::process_responses::Response::GenericError(_) => Ok(0),
        other => Err(format!("Unexpected response: {:?}", other).into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3 Not Configured Integration Test ===\n");

    let port = 10500 + (std::process::id() % 100) as u16;

    let num_shards = 4;

    println!("Starting replicated cluster WITHOUT S3...");

    let follower_port = port + 100;
    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        cluster_role: ConfigClusterRole::Follower,
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: false,
        ..Default::default()
    };
    println!("Starting follower on port {}...", follower_port);
    let mut follower = TestServer::start_with_config(follower_port, follower_config.clone()).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_replication_port = follower_port + 1;
    let leader_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        cluster_role: ConfigClusterRole::Leader,
        follower_address: Some(format!("127.0.0.1:{}", follower_replication_port)),
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: false,
        ..Default::default()
    };
    println!(
        "Starting leader on port {} (replicating to 127.0.0.1:{})...",
        port, follower_replication_port
    );
    let leader = TestServer::start_with_config(port, leader_config).await?;
    println!(
        "Cluster started: leader at {}, follower at {}\n",
        leader.address(),
        follower.address()
    );

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // Phase 1: Normal replication (follower is up)
    // ========================================
    println!("PHASE 1: Normal replication with follower online");
    println!("------------------------------------------------");

    println!("  Writing events 1-3 to leader...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(
        follower_count, 3,
        "Follower should have 3 events after normal replication"
    );
    println!("  ✓ Follower has {} events\n", follower_count);

    // ========================================
    // Phase 2: Follower goes down, no S3 fallback
    // ========================================
    println!("PHASE 2: Follower goes down, NO S3 configured");
    println!("---------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing event 4 to leader (should be REJECTED - no follower, no S3)...");
    let write_result = write_event(&mut leader_client, &aggregate_key, 4, false).await;

    match write_result {
        Ok(_) => {
            return Err("Write should have failed but succeeded!".into());
        }
        Err(e) => {
            println!("  ✓ Write was rejected as expected: {}", e);
        }
    }

    // ========================================
    // Phase 3: Follower restarts, writes resume
    // ========================================
    println!("\nPHASE 3: Follower restarts, writes resume");
    println!("-----------------------------------------");

    println!("  Restarting follower...");
    follower.restart().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    println!("  Writing event 4 to leader (should succeed now)...");
    write_event(&mut leader_client, &aggregate_key, 4, false).await?;
    println!("  ✓ Write succeeded");

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower now has {} events", follower_count);

    assert_eq!(
        follower_count, 4,
        "Follower should have 4 events after replication resumed"
    );
    println!("  ✓ Follower has all 4 events");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
