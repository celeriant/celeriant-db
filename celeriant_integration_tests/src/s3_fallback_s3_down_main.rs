//! S3 Goes Down Integration Test
//!
//! Tests that when S3 is initially available (for election), then goes down,
//! and then the follower goes down, writes are rolled back.
//!
//! Scenario:
//! 1. Start MinIO, follower, and leader. S3 election succeeds.
//! 2. Write events 1-3. Verify follower has 3 events (normal replication)
//! 3. Pause MinIO (S3 becomes unreachable)
//! 4. Stop follower
//! 5. Write event 4 to leader. Follower down, S3 unreachable — fallback fails
//! 6. Verify the write is rejected — client receives error
//!
//! Run with: cargo run --bin s3_fallback_s3_down_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{MinioContainer, ServerConfig, TestServer};
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
    println!("=== S3 Goes Down Integration Test ===\n");

    let port = 10900 + (std::process::id() % 100) as u16;
    let minio_port = port + 10;

    let num_shards = 4;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-s3down").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    println!("Starting replicated cluster with S3...");

    let follower_port = port + 100;
    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        bootstrap_as_leader: false,
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
    println!("Starting follower on port {}...", follower_port);
    let follower = TestServer::start_with_config(follower_port, follower_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        bootstrap_as_leader: true,
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
    println!("Starting leader on port {} (S3 election mode)...", port);
    let leader = TestServer::start_with_config(port, leader_config).await?;

    println!(
        "Cluster started: leader at {}, follower at {}\n",
        leader.address(),
        follower.address()
    );

    // Wait for S3 election to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

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
    // Phase 2: S3 goes down
    // ========================================
    println!("PHASE 2: S3 goes down (pause MinIO)");
    println!("-----------------------------------");

    println!("  Pausing MinIO container...");
    minio.pause()?;
    println!("  ✓ MinIO paused (S3 now unreachable)\n");

    // ========================================
    // Phase 3: Follower goes down, S3 unreachable
    // ========================================
    println!("PHASE 3: Follower goes down, S3 unreachable");
    println!("-------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    drop(follower);
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing event 4 to leader (should be REJECTED - no follower, S3 unreachable)...");
    let write_result = write_event(&mut leader_client, &aggregate_key, 4, false).await;

    match write_result {
        Ok(_) => {
            return Err("Write should have failed but succeeded!".into());
        }
        Err(e) => {
            println!("  ✓ Write was rejected as expected: {}", e);
        }
    }

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
