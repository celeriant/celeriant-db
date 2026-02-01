//! S3 Fallback Integration Test — Happy Path
//!
//! Tests that writes fall back to S3 when the follower goes offline,
//! and data lands at the correct S3 paths.
//!
//! Scenario:
//! 1. Start MinIO container, create bucket
//! 2. Start follower (with S3 config), then leader (with S3 config + follower address)
//! 3. Write events 1-3 to leader. Verify follower has 3 events (normal replication)
//! 4. Stop follower
//! 5. Write events 4-6 to leader. Leader fails to replicate, falls back to S3
//! 6. Verify S3 directly: list objects under cluster/fallback/shard_000/, verify content
//! 7. Write event 7 to leader (still no follower — another S3 fallback)
//! 8. Verify S3 directly: expect second object with higher WAL index
//!
//! Run with: cargo run --bin s3_fallback_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{ConfigClusterRole, MinioContainer, ServerConfig, TestServer};
use celeriant_msg::{
    process_requests::Request,
    request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use celeriant_wire::disk::versioned_block::deserialise_fallback_batch;
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
    println!("=== S3 Fallback Integration Test — Happy Path ===\n");

    let port = 10400 + (std::process::id() % 100) as u16;
    let minio_port = port + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);
    let expected_shard = (aggregate_key.aggregate_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", expected_shard);
    println!("  Expected shard: {} (prefix: {})", expected_shard, shard_prefix);

    println!("Starting replicated cluster with S3 fallback...");

    let follower_port = port + 100;
    let follower_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        cluster_role: ConfigClusterRole::Follower,
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region.clone()),
        s3_bucket: Some(bucket.clone()),
        s3_access_key_id: Some(access_key.clone()),
        s3_secret_access_key: Some(secret_key.clone()),
        s3_endpoint_override: Some(endpoint.clone()),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
        ..Default::default()
    };
    println!("Starting follower on port {}...", follower_port);
    let follower = TestServer::start_with_config(follower_port, follower_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_replication_port = follower_port + 1;
    let leader_config = ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        cluster_role: ConfigClusterRole::Leader,
        follower_address: Some(format!("127.0.0.1:{}", follower_replication_port)),
        routing_rule: RoutingRule::AggregateTypeId,
        s3_enabled: true,
        s3_region: Some(region),
        s3_bucket: Some(bucket),
        s3_access_key_id: Some(access_key),
        s3_secret_access_key: Some(secret_key),
        s3_endpoint_override: Some(endpoint),
        s3_allow_http: allow_http,
        s3_skip_signature: false,
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
    // Phase 2: Follower goes down, S3 fallback
    // ========================================
    println!("PHASE 2: Follower goes down, leader falls back to S3");
    println!("----------------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    drop(follower);
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing events 4-6 to leader (follower is down, S3 fallback)...");
    for i in 4..=6 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }
    println!("  ✓ Leader writes succeeded (S3 fallback active)\n");

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // ========================================
    // Phase 3: Verify S3 objects (first batch)
    // ========================================
    println!("PHASE 3: Verify S3 fallback objects (first batch)");
    println!("-------------------------------------------------");

    let objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects found: {}", objects.len());
    for obj in &objects {
        println!("    - {}", obj);
    }

    assert!(
        !objects.is_empty(),
        "Expected at least one S3 object after fallback"
    );

    let first_object_path = &objects[0];
    println!("  Reading first object: {}", first_object_path);
    let object_bytes = minio.get_object(first_object_path).await?;
    println!("  Object size: {} bytes", object_bytes.len());

    let fallback_batch = deserialise_fallback_batch(&object_bytes)
        .map_err(|e| format!("deserialise fallback batch: {:?}", e))?;
    println!(
        "  ✓ Deserialized FallbackBatch: shard_id={}, fallback_index={}, items={}",
        fallback_batch.shard_id,
        fallback_batch.fallback_index,
        fallback_batch.items.len()
    );

    assert_eq!(
        fallback_batch.shard_id, expected_shard,
        "FallbackBatch shard_id should match expected_shard"
    );
    assert!(
        !fallback_batch.items.is_empty(),
        "FallbackBatch should contain items"
    );

    let first_wal_index = fallback_batch.items[0].metablock.wal_index;
    println!(
        "  ✓ First item WAL index: {} (should match fallback_index={})",
        first_wal_index, fallback_batch.fallback_index
    );
    assert_eq!(
        fallback_batch.fallback_index, first_wal_index,
        "fallback_index should match first item's WAL index"
    );

    // ========================================
    // Phase 4: Write another event, verify second S3 object
    // ========================================
    println!("\nPHASE 4: Write event 7, verify second S3 object");
    println!("-----------------------------------------------");

    println!("  Writing event 7 to leader (still no follower)...");
    write_event(&mut leader_client, &aggregate_key, 7, false).await?;
    println!("  ✓ Write succeeded\n");

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let objects_after = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects now: {}", objects_after.len());
    for obj in &objects_after {
        println!("    - {}", obj);
    }

    assert!(
        objects_after.len() >= 2,
        "Expected at least 2 S3 objects after second fallback"
    );

    // Verify lexicographic ordering (temporal ordering)
    let mut sorted_objects = objects_after.clone();
    sorted_objects.sort();
    assert_eq!(
        objects_after, sorted_objects,
        "S3 objects should be in lexicographic (temporal) order"
    );
    println!("  ✓ S3 objects are lexicographically ordered");

    // Verify second object has higher WAL index
    if objects_after.len() >= 2 {
        let second_object_path = &objects_after[1];
        let second_bytes = minio.get_object(second_object_path).await?;
        let second_batch = deserialise_fallback_batch(&second_bytes)
            .map_err(|e| format!("deserialise second fallback batch: {:?}", e))?;

        println!(
            "  Second batch: fallback_index={}, items={}",
            second_batch.fallback_index,
            second_batch.items.len()
        );

        assert!(
            second_batch.fallback_index > fallback_batch.fallback_index,
            "Second batch fallback_index should be higher than first"
        );
        println!("  ✓ Second batch has higher fallback_index");
    }

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
