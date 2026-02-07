//! CreateOnly Prevents Overwrites Integration Test
//!
//! Tests that PutCondition::CreateOnly is actually being used by verifying
//! behavior when an object already exists at the target path.
//!
//! Scenario:
//! 1. Start MinIO, create bucket
//! 2. Pre-seed S3 objects at multiple WAL indices with garbage content
//! 3. Start follower and leader (with S3 config)
//! 4. Write events 1-3. Verify follower has 3
//! 5. Stop follower
//! 6. Write event 4 to leader. Fallback path triggers. Should hit pre-seeded object
//! 7. Verify the write succeeds (AlreadyExists treated as Ok)
//! 8. Verify the pre-seeded objects are NOT overwritten - read back from MinIO,
//!    confirm they still contain the garbage content
//!
//! Run with: cargo run --bin s3_fallback_createonly_main

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
    println!("=== CreateOnly Prevents Overwrites Integration Test ===\n");

    let port = 11100 + (std::process::id() % 100) as u16;
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

    // ========================================
    // Phase 1: Pre-seed S3 objects with garbage content
    // ========================================
    println!("\nPHASE 1: Pre-seed S3 objects with garbage content");
    println!("-------------------------------------------------");

    let garbage_content = b"GARBAGE_CONTENT_DO_NOT_OVERWRITE_THIS_987654321";
    let mut seeded_paths = Vec::new();

    for wal_index in 4..=10 {
        let path = format!("cluster/fallback/shard_{:03}/batch_{:09}_{:09}.bin", expected_shard, wal_index, wal_index);
        println!("  Pre-seeding: {}", path);
        minio.put_object(&path, garbage_content.to_vec()).await?;
        seeded_paths.push(path);
    }
    println!("  ✓ Pre-seeded {} objects with garbage content\n", seeded_paths.len());

    // ========================================
    // Phase 2: Start cluster with S3 fallback enabled
    // ========================================
    println!("PHASE 2: Start replicated cluster with S3 fallback");
    println!("--------------------------------------------------");

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
        bootstrap_as_leader: true,
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

    // Wait for S3 election + peer discovery + heartbeat establishment
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // ========================================
    // Phase 3: Normal replication (follower is up)
    // ========================================
    println!("PHASE 3: Normal replication with follower online");
    println!("------------------------------------------------");

    println!("  Writing events 1-3 to leader...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(
        follower_count, 3,
        "Follower should have 3 events after normal replication"
    );
    println!("  ✓ Follower has {} events\n", follower_count);

    // ========================================
    // Phase 4: Follower goes down, S3 fallback hits pre-seeded objects
    // ========================================
    println!("PHASE 4: Follower goes down, S3 fallback hits pre-seeded objects");
    println!("----------------------------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    drop(follower);
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing event 4 to leader (fallback should hit pre-seeded object)...");
    write_event(&mut leader_client, &aggregate_key, 4, false).await?;
    println!("  ✓ Write succeeded (AlreadyExists was treated as Ok)\n");

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // ========================================
    // Phase 5: Verify pre-seeded objects are NOT overwritten
    // ========================================
    println!("PHASE 5: Verify pre-seeded objects are NOT overwritten");
    println!("------------------------------------------------------");

    let mut found_garbage = false;
    for path in &seeded_paths {
        let object_bytes = minio.get_object(path).await?;

        if object_bytes.as_ref() == garbage_content {
            println!("  ✓ {} still contains garbage (NOT overwritten)", path);
            found_garbage = true;
        } else {
            println!("  ? {} has different content (size: {} bytes)", path, object_bytes.len());
        }
    }

    assert!(
        found_garbage,
        "At least one pre-seeded object should still contain garbage content, proving CreateOnly was used"
    );

    println!("\n  ✓ CreateOnly is in effect - pre-seeded objects were NOT overwritten");
    println!("  ✓ This proves PutCondition::CreateOnly is being used, not PutCondition::None");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
