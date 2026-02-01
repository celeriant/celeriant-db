//! S3 Fallback + Follower Catchup Integration Test
//!
//! Tests the full cycle: normal replication, follower goes down, S3 fallback,
//! follower comes back, catchup, and continued normal replication.
//!
//! Scenario:
//! 1. Start MinIO, follower, leader (all with S3 config)
//! 2. Write events 1-3. Verify follower has 3 events
//! 3. Stop follower
//! 4. Write events 4-8 to leader (S3 fallback for these)
//! 5. Restart follower
//! 6. Write event 9. This triggers leader to attempt follower replication;
//!    follower reports WAL index mismatch; leader sends catchup entries
//! 7. Wait for replication
//! 8. Verify follower has all 9 events via client protocol read
//! 9. Verify S3 objects exist from step 4
//! 10. Write events 10-12. Verify follower gets them via normal replication
//!     (no new S3 objects)
//!
//! Run with: cargo run --bin s3_fallback_catchup_main

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
    println!("=== S3 Fallback + Follower Catchup Integration Test ===\n");

    let port = 10500 + (std::process::id() % 100) as u16;
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
    let mut follower = TestServer::start_with_config(follower_port, follower_config).await?;

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
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing events 4-8 to leader (follower is down, S3 fallback)...");
    for i in 4..=8 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }
    println!("  ✓ Leader writes succeeded (S3 fallback active)\n");

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // ========================================
    // Phase 3: Follower restarts, catchup happens
    // ========================================
    println!("PHASE 3: Follower restarts, catchup on next write");
    println!("-------------------------------------------------");

    println!("  Restarting follower...");
    follower.restart().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower currently has {} events (behind leader)", follower_count);
    assert_eq!(
        follower_count, 3,
        "Follower should still have only 3 events after restart"
    );

    println!("  Writing event 9 to leader (should trigger catchup)...");
    write_event(&mut leader_client, &aggregate_key, 9, false).await?;

    println!("  Waiting for catchup replication...");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // ========================================
    // Phase 4: Verify follower caught up
    // ========================================
    println!("\nPHASE 4: Verify follower caught up");
    println!("----------------------------------");

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower now has {} events", follower_count);

    assert_eq!(
        follower_count, 9,
        "Follower should have all 9 events after catchup"
    );
    println!("  ✓ Catchup successful! Follower has all 9 events");

    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    assert_eq!(leader_count, 9, "Leader should have 9 events");
    println!("  ✓ Leader has {} events", leader_count);

    // ========================================
    // Phase 5: Verify S3 objects exist from fallback period
    // ========================================
    println!("\nPHASE 5: Verify S3 fallback objects");
    println!("-----------------------------------");

    let objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects found: {}", objects.len());
    for obj in &objects {
        println!("    - {}", obj);
    }

    assert!(
        !objects.is_empty(),
        "Expected S3 objects from fallback period"
    );

    let first_object_path = &objects[0];
    let object_bytes = minio.get_object(first_object_path).await?;
    let fallback_batch = deserialise_fallback_batch(&object_bytes)
        .map_err(|e| format!("deserialise fallback batch: {:?}", e))?;
    println!(
        "  ✓ Valid FallbackBatch: shard_id={}, fallback_index={}, items={}",
        fallback_batch.shard_id,
        fallback_batch.fallback_index,
        fallback_batch.items.len()
    );

    let s3_object_count_before = objects.len();

    // ========================================
    // Phase 6: Normal replication resumes (no new S3 objects)
    // ========================================
    println!("\nPHASE 6: Normal replication resumes (no new S3 objects)");
    println!("-------------------------------------------------------");

    println!("  Writing events 10-12 to leader (follower is online)...");
    for i in 10..=12 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(
        follower_count, 12,
        "Follower should have 12 events after normal replication"
    );
    println!("  ✓ Follower has {} events", follower_count);

    let objects_after = minio.list_objects(&shard_prefix).await?;
    println!("  S3 objects now: {}", objects_after.len());

    assert_eq!(
        objects_after.len(),
        s3_object_count_before,
        "No new S3 objects should appear after follower is back online"
    );
    println!("  ✓ No new S3 objects created (normal replication resumed)");

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
