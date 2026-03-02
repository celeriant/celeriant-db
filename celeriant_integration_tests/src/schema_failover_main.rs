//! Schema Validation Failover Integration Test
//!
//! Tests that schema enforcement survives a leader swap:
//! 1. Start replicated cluster (leader + follower)
//! 2. Register schema on leader
//! 3. Verify valid writes succeed on leader
//! 4. Kill leader, wait for follower to become new leader
//! 5. Verify new leader enforces the schema (valid writes pass, invalid rejected)
//!
//! This validates that schemas replicate via WAL and that the new leader can
//! recover schemas from its local WAL after promotion.
//!
//! Run with: cargo run --bin schema_failover_main -p celeriant_integration_tests --release

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_integration_tests::{write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::requests::{RegisterSchemaRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    schema_key::SchemaKey,
};

const CLIENT_ID: u128 = 8888;

/// Write an event with a specific payload and event type.
async fn write_with_payload(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    major: u64,
    minor: u64,
    payload: &[u8],
) -> Result<(), ClientError> {
    let event = DatablockAggregateEvent {
        client_event_index: rand::random::<u64>() % 100_000,
        event_index: 0,
        event_id: Some(rand::random()),
        event_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        event_type_major: major,
        event_type_minor: minor,
        event_value: Arc::new(payload.to_vec()),
        iv: None,
    };

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

    client
        .send_request(
            &ClientRequest::Write(WriteRequest {
                correlation_id: Some(rand::random()),
                client_id: CLIENT_ID,
                user_id: None,
                writes,
            }),
            CompressionType::None,
        )
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Schema Validation Failover Test ===\n");

    let port_base = 11900 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    let aggregate_key = AggregateKey::new(1, 1, 1);

    // Schema for event_type (100, 0): requires {"event": <integer>}
    // This matches the format used by the write_event() helper.
    let schema = r#"{"type":"object","properties":{"event":{"type":"integer"}},"required":["event"]}"#;

    // ========================================
    // PHASE 1: Start replicated cluster
    // ========================================
    println!("PHASE 1: Start replicated cluster");
    println!("----------------------------------");

    println!("  Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-schema-failover").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("  MinIO ready at {}", endpoint);

    let cluster_config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
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

    println!("  Starting leader on port {}...", leader_port);
    let mut leader = TestServer::start_with_config_labeled(
        leader_port,
        cluster_config.clone(),
        "leader".into(),
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Starting follower on port {}...", follower_port);
    let follower = TestServer::start_with_config_labeled(
        follower_port,
        cluster_config,
        "follower".into(),
    )
    .await?;

    println!("  Waiting for election and replication connection...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // Create the aggregate with an initial write
    println!("  Creating aggregate with initial write...");
    write_event(&mut leader_client, &aggregate_key, 1, true).await?;
    println!("  DONE\n");

    // ========================================
    // PHASE 2: Register schema on leader
    // ========================================
    println!("PHASE 2: Register schema on leader");
    println!("-----------------------------------");

    let register_req = ClientRequest::RegisterSchema(RegisterSchemaRequest {
        correlation_id: Some(rand::random()),
        client_id: CLIENT_ID,
        user_id: None,
        schema_key: SchemaKey::new(1, 1, 100, 0),
        schema_type: 0,
        schema: schema.to_string(),
    });

    leader_client
        .send_request(&register_req, CompressionType::None)
        .await?;
    println!("  Schema registered on leader");

    // Verify valid write succeeds on leader
    write_event(&mut leader_client, &aggregate_key, 2, false).await?;
    println!("  Valid write on leader: PASS");

    // Verify invalid write rejected on leader
    let result =
        write_with_payload(&mut leader_client, &aggregate_key, 100, 0, b"not json").await;
    match &result {
        Err(ClientError::CeleriantError(e)) if e.error_code == 2022 => {
            println!("  Invalid write rejected on leader: PASS");
        }
        _ => return Err(format!("Expected error 2022 on leader, got: {:?}", result).into()),
    }

    // Wait for replication of schema to follower
    println!("  Waiting for schema replication to follower...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("  DONE\n");

    // ========================================
    // PHASE 3: Kill leader, wait for failover
    // ========================================
    println!("PHASE 3: Kill leader, wait for follower to become new leader");
    println!("-------------------------------------------------------------");

    drop(leader_client);
    leader.stop();
    println!("  Leader stopped");

    println!("  Waiting for failover (heartbeat timeout + S3 race)...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut new_leader_client = CeleriantClient::connect(follower.address()).await?;

    // Verify the former follower is now accepting writes
    println!("  Verifying new leader accepts writes...");
    write_event(&mut new_leader_client, &aggregate_key, 3, false).await?;
    println!("  New leader accepted write: PASS\n");

    // ========================================
    // PHASE 4: Verify schema enforcement on new leader
    // ========================================
    println!("PHASE 4: Verify schema enforcement on new leader (former follower)");
    println!("------------------------------------------------------------------");

    // Valid write — {"event":4} matches the schema
    write_event(&mut new_leader_client, &aggregate_key, 4, false).await?;
    println!("  Valid write on new leader: PASS");

    // Invalid write — not JSON
    let result =
        write_with_payload(&mut new_leader_client, &aggregate_key, 100, 0, b"bad data").await;
    match &result {
        Err(ClientError::CeleriantError(e)) if e.error_code == 2022 => {
            println!("  Invalid write (non-JSON) rejected on new leader: PASS");
        }
        _ => {
            return Err(
                format!("Expected error 2022 on new leader, got: {:?}", result).into(),
            )
        }
    }

    // Invalid write — missing required "event" field
    let result = write_with_payload(
        &mut new_leader_client,
        &aggregate_key,
        100,
        0,
        br#"{"other":"field"}"#,
    )
    .await;
    match &result {
        Err(ClientError::CeleriantError(e)) if e.error_code == 2022 => {
            println!("  Invalid write (missing field) rejected on new leader: PASS");
        }
        _ => {
            return Err(
                format!("Expected error 2022 on new leader, got: {:?}", result).into(),
            )
        }
    }

    // Invalid write — wrong type for "event" field
    let result = write_with_payload(
        &mut new_leader_client,
        &aggregate_key,
        100,
        0,
        br#"{"event":"not_integer"}"#,
    )
    .await;
    match &result {
        Err(ClientError::CeleriantError(e)) if e.error_code == 2022 => {
            println!("  Invalid write (wrong type) rejected on new leader: PASS");
        }
        _ => {
            return Err(
                format!("Expected error 2022 on new leader, got: {:?}", result).into(),
            )
        }
    }

    // Duplicate schema registration on new leader — should get 2020
    let result = new_leader_client
        .send_request(&register_req, CompressionType::None)
        .await;
    match &result {
        Err(ClientError::CeleriantError(e)) if e.error_code == 2020 => {
            println!("  Duplicate schema rejected on new leader: PASS");
        }
        _ => {
            return Err(
                format!("Expected error 2020 on new leader, got: {:?}", result).into(),
            )
        }
    }

    println!("\n=== All Schema Failover Tests Passed ===");
    Ok(())
}
