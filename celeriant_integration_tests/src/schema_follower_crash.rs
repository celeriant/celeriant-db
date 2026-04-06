//! Schema Validation — Follower Crash + Restart + Promotion
//!
//! Tests that schema enforcement survives a follower crash, restart, and promotion:
//! 1. Start replicated cluster (leader + follower)
//! 2. Register schema on leader, verify replication to follower
//! 3. Kill follower
//! 4. Write more data on leader while follower is down
//! 5. Restart follower — catches up data via TCP replication
//! 6. Kill leader, wait for follower to become new leader
//! 7. Verify new leader (former follower) still enforces the schema from its local WAL
//!
//! Run with: cargo run --bin schema_follower_crash_main -p celeriant_integration_tests --release

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{count_events, poll_event_count, s3_cluster_config, write_event, MinioContainer, TestServer};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::requests::{RegisterSchemaRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    schema_key::SchemaKey,
};

const CLIENT_ID: u128 = 9001;

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
            compression_type_id: 0,
            compression_level: None,
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

fn expect_schema_violation(result: Result<(), ClientError>, context: &str) {
    use celeriant_client_tokio::server_error::{SchemaError, ServerError};
    match result {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::ValidationFailed, .. })) => {}
        Ok(()) => panic!("{}: expected SchemaValidationFailed, got success", context),
        Err(e) => panic!("{}: expected SchemaValidationFailed, got: {:?}", context, e),
    }
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Schema Validation — Follower Crash + Restart + Promotion ===\n");

    let port_base = 14100 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    let num_shards = 1;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    let schema = r#"{"type":"object","properties":{"event":{"type":"integer"}},"required":["event"]}"#;

    // ========================================
    // PHASE 1: Start cluster, register schema, verify replication
    // ========================================
    println!("PHASE 1: Start cluster and register schema");
    println!("-------------------------------------------");

    println!("  Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-schema-follower-crash").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("  MinIO ready at {}", endpoint);

    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    println!("  Starting leader on port {}...", leader_port);
    let mut leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Starting follower on port {}...", follower_port);
    let mut follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("  Waiting for election, replication, and S3 lease expiry...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    // Create aggregate and write initial events
    println!("  Creating aggregate with initial events 1-3...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    // Register schema while both nodes are up (replicates via TCP)
    println!("  Registering schema...");
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
    println!("  Schema registered");

    // Write a validated event to confirm
    write_event(&mut leader_client, &aggregate_key, 4, false).await?;
    println!("  Valid write: PASS");

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(follower_count, 4, "Follower should have 4 events");
    println!("  Follower has {} events (schema replicated via TCP)\n", follower_count);

    // ========================================
    // PHASE 2: Kill follower, write more data on leader
    // ========================================
    println!("PHASE 2: Kill follower, continue writing on leader");
    println!("--------------------------------------------------");

    drop(follower_client);
    follower.stop();
    println!("  Follower stopped");

    println!("  Waiting for leader self-heal...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Write more events while follower is down
    println!("  Writing events 5-7 on leader...");
    for i in 5..=7 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }

    // Verify schema still enforced on leader
    let result = write_with_payload(&mut leader_client, &aggregate_key, 100, 0, b"bad").await;
    expect_schema_violation(result, "leader reject while follower down");
    println!("  Invalid write rejected on leader: PASS\n");

    // ========================================
    // PHASE 3: Restart follower, let it catch up via TCP
    // ========================================
    println!("PHASE 3: Restart follower (catches up data via TCP)");
    println!("---------------------------------------------------");

    follower.restart().await?;
    println!("  Follower restarted");

    // Poll for follower to catch up with events 1-7
    println!("  Polling for follower catchup (7 events)...");
    poll_event_count(follower.address(), &aggregate_key, 7, Duration::from_secs(30)).await;

    // Write two events: first may go via S3 (stale connection), second via fresh TCP.
    write_event(&mut leader_client, &aggregate_key, 8, false).await?;
    write_event(&mut leader_client, &aggregate_key, 9, false).await?;

    let follower_count = poll_event_count(
        follower.address(), &aggregate_key, 9, Duration::from_secs(45),
    ).await;
    println!("  Follower caught up: {} events\n", follower_count);

    // ========================================
    // PHASE 4: Kill leader, promote follower
    // ========================================
    println!("PHASE 4: Kill leader, wait for follower promotion");
    println!("--------------------------------------------------");

    drop(leader_client);
    leader.stop();
    println!("  Leader stopped");

    // Poll until the follower promotes itself to leader and accepts writes.
    // Failover time: heartbeat TTL expiry (~1.5s) + S3 lease expiry + CAS race.
    println!("  Polling for follower promotion...");
    let promo_timeout = Duration::from_secs(20);
    let promo_start = std::time::Instant::now();
    let mut promoted = false;
    while promo_start.elapsed() < promo_timeout {
        if let Ok(mut c) = CeleriantClient::connect(follower.address()).await {
            if write_event(&mut c, &aggregate_key, 10, false).await.is_ok() {
                promoted = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(promoted, "Follower should have promoted to leader within {}s", promo_timeout.as_secs());
    println!("  New leader accepts writes: PASS\n");

    let mut new_leader_client = CeleriantClient::connect(follower.address()).await?;

    // ========================================
    // PHASE 5: Verify schema enforcement on promoted follower
    // ========================================
    println!("PHASE 5: Verify schema enforcement on new leader (former follower)");
    println!("------------------------------------------------------------------");

    // Valid write
    write_event(&mut new_leader_client, &aggregate_key, 11, false).await?;
    println!("  Valid write: PASS");

    // Invalid — non-JSON
    let result = write_with_payload(&mut new_leader_client, &aggregate_key, 100, 0, b"nope").await;
    expect_schema_violation(result, "new leader non-JSON");
    println!("  Invalid write (non-JSON) rejected: PASS");

    // Invalid — missing required field
    let result = write_with_payload(&mut new_leader_client, &aggregate_key, 100, 0, br#"{"other":1}"#).await;
    expect_schema_violation(result, "new leader missing field");
    println!("  Invalid write (missing field) rejected: PASS");

    // Invalid — wrong type
    let result = write_with_payload(&mut new_leader_client, &aggregate_key, 100, 0, br#"{"event":"string"}"#).await;
    expect_schema_violation(result, "new leader wrong type");
    println!("  Invalid write (wrong type) rejected: PASS");

    // Duplicate registration
    let result = new_leader_client.send_request(&register_req, CompressionType::None).await;
    match &result {
        Err(ClientError::Server(celeriant_client_tokio::server_error::ServerError::Schema {
            kind: celeriant_client_tokio::server_error::SchemaError::AlreadyExists, ..
        })) => {
            println!("  Duplicate registration rejected: PASS");
        }
        _ => return Err(format!("Expected SchemaAlreadyExists, got: {:?}", result).into()),
    }

    println!("\n=== All Tests Passed ===");
    println!("Schema survived: replication -> follower crash -> restart -> catchup -> promotion.\n");
    Ok(())
}
