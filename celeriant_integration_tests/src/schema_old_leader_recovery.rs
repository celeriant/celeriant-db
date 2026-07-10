//! Schema Validation — Old Leader Recovery
//!
//! Tests that schemas survive the full A→B→A leadership cycle:
//! 1. Start two-node cluster (A=leader, B=follower)
//! 2. Register schema on leader (A), verify replication to follower (B)
//! 3. Kill leader (A), wait for B to become new leader
//! 4. Verify schema enforcement on B (new leader)
//! 5. Write more data on B while A is down
//! 6. Restart A — becomes follower, catches up from B via TCP
//! 7. Kill B, wait for A to become leader again
//! 8. Verify schema enforcement on A — proves schema survived the full cycle
//!
//! Run with: cargo run --bin schema_old_leader_recovery_main -p celeriant_integration_tests --release

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{poll_converged_count, s3_cluster_config, write_event, MinioContainer, TestServer, FOLLOWER_CONVERGENCE_TIMEOUT};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::requests::{RegisterSchemaRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    schema_key::SchemaKey,
};

const CLIENT_ID: u128 = 9002;

async fn write_with_payload(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    major: u64,
    minor: u64,
    payload: &[u8],
) -> Result<(), ClientError> {
    let event = DatablockAggregateEvent {
        client_seq: rand::random::<u64>() % 100_000,
        event_seq: 0,
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
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    client
        .send_request(
            &ClientRequest::Write(WriteRequest {
                correlation_id: Some(rand::random()),
                client_id: CLIENT_ID,
                user_id: None,
                writes,
            })
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
    println!("=== Schema Validation — Old Leader Recovery ===\n");

    let port_base = 14500 + (std::process::id() % 100) as u16;
    let node_a_port = port_base;
    let node_b_port = port_base + 100;
    let minio_port = port_base + 10;

    let num_shards = 1;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    let schema = r#"{"type":"object","properties":{"event":{"type":"integer"}},"required":["event"]}"#;

    // ========================================
    // PHASE 1: Start cluster, register schema (both nodes alive)
    // ========================================
    println!("PHASE 1: Start cluster, register schema on leader (A)");
    println!("------------------------------------------------------");

    println!("  Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-schema-old-leader").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("  MinIO ready at {}", endpoint);

    let config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    println!("  Starting node A (leader) on port {}...", node_a_port);
    let mut node_a = TestServer::start_with_config_labeled(node_a_port, config.clone(), "node-a".into()).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("  Starting node B (follower) on port {}...", node_b_port);
    let mut node_b = TestServer::start_with_config_labeled(node_b_port, config, "node-b".into()).await?;

    println!("  Waiting for election, replication, and S3 lease expiry...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut node_a_client = CeleriantClient::connect(node_a.address()).await?;

    println!("  Writing initial events 1-3 through node A...");
    for i in 1..=3 {
        write_event(&mut node_a_client, &aggregate_key, i, i == 1).await?;
    }

    // Register schema while both nodes are up (replicates via TCP)
    println!("  Registering schema on node A...");
    let register_req = ClientRequest::RegisterSchema(RegisterSchemaRequest {
        correlation_id: Some(rand::random()),
        client_id: CLIENT_ID,
        user_id: None,
        schema_key: SchemaKey::new(1, 1, 100, 0),
        schema_type: 0,
        schema: schema.to_string(),
    });

    node_a_client
        .send_request(&register_req)
        .await?;
    println!("  Schema registered");

    // Confirm enforcement on leader
    write_event(&mut node_a_client, &aggregate_key, 4, false).await?;
    println!("  Valid write on A: PASS");

    let result = write_with_payload(&mut node_a_client, &aggregate_key, 100, 0, b"bad").await;
    expect_schema_violation(result, "node A reject");
    println!("  Invalid write rejected on A: PASS");

    let mut node_b_client = CeleriantClient::connect(node_b.address()).await?;
    let b_count =
        poll_converged_count(&mut node_b_client, &aggregate_key, 4, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(b_count, 4, "Node B should have 4 events");
    println!("  Node B has {} events (schema replicated via TCP)\n", b_count);

    // ========================================
    // PHASE 2: Kill leader (A), promote B
    // ========================================
    println!("PHASE 2: Kill leader (A), wait for B to become new leader");
    println!("----------------------------------------------------------");

    drop(node_a_client);
    drop(node_b_client);
    node_a.stop();
    println!("  Node A stopped");

    println!("  Waiting for failover (heartbeat lease 1.5s + S3 race)...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut node_b_client = CeleriantClient::connect(node_b.address()).await?;
    write_event(&mut node_b_client, &aggregate_key, 5, false).await?;
    println!("  Node B accepts writes (is now leader): PASS\n");

    // ========================================
    // PHASE 3: Verify schema enforcement on B (new leader)
    // ========================================
    println!("PHASE 3: Verify schema enforcement on new leader (B)");
    println!("-----------------------------------------------------");

    write_event(&mut node_b_client, &aggregate_key, 6, false).await?;
    println!("  Valid write on B: PASS");

    let result = write_with_payload(&mut node_b_client, &aggregate_key, 100, 0, b"bad").await;
    expect_schema_violation(result, "node B reject");
    println!("  Invalid write rejected on B: PASS");

    // Write more data while A is down
    for i in 7..=9 {
        write_event(&mut node_b_client, &aggregate_key, i, false).await?;
    }
    println!("  Events 7-9 written on B while A is down\n");

    // ========================================
    // PHASE 4: Restart A — becomes follower, catches up via TCP
    // ========================================
    println!("PHASE 4: Restart node A (catches up from B via TCP replication)");
    println!("---------------------------------------------------------------");

    node_a.restart().await?;
    println!("  Node A restarted");

    println!("  Waiting for A to become follower + TCP catchup...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Write one more through B to confirm replication B→A is flowing
    write_event(&mut node_b_client, &aggregate_key, 10, false).await?;

    let mut node_a_client = CeleriantClient::connect(node_a.address()).await?;
    let a_count =
        poll_converged_count(&mut node_a_client, &aggregate_key, 10, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(a_count, 10, "Node A should have 10 events after catchup");
    println!("  Node A caught up: {} events\n", a_count);

    // ========================================
    // PHASE 5: Kill B, promote A back to leader
    // ========================================
    println!("PHASE 5: Kill node B, wait for node A to become leader again");
    println!("-------------------------------------------------------------");

    drop(node_b_client);
    drop(node_a_client);
    node_b.stop();
    println!("  Node B stopped");

    println!("  Waiting for node A to take over (heartbeat lease 1.5s + S3 race)...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut node_a_client = CeleriantClient::connect(node_a.address()).await?;
    write_event(&mut node_a_client, &aggregate_key, 11, false).await?;
    println!("  Node A accepts writes (is now leader): PASS\n");

    // ========================================
    // PHASE 6: Verify schema enforcement on A after full cycle
    // ========================================
    println!("PHASE 6: Verify schema enforcement on node A (re-promoted)");
    println!("-----------------------------------------------------------");

    write_event(&mut node_a_client, &aggregate_key, 12, false).await?;
    println!("  Valid write: PASS");

    let result = write_with_payload(&mut node_a_client, &aggregate_key, 100, 0, b"nope").await;
    expect_schema_violation(result, "node A non-JSON");
    println!("  Invalid write (non-JSON) rejected: PASS");

    let result = write_with_payload(&mut node_a_client, &aggregate_key, 100, 0, br#"{"other":1}"#).await;
    expect_schema_violation(result, "node A missing field");
    println!("  Invalid write (missing field) rejected: PASS");

    let result = write_with_payload(&mut node_a_client, &aggregate_key, 100, 0, br#"{"event":"string"}"#).await;
    expect_schema_violation(result, "node A wrong type");
    println!("  Invalid write (wrong type) rejected: PASS");

    let result = node_a_client
        .send_request(&register_req)
        .await;
    match &result {
        Err(ClientError::Server(celeriant_client_tokio::server_error::ServerError::Schema {
            kind: celeriant_client_tokio::server_error::SchemaError::AlreadyExists, ..
        })) => {
            println!("  Duplicate registration rejected: PASS");
        }
        _ => return Err(format!("Expected SchemaAlreadyExists, got: {:?}", result).into()),
    }

    println!("\n=== All Tests Passed ===");
    println!("Schema registered on original leader (A), survived: A→B failover →");
    println!("A restart as follower → TCP catchup → B→A failover → enforcement on A.\n");
    Ok(())
}
