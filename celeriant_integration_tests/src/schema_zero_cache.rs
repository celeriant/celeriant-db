//! Schema Validation — Zero Cache
//!
//! Tests that schema validation still works when all cache limits are set to 0.
//! The schema LRU cache has a minimum fallback of 1,000 entries (can't be fully
//! disabled), but all other caches are zeroed. This forces schema lookups to
//! exercise the WAL reverse scan + bloom filter path more aggressively.
//!
//! Scenario:
//! 1. Start server with all caches at 0, multi-shard to stress intrashard propagation
//! 2. Register schema, verify enforcement
//! 3. Restart server (cold cache), verify schema recovered from WAL
//!
//! Run with: cargo run --bin schema_zero_cache_main -p celeriant_integration_tests --release

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{ServerConfig, TestServer};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::requests::{RegisterSchemaRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    schema_key::SchemaKey,
};

const CLIENT_ID: u128 = 9003;

fn register_schema_request(
    org_id: u128,
    aggregate_type_id: u128,
    major: u64,
    minor: u64,
    schema: String,
) -> ClientRequest {
    ClientRequest::RegisterSchema(RegisterSchemaRequest {
        correlation_id: Some(rand::random()),
        client_id: CLIENT_ID,
        user_id: None,
        schema_key: SchemaKey::new(org_id, aggregate_type_id, major, minor),
        schema_type: 0,
        schema,
    })
}

fn write_event(
    aggregate: &AggregateKey,
    major: u64,
    minor: u64,
    payload: &[u8],
    aggregate_version: u64,
    allow_create: bool,
) -> ClientRequest {
    let event = DatablockAggregateEvent {
        client_seq: 0,
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
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create,
            expected_version: Some(aggregate_version),
            enforce_client_idempotency: false,
        },
    );

    ClientRequest::Write(WriteRequest {
        correlation_id: Some(rand::random()),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    })
}

fn expect_schema_validation_failed(result: Result<impl std::fmt::Debug, ClientError>) {
    use celeriant_client_tokio::server_error::{SchemaError, ServerError};
    match result {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::ValidationFailed, .. })) => {}
        Ok(resp) => panic!("Expected SchemaValidationFailed, got success: {:?}", resp),
        Err(e) => panic!("Expected SchemaValidationFailed, got: {:?}", e),
    }
}

fn expect_schema_already_exists(result: Result<impl std::fmt::Debug, ClientError>) {
    use celeriant_client_tokio::server_error::{SchemaError, ServerError};
    match result {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::AlreadyExists, .. })) => {}
        Ok(resp) => panic!("Expected SchemaAlreadyExists, got success: {:?}", resp),
        Err(e) => panic!("Expected SchemaAlreadyExists, got: {:?}", e),
    }
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Schema Validation — Zero Cache ===\n");

    let port = 10900 + (std::process::id() % 100) as u16;

    let config = ServerConfig {
        num_shards: Some(4),
        log_level: "warn".to_string(),
        standalone: true,
        memory_budget_bytes: Some(1024),
        ..Default::default()
    };

    // ========================================
    // PHASE 1: Register and enforce with zero caches
    // ========================================
    println!("PHASE 1: Register schema and enforce writes (all caches = 0)");
    println!("-------------------------------------------------------------");

    let mut server = TestServer::start_with_config(port, config).await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    let agg = AggregateKey::new(1, 100, 1);
    let schema = r#"{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}"#;

    // Create aggregate
    let req = write_event(&agg, 1, 0, br#"{"name":"pre","age":0}"#, 0, true);
    client.send_request(&req).await?;
    println!("  Aggregate created");

    // Register schema
    let req = register_schema_request(1, 100, 1, 0, schema.to_string());
    client.send_request(&req).await?;
    println!("  Schema registered: PASS");

    // Valid write
    let req = write_event(&agg, 1, 0, br#"{"name":"Alice","age":30}"#, 1, false);
    client.send_request(&req).await?;
    println!("  Valid write: PASS");

    // Invalid — missing field
    let req = write_event(&agg, 1, 0, br#"{"name":"Bob"}"#, 2, false);
    let result = client.send_request(&req).await;
    expect_schema_validation_failed(result);
    println!("  Invalid write (missing field) rejected: PASS");

    // Invalid — wrong type
    let req = write_event(&agg, 1, 0, br#"{"name":"Carol","age":"thirty"}"#, 2, false);
    let result = client.send_request(&req).await;
    expect_schema_validation_failed(result);
    println!("  Invalid write (wrong type) rejected: PASS");

    // Invalid — non-JSON
    let req = write_event(&agg, 1, 0, b"not json", 2, false);
    let result = client.send_request(&req).await;
    expect_schema_validation_failed(result);
    println!("  Invalid write (non-JSON) rejected: PASS");

    // Duplicate registration
    let req = register_schema_request(1, 100, 1, 0, schema.to_string());
    let result = client.send_request(&req).await;
    expect_schema_already_exists(result);
    println!("  Duplicate registration rejected: PASS\n");

    // ========================================
    // PHASE 2: Restart — schema recovered from WAL with cold cache
    // ========================================
    println!("PHASE 2: Restart server — schema recovery from WAL (cold cache)");
    println!("-----------------------------------------------------------------");

    drop(client);
    server.restart().await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    // Valid write after restart
    let req = write_event(&agg, 1, 0, br#"{"name":"Dave","age":25}"#, 2, false);
    client.send_request(&req).await?;
    println!("  Valid write after restart: PASS");

    // Invalid write after restart
    let req = write_event(&agg, 1, 0, br#"{"name":"Eve"}"#, 3, false);
    let result = client.send_request(&req).await;
    expect_schema_validation_failed(result);
    println!("  Invalid write rejected after restart: PASS");

    // Duplicate registration after restart
    let req = register_schema_request(1, 100, 1, 0, schema.to_string());
    let result = client.send_request(&req).await;
    expect_schema_already_exists(result);
    println!("  Duplicate registration rejected after restart: PASS\n");

    // ========================================
    // PHASE 3: Multiple schemas — stress test with zero caches
    // ========================================
    println!("PHASE 3: Multiple schemas — different event types");
    println!("--------------------------------------------------");

    // Register a second schema for a different event type
    let schema2 = r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;
    let req = register_schema_request(1, 100, 2, 0, schema2.to_string());
    client.send_request(&req).await?;
    println!("  Second schema registered: PASS");

    // Valid write against second schema
    let req = write_event(&agg, 2, 0, br#"{"id":42}"#, 3, false);
    client.send_request(&req).await?;
    println!("  Valid write against second schema: PASS");

    // Invalid write against second schema
    let req = write_event(&agg, 2, 0, br#"{"id":"not_int"}"#, 4, false);
    let result = client.send_request(&req).await;
    expect_schema_validation_failed(result);
    println!("  Invalid write against second schema rejected: PASS");

    // First schema still enforced
    let req = write_event(&agg, 1, 0, br#"{"name":"Frank","age":40}"#, 4, false);
    client.send_request(&req).await?;
    println!("  First schema still enforced (valid): PASS");

    let req = write_event(&agg, 1, 0, br#"{"name":"Grace"}"#, 5, false);
    let result = client.send_request(&req).await;
    expect_schema_validation_failed(result);
    println!("  First schema still enforced (invalid): PASS");

    // Unschema'd event type still passes
    let req = write_event(&agg, 99, 0, b"anything goes", 5, false);
    client.send_request(&req).await?;
    println!("  Unschema'd event type passes: PASS\n");

    // ========================================
    // PHASE 4: Hard restart — both schemas recovered from cold WAL scan
    // ========================================
    println!("PHASE 4: Hard restart — both schemas recovered from WAL");
    println!("--------------------------------------------------------");

    drop(client);
    server.restart().await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    // First schema still enforced
    let req = write_event(&agg, 1, 0, br#"{"name":"Hank","age":50}"#, 6, false);
    client.send_request(&req).await?;
    println!("  First schema valid write: PASS");

    let req = write_event(&agg, 1, 0, br#"{"age":50}"#, 7, false);
    let result = client.send_request(&req).await;
    expect_schema_validation_failed(result);
    println!("  First schema invalid write rejected: PASS");

    // Second schema still enforced
    let req = write_event(&agg, 2, 0, br#"{"id":99}"#, 7, false);
    client.send_request(&req).await?;
    println!("  Second schema valid write: PASS");

    let req = write_event(&agg, 2, 0, br#"{"id":"nope"}"#, 8, false);
    let result = client.send_request(&req).await;
    expect_schema_validation_failed(result);
    println!("  Second schema invalid write rejected: PASS");

    // Both duplicates rejected
    let req = register_schema_request(1, 100, 1, 0, schema.to_string());
    let result = client.send_request(&req).await;
    expect_schema_already_exists(result);
    println!("  First schema duplicate rejected: PASS");

    let req = register_schema_request(1, 100, 2, 0, schema2.to_string());
    let result = client.send_request(&req).await;
    expect_schema_already_exists(result);
    println!("  Second schema duplicate rejected: PASS\n");

    println!("=== All Zero-Cache Tests Passed ===");
    Ok(())
}
