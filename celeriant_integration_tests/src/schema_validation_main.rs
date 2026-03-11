//! Schema Validation Integration Tests
//!
//! End-to-end tests for schema registration, write-path validation,
//! duplicate rejection, invalid schema rejection, and WAL recovery after restart.
//!
//! Run with: cargo run --bin schema_validation_main -p celeriant_integration_tests --release

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_integration_tests::{ServerConfig, TestServer};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::requests::{RegisterSchemaRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    schema_key::SchemaKey,
};

const CLIENT_ID: u128 = 7777;

/// Build a protobuf schema string (base64 FileDescriptorSet + message name) for a message
/// with fields: name (string, field 1) and id (int32, field 2).
fn build_proto_schema(message_name: &str) -> String {
    use base64::Engine;
    use prost::Message;
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto::{Label, Type},
    };

    let parts: Vec<&str> = message_name.rsplitn(2, '.').collect();
    let (msg_name, package) = if parts.len() == 2 {
        (parts[0], Some(parts[1].to_string()))
    } else {
        (parts[0], None)
    };

    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package,
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some(msg_name.to_string()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("name".to_string()),
                        number: Some(1),
                        r#type: Some(Type::String.into()),
                        label: Some(Label::Optional.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("id".to_string()),
                        number: Some(2),
                        r#type: Some(Type::Int32.into()),
                        label: Some(Label::Optional.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let fds_bytes = fds.encode_to_vec();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&fds_bytes);
    format!("{b64}:{message_name}")
}

fn json_schema_for_name_age() -> String {
    r#"{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}"#.to_string()
}

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
    batch_index: u64,
    allow_create: bool,
) -> ClientRequest {
    let event = DatablockAggregateEvent {
        client_event_index: 0,
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
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create,
            expected_event_batch_index: Some(batch_index),
            enforce_client_idempotency: false,
            compression_type_id: 0,
            compression_level: None,
        },
    );

    ClientRequest::Write(WriteRequest {
        correlation_id: Some(rand::random()),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    })
}

fn expect_error_code(result: Result<impl std::fmt::Debug, ClientError>, expected_code: u32) {
    match result {
        Err(ClientError::CeleriantError(e)) => {
            assert_eq!(
                e.error_code, expected_code,
                "Expected error code {}, got {}: {}",
                expected_code, e.error_code, e.error_message
            );
        }
        Ok(resp) => panic!("Expected error {}, got success: {:?}", expected_code, resp),
        Err(e) => panic!("Expected CeleriantError({}), got: {:?}", expected_code, e),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Schema Validation Integration Tests ===\n");

    let port = 10800 + (std::process::id() % 100) as u16;

    // --- Single-shard standalone tests ---
    println!("--- Phase 1: Single-shard standalone ---\n");

    let mut server = TestServer::start_with_port(port).await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    let agg = AggregateKey::new(1, 100, 1);

    // Test 1: Write without schema — should pass through
    println!("Test 1: Write without schema passes through");
    let valid_json = br#"{"name":"Alice","age":30}"#;
    let req = write_event(&agg, 1, 0, valid_json, 0, true);
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 2: Register a JSON schema
    println!("Test 2: Register JSON schema");
    let schema = json_schema_for_name_age();
    let req = register_schema_request(1, 100, 1, 0, schema.clone());
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 3: Write valid event against registered schema
    println!("Test 3: Write valid event against schema");
    let req = write_event(&agg, 1, 0, valid_json, 1, false);
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 4: Write invalid event — missing required field "age"
    println!("Test 4: Write invalid event rejected (error 2022)");
    let invalid_json = br#"{"name":"Bob"}"#;
    let req = write_event(&agg, 1, 0, invalid_json, 2, false);
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2022);
    println!("  PASS\n");

    // Test 5: Write with wrong type — "age" is string instead of integer
    println!("Test 5: Write with wrong type rejected (error 2022)");
    let wrong_type = br#"{"name":"Carol","age":"thirty"}"#;
    let req = write_event(&agg, 1, 0, wrong_type, 2, false);
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2022);
    println!("  PASS\n");

    // Test 6: Write non-JSON bytes — should fail validation
    println!("Test 6: Write non-JSON bytes rejected (error 2022)");
    let not_json = b"this is not json";
    let req = write_event(&agg, 1, 0, not_json, 2, false);
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2022);
    println!("  PASS\n");

    // Test 7: Duplicate schema registration — error 2020
    println!("Test 7: Duplicate schema registration rejected (error 2020)");
    let req = register_schema_request(1, 100, 1, 0, schema.clone());
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2020);
    println!("  PASS\n");

    // Test 8: Invalid schema — malformed JSON
    println!("Test 8: Invalid schema rejected (error 2021)");
    let req = register_schema_request(1, 100, 2, 0, "not valid json schema {{{".to_string());
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2021);
    println!("  PASS\n");

    // Test 9: Register an Avro schema
    println!("Test 9: Register Avro schema");
    let avro_schema = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"}]}"#.to_string();
    let req = ClientRequest::RegisterSchema(RegisterSchemaRequest {
        correlation_id: Some(rand::random()),
        client_id: CLIENT_ID,
        user_id: None,
        schema_key: SchemaKey::new(1, 100, 3, 0),
        schema_type: 1, // Avro
        schema: avro_schema.clone(),
    });
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 9a: Write valid Avro-encoded event
    println!("Test 9a: Write valid Avro event against Avro schema");
    let avro_parsed = apache_avro::Schema::parse_str(&avro_schema).unwrap();
    let valid_avro = apache_avro::to_avro_datum(
        &avro_parsed,
        apache_avro::types::Value::Record(vec![
            ("id".to_string(), apache_avro::types::Value::Int(42)),
            ("label".to_string(), apache_avro::types::Value::String("hello".to_string())),
        ]),
    ).unwrap();
    let agg3 = AggregateKey::new(1, 100, 3);
    let req = write_event(&agg3, 3, 0, &valid_avro, 0, true);
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 9b: Write invalid bytes against Avro schema — should fail
    println!("Test 9b: Write invalid bytes against Avro schema rejected (error 2022)");
    let req = write_event(&agg3, 3, 0, b"not avro data", 1, false);
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2022);
    println!("  PASS\n");

    // Test 9c: Register a Protobuf schema
    println!("Test 9c: Register Protobuf schema");
    let proto_schema_str = build_proto_schema("test.TestEvent");
    let req = ClientRequest::RegisterSchema(RegisterSchemaRequest {
        correlation_id: Some(rand::random()),
        client_id: CLIENT_ID,
        user_id: None,
        schema_key: SchemaKey::new(1, 100, 4, 0),
        schema_type: 2, // Protobuf
        schema: proto_schema_str,
    });
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 9d: Write valid Protobuf-encoded event
    println!("Test 9d: Write valid Protobuf event against Protobuf schema");
    let mut valid_proto = Vec::new();
    prost::encoding::string::encode(1, &"hello".to_string(), &mut valid_proto);
    prost::encoding::int32::encode(2, &42, &mut valid_proto);
    let agg4 = AggregateKey::new(1, 100, 4);
    let req = write_event(&agg4, 4, 0, &valid_proto, 0, true);
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 9e: Write malformed bytes against Protobuf schema — should fail
    println!("Test 9e: Write malformed bytes against Protobuf schema rejected (error 2022)");
    // Truncated length-delimited field: tag says 5 bytes follow but only 2 present
    let req = write_event(&agg4, 4, 0, &[0x0a, 0x05, 0x41, 0x42], 1, false);
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2022);
    println!("  PASS\n");

    // Test 9f: Write invalid UTF-8 string against Protobuf schema — should fail
    println!("Test 9f: Write invalid UTF-8 against Protobuf schema rejected (error 2022)");
    let req = write_event(&agg4, 4, 0, &[0x0a, 0x02, 0xff, 0xfe], 1, false);
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2022);
    println!("  PASS\n");

    // Test 9g: Invalid protobuf schema — bad base64
    println!("Test 9g: Invalid protobuf schema rejected (error 2021)");
    let req = ClientRequest::RegisterSchema(RegisterSchemaRequest {
        correlation_id: Some(rand::random()),
        client_id: CLIENT_ID,
        user_id: None,
        schema_key: SchemaKey::new(1, 100, 5, 0),
        schema_type: 2,
        schema: "!!!not-base64!!!:test.Msg".to_string(),
    });
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2021);
    println!("  PASS\n");

    // Test 10: Write to a different event_type_minor (no schema) — should pass
    println!("Test 10: Write to unregistered minor version passes through");
    let agg2 = AggregateKey::new(1, 100, 2);
    let req = write_event(&agg2, 1, 99, b"anything goes", 0, true);
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // --- Phase 2: WAL recovery after restart (tests bloom filter + reverse scan) ---
    println!("--- Phase 2: Schema survives restart ---\n");

    drop(client);
    server.restart().await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    // Test 11: Write valid event after restart — cache is cold, triggers WAL scan
    println!("Test 11: Valid write succeeds after restart (WAL recovery)");
    let req = write_event(&agg, 1, 0, valid_json, 2, false);
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 12: Invalid write still rejected after restart
    println!("Test 12: Invalid write rejected after restart");
    let req = write_event(&agg, 1, 0, invalid_json, 3, false);
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2022);
    println!("  PASS\n");

    // Test 13: Duplicate registration still rejected after restart
    println!("Test 13: Duplicate registration rejected after restart");
    let req = register_schema_request(1, 100, 1, 0, schema.clone());
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2020);
    println!("  PASS\n");

    drop(client);
    drop(server);

    // --- Phase 3: Multi-shard coordination ---
    println!("--- Phase 3: Multi-shard coordination ---\n");

    let config = ServerConfig {
        num_shards: Some(4),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let server = TestServer::start_with_config(port + 50, config).await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    let agg_ms = AggregateKey::new(2, 200, 1);

    // Create aggregate first
    let req = write_event(&agg_ms, 1, 0, b"pre-schema data", 0, true);
    client.send_request(&req, CompressionType::None).await?;

    // Test 14: Register schema on multi-shard server (routes to shard 0, propagates)
    println!("Test 14: Register schema on multi-shard server");
    let ms_schema = r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#.to_string();
    let req = register_schema_request(2, 200, 1, 0, ms_schema);
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 15: Valid write goes through on multi-shard
    println!("Test 15: Valid write on multi-shard server");
    let req = write_event(&agg_ms, 1, 0, br#"{"id":42}"#, 1, false);
    client.send_request(&req, CompressionType::None).await?;
    println!("  PASS\n");

    // Test 16: Invalid write rejected on multi-shard
    println!("Test 16: Invalid write rejected on multi-shard server (error 2022)");
    let req = write_event(&agg_ms, 1, 0, br#"{"id":"not_a_number"}"#, 2, false);
    let result = client.send_request(&req, CompressionType::None).await;
    expect_error_code(result, 2022);
    println!("  PASS\n");

    println!("=== All schema validation tests passed! ===");
    Ok(())
}
