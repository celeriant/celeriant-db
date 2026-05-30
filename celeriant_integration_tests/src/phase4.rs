//! Phase 4 — schema validation.
//!
//! Oracle: celeriant-docs/docs/concepts/schemas.md,
//! reference/schema-formats.md, reference/error-codes.md.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{SchemaError, ServerError};
use celeriant_msg::request::requests::RegisterSchemaRequest;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::schema_key::SchemaKey;
use celeriant_wal::schema_type::SchemaType;
use crate::TestServer;

use crate::common::{event, port_for, read_all, R};

const ORG: u128 = 7000;
const ATYPE: u128 = 42;

/// A JSON Schema requiring an integer `amount` property.
const SCHEMA: &str = r#"{"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"]}"#;

fn register(major: u64, minor: u64) -> RegisterSchemaRequest {
    RegisterSchemaRequest {
        correlation_id: None,
        client_id: 1,
        user_id: None,
        schema_key: SchemaKey::new(ORG, ATYPE, major, minor),
        schema_type: SchemaType::Json as u8,
        schema: SCHEMA.to_string(),
    }
}

/// 4.1 A registered schema rejects a non-conforming event with
/// WriteSchemaValidationFailed (2022) and nothing is appended (schemas
/// "Server-side, not client-side hope"; error-codes 2022).
pub async fn schema_rejects_bad_event() -> R {
    let server = TestServer::start_with_port(port_for("schema_rejects_bad_event")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.register_schema(register(1, 0)).await?;

    let key = AggregateKey::new(ORG, ATYPE, 1);
    let mut bad = event(1, 1, 1000, r#"{"amount":"not-an-integer"}"#); // wrong type
    bad.event_type_minor = 0;
    let res = c
        .write_events_with(key.clone(), vec![bad], WriteEventsOptions { allow_create: true, ..Default::default() })
        .await;
    match res {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::ValidationFailed, .. })) => {}
        other => return Err(format!("expected SchemaError::ValidationFailed, got {other:?}").into()),
    }
    // Nothing landed: the aggregate must not exist.
    match read_all(&mut c, &key).await {
        Err(e) if e.to_string().contains("aggregate not exists") => Ok(()),
        Ok(b) if b.is_empty() => Ok(()),
        Ok(b) => Err(format!("rejected event still created {} batches", b.len()).into()),
        Err(e) => Err(format!("unexpected read error after rejected write: {e}").into()),
    }
}

/// 4.2 A conforming event passes validation and is durably appended (schemas).
pub async fn schema_accepts_good_event() -> R {
    let server = TestServer::start_with_port(port_for("schema_accepts_good_event")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.register_schema(register(1, 0)).await?;

    let key = AggregateKey::new(ORG, ATYPE, 2);
    let mut good = event(1, 1, 1000, r#"{"amount":42}"#);
    good.event_type_minor = 0;
    c.write_events_with(key.clone(), vec![good], WriteEventsOptions { allow_create: true, ..Default::default() })
        .await?;

    let batches = read_all(&mut c, &key).await?;
    if batches.len() != 1 {
        return Err(format!("conforming event: expected 1 batch, got {}", batches.len()).into());
    }
    let payload = String::from_utf8_lossy(&batches[0].events[0].event_value);
    if !payload.contains("\"amount\":42") {
        return Err(format!("stored payload altered: {payload}").into());
    }
    Ok(())
}

/// 4.3 Re-registering the same (type, major, minor) -> RegisterSchemaAlreadyExists
/// (2020) (schema-formats "Schemas are additive"; error-codes 2020).
pub async fn schema_duplicate_register_rejected() -> R {
    let server = TestServer::start_with_port(port_for("schema_duplicate_register_rejected")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.register_schema(register(5, 0)).await?;

    let res = c.register_schema(register(5, 0)).await;
    match res {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::AlreadyExists, .. })) => Ok(()),
        other => Err(format!("expected SchemaError::AlreadyExists, got {other:?}").into()),
    }
}

/// 4.4 A malformed schema is rejected at registration with RegisterSchemaInvalid
/// (2021) (schema-formats; error-codes 2021).
pub async fn schema_invalid_register_rejected() -> R {
    let server = TestServer::start_with_port(port_for("schema_invalid_register_rejected")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    let bad = RegisterSchemaRequest {
        correlation_id: None,
        client_id: 1,
        user_id: None,
        schema_key: SchemaKey::new(ORG, ATYPE, 6, 0),
        schema_type: SchemaType::Json as u8,
        schema: "{ this is not valid json schema ".to_string(),
    };
    let res = c.register_schema(bad).await;
    match res {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::Invalid, .. })) => Ok(()),
        other => Err(format!("expected SchemaError::Invalid, got {other:?}").into()),
    }
}

/// 4.5 Validation is opt-in per (type, major, minor): an event of a version that
/// has no registered schema is unvalidated, so a payload that would fail the
/// registered version still lands under an unregistered minor (schemas
/// "Validation is opt-in per type"; schema-formats "per (major,minor)").
pub async fn schema_unregistered_version_unvalidated() -> R {
    let server = TestServer::start_with_port(port_for("schema_unregistered_version_unvalidated")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    // Register only (major=1, minor=0).
    c.register_schema(register(1, 0)).await?;

    let key = AggregateKey::new(ORG, ATYPE, 3);
    // Same major, DIFFERENT minor (1) — no schema registered for (1,1).
    // Payload would fail the (1,0) schema, but (1,1) is unvalidated.
    let mut ev = event(1, 1, 1000, r#"{"totally":"unrelated"}"#);
    ev.event_type_minor = 1;
    c.write_events_with(key.clone(), vec![ev], WriteEventsOptions { allow_create: true, ..Default::default() })
        .await?;

    let batches = read_all(&mut c, &key).await?;
    if batches.len() != 1 {
        return Err(format!("unvalidated version: expected 1 batch, got {}", batches.len()).into());
    }
    if batches[0].events[0].event_type_minor != 1 {
        return Err("stored event lost its minor version".into());
    }
    Ok(())
}
