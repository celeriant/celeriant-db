//! Phase 10 — schema formats beyond JSON (Avro, Protobuf) and payload encryption.
//!
//! Oracle:
//! - reference/schema-formats.md: `SchemaType` selects Json / Avro / Protobuf;
//!   "a write whose payload does not conform is rejected with
//!   WriteSchemaValidationFailed"; "Validation reads the payload, so it does not
//!   apply to an encrypted event ... validate or encrypt, not both."
//! - concepts/schemas.md: "Register a schema in JSON Schema, Avro, or Protobuf";
//!   the same server-side validation contract applies to each format.
//! - concepts/encryption.md + guides/encryption.md: the client encrypts and sends
//!   ciphertext + IV; "The server stores those opaque bytes and the IV, and hands
//!   them back unchanged on read. It never sees plaintext"; an encrypted event
//!   "cannot be schema-validated, because it cannot read the bytes."
//! - reference/error-codes.md: 2020 AlreadyExists, 2021 Invalid, 2022
//!   ValidationFailed.
//!
//! The same accept/reject contract proved for JSON in phase 4 must hold across
//! formats. Payload shapes (Avro binary, protobuf wire bytes) are derived from
//! the formats' own encodings — never from server internals.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{SchemaError, ServerError};
use celeriant_msg::request::requests::RegisterSchemaRequest;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::schema_key::SchemaKey;
use celeriant_wal::schema_type::SchemaType;
use std::sync::Arc;
use crate::TestServer;

use crate::common::{port_for, read_all, R};

const ORG: u128 = 7100;
const ATYPE: u128 = 71;

/// Build a register request for a (schema_type, major, minor) with a schema body.
fn register(schema_type: SchemaType, major: u64, minor: u64, schema: &str) -> RegisterSchemaRequest {
    RegisterSchemaRequest {
        correlation_id: None,
        client_id: 1,
        user_id: None,
        schema_key: SchemaKey::new(ORG, ATYPE, major, minor),
        schema_type: schema_type as u8,
        schema: schema.to_string(),
    }
}

/// A raw-bytes event with explicit (major, minor) and payload. Unlike the JSON
/// `common::event`, the payload is arbitrary bytes (Avro/Protobuf-encoded).
fn raw_event(client_seq: u64, major: u64, minor: u64, payload: Vec<u8>) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + client_seq,
        event_type_major: major,
        event_type_minor: minor,
        event_value: Arc::new(payload),
        iv: None,
    }
}

fn create_opts() -> WriteEventsOptions {
    WriteEventsOptions { allow_create: true, ..Default::default() }
}

// ---------------------------------------------------------------------------
// Avro
// ---------------------------------------------------------------------------

/// An Avro schema (JSON text) for a record with a single required `long` field
/// `amount`. The schema document is itself JSON — that is the Avro convention.
const AVRO_SCHEMA: &str = r#"{"type":"record","name":"Amount","fields":[{"name":"amount","type":"long"}]}"#;

/// Avro-binary-encode `{"amount": value}` against AVRO_SCHEMA.
fn avro_encode_amount(value: i64) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use apache_avro::types::Record;
    use apache_avro::{to_avro_datum, Schema};
    let schema = Schema::parse_str(AVRO_SCHEMA)?;
    let mut rec = Record::new(&schema).ok_or("could not build Avro record")?;
    rec.put("amount", value);
    Ok(to_avro_datum(&schema, rec)?)
}

/// 10.1 An Avro schema accepts a conforming Avro-encoded payload and stores it
/// unchanged (schema-formats: Avro is a supported `SchemaType`; the JSON contract
/// from phase 4 holds across formats).
pub async fn avro_accepts_conforming_payload() -> R {
    let server = TestServer::start_with_port(port_for("avro_accepts")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.register_schema(register(SchemaType::Avro, 1, 0, AVRO_SCHEMA)).await?;

    let key = AggregateKey::new(ORG, ATYPE, 1);
    let payload = avro_encode_amount(42)?;
    let ev = raw_event(1, 1, 0, payload.clone());
    c.write_events_with(key.clone(), vec![ev], create_opts()).await?;

    let batches = read_all(&mut c, &key).await?;
    if batches.len() != 1 {
        return Err(format!("avro conforming: expected 1 batch, got {}", batches.len()).into());
    }
    if batches[0].events[0].event_value.as_slice() != payload.as_slice() {
        return Err("avro conforming: stored payload bytes altered".into());
    }
    Ok(())
}

/// 10.2 An Avro schema rejects a non-conforming payload with
/// WriteSchemaValidationFailed (2022) and nothing is appended. The non-conforming
/// payload is encoded against an *incompatible* Avro schema (a `string` field
/// where the registered schema requires a `long`), so its bytes cannot decode
/// against the registered schema. (schema-formats: "a write whose payload does
/// not conform is rejected with WriteSchemaValidationFailed"; error-codes 2022.)
pub async fn avro_rejects_nonconforming_payload() -> R {
    let server = TestServer::start_with_port(port_for("avro_rejects")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.register_schema(register(SchemaType::Avro, 1, 0, AVRO_SCHEMA)).await?;

    // Avro binary is schemaless on the wire, so to be genuinely non-conforming
    // the bytes must fail to *decode* against the registered record{long} schema.
    // A single varint byte with the continuation bit set (0x80) and nothing after
    // is an incomplete zig-zag long: it cannot be decoded as the required `long`
    // field, so the record does not parse.
    let bad_payload = vec![0x80u8];

    let key = AggregateKey::new(ORG, ATYPE, 2);
    let res = c
        .write_events_with(key.clone(), vec![raw_event(1, 1, 0, bad_payload)], create_opts())
        .await;
    match res {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::ValidationFailed, .. })) => {}
        other => return Err(format!("avro non-conforming: expected ValidationFailed, got {other:?}").into()),
    }
    // Nothing landed.
    match read_all(&mut c, &key).await {
        Err(e) if e.to_string().contains("aggregate not exists") => Ok(()),
        Ok(b) if b.is_empty() => Ok(()),
        Ok(b) => Err(format!("avro rejected event still created {} batches", b.len()).into()),
        Err(e) => Err(format!("avro: unexpected read error after rejected write: {e}").into()),
    }
}

/// 10.3 A malformed Avro schema is rejected at registration with
/// RegisterSchemaInvalid (2021) (schema-formats: "A malformed schema is rejected
/// at registration with RegisterSchemaInvalid"; error-codes 2021).
pub async fn avro_malformed_schema_rejected() -> R {
    let server = TestServer::start_with_port(port_for("avro_bad_schema")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    // Not a valid Avro schema (unknown type).
    let res = c
        .register_schema(register(SchemaType::Avro, 9, 0, r#"{"type":"not-a-real-avro-type"}"#))
        .await;
    match res {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::Invalid, .. })) => Ok(()),
        // The server may classify an uncompilable schema as UnsupportedType (2024);
        // both are documented registration rejections for a schema it cannot use.
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::UnsupportedType, .. })) => Ok(()),
        other => Err(format!("avro malformed schema: expected Invalid/UnsupportedType, got {other:?}").into()),
    }
}

// ---------------------------------------------------------------------------
// Protobuf
// ---------------------------------------------------------------------------

/// Build the protobuf schema string the server expects:
/// `base64(FileDescriptorSet):MessageName`.
///
/// NOTE: the docs (reference/schema-formats.md, guides/schemas.md) say only that
/// a Protobuf schema is "submitted as a string" and give no example of its shape
/// (unlike JSON, which has a full example). The concrete required encoding —
/// base64 of a serialized `FileDescriptorSet`, a colon, then the message name —
/// was surfaced through the client API (the server's RegisterSchemaInvalid error
/// message). We build a real descriptor so the positive/negative payload tests
/// are meaningful. See FINDINGS F7 (protobuf schema format is undocumented).
fn proto_schema_string() -> String {
    use prost::Message;
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };

    let field = FieldDescriptorProto {
        name: Some("amount".to_string()),
        number: Some(1),
        label: Some(Label::Optional as i32),
        r#type: Some(Type::Int64 as i32),
        ..Default::default()
    };
    let message = DescriptorProto {
        name: Some("Amount".to_string()),
        field: vec![field],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("amount.proto".to_string()),
        package: Some("p10".to_string()),
        syntax: Some("proto3".to_string()),
        message_type: vec![message],
        ..Default::default()
    };
    let set = FileDescriptorSet { file: vec![file] };

    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(set.encode_to_vec());
    format!("{b64}:p10.Amount")
}

/// Hand-encode the protobuf wire bytes for `Amount { amount: value }`.
/// Field 1, wire type 0 (varint): tag byte = (1 << 3) | 0 = 0x08, then varint.
fn proto_encode_amount(value: u64) -> Vec<u8> {
    let mut out = vec![0x08u8];
    let mut v = value;
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
    out
}

/// 10.4 A Protobuf schema accepts a conforming protobuf-encoded payload and
/// stores it unchanged (schema-formats: Protobuf is a supported `SchemaType`).
pub async fn protobuf_accepts_conforming_payload() -> R {
    let server = TestServer::start_with_port(port_for("proto_accepts")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.register_schema(register(SchemaType::Protobuf, 1, 0, &proto_schema_string())).await?;

    let key = AggregateKey::new(ORG, ATYPE, 3);
    let payload = proto_encode_amount(42);
    c.write_events_with(key.clone(), vec![raw_event(1, 1, 0, payload.clone())], create_opts())
        .await?;

    let batches = read_all(&mut c, &key).await?;
    if batches.len() != 1 {
        return Err(format!("protobuf conforming: expected 1 batch, got {}", batches.len()).into());
    }
    if batches[0].events[0].event_value.as_slice() != payload.as_slice() {
        return Err("protobuf conforming: stored payload bytes altered".into());
    }
    Ok(())
}

/// 10.5 A Protobuf schema rejects a payload that cannot be parsed as the message
/// with WriteSchemaValidationFailed (2022) and nothing is appended. The bad
/// payload encodes field 1 with the wrong wire type (length-delimited, type 2)
/// where the schema declares an int64 (varint, type 0), then a truncated length
/// — bytes that are not a valid `Amount`. (schema-formats: same validation
/// contract across formats; error-codes 2022.)
pub async fn protobuf_rejects_nonconforming_payload() -> R {
    let server = TestServer::start_with_port(port_for("proto_rejects")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.register_schema(register(SchemaType::Protobuf, 1, 0, &proto_schema_string())).await?;

    // Tag for field 1 wire-type 2 = (1<<3)|2 = 0x0a, length 0x05, but only 1
    // payload byte follows — a truncated, unparseable message.
    let bad_payload = vec![0x0au8, 0x05, 0xff];

    let key = AggregateKey::new(ORG, ATYPE, 4);
    let res = c
        .write_events_with(key.clone(), vec![raw_event(1, 1, 0, bad_payload)], create_opts())
        .await;
    match res {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::ValidationFailed, .. })) => {}
        other => return Err(format!("protobuf non-conforming: expected ValidationFailed, got {other:?}").into()),
    }
    match read_all(&mut c, &key).await {
        Err(e) if e.to_string().contains("aggregate not exists") => Ok(()),
        Ok(b) if b.is_empty() => Ok(()),
        Ok(b) => Err(format!("protobuf rejected event still created {} batches", b.len()).into()),
        Err(e) => Err(format!("protobuf: unexpected read error after rejected write: {e}").into()),
    }
}

/// 10.6 A malformed Protobuf schema is rejected at registration (schema-formats:
/// "A malformed schema is rejected at registration with RegisterSchemaInvalid").
pub async fn protobuf_malformed_schema_rejected() -> R {
    let server = TestServer::start_with_port(port_for("proto_bad_schema")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let res = c
        .register_schema(register(SchemaType::Protobuf, 9, 0, "this is not a .proto file {{{"))
        .await;
    match res {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::Invalid, .. })) => Ok(()),
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::UnsupportedType, .. })) => Ok(()),
        other => Err(format!("protobuf malformed schema: expected Invalid/UnsupportedType, got {other:?}").into()),
    }
}

/// 10.7 Re-registering the same (Avro, major, minor) is rejected with
/// RegisterSchemaAlreadyExists (2020) — the "schemas are additive" rule holds for
/// non-JSON formats too (schema-formats; error-codes 2020).
pub async fn avro_duplicate_register_rejected() -> R {
    let server = TestServer::start_with_port(port_for("avro_dup")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.register_schema(register(SchemaType::Avro, 3, 0, AVRO_SCHEMA)).await?;
    let res = c.register_schema(register(SchemaType::Avro, 3, 0, AVRO_SCHEMA)).await;
    match res {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::AlreadyExists, .. })) => Ok(()),
        other => Err(format!("avro duplicate register: expected AlreadyExists, got {other:?}").into()),
    }
}

// ---------------------------------------------------------------------------
// Encryption round-trip
// ---------------------------------------------------------------------------

/// 10.8 Encryption round-trip: the server stores the opaque ciphertext AND its
/// IV and hands both back byte-for-byte unchanged on read; it never transforms
/// the payload. (encryption.md: "The server stores those opaque bytes and the IV,
/// and hands them back unchanged on read. It never sees plaintext"; guides/
/// encryption.md: "the server hands back exactly what you stored".)
pub async fn encrypted_payload_roundtrips_unchanged() -> R {
    let server = TestServer::start_with_port(port_for("enc_roundtrip")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    // Opaque "ciphertext" (the server cannot read it; bytes are arbitrary here)
    // followed by a 16-byte GCM tag, with a 12-byte IV/nonce — exactly the shape
    // the encryption guide describes.
    let ciphertext_and_tag: Vec<u8> = (0u8..48).collect(); // 32 cipher + 16 tag, arbitrary
    let iv: [u8; 12] = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 255, 128];

    let key = AggregateKey::new(ORG, ATYPE, 5);
    let mut ev = raw_event(1, 1, 0, ciphertext_and_tag.clone());
    ev.iv = Some(iv);
    c.write_events_with(key.clone(), vec![ev], create_opts()).await?;

    let batches = read_all(&mut c, &key).await?;
    if batches.len() != 1 || batches[0].events.len() != 1 {
        return Err(format!("encryption: expected 1 batch/1 event, got {} batches", batches.len()).into());
    }
    let stored = &batches[0].events[0];
    if stored.event_value.as_slice() != ciphertext_and_tag.as_slice() {
        return Err("encryption: ciphertext bytes were altered by the server".into());
    }
    match stored.iv {
        Some(returned) if returned == iv => Ok(()),
        Some(returned) => Err(format!("encryption: IV changed: stored {returned:?}, got back {:?}", stored.iv).into()),
        None => Err("encryption: server dropped the IV (must hand it back unchanged)".into()),
    }
}

/// 10.9 "Validate OR encrypt, not both": with a JSON schema registered for an
/// event type, an *encrypted* event of that type (IV present, payload is opaque
/// ciphertext that is NOT valid JSON) is NOT schema-validated and therefore
/// lands — because the server cannot read the bytes to validate them.
/// (schema-formats + encryption.md: "Validation reads the payload, so it does
/// not apply to an encrypted event ... the server cannot read those bytes.")
///
/// Control: the SAME non-JSON bytes WITHOUT an IV (i.e. a plaintext event of the
/// validated type) must be rejected, proving the schema is genuinely active and
/// it is the IV that suppresses validation.
pub async fn encrypted_payload_skips_schema_validation() -> R {
    let server = TestServer::start_with_port(port_for("enc_skips_schema")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    // A JSON schema requiring an integer `amount`.
    let json_schema = r#"{"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"]}"#;
    c.register_schema(register(SchemaType::Json, 1, 0, json_schema)).await?;

    // Opaque ciphertext that is NOT valid JSON for the schema.
    let opaque: Vec<u8> = vec![0x00, 0x01, 0x02, 0xfe, 0xff, 0xaa, 0x55];

    // Control: same bytes, NO iv -> plaintext of the validated type -> must be
    // rejected (proves the schema is enforced for this (major,minor)).
    let plain_key = AggregateKey::new(ORG, ATYPE, 6);
    let plain = raw_event(1, 1, 0, opaque.clone());
    match c.write_events_with(plain_key, vec![plain], create_opts()).await {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::ValidationFailed, .. })) => {}
        other => {
            return Err(format!(
                "encryption-vs-validation control: a plaintext non-JSON payload of a validated type should be rejected, got {other:?}"
            ).into())
        }
    }

    // Now the encrypted variant: same bytes, WITH an IV -> validation is skipped
    // and the event lands.
    let enc_key = AggregateKey::new(ORG, ATYPE, 7);
    let mut enc = raw_event(1, 1, 0, opaque.clone());
    enc.iv = Some([1u8; 12]);
    c.write_events_with(enc_key.clone(), vec![enc], create_opts())
        .await
        .map_err(|e| format!("encrypted event of a validated type should NOT be validated, but write failed: {e:?}"))?;

    let batches = read_all(&mut c, &enc_key).await?;
    if batches.len() != 1 {
        return Err(format!("encryption-skips-validation: expected 1 batch, got {}", batches.len()).into());
    }
    if batches[0].events[0].event_value.as_slice() != opaque.as_slice() {
        return Err("encryption-skips-validation: stored ciphertext altered".into());
    }
    Ok(())
}
