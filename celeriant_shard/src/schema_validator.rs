use base64::Engine;
use celeriant_memcache::cached_schema::{CachedValidator, Validate};
use celeriant_wal::SchemaType;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use std::rc::Rc;

enum Inner {
    Json(jsonschema::Validator),
    Avro(apache_avro::Schema),
    Protobuf(MessageDescriptor),
}

pub struct CompiledValidator {
    inner: Inner,
    schema_size: usize,
}

impl CompiledValidator {
    pub fn compile(schema_type: SchemaType, schema: &str) -> Result<CachedValidator<Self>, String> {
        let inner = match schema_type {
            SchemaType::Json => {
                let schema_value: serde_json::Value = serde_json::from_str(schema)
                    .map_err(|e| format!("Invalid JSON schema: {}", e))?;
                let compiled = jsonschema::validator_for(&schema_value)
                    .map_err(|e| format!("Schema compilation failed: {}", e))?;
                Inner::Json(compiled)
            }
            SchemaType::Avro => {
                let compiled = apache_avro::Schema::parse_str(schema)
                    .map_err(|e| format!("Invalid Avro schema: {}", e))?;
                Inner::Avro(compiled)
            }
            SchemaType::Protobuf => compile_protobuf(schema)?,
        };

        let schema_size = schema.len();
        let validator = Self { inner, schema_size };
        let size_estimate = validator.deep_size_estimate();
        Ok(CachedValidator::new(Rc::new(validator), size_estimate))
    }

    fn deep_size_estimate(&self) -> usize {
        self.schema_size * 5
    }
}

impl Validate for CompiledValidator {
    fn validate(&self, event_value: &[u8]) -> Result<(), String> {
        match &self.inner {
            Inner::Json(compiled) => validate_json(compiled, event_value),
            Inner::Avro(schema) => validate_avro(schema, event_value),
            Inner::Protobuf(descriptor) => validate_protobuf(descriptor, event_value),
        }
    }
}

fn validate_json(compiled: &jsonschema::Validator, event_value: &[u8]) -> Result<(), String> {
    let json: serde_json::Value = serde_json::from_slice(event_value)
        .map_err(|e| format!("Event value is not valid JSON: {}", e))?;

    if let Err(error) = compiled.validate(&json) {
        let mut msg = format!("{}", error);
        if msg.len() > 4096 {
            msg.truncate(4093);
            msg.push_str("...");
        }
        Err(msg)
    } else {
        Ok(())
    }
}

fn validate_avro(schema: &apache_avro::Schema, event_value: &[u8]) -> Result<(), String> {
    let mut cursor = &event_value[..];
    let value = apache_avro::from_avro_datum(schema, &mut cursor, None)
        .map_err(|e| format!("Avro validation failed: {}", e))?;
    if !value.validate(schema) {
        return Err("Avro schema validation failed: decoded value does not match schema".to_string());
    }
    Ok(())
}

fn compile_protobuf(schema: &str) -> Result<Inner, String> {
    let (fds_b64, message_name) = schema
        .split_once(':')
        .ok_or("Invalid protobuf schema format: expected 'base64(FileDescriptorSet):MessageName'")?;

    let fds_bytes = base64::engine::general_purpose::STANDARD
        .decode(fds_b64)
        .map_err(|e| format!("Invalid protobuf schema: base64 decode failed: {e}"))?;

    let pool = prost_reflect::DescriptorPool::decode(fds_bytes.as_slice())
        .map_err(|e| format!("Invalid protobuf schema: descriptor parse failed: {e}"))?;

    let descriptor = pool
        .get_message_by_name(message_name)
        .ok_or_else(|| format!("Invalid protobuf schema: message '{message_name}' not found in descriptor"))?;

    Ok(Inner::Protobuf(descriptor))
}

fn validate_protobuf(descriptor: &MessageDescriptor, event_value: &[u8]) -> Result<(), String> {
    DynamicMessage::decode(descriptor.clone(), event_value)
        .map_err(|e| format!("Protobuf validation failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- JSON Schema tests ---

    const JSON_SCHEMA: &str = r#"{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}"#;

    fn json_compiled() -> CachedValidator<CompiledValidator> {
        CompiledValidator::compile(SchemaType::Json, JSON_SCHEMA).unwrap()
    }

    #[test]
    fn compile_valid_json_schema() {
        let v = json_compiled();
        assert!(v.validate(br#"{"name":"alice","age":30}"#).is_ok());
    }

    #[test]
    fn reject_missing_required_field() {
        let err = json_compiled().validate(br#"{"name":"alice"}"#).unwrap_err();
        assert!(err.contains("age"), "expected mention of missing field: {err}");
    }

    #[test]
    fn reject_wrong_type() {
        let err = json_compiled().validate(br#"{"name":"alice","age":"thirty"}"#).unwrap_err();
        assert!(err.contains("integer") || err.contains("type"), "expected type error: {err}");
    }

    #[test]
    fn reject_non_json_bytes() {
        let err = json_compiled().validate(b"not json at all").unwrap_err();
        assert!(err.contains("not valid JSON"), "{err}");
    }

    #[test]
    fn compile_invalid_json_string() {
        let err = CompiledValidator::compile(SchemaType::Json, "{{broken").unwrap_err();
        assert!(err.contains("Invalid JSON schema"), "{err}");
    }

    // --- Protobuf Schema tests ---

    /// Build a schema string from a FileDescriptorSet and message name.
    fn proto_schema(fds_bytes: &[u8], message_name: &str) -> String {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(fds_bytes);
        format!("{b64}:{message_name}")
    }

    /// Build a minimal FileDescriptorSet with a single message containing a string field.
    fn simple_fds() -> Vec<u8> {
        use prost::Message;
        use prost_reflect::prost_types::{
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
            field_descriptor_proto::{Label, Type},
        };

        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".to_string()),
                package: Some("test".to_string()),
                syntax: Some("proto3".to_string()),
                message_type: vec![DescriptorProto {
                    name: Some("TestEvent".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("name".to_string()),
                        number: Some(1),
                        r#type: Some(Type::String.into()),
                        label: Some(Label::Optional.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        fds.encode_to_vec()
    }

    fn proto_compiled() -> CachedValidator<CompiledValidator> {
        let fds = simple_fds();
        let schema = proto_schema(&fds, "test.TestEvent");
        CompiledValidator::compile(SchemaType::Protobuf, &schema).unwrap()
    }

    #[test]
    fn compile_valid_protobuf_schema() {
        // Field 1 (string), length-delimited: tag=0x0a, len=5, "alice"
        let mut buf = Vec::new();
        prost::encoding::string::encode(1, &"alice".to_string(), &mut buf);
        assert!(proto_compiled().validate(&buf).is_ok());
    }

    #[test]
    fn protobuf_empty_message_is_valid() {
        // Empty bytes are valid for any proto3 message (all fields optional)
        assert!(proto_compiled().validate(b"").is_ok());
    }

    #[test]
    fn protobuf_reject_malformed_bytes() {
        // 0x0a = field 1, length-delimited; 0x05 = 5 bytes follow; but only 2 bytes present
        let err = proto_compiled().validate(&[0x0a, 0x05, 0x41, 0x42]).unwrap_err();
        assert!(err.contains("Protobuf validation failed"), "{err}");
    }

    #[test]
    fn protobuf_reject_invalid_utf8_string() {
        // Field 1 as string with invalid UTF-8
        let err = proto_compiled().validate(&[0x0a, 0x02, 0xff, 0xfe]).unwrap_err();
        assert!(err.contains("Protobuf validation failed"), "{err}");
    }

    #[test]
    fn protobuf_unknown_fields_pass() {
        // Protobuf preserves unknown fields — a message with extra fields not in the
        // schema decodes successfully. This is by design for schema evolution: adding
        // new fields to a schema should be backward-compatible.
        let v = proto_compiled();

        // Encode field 1 (name, string) + field 99 (unknown, varint)
        let mut buf = Vec::new();
        prost::encoding::string::encode(1, &"alice".to_string(), &mut buf);
        prost::encoding::int32::encode(99, &42, &mut buf);
        assert!(v.validate(&buf).is_ok());
    }

    #[test]
    fn protobuf_wire_type_mismatch_rejected() {
        // Schema has field 1 as string (length-delimited, wire type 2).
        // Send field 1 as a varint (wire type 0) — this is a wire type mismatch.
        let v = proto_compiled();

        let mut buf = Vec::new();
        prost::encoding::int32::encode(1, &42, &mut buf); // varint where string expected
        let err = v.validate(&buf).unwrap_err();
        assert!(err.contains("Protobuf validation failed"), "{err}");
    }

    #[test]
    fn protobuf_missing_fields_pass() {
        // In proto3, all fields are optional. A message with only field 1 set
        // is valid even though the schema also defines field 2.
        // (simple_fds has: name=string field 1)
        let v = proto_compiled();

        // Only encode field 1, skip everything else
        let mut buf = Vec::new();
        prost::encoding::string::encode(1, &"alice".to_string(), &mut buf);
        assert!(v.validate(&buf).is_ok());
    }

    #[test]
    fn protobuf_compile_missing_separator() {
        let err = CompiledValidator::compile(SchemaType::Protobuf, "no_colon_here").unwrap_err();
        assert!(err.contains("expected"), "{err}");
    }

    #[test]
    fn protobuf_compile_bad_base64() {
        let err = CompiledValidator::compile(SchemaType::Protobuf, "!!!invalid!!!:test.Msg").unwrap_err();
        assert!(err.contains("base64"), "{err}");
    }

    #[test]
    fn protobuf_compile_message_not_found() {
        let fds = simple_fds();
        let schema = proto_schema(&fds, "test.NonExistent");
        let err = CompiledValidator::compile(SchemaType::Protobuf, &schema).unwrap_err();
        assert!(err.contains("not found"), "{err}");
    }

    #[test]
    fn json_validation_error_truncated_at_4096() {
        let mut props = String::from(r#"{"type":"object","properties":{"#);
        for i in 0..200 {
            if i > 0 { props.push(','); }
            props.push_str(&format!(r#""field_{i}_{pad}": {{"type":"string"}}"#, pad = "x".repeat(20)));
        }
        props.push_str(r#"},"required":["#);
        for i in 0..200 {
            if i > 0 { props.push(','); }
            props.push_str(&format!(r#""field_{i}_{pad}""#, pad = "x".repeat(20)));
        }
        props.push_str("]}");

        let v = CompiledValidator::compile(SchemaType::Json, &props).unwrap();
        let err = v.validate(b"{}").unwrap_err();
        assert!(err.len() <= 4096, "error len {} exceeds 4096", err.len());
    }

    // --- Avro Schema tests ---

    const AVRO_SCHEMA: &str = r#"{
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }"#;

    fn avro_compiled() -> CachedValidator<CompiledValidator> {
        CompiledValidator::compile(SchemaType::Avro, AVRO_SCHEMA).unwrap()
    }

    fn avro_encode(schema_str: &str, value: apache_avro::types::Value) -> Vec<u8> {
        let schema = apache_avro::Schema::parse_str(schema_str).unwrap();
        apache_avro::to_avro_datum(&schema, value).unwrap()
    }

    #[test]
    fn compile_valid_avro_schema() {
        let record = apache_avro::types::Value::Record(vec![
            ("name".to_string(), apache_avro::types::Value::String("alice".to_string())),
            ("age".to_string(), apache_avro::types::Value::Int(30)),
        ]);
        let encoded = avro_encode(AVRO_SCHEMA, record);
        assert!(avro_compiled().validate(&encoded).is_ok());
    }

    #[test]
    fn avro_reject_invalid_bytes() {
        let err = avro_compiled().validate(b"not avro data").unwrap_err();
        assert!(err.contains("Avro"), "{err}");
    }

    #[test]
    fn avro_reject_truncated_data() {
        let record = apache_avro::types::Value::Record(vec![
            ("name".to_string(), apache_avro::types::Value::String("alice".to_string())),
            ("age".to_string(), apache_avro::types::Value::Int(30)),
        ]);
        let mut encoded = avro_encode(AVRO_SCHEMA, record);
        encoded.truncate(2); // corrupt by truncation
        let err = avro_compiled().validate(&encoded).unwrap_err();
        assert!(err.contains("Avro"), "{err}");
    }

    #[test]
    fn compile_invalid_avro_schema() {
        let err = CompiledValidator::compile(SchemaType::Avro, "not valid avro").unwrap_err();
        assert!(err.contains("Invalid Avro schema"), "{err}");
    }
}
