use celeriant_wal::SchemaType;
use celeriant_memcache::cached_schema::{Validate, CachedValidator};
use std::rc::Rc;

enum Inner {
    Json(jsonschema::Validator),
    Avro(apache_avro::Schema),
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
            _ => return Err(format!("Unsupported schema type: {:?}", schema_type)),
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

    #[test]
    fn compile_unsupported_schema_type() {
        let err = CompiledValidator::compile(SchemaType::Protobuf, JSON_SCHEMA).unwrap_err();
        assert!(err.contains("Unsupported"), "{err}");
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
