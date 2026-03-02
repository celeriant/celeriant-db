use serde_json::Value;
use celeriant_wal::SchemaType;
use celeriant_memcache::cached_schema::{Validate, CachedValidator};
use std::rc::Rc;

/// Compiled JSON Schema validator for hot-path validation.
pub struct CompiledValidator {
    compiled: jsonschema::Validator,
    schema_size: usize,
}

impl CompiledValidator {
    pub fn compile(schema_type: SchemaType, schema: &str) -> Result<CachedValidator<Self>, String> {
        match schema_type {
            SchemaType::Json => {
                let schema_value: Value = serde_json::from_str(schema)
                    .map_err(|e| format!("Invalid JSON schema: {}", e))?;
                let compiled = jsonschema::validator_for(&schema_value)
                    .map_err(|e| format!("Schema compilation failed: {}", e))?;
                let schema_size = schema.len();
                let validator = Self {
                    compiled,
                    schema_size
                };
                let size_estimate = validator.deep_size_estimate();
                Ok(CachedValidator::new(Rc::new(validator), size_estimate))
            }
            _ => Err(format!("Unsupported schema type: {:?}", schema_type)),
        }
    }

    fn deep_size_estimate(&self) -> usize {
        self.schema_size * 5
    }
}

impl Validate for CompiledValidator {
    fn validate(&self, event_value: &[u8]) -> Result<(), String> {
        let json: Value = serde_json::from_slice(event_value)
            .map_err(|e| format!("Event value is not valid JSON: {}", e))?;

        if let Err(error) = self.compiled.validate(&json) {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}"#;

    fn compiled() -> CachedValidator<CompiledValidator> {
        CompiledValidator::compile(SchemaType::Json, SCHEMA).unwrap()
    }

    #[test]
    fn compile_valid_json_schema() {
        let v = compiled();
        assert!(v.validate(br#"{"name":"alice","age":30}"#).is_ok());
    }

    #[test]
    fn reject_missing_required_field() {
        let err = compiled().validate(br#"{"name":"alice"}"#).unwrap_err();
        assert!(err.contains("age"), "expected mention of missing field: {err}");
    }

    #[test]
    fn reject_wrong_type() {
        let err = compiled().validate(br#"{"name":"alice","age":"thirty"}"#).unwrap_err();
        assert!(err.contains("integer") || err.contains("type"), "expected type error: {err}");
    }

    #[test]
    fn reject_non_json_bytes() {
        let err = compiled().validate(b"not json at all").unwrap_err();
        assert!(err.contains("not valid JSON"), "{err}");
    }

    #[test]
    fn compile_invalid_json_string() {
        let err = CompiledValidator::compile(SchemaType::Json, "{{broken").unwrap_err();
        assert!(err.contains("Invalid JSON schema"), "{err}");
    }

    #[test]
    fn compile_unsupported_schema_type() {
        for st in [SchemaType::Avro, SchemaType::Protobuf] {
            let err = CompiledValidator::compile(st, SCHEMA).unwrap_err();
            assert!(err.contains("Unsupported"), "{err}");
        }
    }

    #[test]
    fn validation_error_truncated_at_4096() {
        // Schema requiring many properties to generate a long error
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
}
