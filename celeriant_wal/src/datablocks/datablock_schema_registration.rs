use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::schema_type::SchemaType;

/// Datablock for a single schema registration
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct DatablockSchemaRegistration {
    pub schema_type: SchemaType,
    pub schema: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bincode_round_trip() {
        let config = bincode::config::standard().with_fixed_int_encoding();
        let original = DatablockSchemaRegistration {
            schema_type: SchemaType::Json,
            schema: r#"{"type":"object"}"#.to_string(),
        };
        let encoded = bincode::encode_to_vec(&original, config).unwrap();
        let (decoded, _): (DatablockSchemaRegistration, _) =
            bincode::decode_from_slice(&encoded, config).unwrap();
        assert_eq!(decoded.schema_type, SchemaType::Json);
        assert_eq!(decoded.schema, original.schema);
    }
}
