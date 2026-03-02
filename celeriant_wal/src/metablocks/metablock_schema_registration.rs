use crate::schema_key::SchemaKey;
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

/// Metablock for a single schema registration
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct MetablockSchemaRegistration {
    pub schema_key: SchemaKey,
    pub client_id: u128,
    pub user_id: Option<u128>,
}

impl MetablockSchemaRegistration {
    // Wire format layout (bincode fixed-int encoding)
    // SchemaKey is serialized inline (org_id, aggregate_type_id, event_type_major, event_type_minor)
    // Update these if field order or types change!

    const WIRE_SIZE_CLIENT_ID: usize = 16;

    pub const OFFSET_SCHEMA_KEY: usize = 0;

    pub const OFFSET_CLIENT_ID: usize =
        Self::OFFSET_SCHEMA_KEY + SchemaKey::WIRE_SIZE_TOTAL;

    pub const OFFSET_USER_ID: usize =
        Self::OFFSET_CLIENT_ID + Self::WIRE_SIZE_CLIENT_ID;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_key_offsets_match_inline_layout() {
        // SchemaKey fields should be accessible through SchemaKey's own offsets
        // when added to OFFSET_SCHEMA_KEY (which is 0)
        assert_eq!(MetablockSchemaRegistration::OFFSET_SCHEMA_KEY, 0);
        assert_eq!(MetablockSchemaRegistration::OFFSET_CLIENT_ID, SchemaKey::WIRE_SIZE_TOTAL);
        assert_eq!(MetablockSchemaRegistration::OFFSET_USER_ID, SchemaKey::WIRE_SIZE_TOTAL + 16);
    }

    #[test]
    fn round_trip_bincode() {
        let key = SchemaKey::new(100, 200, 10, 20);
        let meta = MetablockSchemaRegistration {
            schema_key: key.clone(),
            client_id: 0xDEAD,
            user_id: Some(0xBEEF),
        };

        let config = bincode::config::standard().with_fixed_int_encoding();
        let encoded = bincode::encode_to_vec(&meta, config).unwrap();
        let (decoded, _): (MetablockSchemaRegistration, _) =
            bincode::decode_from_slice(&encoded, config).unwrap();

        assert_eq!(decoded.schema_key, key);
        assert_eq!(decoded.client_id, 0xDEAD);
        assert_eq!(decoded.user_id, Some(0xBEEF));
    }

    #[test]
    fn round_trip_bincode_user_id_none() {
        let key = SchemaKey::new(1, 2, 3, 4);
        let meta = MetablockSchemaRegistration {
            schema_key: key.clone(),
            client_id: 42,
            user_id: None,
        };

        let config = bincode::config::standard().with_fixed_int_encoding();
        let encoded = bincode::encode_to_vec(&meta, config).unwrap();
        let (decoded, _): (MetablockSchemaRegistration, _) =
            bincode::decode_from_slice(&encoded, config).unwrap();

        assert_eq!(decoded.schema_key, key);
        assert_eq!(decoded.client_id, 42);
        assert_eq!(decoded.user_id, None);
    }
}
