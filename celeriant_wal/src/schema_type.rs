use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub enum SchemaType {
    Json = 0,
    Avro = 1,
    Protobuf = 2,
}

impl TryFrom<u8> for SchemaType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(SchemaType::Json),
            1 => Ok(SchemaType::Avro),
            2 => Ok(SchemaType::Protobuf),
            _ => Err(value),
        }
    }
}

impl From<SchemaType> for u8 {
    fn from(schema_type: SchemaType) -> Self {
        schema_type as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u8_round_trip() {
        for (variant, byte) in [(SchemaType::Json, 0), (SchemaType::Avro, 1), (SchemaType::Protobuf, 2)] {
            assert_eq!(u8::from(variant), byte);
            assert_eq!(SchemaType::try_from(byte), Ok(variant));
        }
    }

    #[test]
    fn unknown_u8_returns_error() {
        for val in [3, 100, 255] {
            assert_eq!(SchemaType::try_from(val), Err(val));
        }
    }
}
