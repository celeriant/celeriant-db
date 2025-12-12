use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize<S>(value: &Option<[u8; 12]>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(array) => {
            let encoded = general_purpose::STANDARD.encode(array);
            serializer.serialize_some(&encoded)
        }
        None => serializer.serialize_none(),
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<[u8; 12]>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt_s: Option<String> = Option::deserialize(deserializer)?;
    match opt_s {
        Some(s) => {
            let bytes = general_purpose::STANDARD
                .decode(&s)
                .map_err(serde::de::Error::custom)?;

            if bytes.len() != 12 {
                return Err(serde::de::Error::custom(format!(
                    "Expected 12 bytes for IV, got {}",
                    bytes.len()
                )));
            }

            let mut array = [0u8; 12];
            array.copy_from_slice(&bytes);
            Ok(Some(array))
        }
        None => Ok(None),
    }
}
