use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<S>(value: &Option<u128>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(v) => {
            let bytes = v.to_le_bytes();
            let encoded = general_purpose::STANDARD.encode(&bytes);
            Some(encoded).serialize(serializer)
        }
        None => None::<String>.serialize(serializer),
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u128>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt_s: Option<String> = Option::deserialize(deserializer)?;
    match opt_s {
        Some(s) => {
            let bytes = general_purpose::STANDARD
                .decode(&s)
                .map_err(serde::de::Error::custom)?;
            if bytes.len() != 16 {
                return Err(serde::de::Error::custom("Invalid base64 length for u128"));
            }
            let mut array = [0u8; 16];
            array.copy_from_slice(&bytes);
            Ok(Some(u128::from_le_bytes(array)))
        }
        None => Ok(None),
    }
}
