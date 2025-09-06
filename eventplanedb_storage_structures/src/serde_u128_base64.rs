use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bytes = value.to_le_bytes();
    let encoded = general_purpose::STANDARD.encode(bytes);
    encoded.serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let bytes = general_purpose::STANDARD
        .decode(&s)
        .map_err(serde::de::Error::custom)?;
    if bytes.len() != 16 {
        return Err(serde::de::Error::custom("Invalid base64 length for u128"));
    }
    let mut array = [0u8; 16];
    array.copy_from_slice(&bytes);
    Ok(u128::from_le_bytes(array))
}
