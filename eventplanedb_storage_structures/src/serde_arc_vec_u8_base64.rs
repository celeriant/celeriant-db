use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

pub fn serialize<S>(value: &Arc<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = general_purpose::STANDARD.encode(value.as_ref());
    encoded.serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let bytes = general_purpose::STANDARD
        .decode(&s)
        .map_err(serde::de::Error::custom)?;
    Ok(Arc::new(bytes))
}
