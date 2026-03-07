use serde::{Deserialize, Deserializer, Serializer};
use std::sync::Arc;

pub fn serialize<S>(value: &Arc<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_bytes(value.as_ref())
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes: serde_bytes::ByteBuf = Deserialize::deserialize(deserializer)?;
    Ok(Arc::new(bytes.into_vec()))
}
