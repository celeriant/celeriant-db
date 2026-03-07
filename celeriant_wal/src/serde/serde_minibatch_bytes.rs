use serde::{Deserialize, Deserializer, Serializer};

use crate::constants::MINIBATCH_SIZE_BYTES;

pub fn serialize<S>(value: &[u8; MINIBATCH_SIZE_BYTES], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_bytes(value)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; MINIBATCH_SIZE_BYTES], D::Error>
where
    D: Deserializer<'de>,
{
    let buf: serde_bytes::ByteBuf = Deserialize::deserialize(deserializer)?;
    let bytes = buf.as_ref();
    if bytes.len() != MINIBATCH_SIZE_BYTES {
        return Err(serde::de::Error::custom(format!(
            "Expected {} bytes, got {}",
            MINIBATCH_SIZE_BYTES,
            bytes.len()
        )));
    }
    let mut array = [0u8; MINIBATCH_SIZE_BYTES];
    array.copy_from_slice(bytes);
    Ok(array)
}
