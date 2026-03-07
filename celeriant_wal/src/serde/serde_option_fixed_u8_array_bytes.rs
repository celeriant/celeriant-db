use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize<S>(value: &Option<[u8; 12]>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(array) => serializer.serialize_bytes(array),
        None => serializer.serialize_none(),
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<[u8; 12]>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<serde_bytes::ByteBuf> = Deserialize::deserialize(deserializer)?;
    match opt {
        Some(buf) => {
            let bytes = buf.as_ref();
            if bytes.len() != 12 {
                return Err(serde::de::Error::custom(format!(
                    "Expected 12 bytes for IV, got {}",
                    bytes.len()
                )));
            }
            let mut array = [0u8; 12];
            array.copy_from_slice(bytes);
            Ok(Some(array))
        }
        None => Ok(None),
    }
}
