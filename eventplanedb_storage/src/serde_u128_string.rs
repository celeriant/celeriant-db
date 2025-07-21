use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    value.to_string().serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}