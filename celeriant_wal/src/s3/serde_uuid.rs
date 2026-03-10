use serde::{self, Deserialize, Deserializer, Serializer};
use uuid::Uuid;

pub fn serialize<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&Uuid::from_u128(*value).to_string())
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Uuid::parse_str(&s)
        .map(|u| u.as_u128())
        .map_err(serde::de::Error::custom)
}
