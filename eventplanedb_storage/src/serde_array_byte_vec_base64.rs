use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<S>(value: &Option<Vec<Option<Vec<u8>>>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(arrays) = value {
        // Only serialize non-empty arrays
        if arrays.is_empty() {
            return Option::<Vec<Option<String>>>::None.serialize(serializer);
        }

        let encoded: Vec<Option<String>> = arrays
            .iter()
            .map(|opt_bytes| opt_bytes.as_ref().map(|bytes| general_purpose::STANDARD.encode(bytes)))
            .collect();
        encoded.serialize(serializer)
    } else {
        Option::<Vec<Option<String>>>::None.serialize(serializer)
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<Option<Vec<u8>>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let encoded: Option<Vec<Option<String>>> = Option::deserialize(deserializer)?;

    match encoded {
        Some(encoded_arrays) => {
            // Handle empty arrays
            if encoded_arrays.is_empty() {
                return Ok(Some(Vec::new()));
            }

            let decoded = encoded_arrays
                .into_iter()
                .map(|opt_str| {
                    opt_str
                        .map(|s| general_purpose::STANDARD.decode(&s).map_err(serde::de::Error::custom))
                        .transpose()
                })
                .collect::<Result<Vec<_>, D::Error>>()?;

            Ok(Some(decoded))
        }
        None => Ok(None),
    }
}
