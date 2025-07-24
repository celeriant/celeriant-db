use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<S>(value: &Option<Vec<Option<[u8; 12]>>>, serializer: S) -> Result<S::Ok, S::Error>
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
            .map(|opt_array| opt_array.as_ref().map(|array| general_purpose::STANDARD.encode(array)))
            .collect();
        encoded.serialize(serializer)
    } else {
        Option::<Vec<Option<String>>>::None.serialize(serializer)
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<Option<[u8; 12]>>>, D::Error>
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
                        .map(|s| {
                            let bytes = general_purpose::STANDARD.decode(&s).map_err(serde::de::Error::custom)?;

                            if bytes.len() != 12 {
                                return Err(serde::de::Error::custom(format!(
                                    "Invalid base64 length for [u8; 12]: got {} bytes",
                                    bytes.len()
                                )));
                            }

                            let mut array = [0u8; 12];
                            array.copy_from_slice(&bytes);
                            Ok(array)
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, D::Error>>()?;

            Ok(Some(decoded))
        }
        None => Ok(None),
    }
}
