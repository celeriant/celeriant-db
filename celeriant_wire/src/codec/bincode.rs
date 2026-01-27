use bincode::{Decode, Encode, error};

pub static CONFIG_FIXED: bincode::config::Configuration<bincode::config::LittleEndian, bincode::config::Fixint> = bincode::config::standard()
    .with_fixed_int_encoding() // Force fixed-length integers
    .with_little_endian();

pub static CONFIG_VARIABLE: bincode::config::Configuration<bincode::config::LittleEndian, bincode::config::Varint> =
    bincode::config::standard().with_variable_int_encoding().with_little_endian();

#[inline]
pub fn fixed_serialise_stack<T>(message: &T, buffer: &mut [u8]) -> Result<usize, error::EncodeError>
where
    T: Encode,
{
    bincode::encode_into_slice(message, buffer, CONFIG_FIXED)
}

#[inline]
pub fn fixed_deserialise<T>(buffer: &[u8]) -> Result<T, error::DecodeError>
where
    T: Decode<()>,
{
    let (result, _len) = bincode::decode_from_slice(buffer, CONFIG_FIXED)?;
    Ok(result)
}

#[inline]
pub fn variable_serialise_heap<T>(message: &T) -> Result<Vec<u8>, error::EncodeError>
where
    T: Encode,
{
    bincode::encode_to_vec(message, CONFIG_VARIABLE)
}

#[inline]
pub fn variable_deserialise<T>(data: &[u8]) -> Result<T, error::DecodeError>
where
    T: Decode<()>,
{
    let (result, _len) = bincode::decode_from_slice(data, CONFIG_VARIABLE)?;
    Ok(result)
}

#[inline]
pub fn variable_serialise_stack<T>(message: &T, buffer: &mut [u8]) -> Result<usize, error::EncodeError>
where
    T: Encode,
{
    bincode::encode_into_slice(message, buffer, CONFIG_VARIABLE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{Decode, Encode};

    #[derive(Debug, Clone, PartialEq, Encode, Decode)]
    struct TestMessage {
        id: u64,
        name: String,
        values: Vec<i32>,
    }

    #[test]
    fn fixed_roundtrip() {
        let original = TestMessage {
            id: 42,
            name: "test".into(),
            values: vec![1, 2, 3],
        };
        let mut buffer = [0u8; 64];

        fixed_serialise_stack(&original, &mut buffer).unwrap();
        let decoded: TestMessage = fixed_deserialise(&buffer).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn variable_roundtrip() {
        let original = TestMessage {
            id: 42,
            name: "test".into(),
            values: vec![1, 2, 3],
        };

        let encoded = variable_serialise_heap(&original).unwrap();
        let decoded: TestMessage = variable_deserialise(&encoded).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn deserialise_ignores_trailing_bytes() {
        let original = TestMessage {
            id: 42,
            name: "test".into(),
            values: vec![1, 2, 3],
        };

        let encoded = variable_serialise_heap(&original).unwrap();
        let msg_len = encoded.len();

        // Copy into over-allocated buffer with garbage trailing bytes
        let mut buffer = [0xFFu8; 1024];
        buffer[..msg_len].copy_from_slice(&encoded);

        // Deserialize from full buffer - bincode finds message boundary
        let decoded: TestMessage = variable_deserialise(&buffer).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn fixed_buffer_too_small() {
        let original = TestMessage {
            id: 42,
            name: "test".into(),
            values: vec![1, 2, 3],
        };
        let mut buffer = [0u8; 4];

        assert!(fixed_serialise_stack(&original, &mut buffer).is_err());
    }
}
