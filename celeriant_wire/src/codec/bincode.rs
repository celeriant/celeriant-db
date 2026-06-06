use bincode::{Decode, Encode, error};

/// Hard ceiling on a single decode. Defense-in-depth against a length-prefixed
/// collection (`Vec`/`String`/`HashMap`) whose length prefix claims a huge
/// element count: bincode reserves capacity for the claimed length before
/// reading any element, so a tiny corrupt input can OOM or panic the decoder.
/// With a limit set, bincode checks the claimed allocation against it before
/// reserving and returns a decode error instead. Must stay above
/// internode_max_request_size (64 MiB default, the largest legitimate decode);
/// a real frame is bounded well under this by the wire-header size check.
const MAX_DECODE_BYTES: usize = 1024 * 1024 * 1024;

pub static CONFIG_FIXED: bincode::config::Configuration<bincode::config::LittleEndian, bincode::config::Fixint, bincode::config::Limit<MAX_DECODE_BYTES>> = bincode::config::standard()
    .with_fixed_int_encoding() // Force fixed-length integers
    .with_little_endian()
    .with_limit::<MAX_DECODE_BYTES>();

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
pub fn fixed_serialise_heap<T>(message: &T) -> Result<Vec<u8>, error::EncodeError>
where
    T: Encode,
{
    bincode::encode_to_vec(message, CONFIG_FIXED)
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
    fn heap_roundtrip() {
        let original = TestMessage {
            id: 42,
            name: "test".into(),
            values: vec![1, 2, 3],
        };

        let encoded = fixed_serialise_heap(&original).unwrap();
        let decoded: TestMessage = fixed_deserialise(&encoded).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn deserialise_ignores_trailing_bytes() {
        let original = TestMessage {
            id: 42,
            name: "test".into(),
            values: vec![1, 2, 3],
        };

        let encoded = fixed_serialise_heap(&original).unwrap();
        let msg_len = encoded.len();

        let mut buffer = [0xFFu8; 1024];
        buffer[..msg_len].copy_from_slice(&encoded);

        let decoded: TestMessage = fixed_deserialise(&buffer).unwrap();
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

    /// A length prefix claiming a huge element count must decode-error, not
    /// reserve capacity for it (capacity-overflow panic / OOM). The decode
    /// limit on `CONFIG_FIXED` enforces this before any allocation. Without it,
    /// these inputs panic the decoder on a few bytes off the wire or off disk.
    #[test]
    fn huge_length_prefix_errors_not_panics() {
        // id: u64 = 0, then a String whose length prefix is u64::MAX.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(fixed_deserialise::<TestMessage>(&buf).is_err());

        // Same for a Vec element count of u64::MAX (after a valid empty string).
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u64.to_le_bytes()); // id
        buf.extend_from_slice(&0u64.to_le_bytes()); // name len = 0
        buf.extend_from_slice(&u64::MAX.to_le_bytes()); // values len = u64::MAX
        assert!(fixed_deserialise::<TestMessage>(&buf).is_err());
    }
}
