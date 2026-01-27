use serde::{Serialize, de::DeserializeOwned};

#[inline]
pub fn serialise_stack<T>(message: &T, mut buffer: &mut [u8]) -> Result<(), rmp_serde::encode::Error>
where
    T: Serialize,
{
    rmp_serde::encode::write(&mut buffer, message)
}

#[inline]
pub fn deserialise<T>(buffer: &[u8]) -> Result<T, rmp_serde::decode::Error>
where
    T: DeserializeOwned,
{
    rmp_serde::from_slice(buffer)
}

#[inline]
pub fn serialise_heap<T>(message: &T) -> Result<Vec<u8>, rmp_serde::encode::Error>
where
    T: Serialize,
{
    rmp_serde::to_vec(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

        serialise_stack(&original, &mut buffer).unwrap();
        let decoded: TestMessage = deserialise(&buffer).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn variable_roundtrip() {
        let original = TestMessage {
            id: 42,
            name: "test".into(),
            values: vec![1, 2, 3],
        };

        let encoded = serialise_heap(&original).unwrap();
        let decoded: TestMessage = deserialise(&encoded).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn deserialise_ignores_trailing_bytes() {
        let original = TestMessage {
            id: 42,
            name: "test".into(),
            values: vec![1, 2, 3],
        };

        // Serialize to get exact size
        let encoded = serialise_heap(&original).unwrap();
        let msg_len = encoded.len();

        // Copy into over-allocated buffer with garbage trailing bytes
        let mut buffer = [0xFFu8; 1024];
        buffer[..msg_len].copy_from_slice(&encoded);

        // Deserialize from full buffer - msgpack finds message boundary
        let decoded: TestMessage = deserialise(&buffer).unwrap();
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

        assert!(serialise_stack(&original, &mut buffer).is_err());
    }
}
