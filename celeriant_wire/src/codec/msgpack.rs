use serde::{Serialize, de::DeserializeOwned};
use std::io::Cursor;

#[inline]
pub fn serialise_stack<T>(message: &T, buffer: &mut [u8]) -> Result<usize, rmp_serde::encode::Error>
where
    T: Serialize,
{
    let mut cursor = Cursor::new(buffer);
    rmp_serde::encode::write(&mut cursor, message)?;
    Ok(cursor.position() as usize)
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

    #[test]
    fn datablock_aggregate_event_with_iv() {
        use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
        use std::sync::Arc;

        let event = DatablockAggregateEvent {
            client_seq: 1,
            event_seq: 10,
            event_id: Some(0xDEADBEEF_CAFEBABE),
            event_timestamp: 1234567890,
            event_type_major: 42,
            event_type_minor: 1,
            event_value: Arc::new(vec![1, 2, 3, 4, 5]),
            iv: Some([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
        };

        let encoded = serialise_heap(&event).unwrap();
        let decoded: DatablockAggregateEvent = deserialise(&encoded).unwrap();

        assert_eq!(event.client_seq, decoded.client_seq);
        assert_eq!(event.event_seq, decoded.event_seq);
        assert_eq!(event.event_id, decoded.event_id);
        assert_eq!(event.iv, decoded.iv);
    }

    #[test]
    fn datablock_aggregate_event_without_iv() {
        use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
        use std::sync::Arc;

        let event = DatablockAggregateEvent {
            client_seq: 1,
            event_seq: 10,
            event_id: Some(0xDEADBEEF_CAFEBABE),
            event_timestamp: 1234567890,
            event_type_major: 42,
            event_type_minor: 1,
            event_value: Arc::new(vec![1, 2, 3, 4, 5]),
            iv: None,
        };

        let encoded = serialise_heap(&event).unwrap();
        let decoded: DatablockAggregateEvent = deserialise(&encoded).unwrap();

        assert_eq!(event.client_seq, decoded.client_seq);
        assert_eq!(event.iv, decoded.iv);
    }
}
