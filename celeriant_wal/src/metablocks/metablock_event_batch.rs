use crate::aggregate_key::AggregateKey;
use crate::constants::BLOOM_BYTES;
use crate::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

/// Per-aggregate metadata for each event batch, stored in metablocks
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct MetablockEventBatch {
    pub aggregate_key: AggregateKey,
    /// Server-assigned ID for this batch within the aggregate
    pub aggregate_version: u64,
    
    /// Since its small and speeds up exists checks a lot, 
    /// cache the min aggregate version on each batch write
    pub trimmed_below_version: u64,

    pub min_client_seq: u64,
    pub max_client_seq: u64,

    pub min_event_timestamp: u64,
    pub max_event_timestamp: u64,

    pub min_event_seq: u64,
    pub max_event_seq: u64,

    pub client_id: u128,
    pub user_id: Option<u128>,

    /// Event types data - either bloom filter bytes or up to 4 event type u64s
    pub event_types_data: EventTypesKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub enum EventTypesKind {
    /// Bloom filter bytes (when more than 4 unique event types)
    Bloom([u64; BLOOM_BYTES / 8]),
    /// Direct event type array (when 4 or fewer unique event types)
    Direct([u64; BLOOM_BYTES / 8]),
}

impl MetablockEventBatch {
    // Wire format layout (bincode fixed-int encoding)
    // Update these if field order or types change!

    // AggregateKey contains 3 x u64 fields (org_id, aggregate_type_id, aggregate_id)
    const WIRE_SIZE_AGGREGATE_KEY: usize = AggregateKey::WIRE_SIZE_TOTAL;
    const WIRE_SIZE_AGGREGATE_VERSION: usize = 8;
    const WIRE_SIZE_MIN_AGGREGATE_VERSION: usize = 8;
    const WIRE_SIZE_MIN_CLIENT_EVENT_SEQ: usize = 8;
    const WIRE_SIZE_MAX_CLIENT_EVENT_SEQ: usize = 8;
    const WIRE_SIZE_MIN_EVENT_TIMESTAMP: usize = 8;
    const WIRE_SIZE_MAX_EVENT_TIMESTAMP: usize = 8;
    const WIRE_SIZE_MIN_EVENT_SEQ: usize = 8;
    const WIRE_SIZE_MAX_EVENT_SEQ: usize = 8;
    const WIRE_SIZE_CLIENT_ID: usize = 16;
    // Option<u128>: 1 byte discriminant + 16 bytes value
    const WIRE_SIZE_USER_ID: usize = 1 + 16;

    pub const OFFSET_AGGREGATE_KEY: usize = 0;

    pub const OFFSET_AGGREGATE_VERSION: usize = 
        Self::OFFSET_AGGREGATE_KEY + Self::WIRE_SIZE_AGGREGATE_KEY;

    pub const OFFSET_MIN_AGGREGATE_VERSION: usize = 
        Self::OFFSET_AGGREGATE_VERSION + Self::WIRE_SIZE_AGGREGATE_VERSION;    

    pub const OFFSET_MIN_CLIENT_EVENT_SEQ: usize = 
        Self::OFFSET_MIN_AGGREGATE_VERSION + Self::WIRE_SIZE_MIN_AGGREGATE_VERSION;

    pub const OFFSET_MAX_CLIENT_EVENT_SEQ: usize = 
        Self::OFFSET_MIN_CLIENT_EVENT_SEQ + Self::WIRE_SIZE_MIN_CLIENT_EVENT_SEQ;

    pub const OFFSET_MIN_EVENT_TIMESTAMP: usize = 
        Self::OFFSET_MAX_CLIENT_EVENT_SEQ + Self::WIRE_SIZE_MAX_CLIENT_EVENT_SEQ;

    pub const OFFSET_MAX_EVENT_TIMESTAMP: usize = 
        Self::OFFSET_MIN_EVENT_TIMESTAMP + Self::WIRE_SIZE_MIN_EVENT_TIMESTAMP;

    pub const OFFSET_MIN_EVENT_SEQ: usize = 
        Self::OFFSET_MAX_EVENT_TIMESTAMP + Self::WIRE_SIZE_MAX_EVENT_TIMESTAMP;

    pub const OFFSET_MAX_EVENT_SEQ: usize = 
        Self::OFFSET_MIN_EVENT_SEQ + Self::WIRE_SIZE_MIN_EVENT_SEQ;

    pub const OFFSET_CLIENT_ID: usize = 
        Self::OFFSET_MAX_EVENT_SEQ + Self::WIRE_SIZE_MAX_EVENT_SEQ;

    pub const OFFSET_USER_ID: usize = 
        Self::OFFSET_CLIENT_ID + Self::WIRE_SIZE_CLIENT_ID;

    pub const OFFSET_EVENT_TYPES_DATA: usize = 
        Self::OFFSET_USER_ID + Self::WIRE_SIZE_USER_ID;

}

impl MetablockEventBatch {
    /// Create metadata from an EventBatchItem
    pub fn from_batch_item(
        client_id: u128,
        user_id: Option<u128>,
        aggregate_key: AggregateKey,
        trimmed_below_version: u64,
        event_batch_item: &DatablockAggregateEventBatch,
        event_types_data: EventTypesKind,
    ) -> Self {
        // Calculate min/max values in a single pass over the events
        let (
            min_client_seq,
            max_client_seq,
            min_event_timestamp,
            max_event_timestamp,
            min_event_seq,
            max_event_seq,
        ) = event_batch_item.events.iter().fold(
            (u64::MAX, 0, u64::MAX, 0, u64::MAX, 0),
            |(min_idx, max_idx, min_time, max_time, min_edx, max_edx), event| {
                (
                    min_idx.min(event.client_seq),
                    max_idx.max(event.client_seq),
                    min_time.min(event.event_timestamp),
                    max_time.max(event.event_timestamp),
                    min_edx.min(event.event_seq),
                    max_edx.max(event.event_seq),
                )
            },
        );

        // Handle the case where events might be empty
        let min_client_seq = if min_client_seq == u64::MAX {
            0
        } else {
            min_client_seq
        };
        let min_event_timestamp = if min_event_timestamp == u64::MAX {
            0
        } else {
            min_event_timestamp
        };
        let min_event_seq = if min_event_seq == u64::MAX {
            0
        } else {
            min_event_seq
        };

        Self {
            aggregate_key,
            event_types_data,
            aggregate_version: event_batch_item.aggregate_version,
            trimmed_below_version,
            client_id,
            user_id,
            min_client_seq,
            max_client_seq,
            min_event_timestamp,
            max_event_timestamp,
            min_event_seq,
            max_event_seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

    fn mk_event(t: u64, eidx: u64, cidx: u64, ts: u64) -> DatablockAggregateEvent {
        DatablockAggregateEvent {
            event_type_major: t,
            event_seq: eidx,
            client_seq: cidx,
            event_timestamp: ts,
            ..Default::default()
        }
    }

    #[test]
    fn from_batch_item_computes_min_max_fields_and_copies_headers() {
        let events = vec![
            mk_event(2, 100, 10, 1_000),
            mk_event(3, 150, 15, 1_500),
            mk_event(4, 200, 20, 2_000),
        ];

        let batch = DatablockAggregateEventBatch {
            aggregate_version: 42,
            events,
        };

        let aggregate_key = AggregateKey::new(3, 4, 5);

        let meta = MetablockEventBatch::from_batch_item(
            0xA,
            None,
            aggregate_key,
            1,
            &batch,
            EventTypesKind::Direct([2, 4, 0, 0]),
        );

        assert_eq!(meta.aggregate_key.org_id, 3);
        assert_eq!(meta.aggregate_key.aggregate_type_id, 4);
        assert_eq!(meta.aggregate_key.aggregate_id, 5);
        assert_eq!(meta.aggregate_version, 42);
        assert_eq!(meta.trimmed_below_version, 1);
        assert_eq!(meta.client_id, 0xA);
        assert_eq!(meta.user_id, None);

        // Min/max from the 3 events
        assert_eq!(meta.min_client_seq, 10);
        assert_eq!(meta.max_client_seq, 20);
        assert_eq!(meta.min_event_timestamp, 1_000);
        assert_eq!(meta.max_event_timestamp, 2_000);
        assert_eq!(meta.min_event_seq, 100);
        assert_eq!(meta.max_event_seq, 200);
    }

    #[test]
    fn from_batch_item_handles_empty_events() {
        let batch = DatablockAggregateEventBatch {
            aggregate_version: 7,
            events: vec![],
        };

        let aggregate_key = AggregateKey::new(3, 4, 5);

        let meta = MetablockEventBatch::from_batch_item(
            0x1,
            None,
            aggregate_key,
            1,
            &batch,
            EventTypesKind::Direct([0, 0, 0, 0]),
        );

        assert_eq!(meta.min_client_seq, 0);
        assert_eq!(meta.min_event_timestamp, 0);
        assert_eq!(meta.min_event_seq, 0);
        assert_eq!(meta.max_client_seq, 0);
        assert_eq!(meta.max_event_timestamp, 0);
        assert_eq!(meta.max_event_seq, 0);
    }
}
