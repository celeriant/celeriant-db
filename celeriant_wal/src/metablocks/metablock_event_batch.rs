use crate::{aggregate_key::AggregateKey, constants::WIRE_SIZE_ENUM_DISCRIMINANT};
use crate::constants::BLOOM_BYTES;
use crate::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

/// Per-aggregate metadata for each event batch, stored in metablocks
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct MetablockEventBatch {
    pub aggregate_key: AggregateKey,
    /// Server-assigned ID for this batch within the aggregate
    pub event_batch_index: u64,

    pub min_client_event_index: u64,
    pub max_client_event_index: u64,

    pub min_event_timestamp: u64,
    pub max_event_timestamp: u64,

    pub min_event_index: u64,
    pub max_event_index: u64,

    pub client_id: u128,
    pub user_id: Option<u128>,

    /// Event types data - either bloom filter bytes or up to 4 event type u64s
    pub event_types_data: EventTypesKind,
}

#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
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
    const WIRE_SIZE_EVENT_BATCH_INDEX: usize = 8;
    const WIRE_SIZE_MIN_CLIENT_EVENT_INDEX: usize = 8;
    const WIRE_SIZE_MAX_CLIENT_EVENT_INDEX: usize = 8;
    const WIRE_SIZE_MIN_EVENT_TIMESTAMP: usize = 8;
    const WIRE_SIZE_MAX_EVENT_TIMESTAMP: usize = 8;
    const WIRE_SIZE_MIN_EVENT_INDEX: usize = 8;
    const WIRE_SIZE_MAX_EVENT_INDEX: usize = 8;
    const WIRE_SIZE_CLIENT_ID: usize = 16;
    // Option<u128>: 1 byte discriminant + 16 bytes value
    const WIRE_SIZE_USER_ID: usize = 1 + 16;
    // EventTypesKind: 4 byte discriminant + [u64; BLOOM_BYTES / 8]
    const WIRE_SIZE_EVENT_TYPES_DATA: usize = WIRE_SIZE_ENUM_DISCRIMINANT + BLOOM_BYTES;

    pub const OFFSET_AGGREGATE_KEY: usize = 0;

    pub const OFFSET_EVENT_BATCH_INDEX: usize = 
        Self::OFFSET_AGGREGATE_KEY + Self::WIRE_SIZE_AGGREGATE_KEY;

    pub const OFFSET_MIN_CLIENT_EVENT_INDEX: usize = 
        Self::OFFSET_EVENT_BATCH_INDEX + Self::WIRE_SIZE_EVENT_BATCH_INDEX;

    pub const OFFSET_MAX_CLIENT_EVENT_INDEX: usize = 
        Self::OFFSET_MIN_CLIENT_EVENT_INDEX + Self::WIRE_SIZE_MIN_CLIENT_EVENT_INDEX;

    pub const OFFSET_MIN_EVENT_TIMESTAMP: usize = 
        Self::OFFSET_MAX_CLIENT_EVENT_INDEX + Self::WIRE_SIZE_MAX_CLIENT_EVENT_INDEX;

    pub const OFFSET_MAX_EVENT_TIMESTAMP: usize = 
        Self::OFFSET_MIN_EVENT_TIMESTAMP + Self::WIRE_SIZE_MIN_EVENT_TIMESTAMP;

    pub const OFFSET_MIN_EVENT_INDEX: usize = 
        Self::OFFSET_MAX_EVENT_TIMESTAMP + Self::WIRE_SIZE_MAX_EVENT_TIMESTAMP;

    pub const OFFSET_MAX_EVENT_INDEX: usize = 
        Self::OFFSET_MIN_EVENT_INDEX + Self::WIRE_SIZE_MIN_EVENT_INDEX;

    pub const OFFSET_CLIENT_ID: usize = 
        Self::OFFSET_MAX_EVENT_INDEX + Self::WIRE_SIZE_MAX_EVENT_INDEX;

    pub const OFFSET_USER_ID: usize = 
        Self::OFFSET_CLIENT_ID + Self::WIRE_SIZE_CLIENT_ID;

    pub const OFFSET_EVENT_TYPES_DATA: usize = 
        Self::OFFSET_USER_ID + Self::WIRE_SIZE_USER_ID;

    /// Total wire size of MetablockEventBatch
    pub const WIRE_SIZE_TOTAL: usize = 
        Self::OFFSET_EVENT_TYPES_DATA + Self::WIRE_SIZE_EVENT_TYPES_DATA;
}

impl MetablockEventBatch {
    /// Create metadata from an EventBatchItem
    pub fn from_batch_item(
        client_id: u128,
        user_id: Option<u128>,
        aggregate_key: AggregateKey,
        event_batch_item: &DatablockAggregateEventBatch,
        event_types_data: EventTypesKind,
    ) -> Self {
        // Calculate min/max values in a single pass over the events
        let (
            min_client_event_index,
            max_client_event_index,
            min_event_timestamp,
            max_event_timestamp,
            min_event_index,
            max_event_index,
        ) = event_batch_item.events.iter().fold(
            (u64::MAX, 0, u64::MAX, 0, u64::MAX, 0),
            |(min_idx, max_idx, min_time, max_time, min_edx, max_edx), event| {
                (
                    min_idx.min(event.client_event_index),
                    max_idx.max(event.client_event_index),
                    min_time.min(event.event_timestamp),
                    max_time.max(event.event_timestamp),
                    min_edx.min(event.event_index),
                    max_edx.max(event.event_index),
                )
            },
        );

        // Handle the case where events might be empty
        let min_client_event_index = if min_client_event_index == u64::MAX {
            0
        } else {
            min_client_event_index
        };
        let min_event_timestamp = if min_event_timestamp == u64::MAX {
            0
        } else {
            min_event_timestamp
        };
        let min_event_index = if min_event_index == u64::MAX {
            0
        } else {
            min_event_index
        };

        Self {
            aggregate_key,
            event_types_data,
            event_batch_index: event_batch_item.event_batch_index,
            client_id,
            user_id,
            min_client_event_index,
            max_client_event_index,
            min_event_timestamp,
            max_event_timestamp,
            min_event_index,
            max_event_index,
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
            event_index: eidx,
            client_event_index: cidx,
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
            event_batch_index: 42,
            events,
        };

        let aggregate_key = AggregateKey::new(3, 4, 5);

        let meta = MetablockEventBatch::from_batch_item(
            0xA,
            None,
            aggregate_key,
            &batch,
            EventTypesKind::Direct([2, 4, 0, 0]),
        );

        assert_eq!(meta.aggregate_key.org_id, 3);
        assert_eq!(meta.aggregate_key.aggregate_type_id, 4);
        assert_eq!(meta.aggregate_key.aggregate_id, 5);
        assert_eq!(meta.event_batch_index, 42);
        assert_eq!(meta.client_id, 0xA);
        assert_eq!(meta.user_id, None);

        // Min/max from the 3 events
        assert_eq!(meta.min_client_event_index, 10);
        assert_eq!(meta.max_client_event_index, 20);
        assert_eq!(meta.min_event_timestamp, 1_000);
        assert_eq!(meta.max_event_timestamp, 2_000);
        assert_eq!(meta.min_event_index, 100);
        assert_eq!(meta.max_event_index, 200);
    }

    #[test]
    fn from_batch_item_handles_empty_events() {
        let batch = DatablockAggregateEventBatch {
            event_batch_index: 7,
            events: vec![],
        };

        let aggregate_key = AggregateKey::new(3, 4, 5);

        let meta = MetablockEventBatch::from_batch_item(
            0x1,
            None,
            aggregate_key,
            &batch,
            EventTypesKind::Direct([0, 0, 0, 0]),
        );

        assert_eq!(meta.min_client_event_index, 0);
        assert_eq!(meta.min_event_timestamp, 0);
        assert_eq!(meta.min_event_index, 0);
        assert_eq!(meta.max_client_event_index, 0);
        assert_eq!(meta.max_event_timestamp, 0);
        assert_eq!(meta.max_event_index, 0);
    }
}
