use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use crate::aggregate_key::AggregateKey;
use crate::constants::MINIBATCH_SIZE_BYTES;
use crate::datablocks::event_batch_item::{CURRENT_VERSION, EventBatchItem};
use crate::metablocks::datablock_style::DatablockStyle;
use crate::{
    compression_type::CompressionType, constants::BLOOM_BYTES, 
};

/// Metadata written to the tail of each event batch for efficient reading and validation
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct EventBatchMetadata {
    pub aggregate_key: AggregateKey,
    pub datablock: DatablockStyle,
    /// Event types data - either bloom filter bytes or up to 4 event type u64s
    pub event_types_data: EventTypesData,
    /// Server-assigned ID for this batch
    pub event_batch_index: u64,
    /// Client ID that created this batch
    pub client_id: u128,
    /// Optional user ID
    pub user_id: Option<u128>,
    /// ID of the node that wrote this batch
    pub node_id: u128,
    /// Lease index at time of write
    pub lease_index: u64,
    /// Server timestamp when batch was processed
    pub server_timestamp: u64,

    pub min_client_event_index: u64,
    pub max_client_event_index: u64,
    pub min_event_timestamp: u64,
    pub max_event_timestamp: u64,
    pub min_event_index: u64,
    pub max_event_index: u64,

    /// Size of the uncompressed event batch data in bytes
    pub uncompressed_size: u64,
    /// Length of the compressed event batch data
    pub compressed_size: u64,
    /// Compression algorithm used
    pub compression_type: u8,
    /// Datablock version, 0 if no datablock
    pub version: u32,
}

impl Default for EventBatchMetadata {
    fn default() -> Self {
        Self {
            aggregate_key: AggregateKey::default(),
            datablock: DatablockStyle::Block { crc32c: 0, datablock_position: 0 },
            event_types_data: EventTypesData::Direct([0; 4]),
            event_batch_index: 0,
            client_id: 0,
            user_id: None,
            node_id: 0,
            lease_index: 0,
            server_timestamp: 0,
            min_client_event_index: 0,
            max_client_event_index: 0,
            min_event_timestamp: 0,
            max_event_timestamp: 0,
            min_event_index: 0,
            max_event_index: 0,
            uncompressed_size: 0,
            compressed_size: 0,
            compression_type: 0,
            version: 0,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub enum EventTypesData {
    /// Bloom filter bytes (when more than 4 unique event types)
    Bloom([u64; BLOOM_BYTES / 8]),
    /// Direct event type array (when 4 or fewer unique event types)
    Direct([u64; BLOOM_BYTES / 8]),
}

impl EventBatchMetadata {
    /// Create metadata from an EventBatchItem
    pub fn from_batch_item(
        aggregate_key: AggregateKey,
        event_batch_item: &EventBatchItem,
        datablock_position: u64,
        uncompressed_size: u64,
        compressed_size: u64,
        crc32c: u32,
        compression_type: CompressionType,
        event_types_data: EventTypesData,
        minibatch: Option<[u8; MINIBATCH_SIZE_BYTES]>,
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

        let datablock = match minibatch.is_some() {
            true => DatablockStyle::Inline { minibatch: minibatch.unwrap() },
            false => DatablockStyle::Block { crc32c, datablock_position },
        };

        Self {
            aggregate_key,
            datablock,
            event_types_data,
            event_batch_index: event_batch_item.event_batch_index,
            client_id: event_batch_item.client_id,
            user_id: event_batch_item.user_id,
            node_id: event_batch_item.node_id,
            lease_index: event_batch_item.lease_index,
            server_timestamp: event_batch_item.server_timestamp,
            min_client_event_index,
            max_client_event_index,
            min_event_timestamp,
            max_event_timestamp,
            min_event_index,
            max_event_index,
            version: CURRENT_VERSION, 
            uncompressed_size, 
            compressed_size, 
            compression_type: compression_type.to_tuple().0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compression_type::CompressionType, datablocks::event_item::EventItem
    };

    fn mk_event(
        t: u64,
        eidx: u64,
        cidx: u64,
        ts: u64,
    ) -> EventItem {
        EventItem {
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

        let batch = EventBatchItem {
            event_batch_index: 42,
            server_timestamp: 9_999,
            client_id: 0xA,
            node_id: 99,
            lease_index: 43,
            user_id: None,
            events,
        };

        let aggregate_key = AggregateKey::new(3, 4, 5);

        let meta = EventBatchMetadata::from_batch_item(
            aggregate_key,
            &batch,
            777,
            1234,                // uncompressed
            567,                 // compressed
            999,
            CompressionType::Snappy,
            EventTypesData::Direct([2, 4, 0, 0]),
            None,
        );

        assert_eq!(meta.aggregate_key.org_id, 3);
        assert_eq!(meta.aggregate_key.aggregate_type_id, 4);
        assert_eq!(meta.aggregate_key.aggregate_id, 5);
        assert_eq!(meta.event_batch_index, 42);
        assert_eq!(meta.server_timestamp, 9_999);
        assert_eq!(meta.client_id, 0xA);
        assert_eq!(meta.user_id, None);
        assert_eq!(meta.node_id, 99);
        assert_eq!(meta.lease_index, 43);
        assert_eq!(meta.version, CURRENT_VERSION);
        assert_eq!(meta.uncompressed_size, 1234);        
        assert_eq!(meta.compressed_size, 567);        
        assert_eq!(meta.compression_type, 2);
        
        let DatablockStyle::Block { 
            crc32c, datablock_position 
        } = meta.datablock else {
            panic!("Expected DatablockStyle::Block");
        };
        assert_eq!(crc32c, 999);
        assert_eq!(datablock_position, 777);

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
        let batch = EventBatchItem {
            event_batch_index: 7,
            server_timestamp: 77,
            client_id: 0x1,
            node_id: 99,
            lease_index: 43,
            user_id: None,
            events: vec![],
        };

        let aggregate_key = AggregateKey::new(3, 4, 5);

        let meta = EventBatchMetadata::from_batch_item(
            aggregate_key,
            &batch,
            666,
            10,
            5,
            999,
            CompressionType::None,
            EventTypesData::Direct([0, 0, 0, 0]),
            None,
        );

        assert_eq!(meta.min_client_event_index, 0);
        assert_eq!(meta.min_event_timestamp, 0);
        assert_eq!(meta.min_event_index, 0);
        assert_eq!(meta.max_client_event_index, 0);
        assert_eq!(meta.max_event_timestamp, 0);
        assert_eq!(meta.max_event_index, 0);
        assert_eq!(meta.node_id, 99);
        assert_eq!(meta.lease_index, 43);
    }
}