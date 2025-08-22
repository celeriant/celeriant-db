use bincode::{Decode, Encode};

use crate::structures::{compression_type::CompressionType, event_batch_item::EventBatchItem};

/// Metadata written to the tail of each event batch for efficient reading and validation
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct EventBatchMetadata {
    /// Size of the uncompressed event batch data in bytes
    pub uncompressed_size: u64,
    /// Event types data - either bloom filter bytes or up to 4 event type u64s
    pub event_types_data: EventTypesData,
    /// Server-assigned ID for this batch
    pub event_batch_index: u64,
    /// Client ID that created this batch (u128 to match EventBatchItem)
    pub client_id: u128,
    /// Optional user ID
    pub user_id: u128,
    /// Server timestamp when batch was processed
    pub server_timestamp: u64,
    /// Length of the compressed event batch data
    pub compressed_size: u64,
    /// Compression algorithm used
    pub compression_type: u8,
    /// CRC32 checksum of the compressed event data
    pub events_crc: u32,
    
    pub min_client_event_index: u64,
    pub max_client_event_index: u64,
    pub min_event_timestamp: u64,
    pub max_event_timestamp: u64,
    pub min_event_index: u64,
    pub max_event_index: u64,
}

impl Default for EventBatchMetadata {
    fn default() -> Self {
        Self {
            uncompressed_size: 0,
            event_types_data: EventTypesData::Direct([0; 4]),
            event_batch_index: 0,
            client_id: 0,
            user_id: 0,
            server_timestamp: 0,
            compressed_size: 0,
            compression_type: 0,
            events_crc: 0,
            min_client_event_index: 0,
            max_client_event_index: 0,
            min_event_timestamp: 0,
            max_event_timestamp: 0,
            min_event_index: 0,
            max_event_index: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum EventTypesData {
    /// Bloom filter bytes (when more than 4 unique event types)
    Bloom([u8; crate::structures::constants::BLOOM_BYTES]),
    /// Direct event type array (when 4 or fewer unique event types)
    Direct([u64; 4]),
}

impl EventBatchMetadata {
    /// Create metadata from an EventBatchItem
    pub fn from_batch_item(
        event_batch_item: &EventBatchItem,
        uncompressed_size: u64,
        compressed_size: u64,
        compression_type: CompressionType,
        event_types_data: EventTypesData,
        events_crc: u32,
    ) -> Self {
        // Calculate min/max values in a single pass over the events
        let (min_client_event_index, max_client_event_index, min_event_timestamp, max_event_timestamp, min_event_index, max_event_index) = 
            event_batch_item.events.iter().fold(
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
                }
            );

        // Handle the case where events might be empty
        let min_client_event_index = if min_client_event_index == u64::MAX { 0 } else { min_client_event_index };
        let min_event_timestamp = if min_event_timestamp == u64::MAX { 0 } else { min_event_timestamp };
        let min_event_index = if min_event_index == u64::MAX { 0 } else { min_event_index };

        Self {
            uncompressed_size,
            event_types_data,
            event_batch_index: event_batch_item.event_batch_index,
            client_id: event_batch_item.client_id,
            user_id: event_batch_item.user_id.unwrap_or_default(),
            server_timestamp: event_batch_item.server_timestamp,
            compressed_size,
            compression_type: compression_type.to_tuple().0,
            events_crc,
            min_client_event_index,
            max_client_event_index,
            min_event_timestamp,
            max_event_timestamp,
            min_event_index,
            max_event_index,
        }
    }
}
