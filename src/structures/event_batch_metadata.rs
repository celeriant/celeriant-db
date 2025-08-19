use bincode::{Decode, Encode};

use crate::structures::{compression_type::CompressionType, event_batch_item::EventBatchItem};

/// Metadata written to the tail of each event batch for efficient reading and validation
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct EventBatchMetadata {
    /// Size of the uncompressed event batch data in bytes
    pub uncompressed_size: u64,
    /// Event types data - either bloom filter bytes or up to 4 event type u64s
    pub event_types_data: EventTypesData,
    /// Last local index from the client for deduplication
    pub last_local_index: u64,
    /// Server-assigned ID for this batch
    pub server_id: u64,
    /// Client ID that created this batch (u128 to match EventBatchItem)
    pub client_id: u128,
    /// Optional user ID
    pub user_id: u128,
    /// Server timestamp when batch was processed
    pub server_time: u64,
    /// Length of the compressed event batch data
    pub compressed_size: u64,
    /// Compression algorithm used
    pub compression_type: u8,
    /// CRC32 checksum of the compressed event data
    pub events_crc: u32,
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
        Self {
            uncompressed_size,
            event_types_data,
            last_local_index: event_batch_item.events.last().map_or(0, |e| e.local_index),
            server_id: event_batch_item.server_id,
            client_id: event_batch_item.client_id,
            user_id: event_batch_item.user_id.unwrap_or_default(),
            server_time: event_batch_item.server_time,
            compressed_size,
            compression_type: compression_type.to_tuple().0,
            events_crc,
        }
    }
}
