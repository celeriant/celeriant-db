use std::u128;

use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::{aggregate_key::AggregateKey, constants::MINIBATCH_SIZE_BYTES, metablocks::{datablock_inline_data::DatablockInlineData, datablock_storage_kind::DatablockStorageKind, metablock_event_batch::{EventTypesKind, MetablockEventBatch}, metablock_kind::MetablockKind}};

/// Metablocks are fixed size 512 byte blocks. They read fast and allow
/// us to avoid pulling in large message payloads (stored in datablocks)
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct Metablock {
    /// WAL global index of this metablock
    pub wal_index: u64,
    /// Server timestamp when batch was processed
    pub server_timestamp: u64,
    /// Lease index at time of write
    pub lease_index: u64,
    /// ID of the node that wrote this batch
    pub node_id: u128,
    /// Size of the uncompressed event batch data in bytes
    pub uncompressed_size: u64,
    /// Length of the compressed event batch data
    pub compressed_size: u64,
    /// Different types of fixed 512 byte metablocks
    pub wal_metablock_type: MetablockKind,
    /// Type of datablock linked to this metablock, if any
    pub datablock: DatablockStorageKind,
}

impl Metablock {
    // Wire format layout (bincode fixed-int encoding)
    // Update these if field order or types change!

    const WIRE_SIZE_WAL_INDEX: usize = 8;
    const WIRE_SIZE_SERVER_TIMESTAMP: usize = 8;
    const WIRE_SIZE_LEASE_INDEX: usize = 8;
    const WIRE_SIZE_NODE_ID: usize = 16;
    const WIRE_SIZE_UNCOMPRESSED_SIZE: usize = 8;
    const WIRE_SIZE_COMPRESSED_SIZE: usize = 8;

    pub const OFFSET_WAL_INDEX: usize = 0;

    pub const OFFSET_SERVER_TIMESTAMP: usize = 
        Self::OFFSET_WAL_INDEX + Self::WIRE_SIZE_WAL_INDEX;

    pub const OFFSET_LEASE_INDEX: usize = 
        Self::OFFSET_SERVER_TIMESTAMP + Self::WIRE_SIZE_SERVER_TIMESTAMP;

    pub const OFFSET_NODE_ID: usize = 
        Self::OFFSET_LEASE_INDEX + Self::WIRE_SIZE_LEASE_INDEX;

    pub const OFFSET_UNCOMPRESSED_SIZE: usize = 
        Self::OFFSET_NODE_ID + Self::WIRE_SIZE_NODE_ID;

    pub const OFFSET_COMPRESSED_SIZE: usize = 
        Self::OFFSET_UNCOMPRESSED_SIZE + Self::WIRE_SIZE_UNCOMPRESSED_SIZE;

    pub const OFFSET_WAL_METABLOCK_TYPE: usize = 
        Self::OFFSET_COMPRESSED_SIZE + Self::WIRE_SIZE_COMPRESSED_SIZE;

    pub fn default_inline_event_batch_metadata(aggregate_key: AggregateKey) -> Self {
        Self { 
            wal_index: 0, 
            server_timestamp: 0, 
            lease_index: 0, 
            node_id: 0, 
            compressed_size: 0,
            uncompressed_size: 0,
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch { 
                aggregate_key, 
                event_batch_index: 0, 
                min_event_batch_index: 0,
                min_client_event_index: 0, 
                max_client_event_index: 0, 
                min_event_timestamp: 0, 
                max_event_timestamp: 0, 
                min_event_index: 0, 
                max_event_index: 0, 
                client_id: 0, 
                user_id: None, 
                event_types_data: EventTypesKind::Direct([0u64; 4]) 
            }), 
            datablock: DatablockStorageKind::Inline(DatablockInlineData {
                minibatch: [0u8; MINIBATCH_SIZE_BYTES]
            })
        }
    }
}
