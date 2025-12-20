use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::metablocks::{datablock_storage_kind::DatablockStorageKind, metablock_kind::MetablockKind};

/// Metablocks are fixed size 512 byte blocks. They read fast and allow
/// us to avoid pulling in large message payloads (stored in datablocks)
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct Metablock {
    /// WAL global index of this metablock
    pub wal_index: u64,
    /// Server timestamp when batch was processed
    pub server_timestamp: u64,
    /// Lease index at time of write
    pub lease_index: u64,
    /// ID of the node that wrote this batch
    pub node_id: u128,
    /// Type of datablock linked to this metablock, if any
    pub datablock: DatablockStorageKind,
    /// Different types of fixed 512 byte metablocks
    pub wal_metablock_type: MetablockKind,
}