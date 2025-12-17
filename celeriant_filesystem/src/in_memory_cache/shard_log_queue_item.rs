use celeriant_wal::{datablocks::{wal_datablock::WalDatablock}, metablocks::wal_metablock::WalMetablock};

/// In-memory queue of data waiting to be written to disk + fsync'd
/// We include the structs here too as they go into the cache after fsync
pub struct ShardLogQueueItem {
    pub datablock_bytes: Option<Vec<u8>>,
    pub datablock: Option<WalDatablock>,
    pub metablock: WalMetablock,
}