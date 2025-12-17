use celeriant_wal::{datablocks::wal_datablock::WalDatablock, metablocks::wal_metablock::WalMetablock};

/// This is where the in-memory data deserialised from the wire finally
/// ends up. We can then provide copies of it to readers as required.
pub struct RecentWrite {
    pub metablock: WalMetablock,
    pub datablock: Option<WalDatablock>,
}