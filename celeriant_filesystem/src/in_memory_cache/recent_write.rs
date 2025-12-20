use celeriant_wal::{datablocks::datablock::Datablock, metablocks::metablock::Metablock};

/// This is where the in-memory data deserialised from the wire finally
/// ends up. We can then provide copies of it to readers as required.
pub struct RecentWrite {
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
    /// Approximate size in bytes (datablock serialized size)
    pub size_bytes: u64,
}