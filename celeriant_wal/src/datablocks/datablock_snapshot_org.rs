use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

/// TODO: Periodic snapshotting of each aggregate into the WAL to avoid replaying the entire WAL on startup
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct DatablockSnapshotOrg {

}