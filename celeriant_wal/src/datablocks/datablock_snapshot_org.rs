use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

/// TODO: Periodic snapshotting of each aggregate into the WAL to avoid replaying the entire WAL on startup
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct DatablockSnapshotOrg {

}