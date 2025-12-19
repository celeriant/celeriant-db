use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

/// Periodic snapshotting of each aggregate into the WAL to avoid replaying the entire WAL on startup
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct SnapshotOrg {

}

impl SnapshotOrg {
    pub fn new(
    ) -> Self {
        Self {
        }
    }
}