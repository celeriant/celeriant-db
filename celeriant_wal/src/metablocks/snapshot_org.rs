use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct SnapshotOrg {
    pub org_id: u128,
}

impl SnapshotOrg {
    pub fn new(
        org_id: u128,
    ) -> Self {
        Self { org_id }
    }
}