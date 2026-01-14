use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

/// TODO: Snapshot of the organisation state at a point in time
/// Could include things like user lists, permissions, settings, list of aggregates etc.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct MetablockSnapshotOrg {
    pub org_id: u128,
}

impl MetablockSnapshotOrg {
    pub fn new(
        org_id: u128,
    ) -> Self {
        Self { org_id }
    }
}