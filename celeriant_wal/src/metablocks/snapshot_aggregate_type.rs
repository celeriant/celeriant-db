use bincode::{Decode, Encode};

#[derive(Debug, Clone, Encode, Decode)]
pub struct SnapshotAggregateType {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub has_schemas: bool,
}

impl SnapshotAggregateType {
    pub fn new(
        org_id: u128,
        aggregate_type_id: u128,
        has_schemas: bool,
    ) -> Self {
        Self { org_id, aggregate_type_id, has_schemas }
    }
}