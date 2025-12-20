use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::aggregate_type_key::AggregateTypeKey;

/// TODO: Snapshot of an aggregate type definition at a point in time
/// Could include things like schema definitions, settings, list of aggregates
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct SnapshotAggregateType {
    pub aggregate_type_key: AggregateTypeKey,
    pub has_schemas: bool,
}

impl SnapshotAggregateType {
    pub fn new(
        aggregate_type_key: AggregateTypeKey,
        has_schemas: bool,
    ) -> Self {
        Self { aggregate_type_key, has_schemas }
    }
}