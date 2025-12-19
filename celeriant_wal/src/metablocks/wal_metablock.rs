use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::{datablocks::{snapshot_aggregate_type::SnapshotAggregateType, snapshot_org::SnapshotOrg}, metablocks::{event_batch_metadata::EventBatchMetadata, snapshot_aggregate::SnapshotAggregate}};

pub const CURRENT_VERSION: u32 = 1;

/// Metablocks are fixed size 512 byte blocks. They read fast and allow
/// us to avoid pulling in large message payloads (stored in datablocks)
/// We use bincode with fixed-length integers so can pull out data
/// like aggregate key without deserialising the entire block
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub enum WalMetablock {
    EventBatchMetadata(EventBatchMetadata),
    SnapshotOrg(SnapshotOrg),
    SnapshotAggregatSnapshotAggregateType(SnapshotAggregateType),
    SnapshotAggregate(SnapshotAggregate),
}