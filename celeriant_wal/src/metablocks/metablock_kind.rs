use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::metablocks::{
    metablock_event_batch::MetablockEventBatch, metablock_snapshot_aggregate::MetablockSnapshotAggregate,
    metablock_snapshot_aggregate_type::MetablockSnapshotAggregateType, metablock_snapshot_org::MetablockSnapshotOrg,
    metablock_soft_delete::MetablockSoftDelete, metablock_soft_trim::MetablockSoftTrim,
};

/// Different kinds of WAL metablocks, snapshots and event batch metadata
/// All metablocks are fixed size 512 byte blocks
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
#[repr(u32)]
pub enum MetablockKind {
    EventBatchMetadata(MetablockEventBatch) = 0,
    SnapshotOrg(MetablockSnapshotOrg) = 1,
    SnapshotAggregateType(MetablockSnapshotAggregateType) = 2,
    SnapshotAggregate(MetablockSnapshotAggregate) = 3,
    SoftDelete(MetablockSoftDelete) = 4,
    SoftTrim(MetablockSoftTrim) = 5,
}
