use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::datablocks::{datablock_aggregate_event_batch::DatablockAggregateEventBatch, datablock_snapshot_aggregate::DatablockSnapshotAggregate, datablock_snapshot_aggregate_type::DatablockSnapshotAggregateType, datablock_snapshot_org::DatablockSnapshotOrg};

/// Different kinds of WAL datablocks, snapshots and event batch items
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub enum DatablockKind {
    EventBatchItem(DatablockAggregateEventBatch),
    SnapshotOrg(DatablockSnapshotOrg),
    SnapshotAggregateType(DatablockSnapshotAggregateType),
    SnapshotAggregate(DatablockSnapshotAggregate),
}