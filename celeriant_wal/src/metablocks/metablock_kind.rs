use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::{datablocks::{datablock_snapshot_aggregate_type::DatablockSnapshotAggregateType, datablock_snapshot_org::DatablockSnapshotOrg}, metablocks::{metablock_event_batch::MetablockEventBatch, metablock_snapshot_aggregate::MetablockSnapshotAggregate}};

/// Different kinds of WAL metablocks, snapshots and event batch metadata
/// All metablocks are fixed size 512 byte blocks
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub enum MetablockKind {
    EventBatchMetadata(MetablockEventBatch),
    SnapshotOrg(DatablockSnapshotOrg),
    SnapshotAggregateType(DatablockSnapshotAggregateType),
    SnapshotAggregate(MetablockSnapshotAggregate),
}