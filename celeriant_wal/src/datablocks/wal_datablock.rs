use bincode::{Decode, Encode};

use crate::datablocks::{event_batch_item::EventBatchItem, snapshot_aggregate::SnapshotAggregate, snapshot_aggregate_type::SnapshotAggregateType, snapshot_org::SnapshotOrg};

/// Variable lenth payload block, stored at the end of the wal growing forward into
/// the middle of the file, eventually meeting with metablocks, then continuing to a new wal file
/// Each datablock has a version + crc at the front for upgradability and protect against corruption/bitrot
#[derive(Debug, Clone, Encode, Decode)]
pub enum WalDatablock {
    EventBatch(EventBatchItem),
    SnapshotOrg(SnapshotOrg),
    SnapshotAggregateType(SnapshotAggregateType),
    SnapshotAggregate(SnapshotAggregate),

}