use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::datablocks::{datablock_aggregate_event_batch::DatablockAggregateEventBatch, datablock_schema_registration::DatablockSchemaRegistration};

/// Different kinds of WAL datablocks
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub enum DatablockKind {
    EventBatchItem(DatablockAggregateEventBatch),
    SchemaRegistration(DatablockSchemaRegistration),
}