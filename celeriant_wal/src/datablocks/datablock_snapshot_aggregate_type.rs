use std::collections::HashMap;
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::datablocks::datablock_event_type_schema::DatablockEventTypeSchema;

/// Periodic snapshotting of each aggregate into the WAL to avoid replaying the entire WAL on startup
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct DatablockSnapshotAggregateType {
    /// Arbitary event type numbers from clients with attached
    /// schemas. If None the event type is deprecated
    pub schemas: HashMap<u64, Option<DatablockEventTypeSchema>>,
}