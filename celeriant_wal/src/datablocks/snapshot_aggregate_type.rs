use std::collections::HashMap;
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::datablocks::event_type_schema::EventTypeSchema;

/// Periodic snapshotting of each aggregate into the WAL to avoid replaying the entire WAL on startup
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct SnapshotAggregateType {

    /// Arbitary event type numbers from clients with attached
    /// schemas. If None the event type is deprecated
    pub schemas: HashMap<u64, Option<EventTypeSchema>>,
}

impl SnapshotAggregateType {
    pub fn new(
    ) -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }
}