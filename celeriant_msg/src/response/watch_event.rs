use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Sent to clients when they are watching an aggregate
/// Allows them visibility on read/write/etc events and pushes batches to them
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, Default)]
pub struct WatchEvent {
    pub from_event_batch_index: Option<u64>,
    pub to_event_batch_index: Option<u64>,
    pub keep_from_event_batch_index: Option<u64>,
}