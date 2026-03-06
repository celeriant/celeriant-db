use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// A single watch notification sent to clients.
/// Flattened for easy consumption in any language.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WatchResponseEvent {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub operation: u8,
    pub from_event_batch_index: Option<u64>,
    pub to_event_batch_index: Option<u64>,
    pub keep_from_event_batch_index: Option<u64>,
}