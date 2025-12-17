use bincode::{Decode, Encode};
use celeriant_wal::datablocks::event_batch_item::EventBatchItem;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, Default)]
pub struct WatchEvent {
    pub event_type: u8,
    pub correlation_id: Option<u128>,
    #[serde(default)]
    pub event_batches: Option<Vec<EventBatchItem>>,
    pub from_event_batch_index: Option<u64>,
    pub to_event_batch_index: Option<u64>,
    pub from_cache: bool,
    pub trim_start_keep_from_event_batch_index: Option<u64>,
}