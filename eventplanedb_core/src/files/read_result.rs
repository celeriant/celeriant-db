use bincode::{Decode, Encode};
use eventplanedb_structures::event_batch_item::EventBatchItem;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadResult {
    /// Events read from the server, in batches. May not contain all event batches, check next_event_batch_index for pagination
    #[serde(rename = "eb")]
    pub event_batches: Vec<EventBatchItem>,

    /// If present, not all event batches were read, this is the event_batch_index of the next event batch to continue reading
    #[serde(rename = "bx")]
    pub next_event_batch_index: Option<u64>,
}
