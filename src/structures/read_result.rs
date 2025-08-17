use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::structures::event_batch_item::EventBatchItem;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadResult {
    /// Events read from the server, in batches. May not contain all event batches, check next_server_id for pagination
    #[serde(rename = "eb")]
    pub event_batches: Vec<EventBatchItem>,

    /// If present, not all event batches were read, this is the server id of the next event batch to continue reading
    #[serde(rename = "si")]
    pub next_server_id: Option<u64>,
}
