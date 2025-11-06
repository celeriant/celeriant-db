use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::batch_metadata_item_pair::BatchMetadataItemPair;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadAllResult {
    /// Event batches with their metadata
    #[serde(rename = "eb")]
    pub batches: Vec<BatchMetadataItemPair>,

    /// If present, not all event batches were read, this is the event_batch_index of the next event batch to continue reading
    #[serde(rename = "bx")]
    pub next_event_batch_index: Option<u64>,
}