use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::{event_batch_item::EventBatchItem, event_batch_metadata::EventBatchMetadata};


#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct BatchMetadataItemPair {
    pub event_batch_metadata: EventBatchMetadata,
    pub event_batch_item: EventBatchItem,
}