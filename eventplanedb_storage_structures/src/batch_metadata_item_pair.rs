use crate::{event_batch_item::EventBatchItem, event_batch_metadata::EventBatchMetadata};

pub struct BatchMetadataItemPair {
    pub metadata: EventBatchMetadata,
    pub item: EventBatchItem,
}