use celeriant_wal::wal::{event_batch_item::EventBatchItem, event_batch_metadata::EventBatchMetadata};

/// This is where the in-memory data deserialised from the wire finally
/// ends up. We can then provide copies of it to readers as required.
pub struct EventBatchCachedItem {
    pub event_batch_item: EventBatchItem,
    pub event_batch_metadata: EventBatchMetadata
}