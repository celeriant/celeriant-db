use std::sync::Arc;

use crate::event_batch_item::EventBatchItem;
#[cfg(test)]
use crate::event_item::EventItem;

#[derive(Debug)]
pub struct CatchupResult {
    pub event_batches: Vec<Arc<EventBatchItem>>,
    pub next_si: Option<u64>
}

impl CatchupResult {

    #[cfg(test)]
    pub fn flatten_events(&self) -> Vec<EventItem> {
        self.event_batches
            .iter()
            .flat_map(|event_batch| {
                event_batch.events.iter().cloned()
            })
            .collect()
    }
}