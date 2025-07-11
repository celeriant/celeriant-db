use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::event_item::EventItem;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EventBatchItem {
    pub si: u64,
    pub cb: Option<String>,
    pub sd: u64,
    pub events: Vec<EventItem>,
}

impl EventBatchItem {
    pub fn new() -> Self {
        EventBatchItem {
            si: 0,
            sd: 0,
            cb: None,
            events: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        EventBatchItem {
            si: 0,
            sd: 0,
            cb: None,
            events: Vec::with_capacity(capacity),
        }
    }

    pub fn add_event(&mut self, event: EventItem) {
        self.events.push(event);
    }
}