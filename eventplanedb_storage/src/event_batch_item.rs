use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::event_item::EventItem;

use crate::serde_u128_base64;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EventBatchItem {
    #[serde(rename = "si")]
    pub server_id: u64,
    #[serde(rename = "sd")]
    pub server_date: u64,
    #[serde(with = "serde_u128_base64", rename = "ci")]
    pub client_id: u128,
    #[serde(skip_serializing_if = "Option::is_none", rename = "ui")]
    pub user_id: Option<String>,
    #[serde(rename = "ev")]
    pub events: Vec<EventItem>,
}

impl EventBatchItem {
    pub fn new() -> Self {
        EventBatchItem {
            server_id: 0,
            server_date: 0,
            user_id: None,
            events: Vec::new(),
            client_id: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        EventBatchItem {
            server_id: 0,
            server_date: 0,
            user_id: None,
            events: Vec::with_capacity(capacity),
            client_id: 0,
        }
    }

    pub fn add_event(&mut self, event: EventItem) {
        self.events.push(event);
    }
}