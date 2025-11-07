use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::event_item::EventItem;
use crate::serde::serde_option_u128_base64;
use crate::serde::serde_u128_base64;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EventBatchItem {
    /// Unique, incremented integer assigned to each event batch when persisted on the server
    #[serde(rename = "bx")]
    pub event_batch_index: u64,

    /// Server side Unix timestamp in milliseconds when the event batch was persisted on the server
    #[serde(rename = "st")]
    pub server_timestamp: u64,

    /// Unique identifyer of the machine that produced these events. Typically the truncated SHA256 of the clients' public key
    #[serde(with = "serde_u128_base64", rename = "ci")]
    pub client_id: u128,

    /// Unique identifyer of the user that produced these events. Typically the truncated SHA256 of the users' sub field
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "serde_option_u128_base64",
        rename = "ui"
    )]
    pub user_id: Option<u128>,

    /// Events present in this batch, all from the same client / user
    #[serde(rename = "ev")]
    pub events: Vec<EventItem>,
}

impl EventBatchItem {
    pub fn new(
        event_batch_index: u64,
        server_time: u64,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
    ) -> Self {
        Self {
            event_batch_index,
            server_timestamp: server_time,
            client_id,
            user_id,
            events,
        }
    }

    pub fn size_bytes(&self) -> usize {
        let mut size = 0;
        
        // Fixed size fields
        size += 8;  // event_batch_index
        size += 8;  // server_timestamp
        size += 16; // client_id
        
        // Option discriminant + potential value
        size += 1; // user_id discriminant
        if self.user_id.is_some() {
            size += 16; // u128
        }
        
        // Vec length prefix for events
        size += 8;
        
        // Sum up all events
        for event in &self.events {
            size += event.size_bytes();
        }
        
        size
    }

}
