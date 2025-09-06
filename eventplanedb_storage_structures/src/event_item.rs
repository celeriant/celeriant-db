use std::sync::Arc;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EventItem {
    /// Client derived incremented index position used to prevent client from writing the same event twice
    #[serde(rename = "cx")]
    pub client_event_index: u64,

    /// A server-side incremented index for each event
    #[serde(rename = "ex")]
    pub event_index: u64,

    /// Client derived Unix timestamp in milliseconds when the event occurred
    #[serde(rename = "et")]
    pub event_timestamp: u64,

    /// Event type which allows clients to determine the schema of the value payload
    #[serde(rename = "tp")]
    pub event_type_major: u64,

    /// Minor version of event, forwards compatible, clients reading event don't need updating
    #[serde(rename = "tm")]
    pub event_type_minor: u64,

    /// Serialized event data payload
    #[serde(rename = "ev")]
    pub event_value: Arc<Vec<u8>>,
}

impl EventItem {
    pub fn new(
        client_event_index: u64,
        event_index: u64,
        event_timestamp: u64,
        event_type_major: u64,
        event_type_minor: u64,
        event_value: Vec<u8>,
    ) -> Self {
        let event_value = Arc::new(event_value);
        Self {
            client_event_index,
            event_index,
            event_timestamp,
            event_type_major,
            event_type_minor,
            event_value,
        }
    }
}
