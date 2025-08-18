use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EventItem {
    /// Client derived incremented index position used to prevent client from writing the same event twice
    #[serde(rename = "li")]
    pub local_index: u64,

    /// Client derived Unix timestamp in milliseconds when the event occurred
    #[serde(rename = "et")]
    pub event_time: u64,

    /// Event type which allows clients to determine the schema of the value payload
    #[serde(rename = "tp")]
    pub event_type_major: u64,

    /// Minor version of event, forwards compatible, clients reading event don't need updating
    #[serde(rename = "tm")]
    pub event_type_minor: u64,

    /// Serialized event data payload
    #[serde(rename = "va")]
    pub value: Vec<u8>,
}

impl EventItem {
    pub fn new(
        local_index: u64,
        event_time: u64,
        event_type_major: u64,
        event_type_minor: u64,
        value: Vec<u8>,
    ) -> Self {
        Self {
            local_index,
            event_time,
            event_type_major,
            event_type_minor,
            value,
        }
    }
}
