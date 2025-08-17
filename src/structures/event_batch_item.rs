use std::io;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::structures::event_item::EventItem;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EventBatchItem {
    /// Unique, incremented integer assigned to each event batch when persisted on the server
    #[serde(rename = "si")]
    pub server_id: u64,

    /// Server side Unix timestamp in milliseconds when the event batch was persisted on the server
    #[serde(rename = "st")]
    pub server_time: u64,

    /// Unique identifyer of the machine that produced these events. Typically the truncated SHA256 of the clients' public key
    #[serde(rename = "ci")]
    pub client_id: u64,

    /// Optional user id, if oauth is used or some other method of identifying a user
    #[serde(skip_serializing_if = "Option::is_none", rename = "ui")]
    pub user_id: Option<String>,

    /// Events present in this batch, all from the same client / user
    #[serde(rename = "ev")]
    pub events: Vec<EventItem>,
}

impl EventBatchItem {
    pub fn new(
        server_id: u64,
        server_time: u64,
        client_id: u64,
        user_id: Option<String>,
        events: Vec<EventItem>,
    ) -> Self {
        Self {
            server_id,
            server_time,
            client_id,
            user_id,
            events,
        }
    }

    /// Serialize and compress the event batch item into a wire format
    pub fn to_wire_format(&self) -> io::Result<Vec<u8>> {
        let serialized = bincode::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| io::Error::other(e.to_string()))?;

        zstd::bulk::compress(&serialized, 6).map_err(|e| io::Error::other(e.to_string()))
    }

    /// Deserialize and decompress from wire format
    pub fn from_wire_format(data: &[u8], capacity: usize) -> io::Result<Self> {
        let decompressed =
            zstd::bulk::decompress(data, capacity).map_err(|e| io::Error::other(e.to_string()))?;

        bincode::decode_from_slice(&decompressed, bincode::config::standard())
            .map(|(events, _)| events)
            .map_err(|e| io::Error::other(e.to_string()))
    }
}
