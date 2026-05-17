use std::sync::Arc;

use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::serde::serde_arc_vec_u8_bytes;
use crate::serde::serde_option_fixed_u8_array_bytes;

/// A single event which has an arbitary length byte message payload from the client
/// Typically validated against a schema based on the event type major+minor versions
#[derive(Default, Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct DatablockAggregateEvent {
    /// Client derived incremented index position used to prevent client from writing the same event twice
    pub client_seq: u64,

    /// A server-side incremented index for each event
    pub event_seq: u64,

    /// Optional unique identifier for the event assigned by the client
    pub event_id: Option<u128>,

    /// Client derived Unix timestamp in milliseconds when the event occurred
    pub event_timestamp: u64,

    /// Event type which allows clients to determine the schema of the value payload
    pub event_type_major: u64,

    /// Minor version of event, forwards compatible, clients reading event don't need updating
    pub event_type_minor: u64,

    /// Serialized event data payload
    /// Needs to be wrapped in an ARC so we don't copy these bytes across thread boundaries
    #[serde(with = "serde_arc_vec_u8_bytes")]
    pub event_value: Arc<Vec<u8>>,

    /// Initialization vector for encrypted event_value (12 bytes for AES-GCM)
    #[serde(with = "serde_option_fixed_u8_array_bytes", default)]
    pub iv: Option<[u8; 12]>,
}
