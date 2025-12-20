use std::sync::Arc;

use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::serde::serde_arc_vec_u8_base64;
use crate::serde::serde_fixed_u8_array_base64;
use crate::serde::serde_option_u128_base64;

/// A single event which has an arbitary length byte message payload from the client
/// Typically validated against a schema based on the event type major+minor versions
#[derive(Default, Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct DatablockAggregateEvent {
    /// Client derived incremented index position used to prevent client from writing the same event twice
    #[serde(rename = "cx")]
    pub client_event_index: u64,

    /// A server-side incremented index for each event
    #[serde(rename = "ex")]
    pub event_index: u64,

    /// Optional unique identifier for the event assigned by the client
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "serde_option_u128_base64",
        rename = "id"
    )]
    pub event_id: Option<u128>,

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
    /// Needs to be wrapped in an ARC so we don't copy these bytes across thread boundaries
    #[serde(with = "serde_arc_vec_u8_base64", rename = "ev")]
    pub event_value: Arc<Vec<u8>>,

    /// Initialization vector for encrypted event_value (12 bytes for AES-GCM)
    #[serde(
        with = "serde_fixed_u8_array_base64",
        rename = "iv",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub iv: Option<[u8; 12]>,
}