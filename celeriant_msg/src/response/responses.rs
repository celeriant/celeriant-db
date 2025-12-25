use std::collections::HashMap;

use bincode::{Decode, Encode};
use celeriant_wal::{aggregate_key::AggregateKey};
use serde::{Deserialize, Serialize};

use crate::response::{aggregate_event_batch::AggregateEventBatch, watch_event::WatchEvent};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ExistsResponse {
    pub correlation_id: Option<u128>,
    pub min_event_batch_index: u64,
    //TODO: Include other metadata.
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadResponse {
    pub correlation_id: Option<u128>,
    pub event_batches: Vec<AggregateEventBatch>,
    pub next_event_batch_index: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, Default)]
pub struct WatchResponse {
    pub events: Option<HashMap<AggregateKey, HashMap<u8, Option<WatchEvent>>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SuccessResponse {
    pub correlation_id: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ProtocolErrorResponse {
    // No correlation id as we couldn't read the request data
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ErrorResponse {
    pub correlation_id: Option<u128>,
    pub error_code: u32,
    pub error_message: String,
}

