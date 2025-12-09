use bincode::{Decode, Encode};
use celeriant_wal::wal::event_batch_item::EventBatchItem;
use serde::{Deserialize, Serialize};

use crate::response::{aggregate_info::AggregateInfo, organisation_info::OrganisationInfo};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListOrganisationsResponse {
    pub correlation_id: Option<u128>,
    pub organisations: Vec<OrganisationInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListAggregatesResponse {
    pub correlation_id: Option<u128>,
    pub aggregates: Vec<AggregateInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ExistsResponse {
    pub correlation_id: Option<u128>,
    pub min_event_batch_index: u64,
    pub max_event_batch_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadResponse {
    pub correlation_id: Option<u128>,
    pub event_batches: Vec<EventBatchItem>,
    pub next_event_batch_index: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WriteResponse {
    pub correlation_id: Option<u128>,
    pub event_batch_index: u64,
    pub start_event_index: u64,
    pub server_timestamp: u64,
    pub compressed_size: u64,
    pub node_id: u128,
    pub lease_index: u64,
    pub events_crc: u32,
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

