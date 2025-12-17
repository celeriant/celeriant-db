use bincode::{Decode, Encode};
use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType, datablocks::{event_item::EventItem}};
use serde::{Deserialize, Serialize};

use crate::request::{directory_filters::DirectoryFilters, read_filters::ReadFilters};

// Individual request structs
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListOrganisationsRequest {
    pub correlation_id: Option<u128>,
    pub filters: DirectoryFilters,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListAggregatesRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: Option<u128>,
    pub filters: DirectoryFilters,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ExistsRequest {
    pub correlation_id: Option<u128>,
    pub aggregate_key: AggregateKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadRequest {
    pub correlation_id: Option<u128>,
    pub aggregate_key: AggregateKey,
    pub filters: ReadFilters,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WriteRequest {
    pub correlation_id: Option<u128>,
    pub aggregate_key: AggregateKey,
    pub client_id: u128,
    pub user_id: Option<u128>,
    pub events: Vec<EventItem>,
    pub allow_create: bool,
    pub expected_event_batch_index: Option<u64>,
    pub enforce_client_idempotency: bool,
    pub compression_type: CompressionType,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TrimStartRequest {
    pub correlation_id: Option<u128>,
    pub aggregate_key: AggregateKey,
    pub keep_from_event_batch_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeleteRequest {
    pub correlation_id: Option<u128>,
    pub aggregate_key: AggregateKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WatchRequest {
    pub subscribe_to_event_types: Vec<u8>,
    pub correlation_id: Option<u128>,
    pub aggregate_key: AggregateKey,
    pub requested_latency_ms: Option<u64>,
    pub requested_throughput_bs: Option<usize>,
    pub filters: Option<ReadFilters>,
}