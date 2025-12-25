use std::collections::HashSet;

use bincode::{Decode, Encode};
use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType, datablocks::datablock_aggregate_event::DatablockAggregateEvent};
use serde::{Deserialize, Serialize};

use crate::request::{read_filters::ReadFilters};

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
    pub events: Vec<DatablockAggregateEvent>,
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
    pub correlation_id: Option<u128>,
    pub requested_latency_ms: Option<u64>,
    pub orgs: Option<HashSet<u128>>,
    pub aggregate_types: Option<HashSet<u128>>,
    pub aggregates: Option<HashSet<u128>>,
    pub operation_types: Option<HashSet<u8>>,
}