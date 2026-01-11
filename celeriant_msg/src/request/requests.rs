use std::collections::{HashMap, HashSet};

use bincode::{Decode, Encode};
use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType, datablocks::datablock_aggregate_event::DatablockAggregateEvent};
use serde::{Deserialize, Serialize};

use crate::request::{read_filters::ReadFilters};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListOrgsRequest {
    pub correlation_id: Option<u128>,
    pub shard_id: u64,
    /// WAL index to continue scanning from (exclusive). None starts from latest.
    pub cursor: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListAggregateTypesRequest {
    pub correlation_id: Option<u128>,
    pub shard_id: u64,
    /// Optional filter by org_id
    pub org_id: Option<u128>,
    /// WAL index to continue scanning from (exclusive). None starts from latest.
    pub cursor: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListAggregatesRequest {
    pub correlation_id: Option<u128>,
    pub shard_id: u64,
    /// Optional filter by org_id
    pub org_id: Option<u128>,
    /// Optional filter by aggregate_type_id (requires org_id if specified)
    pub aggregate_type_id: Option<u128>,
    /// WAL index to continue scanning from (exclusive). None starts from latest.
    pub cursor: Option<u64>,
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
    pub client_id: u128,
    pub user_id: Option<u128>,
    pub writes: HashMap<AggregateKey, SingleAggregateWrite>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SingleAggregateWrite {
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
    pub client_id: u128,
    pub user_id: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeleteRequest {
    pub correlation_id: Option<u128>,
    pub client_id: u128,
    pub user_id: Option<u128>,
    pub deletes: HashMap<AggregateKey, SingleAggregateDelete>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SingleAggregateDelete {
    pub allow_recreate: bool,
    pub allow_index_continuation: bool,
    pub expected_event_batch_index: Option<u64>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchRequest {
    pub correlation_id: Option<u128>,
    pub lease_index: u64,
    pub shard_id: u64,
    pub batches: Vec<ReplicationBatchItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchItem {
    pub wal_index: u64,
    pub metablock_bytes: Vec<u8>,
    pub datablock_bytes: Option<Vec<u8>>,
}