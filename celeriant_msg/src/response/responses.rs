use std::collections::HashMap;

use bincode::{Decode, Encode};
use celeriant_wal::{aggregate_key::AggregateKey, metablocks::metablock::Metablock};
use serde::{Deserialize, Serialize};

use crate::{request::requests::ReplicationBatchItem, response::{aggregate_event_batch::AggregateEventBatch, watch_event::WatchEvent}};


#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct OrgListItem {
    pub org_id: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct AggregateTypeListItem {
    pub org_id: u128,
    pub aggregate_type_id: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct AggregateListItem {
    pub is_deleted: bool,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub event_batch_count: u64,
    pub min_event_timestamp: u64,
    pub max_event_timestamp: u64,
    pub min_event_batch_index: u64,
    pub max_event_batch_index: u64,
    pub min_event_index: u64,
    pub max_event_index: u64,
    pub min_server_timestamp: u64,
    pub max_server_timestamp: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListOrgsResponse {
    pub correlation_id: Option<u128>,
    pub orgs: Vec<OrgListItem>,
    pub next_cursor: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListAggregateTypesResponse {
    pub correlation_id: Option<u128>,
    pub aggregate_types: Vec<AggregateTypeListItem>,
    pub next_cursor: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListAggregatesResponse {
    pub correlation_id: Option<u128>,
    pub aggregates: Vec<AggregateListItem>,
    pub next_cursor: Option<u64>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchResponse {
    pub correlation_id: Option<u128>,
    /// Leader can use last follower metablock to check for replication 
    /// success, position, fall behind, or to decide to kick follower
    pub last_follower_metablock: Option<Metablock>,
    /// Leader will also check the follower's current time for clock drift
    pub follower_timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct CatchUpResponse {
    pub correlation_id: Option<u128>,
    /// Batches for the follower to append to its WAL for the requested shard
    /// May not contain everything (paginated)
    pub batches: Vec<ReplicationBatchItem>,
    /// Leader decides if the follower has caught up enough to become live
    pub continue_catching_up: bool,
}
