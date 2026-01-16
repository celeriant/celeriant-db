use std::collections::{HashMap, HashSet};

use bincode::{Decode, Encode};
use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType, constants::EntryHashBytes, datablocks::{datablock::Datablock, datablock_aggregate_event::DatablockAggregateEvent}, metablocks::metablock::Metablock};
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
    pub shard_id: u64,
    /// Leader provides its current time to follower to catch clock drift
    pub leader_timestamp_ms: u64,
    /// Leader has decided to kick the follower
    /// Follower won't rejoin until it's caught back up
    pub follower_too_far_behind: bool,
    /// Leader's expected tip_hash at follower's current position.
    /// Follower rejects batch if its tip_hash doesn't match.
    pub expected_follower_tip_hash: Option<EntryHashBytes>,
    /// If there are batches to replicate, they are provided to the follower
    /// Otherwise it's just a heartbeat message
    pub batches: Vec<ReplicationBatchItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchItem {
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
}

/// Follower-initiated protocol for pulling WAL entries during initial sync
/// or after falling behind. Follower sends its current position and leader
/// responds with entries up to max_entries.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct CatchUpRequest {
    pub correlation_id: Option<u128>,
    pub shard_id: u64,
    /// Follower current position in the WAL
    /// Leader checks this with its WAL - is the hash chain for the wal index correct?
    /// Is the follower caught up enough?
    pub last_follower_metablock: Option<Metablock>,
    /// Either all 0's or the hash up and including the follower's last metablock
    pub follower_tip_hash: Option<EntryHashBytes>,
}