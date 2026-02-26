use std::collections::{HashMap, HashSet};

use bincode::{Decode, Encode};
use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType, datablocks::datablock_aggregate_event::DatablockAggregateEvent};
#[cfg(feature = "cluster")]
use celeriant_wal::{constants::{EntryHashBytes, STRUCT_TO_MEMORY_REAL_SIZE}, datablocks::datablock::Datablock, metablocks::metablock::Metablock};
use serde::{Deserialize, Serialize};
#[cfg(feature = "cluster")]
use deepsize::DeepSizeOf;

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
pub struct AggregateDetailsRequest {
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

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchRequest {
    pub correlation_id: Option<u128>,
    pub shard_id: u64,
    /// Leader provides its current time to follower to catch clock drift
    pub leader_timestamp_ms: u64,
    /// If there are batches to replicate, they are provided to the follower
    /// Otherwise it's just a heartbeat message
    pub batches: Vec<ReplicationBatchItem>,
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchItem {
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
}

#[cfg(feature = "cluster")]
impl ReplicationBatchItem {
    pub fn size_bytes(&self) -> u64 {
        ((self.metablock.deep_size_of() + self.datablock.deep_size_of()) * STRUCT_TO_MEMORY_REAL_SIZE) as u64
    }
}

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct HeartbeatRequest {
    pub correlation_id: Option<u128>,
    pub shard_id: u64,
    pub leader_timestamp_ms: u64,
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct KickFollowerRequest {
    pub correlation_id: Option<u128>,
}