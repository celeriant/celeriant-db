use std::collections::{HashMap, HashSet};

use bincode::{Decode, Encode};
use celeriant_wal::{aggregate_key::AggregateKey, datablocks::datablock_aggregate_event::DatablockAggregateEvent, schema_key::SchemaKey};

use celeriant_wal::{constants::STRUCT_TO_MEMORY_REAL_SIZE, datablocks::datablock::Datablock, metablocks::metablock::Metablock};
use serde::{Deserialize, Serialize};

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
    pub shard_id: Option<u64>,
    pub orgs: Option<HashSet<u128>>,
    pub aggregate_types: Option<HashSet<u128>>,
    pub aggregates: Option<HashSet<u128>>,
    pub operation_types: Option<HashSet<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct RegisterSchemaRequest {
    pub correlation_id: Option<u128>,
    pub client_id: u128,
    pub user_id: Option<u128>,
    pub schema_key: SchemaKey,
    pub schema_type: u8,
    pub schema: String,
}


#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchRequest {
    pub correlation_id: Option<u128>,
    pub shard_id: u64,
    /// Leader provides its current time to follower to catch clock drift
    pub leader_timestamp_ms: u64,
    /// Leader's `read.wal_index` at send time. Follower uses this as the
    /// promotion-batch upload floor (sets `last_received_replication_wal_index = this + 1`).
    pub leader_confirmed_wal_index: u64,
    /// If there are batches to replicate, they are provided to the follower
    /// Otherwise it's just a heartbeat message
    pub batches: Vec<ReplicationBatchItem>,
}


#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchItem {
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
}


impl ReplicationBatchItem {
    pub fn size_bytes(&self) -> u64 {
        ((self.metablock.deep_size_of() + self.datablock.deep_size_of()) * STRUCT_TO_MEMORY_REAL_SIZE) as u64
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct HeartbeatRequest {
    pub correlation_id: Option<u128>,
    pub shard_id: u64,
    pub leader_timestamp_ms: u64,
    pub lease_index: u64,
}


#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct KickFollowerRequest {
    pub correlation_id: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct IdentifyRequest {
    pub correlation_id: Option<u128>,
    /// Base64-encoded DER public key (SubjectPublicKeyInfo format). None for API-key-only auth.
    pub public_key: Option<String>,
    /// Client-generated nonce: UTC epoch milliseconds as decimal string. None for API-key-only auth.
    pub nonce: Option<String>,
    /// Base64-encoded RSASSA-PKCS1-v1_5-SHA256 signature over the nonce. None for API-key-only auth.
    pub signature: Option<String>,
    /// Base64-encoded 32-byte API key. None when authentication is disabled.
    pub api_key: Option<String>,
    /// SHA-256 hex string of the dict the client already has cached. None if no dict cached
    #[serde(default)]
    pub known_dict_sha256: Option<String>,
}