use std::collections::HashMap;

use bincode::{Decode, Encode};
use celeriant_wal::aggregate_key::AggregateKey;
#[cfg(feature = "cluster")]
use celeriant_wal::{constants::EntryHashBytes, metablocks::metablock::Metablock};
use serde::{Deserialize, Serialize};

#[cfg(feature = "cluster")]
use crate::request::requests::ReplicationBatchItem;
use crate::response::{aggregate_event_batch::AggregateEventBatch, watch_event::WatchEvent};


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
pub struct AggregateDetailsResponse {
    pub correlation_id: Option<u128>,
    pub min_event_batch_index: u64,
    pub max_event_batch_index: u64,
    pub max_event_index: u64,
    pub is_deleted: bool,
    pub allow_recreate: bool,
    pub allow_index_continuation: bool,
    pub last_server_timestamp: u64,
    pub last_client_id: u128,
    pub last_user_id: Option<u128>,
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

impl ErrorResponse {
    /// Error codes returned when a write/trim/delete is sent to a non-leader node.
    /// The error_message JSON may contain `{"leader_address":"host:port"}`.
    pub const WRITE_NOT_LEADER: u32 = 2011;
    pub const TRIM_NOT_LEADER: u32 = 3005;
    pub const DELETE_NOT_LEADER: u32 = 4006;

    /// Server requires client identity verification but none was provided.
    pub const IDENTIFY_REQUIRED: u32 = 10004;

    /// Authentication error codes
    pub const AUTH_REQUIRED: u32 = 1001;
    pub const AUTH_INVALID_KEY: u32 = 1002;
    pub const AUTH_INSUFFICIENT_PERMISSIONS: u32 = 1003;

    pub fn is_not_leader(&self) -> bool {
        matches!(self.error_code, Self::WRITE_NOT_LEADER | Self::TRIM_NOT_LEADER | Self::DELETE_NOT_LEADER)
    }

    pub fn is_identity_required(&self) -> bool {
        self.error_code == Self::IDENTIFY_REQUIRED
    }

    /// Extract leader address from error_message JSON like `{"leader_address":"host:port"}`.
    /// Returns None if the message doesn't contain a leader address.
    pub fn parse_leader_address(&self) -> Option<String> {
        let key = "\"leader_address\":\"";
        let start = self.error_message.find(key)? + key.len();
        let rest = &self.error_message[start..];
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    }
}

#[cfg(feature = "cluster")]
/// Rejection reasons when follower refuses a replication batch.
/// These are logical errors indicating state mismatch, not network failures.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, PartialEq, Eq)]
pub enum FollowerRejection {
    /// Node is not configured as a follower.
    NotAFollower,
    /// Clock skew between leader and follower exceeds threshold.
    TimeDriftTooHigh {
        leader_ms: u64,
        follower_ms: u64,
        max_allowed_ms: u64,
    },
    /// Follower's WAL index doesn't match leader's expected position.
    WalIndexMismatch {
        max_follower_wal_index: u64,
    },
    /// Follower's tip hash doesn't match leader's expected hash.
    TipHashMismatch {
        follower: EntryHashBytes,
        follower_wal_index: u64,
        leader: EntryHashBytes,
        leader_wal_index: u64,
    },
    /// Leader sent empty batch.
    EmptyBatch,
    /// Batch item references external datablock but none provided.
    MissingDatablock,
    /// Follower's lease index doesn't match leader's expectation.
    StaleLease {
        follower_lease_index: u64,
        received_lease_index: u64,
    },
}

#[cfg(feature = "cluster")]
/// Rejection reasons when follower refuses a heartbeat.
/// Lease validation is handled by the replication path (FollowerRejection::StaleLease).
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, PartialEq, Eq)]
pub enum HeartbeatRejection {
    /// Clock skew between leader and follower exceeds threshold.
    ClockDriftTooHigh {
        leader_ms: u64,
        follower_ms: u64,
        max_allowed_ms: u64,
    },
    NotAFollower,
}

#[cfg(feature = "cluster")]
/// Result of a replication batch - either success or explicit rejection.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum ReplicationResult {
    Success {
        last_follower_metablock: Option<Metablock>,
    },
    Rejected(FollowerRejection),
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchResponse {
    pub correlation_id: Option<u128>,
    pub follower_timestamp_ms: u64,
    pub result: ReplicationResult,
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct CatchUpResponse {
    pub correlation_id: Option<u128>,
    /// Leader's expected tip_hash at follower's current position.
    /// Follower rejects batch if its tip_hash doesn't match.
    pub expected_follower_tip_hash: Option<EntryHashBytes>,
    /// Batches for the follower to append to its WAL for the requested shard
    /// May not contain everything (paginated)
    pub batches: Vec<ReplicationBatchItem>,
    /// Leader decides if the follower has caught up enough to become live
    pub continue_catching_up: bool,
}

#[cfg(feature = "cluster")]
/// Result of a heartbeat - either acknowledgement or explicit rejection.
/// The heartbeat is purely a liveness signal. Lease fencing is the replication path's job.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum HeartbeatResult {
    Ack { follower_timestamp_ms: u64 },
    Rejected(HeartbeatRejection),
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct HeartbeatResponse {
    pub correlation_id: Option<u128>,
    pub result: HeartbeatResult,
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct KickFollowerResponse {
    pub correlation_id: Option<u128>,
    pub acknowledged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct IdentifyResponse {
    pub correlation_id: Option<u128>,
    pub client_id: Option<u128>,
    /// Access level granted after authentication. None if auth was disabled.
    pub access_level: Option<AccessLevel>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum AccessLevel {
    ReadWrite = 1,
    ReadOnly = 2,
}
