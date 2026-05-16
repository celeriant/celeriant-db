use bincode::{Decode, Encode};

use celeriant_wal::{constants::EntryHashBytes, metablocks::metablock::Metablock};
use serde::{Deserialize, Serialize};

use crate::response::{aggregate_event_batch::AggregateEventBatch, watch_event::WatchResponseEvent};


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
    pub events: Vec<WatchResponseEvent>,
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
    pub fn is_not_leader(&self) -> bool {
        use crate::error_codes::*;
        matches!(self.error_code, WRITE_NOT_LEADER | TRIM_NOT_LEADER | DELETE_NOT_LEADER)
    }

    pub fn is_identity_required(&self) -> bool {
        self.error_code == crate::error_codes::IDENTIFY_REQUIRED
    }

    pub fn is_server_busy(&self) -> bool {
        self.error_code == crate::error_codes::SERVER_BUSY
            || self.error_code == crate::error_codes::WRITE_REPLICATION_BACKPRESSURE
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


/// Result of a replication batch - either success or explicit rejection.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum ReplicationResult {
    Success {
        last_follower_metablock: Option<Metablock>,
    },
    Rejected(FollowerRejection),
}


#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicationBatchResponse {
    pub correlation_id: Option<u128>,
    pub follower_timestamp_ms: u64,
    pub result: ReplicationResult,
}


/// Result of a heartbeat - either acknowledgement or explicit rejection.
/// The heartbeat is purely a liveness signal. Lease fencing is the replication path's job.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum HeartbeatResult {
    Ack {
        follower_timestamp_ms: u64,
        follower_can_accept_tcp_replication: bool,
    },
    Rejected(HeartbeatRejection),
}


#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct HeartbeatResponse {
    pub correlation_id: Option<u128>,
    pub result: HeartbeatResult,
}


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
    /// SHA-256 hex string of the cluster's current compression dict. None when algorithm != ZstdDict.
    #[serde(default)]
    pub compression_dict_sha256: Option<String>,
    /// Raw dict bytes (~14 KiB). Shipped once per client when client doesn't have this dict yet.
    /// None when client's known_dict_sha256 matches
    #[serde(default)]
    pub compression_dict_bytes: Option<Vec<u8>>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
#[serde(into = "u8", try_from = "u8")]
pub enum AccessLevel {
    ReadWrite = 1,
    ReadOnly = 2,
}

impl From<AccessLevel> for u8 {
    fn from(level: AccessLevel) -> u8 {
        level as u8
    }
}

impl TryFrom<u8> for AccessLevel {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(AccessLevel::ReadWrite),
            2 => Ok(AccessLevel::ReadOnly),
            _ => Err(value),
        }
    }
}
