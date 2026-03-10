use celeriant_wire::codec::codec_error::CodecError;

use crate::error::{replication_error::ReplicationError, shard_cache_load_error::ShardCacheLoadError, shard_fsync_error::ShardFsyncError};

#[derive(Debug, Clone)]
pub enum ShardWriteError {
    /// Write request contained no events.
    EmptyEventsList,

    /// Event type 0 is reserved as a sentinel value.
    ZeroEventType {
        client_event_index: u64,
    },

    /// Client already wrote an event with this or higher client_event_index.
    ClientIdempotencyViolation {
        last_client_event_index: u64,
        attempted_client_event_index: u64,
    },

    /// Expected event_batch_index doesn't match current aggregate state.
    OptimisticConcurrencyViolation {
        expected_event_batch_index: u64,
        current_event_batch_index: u64,
    },

    FailedToSerialiseDatablocks(CodecError),

    AggregateNotExists,

    /// Aggregate was deleted with allow_recreate=false and cannot be recreated
    AggregateRecreateNotAllowed,

    /// Replication error (network or follower rejection and s3 is down).
    ReplicationError(ReplicationError),

    ShardFsyncError(ShardFsyncError),

    CacheAggregateClientError(ShardCacheLoadError),

    AggregateExistsAndCacheError(ShardCacheLoadError),
    ShardCannotAcceptWrites { leader_address: Option<String> },

    /// Event payload failed schema validation.
    SchemaValidationFailed {
        event_type_major: u64,
        event_type_minor: u64,
        client_event_index: u64,
        validation_error: String,
    },

    /// Schema compilation failed when loading from WAL.
    SchemaCompilationFailed {
        event_type_major: u64,
        event_type_minor: u64,
        client_event_index: u64,
        compilation_error: String,
    },
}

impl ShardWriteError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::EmptyEventsList => "empty_events_list",
            Self::ZeroEventType { .. } => "zero_event_type",
            Self::ClientIdempotencyViolation { .. } => "client_idempotency_violation",
            Self::OptimisticConcurrencyViolation { .. } => "optimistic_concurrency_violation",
            Self::FailedToSerialiseDatablocks(_) => "serialise_datablocks_failed",
            Self::AggregateNotExists => "aggregate_not_exists",
            Self::AggregateRecreateNotAllowed => "aggregate_recreate_not_allowed",
            Self::ReplicationError(_) => "replication_error",
            Self::ShardFsyncError(_) => "fsync_error",
            Self::CacheAggregateClientError(_) => "cache_load_error",
            Self::AggregateExistsAndCacheError(_) => "cache_load_error",
            Self::ShardCannotAcceptWrites { .. } => "shard_cannot_accept_writes",
            Self::SchemaValidationFailed { .. } => "schema_validation_failed",
            Self::SchemaCompilationFailed { .. } => "schema_compilation_failed",
        }
    }
}

impl From<ShardFsyncError> for ShardWriteError {
    fn from(e: ShardFsyncError) -> Self {
        Self::ShardFsyncError(e)
    }
}

impl From<ReplicationError> for ShardWriteError {
    fn from(e: ReplicationError) -> Self {
        Self::ReplicationError(e)
    }
}