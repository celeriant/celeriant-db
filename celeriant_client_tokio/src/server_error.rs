use celeriant_msg::error_codes::*;
use celeriant_msg::response::responses::ErrorResponse;

/// Extract a u64 field from a JSON string best-effort. Returns None on any failure.
fn parse_u64_field(json: &str, field: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    v.get(field)?.as_u64()
}

#[derive(Debug)]
pub enum ReadError {
    UnavailableBatchIndex {
        requested_version: Option<u64>,
        minimum_available_version: Option<u64>,
    },
    AggregateNotExists,
    CacheLoadLockTimeout,
    CacheLoadFileScan,
    FetchDatablocks,
    FetchMetablocks,
}

#[derive(Debug)]
pub enum WriteError {
    EmptyEventsList,
    ZeroEventType,
    ClientIdempotencyViolation {
        last_client_seq: Option<u64>,
        attempted_client_seq: Option<u64>,
    },
    OptimisticConcurrencyViolation {
        expected_version: Option<u64>,
        current_aggregate_version: Option<u64>,
    },
    FailedToSerialiseDatablocks,
    AggregateNotExists,
    AggregateRecreateNotAllowed,
    ReplicationError,
    FsyncError,
    CacheAggregateClientError,
    AggregateExistsCacheError,
}

#[derive(Debug)]
pub enum SchemaError {
    AlreadyExists,
    Invalid,
    ValidationFailed,
    CompilationFailed,
    UnsupportedType,
    CacheLoadError,
    FsyncError,
    CannotAcceptWrites,
    ReplicationError,
    CoordinationFailed,
}

#[derive(Debug)]
pub enum DeleteError {
    AggregateNotExists,
    EmptyDeleteList,
    OptimisticConcurrencyViolation {
        expected_version: Option<u64>,
        current_aggregate_version: Option<u64>,
    },
    CacheError,
    ReplicationError,
    FsyncError,
}

#[derive(Debug)]
pub enum TrimError {
    AggregateNotExists,
    CacheError,
    ReplicationError,
    FsyncError,
    IndexOutOfRange,
}

#[derive(Debug)]
pub enum WatchError {
    RequestInvalid,
    LatencyTooHigh,
    ReadIo,
    ReadSerialization,
    ReadOther,
}

#[derive(Debug)]
pub enum DetailsError {
    CacheError,
    AggregateNotExists,
    MetablockReadError,
}

#[derive(Debug)]
pub enum AuthError {
    InvalidNonce,
    InvalidSignature,
    Mismatch,
    Required,
    AuthRequired,
    InvalidKey,
    InsufficientPermissions,
}

#[derive(Debug)]
pub enum ServerError {
    Read { kind: ReadError, error_message: String },
    Write { kind: WriteError, error_message: String },
    Schema { kind: SchemaError, error_message: String },
    Delete { kind: DeleteError, error_message: String },
    Trim { kind: TrimError, error_message: String },
    Watch { kind: WatchError, error_message: String },
    Details { kind: DetailsError, error_message: String },
    Auth { kind: AuthError, error_message: String },
    List { error_code: u32, error_message: String },
    Replication { error_code: u32, error_message: String },
    ShardRouting { error_code: u32, error_message: String },
    Unknown { error_code: u32, error_message: String },
}

impl From<ErrorResponse> for ServerError {
    fn from(e: ErrorResponse) -> Self {
        let code = e.error_code;
        let msg = e.error_message;
        match code {
            READ_UNAVAILABLE_VERSION => ServerError::Read {
                kind: ReadError::UnavailableBatchIndex {
                    requested_version: parse_u64_field(&msg, "requested"),
                    minimum_available_version: parse_u64_field(&msg, "minimum_available"),
                },
                error_message: msg,
            },
            READ_AGGREGATE_NOT_EXISTS => ServerError::Read { kind: ReadError::AggregateNotExists, error_message: msg },
            READ_CACHE_LOAD_LOCK_TIMEOUT => ServerError::Read { kind: ReadError::CacheLoadLockTimeout, error_message: msg },
            READ_CACHE_LOAD_FILE_SCAN => ServerError::Read { kind: ReadError::CacheLoadFileScan, error_message: msg },
            READ_FETCH_DATABLOCKS => ServerError::Read { kind: ReadError::FetchDatablocks, error_message: msg },
            READ_FETCH_METABLOCKS => ServerError::Read { kind: ReadError::FetchMetablocks, error_message: msg },

            WRITE_EMPTY_EVENTS_LIST => ServerError::Write { kind: WriteError::EmptyEventsList, error_message: msg },
            WRITE_ZERO_EVENT_TYPE => ServerError::Write { kind: WriteError::ZeroEventType, error_message: msg },
            WRITE_CLIENT_IDEMPOTENCY_VIOLATION => ServerError::Write {
                kind: WriteError::ClientIdempotencyViolation {
                    last_client_seq: parse_u64_field(&msg, "last_client_seq"),
                    attempted_client_seq: parse_u64_field(&msg, "attempted_client_seq"),
                },
                error_message: msg,
            },
            WRITE_OPTIMISTIC_CONCURRENCY_VIOLATION => ServerError::Write {
                kind: WriteError::OptimisticConcurrencyViolation {
                    expected_version: parse_u64_field(&msg, "expected_version"),
                    current_aggregate_version: parse_u64_field(&msg, "current_aggregate_version"),
                },
                error_message: msg,
            },
            WRITE_FAILED_TO_SERIALISE_DATABLOCKS => ServerError::Write { kind: WriteError::FailedToSerialiseDatablocks, error_message: msg },
            WRITE_AGGREGATE_NOT_EXISTS => ServerError::Write { kind: WriteError::AggregateNotExists, error_message: msg },
            WRITE_AGGREGATE_RECREATE_NOT_ALLOWED => ServerError::Write { kind: WriteError::AggregateRecreateNotAllowed, error_message: msg },
            WRITE_REPLICATION_ERROR => ServerError::Write { kind: WriteError::ReplicationError, error_message: msg },
            WRITE_FSYNC_ERROR => ServerError::Write { kind: WriteError::FsyncError, error_message: msg },
            WRITE_CACHE_AGGREGATE_CLIENT_ERROR => ServerError::Write { kind: WriteError::CacheAggregateClientError, error_message: msg },
            WRITE_AGGREGATE_EXISTS_CACHE_ERROR => ServerError::Write { kind: WriteError::AggregateExistsCacheError, error_message: msg },

            REGISTER_SCHEMA_ALREADY_EXISTS => ServerError::Schema { kind: SchemaError::AlreadyExists, error_message: msg },
            REGISTER_SCHEMA_INVALID => ServerError::Schema { kind: SchemaError::Invalid, error_message: msg },
            WRITE_SCHEMA_VALIDATION_FAILED => ServerError::Schema { kind: SchemaError::ValidationFailed, error_message: msg },
            WRITE_SCHEMA_COMPILATION_FAILED => ServerError::Schema { kind: SchemaError::CompilationFailed, error_message: msg },
            REGISTER_SCHEMA_UNSUPPORTED_TYPE => ServerError::Schema { kind: SchemaError::UnsupportedType, error_message: msg },
            REGISTER_SCHEMA_CACHE_LOAD_ERROR => ServerError::Schema { kind: SchemaError::CacheLoadError, error_message: msg },
            REGISTER_SCHEMA_FSYNC_ERROR => ServerError::Schema { kind: SchemaError::FsyncError, error_message: msg },
            REGISTER_SCHEMA_CANNOT_ACCEPT_WRITES => ServerError::Schema { kind: SchemaError::CannotAcceptWrites, error_message: msg },
            REGISTER_SCHEMA_REPLICATION_ERROR => ServerError::Schema { kind: SchemaError::ReplicationError, error_message: msg },
            REGISTER_SCHEMA_COORDINATION_FAILED => ServerError::Schema { kind: SchemaError::CoordinationFailed, error_message: msg },

            TRIM_AGGREGATE_NOT_EXISTS => ServerError::Trim { kind: TrimError::AggregateNotExists, error_message: msg },
            TRIM_CACHE_ERROR => ServerError::Trim { kind: TrimError::CacheError, error_message: msg },
            TRIM_REPLICATION_ERROR => ServerError::Trim { kind: TrimError::ReplicationError, error_message: msg },
            TRIM_FSYNC_ERROR => ServerError::Trim { kind: TrimError::FsyncError, error_message: msg },
            TRIM_INDEX_OUT_OF_RANGE => ServerError::Trim { kind: TrimError::IndexOutOfRange, error_message: msg },

            DELETE_AGGREGATE_NOT_EXISTS => ServerError::Delete { kind: DeleteError::AggregateNotExists, error_message: msg },
            DELETE_EMPTY_DELETE_LIST => ServerError::Delete { kind: DeleteError::EmptyDeleteList, error_message: msg },
            DELETE_OPTIMISTIC_CONCURRENCY_VIOLATION => ServerError::Delete {
                kind: DeleteError::OptimisticConcurrencyViolation {
                    expected_version: parse_u64_field(&msg, "expected_version"),
                    current_aggregate_version: parse_u64_field(&msg, "current_aggregate_version"),
                },
                error_message: msg,
            },
            DELETE_CACHE_ERROR => ServerError::Delete { kind: DeleteError::CacheError, error_message: msg },
            DELETE_REPLICATION_ERROR => ServerError::Delete { kind: DeleteError::ReplicationError, error_message: msg },
            DELETE_FSYNC_ERROR => ServerError::Delete { kind: DeleteError::FsyncError, error_message: msg },

            LIST_ORGS_DISK_READ | LIST_AGGREGATE_TYPES_DISK_READ | LIST_AGGREGATES_DISK_READ => {
                ServerError::List { error_code: code, error_message: msg }
            }

            REPLICATION_BATCH_FSYNC | REPLICATION_BATCH_SERIALISE_DATABLOCKS | REPLICATION_BATCH_WAL_SEQ_GAP => {
                ServerError::Replication { error_code: code, error_message: msg }
            }

            EXISTS_CACHE_ERROR => ServerError::Details { kind: DetailsError::CacheError, error_message: msg },
            EXISTS_AGGREGATE_NOT_EXISTS => ServerError::Details { kind: DetailsError::AggregateNotExists, error_message: msg },
            EXISTS_METABLOCK_READ_ERROR => ServerError::Details { kind: DetailsError::MetablockReadError, error_message: msg },

            WATCH_REQUEST_INVALID => ServerError::Watch { kind: WatchError::RequestInvalid, error_message: msg },
            WATCH_LATENCY_TOO_HIGH => ServerError::Watch { kind: WatchError::LatencyTooHigh, error_message: msg },
            WATCH_READ_IO => ServerError::Watch { kind: WatchError::ReadIo, error_message: msg },
            WATCH_READ_SERIALIZATION => ServerError::Watch { kind: WatchError::ReadSerialization, error_message: msg },
            WATCH_READ_OTHER => ServerError::Watch { kind: WatchError::ReadOther, error_message: msg },

            SHARD_ROUTING_NO_KEY | SHARD_ROUTING_MULTIPLE_SHARDS | SHARD_ROUTING_INCOMPATIBLE_FILTERS => {
                ServerError::ShardRouting { error_code: code, error_message: msg }
            }

            SERVER_BUSY => ServerError::Unknown { error_code: code, error_message: msg },

            IDENTIFY_INVALID_NONCE => ServerError::Auth { kind: AuthError::InvalidNonce, error_message: msg },
            IDENTIFY_INVALID_SIGNATURE => ServerError::Auth { kind: AuthError::InvalidSignature, error_message: msg },
            IDENTIFY_MISMATCH => ServerError::Auth { kind: AuthError::Mismatch, error_message: msg },
            IDENTIFY_REQUIRED => ServerError::Auth { kind: AuthError::Required, error_message: msg },
            AUTH_REQUIRED => ServerError::Auth { kind: AuthError::AuthRequired, error_message: msg },
            AUTH_INVALID_KEY => ServerError::Auth { kind: AuthError::InvalidKey, error_message: msg },
            AUTH_INSUFFICIENT_PERMISSIONS => ServerError::Auth { kind: AuthError::InsufficientPermissions, error_message: msg },

            _ => ServerError::Unknown { error_code: code, error_message: msg },
        }
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::UnavailableBatchIndex { .. } => write!(f, "unavailable aggregate version"),
            ReadError::AggregateNotExists => write!(f, "aggregate not exists"),
            ReadError::CacheLoadLockTimeout => write!(f, "cache load lock timeout"),
            ReadError::CacheLoadFileScan => write!(f, "cache load file scan"),
            ReadError::FetchDatablocks => write!(f, "fetch datablocks"),
            ReadError::FetchMetablocks => write!(f, "fetch metablocks"),
        }
    }
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::EmptyEventsList => write!(f, "empty events list"),
            WriteError::ZeroEventType => write!(f, "zero event type"),
            WriteError::ClientIdempotencyViolation { .. } => write!(f, "client idempotency violation"),
            WriteError::OptimisticConcurrencyViolation { .. } => write!(f, "optimistic concurrency violation"),
            WriteError::FailedToSerialiseDatablocks => write!(f, "failed to serialise datablocks"),
            WriteError::AggregateNotExists => write!(f, "aggregate not exists"),
            WriteError::AggregateRecreateNotAllowed => write!(f, "aggregate recreate not allowed"),
            WriteError::ReplicationError => write!(f, "replication error"),
            WriteError::FsyncError => write!(f, "fsync error"),
            WriteError::CacheAggregateClientError => write!(f, "cache aggregate client error"),
            WriteError::AggregateExistsCacheError => write!(f, "aggregate exists cache error"),
        }
    }
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::AlreadyExists => write!(f, "schema already exists"),
            SchemaError::Invalid => write!(f, "schema invalid"),
            SchemaError::ValidationFailed => write!(f, "schema validation failed"),
            SchemaError::CompilationFailed => write!(f, "schema compilation failed"),
            SchemaError::UnsupportedType => write!(f, "schema unsupported type"),
            SchemaError::CacheLoadError => write!(f, "schema cache load error"),
            SchemaError::FsyncError => write!(f, "schema fsync error"),
            SchemaError::CannotAcceptWrites => write!(f, "schema cannot accept writes"),
            SchemaError::ReplicationError => write!(f, "schema replication error"),
            SchemaError::CoordinationFailed => write!(f, "schema coordination failed"),
        }
    }
}

impl std::fmt::Display for DeleteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeleteError::AggregateNotExists => write!(f, "aggregate not exists"),
            DeleteError::EmptyDeleteList => write!(f, "empty delete list"),
            DeleteError::OptimisticConcurrencyViolation { .. } => write!(f, "optimistic concurrency violation"),
            DeleteError::CacheError => write!(f, "cache error"),
            DeleteError::ReplicationError => write!(f, "replication error"),
            DeleteError::FsyncError => write!(f, "fsync error"),
        }
    }
}

impl std::fmt::Display for TrimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrimError::AggregateNotExists => write!(f, "aggregate not exists"),
            TrimError::CacheError => write!(f, "cache error"),
            TrimError::ReplicationError => write!(f, "replication error"),
            TrimError::FsyncError => write!(f, "fsync error"),
            TrimError::IndexOutOfRange => write!(f, "index out of range"),
        }
    }
}

impl std::fmt::Display for WatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WatchError::RequestInvalid => write!(f, "watch request invalid"),
            WatchError::LatencyTooHigh => write!(f, "watch latency too high"),
            WatchError::ReadIo => write!(f, "watch read I/O error"),
            WatchError::ReadSerialization => write!(f, "watch read serialization error"),
            WatchError::ReadOther => write!(f, "watch read error"),
        }
    }
}

impl std::fmt::Display for DetailsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DetailsError::CacheError => write!(f, "cache error"),
            DetailsError::AggregateNotExists => write!(f, "aggregate not exists"),
            DetailsError::MetablockReadError => write!(f, "metablock read error"),
        }
    }
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::InvalidNonce => write!(f, "invalid nonce"),
            AuthError::InvalidSignature => write!(f, "invalid signature"),
            AuthError::Mismatch => write!(f, "identity mismatch"),
            AuthError::Required => write!(f, "identity required"),
            AuthError::AuthRequired => write!(f, "authentication required"),
            AuthError::InvalidKey => write!(f, "invalid API key"),
            AuthError::InsufficientPermissions => write!(f, "insufficient permissions"),
        }
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::Read { kind, error_message } => write!(f, "read error ({kind}): {error_message}"),
            ServerError::Write { kind, error_message } => write!(f, "write error ({kind}): {error_message}"),
            ServerError::Schema { kind, error_message } => write!(f, "schema error ({kind}): {error_message}"),
            ServerError::Delete { kind, error_message } => write!(f, "delete error ({kind}): {error_message}"),
            ServerError::Trim { kind, error_message } => write!(f, "trim error ({kind}): {error_message}"),
            ServerError::Watch { kind, error_message } => write!(f, "watch error ({kind}): {error_message}"),
            ServerError::Details { kind, error_message } => write!(f, "details error ({kind}): {error_message}"),
            ServerError::Auth { kind, error_message } => write!(f, "auth error ({kind}): {error_message}"),
            ServerError::List { error_code, error_message } => write!(f, "list error ({error_code}): {error_message}"),
            ServerError::Replication { error_code, error_message } => write!(f, "replication error ({error_code}): {error_message}"),
            ServerError::ShardRouting { error_code, error_message } => write!(f, "shard routing error ({error_code}): {error_message}"),
            ServerError::Unknown { error_code, error_message } => write!(f, "server error ({error_code}): {error_message}"),
        }
    }
}

impl std::error::Error for ServerError {}
