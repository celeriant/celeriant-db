// Canonical error codes for the Celeriant wire protocol.
// Every error code a client can receive is defined here — no other file should define u32 error codes.
//
// Ranges:
//   1xxx  Read errors
//   2xxx  Write errors (2020–2029: schema registration)
//   3xxx  Trim errors
//   4xxx  Delete errors
//   5xxx  Listing errors
//   6xxx  Replication batch errors
//   7xxx  Exists / aggregate-details errors
//   8xxx  Watch errors
//   9xxx  Shard routing errors
//  10xxx  Identity & authentication errors

// --- Read errors: 1xxx ---
pub const READ_UNAVAILABLE_BATCH_INDEX: u32 = 1000;
pub const READ_AGGREGATE_NOT_EXISTS: u32 = 1001;
pub const READ_CACHE_LOAD_LOCK_TIMEOUT: u32 = 1002;
pub const READ_CACHE_LOAD_FILE_SCAN: u32 = 1003;
pub const READ_FETCH_DATABLOCKS: u32 = 1004;
pub const READ_FETCH_METABLOCKS: u32 = 1005;

// --- Write errors: 2xxx ---
pub const WRITE_EMPTY_EVENTS_LIST: u32 = 2000;
pub const WRITE_ZERO_EVENT_TYPE: u32 = 2001;
pub const WRITE_CLIENT_IDEMPOTENCY_VIOLATION: u32 = 2002;
pub const WRITE_OPTIMISTIC_CONCURRENCY_VIOLATION: u32 = 2003;
pub const WRITE_FAILED_TO_SERIALISE_DATABLOCKS: u32 = 2004;
pub const WRITE_AGGREGATE_NOT_EXISTS: u32 = 2005;
pub const WRITE_AGGREGATE_RECREATE_NOT_ALLOWED: u32 = 2006;
pub const WRITE_REPLICATION_ERROR: u32 = 2007;
pub const WRITE_FSYNC_ERROR: u32 = 2008;
pub const WRITE_CACHE_AGGREGATE_CLIENT_ERROR: u32 = 2009;
pub const WRITE_AGGREGATE_EXISTS_CACHE_ERROR: u32 = 2010;
/// Returned when a write is sent to a non-leader node.
pub const WRITE_NOT_LEADER: u32 = 2011;
pub const REGISTER_SCHEMA_ALREADY_EXISTS: u32 = 2020;
pub const REGISTER_SCHEMA_INVALID: u32 = 2021;
pub const WRITE_SCHEMA_VALIDATION_FAILED: u32 = 2022;
pub const WRITE_SCHEMA_COMPILATION_FAILED: u32 = 2023;
pub const REGISTER_SCHEMA_UNSUPPORTED_TYPE: u32 = 2024;
pub const REGISTER_SCHEMA_CACHE_LOAD_ERROR: u32 = 2025;
pub const REGISTER_SCHEMA_FSYNC_ERROR: u32 = 2026;
pub const REGISTER_SCHEMA_CANNOT_ACCEPT_WRITES: u32 = 2027;
pub const REGISTER_SCHEMA_REPLICATION_ERROR: u32 = 2028;
pub const REGISTER_SCHEMA_COORDINATION_FAILED: u32 = 2029;

// --- Trim errors: 3xxx ---
pub const TRIM_AGGREGATE_NOT_EXISTS: u32 = 3000;
pub const TRIM_CACHE_ERROR: u32 = 3001;
pub const TRIM_REPLICATION_ERROR: u32 = 3002;
pub const TRIM_FSYNC_ERROR: u32 = 3003;
pub const TRIM_INDEX_OUT_OF_RANGE: u32 = 3004;
/// Returned when a trim is sent to a non-leader node.
pub const TRIM_NOT_LEADER: u32 = 3005;

// --- Delete errors: 4xxx ---
pub const DELETE_AGGREGATE_NOT_EXISTS: u32 = 4000;
pub const DELETE_EMPTY_DELETE_LIST: u32 = 4001;
pub const DELETE_OPTIMISTIC_CONCURRENCY_VIOLATION: u32 = 4002;
pub const DELETE_CACHE_ERROR: u32 = 4003;
pub const DELETE_REPLICATION_ERROR: u32 = 4004;
pub const DELETE_FSYNC_ERROR: u32 = 4005;
/// Returned when a delete is sent to a non-leader node.
pub const DELETE_NOT_LEADER: u32 = 4006;

// --- Listing errors: 5xxx ---
pub const LIST_ORGS_DISK_READ: u32 = 5000;
pub const LIST_AGGREGATE_TYPES_DISK_READ: u32 = 5001;
pub const LIST_AGGREGATES_DISK_READ: u32 = 5002;

// --- Replication batch errors: 6xxx ---
pub const REPLICATION_BATCH_FSYNC: u32 = 6000;
pub const REPLICATION_BATCH_SERIALISE_DATABLOCKS: u32 = 6001;
pub const REPLICATION_BATCH_WAL_INDEX_GAP: u32 = 6002;

// --- Exists / aggregate-details errors: 7xxx ---
pub const EXISTS_CACHE_ERROR: u32 = 7000;
pub const EXISTS_AGGREGATE_NOT_EXISTS: u32 = 7001;
pub const EXISTS_METABLOCK_READ_ERROR: u32 = 7002;

// --- Watch errors: 8xxx ---
pub const WATCH_REQUEST_INVALID: u32 = 8000;
pub const WATCH_LATENCY_TOO_HIGH: u32 = 8001;
pub const WATCH_READ_IO: u32 = 8002;
pub const WATCH_READ_SERIALIZATION: u32 = 8003;
pub const WATCH_READ_OTHER: u32 = 8004;

// --- Shard routing errors: 9xxx ---
pub const SHARD_ROUTING_NO_KEY: u32 = 9000;
pub const SHARD_ROUTING_MULTIPLE_SHARDS: u32 = 9001;
pub const SHARD_ROUTING_INCOMPATIBLE_FILTERS: u32 = 9002;

// --- Identity & authentication errors: 10xxx ---
pub const IDENTIFY_INVALID_NONCE: u32 = 10001;
pub const IDENTIFY_INVALID_SIGNATURE: u32 = 10002;
pub const IDENTIFY_MISMATCH: u32 = 10003;
pub const IDENTIFY_REQUIRED: u32 = 10004;
pub const AUTH_REQUIRED: u32 = 10005;
pub const AUTH_INVALID_KEY: u32 = 10006;
pub const AUTH_INSUFFICIENT_PERMISSIONS: u32 = 10007;
