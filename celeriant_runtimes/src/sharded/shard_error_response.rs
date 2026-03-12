use celeriant_msg::{
    error_codes::*,
    process_client_responses::ClientResponse,
    process_cluster_responses::ClusterResponse,
    response::responses::ErrorResponse,
};
use celeriant_shard::error::{
    fetch_datablock_error::FetchDatablockError,
    follower_replication_write_error::FollowerReplicationWriteError,
    replication_error::ReplicationError,
    replication_rollback_failure::ReplicationRollbackFailure,
    shard_cache_load_error::ShardCacheLoadError,
    shard_delete_error::ShardDeleteError,
    shard_error::ShardError,
    shard_exists_error::ShardAggregateDetailsError,
    shard_fsync_error::ShardFsyncError,
    shard_listing_error::ShardListingError,
    shard_read_error::ShardReadError,
    shard_schema_error::ShardSchemaError,
    shard_trim_error::ShardTrimError,
    shard_write_error::ShardWriteError,
    watch_session_error::WatchSessionError,
};
use celeriant_watch::aggregate_reader::WatchReadError;

use super::connection_handler::ShardRoutingError;

pub fn shard_error_to_client_response(correlation_id: Option<u128>, error: ShardError) -> ClientResponse {
    let (error_code, error_message) = shard_error_to_code(error);
    ClientResponse::GenericError(ErrorResponse { correlation_id, error_code, error_message })
}

pub fn shard_error_to_cluster_response(correlation_id: Option<u128>, error: ShardError) -> ClusterResponse {
    let (error_code, error_message) = shard_error_to_code(error);
    ClusterResponse::GenericError(ErrorResponse { correlation_id, error_code, error_message })
}

fn shard_error_to_code(error: ShardError) -> (u32, String) {
    match error {
        ShardError::Read(e) => read_error(e),
        ShardError::Write(e) => write_error(e),
        ShardError::TrimStart(e) => trim_error(e),
        ShardError::Delete(e) => delete_error(e),
        ShardError::ListOrgs(e) => listing_error(LIST_ORGS_DISK_READ, e),
        ShardError::ListAggregateTypes(e) => listing_error(LIST_AGGREGATE_TYPES_DISK_READ, e),
        ShardError::ListAggregates(e) => listing_error(LIST_AGGREGATES_DISK_READ, e),
        ShardError::ReplicationBatch(e) => replication_batch_error(e),
        ShardError::AggregateDetails(e) => exists_error(e),
        ShardError::WatchRequestInvalid => (WATCH_REQUEST_INVALID, "{}".into()),
        ShardError::RegisterSchema(e) => register_schema_error(e),
    }
}

pub fn watch_session_error_to_client_response(correlation_id: Option<u128>, error: WatchSessionError) -> ClientResponse {
    let (error_code, error_message) = match error {
        WatchSessionError::WatchLatencyTooHigh { latency_ms, max_latency_ms } => (
            WATCH_LATENCY_TOO_HIGH,
            format!(r#"{{"requested_ms":{},"max_ms":{}}}"#, latency_ms, max_latency_ms),
        ),
    };
    ClientResponse::GenericError(ErrorResponse { correlation_id, error_code, error_message })
}

pub fn watch_read_error_to_client_response(correlation_id: Option<u128>, error: WatchReadError) -> ClientResponse {
    let (error_code, error_message) = match error {
        WatchReadError::Io(msg) => (WATCH_READ_IO, format!(r#"{{"detail":{}}}"#, json_string(&msg))),
        WatchReadError::Serialization(e) => (WATCH_READ_SERIALIZATION, format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e)))),
        WatchReadError::Other(msg) => (WATCH_READ_OTHER, format!(r#"{{"detail":{}}}"#, json_string(&msg))),
    };
    ClientResponse::GenericError(ErrorResponse { correlation_id, error_code, error_message })
}

pub fn shard_routing_error_to_code(error: ShardRoutingError) -> (u32, String) {
    match error {
        ShardRoutingError::NoRoutingKeyProvided => (SHARD_ROUTING_NO_KEY, "{}".into()),
        ShardRoutingError::MultipleShardRoutes { num_shards } => (
            SHARD_ROUTING_MULTIPLE_SHARDS,
            format!(r#"{{"num_shards":{}}}"#, num_shards),
        ),
        ShardRoutingError::IncompatibleFilters { detail, num_shards } => (
            SHARD_ROUTING_INCOMPATIBLE_FILTERS,
            format!(r#"{{"detail":{},"num_shards":{}}}"#, json_string(&detail), num_shards),
        ),
    }
}

fn read_error(e: ShardReadError) -> (u32, String) {
    match e {
        ShardReadError::UnavailableBatchIndex { minimum_available, requested } => (
            READ_UNAVAILABLE_BATCH_INDEX,
            format!(r#"{{"requested":{},"minimum_available":{}}}"#, requested, minimum_available),
        ),
        ShardReadError::AggregateNotExists => (READ_AGGREGATE_NOT_EXISTS, "{}".into()),
        ShardReadError::ShardCacheLoadError(e) => cache_load_error(READ_CACHE_LOAD_LOCK_TIMEOUT, READ_CACHE_LOAD_FILE_SCAN, e),
        ShardReadError::FetchDatablocksError(e) => (READ_FETCH_DATABLOCKS, fetch_datablock_message(e)),
        ShardReadError::FetchMetablocksError(e) => (READ_FETCH_METABLOCKS, format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e)))),
    }
}

fn write_error(e: ShardWriteError) -> (u32, String) {
    match e {
        ShardWriteError::EmptyEventsList => (WRITE_EMPTY_EVENTS_LIST, "{}".into()),
        ShardWriteError::ZeroEventType { client_event_index } => (
            WRITE_ZERO_EVENT_TYPE,
            format!(r#"{{"client_event_index":{}}}"#, client_event_index),
        ),
        ShardWriteError::ClientIdempotencyViolation { last_client_event_index, attempted_client_event_index } => (
            WRITE_CLIENT_IDEMPOTENCY_VIOLATION,
            format!(r#"{{"last_client_event_index":{},"attempted_client_event_index":{}}}"#, last_client_event_index, attempted_client_event_index),
        ),
        ShardWriteError::OptimisticConcurrencyViolation { expected_event_batch_index, current_event_batch_index } => (
            WRITE_OPTIMISTIC_CONCURRENCY_VIOLATION,
            format!(r#"{{"expected_event_batch_index":{},"current_event_batch_index":{}}}"#, expected_event_batch_index, current_event_batch_index),
        ),
        ShardWriteError::FailedToSerialiseDatablocks(e) => (
            WRITE_FAILED_TO_SERIALISE_DATABLOCKS,
            format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e))),
        ),
        ShardWriteError::AggregateNotExists => (WRITE_AGGREGATE_NOT_EXISTS, "{}".into()),
        ShardWriteError::AggregateRecreateNotAllowed => (WRITE_AGGREGATE_RECREATE_NOT_ALLOWED, "{}".into()),
        ShardWriteError::ReplicationError(e) => (WRITE_REPLICATION_ERROR, replication_message(e)),
        ShardWriteError::ShardFsyncError(e) => (WRITE_FSYNC_ERROR, fsync_message(e)),
        ShardWriteError::CacheAggregateClientError(e) => cache_load_error(WRITE_CACHE_AGGREGATE_CLIENT_ERROR, WRITE_CACHE_AGGREGATE_CLIENT_ERROR, e),
        ShardWriteError::AggregateExistsAndCacheError(e) => cache_load_error(WRITE_AGGREGATE_EXISTS_CACHE_ERROR, WRITE_AGGREGATE_EXISTS_CACHE_ERROR, e),
        ShardWriteError::ShardCannotAcceptWrites { leader_address } => (WRITE_NOT_LEADER, cannot_accept_writes_message(leader_address)),
        ShardWriteError::SchemaValidationFailed { event_type_major, event_type_minor, client_event_index, validation_error } => (
            WRITE_SCHEMA_VALIDATION_FAILED,
            format!(
                r#"{{"event_type_major":{},"event_type_minor":{},"client_event_index":{},"validation_error":{}}}"#,
                event_type_major,
                event_type_minor,
                client_event_index,
                json_string(&validation_error)
            ),
        ),
        ShardWriteError::SchemaCompilationFailed { event_type_major, event_type_minor, client_event_index, compilation_error } => (
            WRITE_SCHEMA_COMPILATION_FAILED,
            format!(
                r#"{{"event_type_major":{},"event_type_minor":{},"client_event_index":{},"compilation_error":{}}}"#,
                event_type_major,
                event_type_minor,
                client_event_index,
                json_string(&compilation_error)
            ),
        ),
    }
}

fn trim_error(e: ShardTrimError) -> (u32, String) {
    match e {
        ShardTrimError::AggregateNotExists => (TRIM_AGGREGATE_NOT_EXISTS, "{}".into()),
        ShardTrimError::AggregateExistsAndCacheError(e) => cache_load_error(TRIM_CACHE_ERROR, TRIM_CACHE_ERROR, e),
        ShardTrimError::ReplicationError(e) => (TRIM_REPLICATION_ERROR, replication_message(e)),
        ShardTrimError::ShardFsyncError(e) => (TRIM_FSYNC_ERROR, fsync_message(e)),
        ShardTrimError::TrimIndexOutOfRange { requested, max_event_batch_index } => (
            TRIM_INDEX_OUT_OF_RANGE,
            format!(r#"{{"requested":{},"max_event_batch_index":{}}}"#, requested, max_event_batch_index),
        ),
        ShardTrimError::ShardCannotAcceptWrites { leader_address } => (TRIM_NOT_LEADER, cannot_accept_writes_message(leader_address)),
    }
}

fn delete_error(e: ShardDeleteError) -> (u32, String) {
    match e {
        ShardDeleteError::AggregateNotExists => (DELETE_AGGREGATE_NOT_EXISTS, "{}".into()),
        ShardDeleteError::EmptyDeleteList => (DELETE_EMPTY_DELETE_LIST, "{}".into()),
        ShardDeleteError::OptimisticConcurrencyViolation { expected_event_batch_index, current_event_batch_index } => (
            DELETE_OPTIMISTIC_CONCURRENCY_VIOLATION,
            format!(r#"{{"expected_event_batch_index":{},"current_event_batch_index":{}}}"#, expected_event_batch_index, current_event_batch_index),
        ),
        ShardDeleteError::AggregateExistsAndCacheError(e) => cache_load_error(DELETE_CACHE_ERROR, DELETE_CACHE_ERROR, e),
        ShardDeleteError::ReplicationError(e) => (DELETE_REPLICATION_ERROR, replication_message(e)),
        ShardDeleteError::ShardFsyncError(e) => (DELETE_FSYNC_ERROR, fsync_message(e)),
        ShardDeleteError::ShardCannotAcceptWrites { leader_address } => (DELETE_NOT_LEADER, cannot_accept_writes_message(leader_address)),
    }
}

fn register_schema_error(e: ShardSchemaError) -> (u32, String) {
    match e {
        ShardSchemaError::SchemaAlreadyExists { event_type_major, event_type_minor } => (
            REGISTER_SCHEMA_ALREADY_EXISTS,
            format!(r#"{{"event_type_major":{},"event_type_minor":{}}}"#, event_type_major, event_type_minor),
        ),
        ShardSchemaError::InvalidSchema { schema_type, parse_error } => (
            REGISTER_SCHEMA_INVALID,
            format!(r#"{{"schema_type":{},"parse_error":{}}}"#, schema_type, json_string(&parse_error)),
        ),
        ShardSchemaError::UnsupportedSchemaType { schema_type } => (
            REGISTER_SCHEMA_UNSUPPORTED_TYPE,
            format!(r#"{{"schema_type":{}}}"#, schema_type),
        ),
        ShardSchemaError::ShardCannotAcceptWrites { leader_address } => (
            REGISTER_SCHEMA_CANNOT_ACCEPT_WRITES,
            cannot_accept_writes_message(leader_address),
        ),
        ShardSchemaError::CacheLoadError(e) => cache_load_error(REGISTER_SCHEMA_CACHE_LOAD_ERROR, REGISTER_SCHEMA_CACHE_LOAD_ERROR, e),
        ShardSchemaError::FsyncError(e) => (REGISTER_SCHEMA_FSYNC_ERROR, fsync_message(e)),
        ShardSchemaError::ReplicationError(e) => (REGISTER_SCHEMA_REPLICATION_ERROR, replication_message(e)),
        ShardSchemaError::SchemaCoordinationFailed { failed_shard_count, total_shards } => (
            REGISTER_SCHEMA_COORDINATION_FAILED,
            format!(r#"{{"failed_shard_count":{},"total_shards":{}}}"#, failed_shard_count, total_shards),
        ),
    }
}

fn listing_error(code: u32, e: ShardListingError) -> (u32, String) {
    match e {
        ShardListingError::ReadFromDiskError(scan) => (
            code,
            format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", scan))),
        ),
        ShardListingError::ListSemaphoreClosed => (
            code,
            r#"{"detail":"list semaphore closed"}"#.to_string(),
        ),
    }
}

fn replication_batch_error(e: FollowerReplicationWriteError) -> (u32, String) {
    match e {
        FollowerReplicationWriteError::ShardFSyncError(e) => (REPLICATION_BATCH_FSYNC, fsync_message(e)),
        FollowerReplicationWriteError::FailedToSerialiseDatablocks(e) => (
            REPLICATION_BATCH_SERIALISE_DATABLOCKS,
            format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e))),
        ),
        FollowerReplicationWriteError::BlockBecameInline => (
            REPLICATION_BATCH_SERIALISE_DATABLOCKS,
            r#"{"detail":"leader Block storage became Inline on follower re-serialization"}"#.to_string(),
        ),
        FollowerReplicationWriteError::BatchWalIndexGap { index, expected, actual } => (
            REPLICATION_BATCH_WAL_INDEX_GAP,
            format!(r#"{{"index":{index},"expected":{expected},"actual":{actual}}}"#),
        ),
    }
}

fn exists_error(e: ShardAggregateDetailsError) -> (u32, String) {
    match e {
        ShardAggregateDetailsError::AggregateExistsAndCacheError(e) => cache_load_error(EXISTS_CACHE_ERROR, EXISTS_CACHE_ERROR, e),
        ShardAggregateDetailsError::AggregateNotExists => (EXISTS_AGGREGATE_NOT_EXISTS, "{}".into()),
        ShardAggregateDetailsError::MetablockReadError(detail) => (EXISTS_METABLOCK_READ_ERROR, format!(r#"{{"detail":{}}}"#, json_string(&detail))),
    }
}

fn cache_load_error(lock_code: u32, scan_code: u32, e: ShardCacheLoadError) -> (u32, String) {
    match e {
        ShardCacheLoadError::AggregateLoadingLockTimeout => (lock_code, "{}".into()),
        ShardCacheLoadError::FileScanningError(scan) => (
            scan_code,
            format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", scan))),
        ),
    }
}

fn replication_message(e: ReplicationError) -> String {
    match e {
        ReplicationError::RollbackInProgress => format!(r#"{{"detail":{}}}"#, "RollbackInProgress"),
        ReplicationError::RollbackFailed(f) => format!(r#"{{"detail":{}}}"#, json_string(&rollback_detail(&f))),
        ReplicationError::ReplicationClientLockTimeoutError => format!(r#"{{"detail":{}}}"#, "ReplicationClientLockTimeoutError"),
        ReplicationError::ReplicateToS3Error(e) => format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e))),
        ReplicationError::ExtendedCatchupFailure(e) => format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e))),
    }
}

fn rollback_detail(f: &ReplicationRollbackFailure) -> String {
    match f {
        ReplicationRollbackFailure::FsyncAmortisedBatchLockTimeout => "fsync amortised batch lock timeout".into(),
        ReplicationRollbackFailure::WriteLockTimeout { log_id } => format!("write lock timeout, log_id={}", log_id),
        ReplicationRollbackFailure::LogSegmentFileUnavailable { log_id } => format!("log segment file unavailable, log_id={}", log_id),
        ReplicationRollbackFailure::WriteDualHeaderError { source, log_id } => format!("write dual header error, log_id={}, source={:?}", log_id, source),
        ReplicationRollbackFailure::HeaderFsyncFailed { log_id } => format!("header fsync failed, log_id={}", log_id),
        ReplicationRollbackFailure::UnableToReadDatablocksCarryOver { source, log_id } => format!("unable to read datablocks carry over, log_id={}, source={}", log_id, source),
    }
}

fn fsync_message(e: ShardFsyncError) -> String {
    match e {
        ShardFsyncError::DatablocksCarryOverBufferNotPresent => "{a}".into(),
        ShardFsyncError::RollbackInvalidatedWrites => "{b}".into(),
        ShardFsyncError::BatchesTooLarge { preallocate_bytes } => format!(r#"{{"preallocate_bytes":{}}}"#, preallocate_bytes),
        ShardFsyncError::UnableToRotateToNewLogSegmentFile(e) => format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e))),
        ShardFsyncError::ActiveWriteFileUnavailable => "{c}".into(),
        ShardFsyncError::WriteLockTimeout => "{d}".into(),
        ShardFsyncError::MetablockSerialisationError(msg) => format!(r#"{{"detail":{}}}"#, json_string(&msg)),
        ShardFsyncError::WriteMetablocksError(msg) => format!(r#"{{"detail":{}}}"#, json_string(&msg)),
        ShardFsyncError::LogSegmentFileHeaderWriteFailure(e) => format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e))),
        ShardFsyncError::FDataSyncError(msg) => format!(r#"{{"detail":{}}}"#, json_string(&msg)),
        ShardFsyncError::WriteDatablocksError(msg) => format!(r#"{{"detail":{}}}"#, json_string(&msg)),
    }
}

fn fetch_datablock_message(e: FetchDatablockError) -> String {
    match e {
        FetchDatablockError::DatablockError { log_id, wal_index, source, is_inline } => format!(
            r#"{{"log_id":{},"wal_index":{},"is_inline":{},"detail":{}}}"#,
            log_id, wal_index, is_inline, json_string(&format!("{:?}", source))
        ),
        FetchDatablockError::LogSegmentFileError(e) => format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e))),
        FetchDatablockError::LogSegmentFileReaderContention => "{}".into(),
        FetchDatablockError::LogSegmentFileUnavailable { log_id } => format!(r#"{{"log_id":{}}}"#, log_id),
        FetchDatablockError::DatablockReadError(msg) => format!(r#"{{"detail":{}}}"#, json_string(&msg)),
        FetchDatablockError::MissingDatablocksOnDisk => "{}".into(),
    }
}

fn cannot_accept_writes_message(leader_address: Option<String>) -> String {
    match leader_address {
        Some(addr) => format!(r#"{{"leader_address":{}}}"#, json_string(&addr)),
        None => "{}".into(),
    }
}

/// Escape a string for safe JSON embedding.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str(r#"\""#),
            '\\' => out.push_str(r#"\\"#),
            '\n' => out.push_str(r#"\n"#),
            '\r' => out.push_str(r#"\r"#),
            '\t' => out.push_str(r#"\t"#),
            c if c < '\x20' => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_string_escapes_quotes() {
        assert_eq!(json_string(r#"hello "world""#), r#""hello \"world\"""#);
    }

    #[test]
    fn json_string_escapes_backslash() {
        assert_eq!(json_string(r#"a\b"#), r#""a\\b""#);
    }

    #[test]
    fn json_string_escapes_control_chars() {
        assert_eq!(json_string("a\nb"), r#""a\nb""#);
        assert_eq!(json_string("a\tb"), r#""a\tb""#);
    }

    #[test]
    fn read_aggregate_not_exists_response() {
        let resp = shard_error_to_client_response(Some(42), ShardError::Read(ShardReadError::AggregateNotExists));
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, READ_AGGREGATE_NOT_EXISTS);
                assert_eq!(e.error_message, "{}");
                assert_eq!(e.correlation_id, Some(42));
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn write_occ_violation_response() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation {
                expected_event_batch_index: 5,
                current_event_batch_index: 7,
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, WRITE_OPTIMISTIC_CONCURRENCY_VIOLATION);
                assert_eq!(
                    e.error_message,
                    r#"{"expected_event_batch_index":5,"current_event_batch_index":7}"#
                );
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn delete_occ_violation_response() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::Delete(ShardDeleteError::OptimisticConcurrencyViolation {
                expected_event_batch_index: 1,
                current_event_batch_index: 3,
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, DELETE_OPTIMISTIC_CONCURRENCY_VIOLATION);
                assert_eq!(
                    e.error_message,
                    r#"{"expected_event_batch_index":1,"current_event_batch_index":3}"#
                );
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn trim_out_of_range_response() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::TrimStart(ShardTrimError::TrimIndexOutOfRange {
                requested: 100,
                max_event_batch_index: 50,
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, TRIM_INDEX_OUT_OF_RANGE);
                assert_eq!(
                    e.error_message,
                    r#"{"requested":100,"max_event_batch_index":50}"#
                );
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn watch_latency_too_high_response() {
        let resp = watch_session_error_to_client_response(
            Some(99),
            WatchSessionError::WatchLatencyTooHigh { latency_ms: 5000, max_latency_ms: 1000 },
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, WATCH_LATENCY_TOO_HIGH);
                assert_eq!(
                    e.error_message,
                    r#"{"requested_ms":5000,"max_ms":1000}"#
                );
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn read_unavailable_batch_index_response() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::Read(ShardReadError::UnavailableBatchIndex { minimum_available: 10, requested: 5 }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, READ_UNAVAILABLE_BATCH_INDEX);
                assert_eq!(
                    e.error_message,
                    r#"{"requested":5,"minimum_available":10}"#
                );
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn write_idempotency_violation_response() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::Write(ShardWriteError::ClientIdempotencyViolation {
                last_client_event_index: 10,
                attempted_client_event_index: 8,
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, WRITE_CLIENT_IDEMPOTENCY_VIOLATION);
                assert_eq!(
                    e.error_message,
                    r#"{"last_client_event_index":10,"attempted_client_event_index":8}"#
                );
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn watch_request_invalid_response() {
        let resp = shard_error_to_client_response(None, ShardError::WatchRequestInvalid);
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, WATCH_REQUEST_INVALID);
                assert_eq!(e.error_message, "{}");
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn write_cannot_accept_writes_with_leader_address() {
        let resp = shard_error_to_client_response(
            Some(1),
            ShardError::Write(ShardWriteError::ShardCannotAcceptWrites {
                leader_address: Some("10.0.0.1:9000".into()),
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, WRITE_NOT_LEADER);
                assert_eq!(e.error_message, r#"{"leader_address":"10.0.0.1:9000"}"#);
                assert_eq!(e.correlation_id, Some(1));
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn write_cannot_accept_writes_without_leader_address() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::Write(ShardWriteError::ShardCannotAcceptWrites {
                leader_address: None,
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, WRITE_NOT_LEADER);
                assert_eq!(e.error_message, "{}");
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn delete_cannot_accept_writes_with_leader_address() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::Delete(ShardDeleteError::ShardCannotAcceptWrites {
                leader_address: Some("10.0.0.2:9000".into()),
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, DELETE_NOT_LEADER);
                assert_eq!(e.error_message, r#"{"leader_address":"10.0.0.2:9000"}"#);
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn trim_cannot_accept_writes_with_leader_address() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::TrimStart(ShardTrimError::ShardCannotAcceptWrites {
                leader_address: Some("10.0.0.3:9000".into()),
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, TRIM_NOT_LEADER);
                assert_eq!(e.error_message, r#"{"leader_address":"10.0.0.3:9000"}"#);
            }
            _ => panic!("expected GenericError"),
        }
    }

    fn assert_schema_error(error: ShardSchemaError, expected_code: u32, expected_json_fragment: &str) {
        let resp = shard_error_to_client_response(None, ShardError::RegisterSchema(error));
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, expected_code, "wrong error code");
                assert!(
                    e.error_message.contains(expected_json_fragment),
                    "error_message {:?} missing {:?}", e.error_message, expected_json_fragment,
                );
            }
            other => panic!("expected GenericError, got {other:?}"),
        }
    }

    #[test]
    fn schema_already_exists_response() {
        assert_schema_error(
            ShardSchemaError::SchemaAlreadyExists { event_type_major: 5, event_type_minor: 2 },
            REGISTER_SCHEMA_ALREADY_EXISTS,
            r#""event_type_major":5,"event_type_minor":2"#,
        );
    }

    #[test]
    fn schema_invalid_response() {
        assert_schema_error(
            ShardSchemaError::InvalidSchema { schema_type: 0, parse_error: "bad parse".into() },
            REGISTER_SCHEMA_INVALID,
            r#""schema_type":0"#,
        );
    }

    #[test]
    fn schema_unsupported_type_response() {
        assert_schema_error(
            ShardSchemaError::UnsupportedSchemaType { schema_type: 1 },
            REGISTER_SCHEMA_UNSUPPORTED_TYPE,
            r#""schema_type":1"#,
        );
    }

    #[test]
    fn schema_cannot_accept_writes_response() {
        assert_schema_error(
            ShardSchemaError::ShardCannotAcceptWrites { leader_address: Some("10.0.0.1:9000".into()) },
            REGISTER_SCHEMA_CANNOT_ACCEPT_WRITES,
            r#""leader_address":"10.0.0.1:9000""#,
        );
    }

    #[test]
    fn schema_coordination_failed_response() {
        assert_schema_error(
            ShardSchemaError::SchemaCoordinationFailed { failed_shard_count: 2, total_shards: 4 },
            REGISTER_SCHEMA_COORDINATION_FAILED,
            r#""failed_shard_count":2,"total_shards":4"#,
        );
    }

    #[test]
    fn write_schema_validation_failed_response() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::Write(ShardWriteError::SchemaValidationFailed {
                event_type_major: 1,
                event_type_minor: 0,
                client_event_index: 7,
                validation_error: "missing field".into(),
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, WRITE_SCHEMA_VALIDATION_FAILED);
                assert!(e.error_message.contains(r#""client_event_index":7"#));
            }
            other => panic!("expected GenericError, got {other:?}"),
        }
    }

    #[test]
    fn write_schema_compilation_failed_response() {
        let resp = shard_error_to_client_response(
            None,
            ShardError::Write(ShardWriteError::SchemaCompilationFailed {
                event_type_major: 1,
                event_type_minor: 0,
                client_event_index: 3,
                compilation_error: "bad schema".into(),
            }),
        );
        match resp {
            ClientResponse::GenericError(e) => {
                assert_eq!(e.error_code, WRITE_SCHEMA_COMPILATION_FAILED);
                assert!(e.error_message.contains(r#""client_event_index":3"#));
            }
            other => panic!("expected GenericError, got {other:?}"),
        }
    }
}
