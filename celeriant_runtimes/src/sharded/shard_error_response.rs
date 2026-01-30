use celeriant_msg::{
    process_responses::Response,
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
    shard_exists_error::ShardExistsError,
    shard_fsync_error::ShardFsyncError,
    shard_listing_error::ShardListingError,
    shard_read_error::ShardReadError,
    shard_trim_error::ShardTrimError,
    shard_write_error::ShardWriteError,
    watch_session_error::WatchSessionError,
};
use celeriant_watch::aggregate_reader::WatchReadError;

// Error codes as a cross-language u32 enum.
// Each ShardError leaf variant gets a unique code.

// Read errors: 1xxx
const READ_UNAVAILABLE_BATCH_INDEX: u32 = 1000;
const READ_AGGREGATE_NOT_EXISTS: u32 = 1001;
const READ_CACHE_LOAD_LOCK_TIMEOUT: u32 = 1002;
const READ_CACHE_LOAD_FILE_SCAN: u32 = 1003;
const READ_FETCH_DATABLOCKS: u32 = 1004;
const READ_FETCH_METABLOCKS: u32 = 1005;

// Write errors: 2xxx
const WRITE_EMPTY_EVENTS_LIST: u32 = 2000;
const WRITE_ZERO_EVENT_TYPE: u32 = 2001;
const WRITE_CLIENT_IDEMPOTENCY_VIOLATION: u32 = 2002;
const WRITE_OPTIMISTIC_CONCURRENCY_VIOLATION: u32 = 2003;
const WRITE_FAILED_TO_SERIALISE_DATABLOCKS: u32 = 2004;
const WRITE_AGGREGATE_NOT_EXISTS: u32 = 2005;
const WRITE_AGGREGATE_RECREATE_NOT_ALLOWED: u32 = 2006;
const WRITE_REPLICATION_ERROR: u32 = 2007;
const WRITE_FSYNC_ERROR: u32 = 2008;
const WRITE_CACHE_AGGREGATE_CLIENT_ERROR: u32 = 2009;
const WRITE_AGGREGATE_EXISTS_CACHE_ERROR: u32 = 2010;
const WRITE_INVALID_LEASE_INDEX: u32 = 2011;

// Trim errors: 3xxx
const TRIM_AGGREGATE_NOT_EXISTS: u32 = 3000;
const TRIM_CACHE_ERROR: u32 = 3001;
const TRIM_REPLICATION_ERROR: u32 = 3002;
const TRIM_FSYNC_ERROR: u32 = 3003;
const TRIM_INDEX_OUT_OF_RANGE: u32 = 3004;
const TRIM_INVALID_LEASE_INDEX: u32 = 3005;

// Delete errors: 4xxx
const DELETE_AGGREGATE_NOT_EXISTS: u32 = 4000;
const DELETE_EMPTY_DELETE_LIST: u32 = 4001;
const DELETE_OPTIMISTIC_CONCURRENCY_VIOLATION: u32 = 4002;
const DELETE_CACHE_ERROR: u32 = 4003;
const DELETE_REPLICATION_ERROR: u32 = 4004;
const DELETE_FSYNC_ERROR: u32 = 4005;
const DELETE_INVALID_LEASE_INDEX: u32 = 4006;

// Listing errors: 5xxx
const LIST_ORGS_DISK_READ: u32 = 5000;
const LIST_AGGREGATE_TYPES_DISK_READ: u32 = 5001;
const LIST_AGGREGATES_DISK_READ: u32 = 5002;

// Replication batch errors: 6xxx
const REPLICATION_BATCH_FSYNC: u32 = 6000;
const REPLICATION_BATCH_SERIALISE_DATABLOCKS: u32 = 6001;

// Exists errors: 7xxx
const EXISTS_CACHE_ERROR: u32 = 7000;

// Watch errors: 8xxx
const WATCH_REQUEST_INVALID: u32 = 8000;
const WATCH_LATENCY_TOO_HIGH: u32 = 8001;
const WATCH_READ_IO: u32 = 8002;
const WATCH_READ_SERIALIZATION: u32 = 8003;
const WATCH_READ_OTHER: u32 = 8004;

// Catch-up errors: 9xxx
const CATCHUP_REQUEST_INVALID: u32 = 9000;

pub fn shard_error_to_response(correlation_id: Option<u128>, error: ShardError) -> Response {
    let (error_code, error_message) = match error {
        ShardError::Read(e) => read_error(e),
        ShardError::Write(e) => write_error(e),
        ShardError::TrimStart(e) => trim_error(e),
        ShardError::Delete(e) => delete_error(e),
        ShardError::ListOrgs(e) => listing_error(LIST_ORGS_DISK_READ, e),
        ShardError::ListAggregateTypes(e) => listing_error(LIST_AGGREGATE_TYPES_DISK_READ, e),
        ShardError::ListAggregates(e) => listing_error(LIST_AGGREGATES_DISK_READ, e),
        ShardError::ReplicationBatch(e) => replication_batch_error(e),
        ShardError::Exists(e) => exists_error(e),
        ShardError::WatchRequestInvalid => (WATCH_REQUEST_INVALID, "{}".into()),
        ShardError::CatchUpRequestInvalid => (CATCHUP_REQUEST_INVALID, "{}".into()),
    };
    Response::GenericError(ErrorResponse { correlation_id, error_code, error_message })
}

pub fn watch_session_error_to_response(correlation_id: Option<u128>, error: WatchSessionError) -> Response {
    let (error_code, error_message) = match error {
        WatchSessionError::WatchLatencyTooHigh { latency_ms, max_latency_ms } => (
            WATCH_LATENCY_TOO_HIGH,
            format!(r#"{{"requested_ms":{},"max_ms":{}}}"#, latency_ms, max_latency_ms),
        ),
    };
    Response::GenericError(ErrorResponse { correlation_id, error_code, error_message })
}

pub fn watch_read_error_to_response(correlation_id: Option<u128>, error: WatchReadError) -> Response {
    let (error_code, error_message) = match error {
        WatchReadError::Io(msg) => (WATCH_READ_IO, format!(r#"{{"detail":{}}}"#, json_string(&msg))),
        WatchReadError::Serialization(e) => (WATCH_READ_SERIALIZATION, format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e)))),
        WatchReadError::Other(msg) => (WATCH_READ_OTHER, format!(r#"{{"detail":{}}}"#, json_string(&msg))),
    };
    Response::GenericError(ErrorResponse { correlation_id, error_code, error_message })
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
        ShardWriteError::InvalidLeaseIndex => (WRITE_INVALID_LEASE_INDEX, "{}".into()),
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
        ShardTrimError::InvalidLeaseIndex => (TRIM_INVALID_LEASE_INDEX, "{}".into()),
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
        ShardDeleteError::InvalidLeaseIndex => (DELETE_INVALID_LEASE_INDEX, "{}".into()),
    }
}

fn listing_error(code: u32, e: ShardListingError) -> (u32, String) {
    match e {
        ShardListingError::ReadFromDiskError(scan) => (
            code,
            format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", scan))),
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
    }
}

fn exists_error(e: ShardExistsError) -> (u32, String) {
    match e {
        ShardExistsError::AggregateExistsAndCacheError(e) => cache_load_error(EXISTS_CACHE_ERROR, EXISTS_CACHE_ERROR, e),
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
        ReplicationError::RollbackInProgress => "{}".into(),
        ReplicationError::RollbackFailed(f) => format!(r#"{{"detail":{}}}"#, json_string(&rollback_detail(&f))),
        ReplicationError::ReplicationClientLockTimeoutError => "{}".into(),
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
        ShardFsyncError::DatablocksCarryOverBufferNotPresent => "{}".into(),
        ShardFsyncError::RollbackInvalidatedWrites => "{}".into(),
        ShardFsyncError::BatchesTooLarge { preallocate_bytes } => format!(r#"{{"preallocate_bytes":{}}}"#, preallocate_bytes),
        ShardFsyncError::UnableToRotateToNewLogSegmentFile(e) => format!(r#"{{"detail":{}}}"#, json_string(&format!("{:?}", e))),
        ShardFsyncError::ActiveWriteFileUnavailable => "{}".into(),
        ShardFsyncError::WriteLockTimeout => "{}".into(),
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
        let resp = shard_error_to_response(Some(42), ShardError::Read(ShardReadError::AggregateNotExists));
        match resp {
            Response::GenericError(e) => {
                assert_eq!(e.error_code, READ_AGGREGATE_NOT_EXISTS);
                assert_eq!(e.error_message, "{}");
                assert_eq!(e.correlation_id, Some(42));
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn write_occ_violation_response() {
        let resp = shard_error_to_response(
            None,
            ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation {
                expected_event_batch_index: 5,
                current_event_batch_index: 7,
            }),
        );
        match resp {
            Response::GenericError(e) => {
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
        let resp = shard_error_to_response(
            None,
            ShardError::Delete(ShardDeleteError::OptimisticConcurrencyViolation {
                expected_event_batch_index: 1,
                current_event_batch_index: 3,
            }),
        );
        match resp {
            Response::GenericError(e) => {
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
        let resp = shard_error_to_response(
            None,
            ShardError::TrimStart(ShardTrimError::TrimIndexOutOfRange {
                requested: 100,
                max_event_batch_index: 50,
            }),
        );
        match resp {
            Response::GenericError(e) => {
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
        let resp = watch_session_error_to_response(
            Some(99),
            WatchSessionError::WatchLatencyTooHigh { latency_ms: 5000, max_latency_ms: 1000 },
        );
        match resp {
            Response::GenericError(e) => {
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
        let resp = shard_error_to_response(
            None,
            ShardError::Read(ShardReadError::UnavailableBatchIndex { minimum_available: 10, requested: 5 }),
        );
        match resp {
            Response::GenericError(e) => {
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
        let resp = shard_error_to_response(
            None,
            ShardError::Write(ShardWriteError::ClientIdempotencyViolation {
                last_client_event_index: 10,
                attempted_client_event_index: 8,
            }),
        );
        match resp {
            Response::GenericError(e) => {
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
        let resp = shard_error_to_response(None, ShardError::WatchRequestInvalid);
        match resp {
            Response::GenericError(e) => {
                assert_eq!(e.error_code, WATCH_REQUEST_INVALID);
                assert_eq!(e.error_message, "{}");
            }
            _ => panic!("expected GenericError"),
        }
    }

    #[test]
    fn catchup_request_invalid_response() {
        let resp = shard_error_to_response(None, ShardError::CatchUpRequestInvalid);
        match resp {
            Response::GenericError(e) => {
                assert_eq!(e.error_code, CATCHUP_REQUEST_INVALID);
                assert_eq!(e.error_message, "{}");
            }
            _ => panic!("expected GenericError"),
        }
    }
}
