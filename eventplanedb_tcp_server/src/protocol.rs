use bincode::{Decode, Encode};
use eventplanedb_structures::aggregate_info::AggregateInfo;
use eventplanedb_structures::batch_metadata_item_pair::BatchMetadataItemPair;
use eventplanedb_structures::compression_type::CompressionType;
use eventplanedb_structures::directory_filters::DirectoryFilters;
use eventplanedb_structures::organisation::Organisation;
use eventplanedb_structures::read_all_result::ReadAllResult;
use eventplanedb_structures::{read_filters::ReadFilters, read_result::ReadResult, append_result::AppendResult};
use eventplanedb_structures::{event_item::EventItem};
use serde::{Deserialize, Serialize};

use crate::error_code::EventPlaneDBError;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum ErrorCode {
    NotFound,
    AlreadyExists,
    PermissionDenied,
    InvalidArgument,
    OutOfRange,
    Corruption,
    IoError,
    Timeout,
    ResourceExhausted,
    ConcurrencyConflict,
    Internal,
}

/// Wire protocol requests
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum Request {

    /// All organisations (128 bit ids) that are present on this server
    /// It's possible to apply a filter for created or modified times or disk usage
    ListOrganisations {
        correlation_id: Option<u128>,
        filters: DirectoryFilters,
    },

    /// List of aggregates under an organisation and optionally an aggregate type
    /// It's possible to apply a filter for created or modified times or disk usage
    ListAggregates {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: Option<u128>,
        filters: DirectoryFilters,
    },

    /// Confirm if a specific aggregate still exists on this server
    Exists {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },

    /// Attempt to gain exclusive write access to an aggregate for a specific client
    /// All other clients will fail to write, trim or delete the aggregate
    /// Reads are allowed unless allow_read is false
    Lock {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        timeout_ms: u64,
        allow_read: bool,
    },

    /// Unlock an aggregate early before the timeout was to expire
    Unlock {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },

    /// Get event batches for a specific aggregate
    /// A common use is to catch up the stream from a specific event batch index
    /// Filters can be applied to skip batches or filter events within a returned batch
    /// Does not return metadata entries, use ReadAll instead for this purpose.
    Read {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: ReadFilters,
    },

    /// Get event batches for a specific aggregate
    /// Use ReadAll to get both event batches and associated metadata
    /// Commonly used for backup or data tiering purposes, data can be returned to the aggregate via WriteBatches
    /// Be careful with filters as it may exclude batches or events within a batch
    ReadAll {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: ReadFilters,
    },

    /// Appends all events provided as a single batch to the end of the aggregate event stream
    /// If the aggregate does not exist, it will be created if allow_create is true
    /// If idempotent_client is true, events will be filtered using client_event_index and client_id to prevent duplicate events
    /// Set a durable_write_with_delay_us (eg. 200 micro-seconds) to force durable writes (fsync) to disk after the delay
    /// Turning off durable writes will reduce write latency and throughput but will result in data loss if the server crashes
    /// Set allow_repair_corruption to true to automatically trim away any corrupted events at the end of the aggregate (eg. from power loss or bitrot)
    Write {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        allow_create: bool,
        allow_repair_corruption: bool,
        expected_event_batch_index: Option<u64>,
        idempotent_client: bool,
        durable_write_with_delay_us: Option<u64>,
        compression_type: CompressionType,
    },

    /// Adds a batch and metadata without any additional processing
    /// Will either add to front or back of the aggregate, and even create the aggregate if allow_create is true
    /// Will error if adding would result in a gap in the event batch indexing
    /// (it is critical that event batch index is always ordered and continguous)
    WriteBatches {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        allow_create: bool,
        allow_repair_corruption: bool,
        durable_write_with_delay_us: Option<u64>,
        batches: Vec<BatchMetadataItemPair>,
    },

    /// Removes all event batches before keep_from_event_batch_index
    /// Can be used to free up space on disk
    /// If clients try to read from the trimmed batches, they will get an error
    /// It's recommended to use ReadAll to backup the data elsewhere before triming
    TrimStart {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
    },

    /// Throws away recent event batches in an aggregate
    /// Can be used to repair corruption manually or remove invalid events from buggy clients
    TrimEnd {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        trim_from_event_batch_index: u64,
    },

    /// Completely removes the aggregate from the server and clears any in-memory caches for that aggregate
    Delete {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },
}

/// Wire protocol responses
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum Response {
    ListOrganisationsResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
        organisations: Vec<Organisation>,
    },

    ListAggregatesResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
        aggregates: Vec<AggregateInfo>,
    },

    ExistsResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
        exists: Option<AggregateInfo>,
    },

    LockResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
    },

    UnlockResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
    },

    ReadResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
        result: Option<ReadResult>,
    },

    ReadAllResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
        result: Option<ReadAllResult>,
    },

    WriteResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
        result: Option<AppendResult>,
    },

    WriteBatchesResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
    },

    TrimStartResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
    },

    TrimEndResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
    },

    DeleteResult {
        correlation_id: Option<u128>,
        error: Option<EventPlaneDBError>,
    },
}