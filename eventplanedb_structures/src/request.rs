use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use crate::{batch_metadata_item_pair::BatchMetadataItemPair, compression_type::CompressionType, directory_filters::DirectoryFilters, event_item::EventItem, read_filters::ReadFilters, wire_format::{MAX_MESSAGE_SIZE, PROTOCOL_VERSION_V2, WireError, from_wire_format_variable, to_wire_format_variable}};

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
        enforce_client_idempotency: bool,
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

impl Request {
    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate (they go to shard 0).
    pub fn routing_id(&self) -> u128 {
        match self {
            Request::ListOrganisations { .. } => 0,
            Request::ListAggregates { .. } => 0,
            Request::Exists { aggregate_id, .. }
            | Request::Lock { aggregate_id, .. }
            | Request::Unlock { aggregate_id, .. }
            | Request::Read { aggregate_id, .. }
            | Request::ReadAll { aggregate_id, .. }
            | Request::Write { aggregate_id, .. }
            | Request::WriteBatches { aggregate_id, .. }
            | Request::TrimStart { aggregate_id, .. }
            | Request::TrimEnd { aggregate_id, .. }
            | Request::Delete { aggregate_id, .. } => *aggregate_id,
        }
    }
}
/// Read a request from the wire protocol
pub async fn read_request<R>(reader: &mut R) -> Result<(Request, u32), WireError>
where
    R: AsyncReadExt + Unpin,
{
    // Read entire header (9 bytes: 4 version + 4 length + 1 compression)
    let mut header_buffer = [0u8; 9];
    reader.read_exact(&mut header_buffer).await?;
    
    let version = u32::from_be_bytes(header_buffer[0..4].try_into().unwrap());
    let message_length = u32::from_be_bytes(header_buffer[4..8].try_into().unwrap());
    let compression_type = CompressionType::from_tuple(header_buffer[8], None);

    if version != PROTOCOL_VERSION_V2 {
        return Err(WireError::UnsupportedVersion(version));
    }

    if message_length > MAX_MESSAGE_SIZE {
        return Err(WireError::MessageTooLarge(message_length));
    }

    // Read payload
    let mut payload = vec![0u8; message_length as usize];
    reader.read_exact(&mut payload).await?;

    // Decompress and decode
    let message = from_wire_format_variable(&payload, compression_type, message_length as usize)?;

    Ok((message, PROTOCOL_VERSION_V2))
}

pub async fn write_request<W>(
    writer: &mut W,
    message: &Request,
    compression_type: CompressionType,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    // Encode and compress
    let (_uncompressed_size, encoded) = to_wire_format_variable(message, compression_type)?;

    if encoded.len() > MAX_MESSAGE_SIZE as usize {
        return Err(WireError::MessageTooLarge(encoded.len() as u32));
    }

    // Build header: version (4) + length (4) + compression_type (1)
    let (type_id, _) = compression_type.to_tuple();

    let mut header = Vec::with_capacity(9);
    header.extend_from_slice(&PROTOCOL_VERSION_V2.to_be_bytes());
    header.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    header.push(type_id);

    // Write header and payload
    writer.write_all(&header).await?;
    writer.write_all(&encoded).await?;

    Ok(())
}