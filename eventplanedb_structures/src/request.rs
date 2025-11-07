use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use crate::{
    batch_metadata_item_pair::BatchMetadataItemPair, compression_type::CompressionType, constants::BINCODE_CONFIG_FIXED, directory_filters::DirectoryFilters, event_item::EventItem, read_filters::ReadFilters, wire_format::{MAX_MESSAGE_SIZE, PROTOCOL_VERSION_V2, WireError, from_wire_format_variable, to_wire_format_variable}
};

const HEADER_SIZE: usize = 13; // version(4) + type(4) + length(4) + compression(1)

// Request type discriminants
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    ListOrganisations = 1,
    ListAggregates = 2,
    Exists = 3,
    Lock = 4,
    Unlock = 5,
    Read = 6,
    ReadAll = 7,
    Write = 8,
    WriteBatches = 9,
    TrimStart = 10,
    TrimEnd = 11,
    Delete = 12,
}

impl RequestType {
    pub fn from_u32(value: u32) -> Result<Self, WireError> {
        match value {
            1 => Ok(RequestType::ListOrganisations),
            2 => Ok(RequestType::ListAggregates),
            3 => Ok(RequestType::Exists),
            4 => Ok(RequestType::Lock),
            5 => Ok(RequestType::Unlock),
            6 => Ok(RequestType::Read),
            7 => Ok(RequestType::ReadAll),
            8 => Ok(RequestType::Write),
            9 => Ok(RequestType::WriteBatches),
            10 => Ok(RequestType::TrimStart),
            11 => Ok(RequestType::TrimEnd),
            12 => Ok(RequestType::Delete),
            _ => Err(WireError::InvalidFormatWithVersion(value)),
        }
    }

    pub fn is_fixed_size(&self) -> bool {
        matches!(
            self,
            RequestType::ListAggregates
                | RequestType::Exists
                | RequestType::Lock
                | RequestType::Unlock
                | RequestType::Read
                | RequestType::ReadAll
                | RequestType::TrimStart
                | RequestType::TrimEnd
                | RequestType::Delete
        )
    }
}

// Individual request structs
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListOrganisationsRequest {
    pub correlation_id: Option<u128>,
    pub filters: DirectoryFilters,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListAggregatesRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: Option<u128>,
    pub filters: DirectoryFilters,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ExistsRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct LockRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub client_id: u128,
    pub timeout_ms: u64,
    pub allow_read: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct UnlockRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub filters: ReadFilters,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadAllRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub filters: ReadFilters,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WriteRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub client_id: u128,
    pub user_id: Option<u128>,
    pub events: Vec<EventItem>,
    pub allow_create: bool,
    pub allow_repair_corruption: bool,
    pub expected_event_batch_index: Option<u64>,
    pub enforce_client_idempotency: bool,
    pub durable_write_with_delay_us: Option<u64>,
    pub compression_type: CompressionType,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WriteBatchesRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub allow_create: bool,
    pub allow_repair_corruption: bool,
    pub durable_write_with_delay_us: Option<u64>,
    pub batches: Vec<BatchMetadataItemPair>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TrimStartRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub keep_from_event_batch_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TrimEndRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub trim_from_event_batch_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeleteRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
}

#[derive(Debug, Clone)]
pub enum Request {
    ListOrganisations(ListOrganisationsRequest),
    ListAggregates(ListAggregatesRequest),
    Exists(ExistsRequest),
    Lock(LockRequest),
    Unlock(UnlockRequest),
    Read(ReadRequest),
    ReadAll(ReadAllRequest),
    Write(WriteRequest),
    WriteBatches(WriteBatchesRequest),
    TrimStart(TrimStartRequest),
    TrimEnd(TrimEndRequest),
    Delete(DeleteRequest),
}

impl Request {
    pub fn request_type(&self) -> RequestType {
        match self {
            Request::ListOrganisations(_) => RequestType::ListOrganisations,
            Request::ListAggregates(_) => RequestType::ListAggregates,
            Request::Exists(_) => RequestType::Exists,
            Request::Lock(_) => RequestType::Lock,
            Request::Unlock(_) => RequestType::Unlock,
            Request::Read(_) => RequestType::Read,
            Request::ReadAll(_) => RequestType::ReadAll,
            Request::Write(_) => RequestType::Write,
            Request::WriteBatches(_) => RequestType::WriteBatches,
            Request::TrimStart(_) => RequestType::TrimStart,
            Request::TrimEnd(_) => RequestType::TrimEnd,
            Request::Delete(_) => RequestType::Delete,
        }
    }

    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate (they go to shard 0).
    pub fn routing_id(&self) -> u128 {
        match self {
            Request::ListOrganisations(_) => 0,
            Request::ListAggregates(_) => 0,
            Request::Exists(req) => req.aggregate_id,
            Request::Lock(req) => req.aggregate_id,
            Request::Unlock(req) => req.aggregate_id,
            Request::Read(req) => req.aggregate_id,
            Request::ReadAll(req) => req.aggregate_id,
            Request::Write(req) => req.aggregate_id,
            Request::WriteBatches(req) => req.aggregate_id,
            Request::TrimStart(req) => req.aggregate_id,
            Request::TrimEnd(req) => req.aggregate_id,
            Request::Delete(req) => req.aggregate_id,
        }
    }
}

/// Read a fixed-size request from the wire
async fn read_fixed_size<R, T>(reader: &mut R, size: u32, buffer: &mut [u8]) -> Result<T, WireError>
where
    R: AsyncReadExt + Unpin,
    T: Decode<()>
{
    reader.read_exact(&mut buffer[..size as usize]).await?;

    bincode::decode_from_slice(&buffer[..size as usize], BINCODE_CONFIG_FIXED)
        .map(|(msg, _)| msg)
        .map_err(|e| WireError::BincodeDecode(e))
}

/// Read a request from the wire protocol
pub async fn read_request<R>(reader: &mut R) -> Result<(Request, u32), WireError>
where
    R: AsyncReadExt + Unpin,
{
    // Always read full header: version(4) + type(4) + length(4) + compression(1)
    let mut header = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header).await?;

    let version = u32::from_be_bytes(header[0..4].try_into().unwrap());
    let request_type_id = u32::from_be_bytes(header[4..8].try_into().unwrap());
    let message_length = u32::from_be_bytes(header[8..12].try_into().unwrap());
    let compression_type = CompressionType::from_tuple(header[12], None);

    if version != PROTOCOL_VERSION_V2 {
        return Err(WireError::UnsupportedVersion(version));
    }

    let request_type = RequestType::from_u32(request_type_id)?;

    let request = if request_type.is_fixed_size() {
        // Single buffer large enough for any fixed-size request (294 bytes = largest)
        let mut buffer = [0u8; 294];
        
        match request_type {
            RequestType::ListAggregates => {
                Request::ListAggregates(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            RequestType::Exists => {
                Request::Exists(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            RequestType::Lock => {
                Request::Lock(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            RequestType::Unlock => {
                Request::Unlock(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            RequestType::Read => {
                Request::Read(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            RequestType::ReadAll => {
                Request::ReadAll(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            RequestType::TrimStart => {
                Request::TrimStart(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            RequestType::TrimEnd => {
                Request::TrimEnd(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            RequestType::Delete => {
                Request::Delete(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            _ => unreachable!(),
        }
    } else {
        match request_type {
            RequestType::ListOrganisations => {
                Request::ListOrganisations(read_variable_size(reader, message_length, compression_type).await?)
            }
            RequestType::Write => {
                Request::Write(read_variable_size(reader, message_length, compression_type).await?)
            }
            RequestType::WriteBatches => {
                Request::WriteBatches(read_variable_size(reader, message_length, compression_type).await?)
            }
            _ => unreachable!(),
        }
    };

    Ok((request, version))
}

/// Write a fixed-size request to the wire
async fn write_fixed_size<W, T>(writer: &mut W, message: &T, request_type: RequestType) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
    T: Encode,
{
    // Stack-allocated buffer: HEADER_SIZE (13) + largest fixed size (ReadRequest/ReadAllRequest = 294)
    let mut buffer = [0u8; HEADER_SIZE + 294];
    
    // Encode message directly into the buffer after the header
    let encoded_len = bincode::encode_into_slice(message, &mut buffer[HEADER_SIZE..], BINCODE_CONFIG_FIXED)
        .map_err(|e| WireError::BincodeEncode(e))?;
    
    // Write header at the beginning of the buffer
    buffer[0..4].copy_from_slice(&PROTOCOL_VERSION_V2.to_be_bytes());
    buffer[4..8].copy_from_slice(&(request_type as u32).to_be_bytes());
    buffer[8..12].copy_from_slice(&0u32.to_be_bytes()); // length = 0 for fixed size
    buffer[12] = 0; // compression = 0 for fixed size
    
    // Write only the used portion (header + actual encoded length)
    writer.write_all(&buffer[..HEADER_SIZE + encoded_len]).await?;

    Ok(())
}

/// Write a variable-size request to the wire
async fn write_variable_size<W, T>(
    writer: &mut W,
    message: &T,
    request_type: RequestType,
    compression_type: CompressionType,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
    T: Encode,
{
    // Encode and compress
    let (_uncompressed_size, encoded) = to_wire_format_variable(message, compression_type)?;

    if encoded.len() > MAX_MESSAGE_SIZE as usize {
        return Err(WireError::MessageTooLarge(encoded.len() as u32));
    }

    // Combine header + payload into single buffer for one write
    let (type_id, _) = compression_type.to_tuple();
    let total_size = HEADER_SIZE + encoded.len();
    let mut buffer = Vec::with_capacity(total_size);

    // Header: version(4) + type(4) + length(4) + compression(1)
    buffer.extend_from_slice(&PROTOCOL_VERSION_V2.to_be_bytes());
    buffer.extend_from_slice(&(request_type as u32).to_be_bytes());
    buffer.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    buffer.push(type_id);
    buffer.extend_from_slice(&encoded);

    writer.write_all(&buffer).await?;

    Ok(())
}

pub async fn write_request<W>(
    writer: &mut W,
    request: &Request,
    compression_type: CompressionType,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    let request_type = request.request_type();

    if request_type.is_fixed_size() {
        // Fixed-size requests - no compression needed
        match request {
            Request::ListAggregates(req) => write_fixed_size(writer, req, request_type).await,
            Request::Exists(req) => write_fixed_size(writer, req, request_type).await,
            Request::Lock(req) => write_fixed_size(writer, req, request_type).await,
            Request::Unlock(req) => write_fixed_size(writer, req, request_type).await,
            Request::Read(req) => write_fixed_size(writer, req, request_type).await,
            Request::ReadAll(req) => write_fixed_size(writer, req, request_type).await,
            Request::TrimStart(req) => write_fixed_size(writer, req, request_type).await,
            Request::TrimEnd(req) => write_fixed_size(writer, req, request_type).await,
            Request::Delete(req) => write_fixed_size(writer, req, request_type).await,
            _ => unreachable!(),
        }
    } else {
        // Variable-size requests - with compression
        match request {
            Request::ListOrganisations(req) => write_variable_size(writer, req, request_type, compression_type).await,
            Request::Write(req) => write_variable_size(writer, req, request_type, compression_type).await,
            Request::WriteBatches(req) => write_variable_size(writer, req, request_type, compression_type).await,
            _ => unreachable!(),
        }
    }
}

/// Read a variable-size request from the wire (length and compression already read in header)
async fn read_variable_size<R, T>(reader: &mut R, message_length: u32, compression_type: CompressionType) -> Result<T, WireError>
where
    R: AsyncReadExt + Unpin,
    T: Decode<()>,
{
    if message_length > MAX_MESSAGE_SIZE {
        return Err(WireError::MessageTooLarge(message_length));
    }

    // Read payload
    let mut payload = vec![0u8; message_length as usize];
    reader.read_exact(&mut payload).await?;

    // Decompress and decode
    from_wire_format_variable(&payload, compression_type, message_length as usize)
        .map_err(|e| WireError::Io(e))
}