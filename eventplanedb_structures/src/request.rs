use crate::{
    compression_type::CompressionType, constants::{PROTOCOL_VERSION_V2, WIRE_FIXED_BODY_SIZE}, directory_filters::DirectoryFilters, event_batch_item::EventBatchItem, event_item::EventItem, read_filters::ReadFilters, wire_error::WireError, wire_header::{WireHeader, write_fixed_size, write_variable_size}
};
use bincode::{Decode, Encode};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};

// Request type discriminants
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    ListOrganisations = 1,
    ListAggregates = 2,
    Exists = 3,
    Read = 4,
    Write = 5,
    WriteBatches = 6,
    TrimStart = 7,
    Delete = 8,
    UpdateCacheLimits = 9,
}

impl RequestType {
    pub fn from_u32(value: u32) -> Result<Self, WireError> {
        match value {
            1 => Ok(RequestType::ListOrganisations),
            2 => Ok(RequestType::ListAggregates),
            3 => Ok(RequestType::Exists),
            4 => Ok(RequestType::Read),
            5 => Ok(RequestType::Write),
            6 => Ok(RequestType::WriteBatches),
            7 => Ok(RequestType::TrimStart),
            8 => Ok(RequestType::Delete),
            9 => Ok(RequestType::UpdateCacheLimits),
            _ => Err(WireError::UnknownRequestType(value)),
        }
    }

    pub fn is_fixed_size(&self) -> bool {
        matches!(
            self,
            RequestType::ListAggregates
                | RequestType::Exists
                | RequestType::Read
                | RequestType::TrimStart
                | RequestType::Delete
                | RequestType::UpdateCacheLimits
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct UpdateCacheLimitsRequest {
    pub correlation_id: Option<u128>,
    pub aggregate_write_max_data_cache_size_bytes: u64,
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
pub struct ReadRequest {
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
    pub durable_write_with_delay_us: Option<u64>,
    pub compression_type: CompressionType,
    pub batches: Vec<EventBatchItem>,
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
    Read(ReadRequest),
    Write(WriteRequest),
    WriteBatches(WriteBatchesRequest),
    TrimStart(TrimStartRequest),
    Delete(DeleteRequest),
    UpdateCacheLimits(UpdateCacheLimitsRequest),
}

impl Request {
    pub fn request_type(&self) -> RequestType {
        match self {
            Request::ListOrganisations(_) => RequestType::ListOrganisations,
            Request::ListAggregates(_) => RequestType::ListAggregates,
            Request::Exists(_) => RequestType::Exists,
            Request::Read(_) => RequestType::Read,
            Request::Write(_) => RequestType::Write,
            Request::WriteBatches(_) => RequestType::WriteBatches,
            Request::TrimStart(_) => RequestType::TrimStart,
            Request::Delete(_) => RequestType::Delete,
            Request::UpdateCacheLimits(_) => RequestType::UpdateCacheLimits,
        }
    }

    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate (they go to shard 0).
    pub fn routing_id(&self) -> u128 {
        match self {
            Request::ListOrganisations(_) => 0,
            Request::ListAggregates(_) => 0,
            Request::UpdateCacheLimits(_) => 0,
            Request::Exists(req) => req.aggregate_id,
            Request::Read(req) => req.aggregate_id,
            Request::Write(req) => req.aggregate_id,
            Request::WriteBatches(req) => req.aggregate_id,
            Request::TrimStart(req) => req.aggregate_id,
            Request::Delete(req) => req.aggregate_id,
        }
    }
}

/// Read a request from the wire protocol
pub async fn read_request<R>(
    reader: &mut R,
    max_request_size: Option<u32>,
) -> Result<(Request, u32), WireError>
where
    R: AsyncReadExt + Unpin,
{
    let wire_header = WireHeader::from_reader(reader).await?;

    if wire_header.version != PROTOCOL_VERSION_V2 {
        return Err(WireError::UnsupportedProtocol(wire_header.version));
    }

    let request_type = RequestType::from_u32(wire_header.message_type)?;

    let request = if request_type.is_fixed_size() {
        let mut buffer = [0u8; WIRE_FIXED_BODY_SIZE];

        match request_type {
            RequestType::ListAggregates => {
                Request::ListAggregates(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            RequestType::Exists => {
                Request::Exists(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            RequestType::Read => {
                Request::Read(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            RequestType::TrimStart => {
                Request::TrimStart(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            RequestType::Delete => {
                Request::Delete(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            RequestType::UpdateCacheLimits => {
                Request::UpdateCacheLimits(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            _ => unreachable!(),
        }
    } else {
        match request_type {
            RequestType::ListOrganisations => Request::ListOrganisations(
                wire_header
                    .read_variable_size(reader, max_request_size)
                    .await?,
            ),
            RequestType::Write => Request::Write(
                wire_header
                    .read_variable_size(reader, max_request_size)
                    .await?,
            ),
            RequestType::WriteBatches => Request::WriteBatches(
                wire_header
                    .read_variable_size(reader, max_request_size)
                    .await?,
            ),
            _ => unreachable!(),
        }
    };

    Ok((request, wire_header.version))
}

pub async fn write_request<W>(
    writer: &mut W,
    request: &Request,
    compression_type: CompressionType,
    max_message_size: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    let request_type = request.request_type();
    let request_type_id = request_type as u32;

    if request_type.is_fixed_size() {
        // Fixed-size requests - no compression needed
        match request {
            Request::ListAggregates(req) => write_fixed_size(writer, req, request_type_id).await,
            Request::Exists(req) => write_fixed_size(writer, req, request_type_id).await,
            Request::Read(req) => write_fixed_size(writer, req, request_type_id).await,
            Request::TrimStart(req) => write_fixed_size(writer, req, request_type_id).await,
            Request::Delete(req) => write_fixed_size(writer, req, request_type_id).await,
            Request::UpdateCacheLimits(req) => write_fixed_size(writer, req, request_type_id).await,
            _ => unreachable!(),
        }
    } else {
        // Variable-size requests - with compression
        match request {
            Request::ListOrganisations(req) => {
                write_variable_size(
                    writer,
                    req,
                    request_type_id,
                    compression_type,
                    Some(max_message_size),
                )
                .await
            }
            Request::Write(req) => {
                write_variable_size(
                    writer,
                    req,
                    request_type_id,
                    compression_type,
                    Some(max_message_size),
                )
                .await
            }
            Request::WriteBatches(req) => {
                write_variable_size(
                    writer,
                    req,
                    request_type_id,
                    compression_type,
                    Some(max_message_size),
                )
                .await
            }
            _ => unreachable!(),
        }
    }
}
