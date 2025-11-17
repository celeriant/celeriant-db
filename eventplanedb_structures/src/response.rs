use bincode::{Decode, Encode};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};

use crate::compression_type::CompressionType;
use crate::constants::{PROTOCOL_VERSION_V2, WIRE_FIXED_BODY_SIZE};
use crate::eventplanedb_error::EventPlaneDBError;
use crate::wire_error::WireError;
use crate::wire_header::{WireHeader, write_fixed_size, write_variable_size};
use crate::{
    aggregate_info::AggregateInfo, append_result::AppendResult, organisation::Organisation,
    read_all_result::ReadAllResult, read_result::ReadResult,
};

// Response type discriminants
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseType {
    ListOrganisations = 1,
    ListAggregates = 2,
    Exists = 3,
    Read = 4,
    ReadAll = 5,
    Write = 6,
    WriteBatches = 7,
    TrimStart = 8,
    Delete = 9,
    ProtocolError = 10,
    UpdateCacheLimits = 11,
}

impl ResponseType {
    pub fn from_u32(value: u32) -> Result<Self, WireError> {
        match value {
            1 => Ok(ResponseType::ListOrganisations),
            2 => Ok(ResponseType::ListAggregates),
            3 => Ok(ResponseType::Exists),
            4 => Ok(ResponseType::Read),
            5 => Ok(ResponseType::ReadAll),
            6 => Ok(ResponseType::Write),
            7 => Ok(ResponseType::WriteBatches),
            8 => Ok(ResponseType::TrimStart),
            9 => Ok(ResponseType::Delete),
            10 => Ok(ResponseType::ProtocolError),
            11 => Ok(ResponseType::UpdateCacheLimits),
            _ => Err(WireError::UnknownResponseType(value)),
        }
    }

    pub fn is_fixed_size(&self) -> bool {
        matches!(
            self,
            ResponseType::Exists
                | ResponseType::TrimStart
                | ResponseType::Delete
                | ResponseType::WriteBatches
                | ResponseType::ProtocolError
                | ResponseType::Write
                | ResponseType::UpdateCacheLimits
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct UpdateCacheLimitsResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
    pub accepted: bool,
}

// Individual response structs
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListOrganisationsResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
    pub organisations: Vec<Organisation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ListAggregatesResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
    pub aggregates: Vec<AggregateInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ExistsResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
    pub exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
    pub result: Option<ReadResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadAllResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
    pub result: Option<ReadAllResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WriteResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
    pub result: Option<AppendResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WriteBatchesResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TrimStartResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeleteResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ProtocolErrorResponse {}

#[derive(Debug, Clone)]
pub enum Response {
    ListOrganisations(ListOrganisationsResponse),
    ListAggregates(ListAggregatesResponse),
    Exists(ExistsResponse),
    Read(ReadResponse),
    ReadAll(ReadAllResponse),
    Write(WriteResponse),
    WriteBatches(WriteBatchesResponse),
    TrimStart(TrimStartResponse),
    Delete(DeleteResponse),
    ProtocolError(ProtocolErrorResponse),
    UpdateCacheLimits(UpdateCacheLimitsResponse),
}

impl Response {
    pub fn response_type(&self) -> ResponseType {
        match self {
            Response::ListOrganisations(_) => ResponseType::ListOrganisations,
            Response::ListAggregates(_) => ResponseType::ListAggregates,
            Response::Exists(_) => ResponseType::Exists,
            Response::Read(_) => ResponseType::Read,
            Response::ReadAll(_) => ResponseType::ReadAll,
            Response::Write(_) => ResponseType::Write,
            Response::WriteBatches(_) => ResponseType::WriteBatches,
            Response::TrimStart(_) => ResponseType::TrimStart,
            Response::Delete(_) => ResponseType::Delete,
            Response::ProtocolError(_) => ResponseType::ProtocolError,
            Response::UpdateCacheLimits(_) => ResponseType::UpdateCacheLimits,
        }
    }
}

pub async fn read_response<R>(reader: &mut R) -> Result<Response, WireError>
where
    R: AsyncReadExt + Unpin,
{
    let wire_header = WireHeader::from_reader(reader).await?;

    if wire_header.version != PROTOCOL_VERSION_V2 {
        return Err(WireError::UnsupportedProtocol(wire_header.version));
    }

    let response_type = ResponseType::from_u32(wire_header.message_type)?;

    let response = if response_type.is_fixed_size() {
        // Single buffer large enough for any fixed-size response
        let mut buffer = [0u8; WIRE_FIXED_BODY_SIZE]; // Adjust size based on largest fixed response

        match response_type {
            ResponseType::Exists => {
                Response::Exists(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            ResponseType::TrimStart => {
                Response::TrimStart(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            ResponseType::Delete => {
                Response::Delete(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            ResponseType::WriteBatches => {
                Response::WriteBatches(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            ResponseType::ProtocolError => {
                Response::ProtocolError(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            ResponseType::Write => {
                Response::Write(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            ResponseType::UpdateCacheLimits => {
                Response::UpdateCacheLimits(wire_header.read_fixed_size(reader, &mut buffer).await?)
            }
            _ => unreachable!(),
        }
    } else {
        match response_type {
            ResponseType::ListOrganisations => {
                Response::ListOrganisations(wire_header.read_variable_size(reader, None).await?)
            }
            ResponseType::ListAggregates => {
                Response::ListAggregates(wire_header.read_variable_size(reader, None).await?)
            }
            ResponseType::Read => {
                Response::Read(wire_header.read_variable_size(reader, None).await?)
            }
            ResponseType::ReadAll => {
                Response::ReadAll(wire_header.read_variable_size(reader, None).await?)
            }
            _ => unreachable!(),
        }
    };

    Ok(response)
}

pub async fn write_response<W>(
    writer: &mut W,
    response: &Response,
    compression_type: CompressionType,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    let response_type = response.response_type();
    let response_type_id = response_type as u32;

    if response_type.is_fixed_size() {
        // Fixed-size responses - no compression needed
        match response {
            Response::Exists(res) => write_fixed_size(writer, res, response_type_id).await,
            Response::TrimStart(res) => write_fixed_size(writer, res, response_type_id).await,
            Response::Delete(res) => write_fixed_size(writer, res, response_type_id).await,
            Response::WriteBatches(res) => write_fixed_size(writer, res, response_type_id).await,
            Response::ProtocolError(res) => write_fixed_size(writer, res, response_type_id).await,
            Response::Write(res) => write_fixed_size(writer, res, response_type_id).await,
            Response::UpdateCacheLimits(res) => write_fixed_size(writer, res, response_type_id).await,
            _ => unreachable!(),
        }
    } else {
        // Variable-size responses - with compression
        match response {
            Response::ListOrganisations(res) => {
                write_variable_size(writer, res, response_type_id, compression_type, None).await
            }
            Response::ListAggregates(res) => {
                write_variable_size(writer, res, response_type_id, compression_type, None).await
            }
            Response::Read(res) => {
                write_variable_size(writer, res, response_type_id, compression_type, None).await
            }
            Response::ReadAll(res) => {
                write_variable_size(writer, res, response_type_id, compression_type, None).await
            }
            _ => unreachable!(),
        }
    }
}
