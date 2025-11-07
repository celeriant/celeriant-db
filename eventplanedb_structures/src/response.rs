use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::compression_type::CompressionType;
use crate::eventplanedb_error::EventPlaneDBError;
use crate::wire_format::{MAX_MESSAGE_SIZE, PROTOCOL_VERSION_V2, WireError, from_wire_format_variable, to_wire_format_variable};
use crate::{aggregate_info::AggregateInfo, append_result::AppendResult, constants::BINCODE_CONFIG_FIXED, organisation::Organisation, read_all_result::ReadAllResult, read_result::ReadResult};

const HEADER_SIZE: usize = 9; // version(4) + length(4) + compression(1)

// Response type discriminants
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseType {
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
    ProtocolError = 13,
}

impl ResponseType {
    pub fn from_u32(value: u32) -> Result<Self, WireError> {
        match value {
            1 => Ok(ResponseType::ListOrganisations),
            2 => Ok(ResponseType::ListAggregates),
            3 => Ok(ResponseType::Exists),
            4 => Ok(ResponseType::Lock),
            5 => Ok(ResponseType::Unlock),
            6 => Ok(ResponseType::Read),
            7 => Ok(ResponseType::ReadAll),
            8 => Ok(ResponseType::Write),
            9 => Ok(ResponseType::WriteBatches),
            10 => Ok(ResponseType::TrimStart),
            11 => Ok(ResponseType::TrimEnd),
            12 => Ok(ResponseType::Delete),
            13 => Ok(ResponseType::ProtocolError),
            _ => Err(WireError::InvalidFormatWithVersion(value)),
        }
    }

    pub fn is_fixed_size(&self) -> bool {
        matches!(
            self,
            ResponseType::Exists
                | ResponseType::Lock
                | ResponseType::Unlock
                | ResponseType::TrimStart
                | ResponseType::TrimEnd
                | ResponseType::Delete
                | ResponseType::WriteBatches
                | ResponseType::ProtocolError
                | ResponseType::Write
        )
    }
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
pub struct LockResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct UnlockResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
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
pub struct TrimEndResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeleteResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ProtocolErrorResponse {
    pub correlation_id: Option<u128>,
    pub error: EventPlaneDBError,
}

#[derive(Debug, Clone)]
pub enum Response {
    ListOrganisations(ListOrganisationsResponse),
    ListAggregates(ListAggregatesResponse),
    Exists(ExistsResponse),
    Lock(LockResponse),
    Unlock(UnlockResponse),
    Read(ReadResponse),
    ReadAll(ReadAllResponse),
    Write(WriteResponse),
    WriteBatches(WriteBatchesResponse),
    TrimStart(TrimStartResponse),
    TrimEnd(TrimEndResponse),
    Delete(DeleteResponse),
    ProtocolError(ProtocolErrorResponse),
}

impl Response {
    pub fn response_type(&self) -> ResponseType {
        match self {
            Response::ListOrganisations(_) => ResponseType::ListOrganisations,
            Response::ListAggregates(_) => ResponseType::ListAggregates,
            Response::Exists(_) => ResponseType::Exists,
            Response::Lock(_) => ResponseType::Lock,
            Response::Unlock(_) => ResponseType::Unlock,
            Response::Read(_) => ResponseType::Read,
            Response::ReadAll(_) => ResponseType::ReadAll,
            Response::Write(_) => ResponseType::Write,
            Response::WriteBatches(_) => ResponseType::WriteBatches,
            Response::TrimStart(_) => ResponseType::TrimStart,
            Response::TrimEnd(_) => ResponseType::TrimEnd,
            Response::Delete(_) => ResponseType::Delete,
            Response::ProtocolError(_) => ResponseType::ProtocolError,
        }
    }
}

/// Read a fixed-size response from the wire
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

/// Read a variable-size response from the wire
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

pub async fn read_response<R>(reader: &mut R) -> Result<Response, WireError>
where
    R: AsyncReadExt + Unpin,
{
    // Always read full header: version(4) + type(4) + length(4) + compression(1)
    let mut header = [0u8; 13];
    reader.read_exact(&mut header).await?;

    let version = u32::from_be_bytes(header[0..4].try_into().unwrap());
    let response_type_id = u32::from_be_bytes(header[4..8].try_into().unwrap());
    let message_length = u32::from_be_bytes(header[8..12].try_into().unwrap());
    let compression_type = CompressionType::from_tuple(header[12], None);

    if version != PROTOCOL_VERSION_V2 {
        return Err(WireError::UnsupportedVersion(version));
    }

    let response_type = ResponseType::from_u32(response_type_id)?;

    let response = if response_type.is_fixed_size() {
        // Single buffer large enough for any fixed-size response
        let mut buffer = [0u8; 128]; // Adjust size based on largest fixed response
        
        match response_type {
            ResponseType::Exists => {
                Response::Exists(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            ResponseType::Lock => {
                Response::Lock(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            ResponseType::Unlock => {
                Response::Unlock(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            ResponseType::TrimStart => {
                Response::TrimStart(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            ResponseType::TrimEnd => {
                Response::TrimEnd(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            ResponseType::Delete => {
                Response::Delete(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            ResponseType::WriteBatches => {
                Response::WriteBatches(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            ResponseType::ProtocolError => {
                Response::ProtocolError(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            ResponseType::Write => {
                Response::Write(read_fixed_size(reader, message_length, &mut buffer).await?)
            }
            _ => unreachable!(),
        }
    } else {
        match response_type {
            ResponseType::ListOrganisations => {
                Response::ListOrganisations(read_variable_size(reader, message_length, compression_type).await?)
            }
            ResponseType::ListAggregates => {
                Response::ListAggregates(read_variable_size(reader, message_length, compression_type).await?)
            }
            ResponseType::Read => {
                Response::Read(read_variable_size(reader, message_length, compression_type).await?)
            }
            ResponseType::ReadAll => {
                Response::ReadAll(read_variable_size(reader, message_length, compression_type).await?)
            }
            _ => unreachable!(),
        }
    };

    Ok(response)
}

/// Write a fixed-size response to the wire
async fn write_fixed_size<W, T>(writer: &mut W, message: &T, response_type: ResponseType) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
    T: Encode,
{
    // Stack-allocated buffer: header (13) + largest fixed size (128)
    let mut buffer = [0u8; 13 + 128];
    
    // Encode message directly into the buffer after the header
    let encoded_len = bincode::encode_into_slice(message, &mut buffer[13..], BINCODE_CONFIG_FIXED)
        .map_err(|e| WireError::BincodeEncode(e))?;
    
    // Write header at the beginning of the buffer
    buffer[0..4].copy_from_slice(&PROTOCOL_VERSION_V2.to_be_bytes());
    buffer[4..8].copy_from_slice(&(response_type as u32).to_be_bytes());
    buffer[8..12].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    buffer[12] = 0; // compression = 0 for fixed size
    
    // Write only the used portion (header + actual encoded length)
    writer.write_all(&buffer[..13 + encoded_len]).await?;

    Ok(())
}

/// Write a variable-size response to the wire
async fn write_variable_size<W, T>(
    writer: &mut W,
    message: &T,
    response_type: ResponseType,
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
    let total_size = 13 + encoded.len();
    let mut buffer = Vec::with_capacity(total_size);

    // Header: version(4) + type(4) + length(4) + compression(1)
    buffer.extend_from_slice(&PROTOCOL_VERSION_V2.to_be_bytes());
    buffer.extend_from_slice(&(response_type as u32).to_be_bytes());
    buffer.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    buffer.push(type_id);
    buffer.extend_from_slice(&encoded);

    writer.write_all(&buffer).await?;

    Ok(())
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

    if response_type.is_fixed_size() {
        // Fixed-size responses - no compression needed
        match response {
            Response::Exists(res) => write_fixed_size(writer, res, response_type).await,
            Response::Lock(res) => write_fixed_size(writer, res, response_type).await,
            Response::Unlock(res) => write_fixed_size(writer, res, response_type).await,
            Response::TrimStart(res) => write_fixed_size(writer, res, response_type).await,
            Response::TrimEnd(res) => write_fixed_size(writer, res, response_type).await,
            Response::Delete(res) => write_fixed_size(writer, res, response_type).await,
            Response::WriteBatches(res) => write_fixed_size(writer, res, response_type).await,
            Response::ProtocolError(res) => write_fixed_size(writer, res, response_type).await,
            Response::Write(res) => write_fixed_size(writer, res, response_type).await,
            _ => unreachable!(),
        }
    } else {
        // Variable-size responses - with compression
        match response {
            Response::ListOrganisations(res) => write_variable_size(writer, res, response_type, compression_type).await,
            Response::ListAggregates(res) => write_variable_size(writer, res, response_type, compression_type).await,
            Response::Read(res) => write_variable_size(writer, res, response_type, compression_type).await,
            Response::ReadAll(res) => write_variable_size(writer, res, response_type, compression_type).await,
            _ => unreachable!(),
        }
    }
}