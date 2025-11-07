use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::compression_type::CompressionType;
use crate::eventplanedb_error::EventPlaneDBError;
use crate::wire_format::{PROTOCOL_VERSION_V2, WireError, from_wire_format_variable, to_wire_format_variable};
use crate::{aggregate_info::AggregateInfo, append_result::AppendResult, organisation::Organisation, read_all_result::ReadAllResult, read_result::ReadResult};

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
        exists: bool,
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
    
    /// Generic error response for protocol-level errors where we can't 
    /// determine the request type or correlation ID
    ProtocolError {
        correlation_id: Option<u128>,
        error: EventPlaneDBError,
    },
}

pub async fn read_response<R>(reader: &mut R) -> Result<Response, WireError>
where
    R: AsyncReadExt + Unpin,
{
    // Read version (4 bytes)
    let mut version_buffer = [0u8; 4];
    reader.read_exact(&mut version_buffer).await?;
    let version = u32::from_be_bytes(version_buffer);

    if version != PROTOCOL_VERSION_V2 {
        return Err(WireError::UnsupportedVersion(version));
    }

    // Read length (4 bytes)
    let mut length_buffer = [0u8; 4];
    reader.read_exact(&mut length_buffer).await?;
    let message_length = u32::from_be_bytes(length_buffer);

    // Read compression type (1 byte)
    let mut type_id_buffer = [0u8; 1];
    reader.read_exact(&mut type_id_buffer).await?;
    let compression_type = CompressionType::from_tuple(type_id_buffer[0], None);

    // Read payload
    let mut payload = vec![0u8; message_length as usize];
    reader.read_exact(&mut payload).await?;

    // Decompress and decode
    let message = from_wire_format_variable(&payload, compression_type, message_length as usize)?;

    Ok(message)
}

pub async fn write_response<W>(
    writer: &mut W,
    message: &Response,
    compression_type: CompressionType,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    // Encode and compress
    let (_uncompressed_size, encoded) = to_wire_format_variable(message, compression_type)?;

    // Build header: version (4) + length (4) + compression_type (1)
    let (type_id, _) = compression_type.to_tuple();

    let mut combined = Vec::with_capacity(9 + encoded.len());
    combined.extend_from_slice(&PROTOCOL_VERSION_V2.to_be_bytes());
    combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    combined.extend_from_slice(&type_id.to_be_bytes());
    combined.extend_from_slice(&encoded);

    writer.write_all(&combined).await?;

    Ok(())
}