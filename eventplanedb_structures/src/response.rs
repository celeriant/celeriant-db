use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use std::io::Cursor;

use crate::constants::BINCODE_CONFIG_VARIABLE;
use crate::eventplanedb_error::EventPlaneDBError;
use crate::wire_format::{PROTOCOL_VERSION_V1, PROTOCOL_VERSION_V2, STACK_BUFFER_SIZE, WireError};
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

pub async fn read_response<R>(reader: &mut R) -> Result<(Response, u32), WireError>
where
    R: AsyncReadExt + Unpin,
{
    // Read version (4 bytes)
    let mut version_buffer = [0u8; 4];
    reader.read_exact(&mut version_buffer).await?;
    let version = u32::from_be_bytes(version_buffer);

    // Read length (4 bytes)
    let mut length_buffer = [0u8; 4];
    reader.read_exact(&mut length_buffer).await?;
    let message_length = u32::from_be_bytes(length_buffer);

    let message = match version {
        PROTOCOL_VERSION_V1 => read_response_v1(reader, message_length as usize).await,
        PROTOCOL_VERSION_V2 => read_response_v2(reader, message_length as usize).await,
        _ => Err(WireError::UnsupportedVersion(version)),
    }?;

    Ok((message, version))
}

async fn read_response_v1<R>(reader: &mut R, message_length: usize) -> Result<Response, WireError>
where
    R: AsyncReadExt + Unpin,
{
    // Read payload - use stack for small messages, heap for large ones
    let message = if message_length <= STACK_BUFFER_SIZE as usize {
        // Use stack allocation for small messages
        let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];
        let payload_slice = &mut stack_buffer[..message_length];
        reader.read_exact(payload_slice).await?;
        rmp_serde::from_slice(payload_slice)
            .map_err(|_| WireError::InvalidFormatWithVersion(PROTOCOL_VERSION_V1))?
    } else {
        // Use heap allocation for large messages
        let mut payload = vec![0u8; message_length];
        reader.read_exact(&mut payload).await?;
        rmp_serde::from_slice(&payload)
            .map_err(|_| WireError::InvalidFormatWithVersion(PROTOCOL_VERSION_V1))?
    };

    Ok(message)
}

async fn read_response_v2<R>(reader: &mut R, message_length: usize) -> Result<Response, WireError>
where
    R: AsyncReadExt + Unpin,
{
    let message = if message_length <= STACK_BUFFER_SIZE as usize {
        let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];
        reader
            .read_exact(&mut stack_buffer[..message_length])
            .await?;
        bincode::decode_from_slice(&stack_buffer[..message_length], BINCODE_CONFIG_VARIABLE)
            .map_err(|_| WireError::InvalidFormatWithVersion(PROTOCOL_VERSION_V2))?
            .0
    } else {
        let mut heap_buffer = vec![0u8; message_length];
        reader.read_exact(&mut heap_buffer).await?;
        bincode::decode_from_slice(&heap_buffer, BINCODE_CONFIG_VARIABLE)
            .map_err(|_| WireError::InvalidFormatWithVersion(PROTOCOL_VERSION_V2))?
            .0
    };

    Ok(message)
}

pub async fn write_response<W>(
    writer: &mut W,
    message: &Response,
    use_v2: bool,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    let protocol_version = if use_v2 {
        PROTOCOL_VERSION_V2
    } else {
        PROTOCOL_VERSION_V1
    };

    if use_v2 {
        write_response_v2(writer, message, protocol_version).await
    } else {
        write_response_v1(writer, message, protocol_version).await
    }
}

async fn write_response_v1<W>(
    writer: &mut W,
    message: &Response,
    protocol_version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    match message {
        Response::ReadResult { .. } | Response::ReadAllResult { .. } => {
            // ReadResult and ReadAllResult can be large, use heap allocation directly
            let encoded = rmp_serde::to_vec(message)?;

            let header_size = 8;
            let mut combined = Vec::with_capacity(header_size + encoded.len());
            combined.extend_from_slice(&protocol_version.to_be_bytes());
            combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            combined.extend_from_slice(&encoded);

            writer.write_all(&combined).await?;
        }
        _ => {
            // Other responses are typically small, try stack buffer
            let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];
            let mut cursor = Cursor::new(&mut stack_buffer[8..]);

            match rmp_serde::encode::write(&mut cursor, message) {
                Ok(()) => {
                    let encoded_len = cursor.position() as usize;

                    stack_buffer[0..4].copy_from_slice(&protocol_version.to_be_bytes());
                    stack_buffer[4..8].copy_from_slice(&(encoded_len as u32).to_be_bytes());

                    writer.write_all(&stack_buffer[..8 + encoded_len]).await?;
                }
                Err(_) => {
                    // Stack buffer too small, fall back to heap
                    let encoded = rmp_serde::to_vec(message)?;

                    let header_size = 8;
                    let mut combined = Vec::with_capacity(header_size + encoded.len());
                    combined.extend_from_slice(&protocol_version.to_be_bytes());
                    combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
                    combined.extend_from_slice(&encoded);

                    writer.write_all(&combined).await?;
                }
            }
        }
    }

    Ok(())
}

async fn write_response_v2<W>(
    writer: &mut W,
    message: &Response,
    protocol_version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    match message {
        Response::ReadResult { .. } | Response::ReadAllResult { .. } => {
            // ReadResult and ReadAllResult can be large, use heap allocation directly
            let encoded = bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE)?;

            // Combine header and payload in one write
            let header_size = 8;
            let mut combined = Vec::with_capacity(header_size + encoded.len());
            combined.extend_from_slice(&protocol_version.to_be_bytes());
            combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            combined.extend_from_slice(&encoded);

            writer.write_all(&combined).await?;
        }
        _ => {
            // Other responses are typically small, try stack buffer
            let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];

            match bincode::encode_into_slice(
                message,
                &mut stack_buffer[8..],
                BINCODE_CONFIG_VARIABLE,
            ) {
                Ok(encoded_len) => {
                    // Write header directly into stack buffer
                    stack_buffer[0..4].copy_from_slice(&protocol_version.to_be_bytes());
                    stack_buffer[4..8].copy_from_slice(&(encoded_len as u32).to_be_bytes());

                    // Single write with header + payload
                    writer.write_all(&stack_buffer[..8 + encoded_len]).await?;
                }
                Err(_) => {
                    // Stack buffer too small, fall back to heap
                    let encoded = bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE)?;

                    let header_size = 8;
                    let mut combined = Vec::with_capacity(header_size + encoded.len());
                    combined.extend_from_slice(&protocol_version.to_be_bytes());
                    combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
                    combined.extend_from_slice(&encoded);

                    writer.write_all(&combined).await?;
                }
            }
        }
    }

    Ok(())
}