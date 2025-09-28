use bincode::{Decode, Encode, config};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use thiserror::Error;

use crate::protocol::Response;

#[derive(Error, Debug)]
pub enum WireError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),
    #[error("Bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("Bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("Message too large: {0} bytes")]
    MessageTooLarge(u32),
    #[error("Unsupported protocol version: {0}")]
    UnsupportedVersion(u32),
    #[error("Invalid format")]
    InvalidFormat,
}

/// Protocol constants
//TODO: Make configurable - max message size, stack buffer size
const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024; // 64MB max message size
const STACK_BUFFER_SIZE: u32 = 30 * 1024; // 30KB stack buffer threshold
const PROTOCOL_VERSION_V1: u32 = 1;
const PROTOCOL_VERSION_V2: u32 = 2;

/// Bincode configuration for variable-length encoding
const BINCODE_CONFIG_VARIABLE: config::Configuration = config::standard();

//TODO: When we have a large message, we always allocate on the heap, use a buffer pool?

/// Message framing: [version: u32 BE][length: u32 BE][payload: msgpack/bincode bytes]
pub async fn write_message<W>(
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
        write_message_v2(writer, message, protocol_version).await
    } else {
        write_message_v1(writer, message, protocol_version).await
    }
}

async fn write_message_v2<W>(
    writer: &mut W,
    message: &Response,
    protocol_version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    match message {
        Response::ReadFilteredResult(_) => {
            // ReadFilteredResult can be large, use heap allocation directly
            let encoded = bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE)?;

            if encoded.len() > MAX_MESSAGE_SIZE as usize {
                return Err(WireError::MessageTooLarge(encoded.len() as u32));
            }

            // Combine header and payload in one write
            let header_size = 8; // 4 bytes version + 4 bytes length
            let mut combined = Vec::with_capacity(header_size + encoded.len());
            combined.extend_from_slice(&protocol_version.to_be_bytes());
            combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            combined.extend_from_slice(&encoded);

            writer.write_all(&combined).await?;
        }
        _ => {
            // Other responses are typically small, try stack buffer
            let mut stack_buffer = [0u8; 1024]; // 1KB stack buffer

            match bincode::encode_into_slice(
                message,
                &mut stack_buffer[8..],
                BINCODE_CONFIG_VARIABLE,
            ) {
                Ok(encoded_len) => {
                    if encoded_len > MAX_MESSAGE_SIZE as usize {
                        return Err(WireError::MessageTooLarge(encoded_len as u32));
                    }

                    // Write header directly into stack buffer
                    stack_buffer[0..4].copy_from_slice(&protocol_version.to_be_bytes());
                    stack_buffer[4..8].copy_from_slice(&(encoded_len as u32).to_be_bytes());

                    // Single write with header + payload
                    writer.write_all(&stack_buffer[..8 + encoded_len]).await?;
                }
                Err(_) => {
                    // Stack buffer too small, fall back to heap
                    let encoded = bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE)?;

                    if encoded.len() > MAX_MESSAGE_SIZE as usize {
                        return Err(WireError::MessageTooLarge(encoded.len() as u32));
                    }

                    // Combine header and payload in one write
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

async fn write_message_v1<W>(
    writer: &mut W,
    message: &Response,
    protocol_version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    match message {
        Response::ReadFilteredResult(_) => {
            // ReadFilteredResult can be large, use heap allocation directly
            let encoded = rmp_serde::to_vec(message)?;

            if encoded.len() > MAX_MESSAGE_SIZE as usize {
                return Err(WireError::MessageTooLarge(encoded.len() as u32));
            }

            // Combine header and payload in one write
            let header_size = 8; // 4 bytes version + 4 bytes length
            let mut combined = Vec::with_capacity(header_size + encoded.len());
            combined.extend_from_slice(&protocol_version.to_be_bytes());
            combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            combined.extend_from_slice(&encoded);

            writer.write_all(&combined).await?;
        }
        _ => {
            // Other responses are typically small, try stack buffer
            let mut stack_buffer = [0u8; 1024]; // 1KB stack buffer
            let mut cursor = Cursor::new(&mut stack_buffer[8..]); // Start at offset 8 for header space

            match rmp_serde::encode::write(&mut cursor, message) {
                Ok(()) => {
                    let encoded_len = cursor.position() as usize;

                    if encoded_len > MAX_MESSAGE_SIZE as usize {
                        return Err(WireError::MessageTooLarge(encoded_len as u32));
                    }

                    // Write header directly into stack buffer
                    stack_buffer[0..4].copy_from_slice(&protocol_version.to_be_bytes());
                    stack_buffer[4..8].copy_from_slice(&(encoded_len as u32).to_be_bytes());

                    // Single write with header + payload
                    writer.write_all(&stack_buffer[..8 + encoded_len]).await?;
                }
                Err(_) => {
                    // Stack buffer too small, fall back to heap
                    let encoded = rmp_serde::to_vec(message)?;

                    if encoded.len() > MAX_MESSAGE_SIZE as usize {
                        return Err(WireError::MessageTooLarge(encoded.len() as u32));
                    }

                    // Combine header and payload in one write
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

/// Read a message from the wire protocol
pub async fn read_message<T, R>(reader: &mut R) -> Result<(T, bool), WireError>
where
    T: for<'de> Deserialize<'de> + Decode<()>,
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

    if message_length > MAX_MESSAGE_SIZE {
        return Err(WireError::MessageTooLarge(message_length));
    }

    let message = match version {
        PROTOCOL_VERSION_V1 => read_message_v1(reader, message_length as usize).await,
        PROTOCOL_VERSION_V2 => read_message_v2(reader, message_length as usize).await,
        _ => Err(WireError::UnsupportedVersion(version)),
    }?;

    Ok((message, version == PROTOCOL_VERSION_V2))
}

async fn read_message_v1<T, R>(reader: &mut R, message_length: usize) -> Result<T, WireError>
where
    T: for<'de> Deserialize<'de>,
    R: AsyncReadExt + Unpin,
{
    // Read payload - use stack for small messages, heap for large ones
    let message = if message_length <= STACK_BUFFER_SIZE as usize {
        // Use stack allocation for small messages
        let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];
        let payload_slice = &mut stack_buffer[..message_length];
        reader.read_exact(payload_slice).await?;
        rmp_serde::from_slice(payload_slice)?
    } else {
        // Use heap allocation for large messages
        let mut payload = vec![0u8; message_length];
        reader.read_exact(&mut payload).await?;
        rmp_serde::from_slice(&payload)?
    };

    Ok(message)
}

async fn read_message_v2<T, R>(reader: &mut R, message_length: usize) -> Result<T, WireError>
where
    T: Decode<()>,
    R: AsyncReadExt + Unpin,
{
    let message = if message_length <= STACK_BUFFER_SIZE as usize {
        let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];
        reader
            .read_exact(&mut stack_buffer[..message_length])
            .await?;
        bincode::decode_from_slice(&stack_buffer[..message_length], BINCODE_CONFIG_VARIABLE)
            .map_err(|_| WireError::InvalidFormat)?
            .0
    } else {
        let mut heap_buffer = vec![0u8; message_length];
        reader.read_exact(&mut heap_buffer).await?;
        bincode::decode_from_slice(&heap_buffer, BINCODE_CONFIG_VARIABLE)
            .map_err(|_| WireError::InvalidFormat)?
            .0
    };

    Ok(message)
}
