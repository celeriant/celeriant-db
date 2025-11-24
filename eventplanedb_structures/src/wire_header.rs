use bincode::{Decode, Encode};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use serde::Serialize;

use crate::{compression_type::CompressionType, constants::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3, WIRE_FIXED_BODY_SIZE, WIRE_HEADER_SIZE}, wire_error::WireError, wire_format::{from_wire_format_fixed, from_wire_format_fixed_msgpack, from_wire_format_variable, from_wire_format_variable_msgpack, to_wire_format_fixed, to_wire_format_fixed_msgpack, to_wire_format_variable, to_wire_format_variable_msgpack}};

pub async fn write_fixed_size<W, T>(writer: &mut W, message: &T, request_response_type: u32, version: u32) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
    T: Encode + Serialize,
{
    let mut buffer = [0u8; WIRE_HEADER_SIZE + WIRE_FIXED_BODY_SIZE];
    
    // Encode message based on version
    let encoded_len = match version {
        PROTOCOL_VERSION_V2 => to_wire_format_fixed(message, &mut buffer[WIRE_HEADER_SIZE..])?,
        PROTOCOL_VERSION_V3 => to_wire_format_fixed_msgpack(message, &mut buffer[WIRE_HEADER_SIZE..])?,
        _ => return Err(WireError::UnsupportedProtocol(version)),
    };

    let uncompressed_size = encoded_len as u32;
        
    // Write header at the beginning of the buffer
    buffer[0..4].copy_from_slice(&version.to_le_bytes()); // Use the 'version' parameter
    buffer[4..8].copy_from_slice(&request_response_type.to_le_bytes());
    buffer[8..12].copy_from_slice(&uncompressed_size.to_le_bytes());    
    buffer[12..16].copy_from_slice(&uncompressed_size.to_le_bytes());
    buffer[16] = 0; // no compression
    
    // Write only the used portion (header + actual encoded length)
    writer.write_all(&buffer[..WIRE_HEADER_SIZE + encoded_len]).await?;

    Ok(())
}

pub async fn write_variable_size<W, T>(
    writer: &mut W,
    message: &T,
    request_response_type: u32,
    compression_type: CompressionType,
    max_request_size: Option<u32>,
    version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
    T: Encode + Serialize,
{
    // Encode and compress based on version
    let (uncompressed_size, encoded) = match version {
        PROTOCOL_VERSION_V2 => to_wire_format_variable(message, compression_type)?,
        PROTOCOL_VERSION_V3 => to_wire_format_variable_msgpack(message, compression_type)?,
        _ => return Err(WireError::UnsupportedProtocol(version)),
    };
    
    let uncompressed_size = uncompressed_size as u32;
    let compressed_size = encoded.len() as u32;
    let (compression_type_id, _) = compression_type.to_tuple();

    if let Some(max_request_size) = max_request_size && compressed_size > max_request_size {
        return Err(WireError::MessageTooLarge { message_length: compressed_size, max_request_size });
    }
    
    let mut buffer = Vec::with_capacity(WIRE_HEADER_SIZE + encoded.len());
    buffer.extend_from_slice(&version.to_le_bytes());
    buffer.extend_from_slice(&request_response_type.to_le_bytes());
    buffer.extend_from_slice(&(compressed_size).to_le_bytes());    
    buffer.extend_from_slice(&(uncompressed_size).to_le_bytes());
    buffer.extend_from_slice(&(compression_type_id).to_le_bytes());
    buffer.extend_from_slice(&encoded);
    writer.write_all(&buffer).await?;

    Ok(())
}

pub struct WireHeader {
    pub version: u32,
    pub message_type: u32,
    pub compressed_length: u32,    
    pub uncompressed_length: u32,
    pub compression_type: CompressionType,
}

impl WireHeader {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, WireError>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut header = [0u8; WIRE_HEADER_SIZE];
        reader.read_exact(&mut header).await?;

        let version = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let message_type = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let compressed_length = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let uncompressed_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let compression_type = CompressionType::from_tuple(header[16], None);

        Ok(Self {
            version,
            message_type,
            compressed_length,
            uncompressed_length,
            compression_type,
        })
    }

    pub async fn read_variable_size<R, T>(&self, reader: &mut R, max_request_size: Option<u32>) -> Result<T, WireError>
    where
        R: AsyncReadExt + Unpin,
        T: Decode<()> + serde::de::DeserializeOwned,
    {
        if let Some(max_request_size) = max_request_size && self.compressed_length > max_request_size {
            return Err(WireError::MessageTooLarge { message_length: self.compressed_length, max_request_size });
        }

        let compressed_length = self.compressed_length as usize;

        let mut payload = vec![0u8; compressed_length];
        reader.read_exact(&mut payload).await?;

        let obj = match self.version {
            PROTOCOL_VERSION_V2 => from_wire_format_variable(&payload, self.compression_type, compressed_length)?,
            PROTOCOL_VERSION_V3 => from_wire_format_variable_msgpack(&payload, self.compression_type, compressed_length)?,
            _ => return Err(WireError::UnsupportedProtocol(self.version)),
        };

        Ok(obj)
    }

    pub async fn read_fixed_size<R, T>(&self, reader: &mut R, buffer: &mut [u8]) -> Result<T, WireError>
    where
        R: AsyncReadExt + Unpin,
        T: Decode<()> + serde::de::DeserializeOwned,
    {
        let uncompressed_length = self.uncompressed_length as usize;

        if uncompressed_length as usize > buffer.len() {
            return Err(WireError::BufferTooSmall { 
                required: uncompressed_length, 
                available: buffer.len() 
            });
        }

        reader.read_exact(&mut buffer[..uncompressed_length]).await?;

        let obj: T = match self.version {
            PROTOCOL_VERSION_V2 => from_wire_format_fixed(&buffer[..uncompressed_length])?,
            PROTOCOL_VERSION_V3 => from_wire_format_fixed_msgpack(&buffer[..uncompressed_length])?,
            _ => return Err(WireError::UnsupportedProtocol(self.version)),
        };

        Ok(obj)
    }
}