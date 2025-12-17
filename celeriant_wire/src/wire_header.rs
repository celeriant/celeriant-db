use bincode::{Decode, Encode};
use celeriant_wal::compression_type::CompressionType;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use serde::Serialize;

use crate::{
    constants::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3, WIRE_FIXED_BODY_SIZE, WIRE_HEADER_SIZE},
    wire_error::WireError,
    wire_format::{
        from_wire_format_fixed, from_wire_format_fixed_msgpack, from_wire_format_variable,
        from_wire_format_variable_msgpack, to_wire_format_fixed, to_wire_format_fixed_msgpack,
        to_wire_format_variable, to_wire_format_variable_msgpack,
    },
};

/// Represents the header of a wire protocol message.
///
/// The header contains metadata about the message including protocol version,
/// message type, compression information, and payload lengths. This is used
/// to parse and construct messages for network transmission.
pub struct WireHeader {
    pub version: u32,
    pub message_type: u32,
    pub compressed_length: u32,
    pub uncompressed_length: u32,
    pub compression_type: CompressionType,
}

impl WireHeader {
    /// Reads and parses a wire header from an async reader.
    ///
    /// Reads exactly `WIRE_HEADER_SIZE` bytes from the reader and deserializes
    /// the header fields including version, message type, lengths, and compression type.
    ///
    /// # Errors
    /// Returns `WireError` if reading from the stream fails.
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, WireError>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut header = [0u8; WIRE_HEADER_SIZE];
        reader.read_exact(&mut header).await?;

        let version = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);

        // Support both V2 (bincode) and V3 (messagepack)
        match version {
            PROTOCOL_VERSION_V2 | PROTOCOL_VERSION_V3 => {
                // Version is handled inside read_fixed_size/read_variable_size
            }
            _ => return Err(WireError::UnsupportedProtocol(version)),
        }
        
        let message_type = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let compressed_length = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let uncompressed_length =
            u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let compression_type = CompressionType::from_tuple(header[16], None);

        Ok(Self {
            version,
            message_type,
            compressed_length,
            uncompressed_length,
            compression_type,
        })
    }

    /// Reads a variable-size payload from the reader and deserializes it.
    ///
    /// Uses the header's compression and version information to properly
    /// decompress and decode the payload. Supports both bincode (V2) and
    /// msgpack (V3) serialization formats.
    ///
    /// # Errors
    /// - `WireError::MessageTooLarge` if payload exceeds `max_size_bytes`
    /// - `WireError::UnsupportedProtocol` for unknown protocol versions
    pub async fn read_variable_size<R, T>(
        &self,
        reader: &mut R,
        max_size_bytes: Option<u32>,
    ) -> Result<T, WireError>
    where
        R: AsyncReadExt + Unpin,
        T: Decode<()> + serde::de::DeserializeOwned,
    {
        if let Some(max_request_size) = max_size_bytes
            && self.uncompressed_length > max_request_size
        {
            return Err(WireError::MessageTooLarge {
                message_length: self.compressed_length,
                max_request_size,
            });
        }

        let compressed_length = self.compressed_length as usize;
        let uncompressed_length = self.uncompressed_length as usize;

        let mut payload = vec![0u8; compressed_length];
        reader.read_exact(&mut payload).await?;

        let obj = match self.version {
            PROTOCOL_VERSION_V2 => {
                from_wire_format_variable(&payload, self.compression_type, uncompressed_length)?
            }
            PROTOCOL_VERSION_V3 => from_wire_format_variable_msgpack(
                &payload,
                self.compression_type,
                uncompressed_length,
            )?,
            _ => return Err(WireError::UnsupportedProtocol(self.version)),
        };

        Ok(obj)
    }

    /// Reads a fixed-size payload from the reader into the provided buffer.
    ///
    /// The buffer must be large enough to hold `uncompressed_length` bytes.
    /// Supports both bincode (V2) and msgpack (V3) deserialization.
    ///
    /// # Errors
    /// - `WireError::BufferTooSmall` if the buffer is insufficient
    /// - `WireError::UnsupportedProtocol` for unknown protocol versions
    pub async fn read_fixed_size<R, T>(
        &self,
        reader: &mut R,
        buffer: &mut [u8],
    ) -> Result<T, WireError>
    where
        R: AsyncReadExt + Unpin,
        T: Decode<()> + serde::de::DeserializeOwned,
    {
        let uncompressed_length = self.uncompressed_length as usize;

        if uncompressed_length as usize > buffer.len() {
            return Err(WireError::BufferTooSmall {
                required: uncompressed_length,
                available: buffer.len(),
            });
        }

        reader
            .read_exact(&mut buffer[..uncompressed_length])
            .await?;

        let obj: T = match self.version {
            PROTOCOL_VERSION_V2 => from_wire_format_fixed(&buffer[..uncompressed_length])?.0,
            PROTOCOL_VERSION_V3 => from_wire_format_fixed_msgpack(&buffer[..uncompressed_length])?,
            _ => return Err(WireError::UnsupportedProtocol(self.version)),
        };

        Ok(obj)
    }

    /// Writes a fixed-size message with header to the async writer.
    ///
    /// Serializes the message using bincode (V2) or msgpack (V3) based on the
    /// protocol version, prepends the wire header, and writes the complete
    /// frame to the writer. No compression is applied for fixed-size messages.
    ///
    /// # Errors
    /// - `WireError::UnsupportedProtocol` for unknown protocol versions
    pub async fn write_fixed_size<W, T>(
        writer: &mut W,
        message: &T,
        request_response_type: u32,
        protocol_version: u32,
    ) -> Result<(), WireError>
    where
        W: AsyncWriteExt + Unpin,
        T: Encode + Serialize,
    {
        let mut buffer = [0u8; WIRE_HEADER_SIZE + WIRE_FIXED_BODY_SIZE];

        // Encode message based on version
        let encoded_len = match protocol_version {
            PROTOCOL_VERSION_V2 => to_wire_format_fixed(message, &mut buffer[WIRE_HEADER_SIZE..])?,
            PROTOCOL_VERSION_V3 => {
                to_wire_format_fixed_msgpack(message, &mut buffer[WIRE_HEADER_SIZE..])?
            }
            _ => return Err(WireError::UnsupportedProtocol(protocol_version)),
        };

        let uncompressed_size = encoded_len as u32;

        // Write header at the beginning of the buffer
        buffer[0..4].copy_from_slice(&protocol_version.to_le_bytes()); // Use the 'version' parameter
        buffer[4..8].copy_from_slice(&request_response_type.to_le_bytes());
        buffer[8..12].copy_from_slice(&uncompressed_size.to_le_bytes());
        buffer[12..16].copy_from_slice(&uncompressed_size.to_le_bytes());
        buffer[16] = 0; // no compression

        // Write only the used portion (header + actual encoded length)
        writer
            .write_all(&buffer[..WIRE_HEADER_SIZE + encoded_len])
            .await?;

        Ok(())
    }

    /// Writes a variable-size message with header to the async writer.
    ///
    /// Serializes and optionally compresses the message based on the specified
    /// compression type and protocol version. Supports bincode (V2) and
    /// msgpack (V3) serialization formats.
    ///
    /// # Errors
    /// - `WireError::MessageTooLarge` if compressed size exceeds `max_size_bytes`
    /// - `WireError::UnsupportedProtocol` for unknown protocol versions
    pub async fn write_variable_size<W, T>(
        writer: &mut W,
        message: &T,
        request_response_type: u32,
        compression_type: CompressionType,
        max_size_bytes: Option<u32>,
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

        if let Some(max_request_size) = max_size_bytes
            && compressed_size > max_request_size
        {
            return Err(WireError::MessageTooLarge {
                message_length: compressed_size,
                max_request_size,
            });
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::block_on;
    use futures_lite::io::Cursor;

    #[test]
    fn test_write_and_read_fixed_size_v2() {
        block_on(async {
            let message: u64 = 12345;
            let request_type = 1u32;

            let mut buffer = Vec::new();
            WireHeader::write_fixed_size(&mut buffer, &message, request_type, PROTOCOL_VERSION_V2)
                .await
                .unwrap();

            let mut reader = Cursor::new(buffer);
            let header = WireHeader::from_reader(&mut reader).await.unwrap();

            assert_eq!(header.version, PROTOCOL_VERSION_V2);
            assert_eq!(header.message_type, request_type);

            let mut read_buf = [0u8; WIRE_FIXED_BODY_SIZE];
            let decoded: u64 = header
                .read_fixed_size(&mut reader, &mut read_buf)
                .await
                .unwrap();
            assert_eq!(decoded, message);
        });
    }

    #[test]
    fn test_write_and_read_variable_size_v3() {
        block_on(async {
            let message = vec![1u8, 2, 3, 4, 5];
            let request_type = 2u32;

            let mut buffer = Vec::new();
            WireHeader::write_variable_size(
                &mut buffer,
                &message,
                request_type,
                CompressionType::None, // adjust based on your enum
                None,
                PROTOCOL_VERSION_V3,
            )
            .await
            .unwrap();

            let mut reader = Cursor::new(buffer);
            let header = WireHeader::from_reader(&mut reader).await.unwrap();

            assert_eq!(header.version, PROTOCOL_VERSION_V3);
            assert_eq!(header.message_type, request_type);

            let decoded: Vec<u8> = header.read_variable_size(&mut reader, None).await.unwrap();
            assert_eq!(decoded, message);
        });
    }

    #[test]
    fn test_message_too_large_error() {
        block_on(async {
            let message = vec![0u8; 1000];
            let mut buffer = Vec::new();

            let result = WireHeader::write_variable_size(
                &mut buffer,
                &message,
                1,
                CompressionType::None,
                Some(100), // max size smaller than message
                PROTOCOL_VERSION_V2,
            )
            .await;

            assert!(matches!(result, Err(WireError::MessageTooLarge { .. })));
        });
    }

    #[test]
    fn test_unsupported_protocol_error() {
        block_on(async {
            let message: u64 = 123;
            let mut buffer = Vec::new();

            let result = WireHeader::write_fixed_size(&mut buffer, &message, 1, 999).await;

            assert!(matches!(result, Err(WireError::UnsupportedProtocol(999))));
        });
    }

    #[test]
    fn test_buffer_too_small_error() {
        block_on(async {
            let message: u64 = 12345;
            let request_type = 1u32;

            let mut buffer = Vec::new();
            WireHeader::write_fixed_size(&mut buffer, &message, request_type, PROTOCOL_VERSION_V2)
                .await
                .unwrap();

            let mut reader = Cursor::new(buffer);
            let header = WireHeader::from_reader(&mut reader).await.unwrap();

            // Provide a buffer that's too small
            let mut tiny_buf = [0u8; 2];
            let result: Result<u64, _> = header.read_fixed_size(&mut reader, &mut tiny_buf).await;

            assert!(matches!(result, Err(WireError::BufferTooSmall { .. })));
        });
    }
}
