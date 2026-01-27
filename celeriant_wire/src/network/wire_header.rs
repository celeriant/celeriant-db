use bincode::{Decode, Encode};
use celeriant_wal::compression_type::CompressionType;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use serde::Serialize;

use crate::{codec, network::wire_error::WireError};

const PROTOCOL_VERSION_V2: u32 = 2;
const PROTOCOL_VERSION_V3: u32 = 3;
const WIRE_HEADER_SIZE: usize = 17;
pub const WIRE_FIXED_BODY_SIZE: usize = 1024 - WIRE_HEADER_SIZE;

#[derive(Debug)]
pub struct WireHeader {
    pub version: u32,
    pub message_type: u32,
    pub compressed_length: u32,
    pub uncompressed_length: u32,
    pub compression_type: CompressionType,
}

impl WireHeader {
    /// Reads and parses a wire header from an async reader.
    pub async fn from_reader<R>(reader: &mut R, max_size_bytes: u64) -> Result<Self, WireError>
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

        // Validate BOTH compressed and uncompressed lengths to prevent:
        // 1. Memory exhaustion from large compressed payloads
        // 2. Decompression bombs (small compressed, huge uncompressed)
        if compressed_length as u64 > max_size_bytes {
            return Err(WireError::MessageTooLarge {
                message_length: compressed_length as u64,
                max_size_bytes,
            });
        }

        if uncompressed_length as u64 > max_size_bytes {
            return Err(WireError::MessageTooLarge {
                message_length: uncompressed_length as u64,
                max_size_bytes,
            });
        }

        Ok(Self {
            version,
            message_type,
            compressed_length,
            uncompressed_length,
            compression_type,
        })
    }

    /// Reads compressed_length from an async reader and deserializes
    /// the variable-size payload into the specified type.
    pub async fn read_variable_size<R, T>(
        &self,
        reader: &mut R,
    ) -> Result<T, WireError>
    where
        R: AsyncReadExt + Unpin,
        T: Decode<()> + serde::de::DeserializeOwned,
    {
        // We can use stack for smaller uncompressed messages
        if self.compressed_length <= WIRE_FIXED_BODY_SIZE as u32 && self.compression_type == CompressionType::None {
            return self.read_fixed_size(reader).await;
        }

        let mut payload = vec![0u8; self.compressed_length as usize];
        reader.read_exact(&mut payload).await?;
        
        let uncompressed_length = self.uncompressed_length as usize;
        let obj = match self.version {
            PROTOCOL_VERSION_V2 => {

                // Save the extra heap allocation if no compression
                if self.compression_type == CompressionType::None {
                    codec::bincode::fixed_deserialise(&payload)?
                }

                // Decompress and deserialize
                let decompressed = codec::compression::decompress(
                    &payload,
                    self.compression_type,
                    uncompressed_length,
                )?;

                codec::bincode::fixed_deserialise(&decompressed)?
            }
            PROTOCOL_VERSION_V3 => {

                // Save the extra heap allocation if no compression
                if self.compression_type == CompressionType::None {
                    codec::msgpack::deserialise(&payload)?
                }

                // Decompress and deserialize
                let decompressed = codec::compression::decompress(
                    &payload,
                    self.compression_type,
                    uncompressed_length,
                )?;

                codec::msgpack::deserialise(&decompressed)?
            },
            _ => return Err(WireError::UnsupportedProtocol(self.version)),
        };

        Ok(obj)
    }

    /// Reads a fixed-size payload from the reader into the provided buffer.
    pub async fn read_fixed_size<R, T>(
        &self,
        reader: &mut R,
    ) -> Result<T, WireError>
    where
        R: AsyncReadExt + Unpin,
        T: Decode<()> + serde::de::DeserializeOwned,
    {
        let mut buffer = [0u8; WIRE_FIXED_BODY_SIZE];

        if self.compressed_length as usize > WIRE_FIXED_BODY_SIZE {
            return Err(WireError::MessageTooLarge {
                message_length: self.compressed_length as u64,
                max_size_bytes: WIRE_FIXED_BODY_SIZE as u64,
            });
        }

        reader
            .read_exact(&mut buffer[..self.uncompressed_length as usize])
            .await?;

        let obj: T = match self.version {
            PROTOCOL_VERSION_V2 => codec::bincode::fixed_deserialise(&buffer)?,
            PROTOCOL_VERSION_V3 => codec::msgpack::deserialise(&buffer)?,
            _ => return Err(WireError::UnsupportedProtocol(self.version)),
        };

        Ok(obj)
    }
}

/// Writes a fixed-size message with header to the async writer.
///
/// Serializes the message using bincode (V2) or msgpack (V3) based on the
/// protocol version, prepends the wire header, and writes the complete
/// frame to the writer. No compression is applied for fixed-size messages.
pub async fn wire_header_write_fixed_size<W, T>(
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

    let body_size = match protocol_version {
        PROTOCOL_VERSION_V2 => {
            codec::bincode::fixed_serialise_stack(message, &mut buffer[WIRE_HEADER_SIZE..])?
        }
        PROTOCOL_VERSION_V3 => {
            codec::msgpack::serialise_stack(message, &mut buffer[WIRE_HEADER_SIZE..])?
        }
        _ => return Err(WireError::UnsupportedProtocol(protocol_version)),
    };

    buffer[0..4].copy_from_slice(&protocol_version.to_le_bytes());
    buffer[4..8].copy_from_slice(&request_response_type.to_le_bytes());
    buffer[8..12].copy_from_slice(&(body_size as u32).to_le_bytes());
    buffer[12..16].copy_from_slice(&(body_size as u32).to_le_bytes());
    buffer[16] = 0;

    writer.write_all(&buffer).await?;

    Ok(())
}

/// Writes a variable-size message with header to the async writer.
///
/// Serializes and optionally compresses the message based on the specified
/// compression type and protocol version. Supports bincode (V2) and
/// msgpack (V3) serialization formats.
pub async fn wire_header_write_variable_size<W, T>(
    writer: &mut W,
    message: &T,
    request_response_type: u32,
    compression_type: CompressionType,
    max_size_bytes: u64,
    protocol_version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
    T: Encode + Serialize,
{
    // Encode and compress based on version
    let uncompressed_data = match protocol_version {
        PROTOCOL_VERSION_V2 => codec::bincode::fixed_serialise_heap(message)?,
        PROTOCOL_VERSION_V3 => codec::msgpack::serialise_heap(message)?,
        _ => return Err(WireError::UnsupportedProtocol(protocol_version)),
    };

    let uncompressed_size = uncompressed_data.len();

    let data = if compression_type == CompressionType::None {
        uncompressed_data
    } else {
        codec::compression::compress(&uncompressed_data, compression_type)?
    };

    let uncompressed_size = uncompressed_size as u32;
    let compressed_size = data.len() as u32;
    let (compression_type_id, _) = compression_type.to_tuple();

    if compressed_size as u64 > max_size_bytes
    {
        return Err(WireError::MessageTooLarge {
            message_length: compressed_size as u64,
            max_size_bytes,
        });
    }

    if compressed_size <= WIRE_FIXED_BODY_SIZE as u32 && compression_type == CompressionType::None {
        let mut buffer = [0u8; WIRE_HEADER_SIZE + WIRE_FIXED_BODY_SIZE];

        buffer[0..4].copy_from_slice(&protocol_version.to_le_bytes());
        buffer[4..8].copy_from_slice(&request_response_type.to_le_bytes());
        buffer[8..12].copy_from_slice(&compressed_size.to_le_bytes());
        buffer[12..16].copy_from_slice(&uncompressed_size.to_le_bytes());
        buffer[16] = compression_type_id;
        buffer[WIRE_HEADER_SIZE..WIRE_HEADER_SIZE + compressed_size as usize].copy_from_slice(&data);

        writer.write_all(&buffer[..WIRE_HEADER_SIZE + compressed_size as usize]).await?;

        return Ok(());
    }

    let mut buffer = Vec::with_capacity(WIRE_HEADER_SIZE + data.len());
    buffer.extend_from_slice(&protocol_version.to_le_bytes());
    buffer.extend_from_slice(&request_response_type.to_le_bytes());
    buffer.extend_from_slice(&compressed_size.to_le_bytes());
    buffer.extend_from_slice(&uncompressed_size.to_le_bytes());
    buffer.extend_from_slice(&compression_type_id.to_le_bytes());
    buffer.extend_from_slice(&data);

    writer.write_all(&buffer).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::block_on;
    use futures_lite::io::Cursor;

    const MAX_SIZE: u64 = 64 * 1024;

    fn make_header(version: u32, msg_type: u32, compressed: u32, uncompressed: u32, compression: u8) -> Vec<u8> {
        let mut h = Vec::with_capacity(WIRE_HEADER_SIZE);
        h.extend_from_slice(&version.to_le_bytes());
        h.extend_from_slice(&msg_type.to_le_bytes());
        h.extend_from_slice(&compressed.to_le_bytes());
        h.extend_from_slice(&uncompressed.to_le_bytes());
        h.push(compression);
        h
    }

    async fn roundtrip_fixed<T>(msg: &T, msg_type: u32, version: u32) -> T
    where
        T: Encode + Serialize + Decode<()> + serde::de::DeserializeOwned + std::fmt::Debug,
    {
        let mut buf = Vec::new();
        wire_header_write_fixed_size(&mut buf, msg, msg_type, version).await.unwrap();
        let mut reader = Cursor::new(buf);
        let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap();
        assert_eq!(header.version, version);
        assert_eq!(header.message_type, msg_type);
        header.read_fixed_size(&mut reader).await.unwrap()
    }

    async fn roundtrip_variable<T>(msg: &T, msg_type: u32, compression: CompressionType, version: u32) -> T
    where
        T: Encode + Serialize + Decode<()> + serde::de::DeserializeOwned + std::fmt::Debug,
    {
        let mut buf = Vec::new();
        wire_header_write_variable_size(&mut buf, msg, msg_type, compression, MAX_SIZE, version).await.unwrap();
        let mut reader = Cursor::new(buf);
        let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap();
        assert_eq!(header.version, version);
        assert_eq!(header.message_type, msg_type);
        header.read_variable_size(&mut reader).await.unwrap()
    }

    // ==================== VERSION VALIDATION ====================

    #[test]
    fn from_reader_accepts_v2() {
        block_on(async {
            let header_bytes = make_header(2, 1, 100, 100, 0);
            let mut reader = Cursor::new(header_bytes);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap();
            assert_eq!(header.version, 2);
        });
    }

    #[test]
    fn from_reader_accepts_v3() {
        block_on(async {
            let header_bytes = make_header(3, 1, 100, 100, 0);
            let mut reader = Cursor::new(header_bytes);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap();
            assert_eq!(header.version, 3);
        });
    }

    #[test]
    fn from_reader_rejects_v0() {
        block_on(async {
            let header_bytes = make_header(0, 1, 100, 100, 0);
            let mut reader = Cursor::new(header_bytes);
            let err = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap_err();
            assert!(matches!(err, WireError::UnsupportedProtocol(0)));
        });
    }

    #[test]
    fn from_reader_rejects_v1() {
        block_on(async {
            let header_bytes = make_header(1, 1, 100, 100, 0);
            let mut reader = Cursor::new(header_bytes);
            let err = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap_err();
            assert!(matches!(err, WireError::UnsupportedProtocol(1)));
        });
    }

    #[test]
    fn from_reader_rejects_v4() {
        block_on(async {
            let header_bytes = make_header(4, 1, 100, 100, 0);
            let mut reader = Cursor::new(header_bytes);
            let err = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap_err();
            assert!(matches!(err, WireError::UnsupportedProtocol(4)));
        });
    }

    #[test]
    fn write_fixed_rejects_unsupported_version() {
        block_on(async {
            let mut buf = Vec::new();
            let err = wire_header_write_fixed_size(&mut buf, &42u64, 1, 99).await.unwrap_err();
            assert!(matches!(err, WireError::UnsupportedProtocol(99)));
        });
    }

    #[test]
    fn write_variable_rejects_unsupported_version() {
        block_on(async {
            let mut buf = Vec::new();
            let err = wire_header_write_variable_size(&mut buf, &42u64, 1, CompressionType::None, MAX_SIZE, 0).await.unwrap_err();
            assert!(matches!(err, WireError::UnsupportedProtocol(0)));
        });
    }

    // ==================== SIZE VALIDATION ====================

    #[test]
    fn from_reader_rejects_large_compressed_length() {
        block_on(async {
            let header_bytes = make_header(2, 1, 1_000_000, 100, 0);
            let mut reader = Cursor::new(header_bytes);
            let err = WireHeader::from_reader(&mut reader, 1000).await.unwrap_err();
            match err {
                WireError::MessageTooLarge { message_length, max_size_bytes } => {
                    assert_eq!(message_length, 1_000_000);
                    assert_eq!(max_size_bytes, 1000);
                }
                _ => panic!("expected MessageTooLarge"),
            }
        });
    }

    #[test]
    fn from_reader_rejects_large_uncompressed_length() {
        block_on(async {
            let header_bytes = make_header(2, 1, 100, 1_000_000, 1);
            let mut reader = Cursor::new(header_bytes);
            let err = WireHeader::from_reader(&mut reader, 1000).await.unwrap_err();
            match err {
                WireError::MessageTooLarge { message_length, max_size_bytes } => {
                    assert_eq!(message_length, 1_000_000);
                    assert_eq!(max_size_bytes, 1000);
                }
                _ => panic!("expected MessageTooLarge"),
            }
        });
    }

    #[test]
    fn from_reader_accepts_at_max_size_boundary() {
        block_on(async {
            let header_bytes = make_header(2, 1, 1000, 1000, 0);
            let mut reader = Cursor::new(header_bytes);
            let header = WireHeader::from_reader(&mut reader, 1000).await.unwrap();
            assert_eq!(header.compressed_length, 1000);
        });
    }

    #[test]
    fn from_reader_rejects_one_over_max_size() {
        block_on(async {
            let header_bytes = make_header(2, 1, 1001, 1000, 0);
            let mut reader = Cursor::new(header_bytes);
            let err = WireHeader::from_reader(&mut reader, 1000).await.unwrap_err();
            assert!(matches!(err, WireError::MessageTooLarge { .. }));
        });
    }

    #[test]
    fn write_variable_rejects_message_exceeding_max() {
        block_on(async {
            let large_msg = vec![0u8; 2000];
            let mut buf = Vec::new();
            let err = wire_header_write_variable_size(&mut buf, &large_msg, 1, CompressionType::None, 1000, 2).await.unwrap_err();
            assert!(matches!(err, WireError::MessageTooLarge { .. }));
        });
    }

    // ==================== FIXED SIZE ROUNDTRIP ====================

    #[test]
    fn fixed_size_roundtrip_v2() {
        block_on(async {
            let result: u64 = roundtrip_fixed(&12345u64, 1, 2).await;
            assert_eq!(result, 12345);
        });
    }

    #[test]
    fn fixed_size_roundtrip_v3() {
        block_on(async {
            let result: u64 = roundtrip_fixed(&67890u64, 2, 3).await;
            assert_eq!(result, 67890);
        });
    }

    #[test]
    fn fixed_size_roundtrip_struct_v2() {
        block_on(async {
            let msg = (42u32, 100i64, true);
            let result: (u32, i64, bool) = roundtrip_fixed(&msg, 5, 2).await;
            assert_eq!(result, msg);
        });
    }

    #[test]
    fn fixed_size_roundtrip_struct_v3() {
        block_on(async {
            let msg = (42u32, 100i64, true);
            let result: (u32, i64, bool) = roundtrip_fixed(&msg, 5, 3).await;
            assert_eq!(result, msg);
        });
    }

    // ==================== VARIABLE SIZE ROUNDTRIP ====================

    #[test]
    fn variable_size_roundtrip_v2_no_compression() {
        block_on(async {
            let msg = vec![1u8, 2, 3, 4, 5];
            let result: Vec<u8> = roundtrip_variable(&msg, 1, CompressionType::None, 2).await;
            assert_eq!(result, msg);
        });
    }

    #[test]
    fn variable_size_roundtrip_v3_no_compression() {
        block_on(async {
            let msg = vec![1u8, 2, 3, 4, 5];
            let result: Vec<u8> = roundtrip_variable(&msg, 1, CompressionType::None, 3).await;
            assert_eq!(result, msg);
        });
    }

    #[test]
    fn variable_size_roundtrip_v2_with_zstd() {
        block_on(async {
            let msg = vec![42u8; 2000];
            let result: Vec<u8> = roundtrip_variable(&msg, 1, CompressionType::Zstd { level: 3 }, 2).await;
            assert_eq!(result, msg);
        });
    }

    #[test]
    fn variable_size_roundtrip_v3_with_zstd() {
        block_on(async {
            let msg = vec![42u8; 2000];
            let result: Vec<u8> = roundtrip_variable(&msg, 1, CompressionType::Zstd { level: 3 }, 3).await;
            assert_eq!(result, msg);
        });
    }

    #[test]
    fn variable_size_roundtrip_with_snappy() {
        block_on(async {
            let msg = vec![99u8; 3000];
            let result: Vec<u8> = roundtrip_variable(&msg, 1, CompressionType::Snappy, 2).await;
            assert_eq!(result, msg);
        });
    }

    // ==================== OPTIMIZATION PATH: SMALL VARIABLE USES STACK ====================

    #[test]
    fn small_variable_message_uses_fixed_path() {
        block_on(async {
            let small_msg = vec![1u8, 2, 3];
            let mut buf = Vec::new();
            wire_header_write_variable_size(&mut buf, &small_msg, 1, CompressionType::None, MAX_SIZE, 2).await.unwrap();

            let mut reader = Cursor::new(buf);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap();
            assert!(header.compressed_length <= WIRE_FIXED_BODY_SIZE as u32);
            assert_eq!(header.compression_type, CompressionType::None);

            let result: Vec<u8> = header.read_variable_size(&mut reader).await.unwrap();
            assert_eq!(result, small_msg);
        });
    }

    #[test]
    fn message_at_boundary_uses_stack() {
        block_on(async {
            let boundary_msg = vec![0u8; WIRE_FIXED_BODY_SIZE - 10];
            let result: Vec<u8> = roundtrip_variable(&boundary_msg, 1, CompressionType::None, 2).await;
            assert_eq!(result, boundary_msg);
        });
    }

    #[test]
    fn message_over_boundary_uses_heap() {
        block_on(async {
            let large_msg = vec![0u8; WIRE_FIXED_BODY_SIZE + 100];
            let result: Vec<u8> = roundtrip_variable(&large_msg, 1, CompressionType::None, 2).await;
            assert_eq!(result, large_msg);
        });
    }

    // ==================== IO ERRORS ====================

    #[test]
    fn incomplete_header_returns_io_error() {
        block_on(async {
            let partial = vec![0u8; 10];
            let mut reader = Cursor::new(partial);
            let err = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap_err();
            assert!(matches!(err, WireError::NetworkError(_)));
        });
    }

    #[test]
    fn incomplete_body_returns_io_error() {
        block_on(async {
            let mut data = make_header(2, 1, 100, 100, 0);
            data.extend_from_slice(&[0u8; 50]);
            let mut reader = Cursor::new(data);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap();
            let err: Result<Vec<u8>, _> = header.read_variable_size(&mut reader).await;
            assert!(matches!(err, Err(WireError::NetworkError(_))));
        });
    }

    #[test]
    fn empty_reader_returns_io_error() {
        block_on(async {
            let mut reader = Cursor::new(Vec::new());
            let err = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap_err();
            assert!(matches!(err, WireError::NetworkError(_)));
        });
    }

    // ==================== HEADER FIELD PARSING ====================

    #[test]
    fn header_parses_all_fields_correctly() {
        block_on(async {
            let header_bytes = make_header(2, 0x12345678, 0xAABBCCDD, 0x11223344, 2);
            let mut reader = Cursor::new(header_bytes);
            let header = WireHeader::from_reader(&mut reader, u64::MAX).await.unwrap();
            assert_eq!(header.version, 2);
            assert_eq!(header.message_type, 0x12345678);
            assert_eq!(header.compressed_length, 0xAABBCCDD);
            assert_eq!(header.uncompressed_length, 0x11223344);
            assert_eq!(header.compression_type, CompressionType::Snappy);
        });
    }

    #[test]
    fn header_compression_types_parsed() {
        block_on(async {
            for (id, expected) in [
                (0, CompressionType::None),
                (1, CompressionType::Zstd { level: 6 }),
                (2, CompressionType::Snappy),
                (3, CompressionType::Brotli { level: 6 }),
                (4, CompressionType::Gzip { level: 6 }),
            ] {
                let header_bytes = make_header(2, 1, 100, 100, id);
                let mut reader = Cursor::new(header_bytes);
                let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap();
                assert_eq!(header.compression_type, expected);
            }
        });
    }

    // ==================== MESSAGE TYPE PRESERVED ====================

    #[test]
    fn message_type_preserved_in_header() {
        block_on(async {
            for msg_type in [0u32, 1, 255, 0xFFFFFFFF] {
                let mut buf = Vec::new();
                wire_header_write_fixed_size(&mut buf, &42u64, msg_type, 2).await.unwrap();
                let mut reader = Cursor::new(buf);
                let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.unwrap();
                assert_eq!(header.message_type, msg_type);
            }
        });
    }
}
