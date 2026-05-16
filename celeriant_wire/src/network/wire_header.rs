use bincode::{Decode, Encode};
use celeriant_wal::compression_type::CompressionType;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use serde::Serialize;

use crate::{codec, codec::compression::DictCodec, network::wire_error::WireError};

pub const PROTOCOL_VERSION_V2: u32 = 2;
pub const PROTOCOL_VERSION_V3: u32 = 3;
pub const WIRE_HEADER_SIZE: usize = 17;
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
    pub async fn from_reader<R>(reader: &mut R, max_size_bytes: u64) -> Result<Self, WireError>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut header = [0u8; WIRE_HEADER_SIZE];
        reader.read_exact(&mut header).await?;

        let version = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        match version {
            PROTOCOL_VERSION_V2 | PROTOCOL_VERSION_V3 => {}
            _ => return Err(WireError::UnsupportedProtocol(version)),
        }

        let message_type = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let compressed_length = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let uncompressed_length =
            u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let compression_type = CompressionType::from_byte(header[16])
            .map_err(|b| WireError::MalformedFrame(format!("unknown compression byte {b}")))?;

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

        if compression_type == CompressionType::None && compressed_length != uncompressed_length {
            return Err(WireError::MalformedFrame(format!(
                "uncompressed frame has mismatched lengths (compressed={compressed_length}, uncompressed={uncompressed_length})"
            )));
        }

        Ok(Self {
            version,
            message_type,
            compressed_length,
            uncompressed_length,
            compression_type,
        })
    }

    pub async fn read_fixed_size<R, T>(&self, reader: &mut R) -> Result<T, WireError>
    where
        R: AsyncReadExt + Unpin,
        T: Decode<()> + serde::de::DeserializeOwned,
    {
        if self.compression_type != CompressionType::None {
            return Err(WireError::MalformedFrame(
                "fixed-size frame must not be compressed".into(),
            ));
        }
        if self.compressed_length as usize > WIRE_FIXED_BODY_SIZE {
            return Err(WireError::MessageTooLarge {
                message_length: self.compressed_length as u64,
                max_size_bytes: WIRE_FIXED_BODY_SIZE as u64,
            });
        }
        let mut buffer = [0u8; WIRE_FIXED_BODY_SIZE];
        reader
            .read_exact(&mut buffer[..self.compressed_length as usize])
            .await?;
        deserialise_versioned(&buffer, self.version)
    }

    pub async fn read_variable_size_uncompressed<R, T>(&self, reader: &mut R) -> Result<T, WireError>
    where
        R: AsyncReadExt + Unpin,
        T: Decode<()> + serde::de::DeserializeOwned,
    {
        if self.compression_type != CompressionType::None {
            return Err(WireError::MalformedFrame(
                "uncompressed reader received a compressed frame".into(),
            ));
        }
        let payload = self.read_variable_body_raw(reader).await?;
        deserialise_versioned(&payload, self.version)
    }

    pub async fn read_variable_size_with_codec<R, T>(
        &self,
        reader: &mut R,
        codec: &DictCodec,
    ) -> Result<T, WireError>
    where
        R: AsyncReadExt + Unpin,
        T: Decode<()> + serde::de::DeserializeOwned,
    {
        let payload = self.read_variable_body_raw(reader).await?;
        let bytes = match self.compression_type {
            CompressionType::None => payload,
            CompressionType::ZstdDict => codec.decompress(&payload, self.uncompressed_length as usize)?,
        };
        deserialise_versioned(&bytes, self.version)
    }

    pub async fn read_variable_body_raw<R>(&self, reader: &mut R) -> Result<Vec<u8>, WireError>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut payload = vec![0u8; self.compressed_length as usize];
        reader.read_exact(&mut payload).await?;
        Ok(payload)
    }
}

#[inline]
pub fn deserialise_versioned<T>(bytes: &[u8], version: u32) -> Result<T, WireError>
where
    T: Decode<()> + serde::de::DeserializeOwned,
{
    match version {
        PROTOCOL_VERSION_V2 => Ok(codec::bincode::fixed_deserialise(bytes)?),
        PROTOCOL_VERSION_V3 => Ok(codec::msgpack::deserialise(bytes)?),
        _ => Err(WireError::UnsupportedProtocol(version)),
    }
}

#[inline]
pub fn serialise_heap_versioned<T>(message: &T, version: u32) -> Result<Vec<u8>, WireError>
where
    T: Encode + Serialize,
{
    match version {
        PROTOCOL_VERSION_V2 => Ok(codec::bincode::fixed_serialise_heap(message)?),
        PROTOCOL_VERSION_V3 => Ok(codec::msgpack::serialise_heap(message)?),
        _ => Err(WireError::UnsupportedProtocol(version)),
    }
}

/// Writes a fixed-size message (always uncompressed, body ≤ `WIRE_FIXED_BODY_SIZE`).
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

    writer.write_all(&buffer[..WIRE_HEADER_SIZE + body_size]).await?;
    Ok(())
}

/// Writes a variable-size message with no compression.
pub async fn wire_header_write_variable_size_uncompressed<W, T>(
    writer: &mut W,
    message: &T,
    request_response_type: u32,
    max_size_bytes: u64,
    protocol_version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
    T: Encode + Serialize,
{
    let data = serialise_heap_versioned(message, protocol_version)?;
    write_variable_frame(
        writer,
        request_response_type,
        protocol_version,
        CompressionType::None,
        data.len() as u32,
        &data,
        max_size_bytes,
    )
    .await
}

/// Writes a variable-size message using a precompiled `DictCodec` for `ZstdDict` frames.
pub async fn wire_header_write_variable_size_with_codec<W, T>(
    writer: &mut W,
    message: &T,
    request_response_type: u32,
    compression_type: CompressionType,
    max_size_bytes: u64,
    protocol_version: u32,
    codec: &DictCodec,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
    T: Encode + Serialize,
{
    let uncompressed = serialise_heap_versioned(message, protocol_version)?;
    let uncompressed_size = uncompressed.len() as u32;
    let data = match compression_type {
        CompressionType::None => uncompressed,
        CompressionType::ZstdDict => codec.compress(&uncompressed)?,
    };
    write_variable_frame(
        writer,
        request_response_type,
        protocol_version,
        compression_type,
        uncompressed_size,
        &data,
        max_size_bytes,
    )
    .await
}

/// Writes a pre-built variable-size frame. The body is whatever bytes the caller wants on
/// the wire — already serialised and (if applicable) already compressed. The wire layer
/// just prepends the header.
pub async fn wire_header_write_variable_size_raw<W>(
    writer: &mut W,
    body: &[u8],
    request_response_type: u32,
    compression_type: CompressionType,
    uncompressed_size: u32,
    max_size_bytes: u64,
    protocol_version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    write_variable_frame(
        writer,
        request_response_type,
        protocol_version,
        compression_type,
        uncompressed_size,
        body,
        max_size_bytes,
    )
    .await
}

async fn write_variable_frame<W>(
    writer: &mut W,
    request_response_type: u32,
    protocol_version: u32,
    compression_type: CompressionType,
    uncompressed_size: u32,
    data: &[u8],
    max_size_bytes: u64,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    let compressed_size = data.len() as u32;
    let compression_type_id = compression_type.to_byte();

    if compressed_size as u64 > max_size_bytes {
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
        buffer[WIRE_HEADER_SIZE..WIRE_HEADER_SIZE + compressed_size as usize]
            .copy_from_slice(data);
        writer
            .write_all(&buffer[..WIRE_HEADER_SIZE + compressed_size as usize])
            .await?;
        return Ok(());
    }

    let mut buffer = Vec::with_capacity(WIRE_HEADER_SIZE + data.len());
    buffer.extend_from_slice(&protocol_version.to_le_bytes());
    buffer.extend_from_slice(&request_response_type.to_le_bytes());
    buffer.extend_from_slice(&compressed_size.to_le_bytes());
    buffer.extend_from_slice(&uncompressed_size.to_le_bytes());
    buffer.push(compression_type_id);
    buffer.extend_from_slice(data);
    writer.write_all(&buffer).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
    use futures_lite::future::block_on;
    use futures_lite::io::Cursor;

    const MAX_SIZE: u64 = 64 * 1024;

    fn test_codec() -> DictCodec {
        DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile")
    }

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
        wire_header_write_fixed_size(&mut buf, msg, msg_type, version).await.expect("write_fixed");
        let mut reader = Cursor::new(buf);
        let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
        assert_eq!(header.version, version);
        assert_eq!(header.message_type, msg_type);
        header.read_fixed_size(&mut reader).await.expect("read_fixed")
    }

    async fn roundtrip_variable_uncompressed<T>(msg: &T, msg_type: u32, version: u32) -> T
    where
        T: Encode + Serialize + Decode<()> + serde::de::DeserializeOwned + std::fmt::Debug,
    {
        let mut buf = Vec::new();
        wire_header_write_variable_size_uncompressed(&mut buf, msg, msg_type, MAX_SIZE, version)
            .await
            .expect("write_uncompressed");
        let mut reader = Cursor::new(buf);
        let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
        assert_eq!(header.version, version);
        assert_eq!(header.message_type, msg_type);
        header.read_variable_size_uncompressed(&mut reader).await.expect("read_uncompressed")
    }

    async fn roundtrip_variable_with_codec<T>(msg: &T, msg_type: u32, compression: CompressionType, version: u32) -> T
    where
        T: Encode + Serialize + Decode<()> + serde::de::DeserializeOwned + std::fmt::Debug,
    {
        let codec = test_codec();
        let mut buf = Vec::new();
        wire_header_write_variable_size_with_codec(&mut buf, msg, msg_type, compression, MAX_SIZE, version, &codec)
            .await
            .expect("write_with_codec");
        let mut reader = Cursor::new(buf);
        let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
        header.read_variable_size_with_codec(&mut reader, &codec).await.expect("read_with_codec")
    }

    // ==================== version validation ====================

    #[test]
    fn from_reader_accepts_v2_and_v3() {
        block_on(async {
            for version in [2u32, 3] {
                let header_bytes = make_header(version, 1, 100, 100, 0);
                let mut reader = Cursor::new(header_bytes);
                let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
                assert_eq!(header.version, version);
            }
        });
    }

    #[test]
    fn from_reader_rejects_unsupported_versions() {
        block_on(async {
            for version in [0u32, 1, 4] {
                let header_bytes = make_header(version, 1, 100, 100, 0);
                let mut reader = Cursor::new(header_bytes);
                let err = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect_err("must reject");
                assert!(matches!(err, WireError::UnsupportedProtocol(v) if v == version));
            }
        });
    }

    #[test]
    fn write_fixed_rejects_unsupported_version() {
        block_on(async {
            let mut buf = Vec::new();
            let err = wire_header_write_fixed_size(&mut buf, &42u64, 1, 99).await.expect_err("must reject");
            assert!(matches!(err, WireError::UnsupportedProtocol(99)));
        });
    }

    // ==================== size validation ====================

    #[test]
    fn from_reader_rejects_large_lengths() {
        block_on(async {
            for (compressed, uncompressed) in [(1_000_000u32, 100u32), (100, 1_000_000)] {
                let header_bytes = make_header(2, 1, compressed, uncompressed, 0);
                let mut reader = Cursor::new(header_bytes);
                let err = WireHeader::from_reader(&mut reader, 1000).await.expect_err("must reject");
                assert!(matches!(err, WireError::MessageTooLarge { .. }));
            }
        });
    }

    /// Decompression bomb: a tiny `compressed_length` claiming a huge `uncompressed_length`
    /// is rejected by `from_reader` before any body bytes are read or any decompressor is
    /// invoked.
    #[test]
    fn from_reader_rejects_decompression_bomb() {
        block_on(async {
            let header_bytes = make_header(2, 1, 10, 10_000_000, 1);
            let mut reader = Cursor::new(header_bytes);
            let err = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect_err("must reject");
            assert!(matches!(err, WireError::MessageTooLarge { message_length, .. } if message_length == 10_000_000));
        });
    }

    #[test]
    fn from_reader_rejects_none_with_mismatched_lengths() {
        block_on(async {
            for (compressed, uncompressed) in [(100u32, 200u32), (200, 100)] {
                let header_bytes = make_header(2, 1, compressed, uncompressed, 0);
                let mut reader = Cursor::new(header_bytes);
                let err = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect_err("must reject");
                assert!(matches!(err, WireError::MalformedFrame(_)));
            }
        });
    }

    // ==================== fixed-size roundtrip ====================

    #[test]
    fn fixed_size_roundtrip_v2_and_v3() {
        block_on(async {
            for &version in &[PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3] {
                let result: u64 = roundtrip_fixed(&12345u64, 1, version).await;
                assert_eq!(result, 12345);
            }
        });
    }

    #[test]
    fn read_fixed_size_rejects_compressed_frame() {
        block_on(async {
            // Hand-craft a header that claims ZstdDict compression on a fixed-size body.
            let mut data = make_header(2, 1, 10, 10, 1);
            data.extend_from_slice(&[0u8; 10]);
            let mut reader = Cursor::new(data);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
            let result: Result<u64, _> = header.read_fixed_size(&mut reader).await;
            assert!(matches!(result, Err(WireError::MalformedFrame(_))));
        });
    }

    #[test]
    fn read_fixed_size_rejects_body_past_stack_buffer() {
        block_on(async {
            let oversize = (WIRE_FIXED_BODY_SIZE + 1) as u32;
            let header_bytes = make_header(2, 1, oversize, oversize, 0);
            let mut reader = Cursor::new(header_bytes);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
            let result: Result<u64, _> = header.read_fixed_size(&mut reader).await;
            assert!(matches!(result, Err(WireError::MessageTooLarge { .. })));
        });
    }

    // ==================== uncompressed variable-size roundtrip ====================

    #[test]
    fn variable_size_uncompressed_roundtrip() {
        block_on(async {
            for &version in &[PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3] {
                let msg = vec![42u8; 2000];
                let result: Vec<u8> = roundtrip_variable_uncompressed(&msg, 1, version).await;
                assert_eq!(result, msg);
            }
        });
    }

    #[test]
    fn read_uncompressed_rejects_compressed_frame() {
        block_on(async {
            let codec = test_codec();
            let msg = vec![42u8; 2000];
            let mut buf = Vec::new();
            wire_header_write_variable_size_with_codec(
                &mut buf, &msg, 1, CompressionType::ZstdDict, MAX_SIZE, PROTOCOL_VERSION_V2, &codec,
            ).await.expect("write_with_codec");
            let mut reader = Cursor::new(buf);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
            let result: Result<Vec<u8>, _> = header.read_variable_size_uncompressed(&mut reader).await;
            assert!(result.is_err(), "uncompressed reader must reject ZstdDict frames");
        });
    }

    // ==================== codec-based variable-size roundtrip ====================

    #[test]
    fn variable_size_with_codec_roundtrip_none_and_zstd_dict() {
        block_on(async {
            for compression in [CompressionType::None, CompressionType::ZstdDict] {
                let msg = vec![42u8; 2000];
                let result: Vec<u8> = roundtrip_variable_with_codec(&msg, 1, compression, PROTOCOL_VERSION_V2).await;
                assert_eq!(result, msg);
            }
        });
    }

    // ==================== raw helpers ====================

    /// Writing via `_raw` and reading via `_with_codec` produces the same payload —
    /// proves the raw entry point lays down a wire-format-compatible frame.
    #[test]
    fn raw_write_codec_read_interop() {
        block_on(async {
            let codec = test_codec();
            let original = vec![42u8; 2000];
            // The codec read path will deserialise, so serialise first then compress.
            let serialised = serialise_heap_versioned(&original, PROTOCOL_VERSION_V2).expect("serialise");
            let compressed = codec.compress(&serialised).expect("compress");
            let uncompressed_size = serialised.len() as u32;

            let mut buf = Vec::new();
            wire_header_write_variable_size_raw(
                &mut buf, &compressed, 1, CompressionType::ZstdDict, uncompressed_size, MAX_SIZE, PROTOCOL_VERSION_V2,
            ).await.expect("write_raw");

            let mut reader = Cursor::new(buf);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
            let parsed: Vec<u8> = header.read_variable_size_with_codec(&mut reader, &codec).await.expect("read_with_codec");
            assert_eq!(parsed, original);
        });
    }

    /// `_raw` reading a frame written by `_with_codec` returns the raw (still compressed)
    /// body bytes. Caller decompresses + deserialises themselves.
    #[test]
    fn codec_write_raw_read_returns_compressed_bytes() {
        block_on(async {
            let codec = test_codec();
            let original = vec![42u8; 2000];

            let mut buf = Vec::new();
            wire_header_write_variable_size_with_codec(
                &mut buf, &original, 1, CompressionType::ZstdDict, MAX_SIZE, PROTOCOL_VERSION_V2, &codec,
            ).await.expect("write_with_codec");

            let mut reader = Cursor::new(buf);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
            assert_eq!(header.compression_type, CompressionType::ZstdDict);
            let raw_body = header.read_variable_body_raw(&mut reader).await.expect("read_raw");
            // raw_body is the compressed bytes; decompress them to recover the original.
            let decompressed = codec.decompress(&raw_body, header.uncompressed_length as usize).expect("decompress");
            let parsed: Vec<u8> = deserialise_versioned(&decompressed, header.version).expect("deserialise");
            assert_eq!(parsed, original);
        });
    }

    // ==================== small-payload optimization ====================

    #[test]
    fn small_uncompressed_message_uses_stack_path() {
        block_on(async {
            let small_msg = vec![1u8, 2, 3];
            let mut buf = Vec::new();
            wire_header_write_variable_size_uncompressed(&mut buf, &small_msg, 1, MAX_SIZE, PROTOCOL_VERSION_V2)
                .await.expect("write_uncompressed");

            let mut reader = Cursor::new(buf);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
            assert!(header.compressed_length <= WIRE_FIXED_BODY_SIZE as u32);
            assert_eq!(header.compression_type, CompressionType::None);
        });
    }

    #[test]
    fn message_over_fixed_boundary_uses_heap() {
        block_on(async {
            let large_msg = vec![0u8; WIRE_FIXED_BODY_SIZE + 100];
            let result: Vec<u8> = roundtrip_variable_uncompressed(&large_msg, 1, PROTOCOL_VERSION_V2).await;
            assert_eq!(result, large_msg);
        });
    }

    // ==================== write-side max size enforcement ====================

    /// All three variable-size writers refuse to emit a frame whose compressed body
    /// exceeds `max_size_bytes`, preventing a misconfigured caller from spraying an
    /// oversized frame onto the wire (which the peer would then reject anyway, wasting
    /// bandwidth).
    #[test]
    fn variable_size_writers_reject_oversized_body() {
        block_on(async {
            const TINY_MAX: u64 = 1024;
            let big_msg = vec![7u8; 4096];

            let mut buf = Vec::new();
            let err = wire_header_write_variable_size_uncompressed(
                &mut buf, &big_msg, 1, TINY_MAX, PROTOCOL_VERSION_V2,
            ).await.expect_err("uncompressed must reject");
            assert!(matches!(err, WireError::MessageTooLarge { .. }));

            let codec = test_codec();
            let mut buf = Vec::new();
            let err = wire_header_write_variable_size_with_codec(
                &mut buf, &big_msg, 1, CompressionType::None, TINY_MAX, PROTOCOL_VERSION_V2, &codec,
            ).await.expect_err("with_codec(None) must reject");
            assert!(matches!(err, WireError::MessageTooLarge { .. }));

            let mut buf = Vec::new();
            let raw_body = vec![0u8; (TINY_MAX + 1) as usize];
            let err = wire_header_write_variable_size_raw(
                &mut buf, &raw_body, 1, CompressionType::None, raw_body.len() as u32, TINY_MAX, PROTOCOL_VERSION_V2,
            ).await.expect_err("raw must reject");
            assert!(matches!(err, WireError::MessageTooLarge { .. }));
        });
    }

    // ==================== header parsing ====================

    #[test]
    fn header_parses_all_fields_correctly() {
        block_on(async {
            let header_bytes = make_header(2, 0x12345678, 0xAABBCCDD, 0x11223344, 1);
            let mut reader = Cursor::new(header_bytes);
            let header = WireHeader::from_reader(&mut reader, u64::MAX).await.expect("from_reader");
            assert_eq!(header.version, 2);
            assert_eq!(header.message_type, 0x12345678);
            assert_eq!(header.compressed_length, 0xAABBCCDD);
            assert_eq!(header.uncompressed_length, 0x11223344);
            assert_eq!(header.compression_type, CompressionType::ZstdDict);
        });
    }

    #[test]
    fn header_rejects_unknown_compression_bytes() {
        block_on(async {
            for bad_byte in [2u8, 3, 4, 5, 255] {
                let header_bytes = make_header(2, 1, 100, 100, bad_byte);
                let mut reader = Cursor::new(header_bytes);
                let result = WireHeader::from_reader(&mut reader, MAX_SIZE).await;
                assert!(result.is_err(), "byte {} should be rejected", bad_byte);
            }
        });
    }

    // ==================== IO errors ====================

    #[test]
    fn incomplete_header_returns_io_error() {
        block_on(async {
            let partial = vec![0u8; 10];
            let mut reader = Cursor::new(partial);
            let err = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect_err("must error");
            assert!(matches!(err, WireError::NetworkError(_)));
        });
    }

    #[test]
    fn incomplete_body_returns_io_error() {
        block_on(async {
            let mut data = make_header(2, 1, 100, 100, 0);
            data.extend_from_slice(&[0u8; 50]);
            let mut reader = Cursor::new(data);
            let header = WireHeader::from_reader(&mut reader, MAX_SIZE).await.expect("from_reader");
            let err: Result<Vec<u8>, _> = header.read_variable_size_uncompressed(&mut reader).await;
            assert!(matches!(err, Err(WireError::NetworkError(_))));
        });
    }
}
