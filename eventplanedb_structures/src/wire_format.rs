use std::io;
use bincode::{config, Decode, Encode};
use thiserror::Error;

use crate::eventplanedb_error::EventPlaneDBError;
use crate::{compression_type::CompressionType};


/// Protocol constants
//TODO: Make configurable - max message size, stack buffer size
pub const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024; // 64MB max message size
pub const STACK_BUFFER_SIZE: u32 = 30 * 1024; // 30KB stack buffer threshold
pub const PROTOCOL_VERSION_V1: u32 = 1;
pub const PROTOCOL_VERSION_V2: u32 = 2;

/// Bincode configuration for variable-length encoding
const BINCODE_CONFIG_VARIABLE: config::Configuration = config::standard();

//TODO: When we have a large message, we always allocate on the heap, use a buffer pool?

pub fn to_wire_format_variable_stack<T>(
    item: &T,
    compression_type: CompressionType,
    serialize_buffer: &mut [u8],
    compress_buffer: &mut [u8],
) -> io::Result<(usize, usize)>
where
    T: Encode,
{
    let uncompressed_size = bincode::encode_into_slice(item, serialize_buffer, BINCODE_CONFIG_VARIABLE)
        .map_err(|e| io::Error::other(e.to_string()))?;

    let compressed_size = match compression_type {
        CompressionType::None => {
            // No compression - just copy to output buffer if different from input
            if serialize_buffer.as_ptr() != compress_buffer.as_ptr() {
                compress_buffer[..uncompressed_size].copy_from_slice(&serialize_buffer[..uncompressed_size]);
            }
            uncompressed_size
        }
        CompressionType::Zstd { level } => {
            let size = zstd::bulk::compress_to_buffer(
                &serialize_buffer[..uncompressed_size],
                compress_buffer,
                level,
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
            size
        }
        CompressionType::Snappy => {
            let compressed = snap::raw::Encoder::new()
                .compress_vec(&serialize_buffer[..uncompressed_size])
                .map_err(|e| io::Error::other(e.to_string()))?;
            
            if compressed.len() > compress_buffer.len() {
                return Err(io::Error::other("Compression buffer too small"));
            }
            compress_buffer[..compressed.len()].copy_from_slice(&compressed);
            compressed.len()
        }
        CompressionType::Brotli { level } => {
            let mut output = std::io::Cursor::new(compress_buffer);
            let params = brotli::enc::BrotliEncoderParams {
                quality: level,
                ..Default::default()
            };
            brotli::BrotliCompress(
                &mut std::io::Cursor::new(&serialize_buffer[..uncompressed_size]),
                &mut output,
                &params,
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
            output.position() as usize
        }
        CompressionType::Gzip { level } => {
            use flate2::{Compression, write::GzEncoder};
            let mut encoder = GzEncoder::new(std::io::Cursor::new(compress_buffer), Compression::new(level as u32));
            std::io::Write::write_all(&mut encoder, &serialize_buffer[..uncompressed_size])?;
            let cursor = encoder.finish()?;
            cursor.position() as usize
        }
    };

    Ok((uncompressed_size, compressed_size))
}

pub fn to_wire_format_variable<T>(
    item: &T,
    compression_type: CompressionType,
) -> io::Result<(usize, Vec<u8>)>
where
    T: Encode,
{
    //TODO: There are two heap allocations here. Can we use some kind of vec pool instead?
    let serialized = bincode::encode_to_vec(item, BINCODE_CONFIG_VARIABLE)
        .map_err(|e| io::Error::other(e.to_string()))?;
    let uncompressed_size = serialized.len();

    match compression_type {
        CompressionType::None => Ok((uncompressed_size, serialized)),
        CompressionType::Zstd { level } => {
            let compressed = zstd::bulk::compress(&serialized, level)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok((uncompressed_size, compressed))
        }
        CompressionType::Snappy => {
            let compressed = snap::raw::Encoder::new()
                .compress_vec(&serialized)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok((uncompressed_size, compressed))
        }
        CompressionType::Brotli { level } => {
            let mut compressed = Vec::new();
            let params = brotli::enc::BrotliEncoderParams {
                quality: level,
                ..Default::default()
            };
            brotli::BrotliCompress(
                &mut std::io::Cursor::new(&serialized),
                &mut compressed,
                &params,
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
            Ok((uncompressed_size, compressed))
        }
        CompressionType::Gzip { level } => {
            use flate2::{Compression, write::GzEncoder};
            let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level as u32));
            std::io::Write::write_all(&mut encoder, &serialized)?;
            let compressed = encoder.finish()?;
            Ok((uncompressed_size, compressed))
        }
    }
}

/// Deserialize and decompress from wire format
pub fn from_wire_format_variable<T>(
    data: &[u8],
    compression_type: CompressionType,
    capacity: usize,
) -> io::Result<T>
where
    T: Decode<()>,
{
    let decompressed = match compression_type {
        CompressionType::None => data.to_vec(),
        CompressionType::Zstd { .. } => {
            zstd::bulk::decompress(data, capacity).map_err(|e| io::Error::other(e.to_string()))?
        }
        CompressionType::Snappy => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|e| io::Error::other(e.to_string()))?,
        CompressionType::Brotli { .. } => {
            let mut decompressed = Vec::with_capacity(capacity);
            brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut decompressed)
                .map_err(|e| io::Error::other(e.to_string()))?;
            decompressed
        }
        CompressionType::Gzip { .. } => {
            use flate2::read::GzDecoder;
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::with_capacity(capacity);
            std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            decompressed
        }
    };

    bincode::decode_from_slice(&decompressed, BINCODE_CONFIG_VARIABLE)
        .map(|(events, _)| events)
        .map_err(|e| io::Error::other(e.to_string()))
}

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
    #[error("Invalid format with version specified")]
    InvalidFormatWithVersion(u32),
}

impl From<crate::wire_format::WireError> for EventPlaneDBError {
    fn from(e: crate::wire_format::WireError) -> Self {
        use crate::wire_format::WireError;
        match e {
            WireError::Io(io_err) => EventPlaneDBError::io_error(),
            WireError::Serialization(e) => EventPlaneDBError::serialization_error(),
            WireError::Deserialization(e) => EventPlaneDBError::serialization_error(),
            WireError::BincodeEncode(e) => EventPlaneDBError::serialization_error(),
            WireError::BincodeDecode(e) => EventPlaneDBError::serialization_error(),
            WireError::MessageTooLarge(size) => {
                EventPlaneDBError::message_too_large(size as u64, crate::wire_format::MAX_MESSAGE_SIZE as u64)
            }
            WireError::UnsupportedVersion(version) => {
                EventPlaneDBError::unsupported_protocol_version(version)
            }
            WireError::InvalidFormat => {
                EventPlaneDBError::invalid_wire_format()
            }
            WireError::InvalidFormatWithVersion(version) => {
                EventPlaneDBError::invalid_wire_format()
            },
        }
    }
}