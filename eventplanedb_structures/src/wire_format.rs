use crate::{
    compression_type::CompressionType,
    constants::{BINCODE_CONFIG_FIXED, BINCODE_CONFIG_VARIABLE},
};
use bincode::{Decode, Encode};
use serde::{de::DeserializeOwned, Serialize};
use std::io;

#[derive(Debug)]
pub enum WireFormatError {
    Serialization(String),
    Deserialization(String),
    Compression {
        inner: std::io::Error,
        snappy_error: Option<snap::Error>,
    },
    UnsupportedVersion(u32),
}

impl From<bincode::error::DecodeError> for WireFormatError {
    fn from(v: bincode::error::DecodeError) -> Self {
        Self::Deserialization(v.to_string())
    }
}

impl From<bincode::error::EncodeError> for WireFormatError {
    fn from(v: bincode::error::EncodeError) -> Self {
        Self::Serialization(v.to_string())
    }
}

impl From<rmp_serde::encode::Error> for WireFormatError {
    fn from(v: rmp_serde::encode::Error) -> Self {
        Self::Serialization(v.to_string())
    }
}

impl From<rmp_serde::decode::Error> for WireFormatError {
    fn from(v: rmp_serde::decode::Error) -> Self {
        Self::Deserialization(v.to_string())
    }
}

impl From<snap::Error> for WireFormatError {
    fn from(value: snap::Error) -> Self {
        WireFormatError::Compression {
            inner: io::Error::other(value.to_string()),
            snappy_error: Some(value),
        }
    }
}

impl From<std::io::Error> for WireFormatError {
    fn from(value: std::io::Error) -> Self {
        WireFormatError::Compression {
            inner: value,
            snappy_error: None,
        }
    }
}

pub fn to_wire_format_fixed<T>(message: &T, buffer: &mut [u8]) -> Result<usize, WireFormatError>
where
    T: Encode,
{
    Ok(bincode::encode_into_slice(
        message,
        buffer,
        BINCODE_CONFIG_FIXED,
    )?)
}

pub fn from_wire_format_fixed<T>(buffer: &[u8]) -> Result<T, WireFormatError>
where
    T: Decode<()>,
{
    let result = bincode::decode_from_slice(buffer, BINCODE_CONFIG_FIXED)?;

    Ok(result.0)
}

pub fn to_wire_format_variable<T>(
    item: &T,
    compression_type: CompressionType,
) -> Result<(usize, Vec<u8>), WireFormatError>
where
    T: Encode,
{
    let serialized = bincode::encode_to_vec(item, BINCODE_CONFIG_VARIABLE)?;
    let uncompressed_size = serialized.len();

    match compression_type {
        CompressionType::None => Ok((uncompressed_size, serialized)),
        CompressionType::Zstd { level } => {
            let compressed = zstd::bulk::compress(&serialized, level)?;
            Ok((uncompressed_size, compressed))
        }
        CompressionType::Snappy => {
            let compressed = snap::raw::Encoder::new().compress_vec(&serialized)?;
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
            )?;
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
    compressed_size: usize,
) -> Result<T, WireFormatError>
where
    T: Decode<()>,
{
    let decompressed = match compression_type {
        CompressionType::None => data.to_vec(),
        CompressionType::Zstd { .. } => zstd::bulk::decompress(data, compressed_size)
            .map_err(|e| io::Error::other(e.to_string()))?,
        CompressionType::Snappy => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|e| io::Error::other(e.to_string()))?,
        CompressionType::Brotli { .. } => {
            let mut decompressed = Vec::with_capacity(compressed_size);
            brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut decompressed)?;
            decompressed
        }
        CompressionType::Gzip { .. } => {
            use flate2::read::GzDecoder;
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::with_capacity(compressed_size);
            std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            decompressed
        }
    };

    let result = bincode::decode_from_slice(&decompressed, BINCODE_CONFIG_VARIABLE)?;

    Ok(result.0)
}

pub fn to_wire_format_variable_msgpack<T>(
    item: &T,
    compression_type: CompressionType,
) -> Result<(usize, Vec<u8>), WireFormatError>
where
    T: Serialize,
{
    let serialized = rmp_serde::to_vec(item)?;
    let uncompressed_size = serialized.len();

    match compression_type {
        CompressionType::None => Ok((uncompressed_size, serialized)),
        CompressionType::Zstd { level } => {
            let compressed = zstd::bulk::compress(&serialized, level)?;
            Ok((uncompressed_size, compressed))
        }
        CompressionType::Snappy => {
            let compressed = snap::raw::Encoder::new().compress_vec(&serialized)?;
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
            )?;
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

pub fn from_wire_format_variable_msgpack<T>(
    data: &[u8],
    compression_type: CompressionType,
    compressed_size: usize,
) -> Result<T, WireFormatError>
where
    T: DeserializeOwned,
{
    let decompressed = match compression_type {
        CompressionType::None => data.to_vec(),
        CompressionType::Zstd { .. } => zstd::bulk::decompress(data, compressed_size)
            .map_err(|e| io::Error::other(e.to_string()))?,
        CompressionType::Snappy => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|e| io::Error::other(e.to_string()))?,
        CompressionType::Brotli { .. } => {
            let mut decompressed = Vec::with_capacity(compressed_size);
            brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut decompressed)?;
            decompressed
        }
        CompressionType::Gzip { .. } => {
            use flate2::read::GzDecoder;
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::with_capacity(compressed_size);
            std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            decompressed
        }
    };

    let result = rmp_serde::from_slice(&decompressed)?;
    Ok(result)
}

pub fn to_wire_format_fixed_msgpack<T>(
    message: &T,
    buffer: &mut [u8],
) -> Result<usize, WireFormatError>
where
    T: Serialize,
{
    let serialized = rmp_serde::to_vec(message)?;

    if serialized.len() > buffer.len() {
        return Err(WireFormatError::Serialization(format!(
            "Serialized size {} exceeds buffer size {}",
            serialized.len(),
            buffer.len()
        )));
    }

    buffer[..serialized.len()].copy_from_slice(&serialized);
    Ok(serialized.len())
}

pub fn from_wire_format_fixed_msgpack<T>(buffer: &[u8]) -> Result<T, WireFormatError>
where
    T: DeserializeOwned,
{
    Ok(rmp_serde::from_slice(buffer)?)
}

