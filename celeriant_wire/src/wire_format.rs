use std::io::Cursor;

use bincode::{Decode, Encode};
use celeriant_wal::compression_type::CompressionType;
use serde::{Serialize, de::DeserializeOwned};

use crate::wire_format_error::WireFormatError;

static BINCODE_CONFIG_FIXED: bincode::config::Configuration<
    bincode::config::LittleEndian,
    bincode::config::Fixint,
> = bincode::config::standard()
    .with_fixed_int_encoding() // Force fixed-length integers
    .with_little_endian();

static BINCODE_CONFIG_VARIABLE: bincode::config::Configuration<
    bincode::config::LittleEndian,
    bincode::config::Varint,
> = bincode::config::standard()
    .with_variable_int_encoding()
    .with_little_endian();

pub fn bincode_fixed_serialise<T>(message: &T, buffer: &mut [u8]) -> Result<usize, WireFormatError>
where
    T: Encode,
{
    bincode::encode_into_slice(message, buffer, BINCODE_CONFIG_FIXED).map_err(|e| e.into())
}

pub fn bincode_fixed_deserialise<T>(buffer: &[u8]) -> Result<(T, usize), WireFormatError>
where
    T: Decode<()>,
{
    bincode::decode_from_slice(buffer, BINCODE_CONFIG_FIXED).map_err(|e| e.into())
}

pub fn bincode_variable_serialise<T>(message: &T, compression_type: CompressionType) -> Result<(usize, Vec<u8>), WireFormatError>
where
    T: Encode,
{
    let serialized = bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE)?;
    compress_variable(serialized, compression_type).map_err(|e|e.into())
}

pub fn bincode_variable_serialise_no_compression<T>(message: &T) -> Result<Vec<u8>, WireFormatError>
where
    T: Encode,
{
    bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE).map_err(|e| e.into())
}

pub fn compress_variable(
    serialized: Vec<u8>,
    compression_type: CompressionType,
) -> Result<(usize, Vec<u8>), WireFormatError>
{
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

pub fn decompress_variable(
    data: &[u8],
    compression_type: CompressionType,
    uncompressed_size: usize,
) -> Result<Vec<u8>, WireFormatError> {

    let decompressed = match compression_type {
        CompressionType::None => data.to_vec(),
        CompressionType::Zstd { .. } => zstd::bulk::decompress(data, uncompressed_size)
            .map_err(|e| std::io::Error::other(e.to_string()))?,
        CompressionType::Snappy => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|e| std::io::Error::other(e.to_string()))?,
        CompressionType::Brotli { .. } => {
            let mut decompressed = Vec::with_capacity(uncompressed_size);
            brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut decompressed)?;
            decompressed
        }
        CompressionType::Gzip { .. } => {
            use flate2::read::GzDecoder;
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::with_capacity(uncompressed_size);
            std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            decompressed
        }
    };

    Ok(decompressed)
}

/// Deserialize and decompress from wire format
pub fn bincode_variable_deserialise<T>(
    data: &[u8],
    compression_type: CompressionType,
    uncompressed_size: usize,
) -> Result<T, WireFormatError>
where
    T: Decode<()>,
{
    if compression_type == CompressionType::None {
        let result = bincode::decode_from_slice(data, BINCODE_CONFIG_VARIABLE)?;
        return Ok(result.0);
    }

    let decompressed = decompress_variable(data, compression_type, uncompressed_size)?;

    let result = bincode::decode_from_slice(&decompressed, BINCODE_CONFIG_VARIABLE)?;

    Ok(result.0)
}

pub fn msgpack_variable_serialise<T>(
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

pub fn msgpack_variable_deserialise<T>(
    data: &[u8],
    compression_type: CompressionType,
    uncompressed_size: usize,
) -> Result<T, WireFormatError>
where
    T: DeserializeOwned,
{
    let decompressed = match compression_type {
        CompressionType::None => data.to_vec(),
        CompressionType::Zstd { .. } => zstd::bulk::decompress(data, uncompressed_size)
            .map_err(|e| std::io::Error::other(e.to_string()))?,
        CompressionType::Snappy => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|e| std::io::Error::other(e.to_string()))?,
        CompressionType::Brotli { .. } => {
            let mut decompressed = Vec::with_capacity(uncompressed_size);
            brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut decompressed)?;
            decompressed
        }
        CompressionType::Gzip { .. } => {
            use flate2::read::GzDecoder;
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::with_capacity(uncompressed_size);
            std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            decompressed
        }
    };

    let result = rmp_serde::from_slice(&decompressed)?;
    Ok(result)
}

pub fn msgpack_fixed_serialise<T>(
    message: &T,
    buffer: &mut [u8],
) -> Result<usize, WireFormatError>
where
    T: Serialize,
{
    let mut cursor = Cursor::new(buffer);
    rmp_serde::encode::write(&mut cursor, message)?;
    Ok(cursor.position() as usize)
}

pub fn msgpack_fixed_deserialise<T>(buffer: &[u8]) -> Result<T, WireFormatError>
where
    T: DeserializeOwned,
{
    Ok(rmp_serde::from_slice(buffer)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{Decode, Encode};
    use celeriant_wal::datablocks::{datablock_aggregate_event::DatablockAggregateEvent, datablock_aggregate_event_batch::DatablockAggregateEventBatch};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
    struct TestMessage {
        id: u64,
        name: String,
        values: Vec<i32>,
        flag: bool,
        #[serde(rename = "ui", default)]
        user_id: Option<u128>,
    }

    fn sample_message() -> TestMessage {
        TestMessage {
            id: 12345,
            name: "test message".to_string(),
            values: vec![1, 2, 3, 4, 5],
            flag: true,
            user_id: None,
        }
    }

    fn all_compression_types() -> Vec<CompressionType> {
        vec![
            CompressionType::None,
            CompressionType::Zstd { level: 3 },
            CompressionType::Snappy,
            CompressionType::Brotli { level: 4 },
            CompressionType::Gzip { level: 6 },
        ]
    }

    #[test]
    fn test_fixed_bincode_roundtrip() {
        let original = sample_message();
        let mut buffer = [0u8; 1024];

        let written = bincode_fixed_serialise(&original, &mut buffer).unwrap();
        let decoded: TestMessage = bincode_fixed_deserialise(&buffer[..written]).unwrap().0;

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_variable_bincode_roundtrip_all_compression() {
        let original = sample_message();

        for compression in all_compression_types() {

            let (uncompressed_size, encoded) = bincode_variable_serialise(&original, compression).unwrap();

            let decoded: TestMessage =
                bincode_variable_deserialise(&encoded, compression, uncompressed_size).unwrap();

            assert_eq!(original, decoded, "Failed for {:?}", compression);
        }
    }

    #[test]
    fn test_fixed_msgpack_roundtrip() {
        let original = sample_message();
        let mut buffer = [0u8; 1024];

        let written = msgpack_fixed_serialise(&original, &mut buffer).unwrap();
        let decoded: TestMessage = msgpack_fixed_deserialise(&buffer[..written]).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_variable_msgpack_roundtrip_all_compression_event_batches() {
        let original = DatablockAggregateEventBatch {
            event_batch_index: 1,
            events: vec![
                DatablockAggregateEvent {
                    event_index:10,
                    event_id:Some(1001),
                    event_timestamp:1_000,
                    event_value:std::sync::Arc::new(vec![1,2,3]),
                    iv:None, 
                    client_event_index: 0, 
                    event_type_major: 1, 
                    event_type_minor: 0 },
            ],
        };

        for compression in all_compression_types() {
            let (uncompressed_size, encoded) =
                msgpack_variable_serialise(&original, compression).unwrap();

            let decoded: DatablockAggregateEventBatch =
                msgpack_variable_deserialise(&encoded, compression, uncompressed_size)
                    .unwrap();

            assert_eq!(original.event_batch_index, decoded.event_batch_index, "Failed for {:?}", compression);
        }
    }

    #[test]
    fn test_variable_msgpack_roundtrip_all_compression() {
        let original = sample_message();

        for compression in all_compression_types() {
            let (uncompressed_size, encoded) =
                msgpack_variable_serialise(&original, compression).unwrap();

            let decoded: TestMessage =
                msgpack_variable_deserialise(&encoded, compression, uncompressed_size)
                    .unwrap();

            assert_eq!(original, decoded, "Failed for {:?}", compression);
        }
    }

    #[test]
    fn test_fixed_bincode_buffer_too_small() {
        let original = sample_message();
        let mut buffer = [0u8; 4]; // Too small

        let result = bincode_fixed_serialise(&original, &mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_fixed_msgpack_buffer_too_small() {
        let original = sample_message();
        let mut buffer = [0u8; 4]; // Too small

        let result = msgpack_fixed_serialise(&original, &mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_struct_roundtrip() {
        #[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
        struct Empty {}

        let original = Empty {};

        // Fixed bincode
        let mut buffer = [0u8; 64];
        let written = bincode_fixed_serialise(&original, &mut buffer).unwrap();
        let decoded: Empty = bincode_fixed_deserialise(&buffer[..written]).unwrap().0;
        assert_eq!(original, decoded);

        // Variable bincode        
            let encoded = bincode_variable_serialise_no_compression(&original).unwrap();

        let decoded: Empty =
            bincode_variable_deserialise(&encoded, CompressionType::None, 0).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_large_payload_with_compression() {
        let original = TestMessage {
            id: 999,
            name: "x".repeat(10_000), // Large repetitive data compresses well
            values: (0..1000).collect(),
            flag: false,
            user_id: Some(32423423423),
        };

        for compression in all_compression_types() {
            let (uncompressed_size, encoded) = bincode_variable_serialise(&original, compression).unwrap();

            let decoded: TestMessage =
                bincode_variable_deserialise(&encoded, compression, uncompressed_size).unwrap();

            assert_eq!(original, decoded, "Failed for {:?}", compression);

            // Verify compression actually reduces size for compressible data
            if !matches!(compression, CompressionType::None) {
                assert!(
                    encoded.len() < uncompressed_size,
                    "Expected compression for {:?}",
                    compression
                );
            }
        }
    }
}
