use std::io;

use bincode::{Decode, Encode};
use eventplanedb_structures::{
    compression_type::CompressionType, constants::BINCODE_CONFIG_VARIABLE,
};

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
