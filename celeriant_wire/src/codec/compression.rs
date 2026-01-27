use celeriant_wal::compression_type::CompressionType;

#[derive(Debug, Clone)]
pub enum CompressionError {
    Zstd(String),
    Snappy(String),
    Brotli(String),
    Gzip(String),
}

impl std::fmt::Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Zstd(e) => write!(f, "zstd: {}", e),
            Self::Snappy(e) => write!(f, "snappy: {}", e),
            Self::Brotli(e) => write!(f, "brotli: {}", e),
            Self::Gzip(e) => write!(f, "gzip: {}", e),
        }
    }
}

impl std::error::Error for CompressionError {}

#[inline]
pub fn compress(data: &[u8], compression: CompressionType) -> Result<Vec<u8>, CompressionError> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd { level } => {
            zstd::bulk::compress(data, level).map_err(|e| CompressionError::Zstd(e.to_string()))
        }
        CompressionType::Snappy => snap::raw::Encoder::new()
            .compress_vec(data)
            .map_err(|e| CompressionError::Snappy(e.to_string())),
        CompressionType::Brotli { level } => {
            let mut out = Vec::new();
            let params = brotli::enc::BrotliEncoderParams {
                quality: level,
                ..Default::default()
            };
            brotli::BrotliCompress(&mut std::io::Cursor::new(data), &mut out, &params)
                .map_err(|e| CompressionError::Brotli(e.to_string()))?;
            Ok(out)
        }
        CompressionType::Gzip { level } => {
            use flate2::{write::GzEncoder, Compression};
            let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level as u32));
            std::io::Write::write_all(&mut encoder, data)
                .map_err(|e| CompressionError::Gzip(e.to_string()))?;
            encoder.finish().map_err(|e| CompressionError::Gzip(e.to_string()))
        }
    }
}

#[inline]
pub fn decompress(
    data: &[u8],
    compression: CompressionType,
    uncompressed_size: usize,
) -> Result<Vec<u8>, CompressionError> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd { .. } => zstd::bulk::decompress(data, uncompressed_size)
            .map_err(|e| CompressionError::Zstd(e.to_string())),
        CompressionType::Snappy => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|e| CompressionError::Snappy(e.to_string())),
        CompressionType::Brotli { .. } => {
            let mut out = Vec::with_capacity(uncompressed_size);
            brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut out)
                .map_err(|e| CompressionError::Brotli(e.to_string()))?;
            Ok(out)
        }
        CompressionType::Gzip { .. } => {
            use flate2::read::GzDecoder;
            let mut decoder = GzDecoder::new(data);
            let mut out = Vec::with_capacity(uncompressed_size);
            std::io::Read::read_to_end(&mut decoder, &mut out)
                .map_err(|e| CompressionError::Gzip(e.to_string()))?;
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn roundtrip_all() {
        let data = b"hello world, this is some test data for compression";

        for ct in all_compression_types() {
            let compressed = compress(data, ct).unwrap();
            let decompressed = decompress(&compressed, ct, data.len()).unwrap();
            assert_eq!(data.as_slice(), decompressed.as_slice(), "failed for {:?}", ct);
        }
    }

    #[test]
    fn compressible_data_shrinks() {
        let data: Vec<u8> = "x".repeat(10_000).into_bytes();

        for ct in all_compression_types() {
            if matches!(ct, CompressionType::None) {
                continue;
            }
            let compressed = compress(&data, ct).unwrap();
            assert!(
                compressed.len() < data.len(),
                "{:?} did not compress: {} >= {}",
                ct,
                compressed.len(),
                data.len()
            );
        }
    }

    #[test]
    fn empty_data() {
        for ct in all_compression_types() {
            let compressed = compress(&[], ct).unwrap();
            let decompressed = decompress(&compressed, ct, 0).unwrap();
            assert!(decompressed.is_empty(), "failed for {:?}", ct);
        }
    }
}
