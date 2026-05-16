use std::cell::RefCell;

#[derive(Debug, Clone)]
pub enum CompressionError {
    ZstdDict(String),
}


impl std::fmt::Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZstdDict(e) => write!(f, "zstd-dict: {}", e),
        }
    }
}

impl std::error::Error for CompressionError {}

/// Precompiled zstd compressor + decompressor pair, built once per executor at shard boot.
pub struct DictCodec {
    compressor: RefCell<zstd::bulk::Compressor<'static>>,
    decompressor: RefCell<zstd::bulk::Decompressor<'static>>,
}

impl DictCodec {
    pub fn new(dict: &[u8], level: i32) -> Result<Self, CompressionError> {
        let compressor = zstd::bulk::Compressor::with_dictionary(level, dict)
            .map_err(|e| CompressionError::ZstdDict(e.to_string()))?;
        let decompressor = zstd::bulk::Decompressor::with_dictionary(dict)
            .map_err(|e| CompressionError::ZstdDict(e.to_string()))?;
        Ok(Self {
            compressor: RefCell::new(compressor),
            decompressor: RefCell::new(decompressor),
        })
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        self.compressor
            .borrow_mut()
            .compress(data)
            .map_err(|e| CompressionError::ZstdDict(e.to_string()))
    }

    pub fn decompress(&self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>, CompressionError> {
        self.decompressor
            .borrow_mut()
            .decompress(data, uncompressed_size)
            .map_err(|e| CompressionError::ZstdDict(e.to_string()))
    }
}

pub fn compress_with_dict(
    data: &[u8],
    level: i32,
    dict: &[u8],
) -> Result<Vec<u8>, CompressionError> {
    zstd::bulk::Compressor::with_dictionary(level, dict)
        .and_then(|mut c| c.compress(data))
        .map_err(|e| CompressionError::ZstdDict(e.to_string()))
}

pub fn decompress_with_dict(
    data: &[u8],
    uncompressed_size: usize,
    dict: &[u8],
) -> Result<Vec<u8>, CompressionError> {
    zstd::bulk::Decompressor::with_dictionary(dict)
        .and_then(|mut d| d.decompress(data, uncompressed_size))
        .map_err(|e| CompressionError::ZstdDict(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;

    #[test]
    fn compress_with_dict_roundtrip() {
        let data = br#"{"event":"page_view","url":"/home","user_id":42,"ts":1700000000}"#;
        let dict = BUILTIN_DICT_BYTES;
        let compressed = compress_with_dict(data, 3, dict).unwrap();
        let decompressed = decompress_with_dict(&compressed, data.len(), dict).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn compress_with_dict_empty_data_handled() {
        let dict = BUILTIN_DICT_BYTES;
        let compressed = compress_with_dict(b"", 3, dict).unwrap();
        let decompressed = decompress_with_dict(&compressed, 0, dict).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn decompress_with_dict_rejects_wrong_dict() {
        let data = br#"{"event":"page_view","url":"/home","user_id":42}"#;
        let good_dict = BUILTIN_DICT_BYTES;
        // A minimal valid zstd dict has a magic number (0xEC30A437 LE) header.
        // Using garbage bytes is sufficient to trigger a decompression error.
        let bad_dict = b"this is not a valid zstd dictionary at all";
        let compressed = compress_with_dict(data, 3, good_dict).unwrap();
        let result = decompress_with_dict(&compressed, data.len(), bad_dict);
        assert!(result.is_err(), "expected error when decompressing with wrong dict");
    }

    #[test]
    fn dict_codec_roundtrip_and_reuse() {
        // Build once, call twice — proves the precompiled CCtx is reusable.
        let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).unwrap();
        let data1 = br#"{"event":"page_view","url":"/home","user_id":42,"ts":1700000000}"#;
        let data2 = br#"{"event":"click","url":"/about","user_id":99,"ts":1700000001}"#;

        let c1 = codec.compress(data1).unwrap();
        let c2 = codec.compress(data2).unwrap();

        let d1 = codec.decompress(&c1, data1.len()).unwrap();
        let d2 = codec.decompress(&c2, data2.len()).unwrap();

        assert_eq!(d1.as_slice(), data1.as_slice());
        assert_eq!(d2.as_slice(), data2.as_slice());
    }

    #[test]
    fn to_byte_from_byte_roundtrip() {
        use celeriant_wal::compression_type::CompressionType as CT;

        assert_eq!(CT::ZstdDict.to_byte(), 1);
        assert_eq!(CT::None.to_byte(), 0);
        assert_eq!(CT::from_byte(1), Ok(CT::ZstdDict));
        assert_eq!(CT::from_byte(0), Ok(CT::None));
        assert!(CT::from_byte(2).is_err());
        assert!(CT::from_byte(5).is_err());
    }
}
