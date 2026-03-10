use crate::codec::{codec_error::CodecError, compression::CompressionError};

#[derive(Debug, Clone)]
pub enum DiskFormatError {
    DatablockExpected,
    ExternalDataMissing,
    ChecksumMismatch { expected: u32, actual: u32 },
    UnsupportedVersion(u32),
    Codec(CodecError),
    HeaderSizeMismatch { expected: usize, actual: usize },
    JsonSerialize(String),
    JsonDeserialize(String),
}

impl From<CodecError> for DiskFormatError {
    fn from(e: CodecError) -> Self {
        Self::Codec(e)
    }
}

impl From<bincode::error::DecodeError> for DiskFormatError {
    fn from(e: bincode::error::DecodeError) -> Self {
        Self::Codec(CodecError::Deserialization(e.to_string()))
    }
}

impl From<CompressionError> for DiskFormatError {
    fn from(e: CompressionError) -> Self {
        Self::Codec(CodecError::Compression(e.to_string()))
    }
}
