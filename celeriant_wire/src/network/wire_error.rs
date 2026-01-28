use crate::{codec::{codec_error::CodecError, compression}};

#[derive(Debug)]
pub enum WireError {
    NetworkError(std::io::Error),
    MessageTooLarge {
        message_length: u64,
        max_size_bytes: u64,
    },
    UnsupportedProtocol(u32),
    Codec(CodecError),
}

impl From<std::io::Error> for WireError {
    fn from(value: std::io::Error) -> Self {
        WireError::NetworkError(value)
    }
}

impl From<bincode::error::DecodeError> for WireError {
    fn from(e: bincode::error::DecodeError) -> Self {
        Self::Codec(CodecError::Deserialization(e.to_string()))
    }
}

impl From<rmp_serde::decode::Error> for WireError {
    fn from(v: rmp_serde::decode::Error) -> Self {
        Self::Codec(CodecError::Deserialization(v.to_string()))
    }
}

impl From<bincode::error::EncodeError> for WireError {
    fn from(e: bincode::error::EncodeError) -> Self {
        Self::Codec(CodecError::Serialization(e.to_string()))
    }
}

impl From<rmp_serde::encode::Error> for WireError {
    fn from(v: rmp_serde::encode::Error) -> Self {
        Self::Codec(CodecError::Serialization(v.to_string()))
    }
}

impl From<compression::CompressionError> for WireError {
    fn from(v: compression::CompressionError) -> Self {
        Self::Codec(CodecError::Compression(v.to_string()))
    }
}