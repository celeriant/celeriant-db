#[derive(Debug, Clone)]
pub enum CodecError {
    Serialization(String),
    Deserialization(String),
    Compression(String),
}

impl From<bincode::error::DecodeError> for CodecError {
    fn from(v: bincode::error::DecodeError) -> Self {
        Self::Deserialization(v.to_string())
    }
}

impl From<bincode::error::EncodeError> for CodecError {
    fn from(v: bincode::error::EncodeError) -> Self {
        Self::Serialization(v.to_string())
    }
}

impl From<rmp_serde::encode::Error> for CodecError {
    fn from(v: rmp_serde::encode::Error) -> Self {
        Self::Serialization(v.to_string())
    }
}

impl From<rmp_serde::decode::Error> for CodecError {
    fn from(v: rmp_serde::decode::Error) -> Self {
        Self::Deserialization(v.to_string())
    }
}

impl From<snap::Error> for CodecError {
    fn from(value: snap::Error) -> Self {
        CodecError::Compression(value.to_string())
    }
}

impl From<std::io::Error> for CodecError {
    fn from(value: std::io::Error) -> Self {
        CodecError::Compression(value.to_string())
    }
}

impl From<super::compression::CompressionError> for CodecError {
    fn from(value: super::compression::CompressionError) -> Self {
        CodecError::Compression(value.to_string())
    }
}
