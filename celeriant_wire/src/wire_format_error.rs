#[derive(Debug, Clone)]
pub enum WireFormatError {
    Serialization(String),
    Deserialization(String),
    Compression(String),
    UnsupportedVersion(u32),
    ChecksumMismatch { expected: u32, actual: u32 },
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
        WireFormatError::Compression(value.to_string())
    }
}

impl From<std::io::Error> for WireFormatError {
    fn from(value: std::io::Error) -> Self {
        WireFormatError::Compression(value.to_string())
    }
}
