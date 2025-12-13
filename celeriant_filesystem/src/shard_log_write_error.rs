use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;

#[derive(Debug, Clone)]
pub enum ShardLogWriteError {
    IoError(String),
    SerializationError(WireFormatError),
}

impl From<WireFormatError> for ShardLogWriteError {
    fn from(error: WireFormatError) -> Self {
        ShardLogWriteError::SerializationError(error)
    }
}

impl From<GlommioError<()>> for ShardLogWriteError {
    fn from(error: GlommioError<()>) -> Self {
        ShardLogWriteError::IoError(error.to_string())
    }
}

impl From<std::io::Error> for ShardLogWriteError {
    fn from(error: std::io::Error) -> Self {
        ShardLogWriteError::IoError(error.to_string())
    }
}