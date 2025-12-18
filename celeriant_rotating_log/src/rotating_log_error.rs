use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;


#[derive(Debug, Clone)]
pub enum RotatingLogError {
    InvalidPreallocatedBytes(u64),
    IoError(String),
    WireFormat(WireFormatError),
    HeaderCorrupted { log_id: Option<u64> },
    LogFileNotFound { log_id: u64 },
}

impl std::fmt::Display for RotatingLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPreallocatedBytes(b) => write!(f, "Invalid preallocated bytes: {}", b),
            Self::IoError(msg) => write!(f, "IO error: {}", msg),
            Self::WireFormat(e) => write!(f, "Wire format error: {:?}", e),
            Self::HeaderCorrupted { log_id } => match log_id {
                Some(id) => write!(f, "Header corrupted in log {}, repair required", id),
                None => write!(f, "Header corrupted, repair required"),
            },
            Self::LogFileNotFound { log_id } => write!(f, "Log file not found: {}", log_id),
        }
    }
}

impl std::error::Error for RotatingLogError {}

impl From<GlommioError<()>> for RotatingLogError {
    fn from(e: GlommioError<()>) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<WireFormatError> for RotatingLogError {
    fn from(e: WireFormatError) -> Self {
        Self::WireFormat(e)
    }
}

impl From<std::io::Error> for RotatingLogError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.to_string())
    }
}