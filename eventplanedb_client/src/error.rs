use std::fmt;

#[derive(Debug)]
pub enum ClientError {
    ConnectionTimeout,
    ConnectionFailed(String),
    RequestTimeout,
    ResponseTimeout,
    WriteError(String),
    ReadError(String),
    PoolExhausted,
    UnexpectedResponse,
    ProtocolError(String),
    ServerError(eventplanedb_structures::eventplanedb_error::EventPlaneDBError),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::ConnectionTimeout => write!(f, "Connection timeout"),
            ClientError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            ClientError::RequestTimeout => write!(f, "Request timeout"),
            ClientError::ResponseTimeout => write!(f, "Response timeout"),
            ClientError::WriteError(msg) => write!(f, "Write error: {}", msg),
            ClientError::ReadError(msg) => write!(f, "Read error: {}", msg),
            ClientError::PoolExhausted => write!(f, "Connection pool exhausted"),
            ClientError::UnexpectedResponse => write!(f, "Unexpected response type"),
            ClientError::ProtocolError(msg) => write!(f, "Protocol error: {}", msg),
            ClientError::ServerError(err) => write!(f, "Server error: {:?}", err),
        }
    }
}

impl std::error::Error for ClientError {}

impl ClientError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ClientError::ConnectionTimeout
                | ClientError::ConnectionFailed(_)
                | ClientError::RequestTimeout
                | ClientError::ResponseTimeout
                | ClientError::PoolExhausted
        )
    }
}

pub type ClientResult<T> = Result<T, ClientError>;