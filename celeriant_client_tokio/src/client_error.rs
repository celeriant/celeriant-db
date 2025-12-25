use celeriant_msg::response::responses::ErrorResponse;
use celeriant_wire::wire_error::WireError;

#[derive(Debug)]
pub enum ClientError {
    ConnectionFailed(std::io::Error),
    WireError(WireError),
    ProtocolError,
    CeleriantError(ErrorResponse),
    ConnectionTimeout,
    RequestTimeout,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::ConnectionFailed(e) => write!(f, "Connection failed: {}", e),
            ClientError::WireError(e) => write!(f, "Wire error: {:?}", e),
            ClientError::ProtocolError => write!(f, "Protocol error"),
            ClientError::CeleriantError(e) => write!(f, "CeleriantError server error {}: {}", e.error_message, e.error_message),
            ClientError::RequestTimeout => write!(f, "Request timeout"),
            ClientError::ConnectionTimeout => write!(f, "Connection timeout"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<WireError> for ClientError {
    fn from(e: WireError) -> Self {
        ClientError::WireError(e)
    }
}