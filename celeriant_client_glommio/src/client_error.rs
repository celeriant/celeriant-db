use std::time::Duration;

use celeriant_msg::response::responses::ErrorResponse;
use celeriant_wire::wire_error::WireError;
use glommio::GlommioError;

#[derive(Debug)]
pub enum ClientError {
    ConnectionFailed(glommio::GlommioError<()>),
    WireError(WireError),
    ProtocolError,
    CeleriantError(ErrorResponse),
    Timeout(Duration),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::ConnectionFailed(e) => write!(f, "Connection failed: {}", e),
            ClientError::WireError(e) => write!(f, "Wire error: {:?}", e),
            ClientError::ProtocolError => write!(f, "Protocol error"),
            ClientError::CeleriantError(e) => {
                write!(f, "Celeriant server error: {}", e.error_message)
            }
            ClientError::Timeout(duration) => write!(f, "Request timeout at {}ms", duration.as_millis()),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<WireError> for ClientError {
    fn from(e: WireError) -> Self {
        ClientError::WireError(e)
    }
}

impl From<GlommioError<()>> for ClientError {
    fn from(e: GlommioError<()>) -> Self {
        ClientError::ConnectionFailed(e)
    }
}