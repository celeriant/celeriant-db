use celeriant_msg::read_wire_data_error::ReadWireDataError;
use celeriant_msg::response::responses::ErrorResponse;
use celeriant_wire::network::wire_error::WireError;

#[derive(Debug)]
pub enum ClientError {
    ConnectionFailed(std::io::Error),
    WireError(WireError),
    ReadError(ReadWireDataError),
    ProtocolError,
    /// Node is not the leader for this shard — writes must go to the leader.
    /// `leader_address` is provided when the follower knows who the current leader is.
    NotLeader { leader_address: Option<String>, error: ErrorResponse },
    CeleriantError(ErrorResponse),
    ConnectionTimeout,
    RequestTimeout,
}

impl ClientError {
    pub(crate) fn from_error_response(error: ErrorResponse) -> Self {
        if error.is_not_leader() {
            let leader_address = error.parse_leader_address();
            ClientError::NotLeader { leader_address, error }
        } else {
            ClientError::CeleriantError(error)
        }
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::ConnectionFailed(e) => write!(f, "Connection failed: {}", e),
            ClientError::WireError(e) => write!(f, "Wire error: {:?}", e),
            ClientError::ReadError(e) => write!(f, "Read error: {:?}", e),
            ClientError::ProtocolError => write!(f, "Protocol error"),
            ClientError::NotLeader { leader_address: Some(addr), .. } => write!(f, "Not leader, redirect to {}", addr),
            ClientError::NotLeader { leader_address: None, .. } => write!(f, "Not leader, leader address unknown"),
            ClientError::CeleriantError(e) => write!(f, "Server error {}: {}", e.error_code, e.error_message),
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

impl From<ReadWireDataError> for ClientError {
    fn from(e: ReadWireDataError) -> Self {
        ClientError::ReadError(e)
    }
}