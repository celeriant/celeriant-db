use celeriant_crypto::CryptoError;
use celeriant_msg::read_wire_data_error::ReadWireDataError;
use celeriant_msg::response::responses::ErrorResponse;
use celeriant_wire::network::wire_error::WireError;

use crate::server_error::ServerError;

#[derive(Debug)]
pub enum ClientError {
    ConnectionFailed(std::io::Error),
    WireError(WireError),
    ReadError(ReadWireDataError),
    ProtocolError,
    /// Node is not the leader for this shard — writes must go to the leader.
    /// `leader_address` is provided when the follower knows who the current leader is.
    NotLeader { leader_address: Option<String>, error_message: String },
    Server(ServerError),
    /// Server requires client identity verification (error 10004).
    /// The client should call `identify()` before sending other requests.
    IdentityRequired,
    ConnectionTimeout,
    RequestTimeout,
    /// Identity verification error (nonce generation, signing, or verification failure)
    IdentityError(CryptoError),
}

impl ClientError {
    pub(crate) fn from_error_response(error: ErrorResponse) -> Self {
        if error.is_not_leader() {
            let leader_address = error.parse_leader_address();
            let error_message = error.error_message;
            ClientError::NotLeader { leader_address, error_message }
        } else if error.is_identity_required() {
            ClientError::IdentityRequired
        } else {
            ClientError::Server(ServerError::from(error))
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
            ClientError::Server(e) => write!(f, "{}", e),
            ClientError::IdentityRequired => write!(f, "Server requires client identity verification — call identify() first"),
            ClientError::RequestTimeout => write!(f, "Request timeout"),
            ClientError::ConnectionTimeout => write!(f, "Connection timeout"),
            ClientError::IdentityError(e) => write!(f, "Identity verification error: {}", e),
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

impl From<CryptoError> for ClientError {
    fn from(e: CryptoError) -> Self {
        ClientError::IdentityError(e)
    }
}
