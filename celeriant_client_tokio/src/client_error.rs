use celeriant_crypto::CryptoError;
use celeriant_msg::read_wire_data_error::ReadWireDataError;
use celeriant_msg::response::responses::ErrorResponse;
use celeriant_wire::network::wire_error::WireError;

use crate::server_error::ServerError;

#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    ConnectionFailed(std::io::Error),
    WireError(WireError),
    ReadError(ReadWireDataError),
    ProtocolError,
    /// A response arrived bearing a different correlation id than the request that was sent
    CorrelationMismatch { sent: Option<u128>, received: Option<u128> },
    /// Node is not the leader for this shard — writes must go to the leader.
    /// `leader_address` is provided when the follower knows who the current leader is.
    NotLeader { leader_address: Option<String>, error_message: String },
    Server(ServerError),
    /// Server requires client identity verification (error 10004).
    /// The client should call `identify()` before sending other requests.
    IdentityRequired,
    /// Server is too busy to handle the request.
    /// The client should retry after a brief backoff.
    ServerBusy,
    ConnectionTimeout,
    /// The request was fully written and the node never answered. It may or may
    /// not have been applied, so it must never be re-sent anywhere.
    ConnectionLostAfterSend(std::io::Error),
    /// Gave up waiting for a pool slot to `address`. No node was contacted and
    /// no byte was sent, so retrying cannot duplicate the request.
    PoolTimeout { address: String },
    RequestTimeout,
    /// Identity verification error (nonce generation, signing, or verification failure)
    IdentityError(CryptoError),
    /// The caller named a shard range no shard can satisfy, so no request was
    /// built or sent.
    InvalidShardRange { start_shard: u64, max_shard_hint: u64 },
    /// Identify confirmed a compression dictionary by sha without resending its
    /// bytes, and nothing on this client can resolve that sha. Every later
    /// ZstdDict frame would fail, so the handshake fails instead.
    DictUnavailable { sha: String },
}

impl ClientError {
    pub(crate) fn from_error_response(error: ErrorResponse) -> Self {
        if error.is_not_leader() {
            let leader_address = error.parse_leader_address();
            let error_message = error.error_message;
            ClientError::NotLeader { leader_address, error_message }
        } else if error.is_identity_required() {
            ClientError::IdentityRequired
        } else if error.is_server_busy() {
            ClientError::ServerBusy
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
            ClientError::CorrelationMismatch { sent, received } => write!(
                f,
                "Correlation id mismatch: sent {sent:?}, received {received:?} — the connection returned another request's response"
            ),
            ClientError::NotLeader { leader_address: Some(addr), .. } => write!(f, "Not leader, redirect to {}", addr),
            ClientError::NotLeader { leader_address: None, .. } => write!(f, "Not leader, leader address unknown"),
            ClientError::Server(e) => write!(f, "{}", e),
            ClientError::IdentityRequired => write!(f, "Server requires client identity verification — call identify() first"),
            ClientError::ServerBusy => write!(f, "Server busy — retry after backoff"),
            ClientError::RequestTimeout => write!(f, "Request timeout"),
            ClientError::ConnectionTimeout => write!(f, "Connection timeout"),
            ClientError::ConnectionLostAfterSend(e) => {
                write!(f, "Connection lost after the request was sent; outcome unknown: {e}")
            }
            ClientError::PoolTimeout { address } => write!(
                f,
                "Pool timeout: waited for a connection to {address}; request not sent"
            ),
            ClientError::IdentityError(e) => write!(f, "Identity verification error: {}", e),
            ClientError::InvalidShardRange { start_shard, max_shard_hint } => write!(
                f,
                "Impossible shard range: max_shard_hint {max_shard_hint} is below start_shard {start_shard}"
            ),
            ClientError::DictUnavailable { sha } => write!(
                f,
                "Server confirmed compression dictionary {sha} without sending it, and it is not cached"
            ),
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
