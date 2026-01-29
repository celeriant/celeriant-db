use celeriant_client_glommio::ClientError;
use celeriant_msg::response::responses::FollowerRejection;

pub enum ReplicateToFollowerError {
    FollowerNetworkError(ClientError),
    FollowerRejected(FollowerRejection),
    FollowerServerError(String),
    FollowerUnexpectedResponse,
    FollowerTooFarBehind,
    SystemTimeError(std::time::SystemTimeError),
}

impl From<std::time::SystemTimeError> for ReplicateToFollowerError {
    fn from(e: std::time::SystemTimeError) -> Self {
        ReplicateToFollowerError::SystemTimeError(e)
    }
}

impl From<celeriant_client_glommio::ClientError> for ReplicateToFollowerError {
    fn from(e: celeriant_client_glommio::ClientError) -> Self {
        ReplicateToFollowerError::FollowerNetworkError(e)
    }
}