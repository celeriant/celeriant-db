use celeriant_client_glommio::ClientError;
use celeriant_msg::error_codes::REPLICATION_BATCH_WAL_SEQ_GAP;
use celeriant_msg::response::responses::{ErrorResponse, FollowerRejection};

#[derive(Debug)]
pub enum ReplicateToFollowerError {
    FollowerNetworkError(ClientError),
    FollowerRejected(FollowerRejection),
    /// Follower rejected the batch for non-contiguous wal_seqs (error code 6002).
    /// The follower responded over a healthy connection — this must NOT be
    /// classified as a network error or poison `follower_reachable`.
    FollowerBatchWalSeqGap(ErrorResponse),
    FollowerUnexpectedResponse,
    FollowerTooFarBehind,
    LockTimeout,
    SystemTimeError(std::time::SystemTimeError),
    BudgetExhausted,
}

impl From<std::time::SystemTimeError> for ReplicateToFollowerError {
    fn from(e: std::time::SystemTimeError) -> Self {
        ReplicateToFollowerError::SystemTimeError(e)
    }
}

impl From<celeriant_client_glommio::ClientError> for ReplicateToFollowerError {
    fn from(e: celeriant_client_glommio::ClientError) -> Self {
        match e {
            ClientError::CeleriantError(err) if err.error_code == REPLICATION_BATCH_WAL_SEQ_GAP => {
                ReplicateToFollowerError::FollowerBatchWalSeqGap(err)
            }
            e => ReplicateToFollowerError::FollowerNetworkError(e),
        }
    }
}