use crate::error::{fetch_catchup_entries_error::FetchCatchupEntriesError, replication_rollback_failure::ReplicationRollbackFailure, replication_to_s3_error::ReplicateToS3Error};

#[derive(Debug, Clone)]
pub enum ReplicationError {
    RollbackInProgress,
    RollbackFailed(ReplicationRollbackFailure),
    ReplicationClientLockTimeoutError,
    ReplicateToS3Error(ReplicateToS3Error),
    ExtendedCatchupFailure(FetchCatchupEntriesError),
    LeaderFenced,
    BudgetExhausted,
}