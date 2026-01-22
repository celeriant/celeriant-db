use celeriant_memcache::pending_commit_data::PendingCommitData;

use crate::error::replication_error::ReplicationError;

pub type ReplicationResult = Result<(), ReplicationError>;

/// Trait for network replication operations.
///
/// This allows mocking network failures for testing rollback behavior.
#[allow(async_fn_in_trait)]
pub trait ReplicationClient {
    async fn replicate_to_follower(&self, batches: &[PendingCommitData]) -> Result<(), ReplicationError>;
    async fn replicate_to_s3(&self, batches: &[PendingCommitData]) -> Result<(), ReplicationError>;
}

pub struct StubReplicationClient;

impl ReplicationClient for StubReplicationClient {
    async fn replicate_to_follower(&self, _batches: &[PendingCommitData]) -> Result<(), ReplicationError> {
        glommio::timer::sleep(std::time::Duration::from_millis(30)).await;
        // TODO(network): Implement actual follower replication
        Ok(())
    }

    async fn replicate_to_s3(&self, _batches: &[PendingCommitData]) -> Result<(), ReplicationError> {
        glommio::timer::sleep(std::time::Duration::from_millis(230)).await;
        // TODO(network): Implement actual S3 replication
        Ok(())
    }
}

