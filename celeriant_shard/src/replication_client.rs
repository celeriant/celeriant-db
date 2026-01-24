use std::time::{SystemTime, UNIX_EPOCH};

use celeriant_client_glommio::CeleriantClient;
use celeriant_memcache::pending_commit_data::PendingCommitData;
use celeriant_msg::{
    process_requests::Request,
    process_responses::Response,
    request::requests::{ReplicationBatchItem, ReplicationBatchRequest},
};
use celeriant_wal::compression_type::CompressionType;

use crate::error::replication_error::ReplicationError;

pub type ReplicationResult = Result<(), ReplicationError>;

/// Trait for network replication operations.
///
/// This allows mocking network failures for testing rollback behavior.
#[allow(async_fn_in_trait)]
pub trait ReplicationClient {
    async fn replicate_to_follower(&mut self, batches: &[PendingCommitData]) -> Result<(), ReplicationError>;
    async fn replicate_to_s3(&mut self, batches: &[PendingCommitData]) -> Result<(), ReplicationError>;
}

pub struct StubReplicationClient;

impl ReplicationClient for StubReplicationClient {
    async fn replicate_to_follower(&mut self, _batches: &[PendingCommitData]) -> Result<(), ReplicationError> {
        glommio::timer::sleep(std::time::Duration::from_millis(30)).await;
        // TODO(network): Implement actual follower replication
        Ok(())
    }

    async fn replicate_to_s3(&mut self, _batches: &[PendingCommitData]) -> Result<(), ReplicationError> {
        glommio::timer::sleep(std::time::Duration::from_millis(230)).await;
        // TODO(network): Implement actual S3 replication
        Ok(())
    }
}

/// Glommio-based replication client that sends batches to a follower over TCP
pub struct GlommioReplicationClient {
    follower_address: String,
    shard_id: u64,
    client: Option<CeleriantClient>,
}

impl GlommioReplicationClient {
    pub fn new(follower_address: String, shard_id: u64) -> Self {
        Self {
            follower_address,
            shard_id,
            client: None,
        }
    }

    async fn ensure_connected(&mut self) -> Result<&mut CeleriantClient, ReplicationError> {
        if self.client.is_none() {
            self.client = Some(CeleriantClient::connect(&self.follower_address).await?);
        }
        Ok(self.client.as_mut().unwrap())
    }
}

impl ReplicationClient for GlommioReplicationClient {
    async fn replicate_to_follower(&mut self, batches: &[PendingCommitData]) -> Result<(), ReplicationError> {
        if batches.is_empty() {
            return Ok(());
        }

        let shard_id = self.shard_id;
        let client = self.ensure_connected().await?;
        let request = pending_to_replication_request(shard_id, batches);

        let response = client
            .send_request(&Request::ReplicationBatch(request), CompressionType::Snappy)
            .await?;

        match response {
            Response::ReplicationBatch(_) => Ok(()),
            Response::GenericError(err) => Err(ReplicationError::NetworkFailure(err.error_message)),
            _ => Err(ReplicationError::NetworkFailure("Unexpected response type".to_string())),
        }
    }

    async fn replicate_to_s3(&mut self, _batches: &[PendingCommitData]) -> Result<(), ReplicationError> {
        Err(ReplicationError::S3Unavailable)
    }
}

/// Convert PendingCommitData batches to ReplicationBatchRequest
fn pending_to_replication_request(shard_id: u64, batches: &[PendingCommitData]) -> ReplicationBatchRequest {
    let items: Vec<ReplicationBatchItem> = batches
        .iter()
        .flat_map(|batch| batch.pending_queue.iter())
        .map(|item| ReplicationBatchItem {
            metablock: item.metablock.clone(),
            datablock: item.datablock.clone(),
        })
        .collect();

    let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).expect("System time before Unix epoch");

    ReplicationBatchRequest {
        correlation_id: None,
        shard_id,
        leader_timestamp_ms: current_timestamp.as_millis() as u64,
        follower_too_far_behind: false,
        batches: items,
    }
}
