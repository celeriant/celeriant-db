use std::time::{Duration, SystemTime, UNIX_EPOCH};

use celeriant_client_glommio::{CeleriantClient, ClientError};
use celeriant_msg::{
    process_requests::Request,
    process_responses::Response,
    request::requests::{ReplicationBatchItem, ReplicationBatchRequest},
    response::responses::{ReplicationResult},
};
use celeriant_wal::compression_type::CompressionType;

use crate::error::{replication_to_follower_error::ReplicateToFollowerError, replication_to_s3_error::ReplicateToS3Error};

#[allow(async_fn_in_trait)]
pub trait ReplicationClient {
    async fn replicate_to_follower(&mut self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError>;
    async fn replicate_to_s3(&mut self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error>;
}

pub struct StubReplicationClient;

impl ReplicationClient for StubReplicationClient {
    async fn replicate_to_follower(&mut self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError> {
        glommio::timer::sleep(std::time::Duration::from_millis(30)).await;
        Ok(())
    }

    async fn replicate_to_s3(&mut self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        glommio::timer::sleep(std::time::Duration::from_millis(230)).await;
        Ok(())
    }
}

pub struct GlommioReplicationClient {
    follower_address: String,
    shard_id: u64,
    client: Option<CeleriantClient>,
    connection_timeout: Option<std::time::Duration>,
    max_request_size: u64,
    max_response_size: u64,
}

impl GlommioReplicationClient {
    pub fn new(
        follower_address: String,
        connection_timeout: Option<Duration>,
        max_request_size: u64,
        max_response_size: u64, 
        shard_id: u64) -> Self {
        Self {
            follower_address,
            shard_id,
            client: None,
            connection_timeout,
            max_request_size,
            max_response_size,
        }
    }

    async fn ensure_connected(&mut self, reset: bool) -> Result<&mut CeleriantClient, ClientError> {
        if reset && let Some(client) = self.client.take() {
            client.close().await?;
        }
        if self.client.is_none() {
            self.client = Some(CeleriantClient::connect_with_timeout(&self.follower_address, self.connection_timeout, self.max_request_size, self.max_response_size).await?);
        }
        Ok(self.client.as_mut().unwrap())
    }
}

impl ReplicationClient for GlommioReplicationClient {
    async fn replicate_to_follower(&mut self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError> {
        if batches.is_empty() {
            return Ok(());
        }

        let shard_id = self.shard_id;
        let client = self.ensure_connected(false).await?;

        let replication_request = ReplicationBatchRequest {
            correlation_id: None,
            shard_id,
            leader_timestamp_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            follower_too_far_behind: false,
            batches,
        };

        let mut request = Request::ReplicationBatch(replication_request);

        let response = match client.send_request(&request, CompressionType::Snappy).await
        {
            Ok(r) => r,
            Err(_) => {
                let client = self.ensure_connected(true).await?;
                if let Request::ReplicationBatch(ref mut req) = request {
                    req.leader_timestamp_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
                }
                client.send_request(&request, CompressionType::Snappy).await?
            }
        };

        match response {
            Response::ReplicationBatch(resp) => match resp.result {
                ReplicationResult::Success { .. } => Ok(()),
                ReplicationResult::Rejected(rejection) => Err(ReplicateToFollowerError::FollowerRejected(rejection)),
            },
            Response::GenericError(err) => {
                Err(ReplicateToFollowerError::FollowerServerError(err.error_message))
            }
            _ => Err(ReplicateToFollowerError::FollowerUnexpectedResponse),
        }
    }

    async fn replicate_to_s3(&mut self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        Ok(())
        //TODO: S3 implementation
        //Err(ReplicateToS3Error::S3Unavailable)
    }
}