use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use celeriant_client_glommio::{CeleriantClient, ClientError};
use celeriant_distributed::paths::fallback_batch_path;
use celeriant_msg::{
    process_requests::Request,
    process_responses::Response,
    request::requests::{ReplicationBatchItem, ReplicationBatchRequest},
    response::responses::{ReplicationResult},
};
use celeriant_wal::{compression_type::CompressionType, s3::fallback_batch::{FallbackBatch, FallbackItem}};
use tracing::warn;

use crate::error::{replication_to_follower_error::ReplicateToFollowerError, replication_to_s3_error::ReplicateToS3Error};
use crate::s3_uploader::S3Uploader;

#[allow(async_fn_in_trait)]
pub trait ReplicationClient {
    fn set_follower_address(&mut self, address: Option<String>);
    async fn replicate_to_follower(&mut self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError>;
    async fn replicate_to_s3(&mut self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error>;
}

pub struct StubReplicationClient;

impl ReplicationClient for StubReplicationClient {
    fn set_follower_address(&mut self, _address: Option<String>) {}

    async fn replicate_to_follower(&mut self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError> {
        glommio::timer::sleep(std::time::Duration::from_millis(30)).await;
        Ok(())
    }

    async fn replicate_to_s3(&mut self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        glommio::timer::sleep(std::time::Duration::from_millis(230)).await;
        Ok(())
    }
}

pub struct GlommioReplicationClient<S: S3Uploader> {
    follower_address: Option<String>,
    shard_id: u64,
    client: Option<CeleriantClient>,
    connection_timeout: Option<std::time::Duration>,
    max_request_size: u64,
    max_response_size: u64,
    s3_uploader: Option<S>,
}

impl<S: S3Uploader> GlommioReplicationClient<S> {
    pub fn new(
        follower_address: Option<String>,
        connection_timeout: Option<Duration>,
        max_request_size: u64,
        max_response_size: u64,
        shard_id: u64,
        s3_uploader: Option<S>) -> Self {
        // Validate shard_id fits in u32 (FallbackBatch.shard_id is u32)
        assert!(shard_id <= u32::MAX as u64, "shard_id {} exceeds u32::MAX", shard_id);

        Self {
            follower_address,
            shard_id,
            client: None,
            connection_timeout,
            max_request_size,
            max_response_size,
            s3_uploader,
        }
    }

    async fn ensure_connected(&mut self, reset: bool) -> Result<&mut CeleriantClient, ClientError> {
        if reset && let Some(client) = self.client.take() {
            client.close().await?;
        }
        if self.client.is_none() {
            let address = self.follower_address.as_deref()
                .ok_or(ClientError::NoAddress)?;
            self.client = Some(CeleriantClient::connect_with_timeout(address, self.connection_timeout, self.max_request_size, self.max_response_size).await?);
        }
        Ok(self.client.as_mut().unwrap())
    }
}

impl<S: S3Uploader> ReplicationClient for GlommioReplicationClient<S> {
    fn set_follower_address(&mut self, address: Option<String>) {
        if self.follower_address != address {
            self.follower_address = address;
            self.client = None;
        }
    }

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

    async fn replicate_to_s3(&mut self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        if batches.is_empty() {
            return Ok(());
        }

        let s3_uploader = self.s3_uploader.as_ref()
            .ok_or(ReplicateToS3Error::S3NotConfigured)?;

        let fallback_index = batches[0].metablock.wal_index;
        let end_wal_index = batches.last().unwrap().metablock.wal_index;
        let shard_id = self.shard_id as u32;

        let items: Vec<FallbackItem> = batches.into_iter()
            .map(|b| FallbackItem {
                metablock: b.metablock,
                datablock: b.datablock,
            })
            .collect();

        let fallback_batch = FallbackBatch {
            fallback_index,
            end_wal_index,
            shard_id,
            items,
        };

        let batch_count = fallback_batch.items.len();
        let serialized = celeriant_wire::disk::versioned_block::serialize_versioned_message_heap(
            &fallback_batch,
            celeriant_wal::constants::WIRE_VERSION_S3_FALLBACK_BATCH,
        ).map_err(|e| ReplicateToS3Error::SerializationFailed(e.to_string()))?;

        let total_bytes = serialized.len();
        let path = fallback_batch_path(shard_id, fallback_index, end_wal_index);

        warn!(
            "S3 fallback triggered: shard_id={}, batch_count={}, bytes={}, fallback_index={}, end_wal_index={}, path={}",
            shard_id, batch_count, total_bytes, fallback_index, end_wal_index, path
        );

        s3_uploader.upload(path, Bytes::from(serialized)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::metablocks::metablock_kind::MetablockKind;
    use celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch;
    use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
    use celeriant_wal::metablocks::metablock::Metablock;
    use celeriant_wal::datablocks::datablock::Datablock;
    use celeriant_wal::datablocks::datablock_kind::DatablockKind;
    use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
    use glommio::LocalExecutor;
    use std::cell::RefCell;
    use std::rc::Rc;

    type MockCalls = Rc<RefCell<Vec<(String, Bytes)>>>;

    struct MockS3Uploader {
        calls: MockCalls,
        result: RefCell<Option<Result<(), ReplicateToS3Error>>>,
    }

    impl MockS3Uploader {
        fn new() -> (Self, MockCalls) {
            let calls = Rc::new(RefCell::new(Vec::new()));
            (Self { calls: calls.clone(), result: RefCell::new(None) }, calls)
        }

        fn with_result(result: Result<(), ReplicateToS3Error>) -> Self {
            Self {
                calls: Rc::new(RefCell::new(Vec::new())),
                result: RefCell::new(Some(result)),
            }
        }
    }

    impl S3Uploader for MockS3Uploader {
        async fn upload(&self, path: String, data: Bytes) -> Result<(), ReplicateToS3Error> {
            self.calls.borrow_mut().push((path, data));
            self.result.borrow_mut().take().unwrap_or(Ok(()))
        }
    }

    fn create_test_metablock(wal_index: u64) -> Metablock {
        let aggregate_key = AggregateKey::new(1, 2, 3);
        Metablock {
            wal_index,
            server_timestamp: 1000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key,
                event_batch_index: 10,
                min_event_batch_index: 1,
                min_client_event_index: 1,
                max_client_event_index: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_index: 1,
                max_event_index: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
        }
    }

    fn create_test_datablock() -> Datablock {
        Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 10,
                events: vec![],
            }),
        }
    }

    #[test]
    fn empty_batches_returns_ok() {
        LocalExecutor::default().run(async {
            let (mock_uploader, calls) = MockS3Uploader::new();

            let mut client = GlommioReplicationClient {
                follower_address: Some("127.0.0.1:8080".to_string()),
                shard_id: 7,
                client: None,
                connection_timeout: None,
                max_request_size: 1024,
                max_response_size: 1024,
                s3_uploader: Some(mock_uploader),
            };

            let result = client.replicate_to_s3(vec![]).await;

            assert!(result.is_ok());
            assert_eq!(calls.borrow().len(), 0);
        });
    }

    #[test]
    fn no_uploader_returns_s3_not_configured() {
        LocalExecutor::default().run(async {
            let mut client: GlommioReplicationClient<MockS3Uploader> = GlommioReplicationClient {
                follower_address: Some("127.0.0.1:8080".to_string()),
                shard_id: 7,
                client: None,
                connection_timeout: None,
                max_request_size: 1024,
                max_response_size: 1024,
                s3_uploader: None,
            };

            let batches = vec![ReplicationBatchItem {
                metablock: create_test_metablock(42),
                datablock: None,
            }];

            let result = client.replicate_to_s3(batches).await;

            assert!(matches!(result, Err(ReplicateToS3Error::S3NotConfigured)));
        });
    }

    #[test]
    fn upload_called_with_correct_path_and_data() {
        LocalExecutor::default().run(async {
            let (mock_uploader, calls) = MockS3Uploader::new();

            let mut client = GlommioReplicationClient {
                follower_address: Some("127.0.0.1:8080".to_string()),
                shard_id: 7,
                client: None,
                connection_timeout: None,
                max_request_size: 1024,
                max_response_size: 1024,
                s3_uploader: Some(mock_uploader),
            };

            let batches = vec![
                ReplicationBatchItem {
                    metablock: create_test_metablock(42),
                    datablock: Some(create_test_datablock()),
                },
                ReplicationBatchItem {
                    metablock: create_test_metablock(43),
                    datablock: None,
                },
            ];

            let result = client.replicate_to_s3(batches).await;

            assert!(result.is_ok());

            let calls = calls.borrow();
            assert_eq!(calls.len(), 1);

            let (path, data) = &calls[0];
            assert_eq!(path, "cluster/fallback/shard_007/batch_000000042_000000043.bin");

            let deserialized = celeriant_wire::disk::versioned_block::deserialise_fallback_batch(data)
                .expect("should deserialize");
            assert_eq!(deserialized.shard_id, 7);
            assert_eq!(deserialized.fallback_index, 42);
            assert_eq!(deserialized.end_wal_index, 43);
            assert_eq!(deserialized.items.len(), 2);
        });
    }

    #[test]
    fn fallback_index_is_first_wal_index() {
        LocalExecutor::default().run(async {
            let (mock_uploader, calls) = MockS3Uploader::new();

            let mut client = GlommioReplicationClient {
                follower_address: Some("127.0.0.1:8080".to_string()),
                shard_id: 5,
                client: None,
                connection_timeout: None,
                max_request_size: 1024,
                max_response_size: 1024,
                s3_uploader: Some(mock_uploader),
            };

            let batches = vec![
                ReplicationBatchItem {
                    metablock: create_test_metablock(100),
                    datablock: None,
                },
                ReplicationBatchItem {
                    metablock: create_test_metablock(101),
                    datablock: None,
                },
                ReplicationBatchItem {
                    metablock: create_test_metablock(102),
                    datablock: None,
                },
            ];

            let result = client.replicate_to_s3(batches).await;

            assert!(result.is_ok());

            let calls = calls.borrow();
            assert_eq!(calls.len(), 1);

            let (path, data) = &calls[0];
            assert_eq!(path, "cluster/fallback/shard_005/batch_000000100_000000102.bin");

            let deserialized = celeriant_wire::disk::versioned_block::deserialise_fallback_batch(data)
                .expect("should deserialize");
            assert_eq!(deserialized.fallback_index, 100);
            assert_eq!(deserialized.end_wal_index, 102);
            assert_eq!(deserialized.items[0].metablock.wal_index, 100);
            assert_eq!(deserialized.items[1].metablock.wal_index, 101);
            assert_eq!(deserialized.items[2].metablock.wal_index, 102);
        });
    }

    #[test]
    fn s3_error_propagated() {
        LocalExecutor::default().run(async {
            let mock_uploader = MockS3Uploader::with_result(
                Err(ReplicateToS3Error::S3PutFailed {
                    path: "test".to_string(),
                    message: "network error".to_string(),
                })
            );

            let mut client = GlommioReplicationClient {
                follower_address: Some("127.0.0.1:8080".to_string()),
                shard_id: 7,
                client: None,
                connection_timeout: None,
                max_request_size: 1024,
                max_response_size: 1024,
                s3_uploader: Some(mock_uploader),
            };

            let batches = vec![ReplicationBatchItem {
                metablock: create_test_metablock(42),
                datablock: None,
            }];

            let result = client.replicate_to_s3(batches).await;

            assert!(matches!(result, Err(ReplicateToS3Error::S3PutFailed { .. })));
        });
    }
}