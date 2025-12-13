use std::{num::NonZeroUsize, rc::Rc};

use celeriant_msg::{
    process_requests::Request, request::requests::{
            DeleteRequest, ExistsRequest, ListAggregatesRequest, ListOrganisationsRequest, ReadRequest, TrimStartRequest, WriteRequest
        }, response::responses::{ExistsResponse, ListAggregatesResponse, ListOrganisationsResponse, ReadResponse, SuccessResponse, WriteResponse},
    
};

use crate::{
    node_config::NodeConfig, read_operations::read_error::ReadError, read_write_error::ReadWriteError, watch::watched_aggregates::WatchedAggregates, write_operations::write_error::WriteError,
};

pub struct LocalAggregate {
    pub watched_aggregates: Rc<WatchedAggregates>,
    node_config: NodeConfig,
}

#[allow(async_fn_in_trait)]
pub trait LocalAggregateTrait {
    async fn close(&self);

    async fn process_request(
        &self,
        lease_index: Option<u64>,
        request: Request,
    ) -> Result<celeriant_msg::process_responses::Response, ReadWriteError>;

    async fn trim_start(&self, request: &TrimStartRequest) -> Result<(), ReadWriteError>;

    async fn delete(&self, request: &DeleteRequest) -> Result<(), ReadWriteError>;

    fn list_organisations(
        &self,
        request: ListOrganisationsRequest,
    ) -> Result<ListOrganisationsResponse, ReadError>;

    fn list_aggregates(
        &self,
        request: ListAggregatesRequest,
    ) -> Result<ListAggregatesResponse, ReadError>;

    fn exists(
        &self,
        request: ExistsRequest,
    ) -> Result<ExistsResponse, ReadError>;

    async fn read(&self, request: &ReadRequest) -> Result<ReadResponse, ReadError>;

    async fn write(
        &self,
        lease_index: u64,
        request: WriteRequest,
    ) -> Result<WriteResponse, ReadWriteError>;
}

impl LocalAggregate {
    pub fn new(
        node_config: NodeConfig,
    ) -> Self {
        let capacity = NonZeroUsize::new(node_config.max_open_aggregates).unwrap();
        Self {
            node_config,
            watched_aggregates: Rc::new(WatchedAggregates::new()),
        }
    }
}

impl LocalAggregateTrait for LocalAggregate {

    async fn close(&self) {
    }

    async fn process_request(
        &self,
        lease_index: Option<u64>,
        request: Request,
    ) -> Result<celeriant_msg::process_responses::Response, ReadWriteError> {
        match request {
            Request::ListOrganisations(req) => {
                return Ok(celeriant_msg::process_responses::Response::ListOrganisations(self.list_organisations(req)?));
            }
            Request::ListAggregates(req) => {
                return Ok(celeriant_msg::process_responses::Response::ListAggregates(self.list_aggregates(req)?));
            }
            Request::Exists(req) => {
                return Ok(celeriant_msg::process_responses::Response::Exists(self.exists(req)?));
            }
            Request::Read(req) => {
                return Ok(celeriant_msg::process_responses::Response::Read(self.read(&req).await?));
            }
            Request::Write(req) => {
                let lease_index = lease_index.ok_or(WriteError::InvalidLeaseIndex)?;
                return Ok(celeriant_msg::process_responses::Response::Write(self.write(lease_index, req).await?));
            }
            Request::TrimStart(req) => {
                self.trim_start(&req).await?;
                return Ok(celeriant_msg::process_responses::Response::TrimStart(SuccessResponse {
                    correlation_id: req.correlation_id
                }));
            }
            Request::Delete(req) => {
                self.delete(&req).await?;
                return Ok(celeriant_msg::process_responses::Response::TrimStart(SuccessResponse {
                    correlation_id: req.correlation_id
                }));
            }
            Request::Watch(_) => {
                unreachable!()
            }
        }
    }

    async fn trim_start(&self, request: &TrimStartRequest) -> Result<(), ReadWriteError> {
        

        Ok(())
    }

    async fn delete(&self, request: &DeleteRequest) -> Result<(), ReadWriteError> {
       

        Ok(())
    }

    fn list_organisations(
        &self,
        request: ListOrganisationsRequest,
    ) -> Result<ListOrganisationsResponse, ReadError> {
        let mut organisations = Vec::new();

        Ok(ListOrganisationsResponse {
            correlation_id: request.correlation_id,
            organisations,
        })
    }

    fn exists(
        &self,
        request: ExistsRequest,
    ) -> Result<ExistsResponse, ReadError> {

        Ok(ExistsResponse {
            correlation_id: request.correlation_id,
            min_event_batch_index: 0,
        })
    }

    fn list_aggregates(
        &self,
        request: ListAggregatesRequest,
    ) -> Result<ListAggregatesResponse, ReadError> {
        let org_id = request.org_id;
        let aggregate_type_id = request.aggregate_type_id;
        let filters = request.filters;

        let mut aggregates = Vec::new();

        Ok(ListAggregatesResponse {
            correlation_id: request.correlation_id,
            aggregates,
        })
    }

    async fn read(&self, request: &ReadRequest) -> Result<ReadResponse, ReadError> {
        
        Ok(ReadResponse { correlation_id: None, event_batches: vec![], next_event_batch_index: Some(0) })
    }

    async fn write(
        &self,
        lease_index: u64,
        mut request: WriteRequest,
    ) -> Result<WriteResponse, ReadWriteError> {

        Ok(WriteResponse { correlation_id: None, event_batch_index: 0, start_event_index: 0, server_timestamp: 0, compressed_size: 0, node_id: 0, lease_index, events_crc: 0 })
    }
}

fn get_server_timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod test_local_aggregate_integration {
    use celeriant_msg::request::{
        directory_filters::DirectoryFilters,
        read_filters::ReadFilters,
        requests::{
            DeleteRequest, ListAggregatesRequest, ListOrganisationsRequest,
            ReadRequest, TrimStartRequest, WriteRequest,
        },
    };
    use celeriant_wal::{
        aggregate_key::AggregateKey, compression_type::CompressionType, wal::event_item::EventItem,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        local_aggregate::{LocalAggregate, LocalAggregateTrait}, node_config::test_node_config::test_config,
    };

    /// Helper to create test events
    fn create_events(start_index: u64, count: usize, base_timestamp: u64) -> Vec<EventItem> {
        (0..count)
            .map(|i| {
                EventItem::new(
                    start_index + i as u64,
                    0,
                    None,
                    base_timestamp + i as u64,
                    1,
                    0,
                    vec![i as u8; 50],
                )
            })
            .collect()
    }

    fn create_local_aggregate(data_root: &str) -> LocalAggregate {
        LocalAggregate::new(test_config(data_root))
    }

    #[test]
    fn test_full_write_read_lifecycle() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);

                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Test 1: Write first batch (creates aggregate)
                let write_request = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 3, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };

                let result = local_aggregate.write(1, write_request).await;
                let write_response = result.unwrap();
                assert_eq!(write_response.correlation_id, Some(2));
                assert_eq!(write_response.event_batch_index, 1);

                // Test 2: Write second batch
                let write_request = WriteRequest {
                    correlation_id: Some(4),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: Some(42),
                    events: create_events(4, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };

                let result = local_aggregate.write(1, write_request).await;
                let write_response = result.unwrap();
                assert_eq!(write_response.event_batch_index, 2);

                // Test 3: Read all events
                let read_request = ReadRequest {
                    correlation_id: Some(5),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };

                let result = local_aggregate.read(&read_request).await;
                let read_response = result.unwrap();
                assert_eq!(read_response.correlation_id, Some(5));
                assert_eq!(read_response.event_batches.len(), 2);
                assert_eq!(read_response.event_batches[0].events.len(), 3);
                assert_eq!(read_response.event_batches[1].events.len(), 2);
                assert_eq!(read_response.event_batches[1].user_id, Some(42));

                // Test 4: Read with filters
                let read_request = ReadRequest {
                    correlation_id: Some(6),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1).min_event_timestamp(2000),
                };

                let result = local_aggregate.read(&read_request).await;
                let read_response = result.unwrap();
                assert_eq!(read_response.event_batches.len(), 1);
                assert_eq!(read_response.event_batches[0].event_batch_index, 2);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_aggregate_isolation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);

                let aggregate_key_1 = AggregateKey::new(1, 1, 1);
                let aggregate_key_2 = AggregateKey::new(1, 1, 2);

                // Write to aggregate 1
                let write_req1 = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key_1.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req1).await.unwrap();

                // Write to aggregate 2
                let write_req2 = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key_2.clone(),
                    client_id: 200,
                    user_id: None,
                    events: create_events(1, 3, 2000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req2).await.unwrap();

                // Read from aggregate 1 - should only get its events
                let read_req1 = ReadRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key_1.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req1).await.unwrap();

                assert_eq!(result.event_batches.len(), 1);
                assert_eq!(result.event_batches[0].events.len(), 2);
                assert_eq!(result.event_batches[0].client_id, 100);

                // Read from aggregate 2 - should only get its events
                let read_req2 = ReadRequest {
                    correlation_id: Some(4),
                    aggregate_key: aggregate_key_2.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req2).await.unwrap();

                assert_eq!(result.event_batches.len(), 1);
                assert_eq!(result.event_batches[0].events.len(), 3);
                assert_eq!(result.event_batches[0].client_id, 200);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_concurrency_violations() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write first batch
                let write_req = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Try to write with wrong expected index - should fail
                let write_req = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(1), // Wrong! Should be 2
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                let result = local_aggregate.write(1, write_req).await;
                assert!(result.is_err());

                // Try with overlapping client event index - should fail
                let write_req = WriteRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(2, 2, 2000), // Overlaps with previous write
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                let result = local_aggregate.write(1, write_req).await;
                assert!(result.is_err());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_trim_start_operation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write 5 batches
                for i in 1..=5u64 {
                    let write_req = WriteRequest {
                        correlation_id: Some(i as u128),
                        aggregate_key: aggregate_key.clone(),
                        client_id: 100,
                        user_id: None,
                        events: create_events(i * 10, 2, i * 1000),
                        allow_create: i == 1,
                        expected_event_batch_index: Some(i),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    };
                    local_aggregate.write(1, write_req).await.unwrap();
                }

                // Verify all batches exist
                let read_req = ReadRequest {
                    correlation_id: Some(10),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();
                assert_eq!(result.event_batches.len(), 5);

                // Trim first 2 batches
                let trim_req = TrimStartRequest {
                    correlation_id: Some(11),
                    aggregate_key: aggregate_key.clone(),
                    keep_from_event_batch_index: 3,
                };
                local_aggregate.trim_start(&trim_req).await.unwrap();

                // Verify only batches 3-5 remain
                let read_req = ReadRequest {
                    correlation_id: Some(12),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(3),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 3);
                assert_eq!(result.event_batches[0].event_batch_index, 3);

                // Try to read from batch 1 - should fail
                let read_req = ReadRequest {
                    correlation_id: Some(13),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_err());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_delete_operation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Create aggregate
                let write_req = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Verify it exists by reading
                let read_req = ReadRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_ok());

                // Delete it
                let delete_req = DeleteRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                };
                local_aggregate.delete(&delete_req).await.unwrap();

                // Verify it no longer exists (read should fail)
                let read_req = ReadRequest {
                    correlation_id: Some(4),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_err());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_pagination_requests() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let mut node_config = test_config(data_root);
                node_config.max_event_batches_response_size = Some(300);

                let local_aggregate = LocalAggregate::new(node_config);

                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write 10 batches
                for i in 1..=10u64 {
                    let write_req = WriteRequest {
                        correlation_id: Some(i as u128),
                        aggregate_key: aggregate_key.clone(),
                        client_id: 100,
                        user_id: None,
                        events: create_events(i * 10, 3, i * 1000),
                        allow_create: i == 1,
                        expected_event_batch_index: Some(i),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    };
                    local_aggregate.write(1, write_req).await.unwrap();
                }

                // Read first page with limit
                let read_req = ReadRequest {
                    correlation_id: Some(100),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert!(result.event_batches.len() < 10);
                assert!(result.next_event_batch_index.is_some());

                let next_batch_index = result.next_event_batch_index.unwrap();

                // Read second page
                let read_req = ReadRequest {
                    correlation_id: Some(101),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(next_batch_index),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert!(result.event_batches.len() > 0);
                assert_eq!(result.event_batches[0].event_batch_index, next_batch_index);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_multi_client_writes() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Client 100 writes
                let write_req = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Client 200 writes - different client, same client_event_index is OK
                let write_req = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 200,
                    user_id: None,
                    events: create_events(1, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                let result = local_aggregate.write(1, write_req).await.unwrap();
                assert_eq!(result.event_batch_index, 2);

                // Client 100 continues
                let write_req = WriteRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 3000),
                    allow_create: false,
                    expected_event_batch_index: Some(3),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Read all and verify client isolation
                let read_req = ReadRequest {
                    correlation_id: Some(4),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 3);
                assert_eq!(result.event_batches[0].client_id, 100);
                assert_eq!(result.event_batches[1].client_id, 200);
                assert_eq!(result.event_batches[2].client_id, 100);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_list_organisations_and_aggregates() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);

                // Create aggregates in different organisations
                for org_id in 1u128..=3 {
                    for aggregate_id in 1u128..=2 {
                        let aggregate_key = AggregateKey::new(org_id, 1, aggregate_id);
                        let write_req = WriteRequest {
                            correlation_id: Some(org_id * 10 + aggregate_id),
                            aggregate_key,
                            client_id: 100,
                            user_id: None,
                            events: create_events(1, 2, 1000),
                            allow_create: true,
                            expected_event_batch_index: Some(1),
                            enforce_client_idempotency: true,
                            durable_write_with_delay_us: Some(0),
                            compression_type: CompressionType::None,
                        };
                        local_aggregate.write(1, write_req).await.unwrap();
                    }
                }

                // List organisations
                let list_orgs_req = ListOrganisationsRequest {
                    correlation_id: Some(100),
                    filters: DirectoryFilters::default(),
                };
                let result = local_aggregate.list_organisations(list_orgs_req).unwrap();
                assert_eq!(result.correlation_id, Some(100));
                assert_eq!(result.organisations.len(), 3);
                for org in &result.organisations {
                    assert!(org.disk_usage > 0);
                }

                // List aggregates for org 1
                let list_aggs_req = ListAggregatesRequest {
                    correlation_id: Some(101),
                    org_id: 1,
                    aggregate_type_id: Some(1),
                    filters: DirectoryFilters::default(),
                };
                let result = local_aggregate.list_aggregates(list_aggs_req).unwrap();
                assert_eq!(result.correlation_id, Some(101));
                assert_eq!(result.aggregates.len(), 2);
                for agg in &result.aggregates {
                    assert_eq!(agg.key.org_id, 1);
                    assert_eq!(agg.key.aggregate_type_id, 1);
                    assert!(agg.disk_usage > 0);
                }

                // List all aggregate types for org 2
                let list_aggs_req = ListAggregatesRequest {
                    correlation_id: Some(102),
                    org_id: 2,
                    aggregate_type_id: None,
                    filters: DirectoryFilters::default(),
                };
                let result = local_aggregate.list_aggregates(list_aggs_req).unwrap();
                assert_eq!(result.aggregates.len(), 2);
                for agg in &result.aggregates {
                    assert_eq!(agg.key.org_id, 2);
                }
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_background_vs_synchronous_writes() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write with immediate sync
                let write_req = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0), // Sync immediately
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Write with background sync (None)
                let write_req = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: None, // Background sync
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Give background sync time to complete
                glommio::timer::sleep(std::time::Duration::from_millis(300)).await;

                // Read should work for both
                let read_req = ReadRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 2);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_error_handling_nonexistent_aggregate() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 999);

                // Try to read from non-existent aggregate
                let read_req = ReadRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_err());

                // Try to write with allow_create=false
                let write_req = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: false,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                let result = local_aggregate.write(1, write_req).await;
                assert!(result.is_err());

                // Try to trim non-existent aggregate
                let trim_req = TrimStartRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                    keep_from_event_batch_index: 1,
                };
                let result = local_aggregate.trim_start(&trim_req).await;
                assert!(result.is_err());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_complex_multi_aggregate_scenario() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);

                // Scenario: 3 users (aggregates) in a collaborative app
                // Each user has their own event stream
                for user_aggregate_id in 1u128..=3 {
                    let aggregate_key = AggregateKey::new(1, 1, user_aggregate_id);
                    // Initial write
                    let write_req = WriteRequest {
                        correlation_id: Some(user_aggregate_id),
                        aggregate_key,
                        client_id: user_aggregate_id * 100,
                        user_id: Some(user_aggregate_id),
                        events: create_events(1, 3, 1000),
                        allow_create: true,
                        expected_event_batch_index: Some(1),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    };
                    local_aggregate.write(1, write_req).await.unwrap();
                }

                // All users perform more writes
                for user_aggregate_id in 1u128..=3 {
                    for batch in 2..=5u64 {
                        let aggregate_key = AggregateKey::new(1, 1, user_aggregate_id);
                        let write_req = WriteRequest {
                            correlation_id: Some(user_aggregate_id * 100 + batch as u128),
                            aggregate_key,
                            client_id: user_aggregate_id * 100,
                            user_id: Some(user_aggregate_id),
                            events: create_events(batch * 10, 2, batch * 1000),
                            allow_create: false,
                            expected_event_batch_index: Some(batch),
                            enforce_client_idempotency: true,
                            durable_write_with_delay_us: None, // Background
                            compression_type: CompressionType::None,
                        };
                        local_aggregate.write(1, write_req).await.unwrap();
                    }
                }

                // Wait for background syncs
                glommio::timer::sleep(std::time::Duration::from_millis(500)).await;

                // User 1 reads their full history
                let read_req = ReadRequest {
                    correlation_id: Some(1000),
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 5);
                // Verify all batches belong to user 1
                for batch in &result.event_batches {
                    assert_eq!(batch.client_id, 100);
                }

                // User 2 trims old data
                let trim_req = TrimStartRequest {
                    correlation_id: Some(2000),
                    aggregate_key: AggregateKey::new(1, 1, 2),
                    keep_from_event_batch_index: 3,
                };
                local_aggregate.trim_start(&trim_req).await.unwrap();

                // Verify user 2 only has recent data (reading from batch 1 should error)
                let read_req = ReadRequest {
                    correlation_id: Some(2001),
                    aggregate_key: AggregateKey::new(1, 1, 2),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_err());

                // But user 3 still has all their data
                let read_req = ReadRequest {
                    correlation_id: Some(3000),
                    aggregate_key: AggregateKey::new(1, 1, 3),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 5);

                // List all aggregates
                let list_req = ListAggregatesRequest {
                    correlation_id: Some(4000),
                    org_id: 1,
                    aggregate_type_id: Some(1),
                    filters: DirectoryFilters::default(),
                };
                let result = local_aggregate.list_aggregates(list_req).unwrap();
                assert_eq!(result.aggregates.len(), 3);
            })
            .unwrap();

        handle.join().unwrap();
    }
}
