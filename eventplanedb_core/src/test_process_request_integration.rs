#[cfg(test)]
mod test_process_request_integration {

    use eventplanedb_structures::{
        compression_type::CompressionType,
        event_item::EventItem,
        read_filters::ReadFilters,
        request::*,
        response::Response,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        process_request::ProcessRequest,
        read_operations::read_structures::AggregateReadConfig,
        write_operations::write_structures::AggregateWriteConfig,
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

    #[test]
    fn test_full_write_read_lifecycle() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                    max_data_cache_size_bytes: 1 << 20,
                };

                let write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    read_config,
                    write_config,
                    1000,
                );

                let org_id = 1;
                let aggregate_type_id = 1;
                let aggregate_id = 1;

                // Test 1: Check aggregate doesn't exist yet
                let exists_request = Request::Exists(ExistsRequest {
                    correlation_id: Some(1),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                });

                let response = processor.process(exists_request, None).await;
                match response {
                    Response::Exists(r) => {
                        assert_eq!(r.correlation_id, Some(1));
                        assert!(r.error.is_none());
                        assert!(!r.exists);
                    }
                    _ => panic!("Expected ExistsResponse"),
                }

                // Test 2: Write first batch
                let write_request = Request::Write(WriteRequest {
                    correlation_id: Some(2),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 3, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0), // Sync immediately
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_request, None).await;
                match response {
                    Response::Write(r) => {
                        assert_eq!(r.correlation_id, Some(2));
                        assert!(r.error.is_none());
                        let result = r.result.unwrap();
                        assert_eq!(result.next_event_batch_index, 2);
                    }
                    _ => panic!("Expected WriteResponse"),
                }

                // Test 3: Check aggregate now exists
                let exists_request = Request::Exists(ExistsRequest {
                    correlation_id: Some(3),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                });

                let response = processor.process(exists_request, None).await;
                match response {
                    Response::Exists(r) => {
                        assert_eq!(r.correlation_id, Some(3));
                        assert!(r.exists);
                    }
                    _ => panic!("Expected ExistsResponse"),
                }

                // Test 4: Write second batch
                let write_request = Request::Write(WriteRequest {
                    correlation_id: Some(4),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id: 100,
                    user_id: Some(42),
                    events: create_events(4, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_request, None).await;
                match response {
                    Response::Write(r) => {
                        assert_eq!(r.correlation_id, Some(4));
                        assert!(r.error.is_none());
                        assert_eq!(r.result.unwrap().next_event_batch_index, 3);
                    }
                    _ => panic!("Expected WriteResponse"),
                }

                // Test 5: Read all events
                let read_request = Request::Read(ReadRequest {
                    correlation_id: Some(5),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_request, None).await;
                match response {
                    Response::Read(r) => {
                        assert_eq!(r.correlation_id, Some(5));
                        assert!(r.error.is_none());
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 2);
                        assert_eq!(result.event_batches[0].events.len(), 3);
                        assert_eq!(result.event_batches[1].events.len(), 2);
                        assert_eq!(result.event_batches[1].user_id, Some(42));
                    }
                    _ => panic!("Expected ReadResponse"),
                }

                // Test 6: Read with filters
                let read_request = Request::Read(ReadRequest {
                    correlation_id: Some(6),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(1).min_event_timestamp(2000),
                });

                let response = processor.process(read_request, None).await;
                match response {
                    Response::Read(r) => {
                        assert_eq!(r.correlation_id, Some(6));
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 1);
                        assert_eq!(result.event_batches[0].event_batch_index, 2);
                    }
                    _ => panic!("Expected ReadResponse"),
                }
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

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                // Write to aggregate 1
                let write_req1 = Request::Write(WriteRequest {
                    correlation_id: Some(1),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 1,
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_req1, None).await;
                assert!(matches!(response, Response::Write(_)));

                // Write to aggregate 2
                let write_req2 = Request::Write(WriteRequest {
                    correlation_id: Some(2),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 2,
                    client_id: 200,
                    user_id: None,
                    events: create_events(1, 3, 2000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_req2, None).await;
                assert!(matches!(response, Response::Write(_)));

                // Read from aggregate 1 - should only get its events
                let read_req1 = Request::Read(ReadRequest {
                    correlation_id: Some(3),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 1,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req1, None).await;
                match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 1);
                        assert_eq!(result.event_batches[0].events.len(), 2);
                        assert_eq!(result.event_batches[0].client_id, 100);
                    }
                    _ => panic!("Expected ReadResponse"),
                }

                // Read from aggregate 2 - should only get its events
                let read_req2 = Request::Read(ReadRequest {
                    correlation_id: Some(4),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 2,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req2, None).await;
                match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 1);
                        assert_eq!(result.event_batches[0].events.len(), 3);
                        assert_eq!(result.event_batches[0].client_id, 200);
                    }
                    _ => panic!("Expected ReadResponse"),
                }
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

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                // Write first batch
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(1),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 1,
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                processor.process(write_req, None).await;

                // Try to write with wrong expected index - should fail
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(2),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 1,
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(1), // Wrong! Should be 2
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_req, None).await;
                match response {
                    Response::Write(r) => {
                        assert!(r.error.is_some());
                        assert!(r.result.is_none());
                    }
                    _ => panic!("Expected WriteResponse with error"),
                }

                // Try with overlapping client event index - should fail
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(3),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 1,
                    client_id: 100,
                    user_id: None,
                    events: create_events(2, 2, 2000), // Overlaps with previous write
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_req, None).await;
                match response {
                    Response::Write(r) => {
                        assert!(r.error.is_some());
                    }
                    _ => panic!("Expected WriteResponse with error"),
                }
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

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                let org_id = 1;
                let aggregate_type_id = 1;
                let aggregate_id = 1;

                // Write 5 batches
                for i in 1..=5 {
                    let write_req = Request::Write(WriteRequest {
                        correlation_id: Some(i as u128),
                        org_id,
                        aggregate_type_id,
                        aggregate_id,
                        client_id: 100,
                        user_id: None,
                        events: create_events(i * 10, 2, i * 1000),
                        allow_create: i == 1,
                        expected_event_batch_index: Some(i),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    });

                    processor.process(write_req, None).await;
                }

                // Verify all batches exist
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(10),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        assert_eq!(r.result.unwrap().event_batches.len(), 5);
                    }
                    _ => panic!("Expected ReadResponse"),
                }

                // Trim first 2 batches
                let trim_req = Request::TrimStart(TrimStartRequest {
                    correlation_id: Some(11),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    keep_from_event_batch_index: 3,
                });

                let response = processor.process(trim_req, None).await;
                match response {
                    Response::TrimStart(r) => {
                        assert_eq!(r.correlation_id, Some(11));
                        assert!(r.error.is_none());
                    }
                    _ => panic!("Expected TrimStartResponse"),
                }

                // Verify only batches 3-5 remain
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(12),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(3),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 3);
                        assert_eq!(result.event_batches[0].event_batch_index, 3);
                    }
                    _ => panic!("Expected ReadResponse"),
                }

                // Try to read from batch 1 - should fail
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(13),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        assert!(r.error.is_some());
                    }
                    _ => panic!("Expected ReadResponse with error"),
                }
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

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                let org_id = 1;
                let aggregate_type_id = 1;
                let aggregate_id = 1;

                // Create aggregate
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(1),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                processor.process(write_req, None).await;

                // Verify it exists
                let exists_req = Request::Exists(ExistsRequest {
                    correlation_id: Some(2),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                });

                let response = processor.process(exists_req, None).await;
                match response {
                    Response::Exists(r) => assert!(r.exists),
                    _ => panic!("Expected ExistsResponse"),
                }

                // Delete it
                let delete_req = Request::Delete(DeleteRequest {
                    correlation_id: Some(3),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                });

                let response = processor.process(delete_req, None).await;
                match response {
                    Response::Delete(r) => {
                        assert_eq!(r.correlation_id, Some(3));
                        assert!(r.error.is_none());
                    }
                    _ => panic!("Expected DeleteResponse"),
                }

                // Verify it no longer exists
                let exists_req = Request::Exists(ExistsRequest {
                    correlation_id: Some(4),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                });

                let response = processor.process(exists_req, None).await;
                match response {
                    Response::Exists(r) => assert!(!r.exists),
                    _ => panic!("Expected ExistsResponse"),
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_update_cache_limits() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                // Create an aggregate first
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(1),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 1,
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                processor.process(write_req, None).await;

                // Update cache limits
                let update_req = Request::UpdateCacheLimits(UpdateCacheLimitsRequest {
                    correlation_id: Some(2),
                    aggregate_read_max_data_cache_size_bytes: 1 << 21,
                    aggregate_write_max_data_cache_size_bytes: 1 << 26,
                });

                let response = processor.process(update_req, None).await;
                match response {
                    Response::UpdateCacheLimits(r) => {
                        assert_eq!(r.correlation_id, Some(2));
                        assert!(r.error.is_none());
                        assert!(r.accepted);
                    }
                    _ => panic!("Expected UpdateCacheLimitsResponse"),
                }

                // Write more data - should work with new limits
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(3),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 1,
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_req, None).await;
                match response {
                    Response::Write(r) => {
                        assert!(r.error.is_none());
                    }
                    _ => panic!("Expected WriteResponse"),
                }
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

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                let org_id = 1;
                let aggregate_type_id = 1;
                let aggregate_id = 1;

                // Write 10 batches
                for i in 1..=10 {
                    let write_req = Request::Write(WriteRequest {
                        correlation_id: Some(i as u128),
                        org_id,
                        aggregate_type_id,
                        aggregate_id,
                        client_id: 100,
                        user_id: None,
                        events: create_events(i * 10, 3, i * 1000),
                        allow_create: i == 1,
                        expected_event_batch_index: Some(i),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    });

                    processor.process(write_req, None).await;
                }

                // Read first page with limit
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(100),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req, Some(300)).await;
                let next_batch_index = match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert!(result.event_batches.len() < 10);
                        assert!(result.next_event_batch_index.is_some());
                        result.next_event_batch_index.unwrap()
                    }
                    _ => panic!("Expected ReadResponse"),
                };

                // Read second page
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(101),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(next_batch_index),
                });

                let response = processor.process(read_req, Some(300)).await;
                match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert!(result.event_batches.len() > 0);
                        assert_eq!(result.event_batches[0].event_batch_index, next_batch_index);
                    }
                    _ => panic!("Expected ReadResponse"),
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_write_batches_prepend() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                let org_id = 1;
                let aggregate_type_id = 1;
                let aggregate_id = 1;

                // Write batches 3-5
                for i in 3..=5 {
                    let write_req = Request::Write(WriteRequest {
                        correlation_id: Some(i as u128),
                        org_id,
                        aggregate_type_id,
                        aggregate_id,
                        client_id: 100,
                        user_id: None,
                        events: create_events(i * 10, 2, i * 1000),
                        allow_create: i == 3,
                        expected_event_batch_index: Some(i),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    });

                    processor.process(write_req, None).await;
                }

                // Read to get batches we'll later prepend
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(10),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(3),
                });

                let batches_to_prepend = match processor.process(read_req, None).await {
                    Response::Read(r) => {
                        r.result.unwrap().event_batches
                    }
                    _ => panic!("Expected ReadResponse"),
                };

                // Trim to keep only batch 5
                let trim_req = Request::TrimStart(TrimStartRequest {
                    correlation_id: Some(11),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    keep_from_event_batch_index: 5,
                });

                processor.process(trim_req, None).await;

                // Now prepend batches 3-4
                let prepend_batches = batches_to_prepend[0..2].to_vec();
                let write_batches_req = Request::WriteBatches(WriteBatchesRequest {
                    correlation_id: Some(12),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    allow_create: false,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::Snappy,
                    batches: prepend_batches,
                });

                let response = processor.process(write_batches_req, None).await;
                match response {
                    Response::WriteBatches(r) => {
                        assert_eq!(r.correlation_id, Some(12));
                        assert!(r.error.is_none());
                    }
                    _ => panic!("Expected WriteBatchesResponse"),
                }

                // Verify we now have batches 3-5
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(13),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(3),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 3);
                        assert_eq!(result.event_batches[0].event_batch_index, 3);
                        assert_eq!(result.event_batches[1].event_batch_index, 4);
                        assert_eq!(result.event_batches[2].event_batch_index, 5);
                    }
                    _ => panic!("Expected ReadResponse"),
                }
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

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                let org_id = 1;
                let aggregate_type_id = 1;
                let aggregate_id = 1;

                // Client 100 writes
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(1),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                processor.process(write_req, None).await;

                // Client 200 writes - different client, same client_event_index is OK
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(2),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id: 200,
                    user_id: None,
                    events: create_events(1, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_req, None).await;
                match response {
                    Response::Write(r) => {
                        assert!(r.error.is_none());
                        assert_eq!(r.result.unwrap().next_event_batch_index, 3);
                    }
                    _ => panic!("Expected WriteResponse"),
                }

                // Client 100 continues
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(3),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 3000),
                    allow_create: false,
                    expected_event_batch_index: Some(3),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_req, None).await;
                match response {
                    Response::Write(r) => {
                        assert!(r.error.is_none());
                    }
                    _ => panic!("Expected WriteResponse"),
                }

                // Read all and verify client isolation
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(4),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 3);
                        assert_eq!(result.event_batches[0].client_id, 100);
                        assert_eq!(result.event_batches[1].client_id, 200);
                        assert_eq!(result.event_batches[2].client_id, 100);
                    }
                    _ => panic!("Expected ReadResponse"),
                }
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

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                // Create aggregates in different organisations
                for org_id in 1..=3 {
                    for aggregate_id in 1..=2 {
                        let write_req = Request::Write(WriteRequest {
                            correlation_id: Some((org_id * 10 + aggregate_id) as u128),
                            org_id,
                            aggregate_type_id: 1,
                            aggregate_id,
                            client_id: 100,
                            user_id: None,
                            events: create_events(1, 2, 1000),
                            allow_create: true,
                            expected_event_batch_index: Some(1),
                            enforce_client_idempotency: true,
                            durable_write_with_delay_us: Some(0),
                            compression_type: CompressionType::None,
                        });

                        processor.process(write_req, None).await;
                    }
                }

                // List organisations
                let list_orgs_req = Request::ListOrganisations(ListOrganisationsRequest {
                    correlation_id: Some(100),
                    filters: eventplanedb_structures::directory_filters::DirectoryFilters::default(),
                });

                let response = processor.process(list_orgs_req, None).await;
                match response {
                    Response::ListOrganisations(r) => {
                        assert_eq!(r.correlation_id, Some(100));
                        assert!(r.error.is_none());
                        assert_eq!(r.organisations.len(), 3);
                        for org in &r.organisations {
                            assert!(org.disk_usage > 0);
                        }
                    }
                    _ => panic!("Expected ListOrganisationsResponse"),
                }

                // List aggregates for org 1
                let list_aggs_req = Request::ListAggregates(ListAggregatesRequest {
                    correlation_id: Some(101),
                    org_id: 1,
                    aggregate_type_id: Some(1),
                    filters: eventplanedb_structures::directory_filters::DirectoryFilters::default(),
                });

                let response = processor.process(list_aggs_req, None).await;
                match response {
                    Response::ListAggregates(r) => {
                        assert_eq!(r.correlation_id, Some(101));
                        assert!(r.error.is_none());
                        assert_eq!(r.aggregates.len(), 2);
                        for agg in &r.aggregates {
                            assert_eq!(agg.org_id, 1);
                            assert_eq!(agg.aggregate_type_id, 1);
                            assert!(agg.disk_usage > 0);
                        }
                    }
                    _ => panic!("Expected ListAggregatesResponse"),
                }

                // List all aggregate types for org 2
                let list_aggs_req = Request::ListAggregates(ListAggregatesRequest {
                    correlation_id: Some(102),
                    org_id: 2,
                    aggregate_type_id: None,
                    filters: eventplanedb_structures::directory_filters::DirectoryFilters::default(),
                });

                let response = processor.process(list_aggs_req, None).await;
                match response {
                    Response::ListAggregates(r) => {
                        assert_eq!(r.aggregates.len(), 2);
                        for agg in &r.aggregates {
                            assert_eq!(agg.org_id, 2);
                        }
                    }
                    _ => panic!("Expected ListAggregatesResponse"),
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

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                let org_id = 1;
                let aggregate_type_id = 1;
                let aggregate_id = 1;

                // Write with immediate sync
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(1),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0), // Sync immediately
                    compression_type: CompressionType::None,
                });

                processor.process(write_req, None).await;

                // Write with background sync (simulated by None)
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(2),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: None, // Background sync
                    compression_type: CompressionType::None,
                });

                processor.process(write_req, None).await;

                // Give background sync time to complete
                glommio::timer::sleep(std::time::Duration::from_millis(300)).await;

                // Read should work for both
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(3),
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 2);
                    }
                    _ => panic!("Expected ReadResponse"),
                }
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

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                // Try to read from non-existent aggregate
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(1),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 999,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        assert!(r.error.is_some());
                        assert!(r.result.is_none());
                    }
                    _ => panic!("Expected ReadResponse with error"),
                }

                // Try to write with allow_create=false
                let write_req = Request::Write(WriteRequest {
                    correlation_id: Some(2),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 999,
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: false,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                });

                let response = processor.process(write_req, None).await;
                match response {
                    Response::Write(r) => {
                        assert!(r.error.is_some());
                        assert!(r.result.is_none());
                    }
                    _ => panic!("Expected WriteResponse with error"),
                }

                // Try to trim non-existent aggregate
                let trim_req = Request::TrimStart(TrimStartRequest {
                    correlation_id: Some(3),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 999,
                    keep_from_event_batch_index: 1,
                });

                let response = processor.process(trim_req, None).await;
                match response {
                    Response::TrimStart(r) => {
                        assert!(r.error.is_some());
                    }
                    _ => panic!("Expected TrimStartResponse with error"),
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_complex_multi_aggregate_scenario() {
        // Simulate a realistic scenario with multiple aggregates, clients, and operations
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let processor = ProcessRequest::new(
                    data_root.to_string(),
                    AggregateReadConfig {
                        max_chunk_size: 1 << 20,
                        max_data_cache_size_bytes: 1 << 20,
                    },
                    AggregateWriteConfig {
                        max_data_cache_size_bytes: 1 << 25,
                        cache_trim_factor: 25,
                        max_chunk_size: 1 << 20,
                    },
                    1000,
                );

                // Scenario: 3 users (aggregates) in a collaborative app
                // Each user has their own event stream
                for user_aggregate_id in 1..=3 {
                    // Initial write
                    let write_req = Request::Write(WriteRequest {
                        correlation_id: Some(user_aggregate_id as u128),
                        org_id: 1,
                        aggregate_type_id: 1,
                        aggregate_id: user_aggregate_id,
                        client_id: user_aggregate_id * 100,
                        user_id: Some(user_aggregate_id),
                        events: create_events(1, 3, 1000),
                        allow_create: true,
                        expected_event_batch_index: Some(1),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    });

                    processor.process(write_req, None).await;
                }

                // All users perform more writes
                for user_aggregate_id in 1..=3 {
                    for batch in 2..=5u64 {
                        let write_req = Request::Write(WriteRequest {
                            correlation_id: Some((user_aggregate_id * 100 + batch) as u128),
                            org_id: 1,
                            aggregate_type_id: 1,
                            aggregate_id: user_aggregate_id as u128,
                            client_id: user_aggregate_id as u128 * 100,
                            user_id: Some(user_aggregate_id as u128),
                            events: create_events(batch * 10, 2, batch * 1000),
                            allow_create: false,
                            expected_event_batch_index: Some(batch),
                            enforce_client_idempotency: true,
                            durable_write_with_delay_us: None, // Background
                            compression_type: CompressionType::None,
                        });

                        processor.process(write_req, None).await;
                    }
                }

                // Wait for background syncs
                glommio::timer::sleep(std::time::Duration::from_millis(500)).await;

                // User 1 reads their full history
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(1000),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 1,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 5);
                        // Verify all batches belong to user 1
                        for batch in &result.event_batches {
                            assert_eq!(batch.client_id, 100);
                        }
                    }
                    _ => panic!("Expected ReadResponse"),
                }

                // User 2 trims old data
                let trim_req = Request::TrimStart(TrimStartRequest {
                    correlation_id: Some(2000),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 2,
                    keep_from_event_batch_index: 3,
                });

                processor.process(trim_req, None).await;

                // Verify user 2 only has recent data
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(2001),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 2,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        assert!(r.error.is_some()); // Should error on batch 1
                    }
                    _ => panic!("Expected ReadResponse with error"),
                }

                // But user 3 still has all their data
                let read_req = Request::Read(ReadRequest {
                    correlation_id: Some(3000),
                    org_id: 1,
                    aggregate_type_id: 1,
                    aggregate_id: 3,
                    filters: ReadFilters::new(1),
                });

                let response = processor.process(read_req, None).await;
                match response {
                    Response::Read(r) => {
                        let result = r.result.unwrap();
                        assert_eq!(result.event_batches.len(), 5);
                    }
                    _ => panic!("Expected ReadResponse"),
                }

                // List all aggregates
                let list_req = Request::ListAggregates(ListAggregatesRequest {
                    correlation_id: Some(4000),
                    org_id: 1,
                    aggregate_type_id: Some(1),
                    filters: eventplanedb_structures::directory_filters::DirectoryFilters::default(),
                });

                let response = processor.process(list_req, None).await;
                match response {
                    Response::ListAggregates(r) => {
                        assert_eq!(r.aggregates.len(), 3);
                    }
                    _ => panic!("Expected ListAggregatesResponse"),
                }
            })
            .unwrap();
        handle.join().unwrap();
    }
}