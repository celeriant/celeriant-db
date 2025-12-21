#[cfg(test)]
mod tests {
    use std::{collections::HashSet, rc::Rc, time::Duration};
    use futures_lite::future::poll_once;
    use celeriant_msg::request::requests::WatchRequest;
    use celeriant_wal::{
        aggregate_key::AggregateKey,
        aggregate_type_key::AggregateTypeKey,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        aggregate_reader::AggregateReader,
        aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation},
        aggregate_watchers::AggregateWatchers,
        subscribed_client::MAX_PENDING_EVENTS,
        watch_output_type::WatchOutputType,
        watch_session::WatchSession,
    };

    // ============================================================
    // Mock AggregateReader for WatchSession tests
    // ============================================================

    struct MockAggregateReader {
        watchers: Rc<AggregateWatchers>,
    }

    impl MockAggregateReader {
        fn new(watchers: Rc<AggregateWatchers>) -> Self {
            Self { watchers }
        }
    }

    impl AggregateReader for MockAggregateReader {
        fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
            self.watchers.clone()
        }
    }

    // ============================================================
    // AggregateWatchers tests
    // ============================================================

    #[test]
    fn test_watchers_add_and_remove_subscriber() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                assert!(watchers.is_empty());

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (id1, _client1) = watchers.add_subscriber(request.clone());
                assert!(!watchers.is_empty());

                let (id2, _client2) = watchers.add_subscriber(request.clone());
                assert_eq!(id2, id1 + 1); // IDs are monotonically increasing

                watchers.remove_subscriber(id1);
                assert!(!watchers.is_empty());

                watchers.remove_subscriber(id2);
                assert!(watchers.is_empty());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_watchers_remove_nonexistent_subscriber() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                // Removing non-existent subscriber should not panic
                watchers.remove_subscriber(999);
                assert!(watchers.is_empty());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_watchers_broadcast_to_single_subscriber() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (_id, client) = watchers.add_subscriber(request);

                let event = AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 2, 3),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 10,
                        to_event_batch_index: 10,
                    },
                };

                watchers.broadcast(event);

                // Event should be in client's receiver
                let received = client.borrow().receiver.recv().await;
                assert!(received.is_some());

                let received_event = received.unwrap();
                assert_eq!(received_event.aggregate_key, AggregateKey::new(1, 2, 3));
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_watchers_broadcast_to_multiple_subscribers() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (_id1, client1) = watchers.add_subscriber(request.clone());
                let (_id2, client2) = watchers.add_subscriber(request.clone());
                let (_id3, client3) = watchers.add_subscriber(request);

                let event = AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 2, 3),
                    operation: AggregateWatchEventOperation::Delete {},
                };

                watchers.broadcast(event);

                // All clients should receive the event
                assert!(client1.borrow().receiver.recv().await.is_some());
                assert!(client2.borrow().receiver.recv().await.is_some());
                assert!(client3.borrow().receiver.recv().await.is_some());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_watchers_slow_client_removed_on_full_channel() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (_id, _client) = watchers.add_subscriber(request);
                assert!(!watchers.is_empty());

                // Fill the channel beyond capacity
                for i in 0..(MAX_PENDING_EVENTS + 100) {
                    let event = AggregateWatchEvent {
                        aggregate_key: AggregateKey::new(1, 2, i as u128),
                        operation: AggregateWatchEventOperation::Write {
                            from_event_batch_index: i as u64,
                            to_event_batch_index: i as u64,
                        },
                    };
                    watchers.broadcast(event);
                }

                // Subscriber should have been removed due to full channel
                assert!(watchers.is_empty());
            })
            .unwrap();

        handle.join().unwrap();
    }

    // ============================================================
    // WatcherHandle filter tests
    // ============================================================

    #[test]
    fn test_filter_by_org() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let mut orgs = HashSet::new();
                orgs.insert(100u128);

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: Some(orgs),
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (_id, client) = watchers.add_subscriber(request);

                // Event matching org filter
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(100, 1, 1),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                });
                assert!(client.borrow().receiver.recv().await.is_some());

                // Event NOT matching org filter
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(200, 1, 1),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 2,
                        to_event_batch_index: 2,
                    },
                });
                assert!(poll_once(client.borrow().receiver.recv()).await.is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_filter_by_aggregate_type() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let mut aggregate_types = HashSet::new();
                aggregate_types.insert(AggregateTypeKey::new(100, 50));

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: Some(aggregate_types),
                    aggregates: None,
                    operation_types: None,
                };

                let (_id, client) = watchers.add_subscriber(request);

                // Matching aggregate type
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(100, 50, 999),
                    operation: AggregateWatchEventOperation::Exists {},
                });
                assert!(client.borrow().receiver.recv().await.is_some());

                // Non-matching aggregate type (different type_id)
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(100, 51, 999),
                    operation: AggregateWatchEventOperation::Exists {},
                });
                assert!(poll_once(client.borrow().receiver.recv()).await.is_none());

                // Non-matching aggregate type (different org)
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(101, 50, 999),
                    operation: AggregateWatchEventOperation::Exists {},
                });
                assert!(poll_once(client.borrow().receiver.recv()).await.is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_filter_by_specific_aggregate() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let mut aggregates = HashSet::new();
                aggregates.insert(AggregateKey::new(1, 2, 3));

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: Some(aggregates),
                    operation_types: None,
                };

                let (_id, client) = watchers.add_subscriber(request);

                // Exact match
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 2, 3),
                    operation: AggregateWatchEventOperation::Delete {},
                });
                assert!(client.borrow().receiver.recv().await.is_some());

                // Different aggregate_id
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 2, 4),
                    operation: AggregateWatchEventOperation::Delete {},
                });
                assert!(poll_once(client.borrow().receiver.recv()).await.is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_filter_by_operation_type() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let mut operation_types = HashSet::new();
                operation_types.insert(AggregateWatchEvent::WRITE);
                operation_types.insert(AggregateWatchEvent::DELETE);

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: Some(operation_types),
                };

                let (_id, client) = watchers.add_subscriber(request);

                // Write - should pass
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                });
                assert!(client.borrow().receiver.recv().await.is_some());

                // Delete - should pass
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    operation: AggregateWatchEventOperation::Delete {},
                });
                assert!(client.borrow().receiver.recv().await.is_some());

                // Read - should NOT pass
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    operation: AggregateWatchEventOperation::Read {
                        from_event_batch_index: 1,
                        to_event_batch_index: Some(5),
                    },
                });
                assert!(poll_once(client.borrow().receiver.recv()).await.is_none());

                // Exists - should NOT pass
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    operation: AggregateWatchEventOperation::Exists {},
                });
                assert!(poll_once(client.borrow().receiver.recv()).await.is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_filter_combination_or_logic() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                // Subscribe to org 100 OR specific aggregate (200, 1, 1)
                let mut orgs = HashSet::new();
                orgs.insert(100u128);

                let mut aggregates = HashSet::new();
                aggregates.insert(AggregateKey::new(200, 1, 1));

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: Some(orgs),
                    aggregate_types: None,
                    aggregates: Some(aggregates),
                    operation_types: None,
                };

                let (_id, client) = watchers.add_subscriber(request);

                // Matches org filter
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(100, 99, 99),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                });
                assert!(client.borrow().receiver.recv().await.is_some());

                // Matches aggregate filter
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(200, 1, 1),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 2,
                        to_event_batch_index: 2,
                    },
                });
                assert!(client.borrow().receiver.recv().await.is_some());

                // Matches neither
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(300, 1, 1),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 3,
                        to_event_batch_index: 3,
                    },
                });
                assert!(poll_once(client.borrow().receiver.recv()).await.is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_no_filters_matches_everything() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (_id, client) = watchers.add_subscriber(request);

                // Should receive all events
                for op in [
                    AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                    AggregateWatchEventOperation::Read {
                        from_event_batch_index: 1,
                        to_event_batch_index: Some(5),
                    },
                    AggregateWatchEventOperation::Delete {},
                    AggregateWatchEventOperation::Exists {},
                    AggregateWatchEventOperation::TrimStart {
                        keep_from_event_batch_index: 10,
                    },
                ] {
                    watchers.broadcast(AggregateWatchEvent {
                        aggregate_key: AggregateKey::new(rand::random(), rand::random(), rand::random()),
                        operation: op,
                    });
                    assert!(client.borrow().receiver.recv().await.is_some());
                }
            })
            .unwrap();

        handle.join().unwrap();
    }

    // ============================================================
    // AggregateWatchEvent merging tests
    // ============================================================

    #[test]
    fn test_write_events_merge_ranges() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let mut response = None;

                let key = AggregateKey::new(1, 2, 3);

                // First write: batch 5
                AggregateWatchEvent {
                    aggregate_key: key.clone(),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 5,
                        to_event_batch_index: 5,
                    },
                }
                .add_to_response(&mut response);

                // Second write: batch 10 (should extend to_event_batch_index)
                AggregateWatchEvent {
                    aggregate_key: key.clone(),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 10,
                        to_event_batch_index: 10,
                    },
                }
                .add_to_response(&mut response);

                // Third write: batch 2 (should extend from_event_batch_index)
                AggregateWatchEvent {
                    aggregate_key: key.clone(),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 2,
                        to_event_batch_index: 2,
                    },
                }
                .add_to_response(&mut response);

                let events = response.unwrap().events.unwrap();
                let key_events = events.get(&key).unwrap();
                let write_event = key_events.get(&AggregateWatchEvent::WRITE).unwrap();

                assert!(write_event.is_some());
                let we = write_event.as_ref().unwrap();
                assert_eq!(we.from_event_batch_index, Some(2));
                assert_eq!(we.to_event_batch_index, Some(10));
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_read_events_merge_ranges_with_none() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let mut response = None;
                let key = AggregateKey::new(1, 2, 3);

                // First read: 5 to Some(10)
                AggregateWatchEvent {
                    aggregate_key: key.clone(),
                    operation: AggregateWatchEventOperation::Read {
                        from_event_batch_index: 5,
                        to_event_batch_index: Some(10),
                    },
                }
                .add_to_response(&mut response);

                // Second read: 3 to None (unbounded)
                AggregateWatchEvent {
                    aggregate_key: key.clone(),
                    operation: AggregateWatchEventOperation::Read {
                        from_event_batch_index: 3,
                        to_event_batch_index: None,
                    },
                }
                .add_to_response(&mut response);

                let events = response.unwrap().events.unwrap();
                let key_events = events.get(&key).unwrap();
                let read_event = key_events.get(&AggregateWatchEvent::READ).unwrap();

                let re = read_event.as_ref().unwrap();
                assert_eq!(re.from_event_batch_index, Some(3)); // min of 5 and 3
                assert_eq!(re.to_event_batch_index, Some(10)); // Some(10).or(None) = Some(10)
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_trim_start_replaces_previous() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let mut response = None;
                let key = AggregateKey::new(1, 2, 3);

                // First trim
                AggregateWatchEvent {
                    aggregate_key: key.clone(),
                    operation: AggregateWatchEventOperation::TrimStart {
                        keep_from_event_batch_index: 5,
                    },
                }
                .add_to_response(&mut response);

                // Second trim (should replace)
                AggregateWatchEvent {
                    aggregate_key: key.clone(),
                    operation: AggregateWatchEventOperation::TrimStart {
                        keep_from_event_batch_index: 10,
                    },
                }
                .add_to_response(&mut response);

                let events = response.unwrap().events.unwrap();
                let key_events = events.get(&key).unwrap();
                let trim_event = key_events.get(&AggregateWatchEvent::TRIM_START).unwrap();

                let te = trim_event.as_ref().unwrap();
                assert_eq!(te.keep_from_event_batch_index, Some(10));
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_delete_and_exists_deduplicate() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let mut response = None;
                let key = AggregateKey::new(1, 2, 3);

                // Multiple deletes
                for _ in 0..5 {
                    AggregateWatchEvent {
                        aggregate_key: key.clone(),
                        operation: AggregateWatchEventOperation::Delete {},
                    }
                    .add_to_response(&mut response);
                }

                // Multiple exists
                for _ in 0..5 {
                    AggregateWatchEvent {
                        aggregate_key: key.clone(),
                        operation: AggregateWatchEventOperation::Exists {},
                    }
                    .add_to_response(&mut response);
                }

                let events = response.unwrap().events.unwrap();
                let key_events = events.get(&key).unwrap();

                // Should have exactly one entry for each, both with None payload
                assert_eq!(key_events.len(), 2);
                assert!(key_events.get(&AggregateWatchEvent::DELETE).unwrap().is_none());
                assert!(key_events.get(&AggregateWatchEvent::EXISTS).unwrap().is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_multiple_aggregates_in_response() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let mut response = None;

                let key1 = AggregateKey::new(1, 1, 1);
                let key2 = AggregateKey::new(2, 2, 2);
                let key3 = AggregateKey::new(3, 3, 3);

                AggregateWatchEvent {
                    aggregate_key: key1.clone(),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                }
                .add_to_response(&mut response);

                AggregateWatchEvent {
                    aggregate_key: key2.clone(),
                    operation: AggregateWatchEventOperation::Delete {},
                }
                .add_to_response(&mut response);

                AggregateWatchEvent {
                    aggregate_key: key3.clone(),
                    operation: AggregateWatchEventOperation::TrimStart {
                        keep_from_event_batch_index: 50,
                    },
                }
                .add_to_response(&mut response);

                let events = response.unwrap().events.unwrap();
                assert_eq!(events.len(), 3);
                assert!(events.contains_key(&key1));
                assert!(events.contains_key(&key2));
                assert!(events.contains_key(&key3));
            })
            .unwrap();

        handle.join().unwrap();
    }

    // ============================================================
    // SubscribedClient tests
    // ============================================================

    #[test]
    fn test_subscribed_client_no_latency_immediate_flush() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _sender) = crate::subscribed_client::SubscribedClient::new(None);

                // No events - should not flush
                assert!(!client.should_wait_and_flush().await);

                // Add an event
                client.accumulate_watch_event(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                });

                // With no latency requirement, should flush immediately
                assert!(client.should_wait_and_flush().await);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_subscribed_client_with_latency_waits() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _sender) =
                    crate::subscribed_client::SubscribedClient::new(Some(50));

                client.accumulate_watch_event(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                });

                // Should not flush immediately due to latency requirement
                assert!(!client.should_wait_and_flush().await);

                // Wait for latency period
                glommio::timer::sleep(Duration::from_millis(55)).await;

                // Now should be ready to flush
                assert!(client.should_wait_and_flush().await);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_subscribed_client_take_response_resets_timer() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _sender) =
                    crate::subscribed_client::SubscribedClient::new(Some(20));

                client.accumulate_watch_event(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                });

                glommio::timer::sleep(Duration::from_millis(25)).await;

                // Take the response
                let response = client.take_response();
                assert!(response.is_some());

                // Timer should be reset
                assert!(client.last_send_time.elapsed().as_millis() < 5);

                // Add another event
                client.accumulate_watch_event(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(2, 2, 2),
                    operation: AggregateWatchEventOperation::Delete {},
                });

                // Should not flush yet since timer was reset
                assert!(!client.should_wait_and_flush().await);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_subscribed_client_take_empty_response() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _sender) = crate::subscribed_client::SubscribedClient::new(None);

                // No events accumulated
                let response = client.take_response();
                assert!(response.is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_subscribed_client_watch_wait_time() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                // No latency requirement
                let (client_no_latency, _) = crate::subscribed_client::SubscribedClient::new(None);
                assert!(client_no_latency.watch_wait_time().is_none());

                // With latency but no events
                let (client_latency_no_events, _) =
                    crate::subscribed_client::SubscribedClient::new(Some(100));
                assert!(client_latency_no_events.watch_wait_time().is_none());

                // With latency and events
                let (mut client_latency_events, _) =
                    crate::subscribed_client::SubscribedClient::new(Some(100));
                client_latency_events.accumulate_watch_event(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    operation: AggregateWatchEventOperation::Exists {},
                });

                let wait_time = client_latency_events.watch_wait_time();
                assert!(wait_time.is_some());
                assert!(wait_time.unwrap().as_millis() > 90);
            })
            .unwrap();

        handle.join().unwrap();
    }

    // ============================================================
    // WatchSession tests
    // ============================================================

    #[test]
    fn test_watch_session_receives_events() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = Rc::new(AggregateWatchers::new());
                let reader = Rc::new(MockAggregateReader::new(watchers.clone()));

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None, // No latency = immediate flush
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (watcher_id, subscribed_client) = watchers.add_subscriber(request);
                let mut session = WatchSession::new(watcher_id, subscribed_client, reader);

                // Broadcast an event
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 2, 3),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                });

                // Session should return a response
                let result = session.next().await;
                assert!(result.is_ok());

                match result.unwrap() {
                    WatchOutputType::Response(response) => {
                        assert!(response.events.is_some());
                        assert_eq!(response.events.as_ref().unwrap().len(), 1);
                    }
                    other => panic!("Expected Response, got {:?}", other),
                }
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_watch_session_heartbeat_on_timeout() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = Rc::new(AggregateWatchers::new());
                let reader = Rc::new(MockAggregateReader::new(watchers.clone()));

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (watcher_id, subscribed_client) = watchers.add_subscriber(request);
                let mut session = WatchSession::new(watcher_id, subscribed_client, reader);

                // Don't send any events - should timeout and return heartbeat
                // Default timeout is 5 seconds, but watch_wait_time returns None
                // when no events, so it uses the 5 second default
                // For test speed, we rely on the timeout behavior

                let start = std::time::Instant::now();
                let result = session.next().await;
                let elapsed = start.elapsed();

                assert!(result.is_ok());
                match result.unwrap() {
                    WatchOutputType::Heartbeat => {
                        // Should have waited approximately 5 seconds (default timeout)
                        assert!(elapsed.as_secs() >= 4);
                    }
                    other => panic!("Expected Heartbeat, got {:?}", other),
                }
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_watch_session_continue_during_latency_window() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = Rc::new(AggregateWatchers::new());
                let reader = Rc::new(MockAggregateReader::new(watchers.clone()));

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: Some(200), // 200ms latency
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (watcher_id, subscribed_client) = watchers.add_subscriber(request);
                let mut session = WatchSession::new(watcher_id, subscribed_client, reader);

                // Broadcast an event
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 2, 3),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                });

                // First call should return Continue (within latency window)
                let result = session.next().await;
                assert!(result.is_ok());
                match result.unwrap() {
                    WatchOutputType::Continue => {}
                    other => panic!("Expected Continue, got {:?}", other),
                }

                // Wait for latency window to expire
                glommio::timer::sleep(Duration::from_millis(250)).await;

                // Broadcast another event to trigger processing
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(1, 2, 3),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 2,
                        to_event_batch_index: 2,
                    },
                });

                // Now should get a response with merged events
                let result = session.next().await;
                assert!(result.is_ok());
                match result.unwrap() {
                    WatchOutputType::Response(response) => {
                        let events = response.events.unwrap();
                        let key_events = events.get(&AggregateKey::new(1, 2, 3)).unwrap();
                        let write_event = key_events
                            .get(&AggregateWatchEvent::WRITE)
                            .unwrap()
                            .as_ref()
                            .unwrap();
                        // Events should be merged: from 1 to 2
                        assert_eq!(write_event.from_event_batch_index, Some(1));
                        assert_eq!(write_event.to_event_batch_index, Some(2));
                    }
                    other => panic!("Expected Response, got {:?}", other),
                }
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_watch_session_cleanup_on_drop() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = Rc::new(AggregateWatchers::new());
                let reader = Rc::new(MockAggregateReader::new(watchers.clone()));

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                assert!(watchers.is_empty());

                {
                    let (watcher_id, subscribed_client) = watchers.add_subscriber(request);
                    let _session = WatchSession::new(watcher_id, subscribed_client, reader.clone());
                    assert!(!watchers.is_empty());
                    // Session dropped here
                }

                // Watcher should be cleaned up
                assert!(watchers.is_empty());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_watch_session_done_on_channel_close() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = Rc::new(AggregateWatchers::new());
                let reader = Rc::new(MockAggregateReader::new(watchers.clone()));

                let request = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (watcher_id, subscribed_client) = watchers.add_subscriber(request);
                let mut session = WatchSession::new(watcher_id, subscribed_client, reader);

                // Remove the subscriber (simulates channel being closed)
                watchers.remove_subscriber(watcher_id);

                // Session should return Done
                let result = session.next().await;
                assert!(result.is_ok());
                match result.unwrap() {
                    WatchOutputType::Done => {}
                    other => panic!("Expected Done, got {:?}", other),
                }
            })
            .unwrap();

        handle.join().unwrap();
    }

    // ============================================================
    // Operation type constant tests
    // ============================================================

    #[test]
    fn test_operation_as_u8_values() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let key = AggregateKey::new(1, 1, 1);

                assert_eq!(
                    AggregateWatchEvent {
                        aggregate_key: key.clone(),
                        operation: AggregateWatchEventOperation::Delete {},
                    }
                    .operation_as_u8(),
                    AggregateWatchEvent::DELETE
                );

                assert_eq!(
                    AggregateWatchEvent {
                        aggregate_key: key.clone(),
                        operation: AggregateWatchEventOperation::Write {
                            from_event_batch_index: 0,
                            to_event_batch_index: 0,
                        },
                    }
                    .operation_as_u8(),
                    AggregateWatchEvent::WRITE
                );

                assert_eq!(
                    AggregateWatchEvent {
                        aggregate_key: key.clone(),
                        operation: AggregateWatchEventOperation::Read {
                            from_event_batch_index: 0,
                            to_event_batch_index: None,
                        },
                    }
                    .operation_as_u8(),
                    AggregateWatchEvent::READ
                );

                assert_eq!(
                    AggregateWatchEvent {
                        aggregate_key: key.clone(),
                        operation: AggregateWatchEventOperation::TrimStart {
                            keep_from_event_batch_index: 0,
                        },
                    }
                    .operation_as_u8(),
                    AggregateWatchEvent::TRIM_START
                );

                assert_eq!(
                    AggregateWatchEvent {
                        aggregate_key: key.clone(),
                        operation: AggregateWatchEventOperation::Exists {},
                    }
                    .operation_as_u8(),
                    AggregateWatchEvent::EXISTS
                );

                // Verify constants are contiguous from 0
                assert_eq!(AggregateWatchEvent::DELETE, 0);
                assert_eq!(AggregateWatchEvent::WRITE, 1);
                assert_eq!(AggregateWatchEvent::READ, 2);
                assert_eq!(AggregateWatchEvent::TRIM_START, 3);
                assert_eq!(AggregateWatchEvent::EXISTS, 4);
            })
            .unwrap();

        handle.join().unwrap();
    }

    // ============================================================
    // Integration-style tests
    // ============================================================

    #[test]
    fn test_full_watch_flow_multiple_clients() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = Rc::new(AggregateWatchers::new());

                // Client 1: watches org 100
                let mut orgs1 = HashSet::new();
                orgs1.insert(100u128);
                let request1 = WatchRequest {
                    correlation_id: Some(1),
                    requested_latency_ms: None,
                    orgs: Some(orgs1),
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };
                let (_id1, client1) = watchers.add_subscriber(request1);

                // Client 2: watches org 200
                let mut orgs2 = HashSet::new();
                orgs2.insert(200u128);
                let request2 = WatchRequest {
                    correlation_id: Some(2),
                    requested_latency_ms: None,
                    orgs: Some(orgs2),
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };
                let (_id2, client2) = watchers.add_subscriber(request2);

                // Client 3: watches everything
                let request3 = WatchRequest {
                    correlation_id: Some(3),
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };
                let (_id3, client3) = watchers.add_subscriber(request3);

                
                // Broadcast event for org 100
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(100, 1, 1),
                    operation: AggregateWatchEventOperation::Write {
                        from_event_batch_index: 1,
                        to_event_batch_index: 1,
                    },
                });

                // Client 1 and 3 should receive, client 2 should not
                assert!(client1.borrow().receiver.recv().await.is_some());
                assert!(poll_once(client2.borrow().receiver.recv()).await.is_none());
                assert!(client3.borrow().receiver.recv().await.is_some());

                // Broadcast event for org 200
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(200, 1, 1),
                    operation: AggregateWatchEventOperation::Delete {},
                });

                // Client 2 and 3 should receive, client 1 should not
                assert!(poll_once(client1.borrow().receiver.recv()).await.is_none());
                assert!(client2.borrow().receiver.recv().await.is_some());
                assert!(client3.borrow().receiver.recv().await.is_some());

                // Broadcast event for org 300
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(300, 1, 1),
                    operation: AggregateWatchEventOperation::Exists {},
                });

                // Only client 3 should receive
                assert!(poll_once(client1.borrow().receiver.recv()).await.is_none());
                assert!(poll_once(client2.borrow().receiver.recv()).await.is_none());
                assert!(client3.borrow().receiver.recv().await.is_some());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_high_volume_event_accumulation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let mut response = None;
                let key = AggregateKey::new(1, 1, 1);

                // Simulate 1000 write events to the same aggregate
                for i in 0..1000u64 {
                    AggregateWatchEvent {
                        aggregate_key: key.clone(),
                        operation: AggregateWatchEventOperation::Write {
                            from_event_batch_index: i,
                            to_event_batch_index: i,
                        },
                    }
                    .add_to_response(&mut response);
                }

                // Should have merged into a single write event with range 0-999
                let events = response.unwrap().events.unwrap();
                assert_eq!(events.len(), 1);

                let key_events = events.get(&key).unwrap();
                assert_eq!(key_events.len(), 1);

                let write_event = key_events
                    .get(&AggregateWatchEvent::WRITE)
                    .unwrap()
                    .as_ref()
                    .unwrap();
                assert_eq!(write_event.from_event_batch_index, Some(0));
                assert_eq!(write_event.to_event_batch_index, Some(999));
            })
            .unwrap();

        handle.join().unwrap();
    }

}