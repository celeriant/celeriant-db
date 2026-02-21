#[cfg(test)]
mod tests {
    use std::{collections::HashSet, rc::Rc, time::Duration};

    use celeriant_msg::request::requests::WatchRequest;
    use celeriant_wal::aggregate_key::AggregateKey;
    use futures_lite::future::poll_once;
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        aggregate_reader::AggregateReader,
        aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation},
        aggregate_watchers::AggregateWatchers,
        subscribed_client::{SubscribedClient, MAX_PENDING_EVENTS},
        watch_output_type::WatchOutputType,
        watch_session::WatchSession,
    };

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    fn watch_request() -> WatchRequest {
        WatchRequest {
            correlation_id: Some(1),
            requested_latency_ms: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        }
    }

    fn write_event(org: u128, type_id: u128, id: u128, batch_index: u64) -> AggregateWatchEvent {
        AggregateWatchEvent {
            aggregate_key: AggregateKey::new(org, type_id, id),
            operation: AggregateWatchEventOperation::Write {
                from_event_batch_index: batch_index,
                to_event_batch_index: batch_index,
            },
        }
    }

    fn delete_event(org: u128, type_id: u128, id: u128) -> AggregateWatchEvent {
        AggregateWatchEvent {
            aggregate_key: AggregateKey::new(org, type_id, id),
            operation: AggregateWatchEventOperation::Delete {},
        }
    }

    fn exists_event(org: u128, type_id: u128, id: u128) -> AggregateWatchEvent {
        AggregateWatchEvent {
            aggregate_key: AggregateKey::new(org, type_id, id),
            operation: AggregateWatchEventOperation::AggregateDetails {},
        }
    }

    struct MockAggregateReader {
        watchers: Rc<AggregateWatchers>,
    }

    impl AggregateReader for MockAggregateReader {
        fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
            self.watchers.clone()
        }
    }

    #[test]
    fn watchers_add_and_remove_subscriber() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            assert!(watchers.is_empty());

            let (id1, _) = watchers.add_subscriber(watch_request());
            assert!(!watchers.is_empty());

            let (id2, _) = watchers.add_subscriber(watch_request());
            assert_eq!(id2, id1 + 1);

            watchers.remove_subscriber(id1);
            assert!(!watchers.is_empty());

            watchers.remove_subscriber(id2);
            assert!(watchers.is_empty());
        })
    }

    #[test]
    fn watchers_remove_nonexistent_subscriber_does_not_panic() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            watchers.remove_subscriber(999);
            assert!(watchers.is_empty());
        })
    }

    #[test]
    fn watchers_broadcast_to_single_subscriber() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            let (_, client) = watchers.add_subscriber(watch_request());

            watchers.broadcast(write_event(1, 2, 3, 10));

            let received = client.borrow().receiver.recv().await.unwrap();
            assert_eq!(received.aggregate_key, AggregateKey::new(1, 2, 3));
        })
    }

    #[test]
    fn watchers_broadcast_to_multiple_subscribers() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            let (_, c1) = watchers.add_subscriber(watch_request());
            let (_, c2) = watchers.add_subscriber(watch_request());
            let (_, c3) = watchers.add_subscriber(watch_request());

            watchers.broadcast(delete_event(1, 2, 3));

            assert!(c1.borrow().receiver.recv().await.is_some());
            assert!(c2.borrow().receiver.recv().await.is_some());
            assert!(c3.borrow().receiver.recv().await.is_some());
        })
    }

    #[test]
    fn slow_client_removed_on_full_channel() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            let (_, _client) = watchers.add_subscriber(watch_request());
            assert!(!watchers.is_empty());

            for i in 0..(MAX_PENDING_EVENTS + 100) {
                watchers.broadcast(write_event(1, 2, i as u128, i as u64));
            }

            assert!(watchers.is_empty());
        })
    }

    #[test]
    fn filter_by_org() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            let mut req = watch_request();
            req.orgs = Some(HashSet::from([100u128]));
            let (_, client) = watchers.add_subscriber(req);

            watchers.broadcast(write_event(100, 1, 1, 1));
            assert!(client.borrow().receiver.recv().await.is_some());

            watchers.broadcast(write_event(200, 1, 1, 2));
            assert!(poll_once(client.borrow().receiver.recv()).await.is_none());
        })
    }

    #[test]
    fn filter_by_aggregate_type() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            let mut req = watch_request();
            req.aggregate_types = Some(HashSet::from([50]));
            let (_, client) = watchers.add_subscriber(req);

            watchers.broadcast(exists_event(100, 50, 999));
            assert!(client.borrow().receiver.recv().await.is_some());

            watchers.broadcast(exists_event(100, 51, 999));
            assert!(poll_once(client.borrow().receiver.recv()).await.is_none());

            watchers.broadcast(exists_event(101, 50, 999));
            assert!(client.borrow().receiver.recv().await.is_some());
        })
    }

    #[test]
    fn filter_by_specific_aggregate() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            let mut req = watch_request();
            req.aggregates = Some(HashSet::from([3u128]));
            let (_, client) = watchers.add_subscriber(req);

            watchers.broadcast(delete_event(1, 2, 3));
            assert!(client.borrow().receiver.recv().await.is_some());

            watchers.broadcast(delete_event(1, 2, 4));
            assert!(poll_once(client.borrow().receiver.recv()).await.is_none());
        })
    }

    #[test]
    fn filter_by_operation_type() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            let mut req = watch_request();
            req.operation_types =
                Some(HashSet::from([AggregateWatchEvent::WRITE, AggregateWatchEvent::DELETE]));
            let (_, client) = watchers.add_subscriber(req);

            watchers.broadcast(write_event(1, 1, 1, 1));
            assert!(client.borrow().receiver.recv().await.is_some());

            watchers.broadcast(delete_event(1, 1, 1));
            assert!(client.borrow().receiver.recv().await.is_some());

            watchers.broadcast(AggregateWatchEvent {
                aggregate_key: AggregateKey::new(1, 1, 1),
                operation: AggregateWatchEventOperation::Read {
                    from_event_batch_index: 1,
                    to_event_batch_index: Some(5),
                },
            });
            assert!(poll_once(client.borrow().receiver.recv()).await.is_none());

            watchers.broadcast(exists_event(1, 1, 1));
            assert!(poll_once(client.borrow().receiver.recv()).await.is_none());
        })
    }

    #[test]
    fn filter_combination_and_logic() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            let mut req = watch_request();
            req.orgs = Some(HashSet::from([100u128]));
            req.aggregates = Some(HashSet::from([1u128]));
            let (_, client) = watchers.add_subscriber(req);

            watchers.broadcast(write_event(100, 1, 1, 2));
            assert!(client.borrow().receiver.recv().await.is_some());

            watchers.broadcast(write_event(300, 1, 1, 3));
            assert!(poll_once(client.borrow().receiver.recv()).await.is_none());
        })
    }

    #[test]
    fn no_filters_matches_everything() {
        glommio_test!({
            let watchers = AggregateWatchers::new();
            let (_, client) = watchers.add_subscriber(watch_request());

            let operations = [
                AggregateWatchEventOperation::Write {
                    from_event_batch_index: 1,
                    to_event_batch_index: 1,
                },
                AggregateWatchEventOperation::Read {
                    from_event_batch_index: 1,
                    to_event_batch_index: Some(5),
                },
                AggregateWatchEventOperation::Delete {},
                AggregateWatchEventOperation::AggregateDetails {},
                AggregateWatchEventOperation::TrimStart {
                    keep_from_event_batch_index: 10,
                },
            ];

            for op in operations {
                watchers.broadcast(AggregateWatchEvent {
                    aggregate_key: AggregateKey::new(rand::random(), rand::random(), rand::random()),
                    operation: op,
                });
                assert!(client.borrow().receiver.recv().await.is_some());
            }
        })
    }

    #[test]
    fn write_events_merge_ranges() {
        glommio_test!({
            let mut response = None;

            write_event(1, 2, 3, 5).add_to_response(&mut response);
            write_event(1, 2, 3, 10).add_to_response(&mut response);
            write_event(1, 2, 3, 2).add_to_response(&mut response);

            let k = AggregateKey::new(1, 2, 3);
            let events = response.unwrap().events.unwrap();
            let write = events[&k][&AggregateWatchEvent::WRITE].as_ref().unwrap();
            assert_eq!(write.from_event_batch_index, Some(2));
            assert_eq!(write.to_event_batch_index, Some(10));
        })
    }

    #[test]
    fn read_events_merge_ranges_with_none() {
        glommio_test!({
            let mut response = None;
            let k = AggregateKey::new(1, 2, 3);

            AggregateWatchEvent {
                aggregate_key: k.clone(),
                operation: AggregateWatchEventOperation::Read {
                    from_event_batch_index: 5,
                    to_event_batch_index: Some(10),
                },
            }
            .add_to_response(&mut response);

            AggregateWatchEvent {
                aggregate_key: k.clone(),
                operation: AggregateWatchEventOperation::Read {
                    from_event_batch_index: 3,
                    to_event_batch_index: None,
                },
            }
            .add_to_response(&mut response);

            let events = response.unwrap().events.unwrap();
            let read = events[&k][&AggregateWatchEvent::READ].as_ref().unwrap();
            assert_eq!(read.from_event_batch_index, Some(3));
            assert_eq!(read.to_event_batch_index, Some(10));
        })
    }

    #[test]
    fn trim_start_replaces_previous() {
        glommio_test!({
            let mut response = None;
            let k = AggregateKey::new(1, 2, 3);

            AggregateWatchEvent {
                aggregate_key: k.clone(),
                operation: AggregateWatchEventOperation::TrimStart {
                    keep_from_event_batch_index: 5,
                },
            }
            .add_to_response(&mut response);

            AggregateWatchEvent {
                aggregate_key: k.clone(),
                operation: AggregateWatchEventOperation::TrimStart {
                    keep_from_event_batch_index: 10,
                },
            }
            .add_to_response(&mut response);

            let events = response.unwrap().events.unwrap();
            let trim = events[&k][&AggregateWatchEvent::TRIM_START].as_ref().unwrap();
            assert_eq!(trim.keep_from_event_batch_index, Some(10));
        })
    }

    #[test]
    fn delete_and_exists_deduplicate() {
        glommio_test!({
            let mut response = None;
            let k = AggregateKey::new(1, 2, 3);

            for _ in 0..5 {
                delete_event(1, 2, 3).add_to_response(&mut response);
                exists_event(1, 2, 3).add_to_response(&mut response);
            }

            let events = &response.unwrap().events.unwrap()[&k];
            assert_eq!(events.len(), 2);
            assert!(events[&AggregateWatchEvent::DELETE].is_none());
            assert!(events[&AggregateWatchEvent::DETAILS].is_none());
        })
    }

    #[test]
    fn multiple_aggregates_in_response() {
        glommio_test!({
            let mut response = None;

            write_event(1, 1, 1, 1).add_to_response(&mut response);
            delete_event(2, 2, 2).add_to_response(&mut response);
            AggregateWatchEvent {
                aggregate_key: AggregateKey::new(3, 3, 3),
                operation: AggregateWatchEventOperation::TrimStart {
                    keep_from_event_batch_index: 50,
                },
            }
            .add_to_response(&mut response);

            let events = response.unwrap().events.unwrap();
            assert_eq!(events.len(), 3);
            assert!(events.contains_key(&AggregateKey::new(1, 1, 1)));
            assert!(events.contains_key(&AggregateKey::new(2, 2, 2)));
            assert!(events.contains_key(&AggregateKey::new(3, 3, 3)));
        })
    }

    #[test]
    fn subscribed_client_no_latency_immediate_flush() {
        glommio_test!({
            let (mut client, _) = SubscribedClient::new(None);

            assert!(!client.should_wait_and_flush().await);

            client.accumulate_watch_event(write_event(1, 1, 1, 1));
            assert!(client.should_wait_and_flush().await);
        })
    }

    #[test]
    fn subscribed_client_with_latency_waits() {
        glommio_test!({
            let (mut client, _) = SubscribedClient::new(Some(50));

            client.accumulate_watch_event(write_event(1, 1, 1, 1));
            assert!(!client.should_wait_and_flush().await);

            glommio::timer::sleep(Duration::from_millis(55)).await;
            assert!(client.should_wait_and_flush().await);
        })
    }

    #[test]
    fn subscribed_client_take_response_resets_timer() {
        glommio_test!({
            let (mut client, _) = SubscribedClient::new(Some(20));

            client.accumulate_watch_event(write_event(1, 1, 1, 1));
            glommio::timer::sleep(Duration::from_millis(25)).await;

            assert!(client.take_response().is_some());
            assert!(client.last_send_time.elapsed().as_millis() < 5);

            client.accumulate_watch_event(delete_event(2, 2, 2));
            assert!(!client.should_wait_and_flush().await);
        })
    }

    #[test]
    fn subscribed_client_take_empty_response() {
        glommio_test!({
            let (mut client, _) = SubscribedClient::new(None);
            assert!(client.take_response().is_none());
        })
    }

    #[test]
    fn subscribed_client_watch_wait_time() {
        glommio_test!({
            let (no_latency, _) = SubscribedClient::new(None);
            assert!(no_latency.watch_wait_time().is_none());

            let (latency_no_events, _) = SubscribedClient::new(Some(100));
            assert!(latency_no_events.watch_wait_time().is_none());

            let (mut latency_with_events, _) = SubscribedClient::new(Some(100));
            latency_with_events.accumulate_watch_event(exists_event(1, 1, 1));
            let wait = latency_with_events.watch_wait_time().unwrap();
            assert!(wait.as_millis() > 90);
        })
    }

    #[test]
    fn watch_session_receives_events() {
        glommio_test!({
            let watchers = Rc::new(AggregateWatchers::new());
            let reader = Rc::new(MockAggregateReader {
                watchers: watchers.clone(),
            });

            let (id, client) = watchers.add_subscriber(watch_request());
            let mut session = WatchSession::new(id, client, reader);

            watchers.broadcast(write_event(1, 2, 3, 1));

            match session.next().await.unwrap() {
                WatchOutputType::Response(r) => {
                    assert_eq!(r.events.unwrap().len(), 1);
                }
                other => panic!("Expected Response, got {:?}", other),
            }
        })
    }

    #[test]
    #[ignore] // Takes 5+ seconds - run with `cargo test -- --ignored`
    fn watch_session_heartbeat_on_timeout() {
        glommio_test!({
            let watchers = Rc::new(AggregateWatchers::new());
            let reader = Rc::new(MockAggregateReader {
                watchers: watchers.clone(),
            });

            let (id, client) = watchers.add_subscriber(watch_request());
            let mut session = WatchSession::new(id, client, reader);

            let start = std::time::Instant::now();
            match session.next().await.unwrap() {
                WatchOutputType::Heartbeat => assert!(start.elapsed().as_secs() >= 4),
                other => panic!("Expected Heartbeat, got {:?}", other),
            }
        })
    }

    #[test]
    fn watch_session_continue_during_latency_window() {
        glommio_test!({
            let watchers = Rc::new(AggregateWatchers::new());
            let reader = Rc::new(MockAggregateReader {
                watchers: watchers.clone(),
            });

            let mut req = watch_request();
            req.requested_latency_ms = Some(200);
            let (id, client) = watchers.add_subscriber(req);
            let mut session = WatchSession::new(id, client, reader);

            watchers.broadcast(write_event(1, 2, 3, 1));

            match session.next().await.unwrap() {
                WatchOutputType::Continue => {}
                other => panic!("Expected Continue, got {:?}", other),
            }

            glommio::timer::sleep(Duration::from_millis(250)).await;
            watchers.broadcast(write_event(1, 2, 3, 2));

            match session.next().await.unwrap() {
                WatchOutputType::Response(r) => {
                    let k = AggregateKey::new(1, 2, 3);
                    let events = r.events.unwrap();
                    let write = events[&k][&AggregateWatchEvent::WRITE].as_ref().unwrap();
                    assert_eq!(write.from_event_batch_index, Some(1));
                    assert_eq!(write.to_event_batch_index, Some(2));
                }
                other => panic!("Expected Response, got {:?}", other),
            }
        })
    }

    #[test]
    fn watch_session_cleanup_on_drop() {
        glommio_test!({
            let watchers = Rc::new(AggregateWatchers::new());
            let reader = Rc::new(MockAggregateReader {
                watchers: watchers.clone(),
            });

            assert!(watchers.is_empty());
            {
                let (id, client) = watchers.add_subscriber(watch_request());
                let _session = WatchSession::new(id, client, reader.clone());
                assert!(!watchers.is_empty());
            }
            assert!(watchers.is_empty());
        })
    }

    #[test]
    fn watch_session_done_on_channel_close() {
        glommio_test!({
            let watchers = Rc::new(AggregateWatchers::new());
            let reader = Rc::new(MockAggregateReader {
                watchers: watchers.clone(),
            });

            let (id, client) = watchers.add_subscriber(watch_request());
            let mut session = WatchSession::new(id, client, reader);

            watchers.remove_subscriber(id);

            match session.next().await.unwrap() {
                WatchOutputType::Done => {}
                other => panic!("Expected Done, got {:?}", other),
            }
        })
    }

    #[test]
    fn operation_as_u8_values() {
        assert_eq!(AggregateWatchEvent::DELETE, 0);
        assert_eq!(AggregateWatchEvent::WRITE, 1);
        assert_eq!(AggregateWatchEvent::READ, 2);
        assert_eq!(AggregateWatchEvent::TRIM_START, 3);
        assert_eq!(AggregateWatchEvent::DETAILS, 4);

        assert_eq!(delete_event(1, 1, 1).operation_as_u8(), AggregateWatchEvent::DELETE);
        assert_eq!(write_event(1, 1, 1, 0).operation_as_u8(), AggregateWatchEvent::WRITE);
        assert_eq!(exists_event(1, 1, 1).operation_as_u8(), AggregateWatchEvent::DETAILS);
    }

    #[test]
    fn full_watch_flow_multiple_clients() {
        glommio_test!({
            let watchers = Rc::new(AggregateWatchers::new());

            let mut req1 = watch_request();
            req1.orgs = Some(HashSet::from([100u128]));
            let (_, c1) = watchers.add_subscriber(req1);

            let mut req2 = watch_request();
            req2.orgs = Some(HashSet::from([200u128]));
            let (_, c2) = watchers.add_subscriber(req2);

            let (_, c3) = watchers.add_subscriber(watch_request());

            // Event for org 100: c1 and c3 receive
            watchers.broadcast(write_event(100, 1, 1, 1));
            assert!(c1.borrow().receiver.recv().await.is_some());
            assert!(poll_once(c2.borrow().receiver.recv()).await.is_none());
            assert!(c3.borrow().receiver.recv().await.is_some());

            // Event for org 200: c2 and c3 receive
            watchers.broadcast(delete_event(200, 1, 1));
            assert!(poll_once(c1.borrow().receiver.recv()).await.is_none());
            assert!(c2.borrow().receiver.recv().await.is_some());
            assert!(c3.borrow().receiver.recv().await.is_some());

            // Event for org 300: only c3 receives
            watchers.broadcast(exists_event(300, 1, 1));
            assert!(poll_once(c1.borrow().receiver.recv()).await.is_none());
            assert!(poll_once(c2.borrow().receiver.recv()).await.is_none());
            assert!(c3.borrow().receiver.recv().await.is_some());
        })
    }

    #[test]
    fn high_volume_event_accumulation() {
        glommio_test!({
            let mut response = None;

            for i in 0..1000u64 {
                write_event(1, 1, 1, i).add_to_response(&mut response);
            }

            let k = AggregateKey::new(1, 1, 1);
            let events = response.unwrap().events.unwrap();
            assert_eq!(events.len(), 1);

            let write = events[&k][&AggregateWatchEvent::WRITE].as_ref().unwrap();
            assert_eq!(write.from_event_batch_index, Some(0));
            assert_eq!(write.to_event_batch_index, Some(999));
        })
    }
}
