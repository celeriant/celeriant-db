use std::collections::HashMap;

use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch;
use celeriant_watch::aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation};
use celeriant_watch::aggregate_watchers::AggregateWatchers;

#[derive(Default)]
pub(crate) struct WatchEventCollector {
    create_events: HashMap<AggregateKey, AggregateWatchEventOperation>,
    write_events: HashMap<AggregateKey, AggregateWatchEventOperation>,
    delete_events: HashMap<AggregateKey, AggregateWatchEventOperation>,
    trim_events: HashMap<AggregateKey, AggregateWatchEventOperation>,
}

impl WatchEventCollector {
    /// Creates a new empty event collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a write event, extending the batch index range if the aggregate already has a write event.
    pub fn add_write_event(&mut self, metablock_event_batch: &MetablockEventBatch) {
        self.write_events
            .entry(metablock_event_batch.aggregate_key.clone())
            .and_modify(|event| {
                if let AggregateWatchEventOperation::Write {
                    from_event_batch_index,
                    to_event_batch_index,
                } = event
                {
                    if metablock_event_batch.event_batch_index < *from_event_batch_index {
                        *from_event_batch_index = metablock_event_batch.event_batch_index;
                    }
                    if metablock_event_batch.event_batch_index > *to_event_batch_index {
                        *to_event_batch_index = metablock_event_batch.event_batch_index;
                    }
                }
            })
            .or_insert(AggregateWatchEventOperation::Write {
                from_event_batch_index: metablock_event_batch.event_batch_index,
                to_event_batch_index: metablock_event_batch.event_batch_index,
            });
    }

    /// Records a create event for a new aggregate.
    pub fn add_create_event(&mut self, aggregate_key: AggregateKey) {
        self.create_events.entry(aggregate_key).or_insert(AggregateWatchEventOperation::Create {});
    }

    /// Records a delete event for an aggregate.
    pub fn add_delete_event(&mut self, aggregate_key: AggregateKey) {
        self.delete_events.entry(aggregate_key).or_insert(AggregateWatchEventOperation::Delete {});
    }

    /// Records a trim event for an aggregate.
    pub fn add_trim_event(&mut self, aggregate_key: AggregateKey, keep_from_event_batch_index: u64) {
        self.trim_events
            .entry(aggregate_key)
            .or_insert(AggregateWatchEventOperation::TrimStart { keep_from_event_batch_index });
    }

    /// Broadcasts all collected events to watchers in the correct order.
    ///
    /// Order: Create -> Write -> Delete -> Trim
    pub fn broadcast_all(self, watched_aggregates: &AggregateWatchers) {
        for (aggregate_key, operation) in self.create_events {
            watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
        }

        for (aggregate_key, operation) in self.write_events {
            watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
        }

        for (aggregate_key, operation) in self.delete_events {
            watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
        }

        for (aggregate_key, operation) in self.trim_events {
            watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_msg::request::requests::WatchRequest;
    use celeriant_wal::constants::BLOOM_BYTES;
    use celeriant_wal::metablocks::metablock_event_batch::EventTypesKind;
    use futures_lite::future::poll_once;
    use glommio::{LocalExecutorBuilder, Placement};

    fn make_aggregate_key(org: u128, agg_type: u128, agg_id: u128) -> AggregateKey {
        AggregateKey::new(org, agg_type, agg_id)
    }

    fn make_event_batch(aggregate_key: AggregateKey, event_batch_index: u64) -> MetablockEventBatch {
        MetablockEventBatch {
            client_id: 1,
            user_id: None,
            aggregate_key,
            event_batch_index,
            min_event_batch_index: 1,
            min_event_index: 1,
            max_event_index: 1,
            min_event_timestamp: 0,
            max_event_timestamp: 0,
            min_client_event_index: 1,
            max_client_event_index: 1,
            event_types_data: EventTypesKind::Direct([0u64; BLOOM_BYTES / 8]),
        }
    }

    #[test]
    fn collector_add_write_event_inserts_new_aggregate() {
        let mut collector = WatchEventCollector::new();
        let key = make_aggregate_key(1, 2, 3);
        let batch = make_event_batch(key.clone(), 5);

        collector.add_write_event(&batch);

        match collector.write_events.get(&key) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 5);
                assert_eq!(*to_event_batch_index, 5);
            }
            _ => panic!("Expected Write event"),
        }
    }

    #[test]
    fn collector_add_write_event_expands_range_with_lower_index() {
        let mut collector = WatchEventCollector::new();
        let key = make_aggregate_key(1, 2, 3);

        collector.add_write_event(&make_event_batch(key.clone(), 10));
        collector.add_write_event(&make_event_batch(key.clone(), 5));

        match collector.write_events.get(&key) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 5);
                assert_eq!(*to_event_batch_index, 10);
            }
            _ => panic!("Expected Write event"),
        }
    }

    #[test]
    fn collector_add_write_event_expands_range_with_higher_index() {
        let mut collector = WatchEventCollector::new();
        let key = make_aggregate_key(1, 2, 3);

        collector.add_write_event(&make_event_batch(key.clone(), 5));
        collector.add_write_event(&make_event_batch(key.clone(), 15));

        match collector.write_events.get(&key) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 5);
                assert_eq!(*to_event_batch_index, 15);
            }
            _ => panic!("Expected Write event"),
        }
    }

    #[test]
    fn collector_add_write_event_handles_multiple_aggregates() {
        let mut collector = WatchEventCollector::new();
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(2, 2, 2);

        collector.add_write_event(&make_event_batch(key1.clone(), 5));
        collector.add_write_event(&make_event_batch(key2.clone(), 10));
        collector.add_write_event(&make_event_batch(key1.clone(), 7));

        assert_eq!(collector.write_events.len(), 2);

        match collector.write_events.get(&key1) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 5);
                assert_eq!(*to_event_batch_index, 7);
            }
            _ => panic!("Expected Write event for key1"),
        }

        match collector.write_events.get(&key2) {
            Some(AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            }) => {
                assert_eq!(*from_event_batch_index, 10);
                assert_eq!(*to_event_batch_index, 10);
            }
            _ => panic!("Expected Write event for key2"),
        }
    }

    #[test]
    fn collector_add_create_event_inserts_new_aggregate() {
        let mut collector = WatchEventCollector::new();
        let key = make_aggregate_key(1, 2, 3);

        collector.add_create_event(key.clone());

        assert_eq!(collector.create_events.len(), 1);
        assert!(matches!(collector.create_events.get(&key), Some(AggregateWatchEventOperation::Create {})));
    }

    #[test]
    fn collector_add_create_event_does_not_duplicate() {
        let mut collector = WatchEventCollector::new();
        let key = make_aggregate_key(1, 2, 3);

        collector.add_create_event(key.clone());
        collector.add_create_event(key.clone());

        assert_eq!(collector.create_events.len(), 1);
    }

    #[test]
    fn collector_add_create_event_handles_multiple_aggregates() {
        let mut collector = WatchEventCollector::new();
        let key1 = make_aggregate_key(1, 1, 1);
        let key2 = make_aggregate_key(2, 2, 2);
        let key3 = make_aggregate_key(3, 3, 3);

        collector.add_create_event(key1.clone());
        collector.add_create_event(key2.clone());
        collector.add_create_event(key3.clone());

        assert_eq!(collector.create_events.len(), 3);
        assert!(matches!(
            collector.create_events.get(&key1),
            Some(AggregateWatchEventOperation::Create {})
        ));
        assert!(matches!(
            collector.create_events.get(&key2),
            Some(AggregateWatchEventOperation::Create {})
        ));
        assert!(matches!(
            collector.create_events.get(&key3),
            Some(AggregateWatchEventOperation::Create {})
        ));
    }

    #[test]
    fn collector_add_delete_event_inserts_new_aggregate() {
        let mut collector = WatchEventCollector::new();
        let key = make_aggregate_key(1, 2, 3);

        collector.add_delete_event(key.clone());

        assert_eq!(collector.delete_events.len(), 1);
        assert!(matches!(collector.delete_events.get(&key), Some(AggregateWatchEventOperation::Delete {})));
    }

    #[test]
    fn collector_add_trim_event_inserts_new_aggregate() {
        let mut collector = WatchEventCollector::new();
        let key = make_aggregate_key(1, 2, 3);

        collector.add_trim_event(key.clone(), 5);

        assert_eq!(collector.trim_events.len(), 1);
        match collector.trim_events.get(&key) {
            Some(AggregateWatchEventOperation::TrimStart { keep_from_event_batch_index }) => {
                assert_eq!(*keep_from_event_batch_index, 5);
            }
            _ => panic!("Expected TrimStart event"),
        }
    }

    #[test]
    fn collector_broadcast_all_sends_events_to_watchers() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let watch_request = WatchRequest {
                    correlation_id: None,
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (_id, subscribed_client) = watchers.add_subscriber(watch_request);

                let mut collector = WatchEventCollector::new();
                let key1 = make_aggregate_key(1, 1, 1);
                let key2 = make_aggregate_key(1, 1, 2);
                let key3 = make_aggregate_key(1, 1, 3);
                let key4 = make_aggregate_key(1, 1, 4);

                collector.add_create_event(key1.clone());
                collector.add_write_event(&make_event_batch(key2.clone(), 5));
                collector.add_delete_event(key3.clone());
                collector.add_trim_event(key4.clone(), 10);

                collector.broadcast_all(&watchers);

                // Verify all 4 events were received
                let event1 = subscribed_client.borrow().receiver.recv().await.unwrap();
                assert_eq!(event1.aggregate_key, key1);
                assert!(matches!(event1.operation, AggregateWatchEventOperation::Create {}));

                let event2 = subscribed_client.borrow().receiver.recv().await.unwrap();
                assert_eq!(event2.aggregate_key, key2);
                assert!(matches!(
                    event2.operation,
                    AggregateWatchEventOperation::Write {
                        from_event_batch_index: 5,
                        to_event_batch_index: 5
                    }
                ));

                let event3 = subscribed_client.borrow().receiver.recv().await.unwrap();
                assert_eq!(event3.aggregate_key, key3);
                assert!(matches!(event3.operation, AggregateWatchEventOperation::Delete {}));

                let event4 = subscribed_client.borrow().receiver.recv().await.unwrap();
                assert_eq!(event4.aggregate_key, key4);
                assert!(matches!(
                    event4.operation,
                    AggregateWatchEventOperation::TrimStart {
                        keep_from_event_batch_index: 10
                    }
                ));

                // No more events
                assert!(poll_once(subscribed_client.borrow().receiver.recv()).await.is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn collector_broadcast_all_filters_by_operation_type() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                // Only subscribe to Write events
                let mut operation_types = std::collections::HashSet::new();
                operation_types.insert(AggregateWatchEvent::WRITE);

                let watch_request = WatchRequest {
                    correlation_id: None,
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: Some(operation_types),
                };

                let (_id, subscribed_client) = watchers.add_subscriber(watch_request);

                let mut collector = WatchEventCollector::new();
                let key1 = make_aggregate_key(1, 1, 1);
                let key2 = make_aggregate_key(1, 1, 2);

                collector.add_create_event(key1.clone());
                collector.add_write_event(&make_event_batch(key2.clone(), 5));
                collector.add_delete_event(make_aggregate_key(1, 1, 3));

                collector.broadcast_all(&watchers);

                // Only write event should be received
                let event = subscribed_client.borrow().receiver.recv().await.unwrap();
                assert_eq!(event.aggregate_key, key2);
                assert!(matches!(event.operation, AggregateWatchEventOperation::Write { .. }));

                // No more events
                assert!(poll_once(subscribed_client.borrow().receiver.recv()).await.is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn collector_broadcast_all_with_empty_collector_sends_nothing() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let watch_request = WatchRequest {
                    correlation_id: None,
                    requested_latency_ms: None,
                    orgs: None,
                    aggregate_types: None,
                    aggregates: None,
                    operation_types: None,
                };

                let (_id, subscribed_client) = watchers.add_subscriber(watch_request);

                let collector = WatchEventCollector::new();
                collector.broadcast_all(&watchers);

                assert!(poll_once(subscribed_client.borrow().receiver.recv()).await.is_none());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn collector_broadcast_all_with_no_subscribers() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let watchers = AggregateWatchers::new();

                let mut collector = WatchEventCollector::new();
                collector.add_create_event(make_aggregate_key(1, 1, 1));
                collector.add_write_event(&make_event_batch(make_aggregate_key(1, 1, 2), 5));

                // Should not panic with no subscribers
                collector.broadcast_all(&watchers);

                assert!(watchers.is_empty());
            })
            .unwrap();

        handle.join().unwrap();
    }
}
