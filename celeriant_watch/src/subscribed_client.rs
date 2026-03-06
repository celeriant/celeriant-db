use celeriant_msg::response::responses::WatchResponse;
use glommio::channels::local_channel::LocalSender;
use std::time::{Duration, Instant};

use crate::aggregate_watch_event::{AggregateWatchEvent, WatchEventAccumulator};

/// Represents a single client, actively listening to a single aggregate on a shard
pub struct SubscribedClient {
    /// Configuration options for the client
    pub requested_latency: Option<Duration>,

    /// Updated when we push events to the client
    pub last_send_time: std::time::Instant,

    /// Allows us to listen to aggregate changes in the Shard WAL
    pub receiver: glommio::channels::local_channel::LocalReceiver<AggregateWatchEvent>,

    /// Events accumulate here via hashmap merging, then flatten to vec on take
    pub accumulator: Option<WatchEventAccumulator>,
}
pub const MAX_PENDING_EVENTS: usize = 10000;

impl SubscribedClient {
    pub fn new(requested_latency_ms: Option<u64>) -> (Self, LocalSender<AggregateWatchEvent>) {
        let (sender, receiver) = glommio::channels::local_channel::new_bounded(MAX_PENDING_EVENTS);

        let client = Self {
            requested_latency: requested_latency_ms.map(Duration::from_millis),
            receiver,
            last_send_time: Instant::now(),
            accumulator: None,
        };
        (client, sender)
    }

    pub fn accumulate_watch_event(&mut self, watch_event: AggregateWatchEvent) {
        self.accumulator.get_or_insert_default().accumulate(watch_event);
    }

    pub async fn should_wait_and_flush(&self) -> bool {
        if self.accumulator.is_none() {
            return false;
        }

        self.additional_latency_wait_time() == Duration::ZERO
    }

    pub fn take_response(&mut self) -> Option<WatchResponse> {
        self.last_send_time = Instant::now();
        self.accumulator.take().map(|acc| acc.into_response())
    }

    fn additional_latency_wait_time(&self) -> Duration {
        let Some(latency) = self.requested_latency else {
            return Duration::ZERO;
        };

        let elapsed = self.last_send_time.elapsed();
        latency.saturating_sub(elapsed)
    }

    pub fn watch_wait_time(&self) -> Option<Duration> {
        let Some(latency) = self.requested_latency else {
            return None;
        };

        if self.accumulator.is_none() {
            return None;
        }

        let elapsed = self.last_send_time.elapsed();
        Some(latency.saturating_sub(elapsed))
    }
}

#[cfg(test)]
mod test_subscribed_client {
    use std::time::{Duration, Instant};

    use celeriant_wal::aggregate_key::AggregateKey;
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{aggregate_watch_event::AggregateWatchEvent, subscribed_client::SubscribedClient};

    #[test]
    fn test_individual_requirements() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _) = SubscribedClient::new(None);

                // No requirements
                assert_eq!(client.additional_latency_wait_time(), Duration::ZERO);

                // Latency in isolation
                client.requested_latency = Some(Duration::from_millis(10));

                assert_ne!(client.additional_latency_wait_time(), Duration::ZERO);
                assert!(client.additional_latency_wait_time().as_millis() > 5);

                glommio::timer::sleep(client.additional_latency_wait_time()).await;

                assert_eq!(client.additional_latency_wait_time(), Duration::ZERO);

                client.last_send_time = Instant::now();
                assert!(client.additional_latency_wait_time().as_millis() > 5);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_flow() {
        let handle =
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move {
                    let (mut client, _) = SubscribedClient::new(None);

                    client.requested_latency = Some(Duration::from_millis(10));

                    client.accumulate_watch_event(AggregateWatchEvent {
                        aggregate_key: AggregateKey::new(1, 2, 3),
                        operation:
                            crate::aggregate_watch_event::AggregateWatchEventOperation::Delete {},
                    });

                    assert!(!client.accumulator.as_ref().unwrap().is_empty());
                    assert!(client.last_send_time.elapsed().as_millis() <= 50);
                    assert!(!client.should_wait_and_flush().await);

                    client.accumulate_watch_event(AggregateWatchEvent {
                        aggregate_key: AggregateKey::new(1, 2, 4),
                        operation:
                            crate::aggregate_watch_event::AggregateWatchEventOperation::Read {
                                from_event_batch_index: 44,
                                to_event_batch_index: Some(46),
                            },
                    });

                    assert!(!client.accumulator.as_ref().unwrap().is_empty());
                    assert!(client.last_send_time.elapsed().as_millis() <= 50);
                    assert!(!client.should_wait_and_flush().await);

                    glommio::timer::sleep(client.additional_latency_wait_time()).await;

                    assert!(client.should_wait_and_flush().await);
                    let elapsed_ms = client.last_send_time.elapsed().as_millis();
                    assert!(
                        elapsed_ms >= 10 && elapsed_ms <= 100,
                        "elapsed_ms was {elapsed_ms}, expected 10-100"
                    );

                    let watch_response = client.take_response().unwrap();
                    assert!(client.accumulator.is_none());
                    assert!(client.last_send_time.elapsed().as_millis() <= 50);

                    // 2 events: Delete for (1,2,3) and Read for (1,2,4)
                    assert_eq!(watch_response.events.len(), 2);

                    let delete = watch_response.events.iter()
                        .find(|e| e.aggregate_id == 3 && e.operation == AggregateWatchEvent::DELETE)
                        .expect("expected delete event");
                    assert!(delete.from_event_batch_index.is_none());

                    let read = watch_response.events.iter()
                        .find(|e| e.aggregate_id == 4 && e.operation == AggregateWatchEvent::READ)
                        .expect("expected read event");
                    assert_eq!(read.from_event_batch_index, Some(44));
                    assert_eq!(read.to_event_batch_index, Some(46));
                })
                .unwrap();

        handle.join().unwrap();
    }
}
