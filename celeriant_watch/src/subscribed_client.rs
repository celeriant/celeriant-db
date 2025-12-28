use celeriant_msg::response::responses::WatchResponse;
use glommio::channels::local_channel::LocalSender;
use std::time::{Duration, Instant};

use crate::aggregate_watch_event::AggregateWatchEvent;

/// Represents a single client, actively listening to a single aggregate on a shard
pub struct SubscribedClient {
    /// Configuration options for the client
    pub requested_latency: Option<Duration>,

    /// Updated when we push events to the client
    pub last_send_time: std::time::Instant,

    /// Allows us to listen to aggregate changes in the Shard WAL
    pub receiver: glommio::channels::local_channel::LocalReceiver<AggregateWatchEvent>,

    /// Events accumulate here as other clients perform operations
    /// They come from the local_channel, and after a certain amount of time they are sent to the client
    pub watch_response: Option<WatchResponse>,
}
pub const MAX_PENDING_EVENTS: usize = 10000;

impl SubscribedClient {
    pub fn new(requested_latency_ms: Option<u64>) -> (Self, LocalSender<AggregateWatchEvent>) {
        let (sender, receiver) = glommio::channels::local_channel::new_bounded(MAX_PENDING_EVENTS);

        let client = Self {
            requested_latency: requested_latency_ms.map(Duration::from_millis),
            receiver,
            last_send_time: Instant::now(),
            watch_response: None,
        };
        (client, sender)
    }

    pub fn accumulate_watch_event(&mut self, watch_event: AggregateWatchEvent) {
        watch_event.add_to_response(&mut self.watch_response);
    }

    pub async fn should_wait_and_flush(&self) -> bool {
        // No events? No need to wait, no need to flush
        if self.watch_response.is_none() {
            return false;
        }

        let latency_wait_time = self.additional_latency_wait_time();

        // No need to wait? Just flush
        if latency_wait_time == Duration::ZERO {
            return true;
        }

        false
    }

    pub fn take_response(&mut self) -> Option<WatchResponse> {
        self.last_send_time = Instant::now();
        if self.watch_response.is_none() {
            return None;
        }
        Some(self.watch_response.take().unwrap())
    }

    /// How much time is left before we are 'green' to send the client
    /// Need to check last_send_time and if there is a latency requirement
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

        if self.watch_response.is_none() {
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

                    assert_eq!(
                        client
                            .watch_response
                            .as_ref()
                            .unwrap()
                            .events
                            .as_ref()
                            .unwrap()
                            .len(),
                        1
                    );
                    assert!(client.last_send_time.elapsed().as_millis() <= 1);
                    assert!(!client.should_wait_and_flush().await);

                    client.accumulate_watch_event(AggregateWatchEvent {
                        aggregate_key: AggregateKey::new(1, 2, 4),
                        operation:
                            crate::aggregate_watch_event::AggregateWatchEventOperation::Read {
                                from_event_batch_index: 44,
                                to_event_batch_index: Some(46),
                            },
                    });

                    assert_eq!(
                        client
                            .watch_response
                            .as_ref()
                            .unwrap()
                            .events
                            .as_ref()
                            .unwrap()
                            .len(),
                        2
                    );
                    assert!(client.last_send_time.elapsed().as_millis() <= 1);
                    assert!(!client.should_wait_and_flush().await);

                    glommio::timer::sleep(client.additional_latency_wait_time()).await;

                    assert!(client.should_wait_and_flush().await);
                    assert!(
                        client.last_send_time.elapsed().as_millis() >= 10
                            && client.last_send_time.elapsed().as_millis() <= 13
                    );

                    let watch_response = client.take_response();
                    assert!(client.watch_response.as_ref().is_none());
                    assert!(client.last_send_time.elapsed().as_millis() <= 1);

                    assert_eq!(
                        watch_response
                            .as_ref()
                            .unwrap()
                            .events
                            .as_ref()
                            .unwrap()
                            .len(),
                        2
                    );

                    assert!(
                        watch_response
                            .as_ref()
                            .unwrap()
                            .events
                            .as_ref()
                            .unwrap()
                            .get(&AggregateKey::new(1, 2, 3))
                            .unwrap()
                            .get(&AggregateWatchEvent::DELETE)
                            .unwrap()
                            .is_none()
                    );
                    assert!(
                        watch_response
                            .as_ref()
                            .unwrap()
                            .events
                            .as_ref()
                            .unwrap()
                            .get(&AggregateKey::new(1, 2, 4))
                            .unwrap()
                            .get(&AggregateWatchEvent::READ)
                            .unwrap()
                            .is_some()
                    );
                })
                .unwrap();

        handle.join().unwrap();
    }
}
