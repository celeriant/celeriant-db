use celeriant_msg::response::{responses::WatchResponse, watch_event::WatchEvent};
use celeriant_wal::datablocks::event_batch_item::EventBatchItem;
use glommio::channels::local_channel::LocalSender;
use std::time::{Duration, Instant};

use crate::watch::aggregate_watch_event::AggregateWatchEvent;

pub struct SubscribedClient {
    pub requested_latency: Option<Duration>,
    pub requested_throughput: Option<usize>,
    pub last_send_time: std::time::Instant,
    pub receiver: glommio::channels::local_channel::LocalReceiver<AggregateWatchEvent>,
    pub events: Option<Vec<WatchEvent>>,
    pub accumulated_bytes: usize,
    pub max_response_size: Option<usize>,
}

impl SubscribedClient {
    pub fn new(
        requested_latency_ms: Option<u64>,
        requested_throughput_bs: Option<usize>,
        max_response_size: Option<usize>,
    ) -> (Self, LocalSender<AggregateWatchEvent>) {
        let (sender, receiver) = glommio::channels::local_channel::new_unbounded();
        let client = Self {
            requested_latency: requested_latency_ms.map(Duration::from_millis),
            requested_throughput: requested_throughput_bs,
            receiver,
            last_send_time: Instant::now(),
            events: None,
            accumulated_bytes: 0,
            max_response_size,
        };
        (client, sender)
    }

    pub fn accumulate_watch_event(
        &mut self,
        data_size_bytes: usize,
        event_batches: Option<Vec<EventBatchItem>>,
        watch_event: AggregateWatchEvent,
    ) {
        let mut watch_response = WatchEvent {
            event_type: watch_event.to_u8(),
            event_batches,
            ..Default::default()
        };

        match watch_event {
            AggregateWatchEvent::Delete { correlation_id } => {
                watch_response.correlation_id = correlation_id;
            }
            AggregateWatchEvent::Write {
                from_event_batch_index,
                to_event_batch_index,
            } => {
                watch_response.from_event_batch_index = Some(from_event_batch_index);
                watch_response.to_event_batch_index = Some(to_event_batch_index);
            }
            AggregateWatchEvent::Read {
                correlation_id,
                from_event_batch_index,
                to_event_batch_index,
                is_cached_read,
            } => {
                watch_response.correlation_id = correlation_id;
                watch_response.from_event_batch_index = Some(from_event_batch_index);
                watch_response.to_event_batch_index = to_event_batch_index;
                watch_response.from_cache = is_cached_read;
            }
            AggregateWatchEvent::TrimStart {
                correlation_id,
                keep_from_event_batch_index,
            } => {
                watch_response.correlation_id = correlation_id;
                watch_response.trim_start_keep_from_event_batch_index =
                    Some(keep_from_event_batch_index);
            }
            AggregateWatchEvent::Exists { correlation_id } => {
                watch_response.correlation_id = correlation_id;
            }
            AggregateWatchEvent::PrependBatches {
                correlation_id,
                from_event_batch_index,
                to_event_batch_index,
            } => {
                watch_response.correlation_id = correlation_id;
                watch_response.from_event_batch_index = Some(from_event_batch_index);
                watch_response.to_event_batch_index = Some(to_event_batch_index);
            }
        };

        // Add event to batch
        if self.events.is_none() {
            self.events = Some(Vec::new());
        }
        self.events.as_mut().unwrap().push(watch_response);
        self.accumulated_bytes += data_size_bytes;
    }

    pub async fn should_wait_and_flush(&self, extra_bytes: usize) -> bool {
        // No events? No need to wait, no need to flush
        if self.events.is_none() || self.events.as_ref().unwrap().is_empty() {
            return false;
        }

        // Is the next event going to be bigger than our allowed wire size limit? Wait and force flush
        if self.max_response_size.is_some()
            && self.accumulated_bytes + extra_bytes >= self.max_response_size.unwrap()
        {
            self.wait_for_latency_requirement().await;
            self.wait_for_throughput_requirement(0).await;
            return true;
        }

        let latency_wait_time = self.additional_latency_wait_time();
        let throughput_wait_time = self.additional_throughput_wait_time(0);
        let potential_throughput_wait_time = self.additional_throughput_wait_time(extra_bytes);

        // No need to wait? Just flush
        if latency_wait_time == Duration::ZERO && throughput_wait_time == Duration::ZERO {
            return true;
        }

        // Have we already accumulated enough data to exceed the next latency bracket?
        // If this is the case, we should wait out the thoughput requirement and then flush
        if throughput_wait_time > latency_wait_time {
            self.wait_for_throughput_requirement(0).await;
            return true;
        }

        // Would adding the next event cause a delay in latency? If so, wait out the latency and flush
        if potential_throughput_wait_time > latency_wait_time {
            self.wait_for_latency_requirement().await;
            return true;
        }

        false
    }

    pub fn take_response(&mut self) -> WatchResponse {
        self.last_send_time = Instant::now();
        self.accumulated_bytes = 0;
        WatchResponse {
            events: self.events.take(),
            is_heartbeat: false,
        }
    }

    /// If the client has a latency requirement we don't
    /// want to send them a push message until the next
    /// available time window
    async fn wait_for_latency_requirement(&self) {
        let wait_time = self.additional_latency_wait_time();
        if !wait_time.is_zero() {
            glommio::timer::sleep(wait_time).await;
        }
    }

    /// Some clients can't accept too much data at once
    /// so we assuming we already have pooled accumulated_bytes
    /// in data, wait until it's safe for the client to accept it,
    /// based on the last_send_time for the client
    async fn wait_for_throughput_requirement(&self, extra_bytes: usize) {
        let wait_time = self.additional_throughput_wait_time(extra_bytes);
        if !wait_time.is_zero() {
            glommio::timer::sleep(wait_time).await;
        }
    }

    /// We have accumulated_bytes already, and potentially extra_bytes
    /// too. How long would we have to wait for the client to accept it
    /// based on the last_send_time for the client
    fn additional_throughput_wait_time(&self, extra_bytes: usize) -> Duration {
        let Some(throughput_bytes_per_sec) = self.requested_throughput else {
            return Duration::ZERO;
        };

        let total_bytes = self.accumulated_bytes + extra_bytes;
        if total_bytes == 0 || throughput_bytes_per_sec == 0 {
            return Duration::ZERO;
        }

        // Calculate required time to send total_bytes at throughput bytes/sec
        let required_duration =
            Duration::from_secs_f64(total_bytes as f64 / throughput_bytes_per_sec as f64);
        let elapsed = self.last_send_time.elapsed();

        required_duration.saturating_sub(elapsed)
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

        if self.events.is_none() || self.events.as_ref().unwrap().len() == 0 {
            return None;
        }

        let elapsed = self.last_send_time.elapsed();
        Some(latency.saturating_sub(elapsed))
    }
}

#[cfg(test)]
mod test_subscribed_client {
    use std::time::{Duration, Instant};

    use celeriant_msg::response::watch_event::WatchEvent;
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::watch::{aggregate_watch_event::AggregateWatchEvent, subscribed_client::SubscribedClient};

    #[test]
    fn test_individual_requirements() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _) = SubscribedClient::new(None, None, None);

                // No requirements
                assert_eq!(client.additional_latency_wait_time(), Duration::ZERO);
                assert_eq!(client.additional_throughput_wait_time(999), Duration::ZERO);

                // Latency in isolation
                client.requested_latency = Some(Duration::from_millis(10));

                assert_ne!(client.additional_latency_wait_time(), Duration::ZERO);
                assert!(client.additional_latency_wait_time().as_millis() > 5);

                glommio::timer::sleep(client.additional_latency_wait_time()).await;

                assert_eq!(client.additional_latency_wait_time(), Duration::ZERO);

                client.last_send_time = Instant::now();
                assert!(client.additional_latency_wait_time().as_millis() > 5);

                // Throughput in isolation
                client.requested_latency = None;
                client.requested_throughput = Some(100);
                client.last_send_time = Instant::now();
                client.accumulated_bytes = 0;
                assert!(client.additional_throughput_wait_time(110).as_secs_f64() > 1.0);

                client.accumulated_bytes = 110;
                client.last_send_time = Instant::now();
                let w = client.additional_throughput_wait_time(0).as_secs_f64();
                assert!(w > 1.0);

                client.accumulated_bytes = 110;
                client.last_send_time = Instant::now();
                assert!(client.additional_throughput_wait_time(100).as_secs_f64() > 2.0);

                client.accumulated_bytes = 100;
                client.last_send_time = Instant::now();
                glommio::timer::sleep(client.additional_throughput_wait_time(100)).await;
                assert!(client.additional_throughput_wait_time(100).as_secs_f64() == 0.0);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_delay_latency_due_to_throughput() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _) = SubscribedClient::new(None, None, None);

                // Throughput and latency together
                // We MUST respect both - cannot exceed throughput to maintain latency, cannot shorten latency to maintain throughput

                // In our tests, latency is 10ms, throughput is 1000 bytes per second
                client.requested_latency = Some(Duration::from_millis(10));
                client.requested_throughput = Some(1000);

                // Scenario 1 - extend latency due to exceeding throughput
                client.events = Some(vec![WatchEvent {
                    ..Default::default()
                }]);
                client.accumulated_bytes = 12;
                client.last_send_time = Instant::now();

                // The 12 bytes already exceeds latency requirements due to throughput, so it will wait and ask for flush first
                let should_flush = client.should_wait_and_flush(0).await;
                let elapsed_ms = client.last_send_time.elapsed().as_millis();
                assert!(should_flush);
                assert!(elapsed_ms >= 12 && elapsed_ms <= 13);

                // Any additional bytes shouldn't affect the fact we need to flush now
                client.last_send_time = Instant::now();

                // The 12 bytes already exceeds latency requirements due to throughput, so it will wait and ask for flush first
                let should_flush = client.should_wait_and_flush(20).await;
                let elapsed_ms = client.last_send_time.elapsed().as_millis();
                assert!(should_flush);
                assert!(elapsed_ms >= 12 && elapsed_ms <= 13);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_next_event_would_push_out_latency() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _) = SubscribedClient::new(None, None, None);

                client.requested_latency = Some(Duration::from_millis(10));
                client.requested_throughput = Some(1000);

                client.events = Some(vec![WatchEvent {
                    ..Default::default()
                }]);
                client.accumulated_bytes = 8;
                client.last_send_time = Instant::now();

                // The additional 3 bytes would result in a latency delay, so flush now
                let should_flush = client.should_wait_and_flush(3).await;
                let elapsed_ms = client.last_send_time.elapsed().as_millis();
                assert!(should_flush);
                assert!(elapsed_ms >= 10 && elapsed_ms <= 11);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_can_still_add_events() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _) = SubscribedClient::new(None, None, None);

                client.requested_latency = Some(Duration::from_millis(10));
                client.requested_throughput = Some(1000);

                client.events = Some(vec![WatchEvent {
                    ..Default::default()
                }]);
                client.accumulated_bytes = 3;
                client.last_send_time = Instant::now();

                // Only 3 bytes in the pending payload,
                let should_flush = client.should_wait_and_flush(0).await;
                let elapsed_ms = client.last_send_time.elapsed().as_millis();
                assert!(!should_flush);
                assert!(elapsed_ms < 1);

                // Simulate trying to add an additional event but still within throughput
                let should_flush = client.should_wait_and_flush(7).await;
                let elapsed_ms = client.last_send_time.elapsed().as_millis();
                assert!(!should_flush);
                assert!(elapsed_ms < 1);

                // Simulate trying to add an additional event but exceed throughput
                let should_flush = client.should_wait_and_flush(8).await;
                let elapsed_ms = client.last_send_time.elapsed().as_millis();
                assert!(should_flush);
                assert!(elapsed_ms >= 10 && elapsed_ms <= 11);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_flow() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (mut client, _) = SubscribedClient::new(None, None, None);

                client.requested_latency = Some(Duration::from_millis(10));
                client.requested_throughput = Some(1000);

                client.accumulate_watch_event(
                    3,
                    None,
                    crate::watch::aggregate_watch_event::AggregateWatchEvent::Delete {
                        correlation_id: None,
                    },
                );

                assert_eq!(client.accumulated_bytes, 3);
                assert_eq!(client.events.as_ref().unwrap().len(), 1);
                assert!(client.last_send_time.elapsed().as_millis() <= 1);
                assert!(!client.should_wait_and_flush(0).await);


                client.accumulate_watch_event(
                    3,
                    None,
                    crate::watch::aggregate_watch_event::AggregateWatchEvent::Read { 
                        correlation_id: Some(88), 
                        from_event_batch_index: 44, 
                        to_event_batch_index: Some(46), 
                        is_cached_read: true 
                    },
                );

                assert_eq!(client.accumulated_bytes, 6);
                assert_eq!(client.events.as_ref().unwrap().len(), 2);
                assert!(client.last_send_time.elapsed().as_millis() <= 1);
                assert!(!client.should_wait_and_flush(0).await);

                glommio::timer::sleep(client.additional_latency_wait_time()).await;

                assert!(client.should_wait_and_flush(0).await);
                assert!(client.last_send_time.elapsed().as_millis() >= 10 && client.last_send_time.elapsed().as_millis() <= 12);

                let watch_response = client.take_response();
                assert_eq!(client.accumulated_bytes, 0);
                assert!(client.events.as_ref().is_none());
                assert!(client.last_send_time.elapsed().as_millis() <= 1);

                assert!(!watch_response.is_heartbeat);
                assert_eq!(watch_response.events.as_ref().unwrap().len(), 2);

                assert!(watch_response.events.as_ref().unwrap()[0].correlation_id.is_none());
                assert_eq!(watch_response.events.as_ref().unwrap()[0].event_type, AggregateWatchEvent::DELETE);

                assert_eq!(watch_response.events.as_ref().unwrap()[1].correlation_id.unwrap(), 88);
                assert_eq!(watch_response.events.as_ref().unwrap()[1].event_type, AggregateWatchEvent::READ);
                assert_eq!(watch_response.events.as_ref().unwrap()[1].from_event_batch_index, Some(44));
                assert_eq!(watch_response.events.as_ref().unwrap()[1].to_event_batch_index, Some(46));
                assert_eq!(watch_response.events.as_ref().unwrap()[1].from_cache, true);

            })
            .unwrap();

        handle.join().unwrap();
    }
}
