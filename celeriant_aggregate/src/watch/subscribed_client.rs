use std::time::{Duration, Instant};
use celeriant_msg::{request::read_filters::ReadFilters, response::{responses::WatchResponse, watch_event::WatchEvent}};
use celeriant_wal::wal::event_batch_item::EventBatchItem;
use glommio::channels::local_channel::LocalSender;

use crate::{watch::aggregate_watch_event::AggregateWatchEvent};

pub struct SubscribedClient {
    pub requested_latency: Option<Duration>,
    pub requested_throughput: Option<usize>,
    pub last_send_time: std::time::Instant,
    pub message_version: u32,
    pub read_filters: Option<ReadFilters>,
    pub receiver: glommio::channels::local_channel::LocalReceiver<AggregateWatchEvent>,
    pub events: Option<Vec<WatchEvent>>,
    pub accumulated_bytes: usize,
    pub max_response_size: Option<usize>,
}

impl SubscribedClient {

    pub fn new(
        requested_latency_ms: Option<u64>,
        requested_throughput_bs: Option<usize>,
        read_filters: Option<ReadFilters>,
        max_response_size: Option<usize>,
        message_version: u32,
    ) -> (Self, LocalSender<AggregateWatchEvent>) {
        let (sender, receiver) = glommio::channels::local_channel::new_unbounded();
        let client = Self {
            requested_latency: requested_latency_ms.map(Duration::from_millis),
            requested_throughput: requested_throughput_bs,
            read_filters,
            receiver,
            last_send_time: Instant::now(),
            events: None,
            accumulated_bytes: 0,
            max_response_size,
            message_version,
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
            },
            AggregateWatchEvent::Write { from_event_batch_index, to_event_batch_index } => {
                watch_response.from_event_batch_index = Some(from_event_batch_index);
                watch_response.to_event_batch_index = Some(to_event_batch_index);
            },
            AggregateWatchEvent::Read { correlation_id, from_event_batch_index, to_event_batch_index, is_cached_read } => {
                watch_response.correlation_id = correlation_id;
                watch_response.from_event_batch_index = Some(from_event_batch_index);
                watch_response.to_event_batch_index = to_event_batch_index;
                watch_response.from_cache = is_cached_read;
            },
            AggregateWatchEvent::TrimStart { correlation_id, keep_from_event_batch_index } => {
                watch_response.correlation_id = correlation_id;
                watch_response.trim_start_keep_from_event_batch_index = Some(keep_from_event_batch_index);
            },
            AggregateWatchEvent::Exists { correlation_id } => {
                watch_response.correlation_id = correlation_id;
            },
            AggregateWatchEvent::PrependBatches { correlation_id, from_event_batch_index, to_event_batch_index } => {
                watch_response.correlation_id = correlation_id;
                watch_response.from_event_batch_index = Some(from_event_batch_index);
                watch_response.to_event_batch_index = Some(to_event_batch_index);
            },
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
        if self.max_response_size.is_some() && self.accumulated_bytes + extra_bytes >= self.max_response_size.unwrap() {
            self.wait_for_latency_requirement().await;
            self.wait_for_throughput_requirement().await;
            return true;
        }

        // Is there a latency AND throughput requirement? If adding extra_bytes would
        // result in a latency delay, we should wait and flush first before adding that event
        if self.requested_latency.is_some() && self.requested_throughput.is_some() {
            if self.additional_throughput_wait_time(extra_bytes) > self.additional_latency_wait_time() {
                self.wait_for_latency_requirement().await;
                return true;
            }
        }

        // Is there only a throughput requirement? We can flush immediately after we satisfy throughput
        if self.requested_latency.is_none() && self.requested_throughput.is_some() {
            self.wait_for_throughput_requirement().await;
            return true;
        }

        // Not ready to sync yet, but no need to intentionally delay in-line
        if self.requested_latency.is_some() && self.additional_latency_wait_time() > Duration::ZERO {
            return false;
        }

        // We have events and no need to wait on any requirements
        true
    }

    pub fn time_until_flush(&self) -> Option<Duration> {
        if self.events.is_none() || self.events.as_ref().unwrap().is_empty() {
            return None;
        }

        let wait = self.additional_latency_wait_time();
        if wait.is_zero() {
            return None
        }

        Some(wait)
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
    pub async fn wait_for_latency_requirement(&self) {
        let wait_time = self.additional_latency_wait_time();
        if !wait_time.is_zero() {
            glommio::timer::sleep(wait_time).await;
        }
    }
    
    /// Some clients can't accept too much data at once
    /// so we assuming we already have pooled accumulated_bytes
    /// in data, wait until it's safe for the client to accept it,
    /// based on the last_send_time for the client 
    pub async fn wait_for_throughput_requirement(&self) {
        let wait_time = self.additional_throughput_wait_time(0);
        if !wait_time.is_zero() {
            glommio::timer::sleep(wait_time).await;
        }
    }
    
    /// We have accumulated_bytes already, and potentially extra_bytes
    /// too. How long would we have to wait for the client to accept it
    /// based on the last_send_time for the client 
    pub fn additional_throughput_wait_time(&self, extra_bytes: usize) -> Duration {
        let Some(throughput_bytes_per_sec) = self.requested_throughput else {
            return Duration::ZERO;
        };

        let total_bytes = self.accumulated_bytes + extra_bytes;
        if total_bytes == 0 || throughput_bytes_per_sec == 0 {
            return Duration::ZERO;
        }

        // Calculate required time to send total_bytes at throughput bytes/sec
        let required_duration = Duration::from_secs_f64(total_bytes as f64 / throughput_bytes_per_sec as f64);
        let elapsed = self.last_send_time.elapsed();

        required_duration.saturating_sub(elapsed)
    }

    /// How much time is left before we are 'green' to send the client
    /// Need to check last_send_time and if there is a latency requirement
    pub fn additional_latency_wait_time(&self) -> Duration {
        let Some(latency) = self.requested_latency else {
            return Duration::ZERO;
        };

        let elapsed = self.last_send_time.elapsed();
        latency.saturating_sub(elapsed)
    }

}


#[cfg(test)]
mod test_subscribed_client {
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::watch::subscribed_client::SubscribedClient;


    #[test]
    fn test_additional_latency_wait_time() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {

            let (client, sender) = SubscribedClient::new(
                None,
                None,
                None,
                None,
                1,
            );

        })
        .unwrap();

    handle.join().unwrap();
    }
}