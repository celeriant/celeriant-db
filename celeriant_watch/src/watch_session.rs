use std::{cell::RefCell, rc::Rc, time::Duration};

use celeriant_wal::aggregate_key::AggregateKey;

use crate::{
    aggregate_reader::{AggregateReader, WatchReadError}, subscribed_client::SubscribedClient, watch_output_type::WatchOutputType
};

pub struct WatchSession<R: AggregateReader> {
    watcher_id: u64,
    subscribed_client: Rc<RefCell<SubscribedClient>>,
    reader: Rc<R>,
}

impl<R: AggregateReader> WatchSession<R> {
    pub fn new(
        watcher_id: u64,
        subscribed_client: Rc<RefCell<SubscribedClient>>,
        reader: Rc<R>,
    ) -> Self {

        Self {
            watcher_id,
            subscribed_client,
            reader,
        }
    }

    pub async fn next(&mut self) -> Result<WatchOutputType, WatchReadError> {
        let timeout_duration = {
            let client = self.subscribed_client.borrow();
            client.watch_wait_time().unwrap_or(Duration::from_secs(5))
        };

        match glommio::timer::timeout(timeout_duration, async {
            self.subscribed_client.borrow().receiver.recv().await
                .ok_or(glommio::GlommioError::Closed(glommio::ResourceType::Channel(())))
        }).await {
            Ok(aggregate_watch_event) => {
                let mut client = self.subscribed_client.borrow_mut();
                
                client.accumulate_watch_event(aggregate_watch_event);

                if client.should_wait_and_flush().await {
                    if let Some(response) = client.take_response() {
                        return Ok(WatchOutputType::Response(response));
                    }
                }

                Ok(WatchOutputType::Continue)
            }
            Err(glommio::GlommioError::Closed(_)) => Ok(WatchOutputType::Done),
            Err(_) => {
                // Timeout
                let mut client = self.subscribed_client.borrow_mut();
                if client.should_wait_and_flush().await {
                    if let Some(response) = client.take_response() {
                        return Ok(WatchOutputType::Response(response));
                    }
                    Ok(WatchOutputType::Continue)
                } else {
                    Ok(WatchOutputType::Heartbeat)
                }
            }
        }
    }

    pub fn cleanup(&self) {
        let _watchers = self.reader.watched_aggregates().remove_subscriber(self.watcher_id);
    }
}

pub fn estimate_data_size_bytes(event_batches: &Vec<celeriant_wal::datablocks::event_batch_item::EventBatchItem>) -> usize {
    event_batches.iter().map(|b| {
        b.events.iter().map(|e| e.event_value.len()).sum::<usize>() + 100 // overhead estimate per batch
    }).sum()
}

impl<R: AggregateReader> Drop for WatchSession<R> {
    fn drop(&mut self) {
        self.cleanup();
    }
}