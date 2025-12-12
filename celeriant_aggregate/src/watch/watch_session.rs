use std::{cell::RefCell, rc::Rc, time::Duration};

use celeriant_msg::{
    request::{read_filters::ReadFilters, requests::{ReadRequest}},
    response::responses::WatchResponse,
};
use celeriant_wal::{aggregate_key::AggregateKey, wal::event_batch_item::EventBatchItem};

use crate::{
    local_aggregate::{LocalAggregate, LocalAggregateTrait}, read_operations::read_error::ReadError, read_write_error::ReadWriteError, watch::{aggregate_watch_event::AggregateWatchEvent, subscribed_client::SubscribedClient}
};

pub enum WatchOutput {
    Response(WatchResponse),
    Heartbeat,
    Done,
    Continue,
}

pub struct WatchSession {
    aggregate_key: AggregateKey,
    correlation_id: Option<u128>,
    watcher_id: u64,
    subscribed_client: Rc<RefCell<SubscribedClient>>,
    local_aggregate: Rc<LocalAggregate>,
    read_filters: Option<ReadFilters>,
    initial_catchup_complete: bool,
}

impl WatchSession {
    pub fn new(
        aggregate_key: AggregateKey,
        correlation_id: Option<u128>,
        watcher_id: u64,
        subscribed_client: Rc<RefCell<SubscribedClient>>,
        local_aggregate: Rc<LocalAggregate>,
        mut read_filters: Option<ReadFilters>,
        watching_writes: bool,
    ) -> Self {

        // Ensure if we have read filters, there is no upper limit as we are watching
        if let Some(ref mut filters) = read_filters {
            filters.to_event_batch_index = None;
            filters.max_client_event_index = None;
            filters.max_event_index = None;
            filters.max_event_timestamp = None;
            filters.max_server_timestamp = None;
        }

        Self {
            aggregate_key,
            correlation_id,
            watcher_id,
            subscribed_client,
            local_aggregate,
            read_filters,
            initial_catchup_complete: !watching_writes,
        }
    }

    pub async fn next(&mut self) -> Result<WatchOutput, ReadWriteError> {
        // Handle initial catchup for write watchers
        if !self.initial_catchup_complete {
            if let Some(output) = self.process_initial_catchup().await? {
                return Ok(output);
            }
            self.initial_catchup_complete = true;
        }

        // Main watch loop iteration
        self.process_watch_event().await
    }

    async fn process_initial_catchup(&mut self) -> Result<Option<WatchOutput>, ReadWriteError> {

        if self.read_filters.as_ref().is_none() {
            return Ok(None);
        }

        let read_request = ReadRequest {
            correlation_id: self.correlation_id,
            aggregate_key: self.aggregate_key.clone(),
            filters: self.read_filters.as_ref().unwrap().clone(),
        };

        //TODO: Some more detail here - should stream results back to client
        //Currently just waits due to throughput limitation and then dumps it all over the wire at once
        let read_result = self.local_aggregate.read(&read_request).await?;
        if read_result.event_batches.is_empty() {
            return Ok(None); // Catchup complete
        }

        let mut client = self.subscribed_client.borrow_mut();

        let from_event_batch_index = read_request.filters.from_event_batch_index;
        let to_event_batch_index = read_result.event_batches.last().unwrap().event_batch_index;

        client.accumulate_watch_event(
            estimate_data_size_bytes(&read_result.event_batches),
            Some(read_result.event_batches),
            AggregateWatchEvent::Write { from_event_batch_index, to_event_batch_index },
        );

        client.should_wait_and_flush(0).await;

        // Update read_filters for next catchup page
        if let Some(filters) = self.read_filters.as_mut() {
            filters.from_event_batch_index = to_event_batch_index + 1;
        }

        let response = client.take_response();
        Ok(Some(WatchOutput::Response(response)))
    }

    async fn get_batches_for_write_event(&mut self, aggregate_watch_event: &AggregateWatchEvent) -> Result<(usize, Option<Vec<EventBatchItem>>), ReadError> {
        let (data_size_bytes, event_batches) = if let AggregateWatchEvent::Write { 
                    from_event_batch_index, to_event_batch_index 
                } = aggregate_watch_event.clone() {
                    let mut read_request = ReadRequest {
                        correlation_id: self.correlation_id,
                        aggregate_key: self.aggregate_key.clone(),
                        filters: self.read_filters.as_ref().unwrap().clone(),
                    };
                    read_request.filters.from_event_batch_index = from_event_batch_index;
                    read_request.filters.to_event_batch_index = Some(to_event_batch_index);
                    let read_result = self.local_aggregate.read(&read_request).await?;

                    //Ensure the next write event pulls batches from the right offset for the client
                    if let Some(read_filters) = self.read_filters.as_mut() {
                        read_filters.from_event_batch_index = to_event_batch_index + 1;
                    }
                    
                    (estimate_data_size_bytes(&read_result.event_batches), Some(read_result.event_batches))
                } else {
                    (0, None)
                };
        Ok((data_size_bytes, event_batches))
    }

    async fn process_watch_event(&mut self) -> Result<WatchOutput, ReadWriteError> {
        let timeout_duration = {
            let client = self.subscribed_client.borrow();
            client.watch_wait_time().unwrap_or(Duration::from_secs(5))
        };

        match glommio::timer::timeout(timeout_duration, async {
            self.subscribed_client.borrow().receiver.recv().await
                .ok_or(glommio::GlommioError::Closed(glommio::ResourceType::Channel(())))
        }).await {
            Ok(aggregate_watch_event) => {
                let (data_size_bytes, event_batches) = self.get_batches_for_write_event(&aggregate_watch_event).await?;

                let mut client = self.subscribed_client.borrow_mut();
                
                if client.should_wait_and_flush(data_size_bytes).await {
                    let early_response = client.take_response();
                    client.accumulate_watch_event(data_size_bytes, event_batches, aggregate_watch_event);
                    return Ok(WatchOutput::Response(early_response));
                }

                client.accumulate_watch_event(data_size_bytes, event_batches, aggregate_watch_event);

                if client.should_wait_and_flush(0).await {
                    return Ok(WatchOutput::Response(client.take_response()));
                }

                Ok(WatchOutput::Continue)
            }
            Err(glommio::GlommioError::Closed(_)) => Ok(WatchOutput::Done),
            Err(_) => {
                // Timeout
                let mut client = self.subscribed_client.borrow_mut();
                if client.should_wait_and_flush(0).await {
                    let response = client.take_response();
                    Ok(WatchOutput::Response(response))
                } else {
                    Ok(WatchOutput::Heartbeat)
                }
            }
        }
    }

    pub fn cleanup(&self) {
        let watchers = self.local_aggregate.watched_aggregates.get_or_create(&self.aggregate_key);
        watchers.remove_subscriber(self.watcher_id);
    }
}

pub fn estimate_data_size_bytes(event_batches: &Vec<celeriant_wal::wal::event_batch_item::EventBatchItem>) -> usize {
    event_batches.iter().map(|b| {
        b.events.iter().map(|e| e.event_value.len()).sum::<usize>() + 100 // overhead estimate per batch
    }).sum()
}