use std::collections::VecDeque;

use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_wal::aggregate_key::AggregateKey;

use crate::celeriant_client::CeleriantClient;
use crate::client_error::ClientError;

/// Streaming iterator for reading all event batches from an aggregate.
///
/// Automatically follows `next_event_batch_index` cursors until the aggregate
/// is fully consumed.
pub struct ReadAllIterator<'a> {
    client: &'a mut CeleriantClient,
    aggregate_key: AggregateKey,
    filters: ReadFilters,
    buffer: VecDeque<AggregateEventBatch>,
    exhausted: bool,
}

impl<'a> ReadAllIterator<'a> {
    pub fn new(
        client: &'a mut CeleriantClient,
        aggregate_key: AggregateKey,
        filters: Option<ReadFilters>,
    ) -> Self {
        Self {
            client,
            aggregate_key,
            filters: filters.unwrap_or_else(|| ReadFilters::new(1)),
            buffer: VecDeque::new(),
            exhausted: false,
        }
    }

    /// Get the next event batch, or None if exhausted
    pub async fn next(&mut self) -> Option<Result<AggregateEventBatch, ClientError>> {
        loop {
            if let Some(batch) = self.buffer.pop_front() {
                return Some(Ok(batch));
            }

            if self.exhausted {
                return None;
            }

            match self.fetch_next_page().await {
                Ok(true) => continue,
                Ok(false) => {
                    self.exhausted = true;
                    continue;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn fetch_next_page(&mut self) -> Result<bool, ClientError> {
        let request = ReadRequest {
            correlation_id: None,
            aggregate_key: self.aggregate_key.clone(),
            filters: self.filters.clone(),
        };

        let response = self.client.read(request).await?;

        self.buffer.extend(response.event_batches);

        match response.next_event_batch_index {
            Some(next_index) => {
                self.filters.from_event_batch_index = next_index;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Collect all remaining event batches into a Vec
    pub async fn collect(mut self) -> Result<Vec<AggregateEventBatch>, ClientError> {
        let mut results = Vec::new();
        while let Some(batch) = self.next().await {
            results.push(batch?);
        }
        Ok(results)
    }
}
