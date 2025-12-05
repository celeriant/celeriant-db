use std::{num::NonZeroUsize, time::Duration};

use celeriant_msg::{request::requests::{ReadRequest, WriteRequest}, response::responses::{ReadResponse, WriteResponse}};

use crate::{cache::aggregate_cache::AggregateCache, node_config::NodeConfig, read_operations::{read_error::ReadError, read_operations::ReadOperations, read_structures::AggregateReadConfig}, read_write_error::ReadWriteError, write_operations::{aggregate_write_config::AggregateWriteConfig, write_operations::WriteOperations}};


pub struct LocalAggregate {
    aggregate_cache: AggregateCache,
    node_config: NodeConfig,
}

impl LocalAggregate {
    pub fn new(
        aggregate_read_config: AggregateReadConfig,
        aggregate_write_config: AggregateWriteConfig,
        node_config: NodeConfig,
    ) -> Self {
        let capacity = NonZeroUsize::new(node_config.max_open_aggregates).unwrap();
        Self {
            aggregate_cache: AggregateCache::new(
                capacity,
                node_config.clone(),
                aggregate_read_config,
                aggregate_write_config,
            ),
            node_config,
        }
    }

    pub async fn read(
        &self,
        request: &ReadRequest,
    ) -> Result<ReadResponse, ReadError> {
        let aggregate_resources = self.aggregate_cache.get_aggregate_resources(&request.aggregate_key);

        // Writer is the source of truth for current file lengths and available batches
        let (file_len_metadata, file_len_event_batch, minimum_available_event_batch_index) = {
            let writer = aggregate_resources.get_writer(false).await?;
            let r_writer = writer.as_ref().unwrap();

            // Check if we can serve the read request via the in-memory cache
            if let Ok(result) =
                r_writer.maybe_read_cached_events(&request.filters, self.node_config.max_event_batches_response_size)
            {
                return Ok(result);
            }

            (
                r_writer.file_len_metadata,
                r_writer.file_len_event_batch,
                r_writer.minimum_available_event_batch_index,
            )
        };

        let read_result = {
            let reader = aggregate_resources.get_reader(false).await?;
            let r_reader = reader.as_ref().unwrap();
            r_reader
                .read(
                    minimum_available_event_batch_index,
                    file_len_metadata,
                    file_len_event_batch,
                    &request.filters,
                    self.node_config.max_event_batches_response_size,
                )
                .await?
        };

        Ok(read_result)
    }

    // Helper: Get current server timestamp
    fn get_server_timestamp_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub async fn write(
        &self,
        lease_index: u64,
        mut request: WriteRequest,
    ) -> Result<WriteResponse, ReadWriteError> {
        let aggregate_resources = self.aggregate_cache.get_aggregate_resources(&request.aggregate_key);
        let server_timestamp_ms = Self::get_server_timestamp_millis();

        // Check if previous async sync failed - force durable write to surface error early
        let force_durable = aggregate_resources.has_pending_sync_error();

        let append_result = {
            let mut writer = aggregate_resources
                .get_writer_mut(request.allow_create)
                .await?;
            let r_writer = writer.as_mut().unwrap();

            r_writer.queue_events_in_memory(self.node_config.node_id, lease_index, server_timestamp_ms, &mut request)?
        };

        // Either wait on an amortised fsync or spawn a task to do it and return to client immediately
        if force_durable {
            // Force immediate durable write due to previous sync error
            aggregate_resources.sync_with_delay(None).await?;
            aggregate_resources.clear_pending_sync_error();
        } else if let Some(delay_us) = request.durable_write_with_delay_us {
            aggregate_resources
                .sync_with_delay(Some(Duration::from_micros(delay_us)))
                .await?;
        } else {
            let aggregate_resources = aggregate_resources.clone();
            let async_flush_ms = self.node_config.async_flush_ms;

            glommio::spawn_local(async move {
                let sync_result = aggregate_resources
                    .sync_with_delay(Some(Duration::from_millis(async_flush_ms)))
                    .await;
                if let Err(_e) = sync_result {
                    aggregate_resources.set_pending_sync_error();
                }
            }).detach();
        }

        Ok(append_result)
    }

}