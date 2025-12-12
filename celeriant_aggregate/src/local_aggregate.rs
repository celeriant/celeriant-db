use std::{num::NonZeroUsize, rc::Rc, time::Duration};

use celeriant_disk::files::open_dma_files::existing_file_read_only_dma;
use celeriant_msg::{
    process_requests::Request, request::{
        directory_filters::DirectoryFilters, requests::{
            DeleteRequest, ListAggregatesRequest, ListOrganisationsRequest, PrependBatchesRequest, ReadRequest, TrimStartRequest, UpdateCacheLimitsRequest, WriteRequest
        }
    }, response::{
        aggregate_info::AggregateInfo,
        organisation_info::OrganisationInfo,
        responses::{
            ListAggregatesResponse, ListOrganisationsResponse, ReadResponse, SuccessResponse, WriteResponse
        },
    }
};
use celeriant_wal::aggregate_key::AggregateKey;

use crate::{
    cache::aggregate_cache::AggregateCache, node_config::NodeConfig, read_operations::{
        read_error::ReadError, read_operations::ReadOperations,
        read_structures::AggregateReadConfig,
    }, read_write_error::ReadWriteError, watch::{aggregate_watch_event::AggregateWatchEvent, watched_aggregates::WatchedAggregates}, write_operations::{
        aggregate_write_config::AggregateWriteConfig, write_error::WriteError,
        write_operations::WriteOperations,
    }
};

pub struct LocalAggregate {
    pub aggregate_cache: AggregateCache,
    pub watched_aggregates: Rc<WatchedAggregates>,
    node_config: NodeConfig,
}

#[allow(async_fn_in_trait)]
pub trait LocalAggregateTrait {
    async fn close(&self);

    async fn process_request(
        &self,
        lease_index: Option<u64>,
        request: Request,
    ) -> Result<celeriant_msg::process_responses::Response, ReadWriteError>;

    async fn trim_start(&self, request: &TrimStartRequest) -> Result<(), ReadWriteError>;

    async fn delete(&self, request: &DeleteRequest) -> Result<(), ReadWriteError>;

    async fn prepend_batches(
        &self,
        request: &PrependBatchesRequest,
    ) -> Result<(), ReadWriteError>;

    fn list_organisations(
        &self,
        request: ListOrganisationsRequest,
    ) -> Result<ListOrganisationsResponse, ReadError>;

    fn list_aggregates(
        &self,
        request: ListAggregatesRequest,
    ) -> Result<ListAggregatesResponse, ReadError>;

    async fn update_cache_limits(
        &self,
        request: &UpdateCacheLimitsRequest,
    ) -> Result<(), ReadError>;

    async fn read(&self, request: &ReadRequest) -> Result<ReadResponse, ReadError>;

    async fn write(
        &self,
        lease_index: u64,
        request: WriteRequest,
    ) -> Result<WriteResponse, ReadWriteError>;
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
            watched_aggregates: Rc::new(WatchedAggregates::new()),
        }
    }
}

impl LocalAggregateTrait for LocalAggregate {

    async fn close(&self) {
        self.aggregate_cache.close().await;
    }

    async fn process_request(
        &self,
        lease_index: Option<u64>,
        request: Request,
    ) -> Result<celeriant_msg::process_responses::Response, ReadWriteError> {
        match request {
            Request::ListOrganisations(req) => {
                return Ok(celeriant_msg::process_responses::Response::ListOrganisations(self.list_organisations(req)?));
            }
            Request::ListAggregates(req) => {
                return Ok(celeriant_msg::process_responses::Response::ListAggregates(self.list_aggregates(req)?));
            }
            Request::Exists(req) => {
                let aggregate_resources = self
                    .aggregate_cache
                    .get_aggregate_resources(&req.aggregate_key);
                // Attempt to get reader without create - will error if aggregate doesn't exist
                let writer = aggregate_resources.get_writer(false).await?;
                return Ok(celeriant_msg::process_responses::Response::Exists(celeriant_msg::response::responses::ExistsResponse { 
                    correlation_id: req.correlation_id, 
                    min_event_batch_index: writer.minimum_available_event_batch_index,
                }));
            }
            Request::Read(req) => {
                return Ok(celeriant_msg::process_responses::Response::Read(self.read(&req).await?));
            }
            Request::Write(req) => {
                let lease_index = lease_index.ok_or(WriteError::InvalidLeaseIndex)?;
                return Ok(celeriant_msg::process_responses::Response::Write(self.write(lease_index, req).await?));
            }
            Request::PrependBatches(req) => {
                self.prepend_batches(&req).await?;
                return Ok(celeriant_msg::process_responses::Response::PrependBatches(SuccessResponse {
                    correlation_id: req.correlation_id
                }));
            }
            Request::TrimStart(req) => {
                self.trim_start(&req).await?;
                return Ok(celeriant_msg::process_responses::Response::TrimStart(SuccessResponse {
                    correlation_id: req.correlation_id
                }));
            }
            Request::Delete(req) => {
                self.delete(&req).await?;
                return Ok(celeriant_msg::process_responses::Response::TrimStart(SuccessResponse {
                    correlation_id: req.correlation_id
                }));
            }
            Request::UpdateCacheLimits(req) => {
                self.update_cache_limits(&req).await?;
                return Ok(celeriant_msg::process_responses::Response::TrimStart(SuccessResponse {
                    correlation_id: req.correlation_id
                }));
            }
            Request::Watch(_) => {
                unreachable!()
            }
        }
    }

    async fn trim_start(&self, request: &TrimStartRequest) -> Result<(), ReadWriteError> {
        let aggregate_resources = self
            .aggregate_cache
            .get_aggregate_resources(&request.aggregate_key);

        let mut writer = aggregate_resources.get_writer_mut(false).await?;
        let mut reader = aggregate_resources.get_reader_mut(false).await?;

        //TODO: Minor invariant problem where next_event_batch_index represents the in-memory queue, not fsynced yet
        if request.keep_from_event_batch_index >= writer.next_event_batch_index {
            return Err(ReadError::UnavailableBatchIndex { 
                minimum_available_event_batch_index: writer.minimum_available_event_batch_index, 
                requested_event_batch_index: request.keep_from_event_batch_index })?;
        }

        let (source_metadata_dma_file, source_event_batches_dma_file) = reader
            .metadata_dma_file
            .as_ref()
            .zip(reader.event_batches_dma_file.as_ref())
            .ok_or_else(|| {
                ReadWriteError::Read(ReadError::IoError(
                    "Prepend failure due to missing dma files in reader".to_string(),
                ))
            })?;

        let file_positions = reader
            .get_file_positions(
                writer.minimum_available_event_batch_index,
                request.keep_from_event_batch_index,
                writer.file_len_metadata,
                writer.file_len_event_batch,
            )
            .await?;

        writer
            .trim_start(
                request.keep_from_event_batch_index,
                &source_metadata_dma_file,
                &source_event_batches_dma_file,
                file_positions.metadata_position,
                file_positions.event_batch_position,
            )
            .await?;

        let data_requirements = reader
            .replace_dma_files(
                writer.metadata_dma_file.as_ref().unwrap().dup().map_err(ReadError::from)?,
                writer.event_batches_dma_file.as_ref().unwrap().dup().map_err(ReadError::from)?,
            )
            .await?;

        writer.update_write_operations_data_requirements(data_requirements);

        Ok(())
    }

    async fn delete(&self, request: &DeleteRequest) -> Result<(), ReadWriteError> {
        self.aggregate_cache.pop(&request.aggregate_key).await?;

        // Delete files
        let data_root_folder =
            std::path::Path::new(&self.node_config.data_root_folder).to_path_buf();
        
        let aggregate_folder = data_root_folder.join(format!(
            "{}/{}/{}",
            request.aggregate_key.org_id,
            request.aggregate_key.aggregate_type_id,
            request.aggregate_key.aggregate_id
        ));
        
        let metadata_path = aggregate_folder.join("metadata.bin");
        let events_path = aggregate_folder.join("event_batches.bin");

        std::fs::remove_file(&metadata_path).map_err(WriteError::from)?;
        std::fs::remove_file(&events_path).map_err(WriteError::from)?;
        
        // Remove the now-empty aggregate folder
        std::fs::remove_dir(&aggregate_folder).map_err(WriteError::from)?;

        Ok(())
    }

    async fn prepend_batches(
        &self,
        request: &PrependBatchesRequest,
    ) -> Result<(), ReadWriteError> {
        let aggregate_resources = self
            .aggregate_cache
            .get_aggregate_resources(&request.aggregate_key);

        let mut writer = aggregate_resources
            .get_writer_mut(request.allow_create)
            .await?;
        let mut reader = aggregate_resources
            .get_reader_mut(request.allow_create)
            .await?;

        let (source_metadata_dma_file, source_event_batches_dma_file) = reader
            .metadata_dma_file
            .as_ref()
            .zip(reader.event_batches_dma_file.as_ref())
            .ok_or_else(|| {
                ReadWriteError::Read(ReadError::IoError(
                    "Prepend failure due to missing dma files in reader".to_string(),
                ))
            })?;

        writer
            .prepend_batches(
                request.compression_type,
                &request.batches,
                &source_metadata_dma_file,
                &source_event_batches_dma_file,
            )
            .await?;

        let data_requirements = reader
            .replace_dma_files(
                existing_file_read_only_dma(&aggregate_resources.path_metadata)
                    .await
                    .map_err(ReadError::from)?,
                existing_file_read_only_dma(&aggregate_resources.path_event_batches)
                    .await
                    .map_err(ReadError::from)?,
            )
            .await?;

        writer.update_write_operations_data_requirements(data_requirements);

        Ok(())
    }

    fn list_organisations(
        &self,
        request: ListOrganisationsRequest,
    ) -> Result<ListOrganisationsResponse, ReadError> {
        let mut organisations = Vec::new();

        let data_root_folder =
            std::path::Path::new(&self.node_config.data_root_folder).to_path_buf();

        let entries = std::fs::read_dir(&data_root_folder)?;

        for entry in entries {
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }

            // Parse directory name as org_id
            let org_id = match path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| s.parse::<u128>().ok())
            {
                Some(id) => id,
                None => continue,
            };

            // Get directory metadata
            let metadata = match std::fs::metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let created_at = metadata
                .created()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            let modified_at = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            // Calculate disk usage by walking the directory tree
            let disk_usage = calculate_disk_usage(&path)?;

            // Apply filters
            if let Some(after) = request.filters.created_after_or_on {
                if created_at < after {
                    continue;
                }
            }
            if let Some(before) = request.filters.created_before_or_on {
                if created_at > before {
                    continue;
                }
            }
            if let Some(after) = request.filters.modified_after_or_on {
                if modified_at < after {
                    continue;
                }
            }
            if let Some(before) = request.filters.modified_before_or_on {
                if modified_at > before {
                    continue;
                }
            }
            if let Some(max) = request.filters.disk_usage_less_than_or_equal {
                if disk_usage > max {
                    continue;
                }
            }
            if let Some(min) = request.filters.disk_usage_greater_than_or_equal {
                if disk_usage < min {
                    continue;
                }
            }

            organisations.push(OrganisationInfo {
                org_id,
                created_at,
                modified_at,
                disk_usage,
            });
        }

        Ok(ListOrganisationsResponse {
            correlation_id: request.correlation_id,
            organisations,
        })
    }

    fn list_aggregates(
        &self,
        request: ListAggregatesRequest,
    ) -> Result<ListAggregatesResponse, ReadError> {
        let org_id = request.org_id;
        let aggregate_type_id = request.aggregate_type_id;
        let filters = request.filters;

        let mut aggregates = Vec::new();

        let data_root_folder =
            std::path::Path::new(&self.node_config.data_root_folder).to_path_buf();

        let base_path = if let Some(type_id) = aggregate_type_id {
            // List specific aggregate type
            data_root_folder.join(format!("{}/{}", org_id, type_id))
        } else {
            // List all aggregate types
            data_root_folder.join(format!("{}", org_id))
        };

        if !base_path.exists() {
            return Ok(ListAggregatesResponse {
                correlation_id: request.correlation_id,
                aggregates,
            });
        }

        if aggregate_type_id.is_some() {
            // List aggregate instances
            list_aggregate_instances(
                &base_path,
                org_id,
                aggregate_type_id.unwrap(),
                &filters,
                &mut aggregates,
            )?;
        } else {
            // List aggregate types, then their instances
            let type_entries = std::fs::read_dir(&base_path)?;

            for type_entry in type_entries {
                let type_entry = type_entry?;
                let type_path = type_entry.path();

                if !type_path.is_dir() {
                    continue;
                }

                let type_id = match type_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| s.parse::<u128>().ok())
                {
                    Some(id) => id,
                    None => continue,
                };

                list_aggregate_instances(&type_path, org_id, type_id, &filters, &mut aggregates)?;
            }
        }

        Ok(ListAggregatesResponse {
            correlation_id: request.correlation_id,
            aggregates,
        })
    }

    async fn update_cache_limits(
        &self,
        request: &UpdateCacheLimitsRequest,
    ) -> Result<(), ReadError> {
        let max_chunk_size = self
            .aggregate_cache
            .aggregate_read_config
            .borrow()
            .max_chunk_size;
        let new_read_config = AggregateReadConfig { max_chunk_size };

        let new_write_config = {
            let existing_write_config = self.aggregate_cache.aggregate_write_config.borrow();
            AggregateWriteConfig {
                max_data_cache_size_bytes: request.aggregate_write_max_data_cache_size_bytes as usize,
                cache_trim_factor: existing_write_config.cache_trim_factor,
                max_chunk_size: existing_write_config.max_chunk_size,
            }
        };

        self.aggregate_cache
            .update_configs(new_read_config, new_write_config);

        // Update all existing cached aggregates
        let keys = self.aggregate_cache.get_all_keys();

        for key in keys {
            let aggregate_resources = self.aggregate_cache.get_aggregate_resources(&key);

            // Update writer cache limit
            aggregate_resources
                .get_writer_mut(false)
                .await?
                .update_max_data_cache_size_bytes(
                    request.aggregate_write_max_data_cache_size_bytes as usize,
                );
        }

        Ok(())
    }

    async fn read(&self, request: &ReadRequest) -> Result<ReadResponse, ReadError> {
        let aggregate_resources = self
            .aggregate_cache
            .get_aggregate_resources(&request.aggregate_key);

        // Writer is the source of truth for current file lengths and available batches
        // Note the design here is to acquire a read lock on the writer, which will wait for any current queing or flush to finish
        let (file_len_metadata, file_len_event_batch, minimum_available_event_batch_index) = {
            let writer = aggregate_resources.get_writer(false).await?;

            // Check if we can serve the read request via the in-memory cache
            if let Ok(result) = writer.maybe_read_cached_events(
                request.correlation_id,
                &request.filters,
                self.node_config.max_event_batches_response_size,
            ) {

                self.watched_aggregates.notify(&request.aggregate_key, AggregateWatchEvent::Read { 
                    correlation_id: request.correlation_id, 
                    from_event_batch_index: request.filters.from_event_batch_index, 
                    to_event_batch_index: result.event_batches.last().map(|eb| eb.event_batch_index),
                    is_cached_read: true
                });

                return Ok(result);
            }

            (
                writer.file_len_metadata,
                writer.file_len_event_batch,
                writer.minimum_available_event_batch_index,
            )
        };

        let read_result = {
            let reader = aggregate_resources.get_reader(false).await?;
            reader
                .read(
                    request.correlation_id,
                    minimum_available_event_batch_index,
                    file_len_metadata,
                    file_len_event_batch,
                    &request.filters,
                    self.node_config.max_event_batches_response_size,
                )
                .await?
        };

        self.watched_aggregates.notify(&request.aggregate_key, AggregateWatchEvent::Read { 
            correlation_id: request.correlation_id, 
            from_event_batch_index: request.filters.from_event_batch_index, 
            to_event_batch_index: read_result.event_batches.last().map(|eb| eb.event_batch_index),
            is_cached_read: false
        });

        Ok(read_result)
    }

    async fn write(
        &self,
        lease_index: u64,
        mut request: WriteRequest,
    ) -> Result<WriteResponse, ReadWriteError> {
        let aggregate_resources = self
            .aggregate_cache
            .get_aggregate_resources(&request.aggregate_key);
        let server_timestamp_ms = get_server_timestamp_millis();

        // Check if previous async sync failed - force durable write to surface error early
        let force_durable = aggregate_resources.has_pending_sync_error();

        // Write lock #1 is for queing data into memory
        let append_result = {
            let mut writer = aggregate_resources
                .get_writer_mut(request.allow_create)
                .await?;

            writer.queue_events_in_memory(
                self.node_config.node_id,
                lease_index,
                server_timestamp_ms,
                &mut request,
            )?
        };

        // Either wait on an amortised fsync or spawn a task to do it and return to client immediately
        // Write lock #2 is for flushing data and updating cache in writer
        if force_durable {
            // Force immediate durable write due to previous sync error
            aggregate_resources.sync_with_delay(None, self.watched_aggregates.clone()).await?;
            aggregate_resources.clear_pending_sync_error();
        } else if let Some(delay_us) = request.durable_write_with_delay_us {
            aggregate_resources
                .sync_with_delay(Some(Duration::from_micros(delay_us)), self.watched_aggregates.clone())
                .await?;
        } else {
            let aggregate_resources = aggregate_resources.clone();
            let async_flush_ms = self.node_config.async_flush_ms;

            let watched_aggregates = self.watched_aggregates.clone();
            glommio::spawn_local(async move {
                let sync_result = aggregate_resources
                    .sync_with_delay(Some(Duration::from_millis(async_flush_ms)), watched_aggregates)
                    .await;
                if let Err(_e) = sync_result {
                    aggregate_resources.set_pending_sync_error();
                }
            })
            .detach();
        }

        Ok(append_result)
    }
}

fn get_server_timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn calculate_disk_usage(path: &std::path::Path) -> Result<u64, ReadError> {
    let mut total = 0u64;

    for entry in walkdir::WalkDir::new(path) {
        let entry = entry?;
        if entry.file_type().is_file() {
            total += entry.metadata()?.len();
        }
    }

    Ok(total)
}

fn list_aggregate_instances(
    path: &std::path::Path,
    org_id: u128,
    aggregate_type_id: u128,
    filters: &DirectoryFilters,
    aggregates: &mut Vec<AggregateInfo>,
) -> Result<(), ReadError> {
    let entries = std::fs::read_dir(path)?;

    for entry in entries {
        let entry = entry?;
        let aggregate_path = entry.path();

        if !aggregate_path.is_dir() {
            continue;
        }

        let aggregate_id = match aggregate_path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|s| s.parse::<u128>().ok())
        {
            Some(id) => id,
            None => continue,
        };

        // Get directory metadata
        let metadata = match std::fs::metadata(&aggregate_path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        let created_at = metadata
            .created()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let modified_at = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let disk_usage = calculate_disk_usage(&aggregate_path)?;

        // Apply filters
        if let Some(after) = filters.created_after_or_on {
            if created_at < after {
                continue;
            }
        }
        if let Some(before) = filters.created_before_or_on {
            if created_at > before {
                continue;
            }
        }
        if let Some(after) = filters.modified_after_or_on {
            if modified_at < after {
                continue;
            }
        }
        if let Some(before) = filters.modified_before_or_on {
            if modified_at > before {
                continue;
            }
        }
        if let Some(max) = filters.disk_usage_less_than_or_equal {
            if disk_usage > max {
                continue;
            }
        }
        if let Some(min) = filters.disk_usage_greater_than_or_equal {
            if disk_usage < min {
                continue;
            }
        }

        aggregates.push(AggregateInfo {
            created_at,
            modified_at,
            disk_usage,
            key: AggregateKey::new(org_id, aggregate_type_id, aggregate_id),
        });
    }

    Ok(())
}

#[cfg(test)]
mod test_local_aggregate_integration {
    use celeriant_msg::request::{
        directory_filters::DirectoryFilters,
        read_filters::ReadFilters,
        requests::{
            DeleteRequest, ListAggregatesRequest, ListOrganisationsRequest, PrependBatchesRequest,
            ReadRequest, TrimStartRequest, UpdateCacheLimitsRequest, WriteRequest,
        },
    };
    use celeriant_wal::{
        aggregate_key::AggregateKey, compression_type::CompressionType, wal::event_item::EventItem,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        local_aggregate::{LocalAggregate, LocalAggregateTrait}, node_config::test_node_config::test_config,
        read_operations::read_structures::AggregateReadConfig,
        write_operations::aggregate_write_config::AggregateWriteConfig,
    };

    /// Helper to create test events
    fn create_events(start_index: u64, count: usize, base_timestamp: u64) -> Vec<EventItem> {
        (0..count)
            .map(|i| {
                EventItem::new(
                    start_index + i as u64,
                    0,
                    None,
                    base_timestamp + i as u64,
                    1,
                    0,
                    vec![i as u8; 50],
                )
            })
            .collect()
    }

    fn create_local_aggregate(data_root: &str) -> LocalAggregate {
        let read_config = AggregateReadConfig {
            max_chunk_size: 1 << 20,
        };
        let write_config = AggregateWriteConfig {
            max_data_cache_size_bytes: 1 << 25,
            cache_trim_factor: 25,
            max_chunk_size: 1 << 20,
        };
        LocalAggregate::new(read_config, write_config, test_config(data_root))
    }

    #[test]
    fn test_full_write_read_lifecycle() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);

                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Test 1: Write first batch (creates aggregate)
                let write_request = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 3, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };

                let result = local_aggregate.write(1, write_request).await;
                let write_response = result.unwrap();
                assert_eq!(write_response.correlation_id, Some(2));
                assert_eq!(write_response.event_batch_index, 1);

                // Test 2: Write second batch
                let write_request = WriteRequest {
                    correlation_id: Some(4),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: Some(42),
                    events: create_events(4, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };

                let result = local_aggregate.write(1, write_request).await;
                let write_response = result.unwrap();
                assert_eq!(write_response.event_batch_index, 2);

                // Test 3: Read all events
                let read_request = ReadRequest {
                    correlation_id: Some(5),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };

                let result = local_aggregate.read(&read_request).await;
                let read_response = result.unwrap();
                assert_eq!(read_response.correlation_id, Some(5));
                assert_eq!(read_response.event_batches.len(), 2);
                assert_eq!(read_response.event_batches[0].events.len(), 3);
                assert_eq!(read_response.event_batches[1].events.len(), 2);
                assert_eq!(read_response.event_batches[1].user_id, Some(42));

                // Test 4: Read with filters
                let read_request = ReadRequest {
                    correlation_id: Some(6),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1).min_event_timestamp(2000),
                };

                let result = local_aggregate.read(&read_request).await;
                let read_response = result.unwrap();
                assert_eq!(read_response.event_batches.len(), 1);
                assert_eq!(read_response.event_batches[0].event_batch_index, 2);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_aggregate_isolation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);

                let aggregate_key_1 = AggregateKey::new(1, 1, 1);
                let aggregate_key_2 = AggregateKey::new(1, 1, 2);

                // Write to aggregate 1
                let write_req1 = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key_1.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req1).await.unwrap();

                // Write to aggregate 2
                let write_req2 = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key_2.clone(),
                    client_id: 200,
                    user_id: None,
                    events: create_events(1, 3, 2000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req2).await.unwrap();

                // Read from aggregate 1 - should only get its events
                let read_req1 = ReadRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key_1.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req1).await.unwrap();

                assert_eq!(result.event_batches.len(), 1);
                assert_eq!(result.event_batches[0].events.len(), 2);
                assert_eq!(result.event_batches[0].client_id, 100);

                // Read from aggregate 2 - should only get its events
                let read_req2 = ReadRequest {
                    correlation_id: Some(4),
                    aggregate_key: aggregate_key_2.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req2).await.unwrap();

                assert_eq!(result.event_batches.len(), 1);
                assert_eq!(result.event_batches[0].events.len(), 3);
                assert_eq!(result.event_batches[0].client_id, 200);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_concurrency_violations() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write first batch
                let write_req = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Try to write with wrong expected index - should fail
                let write_req = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(1), // Wrong! Should be 2
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                let result = local_aggregate.write(1, write_req).await;
                assert!(result.is_err());

                // Try with overlapping client event index - should fail
                let write_req = WriteRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(2, 2, 2000), // Overlaps with previous write
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                let result = local_aggregate.write(1, write_req).await;
                assert!(result.is_err());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_trim_start_operation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write 5 batches
                for i in 1..=5u64 {
                    let write_req = WriteRequest {
                        correlation_id: Some(i as u128),
                        aggregate_key: aggregate_key.clone(),
                        client_id: 100,
                        user_id: None,
                        events: create_events(i * 10, 2, i * 1000),
                        allow_create: i == 1,
                        expected_event_batch_index: Some(i),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    };
                    local_aggregate.write(1, write_req).await.unwrap();
                }

                // Verify all batches exist
                let read_req = ReadRequest {
                    correlation_id: Some(10),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();
                assert_eq!(result.event_batches.len(), 5);

                // Trim first 2 batches
                let trim_req = TrimStartRequest {
                    correlation_id: Some(11),
                    aggregate_key: aggregate_key.clone(),
                    keep_from_event_batch_index: 3,
                };
                local_aggregate.trim_start(&trim_req).await.unwrap();

                // Verify only batches 3-5 remain
                let read_req = ReadRequest {
                    correlation_id: Some(12),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(3),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 3);
                assert_eq!(result.event_batches[0].event_batch_index, 3);

                // Try to read from batch 1 - should fail
                let read_req = ReadRequest {
                    correlation_id: Some(13),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_err());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_delete_operation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Create aggregate
                let write_req = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Verify it exists by reading
                let read_req = ReadRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_ok());

                // Delete it
                let delete_req = DeleteRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                };
                local_aggregate.delete(&delete_req).await.unwrap();

                // Verify it no longer exists (read should fail)
                let read_req = ReadRequest {
                    correlation_id: Some(4),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_err());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_update_cache_limits() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Create an aggregate first
                let write_req = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Update cache limits
                let update_req = UpdateCacheLimitsRequest {
                    correlation_id: Some(2),
                    aggregate_write_max_data_cache_size_bytes: 1 << 26,
                };
                local_aggregate
                    .update_cache_limits(&update_req)
                    .await
                    .unwrap();

                // Write more data - should work with new limits
                let write_req = WriteRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                let result = local_aggregate.write(1, write_req).await;
                assert!(result.is_ok());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_pagination_requests() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let mut node_config = test_config(data_root);
                node_config.max_event_batches_response_size = Some(300);

                let read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };
                let write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };
                let local_aggregate = LocalAggregate::new(read_config, write_config, node_config);

                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write 10 batches
                for i in 1..=10u64 {
                    let write_req = WriteRequest {
                        correlation_id: Some(i as u128),
                        aggregate_key: aggregate_key.clone(),
                        client_id: 100,
                        user_id: None,
                        events: create_events(i * 10, 3, i * 1000),
                        allow_create: i == 1,
                        expected_event_batch_index: Some(i),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    };
                    local_aggregate.write(1, write_req).await.unwrap();
                }

                // Read first page with limit
                let read_req = ReadRequest {
                    correlation_id: Some(100),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert!(result.event_batches.len() < 10);
                assert!(result.next_event_batch_index.is_some());

                let next_batch_index = result.next_event_batch_index.unwrap();

                // Read second page
                let read_req = ReadRequest {
                    correlation_id: Some(101),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(next_batch_index),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert!(result.event_batches.len() > 0);
                assert_eq!(result.event_batches[0].event_batch_index, next_batch_index);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_prepend_batches() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write batches 1-5
                for i in 1..=5u64 {
                    let write_req = WriteRequest {
                        correlation_id: Some(i as u128),
                        aggregate_key: aggregate_key.clone(),
                        client_id: 100,
                        user_id: None,
                        events: create_events(i * 10, 2, i * 1000),
                        allow_create: i == 1,
                        expected_event_batch_index: Some(i),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(20),
                        compression_type: CompressionType::None,
                    };
                    local_aggregate.write(1, write_req).await.unwrap();
                }

                // Read to get batches we'll later prepend
                let read_req = ReadRequest {
                    correlation_id: Some(10),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(3),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                // Trim to keep only batch 5
                let trim_req = TrimStartRequest {
                    correlation_id: Some(11),
                    aggregate_key: aggregate_key.clone(),
                    keep_from_event_batch_index: 5,
                };
                local_aggregate.trim_start(&trim_req).await.unwrap();

                // Now prepend batches 3-4
                let prepend_batches = result.event_batches[0..2].to_vec();
                let prepend_req = PrependBatchesRequest {
                    correlation_id: Some(12),
                    aggregate_key: aggregate_key.clone(),
                    allow_create: false,
                    compression_type: CompressionType::Snappy,
                    batches: prepend_batches,
                    durable_write_with_delay_us: Some(0),
                };
                local_aggregate.prepend_batches(&prepend_req).await.unwrap();

                // Verify we now have batches 3-5
                let read_req = ReadRequest {
                    correlation_id: Some(13),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(3),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 3);
                assert_eq!(result.event_batches[0].event_batch_index, 3);
                assert_eq!(result.event_batches[1].event_batch_index, 4);
                assert_eq!(result.event_batches[2].event_batch_index, 5);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_multi_client_writes() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Client 100 writes
                let write_req = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Client 200 writes - different client, same client_event_index is OK
                let write_req = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 200,
                    user_id: None,
                    events: create_events(1, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                let result = local_aggregate.write(1, write_req).await.unwrap();
                assert_eq!(result.event_batch_index, 2);

                // Client 100 continues
                let write_req = WriteRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 3000),
                    allow_create: false,
                    expected_event_batch_index: Some(3),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Read all and verify client isolation
                let read_req = ReadRequest {
                    correlation_id: Some(4),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 3);
                assert_eq!(result.event_batches[0].client_id, 100);
                assert_eq!(result.event_batches[1].client_id, 200);
                assert_eq!(result.event_batches[2].client_id, 100);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_list_organisations_and_aggregates() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);

                // Create aggregates in different organisations
                for org_id in 1u128..=3 {
                    for aggregate_id in 1u128..=2 {
                        let aggregate_key = AggregateKey::new(org_id, 1, aggregate_id);
                        let write_req = WriteRequest {
                            correlation_id: Some(org_id * 10 + aggregate_id),
                            aggregate_key,
                            client_id: 100,
                            user_id: None,
                            events: create_events(1, 2, 1000),
                            allow_create: true,
                            expected_event_batch_index: Some(1),
                            enforce_client_idempotency: true,
                            durable_write_with_delay_us: Some(0),
                            compression_type: CompressionType::None,
                        };
                        local_aggregate.write(1, write_req).await.unwrap();
                    }
                }

                // List organisations
                let list_orgs_req = ListOrganisationsRequest {
                    correlation_id: Some(100),
                    filters: DirectoryFilters::default(),
                };
                let result = local_aggregate.list_organisations(list_orgs_req).unwrap();
                assert_eq!(result.correlation_id, Some(100));
                assert_eq!(result.organisations.len(), 3);
                for org in &result.organisations {
                    assert!(org.disk_usage > 0);
                }

                // List aggregates for org 1
                let list_aggs_req = ListAggregatesRequest {
                    correlation_id: Some(101),
                    org_id: 1,
                    aggregate_type_id: Some(1),
                    filters: DirectoryFilters::default(),
                };
                let result = local_aggregate.list_aggregates(list_aggs_req).unwrap();
                assert_eq!(result.correlation_id, Some(101));
                assert_eq!(result.aggregates.len(), 2);
                for agg in &result.aggregates {
                    assert_eq!(agg.key.org_id, 1);
                    assert_eq!(agg.key.aggregate_type_id, 1);
                    assert!(agg.disk_usage > 0);
                }

                // List all aggregate types for org 2
                let list_aggs_req = ListAggregatesRequest {
                    correlation_id: Some(102),
                    org_id: 2,
                    aggregate_type_id: None,
                    filters: DirectoryFilters::default(),
                };
                let result = local_aggregate.list_aggregates(list_aggs_req).unwrap();
                assert_eq!(result.aggregates.len(), 2);
                for agg in &result.aggregates {
                    assert_eq!(agg.key.org_id, 2);
                }
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_background_vs_synchronous_writes() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write with immediate sync
                let write_req = WriteRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: true,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0), // Sync immediately
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Write with background sync (None)
                let write_req = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(3, 2, 2000),
                    allow_create: false,
                    expected_event_batch_index: Some(2),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: None, // Background sync
                    compression_type: CompressionType::None,
                };
                local_aggregate.write(1, write_req).await.unwrap();

                // Give background sync time to complete
                glommio::timer::sleep(std::time::Duration::from_millis(300)).await;

                // Read should work for both
                let read_req = ReadRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 2);
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_error_handling_nonexistent_aggregate() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);
                let aggregate_key = AggregateKey::new(1, 1, 999);

                // Try to read from non-existent aggregate
                let read_req = ReadRequest {
                    correlation_id: Some(1),
                    aggregate_key: aggregate_key.clone(),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_err());

                // Try to write with allow_create=false
                let write_req = WriteRequest {
                    correlation_id: Some(2),
                    aggregate_key: aggregate_key.clone(),
                    client_id: 100,
                    user_id: None,
                    events: create_events(1, 2, 1000),
                    allow_create: false,
                    expected_event_batch_index: Some(1),
                    enforce_client_idempotency: true,
                    durable_write_with_delay_us: Some(0),
                    compression_type: CompressionType::None,
                };
                let result = local_aggregate.write(1, write_req).await;
                assert!(result.is_err());

                // Try to trim non-existent aggregate
                let trim_req = TrimStartRequest {
                    correlation_id: Some(3),
                    aggregate_key: aggregate_key.clone(),
                    keep_from_event_batch_index: 1,
                };
                let result = local_aggregate.trim_start(&trim_req).await;
                assert!(result.is_err());
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_complex_multi_aggregate_scenario() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_str().unwrap();

                let local_aggregate = create_local_aggregate(data_root);

                // Scenario: 3 users (aggregates) in a collaborative app
                // Each user has their own event stream
                for user_aggregate_id in 1u128..=3 {
                    let aggregate_key = AggregateKey::new(1, 1, user_aggregate_id);
                    // Initial write
                    let write_req = WriteRequest {
                        correlation_id: Some(user_aggregate_id),
                        aggregate_key,
                        client_id: user_aggregate_id * 100,
                        user_id: Some(user_aggregate_id),
                        events: create_events(1, 3, 1000),
                        allow_create: true,
                        expected_event_batch_index: Some(1),
                        enforce_client_idempotency: true,
                        durable_write_with_delay_us: Some(0),
                        compression_type: CompressionType::None,
                    };
                    local_aggregate.write(1, write_req).await.unwrap();
                }

                // All users perform more writes
                for user_aggregate_id in 1u128..=3 {
                    for batch in 2..=5u64 {
                        let aggregate_key = AggregateKey::new(1, 1, user_aggregate_id);
                        let write_req = WriteRequest {
                            correlation_id: Some(user_aggregate_id * 100 + batch as u128),
                            aggregate_key,
                            client_id: user_aggregate_id * 100,
                            user_id: Some(user_aggregate_id),
                            events: create_events(batch * 10, 2, batch * 1000),
                            allow_create: false,
                            expected_event_batch_index: Some(batch),
                            enforce_client_idempotency: true,
                            durable_write_with_delay_us: None, // Background
                            compression_type: CompressionType::None,
                        };
                        local_aggregate.write(1, write_req).await.unwrap();
                    }
                }

                // Wait for background syncs
                glommio::timer::sleep(std::time::Duration::from_millis(500)).await;

                // User 1 reads their full history
                let read_req = ReadRequest {
                    correlation_id: Some(1000),
                    aggregate_key: AggregateKey::new(1, 1, 1),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 5);
                // Verify all batches belong to user 1
                for batch in &result.event_batches {
                    assert_eq!(batch.client_id, 100);
                }

                // User 2 trims old data
                let trim_req = TrimStartRequest {
                    correlation_id: Some(2000),
                    aggregate_key: AggregateKey::new(1, 1, 2),
                    keep_from_event_batch_index: 3,
                };
                local_aggregate.trim_start(&trim_req).await.unwrap();

                // Verify user 2 only has recent data (reading from batch 1 should error)
                let read_req = ReadRequest {
                    correlation_id: Some(2001),
                    aggregate_key: AggregateKey::new(1, 1, 2),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await;
                assert!(result.is_err());

                // But user 3 still has all their data
                let read_req = ReadRequest {
                    correlation_id: Some(3000),
                    aggregate_key: AggregateKey::new(1, 1, 3),
                    filters: ReadFilters::new(1),
                };
                let result = local_aggregate.read(&read_req).await.unwrap();

                assert_eq!(result.event_batches.len(), 5);

                // List all aggregates
                let list_req = ListAggregatesRequest {
                    correlation_id: Some(4000),
                    org_id: 1,
                    aggregate_type_id: Some(1),
                    filters: DirectoryFilters::default(),
                };
                let result = local_aggregate.list_aggregates(list_req).unwrap();
                assert_eq!(result.aggregates.len(), 3);
            })
            .unwrap();

        handle.join().unwrap();
    }
}
