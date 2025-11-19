use std::{num::NonZeroUsize, path::Path, time::Duration};

use eventplanedb_structures::{
    aggregate_info::AggregateInfo,
    aggregate_key::AggregateKey,
    directory_filters::DirectoryFilters,
    eventplanedb_error::EventPlaneDBError,
    organisation::Organisation,
    read_result::ReadResult,
    request::{
        DeleteRequest, ListAggregatesRequest, ListOrganisationsRequest,
        ReadRequest, Request, TrimStartRequest, UpdateCacheLimitsRequest, WriteBatchesRequest,
        WriteRequest,
    },
    response::{
        DeleteResponse, ExistsResponse, ListAggregatesResponse, ListOrganisationsResponse,
        ReadResponse, Response, TrimStartResponse, UpdateCacheLimitsResponse, WriteBatchesResponse,
        WriteResponse,
    },
};
use glommio::spawn_local;
use log::error;

use crate::{
    cache::aggregate_cache::AggregateCache,
    read_operations::{read_operations::ReadOperations, read_structures::AggregateReadConfig},
    write_operations::{
        write_operations::WriteOperations,
        write_structures::{AggregateWriteConfig, WriteOptions},
    },
};

pub struct ProcessRequest {
    data_root_folder: String,
    aggregate_cache: AggregateCache,
}

impl ProcessRequest {
    pub fn new(
        data_root_folder: String,
        aggregate_read_config: AggregateReadConfig,
        aggregate_write_config: AggregateWriteConfig,
        max_open_aggregates: usize,
    ) -> Self {
        let capacity = NonZeroUsize::new(max_open_aggregates).unwrap();
        Self {
            data_root_folder: data_root_folder.clone(),
            aggregate_cache: AggregateCache::new(
                capacity,
                data_root_folder,
                aggregate_read_config,
                aggregate_write_config,
            ),
        }
    }

    // Helper: Get current server timestamp
    fn get_server_timestamp_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub async fn process(
        &self,
        request: Request,
        max_event_batches_response_size: Option<usize>,
    ) -> Response {
        match request {
            Request::UpdateCacheLimits(request) => {
                let correlation_id = request.correlation_id;
                let result = self.handle_update_cache_limits(request).await;

                match result {
                    Ok(accepted) => Response::UpdateCacheLimits(UpdateCacheLimitsResponse {
                        correlation_id,
                        error: None,
                        accepted,
                    }),
                    Err(e) => Response::UpdateCacheLimits(UpdateCacheLimitsResponse {
                        correlation_id,
                        error: Some(e),
                        accepted: false,
                    }),
                }
            }

            Request::Write(request) => {
                let correlation_id = request.correlation_id;
                let result = self.handle_write(request).await;

                match result {
                    Ok(append_result) => Response::Write(WriteResponse {
                        correlation_id,
                        error: None,
                        result: Some(append_result),
                    }),
                    Err(e) => Response::Write(WriteResponse {
                        correlation_id,
                        error: Some(e),
                        result: None,
                    }),
                }
            }

            Request::Read(request) => {
                let correlation_id = request.correlation_id;
                let result = self
                    .handle_read(request, max_event_batches_response_size)
                    .await;

                match result {
                    Ok(read_result) => Response::Read(ReadResponse {
                        correlation_id,
                        error: None,
                        result: Some(read_result),
                    }),
                    Err(e) => Response::Read(ReadResponse {
                        correlation_id,
                        error: Some(e),
                        result: None,
                    }),
                }
            }

            Request::Exists(request) => {
                let aggregate_key = AggregateKey::new(
                    request.org_id,
                    request.aggregate_type_id,
                    request.aggregate_id,
                );
                let aggregate_resources = self.aggregate_cache.get(&aggregate_key);
                let exists = aggregate_resources.get_reader(false).await.is_ok();

                Response::Exists(ExistsResponse {
                    correlation_id: request.correlation_id,
                    error: None,
                    exists,
                })
            }

            Request::TrimStart(request) => {
                let correlation_id = request.correlation_id;
                let result = self.handle_trim_start(request).await;

                match result {
                    Ok(()) => Response::TrimStart(TrimStartResponse {
                        correlation_id,
                        error: None,
                    }),
                    Err(e) => Response::TrimStart(TrimStartResponse {
                        correlation_id,
                        error: Some(e),
                    }),
                }
            }

            Request::Delete(request) => {
                let correlation_id = request.correlation_id;
                let result = self.handle_delete(request).await;

                match result {
                    Ok(()) => Response::Delete(DeleteResponse {
                        correlation_id,
                        error: None,
                    }),
                    Err(e) => Response::Delete(DeleteResponse {
                        correlation_id,
                        error: Some(e),
                    }),
                }
            }

            Request::ListOrganisations(request) => {
                let correlation_id = request.correlation_id;
                let result = self.handle_list_organisations(request);

                match result {
                    Ok(organisations) => Response::ListOrganisations(ListOrganisationsResponse {
                        correlation_id,
                        error: None,
                        organisations,
                    }),
                    Err(e) => Response::ListOrganisations(ListOrganisationsResponse {
                        correlation_id,
                        error: Some(e),
                        organisations: vec![],
                    }),
                }
            }

            Request::ListAggregates(request) => {
                let correlation_id = request.correlation_id;
                let result = self.handle_list_aggregates(request);

                match result {
                    Ok(aggregates) => Response::ListAggregates(ListAggregatesResponse {
                        correlation_id,
                        error: None,
                        aggregates,
                    }),
                    Err(e) => Response::ListAggregates(ListAggregatesResponse {
                        correlation_id,
                        error: Some(e),
                        aggregates: vec![],
                    }),
                }
            }

            Request::WriteBatches(request) => {
                let correlation_id = request.correlation_id;
                let result = self.handle_write_batches(request).await;

                match result {
                    Ok(()) => Response::WriteBatches(WriteBatchesResponse {
                        correlation_id,
                        error: None,
                    }),
                    Err(e) => Response::WriteBatches(WriteBatchesResponse {
                        correlation_id,
                        error: Some(e),
                    }),
                }
            }
        }
    }

    async fn handle_update_cache_limits(
        &self,
        request: UpdateCacheLimitsRequest,
    ) -> Result<bool, EventPlaneDBError> {
        // Update the stored configs for new aggregates
        let new_read_config = AggregateReadConfig {
            max_chunk_size: self
                .aggregate_cache
                .aggregate_read_config
                .borrow()
                .max_chunk_size,
            max_data_cache_size_bytes: request.aggregate_read_max_data_cache_size_bytes as usize,
        };

        let new_write_config = AggregateWriteConfig {
            max_data_cache_size_bytes: request.aggregate_write_max_data_cache_size_bytes as usize,
            cache_trim_factor: self
                .aggregate_cache
                .aggregate_write_config
                .borrow()
                .cache_trim_factor,
            max_chunk_size: self
                .aggregate_cache
                .aggregate_write_config
                .borrow()
                .max_chunk_size,
        };

        // Update configs for new aggregates
        self.aggregate_cache
            .update_configs(new_read_config, new_write_config);

        // Update all existing cached aggregates
        let keys = self.aggregate_cache.get_all_keys();

        for key in keys {
            let aggregate_resources = self.aggregate_cache.get(&key);

            // Update reader cache limit
            if let Ok(mut reader) = aggregate_resources.get_reader_mut(false).await {
                if let Some(r_reader) = reader.as_mut() {
                    r_reader.update_max_data_cache_size_bytes(
                        request.aggregate_read_max_data_cache_size_bytes as usize,
                    );
                }
            }

            // Update writer cache limit
            if let Ok(mut writer) = aggregate_resources.get_writer_mut(false).await {
                if let Some(r_writer) = writer.as_mut() {
                    r_writer.update_max_data_cache_size_bytes(
                        request.aggregate_write_max_data_cache_size_bytes as usize,
                    );
                }
            }
        }

        Ok(true)
    }

    async fn handle_write_batches(
        &self,
        request: WriteBatchesRequest,
    ) -> Result<(), EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(
            request.org_id,
            request.aggregate_type_id,
            request.aggregate_id,
        );
        let aggregate_resources = self.aggregate_cache.get(&aggregate_key);
        let mut reader = aggregate_resources
            .get_reader_mut(request.allow_create)
            .await?;
        let mut writer = aggregate_resources
            .get_writer_mut(request.allow_create)
            .await?;
        let reader_ref = reader.as_mut().unwrap();
        let writer_ref = writer.as_mut().unwrap();
        writer_ref.prepend_batches(request.compression_type, &request.batches)
            .await?;
        reader_ref.trim_start(
            writer_ref.metadata_dma_file.dup().unwrap(),
            writer_ref.event_batches_dma_file.dup().unwrap(),
        );

        Ok(())
    }

    fn handle_list_organisations(
        &self,
        request: ListOrganisationsRequest,
    ) -> Result<Vec<Organisation>, EventPlaneDBError> {
        let mut orgs = Vec::new();

        let data_root_folder = Path::new(&self.data_root_folder).to_path_buf();

        let entries =
            std::fs::read_dir(&data_root_folder).map_err(|_| EventPlaneDBError::io_error())?;

        for entry in entries {
            let entry = entry.map_err(|_| EventPlaneDBError::io_error())?;
            let path = entry.path();

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

            orgs.push(Organisation {
                org_id,
                created_at,
                modified_at,
                disk_usage,
            });
        }

        Ok(orgs)
    }

    fn handle_list_aggregates(
        &self,
        request: ListAggregatesRequest,
    ) -> Result<Vec<AggregateInfo>, EventPlaneDBError> {
        let org_id = request.org_id;
        let aggregate_type_id = request.aggregate_type_id;
        let filters = request.filters;

        let mut aggregates = Vec::new();

        let data_root_folder = Path::new(&self.data_root_folder).to_path_buf();

        let base_path = if let Some(type_id) = aggregate_type_id {
            // List specific aggregate type
            data_root_folder.join(format!("{}/{}", org_id, type_id))
        } else {
            // List all aggregate types
            data_root_folder.join(format!("{}", org_id))
        };

        if !base_path.exists() {
            return Ok(aggregates);
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
            let type_entries =
                std::fs::read_dir(&base_path).map_err(|_| EventPlaneDBError::io_error())?;

            for type_entry in type_entries {
                let type_entry = type_entry.map_err(|_| EventPlaneDBError::io_error())?;
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

        Ok(aggregates)
    }

    async fn handle_write(
        &self,
        request: WriteRequest,
    ) -> Result<eventplanedb_structures::write_result::WriteResult, EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(
            request.org_id,
            request.aggregate_type_id,
            request.aggregate_id,
        );
        let aggregate_resources = self.aggregate_cache.get(&aggregate_key);

        let append_result = {
            let mut writer = aggregate_resources
                .get_writer_mut(request.allow_create)
                .await?;
            let r_writer = writer.as_mut().unwrap();

            let append_options = WriteOptions {
                client_id: request.client_id,
                user_id: request.user_id,
                expected_event_batch_index: request.expected_event_batch_index,
                enforce_client_idempotency: request.enforce_client_idempotency,
                server_timestamp_millis: Self::get_server_timestamp_millis(),
                compression_type: request.compression_type,
            };
            r_writer.queue_events_in_memory(request.events, &append_options)?
        };

        if let Some(delay_us) = request.durable_write_with_delay_us {
            aggregate_resources
                .sync_with_delay(Duration::from_micros(delay_us))
                .await?;
        } else {
            let aggregate_resources = aggregate_resources.clone();
            let delay_us = 200;

            spawn_local(async move {
                let sync_result = aggregate_resources
                    .sync_with_delay(Duration::from_micros(delay_us))
                    .await;
                if let Err(e) = sync_result {
                    error!("Background sync failed: {:?}", e);
                }
            })
            .detach();
        }

        Ok(append_result)
    }

    async fn handle_read(
        &self,
        request: ReadRequest,
        max_event_batches_response_size: Option<usize>,
    ) -> Result<ReadResult, EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(
            request.org_id,
            request.aggregate_type_id,
            request.aggregate_id,
        );
        let aggregate_resources = self.aggregate_cache.get(&aggregate_key);

        let (file_len_metadata, file_len_event_batch, minimum_available_event_batch_index) = {
            let writer = aggregate_resources.get_writer(false).await?;
            let r_writer = writer.as_ref().unwrap();
            if let Ok(result) =
                r_writer.maybe_read_cached_events(&request.filters, max_event_batches_response_size)
            {
                return Ok(ReadResult {
                    event_batches: result.filtered_event_batches,
                    next_event_batch_index: result.next_event_batch_index,
                });
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
                    max_event_batches_response_size,
                )
                .await?
        };

        //TODO: If we have an UNFILTERED contiguous read of batches that can be added to writer cache, add it!
        //The writer cache could be empty and we have read up to the most recent event batch (although have to check again after getting write lock on writer)
        //Or there could be data in the cache but the read matches up to that data and we can insert it in front

        if !read_result.uncached_metadata_set.is_empty() {
            let mut reader = aggregate_resources.get_reader_mut(false).await?;
            reader
                .as_mut()
                .unwrap()
                .update_metadata_cache(read_result.uncached_metadata_set);
        }

        Ok(ReadResult {
            event_batches: read_result.filtered_event_batches,
            next_event_batch_index: read_result.next_event_batch_index,
        })
    }

    async fn handle_trim_start(&self, request: TrimStartRequest) -> Result<(), EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(
            request.org_id,
            request.aggregate_type_id,
            request.aggregate_id,
        );
        let aggregate_resources = self.aggregate_cache.get(&aggregate_key);

        let mut writer = aggregate_resources.get_writer_mut(false).await?;
        let r_writer = writer.as_mut().unwrap();
        let mut reader = aggregate_resources.get_reader_mut(false).await?;
        let r_reader = reader.as_mut().unwrap();

        let file_positions = r_reader
            .get_file_positions(
                r_writer.minimum_available_event_batch_index,
                request.keep_from_event_batch_index,
                r_writer.file_len_metadata,
                r_writer.file_len_event_batch,
            )
            .await?;

        r_writer
            .trim_start(
                request.keep_from_event_batch_index,
                file_positions.metadata_position,
                file_positions.event_batch_position,
            )
            .await?;

        let metadata_dma_file = r_writer
            .metadata_dma_file
            .dup()
            .map_err(|_e| EventPlaneDBError::io_error())?;
        let event_batches_dma_file = r_writer
            .event_batches_dma_file
            .dup()
            .map_err(|_e| EventPlaneDBError::io_error())?;

        r_reader.trim_start(metadata_dma_file, event_batches_dma_file);

        Ok(())
    }

    async fn handle_delete(&self, request: DeleteRequest) -> Result<(), EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(
            request.org_id,
            request.aggregate_type_id,
            request.aggregate_id,
        );
        self.aggregate_cache.pop(&aggregate_key);

        // Delete files
        let data_root_folder = Path::new(&self.data_root_folder).to_path_buf();
        let metadata_path = data_root_folder.join(format!(
            "{}/{}/{}/metadata.bin",
            request.org_id, request.aggregate_type_id, request.aggregate_id
        ));
        let events_path = data_root_folder.join(format!(
            "{}/{}/{}/event_batches.bin",
            request.org_id, request.aggregate_type_id, request.aggregate_id
        ));

        std::fs::remove_file(&metadata_path).map_err(|_e| EventPlaneDBError::io_error())?;
        std::fs::remove_file(&events_path).map_err(|_e| EventPlaneDBError::io_error())?;

        Ok(())
    }
}

fn calculate_disk_usage(path: &Path) -> Result<u64, EventPlaneDBError> {
    let mut total = 0u64;

    for entry in walkdir::WalkDir::new(path) {
        let entry = entry.map_err(|_| EventPlaneDBError::io_error())?;
        if entry.file_type().is_file() {
            total += entry
                .metadata()
                .map_err(|_| EventPlaneDBError::io_error())?
                .len();
        }
    }

    Ok(total)
}

// Helper function to list aggregate instances in a directory
fn list_aggregate_instances(
    path: &Path,
    org_id: u128,
    aggregate_type_id: u128,
    filters: &DirectoryFilters,
    aggregates: &mut Vec<AggregateInfo>,
) -> Result<(), EventPlaneDBError> {
    let entries = std::fs::read_dir(path).map_err(|_| EventPlaneDBError::io_error())?;

    for entry in entries {
        let entry = entry.map_err(|_| EventPlaneDBError::io_error())?;
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
            org_id,
            aggregate_type_id,
            aggregate_id,
            created_at,
            modified_at,
            disk_usage,
        });
    }

    Ok(())
}
