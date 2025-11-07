use std::{collections::HashMap, rc::Rc, time::Duration};

use eventplanedb_core::{files::{helper::{get_or_create_reader, get_or_create_writer}, read_operations::{AggregateReadConfig, ReadOperations}, write_operations::{AggregateWriteConfig, AppendOptions, WriteOperations}}, local_event::LocalEvent};
use eventplanedb_structures::{aggregate_key::AggregateKey, compression_type::CompressionType, event_item::EventItem, eventplanedb_error::EventPlaneDBError, read_result::ReadResult, request::Request, response::Response};
use glommio::{spawn_local, sync::{RwLock, Semaphore}, timer::sleep};
use log::error;

type SyncResult = Result<(), EventPlaneDBError>;

pub struct ProcessRequest {
    aggregate_read_config: AggregateReadConfig,
    aggregate_write_config: AggregateWriteConfig,    
    read_operations: RwLock<HashMap<AggregateKey, Rc<RwLock<ReadOperations>>>>,
    write_operations: RwLock<HashMap<AggregateKey, Rc<RwLock<WriteOperations>>>>,
    wal_sync_events: RwLock<HashMap<AggregateKey, Rc<RwLock<Option<Rc<LocalEvent<SyncResult>>>>>>>,
    semaphores: RwLock<HashMap<AggregateKey, Rc<Semaphore>>>,
}

impl ProcessRequest {
    pub fn new(
        aggregate_read_config: AggregateReadConfig,
        aggregate_write_config: AggregateWriteConfig,
    ) -> Self {
        Self {
            aggregate_read_config,
            aggregate_write_config,
            read_operations: RwLock::new(HashMap::new()),
            write_operations: RwLock::new(HashMap::new()),
            wal_sync_events: RwLock::new(HashMap::new()),
            semaphores: RwLock::new(HashMap::new()),
        }
    }

    // Helper: Get current server timestamp
    fn get_server_timestamp_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    // Helper: Get or create writer
    async fn get_or_create_writer(
        &self,
        aggregate_key: &AggregateKey,
        allow_create: bool,
    ) -> Result<Rc<RwLock<WriteOperations>>, EventPlaneDBError> {
        get_or_create_writer(
            aggregate_key,
            "data",
            allow_create,
            &self.read_operations,
            &self.aggregate_read_config,
            &self.write_operations,
            &self.aggregate_write_config,
        )
        .await
        .map_err(EventPlaneDBError::from)
    }

    // Helper: Get or create reader
    async fn get_or_create_reader(
        &self,
        aggregate_key: &AggregateKey,
    ) -> Result<Rc<RwLock<ReadOperations>>, EventPlaneDBError> {        
        get_or_create_reader(
            aggregate_key,
            "data",
            false,
            &self.read_operations,
            &self.aggregate_read_config,
        )
        .await
        .map_err(|e| EventPlaneDBError::io_error(e))
    }

    // Helper: Get or create WAL sync event
    async fn get_or_create_wal_sync_event(
        &self,
        aggregate_key: &AggregateKey,
    ) -> Rc<RwLock<Option<Rc<LocalEvent<SyncResult>>>>> {
        let existing = {
            let map = self.wal_sync_events.read().await.unwrap();
            map.get(aggregate_key).cloned()
        };

        match existing {
            Some(event) => event,
            None => {
                let event = Rc::new(RwLock::new(None));
                self.wal_sync_events.write().await.unwrap()
                    .insert(aggregate_key.clone(), event.clone());
                event
            }
        }
    }

    // Helper: Get or create semaphore
    async fn get_or_create_semaphore(
        &self,
        aggregate_key: &AggregateKey,
    ) -> Rc<Semaphore> {
        let existing = {
            let map = self.semaphores.read().await.unwrap();
            map.get(aggregate_key).cloned()
        };

        match existing {
            Some(sem) => sem,
            None => {
                let sem = Rc::new(Semaphore::new(1));
                self.semaphores.write().await.unwrap()
                    .insert(aggregate_key.clone(), sem.clone());
                sem
            }
        }
    }

    pub async fn process(&self, request: Request) -> Response {
        match request {
            Request::Write {
                correlation_id,
                org_id,
                aggregate_type_id,
                aggregate_id,
                client_id,
                user_id,
                events,
                allow_create,
                allow_repair_corruption: _,
                expected_event_batch_index,
                enforce_client_idempotency,
                durable_write_with_delay_us,
                compression_type,
            } => {
                let result = self.handle_write(
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id,
                    user_id,
                    events,
                    allow_create,
                    expected_event_batch_index,
                    enforce_client_idempotency,
                    durable_write_with_delay_us,
                    compression_type,
                ).await;

                match result {
                    Ok(append_result) => Response::WriteResult {
                        correlation_id,
                        error: None,
                        result: Some(append_result),
                    },
                    Err(e) => Response::WriteResult {
                        correlation_id,
                        error: Some(e),
                        result: None,
                    },
                }
            }

            Request::Read {
                correlation_id,
                org_id,
                aggregate_type_id,
                aggregate_id,
                filters,
            } => {
                let result = self.handle_read(
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    filters,
                ).await;

                match result {
                    Ok(read_result) => Response::ReadResult {
                        correlation_id,
                        error: None,
                        result: Some(read_result),
                    },
                    Err(e) => Response::ReadResult {
                        correlation_id,
                        error: Some(e),
                        result: None,
                    },
                }
            }

            Request::Exists {
                correlation_id,
                org_id,
                aggregate_type_id,
                aggregate_id,
            } => {
                let aggregate_key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
                let exists = self.get_or_create_reader(&aggregate_key).await.is_ok();

                Response::ExistsResult {
                    correlation_id,
                    error: None,
                    exists,
                }
            }

            Request::TrimStart {
                correlation_id,
                org_id,
                aggregate_type_id,
                aggregate_id,
                keep_from_event_batch_index,
            } => {
                let result = self.handle_trim_start(
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    keep_from_event_batch_index,
                ).await;

                match result {
                    Ok(()) => Response::TrimStartResult {
                        correlation_id,
                        error: None,
                    },
                    Err(e) => Response::TrimStartResult {
                        correlation_id,
                        error: Some(e),
                    },
                }
            }

            Request::Delete {
                correlation_id,
                org_id,
                aggregate_type_id,
                aggregate_id,
            } => {
                let result = self.handle_delete(org_id, aggregate_type_id, aggregate_id).await;

                match result {
                    Ok(()) => Response::DeleteResult {
                        correlation_id,
                        error: None,
                    },
                    Err(e) => Response::DeleteResult {
                        correlation_id,
                        error: Some(e),
                    },
                }
            }

            Request::ListOrganisations { correlation_id, filters: _ } => {
                Response::ListOrganisationsResult {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal("Not implemented")),
                    organisations: vec![],
                }
            }

            Request::ListAggregates { correlation_id, org_id: _, aggregate_type_id: _, filters: _ } => {
                Response::ListAggregatesResult {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal("Not implemented")),
                    aggregates: vec![],
                }
            }

            Request::Lock { correlation_id, org_id: _, aggregate_type_id: _, aggregate_id: _, client_id: _, timeout_ms: _, allow_read: _ } => {
                Response::LockResult {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal("Not implemented")),
                }
            }

            Request::Unlock { correlation_id, org_id: _, aggregate_type_id: _, aggregate_id: _ } => {
                Response::UnlockResult {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal("Not implemented")),
                }
            }

            Request::ReadAll { correlation_id, org_id: _, aggregate_type_id: _, aggregate_id: _, filters: _ } => {
                Response::ReadAllResult {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal("Not implemented")),
                    result: None,
                }
            }

            Request::WriteBatches { correlation_id, org_id: _, aggregate_type_id: _, aggregate_id: _, allow_create: _, allow_repair_corruption: _, durable_write_with_delay_us: _, batches: _ } => {
                Response::WriteBatchesResult {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal("Not implemented")),
                }
            }

            Request::TrimEnd { correlation_id, org_id: _, aggregate_type_id: _, aggregate_id: _, trim_from_event_batch_index: _ } => {
                Response::TrimEndResult {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal("Not implemented")),
                }
            }
        }
    }

    async fn handle_write(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        allow_create: bool,
        expected_event_batch_index: Option<u64>,
        enforce_client_idempotency: bool,
        durable_write_with_delay_us: Option<u64>,
        compression_type: CompressionType,
    ) -> Result<eventplanedb_structures::append_result::AppendResult, EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        let writer = self.get_or_create_writer(&aggregate_key, allow_create).await?;

        let server_timestamp_millis = Self::get_server_timestamp_millis();

        let append_result = {
            let mut wo = writer.write().await.unwrap();
            let append_options = AppendOptions {
                client_id,
                user_id,
                expected_event_batch_index,
                enforce_client_idempotency,
                server_timestamp_millis,
                compression_type,
            };
            wo.queue_events_in_memory(events, &append_options)?
        };

        // Handle durable write
        if let Some(delay_us) = durable_write_with_delay_us {
            let wal_sync_event = self.get_or_create_wal_sync_event(&aggregate_key).await;
            
            // Wait for write to disk and propagate error if it occurs
            sync_with_delay(&writer, &wal_sync_event, Duration::from_micros(delay_us)).await?;
        } else {
            // Spawn background sync without waiting
            let writer_clone = writer.clone();
            let wal_sync_event = self.get_or_create_wal_sync_event(&aggregate_key).await;
            let delay_us = 200; // Default delay

            spawn_local(async move {
                let sync_result = sync_with_delay(&writer_clone, &wal_sync_event, Duration::from_micros(delay_us)).await;
                if let Err(e) = sync_result {
                    error!("Background sync failed: {:?}", e);
                }
            }).detach();
        }

        Ok(append_result)
    }

    async fn handle_read(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: eventplanedb_structures::read_filters::ReadFilters,
    ) -> Result<ReadResult, EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        let writer = self.get_or_create_writer(&aggregate_key, false).await?;

        // Try cache first
        {
            let r_writer = writer.read().await.unwrap();
            if let Ok(result) = r_writer.maybe_read_cached_events(&filters) {
                return Ok(ReadResult {
                    event_batches: result.filtered_event_batches,
                    next_event_batch_index: result.next_event_batch_index,
                });
            }
        }

        // Cache miss, read from disk
        let reader = self.get_or_create_reader(&aggregate_key).await?;

        let read_result = {
            let r_reader = reader.read().await.unwrap();
            let r_writer = writer.read().await.unwrap();
            r_reader.read(
                r_writer.minimum_available_event_batch_index,
                r_writer.file_len_metadata(),
                r_writer.file_len_event_batch(),
                &filters,
            ).await?
        };

        // Update metadata cache if needed
        if !read_result.uncached_metadata_set.is_empty() {
            let mut w_reader = reader.write().await.unwrap();
            w_reader.update_metadata_cache(read_result.uncached_metadata_set);
        }

        Ok(ReadResult {
            event_batches: read_result.filtered_event_batches,
            next_event_batch_index: read_result.next_event_batch_index,
        })
    }

    async fn handle_trim_start(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
    ) -> Result<(), EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        let reader = self.get_or_create_reader(&aggregate_key).await?;
        let writer = self.get_or_create_writer(&aggregate_key, false).await?;
        let sem = self.get_or_create_semaphore(&aggregate_key).await;

        let _permit = sem.acquire_permit(1).await
            .map_err(|_| EventPlaneDBError::internal("Failed to acquire write lock for trim"))?;

        let (bytes_to_trim_metadata, bytes_to_trim_event_batch) = {
            let r_writer = writer.read().await.unwrap();
            reader.read().await.unwrap().get_file_positions(
                r_writer.minimum_available_event_batch_index,
                keep_from_event_batch_index,
                r_writer.file_len_metadata(),
                r_writer.file_len_event_batch(),
            ).await?
        };

        let (metadata_dma_file, event_batches_dma_file) = {
            let mut wo = writer.write().await.unwrap();
            wo.trim_start(bytes_to_trim_metadata, bytes_to_trim_event_batch).await
                .map_err(|e| EventPlaneDBError::write_error(format!("Failed to trim: {:?}", e)))?
        };

        let mut reader_mut = reader.write().await.unwrap();
        reader_mut.trim_start(metadata_dma_file, event_batches_dma_file);

        Ok(())
    }

    async fn handle_delete(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Result<(), EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);

        // Remove from all caches
        {
            let mut write_cache = self.write_operations.write().await.unwrap();
            let mut read_cache = self.read_operations.write().await.unwrap();
            let mut wal_events = self.wal_sync_events.write().await.unwrap();
            let mut sems = self.semaphores.write().await.unwrap();

            write_cache.remove(&aggregate_key);
            read_cache.remove(&aggregate_key);
            wal_events.remove(&aggregate_key);
            sems.remove(&aggregate_key);
        }

        // Delete files from filesystem
        let metadata_path = format!("data/{}/{}/{}/metadata.bin", org_id, aggregate_type_id, aggregate_id);
        let events_path = format!("data/{}/{}/{}/events.bin", org_id, aggregate_type_id, aggregate_id);

        std::fs::remove_file(&metadata_path)
            .map_err(|e| EventPlaneDBError::io_error(format!("Failed to delete metadata file: {}", e)))?;
        std::fs::remove_file(&events_path)
            .map_err(|e| EventPlaneDBError::io_error(format!("Failed to delete events file: {}", e)))?;

        Ok(())
    }
}

async fn sync_with_delay(
    write_operations: &Rc<RwLock<WriteOperations>>, 
    wal_sync_event: &RwLock<Option<Rc<LocalEvent<SyncResult>>>>, 
    wal_sync_delay: Duration
) -> SyncResult {
    // Try to become the sync coordinator
    match wal_sync_event.try_write() {
        Ok(mut maybe_event) => {
            // We won! Check if sync is already in progress
            if let Some(event) = maybe_event.as_ref() {
                // Another task beat us between our check and lock acquisition
                let event = event.clone();
                drop(maybe_event); // Release lock before awaiting
                return event.listen().await;
            }
            
            // We're the coordinator - create the event
            let event = Rc::new(LocalEvent::new());
            *maybe_event = Some(event.clone());
            drop(maybe_event); // Release lock while sleeping
            
            // Sleep for the delay period
            sleep(wal_sync_delay).await;
            
            // Clear the event before sync (need write lock again)
            wal_sync_event.write().await.unwrap().take();
            
            // Do the actual sync
            let sync_result = {
                let mut write_operations = write_operations.write().await.unwrap();
                write_operations.sync_with_rollback().await
                    .map_err(|e| EventPlaneDBError::write_error(format!("Failed to sync events to disk: {:?}", e)))
            };
            
            // Notify all waiters
            event.notify(sync_result.clone());
            
            sync_result
        }
        
        Err(_) => {
            // Another task is coordinating the sync - just wait for it
            let event = {
                let maybe_event = wal_sync_event.read().await.unwrap();
                maybe_event.as_ref()
                    .expect("If try_write failed, event should exist")
                    .clone()
            };
            
            event.listen().await
        }
    }
}