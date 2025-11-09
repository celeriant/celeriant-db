use std::{collections::HashMap, rc::Rc, time::Duration};

use eventplanedb_core::{files::{helper::{get_or_create_reader, get_or_create_writer}, read_operations::{AggregateReadConfig, ReadOperations}, write_operations::{AggregateWriteConfig, AppendOptions, WriteOperations}}, local_event::LocalEvent};
use glommio::{spawn_local, sync::{RwLock, Semaphore}, timer::sleep};
use log::error;
use eventplanedb_structures::{aggregate_key::AggregateKey, eventplanedb_error::EventPlaneDBError, read_result::ReadResult, request::{DeleteRequest, ReadRequest, Request, TrimStartRequest, WriteBatchesRequest, WriteRequest}, response::{DeleteResponse, ExistsResponse, ListAggregatesResponse, ListOrganisationsResponse, ReadAllResponse, ReadResponse, Response, TrimStartResponse, WriteBatchesResponse, WriteResponse}};

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
        .map_err(|_e| EventPlaneDBError::io_error())
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
                let result = self.handle_read(request).await;

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
                let aggregate_key = AggregateKey::new(request.org_id, request.aggregate_type_id, request.aggregate_id);
                let exists = self.get_or_create_reader(&aggregate_key).await.is_ok();

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
                Response::ListOrganisations(ListOrganisationsResponse {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal()),
                    organisations: vec![],
                })
            }

            Request::ListAggregates(request) => {
                let correlation_id = request.correlation_id;
                Response::ListAggregates(ListAggregatesResponse {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal()),
                    aggregates: vec![],
                })
            }

            Request::ReadAll(request) => {
                let correlation_id = request.correlation_id;
                Response::ReadAll(ReadAllResponse {
                    correlation_id,
                    error: Some(EventPlaneDBError::internal()),
                    result: None,
                })
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

    async fn handle_write_batches(
        &self,
        request: WriteBatchesRequest,
    ) -> Result<(), EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(request.org_id, request.aggregate_type_id, request.aggregate_id);
        let writer = self.get_or_create_writer(&aggregate_key, request.allow_create).await?;

        let mut wo = writer.write().await.unwrap();
        wo.prepend_batches(&request.batches).await?;

        Ok(())
    }

    async fn handle_write(
        &self,
        request: WriteRequest,
    ) -> Result<eventplanedb_structures::append_result::AppendResult, EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(request.org_id, request.aggregate_type_id, request.aggregate_id);
        let writer = self.get_or_create_writer(&aggregate_key, request.allow_create).await?;

        let server_timestamp_millis = Self::get_server_timestamp_millis();

        let append_result = {
            let mut wo = writer.write().await.unwrap();
            let append_options = AppendOptions {
                client_id: request.client_id,
                user_id: request.user_id,
                expected_event_batch_index: request.expected_event_batch_index,
                enforce_client_idempotency: request.enforce_client_idempotency,
                server_timestamp_millis,
                compression_type: request.compression_type,
            };
            wo.queue_events_in_memory(request.events, &append_options)?
        };

        // Handle durable write
        if let Some(delay_us) = request.durable_write_with_delay_us {
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
        request: ReadRequest,
    ) -> Result<ReadResult, EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(request.org_id, request.aggregate_type_id, request.aggregate_id);
        let writer = self.get_or_create_writer(&aggregate_key, false).await?;

        // Try cache first
        {
            let r_writer = writer.read().await.unwrap();
            if let Ok(result) = r_writer.maybe_read_cached_events(&request.filters) {
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
                &request.filters,
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
        request: TrimStartRequest
    ) -> Result<(), EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(request.org_id, request.aggregate_type_id, request.aggregate_id);
        let reader = self.get_or_create_reader(&aggregate_key).await?;
        let writer = self.get_or_create_writer(&aggregate_key, false).await?;
        let sem = self.get_or_create_semaphore(&aggregate_key).await;

        let _permit = sem.acquire_permit(1).await
            .map_err(|_| EventPlaneDBError::internal())?;

        let (bytes_to_trim_metadata, bytes_to_trim_event_batch) = {
            let r_writer = writer.read().await.unwrap();
            reader.read().await.unwrap().get_file_positions(
                r_writer.minimum_available_event_batch_index,
                request.keep_from_event_batch_index,
                r_writer.file_len_metadata(),
                r_writer.file_len_event_batch(),
            ).await?
        };

        let (metadata_dma_file, event_batches_dma_file) = {
            let mut wo = writer.write().await.unwrap();
            wo.trim_start(bytes_to_trim_metadata, bytes_to_trim_event_batch).await
                .map_err(|_e| EventPlaneDBError::write_error())?
        };

        let mut reader_mut = reader.write().await.unwrap();
        reader_mut.trim_start(metadata_dma_file, event_batches_dma_file);

        Ok(())
    }

    async fn handle_delete(
        &self,
        request: DeleteRequest,
    ) -> Result<(), EventPlaneDBError> {
        let aggregate_key = AggregateKey::new(request.org_id, request.aggregate_type_id, request.aggregate_id);

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
        //TODO: Configurable settings required
        let metadata_path = format!("data/{}/{}/{}/metadata.bin", request.org_id, request.aggregate_type_id, request.aggregate_id);
        let events_path = format!("data/{}/{}/{}/event_batches.bin", request.org_id, request.aggregate_type_id, request.aggregate_id);

        std::fs::remove_file(&metadata_path)
            .map_err(|_e| EventPlaneDBError::io_error())?;
        std::fs::remove_file(&events_path)
            .map_err(|_e| EventPlaneDBError::io_error())?;

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
                    .map_err(|_e| EventPlaneDBError::write_error())
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