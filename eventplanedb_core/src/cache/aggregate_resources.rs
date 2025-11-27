use std::{rc::Rc, time::Duration};

use eventplanedb_structures::{aggregate_key::AggregateKey, eventplanedb_error::EventPlaneDBError};
use glommio::{sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, Semaphore}, timer::sleep};

use crate::{
    files::open_dma_files::{read_only_dma, write_only_dma}, local_event::LocalEvent, read_operations::{
        self, read_error::ReadError, read_operations::{ReadOperations, ReadOperationsWithDmaFiles}, read_structures::AggregateReadConfig
    }, sync_result::SyncResult, write_operations::{
        self, write_operations::{WriteOperations, WriteOperationsWithDmaFile}, write_structures::AggregateWriteConfig
    }
};

pub struct AggregateResources {
    base_folder: String,
    path_metadata: String,
    path_event_batches: String,
    aggregate_read_config: AggregateReadConfig,
    aggregate_write_config: AggregateWriteConfig,
    reader: RwLock<Option<ReadOperationsWithDmaFiles>>,
    writer: RwLock<Option<WriteOperationsWithDmaFile>>,
    _semaphore: Rc<Semaphore>,
    wal_sync_event: RwLock<Option<Rc<LocalEvent<SyncResult>>>>,
}

impl AggregateResources {
    pub fn new(
        aggregate_key: AggregateKey,
        data_root_folder: &str,
        aggregate_read_config: AggregateReadConfig,
        aggregate_write_config: AggregateWriteConfig,
    ) -> Self {
        let base_folder = format!(
            "{}/{}/{}/{}",
            data_root_folder,
            aggregate_key.org_id,
            aggregate_key.aggregate_type_id,
            aggregate_key.aggregate_id
        );
        let path_metadata = format!("{}/metadata.bin", base_folder);
        let path_event_batches = format!("{}/event_batches.bin", base_folder);

        Self {
            aggregate_read_config,
            aggregate_write_config,
            base_folder,
            path_metadata,
            path_event_batches,
            reader: RwLock::new(None),
            writer: RwLock::new(None),
            _semaphore: Rc::new(Semaphore::new(1)),
            wal_sync_event: RwLock::new(None),
        }
    }

    async fn internal_init(
        &self,
        create_if_not_exists: bool,
    ) -> Result<(), ReadError> {
        
        // If the reader is setup, we're done!
        {
            let reader = self.reader.read().await?;
            if reader.is_some() {
                return Ok(());
            }
        }

        // Don't create files and setup cache if they don't exist
        if !create_if_not_exists && !std::path::Path::new(&self.path_metadata).exists() {
            return Ok(());
        }

        // Acquire write locks to initialize
        let mut guard_writer = self.writer.write().await?;
        let mut guard_reader = self.reader.write().await?;

        // Double-check in case another task initialized while we were waiting
        if guard_reader.is_some() {
            return Ok(());
        }

        // Create base folder if needed
        std::fs::create_dir_all(&self.base_folder)?;

        // Open DMA files - must be done in this order due to direct I/O fs constraints
        let writer_metadata_dma_file = write_only_dma(&self.path_metadata).await.unwrap();
        let reader_metadata_dma_file = read_only_dma(&self.path_metadata).await.unwrap();
        let writer_event_batch_dma_file = write_only_dma(&self.path_event_batches).await.unwrap();
        let reader_event_batch_dma_file = read_only_dma(&self.path_event_batches).await.unwrap();

        let read_operations = ReadOperationsWithDmaFiles::new(
            reader_metadata_dma_file, reader_event_batch_dma_file, self.aggregate_read_config.clone());
        let data_requirements = read_operations.get_write_operations_data_requirements().await?;
        let write_operations = WriteOperationsWithDmaFile::new(
            writer_metadata_dma_file, writer_event_batch_dma_file, data_requirements,
            self.aggregate_write_config.clone(),
        );

        *guard_reader = Some(read_operations);
        *guard_writer = Some(write_operations);

        Ok(())
    }

    pub async fn get_reader(
        &self,
        create_if_not_exists: bool,
    ) -> Result<RwLockReadGuard<'_, Option<ReadOperationsWithDmaFiles>>, ReadError> {
        // First check with read lock
        {
            let reader = self.reader.read().await?;
            if reader.is_some() {
                return Ok(reader);
            }
        }

        self.internal_init(create_if_not_exists).await?;

        self.reader.read().await.map_err(Into::into)
    }

    pub async fn get_writer(
        &self,
        create_if_not_exists: bool,
    ) -> Result<RwLockReadGuard<'_, Option<WriteOperationsWithDmaFile>>, ReadError> {
        // First check with read lock
        {
            let writer = self.writer.read().await?;
            if writer.is_some() {
                return Ok(writer);
            }
        }

        self.internal_init(create_if_not_exists).await?;

        self.writer.read().await.map_err(Into::into)
    }

    pub async fn get_reader_mut(
        &self,
        create_if_not_exists: bool,
    ) -> Result<RwLockWriteGuard<'_, Option<ReadOperationsWithDmaFiles>>, ReadError> {
        {
            let writer_guard = self.reader.write().await?;
            if writer_guard.is_some() {
                return Ok(writer_guard);
            }
        }

        self.internal_init(create_if_not_exists).await?;

        self.reader.write().await.map_err(Into::into)
    }

    pub async fn get_writer_mut(
        &self,
        create_if_not_exists: bool,
    ) -> Result<RwLockWriteGuard<'_, Option<WriteOperationsWithDmaFile>>, ReadError> {
        {
            let writer_guard = self.writer.write().await?;
            if writer_guard.is_some() {
                return Ok(writer_guard);
            }
        }

        self.internal_init(create_if_not_exists).await?;

        self.writer.write().await.map_err(Into::into)
    }

    pub async fn sync_with_delay(
        &self,
        wal_sync_delay: Option<Duration>
    ) -> SyncResult {

        if wal_sync_delay.is_none() || wal_sync_delay.unwrap().as_micros() == 0 {
            // No delay - do immediate sync
            let mut writer = self.get_writer_mut(false).await?;
            let r_writer = writer.as_mut().unwrap();
            return r_writer.sync_with_rollback().await
                .map_err(|_e| EventPlaneDBError::write_error());
        }

        let wal_sync_delay = wal_sync_delay.unwrap();

        // Try to become the sync coordinator
        match self.wal_sync_event.try_write() {
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
                self.wal_sync_event.write().await.unwrap().take();
                
                // Do the actual sync
                let sync_result = {
                    let mut writer = self.get_writer_mut(false).await?;
                    let r_writer = writer.as_mut().unwrap();
                    r_writer.sync_with_rollback().await
                        .map_err(|_e| EventPlaneDBError::write_error())
                };
                
                // Notify all waiters
                event.notify(sync_result.clone());
                
                sync_result
            }
            
            Err(_) => {
                // Another task is coordinating the sync - just wait for it
                let event = {
                    let maybe_event = self.wal_sync_event.read().await.unwrap();
                    maybe_event.as_ref()
                        .expect("If try_write failed, event should exist")
                        .clone()
                };
                
                event.listen().await
            }
        }
    }
}
