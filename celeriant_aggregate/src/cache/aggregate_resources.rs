use std::ops::{Deref, DerefMut};
use celeriant_disk::files::open_dma_files::{
    create_and_write_only_dma, existing_file_read_only_dma, existing_file_write_only_dma,
};
use celeriant_wal::aggregate_key::AggregateKey;
use glommio::{
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    timer::sleep,
};
use std::{cell::Cell, rc::Rc, time::Duration};

use crate::{
    local_event::LocalEvent,
    read_operations::{
        read_error::ReadError,
        read_operations::{ReadOperations, ReadOperationsWithDmaFiles},
        read_structures::AggregateReadConfig,
    },
    read_write_error::ReadWriteError,
    write_operations::{
        write_operations::{WriteOperations, WriteOperationsWithDmaFile},
        aggregate_write_config::AggregateWriteConfig,
    },
};

pub type SyncResult = Result<(), ReadWriteError>;

pub struct AggregateResources {
    pub base_folder: String,
    pub path_metadata: String,
    pub path_event_batches: String,
    aggregate_read_config: AggregateReadConfig,
    aggregate_write_config: AggregateWriteConfig,
    reader: RwLock<Option<ReadOperationsWithDmaFiles>>,
    writer: RwLock<Option<WriteOperationsWithDmaFile>>,
    wal_sync_event: RwLock<Option<Rc<LocalEvent<SyncResult>>>>,
    /// Tracks if a previous async sync failed - forces next write to be durable
    has_pending_sync_error: Cell<bool>,
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
            wal_sync_event: RwLock::new(None),
            has_pending_sync_error: Cell::new(false),
        }
    }

    /// Check if there's a pending sync error from a previous async flush
    pub fn has_pending_sync_error(&self) -> bool {
        self.has_pending_sync_error.get()
    }

    /// Mark that an async sync failed
    pub fn set_pending_sync_error(&self) {
        self.has_pending_sync_error.set(true);
    }

    /// Clear the pending sync error flag after successful durable write
    pub fn clear_pending_sync_error(&self) {
        self.has_pending_sync_error.set(false);
    }

    async fn internal_init(&self, create_if_not_exists: bool) -> Result<(), ReadError> {
        // If the reader is setup, we're done!
        {
            let reader = self.reader.read().await?;
            if reader.is_some() {
                return Ok(());
            }
        }

        // Don't create files and setup cache if they don't exist
        let is_metadata_exists = std::path::Path::new(&self.path_metadata).exists();
        let is_event_batch_exists = std::path::Path::new(&self.path_event_batches).exists();

        if !create_if_not_exists && (!is_metadata_exists || !is_event_batch_exists) {
            return Err(ReadError::NotExists);
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
        let writer_metadata_dma_file = if !is_metadata_exists {
            create_and_write_only_dma(&self.path_metadata).await?
        } else {
            existing_file_write_only_dma(&self.path_metadata).await?
        };
        let reader_metadata_dma_file = existing_file_read_only_dma(&self.path_metadata).await?;
        let writer_event_batch_dma_file = if !is_event_batch_exists {
            create_and_write_only_dma(&self.path_event_batches).await?
        } else {
            existing_file_write_only_dma(&self.path_event_batches).await?
        };
        let reader_event_batch_dma_file =
            existing_file_read_only_dma(&self.path_event_batches).await?;

        let read_operations = ReadOperationsWithDmaFiles::new(
            reader_metadata_dma_file,
            reader_event_batch_dma_file,
            self.aggregate_read_config.clone(),
        );
        let data_requirements = read_operations
            .get_write_operations_data_requirements()
            .await?;
        let write_operations = WriteOperationsWithDmaFile::new(
            writer_metadata_dma_file,
            writer_event_batch_dma_file,
            data_requirements,
            self.aggregate_write_config.clone(),
        );

        *guard_reader = Some(read_operations);
        *guard_writer = Some(write_operations);

        Ok(())
    }

    pub async fn get_reader(&self, create: bool) -> Result<ReaderGuard<'_>, ReadError> {
        {
            let guard = self.reader.read().await?;
            if guard.is_some() {
                return Ok(ReaderGuard { guard });
            }
        }

        self.internal_init(create).await?;

        Ok(ReaderGuard {
            guard: self.reader.read().await?,
        })
    }

    pub async fn get_reader_mut(&self, create: bool) -> Result<ReaderGuardMut<'_>, ReadError> {
        {
            let guard = self.reader.write().await?;
            if guard.is_some() {
                return Ok(ReaderGuardMut { guard });
            }
        }

        self.internal_init(create).await?;

        Ok(ReaderGuardMut {
            guard: self.reader.write().await?,
        })
    }

    pub async fn get_writer(&self, create: bool) -> Result<WriterGuard<'_>, ReadError> {
        {
            let guard = self.writer.read().await?;
            if guard.is_some() {
                return Ok(WriterGuard { guard });
            }
        }

        self.internal_init(create).await?;

        Ok(WriterGuard {
            guard: self.writer.read().await?,
        })
    }

    pub async fn get_writer_mut(&self, create: bool) -> Result<WriterGuardMut<'_>, ReadError> {
        {
            let guard = self.writer.write().await?;
            if guard.is_some() {
                return Ok(WriterGuardMut { guard });
            }
        }

        self.internal_init(create).await?;

        Ok(WriterGuardMut {
            guard: self.writer.write().await?,
        })
    }

    pub async fn sync_with_delay(&self, wal_sync_delay: Option<Duration>) -> SyncResult {
        if wal_sync_delay.is_none() || wal_sync_delay.unwrap().as_micros() == 0 {
            // No delay - do immediate sync
            let mut writer = self.get_writer_mut(false).await?;
            return Ok(writer.sync_with_rollback().await?);
        }

        let wal_sync_delay = wal_sync_delay.unwrap();

        loop {
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
                        writer.sync_with_rollback().await?
                    };

                    // Notify all waiters
                    event.notify(Ok(sync_result.clone()));

                    return Ok(sync_result);
                }
                Err(_) => {
                    let maybe_event = self.wal_sync_event.read().await.unwrap();
                    if let Some(event) = maybe_event.as_ref() {
                        let event = event.clone();
                        drop(maybe_event);
                        return event.listen().await;
                    }
                    // Retry - coordinator cleared event while we were waiting
                    drop(maybe_event);
                    continue;
                }
            }
        }
        
    }
}

pub struct WriterGuardMut<'a> {
    guard: RwLockWriteGuard<'a, Option<WriteOperationsWithDmaFile>>,
}

impl Deref for WriterGuardMut<'_> {
    type Target = WriteOperationsWithDmaFile;
    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap()
    }
}

impl DerefMut for WriterGuardMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().unwrap()
    }
}

pub struct WriterGuard<'a> {
    guard: RwLockReadGuard<'a, Option<WriteOperationsWithDmaFile>>,
}

impl Deref for WriterGuard<'_> {
    type Target = WriteOperationsWithDmaFile;
    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap()
    }
}

pub struct ReaderGuardMut<'a> {
    guard: RwLockWriteGuard<'a, Option<ReadOperationsWithDmaFiles>>,
}

impl Deref for ReaderGuardMut<'_> {
    type Target = ReadOperationsWithDmaFiles;
    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap()
    }
}

impl DerefMut for ReaderGuardMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().unwrap()
    }
}

pub struct ReaderGuard<'a> {
    guard: RwLockReadGuard<'a, Option<ReadOperationsWithDmaFiles>>,
}

impl Deref for ReaderGuard<'_> {
    type Target = ReadOperationsWithDmaFiles;
    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap()
    }
}