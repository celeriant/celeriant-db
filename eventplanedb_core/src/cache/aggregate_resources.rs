use std::rc::Rc;

use eventplanedb_structures::aggregate_key::AggregateKey;
use glommio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, Semaphore};

use crate::{
    local_event::LocalEvent,
    read_operations::{
        read_error::ReadError,
        read_operations::{ReadOperations, ReadOperationsWithDmaFiles},
        read_structures::AggregateReadConfig,
    },
    sync_result::SyncResult,
    write_operations::{
        write_operations::WriteOperationsWithDmaFile, write_structures::AggregateWriteConfig,
    },
};

pub struct AggregateResources {
    aggregate_key: AggregateKey,
    base_folder: String,
    path_metadata: String,
    path_event_batches: String,
    aggregate_read_config: AggregateReadConfig,
    aggregate_write_config: AggregateWriteConfig,
    reader: RwLock<Option<ReadOperationsWithDmaFiles>>,
    writer: RwLock<Option<WriteOperationsWithDmaFile>>,
    semaphore: Rc<Semaphore>,
    wal_sync_event: Rc<RwLock<Option<Rc<LocalEvent<SyncResult>>>>>,
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
            aggregate_key,
            aggregate_read_config,
            aggregate_write_config,
            base_folder,
            path_metadata,
            path_event_batches,
            reader: RwLock::new(None),
            writer: RwLock::new(None),
            semaphore: Rc::new(Semaphore::new(1)),
            wal_sync_event: Rc::new(RwLock::new(None)),
        }
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

        // Acquire write lock to initialize
        let mut writer_guard = self.reader.write().await?;

        // Double-check in case another task initialized it while we were waiting
        if writer_guard.is_some() {
            drop(writer_guard);
            return self.reader.read().await.map_err(Into::into);
        }

        // Initialize the reader
        let read_operations = ReadOperationsWithDmaFiles::open(
            &self.base_folder,
            &self.path_metadata,
            &self.path_event_batches,
            create_if_not_exists,
            self.aggregate_read_config.clone(),
        )
        .await?;

        *writer_guard = Some(read_operations);
        drop(writer_guard);

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

        // Acquire write lock to initialize
        let mut writer_guard = self.writer.write().await?;

        // Double-check in case another task initialized it while we were waiting
        if writer_guard.is_some() {
            drop(writer_guard);
            return self.writer.read().await.map_err(Into::into);
        }

        let uncached_metadata_set = {
            let reader_guard = self.get_reader(create_if_not_exists).await?;
            let reader = reader_guard.as_ref().unwrap();
            let data_requirements = reader.get_write_operations_data_requirements().await?;

            let writer_operations = WriteOperationsWithDmaFile::open(
                reader.metadata_dma_file.dup()?,
                reader.event_batches_dma_file.dup()?,
                data_requirements.write_operations_data_requirements,
                self.aggregate_write_config.clone(),
            )?;

            *writer_guard = Some(writer_operations);

            data_requirements.uncached_metadata_set
        };

        if !uncached_metadata_set.is_empty() {
            let mut reader_write_guard = self.reader.write().await?;
            if let Some(reader_mut) = reader_write_guard.as_mut() {
                reader_mut.update_metadata_cache(uncached_metadata_set);
            }
        }

        drop(writer_guard);

        self.writer.read().await.map_err(Into::into)
    }

    pub async fn get_reader_mut(
        &self,
        create_if_not_exists: bool,
    ) -> Result<RwLockWriteGuard<'_, Option<ReadOperationsWithDmaFiles>>, ReadError> {
        let mut writer_guard = self.reader.write().await?;

        if writer_guard.is_none() {
            let read_operations = ReadOperationsWithDmaFiles::open(
                &self.base_folder,
                &self.path_metadata,
                &self.path_event_batches,
                create_if_not_exists,
                self.aggregate_read_config.clone(),
            )
            .await?;

            *writer_guard = Some(read_operations);
        }

        Ok(writer_guard)
    }

    pub async fn get_writer_mut(
        &self,
        create_if_not_exists: bool,
    ) -> Result<RwLockWriteGuard<'_, Option<WriteOperationsWithDmaFile>>, ReadError> {
        let mut writer_guard = self.writer.write().await?;

        if writer_guard.is_none() {
            let uncached_metadata_set = {
                let reader_guard = self.get_reader(create_if_not_exists).await?;
                let reader = reader_guard.as_ref().unwrap();
                let data_requirements = reader.get_write_operations_data_requirements().await?;

                let writer_operations = WriteOperationsWithDmaFile::open(
                    reader.metadata_dma_file.dup()?,
                    reader.event_batches_dma_file.dup()?,
                    data_requirements.write_operations_data_requirements,
                    self.aggregate_write_config.clone(),
                )?;

                *writer_guard = Some(writer_operations);

                data_requirements.uncached_metadata_set
            };

            if !uncached_metadata_set.is_empty() {
                let mut reader_write_guard = self.reader.write().await?;
                if let Some(reader_mut) = reader_write_guard.as_mut() {
                    reader_mut.update_metadata_cache(uncached_metadata_set);
                }
            }
        }

        Ok(writer_guard)
    }
}
