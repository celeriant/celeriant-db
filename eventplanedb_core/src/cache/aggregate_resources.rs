use std::rc::Rc;

use eventplanedb_structures::aggregate_key::AggregateKey;
use glommio::sync::{RwLock, Semaphore};

use crate::{
    local_event::LocalEvent,
    read_operations::{
        read_error::ReadError, read_operations::{ReadOperations, ReadOperationsWithDmaFiles},
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
    reader: Rc<RwLock<Option<Rc<ReadOperationsWithDmaFiles>>>>,
    writer: Rc<RwLock<Option<Rc<WriteOperationsWithDmaFile>>>>,
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
            reader: Rc::new(RwLock::new(None)),
            writer: Rc::new(RwLock::new(None)),
            semaphore: Rc::new(Semaphore::new(1)),
            wal_sync_event: Rc::new(RwLock::new(None)),
        }
    }

    pub async fn get_reader(&self) -> Result<Rc<ReadOperationsWithDmaFiles>, ReadError> {
        // First check with read lock
        {
            let reader = self.reader.read().await?;
            if let Some(reader) = reader.as_ref() {
                return Ok(Rc::clone(reader));
            }
        }

        // Acquire write lock to initialize
        let mut reader = self.reader.write().await?;

        // Double-check in case another task initialized it while we were waiting
        if let Some(reader) = reader.as_ref() {
            return Ok(Rc::clone(reader));
        }

        // Initialize the reader
        let read_operations = ReadOperationsWithDmaFiles::open(
            &self.path_metadata,
            &self.path_event_batches,
            self.aggregate_read_config.clone(),
        )
        .await?;

        let read_operations_rc = Rc::new(read_operations);
        *reader = Some(Rc::clone(&read_operations_rc));

        Ok(read_operations_rc)
    }

    pub async fn get_writer(&self) -> Result<Rc<WriteOperationsWithDmaFile>, ReadError> {
        // First check with read lock
        {
            let writer = self.writer.read().await?;
            if let Some(writer) = writer.as_ref() {
                return Ok(Rc::clone(writer));
            }
        }

        // Acquire write lock to initialize
        let mut writer = self.writer.write().await?;

        // Double-check in case another task initialized it while we were waiting
        if let Some(writer) = writer.as_ref() {
            return Ok(Rc::clone(writer));
        }

        let reader = self.get_reader().await?;
        let data_requirements = reader.get_write_operations_data_requirements().await?;

        if !data_requirements.uncached_metadata_set.is_empty() {
            let mut borrow = reader_ref.write().await?;
            borrow.update_metadata_cache(data_requirements.uncached_metadata_set);
        }

        // Initialize the reader
        let writer_operations = WriteOperationsWithDmaFile::open(
            reader.metadata_dma_file.dup()?,
            reader.event_batches_dma_file.dup()?,
            data_requirements.write_operations_data_requirements,
            self.aggregate_write_config.clone(),
        )?;

        let writer_operations_rc = Rc::new(writer_operations);
        *writer = Some(Rc::clone(&writer_operations_rc));

        Ok(writer_operations_rc)
    }
}
