use std::{collections::HashMap, path::Path, rc::Rc};

use bincode::{Decode, Encode};
use eventplanedb_structures::{append_result::AppendResult, constants::METADATA_BATCH_SIZE_BYTES, event_batch_item::EventBatchItem, event_batch_metadata::EventBatchMetadata, event_item::EventItem, read_filters::ReadFilters};
use glommio::{io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions}, GlommioError};
use serde::{Deserialize, Serialize};

pub struct BatchMetadataItemPair {
    pub metadata: EventBatchMetadata,
    pub item: EventBatchItem,
}

#[derive(Debug)]
pub enum AppendError {
    IoError(GlommioError<()>),
    NoEventsToAppend {
        client_id: u128,
        existing_event_index: u64,
    },
}

#[derive(Debug)]
pub enum CacheReadError {
    // Cache data should be contiguous and always present at the latest write
    // So cache miss can only occur for older event batch index ranges
    CacheMiss {
        missing_from_event_batch_index: u64,        
        missing_to_event_batch_index: u64,
    },
}

impl From<GlommioError<()>> for AppendError {
    fn from(error: GlommioError<()>) -> Self {
        AppendError::IoError(error)
    }
}

pub struct AggregateWriteConfig {
    pub event_batches_buffer_size_bytes: usize,
    pub event_batches_write_behind_count: usize,
    pub metadata_write_behind_count: usize,
    pub max_data_cache_size_bytes: usize,
}

pub struct AppendOptions {
    pub client_id: u128,
    pub user_id: Option<u128>,
    pub expected_event_batch_index: Option<u64>,
    pub enforce_client_idempotency: bool,
    pub server_timestamp_millis: u64,
}

pub struct WriteOperations {
    metadata_stream_writer: DmaStreamWriter,
    event_batches_stream_writer: DmaStreamWriter,
    data_cache: Vec<BatchMetadataItemPair>,
    next_event_index: u64,
    next_event_batch_index: u64,
    client_event_indexes: HashMap<u128, u64>,
    max_data_cache_size_bytes: usize,
    write_semaphore: Rc<glommio::sync::Semaphore>,
}

async fn append_only_file<P: AsRef<Path>>(path: P) -> Result<DmaFile, GlommioError<()>> {
    let dma_file = OpenOptions::new()
        .read(false)
        .write(false)
        .create(false)
        .append(true)
        .dma_open(path)
        .await?;

    Ok(dma_file)
}

fn stream_writer(dma_file: DmaFile, buffer_size: usize, write_behind: usize) -> DmaStreamWriter {
    DmaStreamWriterBuilder::new(dma_file)
        .with_buffer_size(buffer_size)
        .with_write_behind(write_behind)
        .build()
}

/// Allows appending new events for an aggregate. Note this doesn't handle fdatasync.
/// Also caches recent events and indexes in memory for fast read access
/// If cached read fails, you should fall back to the AggregateReadFileOperations struct
/// This struct never reads from disk, only appends. So it requires cache data on initialization.
impl WriteOperations {

    pub async fn open<P: AsRef<Path>>(
        path_metadata: P, 
        path_event_batches: P, 
        data_cache: Vec<BatchMetadataItemPair>, 
        next_event_index: u64, 
        next_event_batch_index: u64, 
        client_event_indexes: HashMap<u128, u64>,
        aggregate_write_config: AggregateWriteConfig,
        ) -> Result<WriteOperations, GlommioError<()>> {

        let metadata_dma_file = append_only_file(path_metadata).await?;
        let metadata_stream_writer = stream_writer(
            metadata_dma_file, METADATA_BATCH_SIZE_BYTES, aggregate_write_config.metadata_write_behind_count);

        let event_batches_dma_file = append_only_file(path_event_batches).await?;
        let event_batches_stream_writer = stream_writer(
            event_batches_dma_file, aggregate_write_config.event_batches_buffer_size_bytes, aggregate_write_config.event_batches_write_behind_count);

        Ok(WriteOperations {
            metadata_stream_writer, 
            event_batches_stream_writer, 
            data_cache, 
            next_event_batch_index, 
            next_event_index, 
            client_event_indexes,
            max_data_cache_size_bytes: aggregate_write_config.max_data_cache_size_bytes,
            write_semaphore: Rc::new(glommio::sync::Semaphore::new(1)),
        })
    }

    pub fn clone_write_semaphore(&self) -> Rc<glommio::sync::Semaphore> {
        self.write_semaphore.clone()
    }

    pub async fn sync(&mut self) -> Result<(), GlommioError<()>> {
        self.metadata_stream_writer.sync().await?;
        self.event_batches_stream_writer.sync().await?;

        Ok(())
    }

    /// We require the ownership of the events to be transferred, as they will be stored in the in-memory cache
    /// The events are also mutable as we need to filter out events for client idempotency requirements
    pub async fn append_events(&mut self, mut events: Vec<EventItem>, append_options: &AppendOptions) -> Result<AppendResult, AppendError> {
        // Requires mutable write buffer access

        // Requires mutable access to cache data (events, indexes)

        Ok(AppendResult {
            event_indexes: vec![],
            event_batch_index: 0,
            events_written: 0,
        })
    }

    pub fn maybe_read_cached_events(&self, filters: &ReadFilters) -> Result<Vec<EventItem>, CacheReadError> {
        // Not async, doesn't require mutable access as it just reads from in-memory cache

        // Will create a new vec with items that are copied from the cache

        Ok(vec![])
    }
}

//Some tests
#[cfg(test)]
mod tests {
    use glommio::{LocalExecutorBuilder, Placement};

    use super::*;

    fn write_config() -> AggregateWriteConfig {
        AggregateWriteConfig {
            event_batches_buffer_size_bytes: 1024 * 1024,
            event_batches_write_behind_count: 4,
            metadata_write_behind_count: 4,
            max_data_cache_size_bytes: 10 * 1024 * 1024,
        }
    }

    fn create_files(folder: &str) {
        let metadata_path = format!("{}/metadata.bin", folder);
        let event_batches_path = format!("{}/event_batches.bin", folder);
        std::fs::File::create(metadata_path).unwrap();
        std::fs::File::create(event_batches_path).unwrap();
    }

    async fn empty_aggregate_write_file_operations(folder: &str) -> Result<WriteOperations, GlommioError<()>> {
       let service = WriteOperations::open(
        format!("{}/metadata.bin", folder),
        format!("{}/event_batches.bin", folder),
            vec![],
            0,
            0,
            HashMap::new(),
            write_config(),
        ).await?;

        Ok(service)
    }

    #[test]
    fn test_fail_if_files_not_exist() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn( || async move {

            let tempdir = tempfile::tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();
            let result = empty_aggregate_write_file_operations(folder).await;
            assert!(result.is_err());

        }).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_open_existing_files() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn( || async move {

            let tempdir = tempfile::tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();
            create_files(folder);
            let result = empty_aggregate_write_file_operations(folder).await;
            assert!(result.is_ok());

        }).unwrap();
        handle.join().unwrap();
    }
}