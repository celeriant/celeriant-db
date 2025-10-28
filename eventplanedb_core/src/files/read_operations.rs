use std::path::Path;

use eventplanedb_structures::read_filters::ReadFilters;
use glommio::{GlommioError, io::{DmaFile, OpenOptions}};

use crate::files::{read_objects, write_operations::BatchMetadataItemPair};

#[derive(Debug)]
pub enum ReadError {
    IoError(GlommioError<()>),
    SerializationError {
        message: String,
    },
}

impl From<GlommioError<()>> for ReadError {
    fn from(error: GlommioError<()>) -> Self {
        ReadError::IoError(error)
    }
}

pub struct AggregateReadConfig {
    pub max_data_cache_size_bytes: usize,
}

pub struct ReadOperations {
    metadata_dma_file: DmaFile,
    event_batches_dma_file: DmaFile,
    data_cache: Vec<BatchMetadataItemPair>,
    config: AggregateReadConfig,
}

async fn read_only_file<P: AsRef<Path>>(path: P) -> Result<DmaFile, GlommioError<()>> {
    let dma_file = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .append(false)
        .dma_open(path)
        .await?;

    Ok(dma_file)
}

impl ReadOperations {
    pub async fn open<P: AsRef<Path>>(
        path_metadata: P, 
        path_event_batches: P, 
        data_cache: Vec<BatchMetadataItemPair>,
        aggregate_read_config: AggregateReadConfig,
        ) -> Result<ReadOperations, GlommioError<()>> {

        let metadata_dma_file = read_only_file(path_metadata).await?;
        let event_batches_dma_file = read_only_file(path_event_batches).await?;
        
        Ok(ReadOperations {
            metadata_dma_file, 
            event_batches_dma_file, 
            data_cache,
            config: aggregate_read_config
        })
    }

    pub async fn read(read_filters: ReadFilters) -> Result<(), ReadError> {
        Err(ReadError::SerializationError { message: "hello world".to_string() })

        // read_objects::read_fixed_records_visit_const(path, start, end_exclusive, max_chunk_size, on_record)
    }
}