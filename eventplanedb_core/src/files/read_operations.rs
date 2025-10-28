use std::path::Path;
use eventplanedb_structures::{constants::{BINCODE_CONFIG_FIXED, BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED, METADATA_BATCH_SIZE_BYTES}, event_batch_metadata::{EventBatchMetadata, EventTypesData}, read_filters::ReadFilters, read_result::ReadResult};
use fastbloom::BloomFilter;
use glommio::{GlommioError, io::{DmaFile, OpenOptions}};

use crate::files::{read_objects::{self, AbsoluteObjectPosition, ReadVisitError}, write_operations::BatchMetadataItemPair};

#[derive(Debug)]
pub enum ReadError {
    IoError(GlommioError<()>),
    MaxBytesTooSmall {
        current_max_bytes: u64,
        required_max_bytes: u64,
    },
    SerializationError {
        message: String,
    },
    UnavailableBatchIndex {
        minimum_available_event_batch_index: u64,
        requested_event_batch_index: u64,
    }
}

impl From<GlommioError<()>> for ReadError {
    fn from(error: GlommioError<()>) -> Self {
        ReadError::IoError(error)
    }
}

impl From<ReadVisitError<ReadError>> for ReadError {
    fn from(error: ReadVisitError<ReadError>) -> Self {
        match error {
            ReadVisitError::Io(glommio_error) => ReadError::IoError(glommio_error),
            ReadVisitError::Visitor(e) => e,
        }
    }
}

pub struct AggregateReadConfig {
    pub max_data_cache_size_bytes: usize,
    pub max_chunk_size: u64
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

    pub async fn read(&self, minimum_available_event_batch_index: u64, read_filters: ReadFilters) -> Result<ReadResult, ReadError> {
        
        if minimum_available_event_batch_index > read_filters.from_event_batch_index {
            return Err(ReadError::UnavailableBatchIndex { minimum_available_event_batch_index, requested_event_batch_index: read_filters.from_event_batch_index });
        }

        // Calculate the offset in the metadata file to start reading metadata chunks
        let start_reading_metadata_from_offset_position = (read_filters.from_event_batch_index - minimum_available_event_batch_index)
            * METADATA_BATCH_SIZE_BYTES as u64;

        //TODO: Writing could occur across await boundary and mess this up?
        let file_len_metadata = self.metadata_dma_file.file_size().await?;
        let file_len_event_batch = self.metadata_dma_file.file_size().await?;

        // Calculate how many metadata entries we can read
        let remaining_metadata_bytes =
            file_len_metadata.saturating_sub(start_reading_metadata_from_offset_position);
        let max_metadata_entries =
            (remaining_metadata_bytes / METADATA_BATCH_SIZE_BYTES as u64) as usize;

        // Handle scenario where client requests server_id that hasn't been written yet
        if max_metadata_entries == 0 {
            return Ok(ReadResult {
                event_batches: Vec::new(),
                next_event_batch_index: None,
            });
        }

        let mut batches: Vec<MetadataBatchInfo> = Vec::with_capacity(max_metadata_entries);

        read_objects::read_fixed_records_visit_const::<METADATA_BATCH_SIZE_BYTES, ReadError>(
            &self.metadata_dma_file, 
            start_reading_metadata_from_offset_position, 
            Some(file_len_metadata), //In case file changes due to concurrent writes...
            self.config.max_chunk_size, 
            | metadata_bytes | {
                let metadata = bincode::decode_from_slice(metadata_bytes, BINCODE_CONFIG_FIXED)
                    .map_err(|e| ReadError::SerializationError { message: e.to_string() })?
                    .0;

                let metadata_batch_info = MetadataBatchInfo {
                    include: is_include_batch(&metadata, &read_filters),
                    server_id: metadata.event_batch_index,
                    uncompressed_size: metadata.uncompressed_size,
                    compressed_size: metadata.compressed_size,
                    compression_type: metadata.compression_type,
                    events_crc: metadata.events_crc,
                    file_offset: 0,
                };

                batches.push(metadata_batch_info);
                Ok(())
            }
        ).await?;

        calculate_absolute_positions(file_len_event_batch, &mut batches);

        // Nothing to read after filtering
        if batches.is_empty() {
            return Ok(ReadResult {
                event_batches: Vec::new(),
                next_event_batch_index: None,
            });
        }

        let next_server_id: Option<u64> =
            trim_end_if_exceeds_max_bytes(&mut batches, read_filters.max_bytes)?;

        let object_positions = batches.iter().map(|f| AbsoluteObjectPosition { start_pos: f.file_offset, end_pos: f.file_offset + f.compressed_size }).collect();
        let event_batches_bytes = read_objects::read_objects_absolute(&self.event_batches_dma_file, &object_positions, 1 << 20).await?;

        Ok(ReadResult {
            event_batches,
            next_event_batch_index: next_server_id,
        })
    }
}

fn trim_end_if_exceeds_max_bytes(
    batches: &mut Vec<MetadataBatchInfo>,
    max_bytes: Option<usize>,
) -> Result<Option<u64>, ReadError> {
    // Only keep batches where include is true
    batches.retain(|batch| batch.include);

    // If no max_bytes limit is specified, we don't need to trim
    let max_bytes = match max_bytes {
        Some(limit) => limit as u64,
        None => return Ok(None),
    };

    // If after filtering we don't have any batches, return None
    if batches.is_empty() {
        return Ok(None);
    }

    // Calculate cumulative compressed size
    let mut cumulative_size: u64 = 0;
    let mut cut_index: Option<usize> = None;

    // Batches are sorted by server_id (ascending)
    for (index, batch) in batches.iter().enumerate() {
        cumulative_size += batch.compressed_size;

        // If we exceed the max_bytes limit, store this index as the cut point
        if cumulative_size > max_bytes {
            cut_index = Some(index);
            break;
        }
    }

    // If we need to trim
    if let Some(index) = cut_index {
        // Get the server_id of the first batch we're trimming
        let next_server_id = if index < batches.len() {
            Some(batches[index].server_id)
        } else {
            None
        };

        // Keep only the batches that fit within the max_bytes limit
        batches.truncate(index);

        if batches.is_empty() {
            // Throw an error as max bytes was too small to return any event batches
            return Err(ReadError::MaxBytesTooSmall {
                current_max_bytes: max_bytes,
                required_max_bytes: cumulative_size
            });
        }

        Ok(next_server_id)
    } else {
        // No trimming needed, all batches fit within the limit
        Ok(None)
    }
}

fn calculate_absolute_positions(event_batches_file_len: u64, batches: &mut [MetadataBatchInfo]) {
    let mut current_offset = 0u64;

    for batch in batches.iter_mut().rev() {
        current_offset += batch.compressed_size;
        batch.file_offset = event_batches_file_len - current_offset;
    }
}

fn is_include_batch(metadata: &EventBatchMetadata, filters: &ReadFilters) -> bool {
    if metadata.event_batch_index < filters.from_event_batch_index {
        return false;
    }

    if filters.to_event_batch_index.map_or(false, |to_server_id| {
        metadata.event_batch_index > to_server_id
    }) {
        return false;
    }

    if filters
        .min_server_timestamp
        .map_or(false, |before_server_time| {
            metadata.server_timestamp < before_server_time
        })
    {
        return false;
    }

    if filters
        .max_server_timestamp
        .map_or(false, |after_server_time| {
            metadata.server_timestamp > after_server_time
        })
    {
        return false;
    }

    if filters
        .exclude_client_id
        .map_or(false, |exclude_client_id| {
            metadata.client_id == exclude_client_id
        })
    {
        return false;
    }

    if filters
        .include_client_id
        .map_or(false, |include_client_id| {
            metadata.client_id != include_client_id
        })
    {
        return false;
    }

    if filters
        .exclude_user_id
        .map_or(false, |exclude_user_id| metadata.user_id == exclude_user_id)
    {
        return false;
    }

    if filters
        .include_user_id
        .map_or(false, |include_user_id| metadata.user_id != include_user_id)
    {
        return false;
    }

    if filters.min_client_event_index.map_or(false, |min_index| {
        metadata.max_client_event_index < min_index
    }) {
        return false;
    }

    if filters.max_client_event_index.map_or(false, |max_index| {
        metadata.min_client_event_index > max_index
    }) {
        return false;
    }

    if filters
        .min_event_timestamp
        .map_or(false, |min_time| metadata.max_event_timestamp < min_time)
    {
        return false;
    }

    if filters
        .max_event_timestamp
        .map_or(false, |max_time| metadata.min_event_timestamp > max_time)
    {
        return false;
    }

    if filters
        .min_event_index
        .map_or(false, |min_index| metadata.max_event_index < min_index)
    {
        return false;
    }

    if filters
        .max_event_index
        .map_or(false, |max_index| metadata.min_event_index > max_index)
    {
        return false;
    }

    if filters
        .include_event_types
        .as_ref()
        .map_or(false, |include_event_types| {
            //Is there at least one of the include_event_types in the event batch? If not, return true to skip
            let at_least_one_match =
                check_event_types_match(&metadata.event_types_data, &include_event_types);
            !at_least_one_match
        })
    {
        return false;
    }

    true
}

fn check_event_types_match(event_types_data: &EventTypesData, include_event_types: &[u64]) -> bool {
    match event_types_data {
        EventTypesData::Direct(event_types) => {
            // Check if any of the required types are in the direct array
            if event_types.len() < include_event_types.len() {
                event_types
                    .iter()
                    .any(|&batch_type| include_event_types.contains(&batch_type))
            } else {
                include_event_types
                    .iter()
                    .any(|&include_event_type| event_types.contains(&include_event_type))
            }
        }
        EventTypesData::Bloom(bloom_bytes) => {
            // Create bloom filter and test each required type
            let bloom = bloom_filter_from_bytes(bloom_bytes);
            include_event_types
                .iter()
                .any(|&include_event_type| bloom.contains(&include_event_type.to_le_bytes()))
        }
    }
}

fn bloom_filter_from_bytes(bloom_bytes: &[u64; BLOOM_BYTES / 8]) -> BloomFilter {
    BloomFilter::from_vec(bloom_bytes.to_vec())
        .seed(&BLOOM_HASH_SEED)
        .hashes(BLOOM_HASH_COUNT)
}

#[derive(Debug)]
struct MetadataBatchInfo {
    server_id: u64,
    uncompressed_size: u64,
    compressed_size: u64,
    compression_type: u8,
    events_crc: u32,
    file_offset: u64,
    include: bool,
}