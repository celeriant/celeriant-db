use std::{collections::HashMap, path::Path, ptr::read};
use eventplanedb_structures::{compression_type::CompressionType, constants::{BINCODE_CONFIG_FIXED, BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED, METADATA_BATCH_SIZE_BYTES}, event_batch_item::EventBatchItem, event_batch_metadata::{EventBatchMetadata, EventTypesData}, read_filters::ReadFilters, read_result::ReadResult, wire_format::from_wire_format_variable};
use fastbloom::BloomFilter;
use glommio::{GlommioError, io::{DmaFile, OpenOptions}};

use crate::files::{read_objects::{self, AbsoluteObjectPosition, ReadVisitError}, write_operations::{BatchMetadataItemPair, WriteOperationsDataRequirements}};

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
    },
    CorruptEventBatch {
        expected_crc: u32,
        actual_crc: u32,
        event_batch_index: u64,
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

#[derive(Debug)]
pub struct MetadataWithAbsolutePosition {
    event_batch_metadata: EventBatchMetadata,
    event_batch_absolute_position: u64,
}

pub struct ReadOperations {
    metadata_dma_file: DmaFile,
    event_batches_dma_file: DmaFile,
    cache_metadata: Vec<MetadataWithAbsolutePosition>,
    config: AggregateReadConfig,
}

#[derive(Debug)]
pub struct CacheableReadResult {
    pub uncached_metadata_set: Vec<MetadataWithAbsolutePosition>,
    pub filtered_event_batches: Vec<EventBatchItem>,
    pub next_event_batch_index: Option<u64>,
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

pub struct WriteOperationsDataRequirementsAndCachedData {
    pub uncached_metadata_set: Vec<MetadataWithAbsolutePosition>,
    pub write_operations_data_requirements: WriteOperationsDataRequirements,
}

impl ReadOperations {

    pub async fn get_write_operations_data_requirements(
        &self,
    ) -> Result<WriteOperationsDataRequirementsAndCachedData, ReadError> {

        let file_len_metadata = self.metadata_dma_file.file_size().await?;
        let file_len_event_batch = self.event_batches_dma_file.file_size().await?;

        // No metadata in file => initial state
        if file_len_metadata == 0 {
            let write_operations_data_requirements = WriteOperationsDataRequirements {
                data_cache: vec![],
                next_event_index: 1,
                next_event_batch_index: 1,
                client_event_indexes: HashMap::new(),
            };
            return Ok(WriteOperationsDataRequirementsAndCachedData {
                uncached_metadata_set: vec![],
                write_operations_data_requirements,
            });
        }

        let rec_size = METADATA_BATCH_SIZE_BYTES as u64;
        assert!(
            file_len_metadata % rec_size == 0,
            "metadata file is not a multiple of METADATA_BATCH_SIZE_BYTES"
        );

        // Snapshot cache (tail of file)
        let cached_snapshot: Vec<&MetadataWithAbsolutePosition> = self.cache_metadata.iter().collect();
        let cached_count = cached_snapshot.len() as u64;

        // Determine uncached prefix size (in bytes) and record count
        let total_records = file_len_metadata / rec_size;
        let cached_bytes = std::cmp::min(cached_count, total_records) * rec_size;
        let uncached_bytes = file_len_metadata.saturating_sub(cached_bytes);
        let uncached_records = (uncached_bytes / rec_size) as usize;

        // Bound how many we return to prime read cache
        let metadata_cache_capacity = self.config.max_data_cache_size_bytes / METADATA_BATCH_SIZE_BYTES;
        let return_count = std::cmp::min(uncached_records, metadata_cache_capacity);

        // Single pass over the uncached prefix to:
        // - build client_event_indexes
        // - track last scanned metadata (for next_* if no cache)
        // - collect the last `return_count` items of the scanned prefix to return
        let mut client_event_indexes: HashMap<u128, u64> = HashMap::new();
        let mut last_scanned: Option<EventBatchMetadata> = None;

        // Use a small ring buffer for the tail we want to return
        let mut ring: std::collections::VecDeque<MetadataWithAbsolutePosition> =
            std::collections::VecDeque::with_capacity(return_count.max(1));

        if uncached_bytes > 0 && return_count > 0 || uncached_bytes > 0 {
            read_objects::read_fixed_records_visit_const::<METADATA_BATCH_SIZE_BYTES, ReadError>(
                &self.metadata_dma_file,
                0,
                Some(uncached_bytes),
                self.config.max_chunk_size,
                |metadata_bytes| {
                    let meta: EventBatchMetadata =
                        bincode::decode_from_slice(metadata_bytes, BINCODE_CONFIG_FIXED)
                            .map_err(|e| ReadError::SerializationError { message: e.to_string() })?
                            .0;

                    // Build client map (latest max index per client)
                    client_event_indexes
                        .entry(meta.client_id)
                        .and_modify(|v| { if meta.max_client_event_index > *v { *v = meta.max_client_event_index; } })
                        .or_insert(meta.max_client_event_index);

                    // Keep the tail subset to return
                    if return_count > 0 {
                        ring.push_back(MetadataWithAbsolutePosition {
                            event_batch_metadata: meta.clone(),
                            event_batch_absolute_position: 0,
                        });
                        if ring.len() > return_count {
                            ring.pop_front();
                        }
                    }

                    last_scanned = Some(meta);
                    Ok(())
                },
            ).await?;
        }

        // Merge cached client indexes
        for c in cached_snapshot.iter() {
            let meta = &c.event_batch_metadata;
            client_event_indexes
                .entry(meta.client_id)
                .and_modify(|v| { if meta.max_client_event_index > *v { *v = meta.max_client_event_index; } })
                .or_insert(meta.max_client_event_index);
        }

        // Compute next_* from latest record (prefer cached tail if available)
        let (next_event_index, next_event_batch_index) = if let Some(last_cached) = cached_snapshot.last() {
            let m = &last_cached.event_batch_metadata;
            (m.max_event_index.saturating_add(1), m.event_batch_index.saturating_add(1))
        } else if let Some(m) = last_scanned {
            (m.max_event_index.saturating_add(1), m.event_batch_index.saturating_add(1))
        } else {
            // Shouldn't happen since file_len_metadata > 0, but be safe
            (1, 1)
        };

        // Materialize uncached metadata set and assign absolute positions using the first cached
        // item's absolute position as the base (or EOF if no cache).
        let mut uncached_metadata_set: Vec<MetadataWithAbsolutePosition> = ring.into_iter().collect();

        if !uncached_metadata_set.is_empty() {
            let mut base_pos = if let Some(first_cached) = cached_snapshot.first() {
                first_cached.event_batch_absolute_position
            } else {
                file_len_event_batch
            };

            // Assign positions by walking backward
            for m in uncached_metadata_set.iter_mut().rev() {
                base_pos = base_pos.saturating_sub(m.event_batch_metadata.compressed_size);
                m.event_batch_absolute_position = base_pos;
            }
        }

        let write_operations_data_requirements = WriteOperationsDataRequirements {
            data_cache: vec![], // writer builds cache from newly appended data
            next_event_index,
            next_event_batch_index,
            client_event_indexes,
        };

        Ok(WriteOperationsDataRequirementsAndCachedData {
            uncached_metadata_set,
            write_operations_data_requirements,
        })
    }

    pub async fn open<P: AsRef<Path>>(
        path_metadata: P, 
        path_event_batches: P, 
        cache_metadata: Vec<MetadataWithAbsolutePosition>,
        aggregate_read_config: AggregateReadConfig,
        ) -> Result<ReadOperations, GlommioError<()>> {

        let metadata_dma_file = read_only_file(path_metadata).await?;
        let event_batches_dma_file = read_only_file(path_event_batches).await?;
        
        Ok(ReadOperations {
            metadata_dma_file, 
            event_batches_dma_file, 
            cache_metadata,
            config: aggregate_read_config
        })
    }

    //minimum_available_event_batch_index comes from writer as it only changes during a trim operation
    pub async fn read(&self, minimum_available_event_batch_index: u64, file_len_metadata: u64, file_len_event_batch: u64, read_filters: &ReadFilters) -> Result<CacheableReadResult, ReadError> {
        
        if minimum_available_event_batch_index > read_filters.from_event_batch_index {
            return Err(ReadError::UnavailableBatchIndex { minimum_available_event_batch_index, requested_event_batch_index: read_filters.from_event_batch_index });
        }

        let metadata_read_from_bytes = read_filters.from_event_batch_index.saturating_sub(minimum_available_event_batch_index) * METADATA_BATCH_SIZE_BYTES as u64;

        // Handle scenario where client requests server_id that hasn't been written yet
        if file_len_metadata.saturating_sub(metadata_read_from_bytes) == 0 {
            return Ok(CacheableReadResult {
                uncached_metadata_set: Vec::new(),
                filtered_event_batches: Vec::new(),
                next_event_batch_index: None,
            });
        }
        
        // Any cached metadata is not required to be re-read
        // Take a snapshot of the cache as refs, as it could change at any point over await boundaries
        let cached_metadata_set_snapshot: Vec<&MetadataWithAbsolutePosition> = self.cache_metadata.iter().collect();

        // Work out if the cache covers the required read boudnaries or do we need to pull more metadata entries from disk
        let uncached_metadata_read_to_bytes = file_len_metadata.saturating_sub(cached_metadata_set_snapshot.len() as u64 * METADATA_BATCH_SIZE_BYTES as u64);
        let uncached_metadata_count = (uncached_metadata_read_to_bytes.saturating_sub(metadata_read_from_bytes) / METADATA_BATCH_SIZE_BYTES as u64) as usize;
        let mut uncached_metadata_set: Vec<MetadataWithAbsolutePosition> = Vec::with_capacity(uncached_metadata_count);

        // Absolute position reference from cache, uncached metadata extends in reverse from here
        let mut event_batch_absolute_position = if cached_metadata_set_snapshot.len() > 0 {
            cached_metadata_set_snapshot[0].event_batch_absolute_position
        } else {
            file_len_event_batch
        };

        if uncached_metadata_count > 0 {
            //This is where we reach out to disk to get additional metadata
            //It's an async boundary so it could yield for other processing on the thread
            read_objects::read_fixed_records_visit_const::<METADATA_BATCH_SIZE_BYTES, ReadError>(
                &self.metadata_dma_file, 
                metadata_read_from_bytes, 
                Some(uncached_metadata_read_to_bytes), //In case file changes due to concurrent writes...
                self.config.max_chunk_size, 
                | metadata_bytes | {
                    let event_batch_metadata = bincode::decode_from_slice(metadata_bytes, BINCODE_CONFIG_FIXED)
                        .map_err(|e| ReadError::SerializationError { message: e.to_string() })?
                        .0;
                    // We use MetadataWithAbsolutePosition here as this is the owned struct we will cache later
                    uncached_metadata_set.push(MetadataWithAbsolutePosition {
                        event_batch_metadata,
                        event_batch_absolute_position: 0
                    });
                    Ok(())
                }
            ).await?;
        }

        // Calculate the absolution start positions for each event batch in the data file
        // Now uncached_metadata_set is ready for caching, we can return it later!
        for metadata_with_absolute_position in uncached_metadata_set.iter_mut().rev() {
            event_batch_absolute_position -= metadata_with_absolute_position.event_batch_metadata.compressed_size;
            metadata_with_absolute_position.event_batch_absolute_position = event_batch_absolute_position;
        }

        // Let's build the complete, contiguous set of metadata entries (uncached + cached) so that we can read the event batches
        // It's mutable because we want to remove batches that don't match the read_filters
        let mut metadata_for_reading: Vec<&MetadataWithAbsolutePosition> = Vec::with_capacity(uncached_metadata_set.len() + cached_metadata_set_snapshot.len());
        metadata_for_reading.extend(uncached_metadata_set.iter());
        metadata_for_reading.extend(cached_metadata_set_snapshot.iter());

        // Now we exclude metadata entries based on the filter and only take up to max_bytes (pagination)
        // This could error if the server's max_bytes settings means we can't even return the first batch
        let next_event_batch_index: Option<u64> =
            trim_end_if_exceeds_max_bytes(&mut metadata_for_reading, &read_filters, read_filters.max_bytes)?;

        // Another async boundary - this time reading the actual event batches at specific positions in the file
        // Note the set is still ordered but there may be gaps
        let object_positions: Vec<AbsoluteObjectPosition> = metadata_for_reading.iter()
            .map(|f| AbsoluteObjectPosition { 
                start_pos: f.event_batch_absolute_position, 
                end_pos: f.event_batch_absolute_position + f.event_batch_metadata.compressed_size 
            }).collect();

        let event_batches_bytes_set = read_objects::read_objects_absolute(
            &self.event_batches_dma_file, 
            &object_positions, 
            self.config.max_chunk_size).await?;

        // Let's deser our event batches now and do final filtering
        // Note event_batches_bytes_set and event_batches_bytes_set are always the same size
        assert!(event_batches_bytes_set.len() == metadata_for_reading.len());

        let mut index = 0;
        let mut filtered_event_batches: Vec<EventBatchItem> = Vec::with_capacity(event_batches_bytes_set.len());
        for event_batch_bytes in event_batches_bytes_set.iter() {

            let metadata = &metadata_for_reading[index].event_batch_metadata;
            let actual_crc = crc32fast::hash(&event_batch_bytes);

            if actual_crc != metadata.events_crc {
                return Err(ReadError::CorruptEventBatch { expected_crc: metadata.events_crc, actual_crc, event_batch_index: metadata.event_batch_index })
            }

            let compression_type = CompressionType::from_tuple(metadata.compression_type, None);
            let mut event_batch = from_wire_format_variable::<EventBatchItem>(
                &event_batch_bytes,
                compression_type,
                metadata.uncompressed_size as usize,
            ).map_err(|e| ReadError::SerializationError { message: e.to_string() })?;

            // Apply all event filters
            apply_event_filters(&mut event_batch, &read_filters);

            if !event_batch.events.is_empty() {
                filtered_event_batches.push(event_batch);
            }

            index += 1;
        }

        Ok(CacheableReadResult {
            uncached_metadata_set,
            filtered_event_batches,
            next_event_batch_index,
        })
    }
    
    pub fn update_metadata_cache(&mut self, mut uncached_metadata_set: Vec<MetadataWithAbsolutePosition>) {
        if uncached_metadata_set.is_empty() {
            return;
        }
        
        // Find split point - how many items from uncached_metadata_set to keep
        let items_to_keep = if let Some(first_cached) = self.cache_metadata.first() {
            let min_cached_index = first_cached.event_batch_metadata.event_batch_index;
            // Find first item >= min_cached_index and keep everything before it
            uncached_metadata_set.iter()
                .position(|m| m.event_batch_metadata.event_batch_index >= min_cached_index)
                .unwrap_or(uncached_metadata_set.len())
        } else {
            uncached_metadata_set.len()
        };
        
        // Truncate to remove any overlapping items
        uncached_metadata_set.truncate(items_to_keep);
        
        // Insert at the front (moves uncached_metadata_set, no allocation)
        self.cache_metadata.splice(0..0, uncached_metadata_set);
        
        // Trim cache to fit within max_data_cache_size_bytes
        // Keep the newest items (at the back) and remove oldest (at the front)
        let mut total_size = 0;
        let mut keep_count = 0;
        
        for _metadata in self.cache_metadata.iter() {
            let entry_size = METADATA_BATCH_SIZE_BYTES as usize;
            if total_size + entry_size > self.config.max_data_cache_size_bytes {
                break;
            }
            total_size += entry_size;
            keep_count += 1;
        }
        
        // Remove oldest items from the front if cache is too large
        if self.cache_metadata.len() > keep_count {
            let remove_count = self.cache_metadata.len() - keep_count;
            self.cache_metadata.drain(0..remove_count);
        }
    }

}

pub fn apply_event_filters(event_batch: &mut EventBatchItem, read_filters: &ReadFilters) {
    
    // Final event type filtering (bloom filter might have false positives)
    if let Some(event_types) = read_filters.include_event_types.as_deref() {
        event_batch
            .events
            .retain(|event| event_types.contains(&event.event_type_major));
    }

    // Final filtering for local_index
    if let Some(min_event_index) = read_filters.min_event_index {
        event_batch
            .events
            .retain(|event| event.event_index >= min_event_index);
    }

    if let Some(max_event_index) = read_filters.max_event_index {
        event_batch
            .events
            .retain(|event| event.event_index <= max_event_index);
    }

    // Final filtering for event_time
    if let Some(min_event_time) = read_filters.min_event_timestamp {
        event_batch
            .events
            .retain(|event| event.event_timestamp >= min_event_time);
    }

    if let Some(max_event_time) = read_filters.max_event_timestamp {
        event_batch
            .events
            .retain(|event| event.event_timestamp <= max_event_time);
    }

    // Final filtering for event index
    if let Some(min_client_event_index) = read_filters.min_client_event_index {
        event_batch
            .events
            .retain(|event| event.client_event_index >= min_client_event_index);
    }

    if let Some(max_client_event_index) = read_filters.max_client_event_index {
        event_batch
            .events
            .retain(|event| event.client_event_index <= max_client_event_index);
    }
}

fn trim_end_if_exceeds_max_bytes(
    metadata_for_reading: &mut Vec<&MetadataWithAbsolutePosition>,
    read_filters: &ReadFilters,
    max_bytes: Option<usize>,
) -> Result<Option<u64>, ReadError> {
    // Only keep batches where include is true
    metadata_for_reading.retain(|batch| is_include_batch(&batch.event_batch_metadata, read_filters));

    // If no max_bytes limit is specified, we don't need to trim
    let max_bytes = match max_bytes {
        Some(limit) => limit as u64,
        None => return Ok(None),
    };

    // If after filtering we don't have any batches, return None
    if metadata_for_reading.is_empty() {
        return Ok(None);
    }

    // Calculate cumulative compressed size
    let mut cumulative_size: u64 = 0;
    let mut cut_index: Option<usize> = None;

    // Batches are sorted by event_batch_index (ascending)
    for (index, batch) in metadata_for_reading.iter().enumerate() {
        cumulative_size += batch.event_batch_metadata.compressed_size;

        // If we exceed the max_bytes limit, store this index as the cut point
        if cumulative_size > max_bytes {
            cut_index = Some(index);
            break;
        }
    }

    // If we need to trim
    if let Some(index) = cut_index {
        // Get the server_id of the first batch we're trimming
        let next_event_batch_index = if index < metadata_for_reading.len() {
            Some(metadata_for_reading[index].event_batch_metadata.event_batch_index)
        } else {
            None
        };

        // Keep only the batches that fit within the max_bytes limit
        metadata_for_reading.truncate(index);

        if metadata_for_reading.is_empty() {
            // Throw an error as max bytes was too small to return any event batches
            return Err(ReadError::MaxBytesTooSmall {
                current_max_bytes: max_bytes,
                required_max_bytes: cumulative_size
            });
        }

        Ok(next_event_batch_index)
    } else {
        // No trimming needed, all batches fit within the limit
        Ok(None)
    }
}

pub fn is_include_batch(metadata: &EventBatchMetadata, filters: &ReadFilters) -> bool {
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

//Some tests
#[cfg(test)]
pub mod test_read_operations {
    use std::fs;

    use super::*;

    fn read_config() -> AggregateReadConfig {
        AggregateReadConfig {
            max_chunk_size: 1 << 20,
            max_data_cache_size_bytes: 1 << 24
        }
    }

    pub async fn read_file_operations(folder: &str) -> Result<ReadOperations, GlommioError<()>> {
       let service = ReadOperations::open(
        format!("{}/metadata.bin", folder),
        format!("{}/event_batches.bin", folder),
            vec![],
            read_config()
        ).await?;

        Ok(service)
    }

    /// Return the lengths (in bytes) of `metadata.bin` and `event_batches.bin` in `folder`.
    pub fn file_lengths(folder: &str) -> std::io::Result<(u64, u64)> {
        let meta_path = format!("{}/metadata.bin", folder);
        let events_path = format!("{}/event_batches.bin", folder);

        let meta_len = fs::metadata(meta_path)?.len();
        let events_len = fs::metadata(events_path)?.len();

        Ok((meta_len, events_len))
    }
}