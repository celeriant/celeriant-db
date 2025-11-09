use std::{collections::{HashMap, VecDeque}, path::Path};
use eventplanedb_structures::{compression_type::CompressionType, constants::{BINCODE_CONFIG_FIXED, BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED, METADATA_BATCH_SIZE_BYTES}, event_batch_item::EventBatchItem, event_batch_metadata::{EventBatchMetadata, EventTypesData}, read_filters::ReadFilters, wire_format::from_wire_format_variable};
use fastbloom::BloomFilter;
use glommio::{GlommioError, io::{DmaFile, OpenOptions}};

use crate::files::{read_objects::{self, AbsoluteObjectPosition, ReadVisitError}, write_operations::{WriteOperationsDataRequirements}};

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
        file_pos_metadata: u64, 
        file_pos_event_batch: u64
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

#[derive(Clone)]
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

#[derive(Debug)]
pub struct CacheableReadAllResult {
    pub uncached_metadata_set: Vec<MetadataWithAbsolutePosition>,
    pub batches: Vec<(EventBatchMetadata, EventBatchItem)>,
    pub next_event_batch_index: Option<u64>,
}

async fn get_existing_file_as_dma<P: AsRef<Path>>(path: P) -> Result<DmaFile, GlommioError<()>> {
    let dma_file = OpenOptions::new()
        .read(true)
        .write(true)
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

    pub async fn read_all(
        &self, 
        minimum_available_event_batch_index: u64, 
        file_len_metadata: u64, 
        file_len_event_batch: u64,
        from_event_batch_index: u64,
        to_event_batch_index: Option<u64>,
        max_bytes: Option<usize>,
    ) -> Result<CacheableReadAllResult, ReadError> {
        
        let (uncached_metadata_set, cached_metadata_set_snapshot) = self.get_metadata_range(
            minimum_available_event_batch_index,
            from_event_batch_index,
            file_len_metadata,
            file_len_event_batch,
            to_event_batch_index,
        ).await?;

        if uncached_metadata_set.is_empty() && cached_metadata_set_snapshot.is_empty() {
            return Ok(CacheableReadAllResult {
                uncached_metadata_set: Vec::new(),
                batches: Vec::new(),
                next_event_batch_index: None,
            });
        }

        let mut metadata_for_reading: Vec<&MetadataWithAbsolutePosition> = Vec::with_capacity(
            uncached_metadata_set.len() + cached_metadata_set_snapshot.len()
        );
        metadata_for_reading.extend(uncached_metadata_set.iter());
        metadata_for_reading.extend(cached_metadata_set_snapshot.iter());

        let next_event_batch_index = apply_max_bytes_pagination(&mut metadata_for_reading, max_bytes)?;

        let object_positions: Vec<AbsoluteObjectPosition> = metadata_for_reading.iter()
            .map(|m| AbsoluteObjectPosition { 
                start_pos: m.event_batch_absolute_position, 
                end_pos: m.event_batch_absolute_position + m.event_batch_metadata.compressed_size 
            }).collect();

        let event_batches_bytes_set = read_objects::read_objects_absolute(
            &self.event_batches_dma_file, 
            &object_positions, 
            self.config.max_chunk_size
        ).await?;

        assert!(event_batches_bytes_set.len() == metadata_for_reading.len());

        let mut batches: Vec<(EventBatchMetadata, EventBatchItem)> = Vec::with_capacity(event_batches_bytes_set.len());
        
        for (index, event_batch_bytes) in event_batches_bytes_set.iter().enumerate() {
            let metadata = &metadata_for_reading[index].event_batch_metadata;
            let actual_crc = crc32fast::hash(event_batch_bytes);

            if actual_crc != metadata.events_crc {
                return Err(ReadError::CorruptEventBatch { 
                    expected_crc: metadata.events_crc, 
                    actual_crc, 
                    event_batch_index: metadata.event_batch_index,
                    file_pos_event_batch: object_positions[index].start_pos,
                    file_pos_metadata: metadata_for_reading[index].event_batch_absolute_position
                });
            }

            let compression_type = CompressionType::from_tuple(metadata.compression_type, None);
            let event_batch = from_wire_format_variable::<EventBatchItem>(
                event_batch_bytes,
                compression_type,
                metadata.uncompressed_size as usize,
            ).map_err(|e| ReadError::SerializationError { message: e.to_string() })?;

            batches.push((metadata.clone(), event_batch));
        }

        Ok(CacheableReadAllResult {
            uncached_metadata_set,
            batches,
            next_event_batch_index,
        })
    }

    async fn get_metadata_range(
        &self,
        minimum_available_event_batch_index: u64,
        from_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
        to_event_batch_index: Option<u64>, // None means read to end of file
    ) -> Result<(Vec<MetadataWithAbsolutePosition>, Vec<&MetadataWithAbsolutePosition>), ReadError> {
        
        if minimum_available_event_batch_index > from_event_batch_index {
            return Err(ReadError::UnavailableBatchIndex { 
                minimum_available_event_batch_index, 
                requested_event_batch_index: from_event_batch_index 
            });
        }

        let metadata_read_from_bytes = from_event_batch_index.saturating_sub(minimum_available_event_batch_index) * METADATA_BATCH_SIZE_BYTES as u64;

        // Handle scenario where we're reading past the end of the file
        if file_len_metadata.saturating_sub(metadata_read_from_bytes) == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        // Calculate read boundaries
        let metadata_read_to_bytes = if let Some(to_index) = to_event_batch_index {
            let to_bytes = (to_index.saturating_sub(minimum_available_event_batch_index) + 1) * METADATA_BATCH_SIZE_BYTES as u64;
            std::cmp::min(to_bytes, file_len_metadata)
        } else {
            file_len_metadata
        };

        // Take a snapshot of the cache
        let cached_metadata_set_snapshot: Vec<&MetadataWithAbsolutePosition> = self.cache_metadata.iter().collect();

        // Determine uncached range
        let uncached_metadata_read_to_bytes = file_len_metadata.saturating_sub(
            cached_metadata_set_snapshot.len() as u64 * METADATA_BATCH_SIZE_BYTES as u64
        );
        
        let actual_read_to = std::cmp::min(metadata_read_to_bytes, uncached_metadata_read_to_bytes);
        let uncached_metadata_count = ((actual_read_to.saturating_sub(metadata_read_from_bytes)) / METADATA_BATCH_SIZE_BYTES as u64) as usize;
        let mut uncached_metadata_set: Vec<MetadataWithAbsolutePosition> = Vec::with_capacity(uncached_metadata_count);

        // Absolute position reference from cache
        let mut event_batch_absolute_position = if !cached_metadata_set_snapshot.is_empty() {
            cached_metadata_set_snapshot[0].event_batch_absolute_position
        } else {
            file_len_event_batch
        };

        // Read uncached metadata from disk
        if uncached_metadata_count > 0 {
            read_objects::read_fixed_records_visit_const::<METADATA_BATCH_SIZE_BYTES, ReadError>(
                &self.metadata_dma_file,
                metadata_read_from_bytes,
                Some(actual_read_to),
                self.config.max_chunk_size,
                |metadata_bytes| {
                    let event_batch_metadata = bincode::decode_from_slice(metadata_bytes, BINCODE_CONFIG_FIXED)
                        .map_err(|e| ReadError::SerializationError { message: e.to_string() })?
                        .0;
                    uncached_metadata_set.push(MetadataWithAbsolutePosition {
                        event_batch_metadata,
                        event_batch_absolute_position: 0
                    });
                    Ok(())
                }
            ).await?;
        }

        // Calculate absolute positions
        for metadata_with_absolute_position in uncached_metadata_set.iter_mut().rev() {
            event_batch_absolute_position -= metadata_with_absolute_position.event_batch_metadata.compressed_size;
            metadata_with_absolute_position.event_batch_absolute_position = event_batch_absolute_position;
        }

        Ok((uncached_metadata_set, cached_metadata_set_snapshot))
    }
    
    pub async fn get_file_positions(
        &self, 
        minimum_available_event_batch_index: u64, 
        keep_from_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
    ) -> Result<(u64, u64), ReadError> {
        
        let metadata_position = (keep_from_event_batch_index - minimum_available_event_batch_index) * METADATA_BATCH_SIZE_BYTES as u64;

        // Get all metadata from start to keep_from_event_batch_index (exclusive)
        let (uncached_metadata_set, cached_metadata_set_snapshot) = self.get_metadata_range(
            minimum_available_event_batch_index,
            minimum_available_event_batch_index,
            file_len_metadata,
            file_len_event_batch,
            Some(keep_from_event_batch_index.saturating_sub(1)),
        ).await?;

        // Sum compressed sizes to get event batch position
        let mut event_batch_position = 0u64;
        
        for metadata in uncached_metadata_set.iter() {
            event_batch_position += metadata.event_batch_metadata.compressed_size;
        }

        for metadata in cached_metadata_set_snapshot.iter() {
            if metadata.event_batch_metadata.event_batch_index < keep_from_event_batch_index {
                event_batch_position += metadata.event_batch_metadata.compressed_size;
            } else {
                break;
            }
        }

        Ok((metadata_position, event_batch_position))
    }

    pub fn trim_start(&mut self, metadata_dma_file: DmaFile, event_batches_dma_file: DmaFile) {
        self.metadata_dma_file = metadata_dma_file;
        self.event_batches_dma_file = event_batches_dma_file;
        //TODO: Could be more specific about what to clear
        self.cache_metadata.clear();
    }

    pub async fn get_write_operations_data_requirements(
        &self,
    ) -> Result<WriteOperationsDataRequirementsAndCachedData, ReadError> {

        let mut file_len_metadata = self.metadata_dma_file.file_size().await?;
        let mut file_len_event_batch = self.event_batches_dma_file.file_size().await?;

        let event_batches_dma_file = self.event_batches_dma_file.dup()?;
        let metadata_dma_file = self.metadata_dma_file.dup()?;

        // No metadata in file => initial state
        if file_len_metadata == 0 {
            let write_operations_data_requirements = WriteOperationsDataRequirements {
                event_batches_dma_file,
                metadata_dma_file,
                file_len_event_batch,
                file_len_metadata,
                data_cache: VecDeque::new(),
                minimum_available_event_batch_index: 0,
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
        
        // Trim metadata file if not aligned (corrupt partial record at end)
        if file_len_metadata % rec_size != 0 {
            let aligned_len = (file_len_metadata / rec_size) * rec_size;
            metadata_dma_file.truncate(aligned_len).await?;
            file_len_metadata = aligned_len;
            
            // If we truncated everything, return initial state
            if file_len_metadata == 0 {
                let write_operations_data_requirements = WriteOperationsDataRequirements {
                    event_batches_dma_file,
                    metadata_dma_file,
                    file_len_event_batch,
                    file_len_metadata,
                    data_cache: VecDeque::new(),
                    minimum_available_event_batch_index: 0,
                    next_event_index: 1,
                    next_event_batch_index: 1,
                    client_event_indexes: HashMap::new(),
                };
                return Ok(WriteOperationsDataRequirementsAndCachedData {
                    uncached_metadata_set: vec![],
                    write_operations_data_requirements,
                });
            }
        }

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

        let mut minimum_available_event_batch_index: Option<u64> = None;

        if uncached_bytes > 0 {
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

                    if minimum_available_event_batch_index.is_none() {
                        minimum_available_event_batch_index = Some(meta.event_batch_index);
                    }

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
        
        if !self.cache_metadata.is_empty() && self.cache_metadata.first().unwrap().event_batch_absolute_position == 0 {
            // Can get min event batch from the cache
            minimum_available_event_batch_index = Some(self.cache_metadata.first().unwrap().event_batch_metadata.event_batch_index);
        }

        assert!(minimum_available_event_batch_index.is_some(), "Should get min event batch index");

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

        // Verify CRC of last metadata entry against event batch bytes
        // Get the last metadata entry (prefer cached, fall back to uncached)
        let last_metadata = if let Some(last_cached) = cached_snapshot.last() {
            Some(*last_cached)
        } else if let Some(last_uncached) = uncached_metadata_set.last() {
            Some(last_uncached)
        } else {
            None
        };

        if let Some(last_meta) = last_metadata {
            // Read the last event batch
            let last_batch_pos = AbsoluteObjectPosition {
                start_pos: last_meta.event_batch_absolute_position,
                end_pos: last_meta.event_batch_absolute_position + last_meta.event_batch_metadata.compressed_size,
            };

            let last_batch_pos_start_pos = last_batch_pos.start_pos;

            let last_batch_bytes = read_objects::read_objects_absolute(
                &event_batches_dma_file,
                &[last_batch_pos],
                self.config.max_chunk_size,
            ).await?;

            let last_actual_crc = crc32fast::hash(&last_batch_bytes[0]);

            if last_actual_crc != last_meta.event_batch_metadata.events_crc {
                // Last entry is corrupt, check second-to-last
                let second_last_metadata = if cached_snapshot.len() >= 2 {
                    Some(cached_snapshot[cached_snapshot.len() - 2])
                } else if cached_snapshot.len() == 1 && !uncached_metadata_set.is_empty() {
                    Some(uncached_metadata_set.last().unwrap())
                } else if uncached_metadata_set.len() >= 2 {
                    Some(&uncached_metadata_set[uncached_metadata_set.len() - 2])
                } else {
                    None
                };

                if let Some(second_last_meta) = second_last_metadata {
                    // Read the second-to-last event batch
                    let second_last_batch_pos = AbsoluteObjectPosition {
                        start_pos: second_last_meta.event_batch_absolute_position,
                        end_pos: second_last_meta.event_batch_absolute_position + second_last_meta.event_batch_metadata.compressed_size,
                    };

                    let second_last_batch_pos_start_pos = second_last_batch_pos.start_pos;

                    let second_last_batch_bytes = read_objects::read_objects_absolute(
                        &event_batches_dma_file,
                        &[second_last_batch_pos],
                        self.config.max_chunk_size,
                    ).await?;

                    let second_last_actual_crc = crc32fast::hash(&second_last_batch_bytes[0]);

                    if second_last_actual_crc == second_last_meta.event_batch_metadata.events_crc {
                        // Second-to-last is valid, trim to that point
                        let trim_metadata_pos = file_len_metadata - rec_size;
                        let trim_event_batch_pos = second_last_meta.event_batch_absolute_position + second_last_meta.event_batch_metadata.compressed_size;

                        metadata_dma_file.truncate(trim_metadata_pos).await?;
                        event_batches_dma_file.truncate(trim_event_batch_pos).await?;

                        file_len_metadata = trim_metadata_pos;
                        file_len_event_batch = trim_event_batch_pos;

                        // Remove last entry from uncached_metadata_set if it's there
                        if !uncached_metadata_set.is_empty() {
                            uncached_metadata_set.pop();
                        }
                    } else {
                        // Both last and second-to-last are corrupt
                        return Err(ReadError::CorruptEventBatch {
                            expected_crc: second_last_meta.event_batch_metadata.events_crc,
                            actual_crc: second_last_actual_crc,
                            event_batch_index: second_last_meta.event_batch_metadata.event_batch_index,
                            file_pos_event_batch: second_last_batch_pos_start_pos,
                            file_pos_metadata: file_len_metadata - (2 * rec_size),
                        });
                    }
                } else {
                    // Only one entry exists and it's corrupt
                    return Err(ReadError::CorruptEventBatch {
                        expected_crc: last_meta.event_batch_metadata.events_crc,
                        actual_crc: last_actual_crc,
                        event_batch_index: last_meta.event_batch_metadata.event_batch_index,
                        file_pos_event_batch: last_batch_pos_start_pos,
                        file_pos_metadata: file_len_metadata - rec_size,
                    });
                }
            }
        }

        let write_operations_data_requirements = WriteOperationsDataRequirements {
            data_cache: VecDeque::new(), // writer builds cache from newly appended data
            event_batches_dma_file,
            metadata_dma_file,
            file_len_event_batch,
            file_len_metadata,
            minimum_available_event_batch_index: minimum_available_event_batch_index.unwrap_or(1),
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
        aggregate_read_config: AggregateReadConfig,
        ) -> Result<ReadOperations, GlommioError<()>> {

        let metadata_dma_file = get_existing_file_as_dma(path_metadata).await?;
        let event_batches_dma_file = get_existing_file_as_dma(path_event_batches).await?;

        let metadata_dma_file_size = metadata_dma_file.file_size().await? as usize;

        let cache_capacity_bytes = std::cmp::min(metadata_dma_file_size, aggregate_read_config.max_data_cache_size_bytes);
        let cache_metadata = Vec::with_capacity(cache_capacity_bytes / METADATA_BATCH_SIZE_BYTES);
        
        Ok(ReadOperations {
            metadata_dma_file, 
            event_batches_dma_file, 
            cache_metadata,
            config: aggregate_read_config
        })
    }

    //minimum_available_event_batch_index comes from writer as it only changes during a trim operation
    pub async fn read(&self, minimum_available_event_batch_index: u64, file_len_metadata: u64, file_len_event_batch: u64, read_filters: &ReadFilters) -> Result<CacheableReadResult, ReadError> {
        
        // Use the helper to get metadata range
        let (uncached_metadata_set, cached_metadata_set_snapshot) = self.get_metadata_range(
            minimum_available_event_batch_index,
            read_filters.from_event_batch_index,
            file_len_metadata,
            file_len_event_batch,
            read_filters.to_event_batch_index,
        ).await?;

        // Handle empty result
        if uncached_metadata_set.is_empty() && cached_metadata_set_snapshot.is_empty() {
            return Ok(CacheableReadResult {
                uncached_metadata_set: Vec::new(),
                filtered_event_batches: Vec::new(),
                next_event_batch_index: None,
            });
        }

        // Build the complete, contiguous set of metadata entries
        let mut metadata_for_reading: Vec<&MetadataWithAbsolutePosition> = Vec::with_capacity(
            uncached_metadata_set.len() + cached_metadata_set_snapshot.len()
        );
        metadata_for_reading.extend(uncached_metadata_set.iter());
        metadata_for_reading.extend(cached_metadata_set_snapshot.iter());

        // Exclude metadata entries based on filters and apply max_bytes pagination
        let next_event_batch_index: Option<u64> =
            trim_end_if_exceeds_max_bytes(&mut metadata_for_reading, &read_filters, read_filters.max_bytes)?;

        // Read the actual event batches at specific positions in the file
        let object_positions: Vec<AbsoluteObjectPosition> = metadata_for_reading.iter()
            .map(|f| AbsoluteObjectPosition { 
                start_pos: f.event_batch_absolute_position, 
                end_pos: f.event_batch_absolute_position + f.event_batch_metadata.compressed_size 
            }).collect();

        let event_batches_bytes_set = read_objects::read_objects_absolute(
            &self.event_batches_dma_file, 
            &object_positions, 
            self.config.max_chunk_size).await?;

        // Deserialize and filter event batches
        assert!(event_batches_bytes_set.len() == metadata_for_reading.len());

        let mut index = 0;
        let mut filtered_event_batches: Vec<EventBatchItem> = Vec::with_capacity(event_batches_bytes_set.len());
        for event_batch_bytes in event_batches_bytes_set.iter() {

            let metadata = &metadata_for_reading[index].event_batch_metadata;
            let actual_crc = crc32fast::hash(&event_batch_bytes);

            if actual_crc != metadata.events_crc {
                return Err(ReadError::CorruptEventBatch { 
                    expected_crc: metadata.events_crc, 
                    actual_crc, 
                    event_batch_index: metadata.event_batch_index,
                    file_pos_event_batch: object_positions[index].start_pos,
                    file_pos_metadata: metadata_for_reading[index].event_batch_absolute_position
                })
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
    if read_filters.include_event_type_1.is_some() 
        || read_filters.include_event_type_2.is_some() 
        || read_filters.include_event_type_3.is_some() 
        || read_filters.include_event_type_4.is_some() {
        
        let include_event_types = [
            read_filters.include_event_type_1, 
            read_filters.include_event_type_2, 
            read_filters.include_event_type_3, 
            read_filters.include_event_type_4
        ];
        
        event_batch
            .events
            .retain(|event| {
                include_event_types
                    .iter()
                    .filter_map(|&et| et)
                    .any(|include_type| event.event_type_major == include_type)
            });
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

    if filters.include_event_type_1.is_some() ||
        filters.include_event_type_2.is_some() ||
        filters.include_event_type_3.is_some() ||
        filters.include_event_type_4.is_some() {
        
        let include_event_types = [filters.include_event_type_1, filters.include_event_type_2, filters.include_event_type_3, filters.include_event_type_4];

        if !check_event_types_match(&metadata.event_types_data, &include_event_types) {
            return false;
        }
    }

    true
}

fn check_event_types_match(event_types_data: &EventTypesData, include_event_types: &[Option<u64>; 4]) -> bool {
    match event_types_data {
        EventTypesData::Direct(event_types) => {
            // Check if any of the required types are in the batch's event types
            include_event_types
                .iter()
                .filter_map(|&et| et)  // Filter out None values
                .any(|include_type| event_types.contains(&include_type))
        }
        EventTypesData::Bloom(bloom_bytes) => {
            // Create bloom filter and test each required type
            let bloom = bloom_filter_from_bytes(bloom_bytes);
            include_event_types
                .iter()
                .filter_map(|&et| et)  // Filter out None values
                .any(|include_type| bloom.contains(&include_type.to_le_bytes()))
        }
    }
}

fn bloom_filter_from_bytes(bloom_bytes: &[u64; BLOOM_BYTES / 8]) -> BloomFilter {
    BloomFilter::from_vec(bloom_bytes.to_vec())
        .seed(&BLOOM_HASH_SEED)
        .hashes(BLOOM_HASH_COUNT)
}

fn apply_max_bytes_pagination(
    metadata_for_reading: &mut Vec<&MetadataWithAbsolutePosition>,
    max_bytes: Option<usize>,
) -> Result<Option<u64>, ReadError> {
    let max_bytes = match max_bytes {
        Some(limit) => limit as u64,
        None => return Ok(None),
    };

    if metadata_for_reading.is_empty() {
        return Ok(None);
    }

    let mut cumulative_size: u64 = 0;
    let mut cut_index: Option<usize> = None;

    for (index, batch) in metadata_for_reading.iter().enumerate() {
        cumulative_size += batch.event_batch_metadata.compressed_size;

        if cumulative_size > max_bytes {
            cut_index = Some(index);
            break;
        }
    }

    if let Some(index) = cut_index {
        let next_event_batch_index = if index < metadata_for_reading.len() {
            Some(metadata_for_reading[index].event_batch_metadata.event_batch_index)
        } else {
            None
        };

        metadata_for_reading.truncate(index);

        if metadata_for_reading.is_empty() {
            return Err(ReadError::MaxBytesTooSmall {
                current_max_bytes: max_bytes,
                required_max_bytes: cumulative_size
            });
        }

        Ok(next_event_batch_index)
    } else {
        Ok(None)
    }
}