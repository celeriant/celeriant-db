use eventplanedb_structures::{
    compression_type::CompressionType,
    constants::METADATA_BATCH_SIZE_BYTES,
    event_batch_item::EventBatchItem,
    read_filters::ReadFilters,
    version_aware_wire_format::{
        deserialize_event_batch_metadata_versioned, deserialize_event_batch_versioned,
    },
};
use glommio::io::{DmaFile, OpenOptions};
use std::{collections::HashMap, path::Path};

use crate::{
    files::{
        read_fixed_records_visit_const,
        read_objects_absolute::{self, AbsoluteObjectPosition},
    },
    read_operations::read_structures::{
        AggregateReadConfig, CacheableReadResult, MetadataWithAbsolutePosition,
        WriteOperationsDataRequirements, WriteOperationsDataRequirementsAndCachedData,
    },
};
use crate::{
    read_operations::in_memory_filtering::apply_event_filters,
    read_operations::{
        in_memory_filtering::trim_end_if_exceeds_max_bytes, read_error::ReadError,
        read_structures::FilePositions,
    },
};

pub struct ReadOperationsWithDmaFiles {
    pub metadata_dma_file: DmaFile,
    pub event_batches_dma_file: DmaFile,
    pub cache_metadata: Vec<MetadataWithAbsolutePosition>,
    pub config: AggregateReadConfig,
}

#[allow(async_fn_in_trait)]
pub trait ReadOperations {
    fn update_max_data_cache_size_bytes(&mut self, value: usize);

    /// Calculates the file positions (in bytes) for trimming operation.
    ///
    /// Computes where to trim both metadata and event batch files to remove
    /// all data before the specified keep_from_event_batch_index.
    ///
    /// # Parameters
    /// * `minimum_available_event_batch_index` - Current oldest available event batch index
    /// * `keep_from_event_batch_index` - First event batch index to keep (all before will be trimmed)
    /// * `file_len_metadata` - Current size of the metadata file in bytes
    /// * `file_len_event_batch` - Current size of the event batches file in bytes
    ///
    /// # Returns
    /// Starting file positions (metadata_file_position, event_batch_file_position) in bytes
    async fn get_file_positions(
        &self,
        minimum_available_event_batch_index: u64,
        keep_from_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
    ) -> Result<FilePositions, ReadError>;

    /// Updates internal file handles after a trim operation.
    ///
    /// Called after the writer has trimmed files to update this reader's
    /// file handles and clear any invalidated cache entries.
    ///
    /// # Parameters
    /// * `metadata_dma_file` - New DMA file handle for metadata
    /// * `event_batches_dma_file` - New DMA file handle for event batches
    fn trim_start(&mut self, metadata_dma_file: DmaFile, event_batches_dma_file: DmaFile);

    /// Retrieves state information required by write operations.
    ///
    /// Scans metadata to determine the next event/batch indices, client event indexes,
    /// and validates data integrity. Also primes the metadata cache with recent entries.
    ///
    /// # Returns
    /// `WriteOperationsDataRequirementsAndCachedData` containing state for writer
    /// plus uncached metadata to update the cache
    async fn get_write_operations_data_requirements(
        &self,
    ) -> Result<WriteOperationsDataRequirementsAndCachedData, ReadError>;

    /// Reads and filters event batches based on provided criteria.
    ///
    /// Applies metadata-level filters first to minimize I/O, then reads matching
    /// event batches and applies event-level filters (event type, timestamps, indices).
    ///
    /// # Parameters
    /// * `minimum_available_event_batch_index` - The oldest event batch index still available
    /// * `file_len_metadata` - Current size of the metadata file in bytes
    /// * `file_len_event_batch` - Current size of the event batches file in bytes
    /// * `read_filters` - Filter criteria for events and batches
    ///
    /// # Returns
    /// `CacheableReadResult` containing uncached metadata, filtered events, and pagination info
    async fn read(
        &self,
        minimum_available_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
        read_filters: &ReadFilters,
        max_bytes: Option<usize>,
    ) -> Result<CacheableReadResult, ReadError>;

    /// Updates the internal metadata cache with newly read entries.
    ///
    /// Adds uncached metadata to the front of the cache (oldest entries) and
    /// evicts oldest entries if the cache exceeds its size limit.
    ///
    /// # Parameters
    /// * `uncached_metadata_set` - Newly read metadata to add to cache
    fn update_metadata_cache(&mut self, uncached_metadata_set: Vec<MetadataWithAbsolutePosition>);
}

impl ReadOperationsWithDmaFiles {
    /// Opens an existing aggregate for reading.
    ///
    /// # Parameters
    /// * `path_metadata` - Path to the metadata file
    /// * `path_event_batches` - Path to the event batches file
    /// * `aggregate_read_config` - Configuration for read operations (cache size, chunk size, etc.)
    ///
    /// # Returns
    /// New `ReadOperations` instance ready for reading
    pub async fn open<P: AsRef<Path>>(
        base_folder: P,
        path_metadata: P,
        path_event_batches: P,
        create_if_not_exists: bool,
        aggregate_read_config: AggregateReadConfig,
    ) -> Result<ReadOperationsWithDmaFiles, ReadError> {
        if create_if_not_exists {
            std::fs::create_dir_all(&base_folder).map_err(|error| {
                ReadError::CannotCreateFolders {
                    path: base_folder.as_ref().to_string_lossy().to_string(),
                    error,
                }
            })?;
        }

        let metadata_dma_file =
            get_existing_file_as_dma(path_metadata, create_if_not_exists).await?;
        let event_batches_dma_file =
            get_existing_file_as_dma(path_event_batches, create_if_not_exists).await?;

        let metadata_dma_file_size = metadata_dma_file.file_size().await? as usize;

        let cache_capacity_bytes = std::cmp::min(
            metadata_dma_file_size,
            aggregate_read_config.max_data_cache_size_bytes,
        );
        let cache_metadata = Vec::with_capacity(cache_capacity_bytes / METADATA_BATCH_SIZE_BYTES);

        Ok(ReadOperationsWithDmaFiles {
            metadata_dma_file,
            event_batches_dma_file,
            cache_metadata,
            config: aggregate_read_config,
        })
    }

    async fn get_metadata_range(
        &self,
        minimum_available_event_batch_index: u64,
        from_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
        to_event_batch_index: Option<u64>, // None means read to end of file
    ) -> Result<
        (
            Vec<MetadataWithAbsolutePosition>,
            Vec<&MetadataWithAbsolutePosition>,
        ),
        ReadError,
    > {
        if minimum_available_event_batch_index > from_event_batch_index {
            return Err(ReadError::UnavailableBatchIndex {
                minimum_available_event_batch_index,
                requested_event_batch_index: from_event_batch_index,
            });
        }

        let metadata_read_from_bytes = from_event_batch_index
            .saturating_sub(minimum_available_event_batch_index)
            * METADATA_BATCH_SIZE_BYTES as u64;

        // Handle scenario where we're reading past the end of the file
        if file_len_metadata.saturating_sub(metadata_read_from_bytes) == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        // Calculate read boundaries
        let metadata_read_to_bytes = if let Some(to_index) = to_event_batch_index {
            let to_bytes = (to_index.saturating_sub(minimum_available_event_batch_index) + 1)
                * METADATA_BATCH_SIZE_BYTES as u64;
            std::cmp::min(to_bytes, file_len_metadata)
        } else {
            file_len_metadata
        };

        // Take a snapshot of the cache
        let cached_metadata_set_snapshot: Vec<&MetadataWithAbsolutePosition> =
            self.cache_metadata.iter().collect();

        // Determine uncached range
        let uncached_metadata_read_to_bytes = file_len_metadata.saturating_sub(
            cached_metadata_set_snapshot.len() as u64 * METADATA_BATCH_SIZE_BYTES as u64,
        );

        let actual_read_to = std::cmp::min(metadata_read_to_bytes, uncached_metadata_read_to_bytes);
        let uncached_metadata_count = ((actual_read_to.saturating_sub(metadata_read_from_bytes))
            / METADATA_BATCH_SIZE_BYTES as u64) as usize;
        let mut uncached_metadata_set: Vec<MetadataWithAbsolutePosition> =
            Vec::with_capacity(uncached_metadata_count);

        // Absolute position reference from cache
        let mut event_batch_absolute_position = if !cached_metadata_set_snapshot.is_empty() {
            cached_metadata_set_snapshot[0].event_batch_absolute_position
        } else {
            file_len_event_batch
        };

        // Read uncached metadata from disk
        if uncached_metadata_count > 0 {
            read_fixed_records_visit_const::read_fixed_records_visit_const::<
                METADATA_BATCH_SIZE_BYTES,
                ReadError,
            >(
                &self.metadata_dma_file,
                file_len_metadata,
                metadata_read_from_bytes,
                Some(actual_read_to),
                self.config.max_chunk_size,
                |metadata_bytes| {
                    let (event_batch_metadata, format_version_on_disk) =
                        deserialize_event_batch_metadata_versioned(metadata_bytes)?;
                    uncached_metadata_set.push(MetadataWithAbsolutePosition {
                        event_batch_metadata,
                        event_batch_absolute_position: 0,
                        format_version_on_disk,
                    });
                    Ok(())
                },
            )
            .await?;
        }

        // Calculate absolute positions
        for metadata_with_absolute_position in uncached_metadata_set.iter_mut().rev() {
            event_batch_absolute_position -= metadata_with_absolute_position
                .event_batch_metadata
                .compressed_size;
            metadata_with_absolute_position.event_batch_absolute_position =
                event_batch_absolute_position;
        }

        Ok((uncached_metadata_set, cached_metadata_set_snapshot))
    }
}

impl ReadOperations for ReadOperationsWithDmaFiles {
    fn update_max_data_cache_size_bytes(&mut self, value: usize) {
        self.config.max_data_cache_size_bytes = value;

        // Proactively trim cache if it exceeds the new size
        let mut total_size = 0;
        let mut keep_count = 0;

        for _metadata in self.cache_metadata.iter() {
            let entry_size = METADATA_BATCH_SIZE_BYTES as usize;
            if total_size + entry_size > value {
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

    async fn get_file_positions(
        &self,
        minimum_available_event_batch_index: u64,
        keep_from_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
    ) -> Result<FilePositions, ReadError> {
        let metadata_position = (keep_from_event_batch_index - minimum_available_event_batch_index)
            * METADATA_BATCH_SIZE_BYTES as u64;

        // Get all metadata from start to keep_from_event_batch_index (exclusive)
        let (uncached_metadata_set, cached_metadata_set_snapshot) = self
            .get_metadata_range(
                minimum_available_event_batch_index,
                minimum_available_event_batch_index,
                file_len_metadata,
                file_len_event_batch,
                Some(keep_from_event_batch_index.saturating_sub(1)),
            )
            .await?;

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

        Ok(FilePositions {
            metadata_position,
            event_batch_position,
        })
    }

    fn trim_start(&mut self, metadata_dma_file: DmaFile, event_batches_dma_file: DmaFile) {
        self.metadata_dma_file = metadata_dma_file;
        self.event_batches_dma_file = event_batches_dma_file;
        //TODO: Could be more specific about what to clear
        self.cache_metadata.clear();
    }

    async fn get_write_operations_data_requirements(
        &self,
    ) -> Result<WriteOperationsDataRequirementsAndCachedData, ReadError> {
        let mut file_len_metadata = self.metadata_dma_file.file_size().await?;
        let mut file_len_event_batch = self.event_batches_dma_file.file_size().await?;

        // No metadata in file => initial state
        if file_len_metadata == 0 {
            let write_operations_data_requirements = WriteOperationsDataRequirements {
                file_len_event_batch,
                file_len_metadata,
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
        // This is a critical failure as we have already confirmed to client that we wrote this data
        if file_len_metadata % rec_size != 0 {
            let aligned_len = (file_len_metadata / rec_size) * rec_size;
            return Err(ReadError::CorruptMetadata {
                file_pos_metadata: aligned_len,
            });
        }

        // Snapshot cache (tail of file)
        let cached_snapshot: Vec<&MetadataWithAbsolutePosition> =
            self.cache_metadata.iter().collect();
        let cached_count = cached_snapshot.len() as u64;

        // Determine uncached prefix size (in bytes) and record count
        let total_records = file_len_metadata / rec_size;
        let cached_bytes = std::cmp::min(cached_count, total_records) * rec_size;
        let uncached_bytes = file_len_metadata.saturating_sub(cached_bytes);
        let uncached_records = (uncached_bytes / rec_size) as usize;

        // Bound how many we return to prime read cache
        let metadata_cache_capacity =
            self.config.max_data_cache_size_bytes / METADATA_BATCH_SIZE_BYTES;
        let return_count = std::cmp::min(uncached_records, metadata_cache_capacity);

        // Single pass over the uncached prefix to:
        // - build client_event_indexes
        // - track last scanned metadata (for next_* if no cache)
        // - collect the last `return_count` items of the scanned prefix to return
        let mut client_event_indexes: HashMap<u128, u64> = HashMap::new();

        // Use a small ring buffer for the tail we want to return
        let mut ring: std::collections::VecDeque<MetadataWithAbsolutePosition> =
            std::collections::VecDeque::with_capacity(return_count.max(1));

        let mut minimum_available_event_batch_index: Option<u64> = None;

        if uncached_bytes > 0 {
            read_fixed_records_visit_const::read_fixed_records_visit_const::<
                METADATA_BATCH_SIZE_BYTES,
                ReadError,
            >(
                &self.metadata_dma_file,
                file_len_metadata,
                0,
                Some(uncached_bytes),
                self.config.max_chunk_size,
                |metadata_bytes| {
                    let (meta, format_version_on_disk) =
                        deserialize_event_batch_metadata_versioned(metadata_bytes)?;

                    if minimum_available_event_batch_index.is_none() {
                        minimum_available_event_batch_index = Some(meta.event_batch_index);
                    }

                    // Build client map (latest max index per client)
                    client_event_indexes
                        .entry(meta.client_id)
                        .and_modify(|v| {
                            if meta.max_client_event_index > *v {
                                *v = meta.max_client_event_index;
                            }
                        })
                        .or_insert(meta.max_client_event_index);

                    // Keep the tail subset to return
                    if return_count > 0 {
                        ring.push_back(MetadataWithAbsolutePosition {
                            event_batch_metadata: meta,
                            event_batch_absolute_position: 0,
                            format_version_on_disk,
                        });
                        if ring.len() > return_count {
                            ring.pop_front();
                        }
                    }

                    Ok(())
                },
            )
            .await?;
        }

        if !self.cache_metadata.is_empty()
            && self
                .cache_metadata
                .first()
                .unwrap()
                .event_batch_absolute_position
                == 0
        {
            // Can get min event batch from the cache
            minimum_available_event_batch_index = Some(
                self.cache_metadata
                    .first()
                    .unwrap()
                    .event_batch_metadata
                    .event_batch_index,
            );
        }

        assert!(
            minimum_available_event_batch_index.is_some(),
            "Should get min event batch index"
        );

        // Materialize uncached metadata set and assign absolute positions using the first cached
        // item's absolute position as the base (or EOF if no cache).
        let mut uncached_metadata_set: Vec<MetadataWithAbsolutePosition> =
            ring.into_iter().collect();

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

        // First time reading (no cache) so check for corruption
        if cached_snapshot.len() == 0
            && let Some(last_meta) = last_metadata
        {
            // Read the last event batch
            let last_batch_pos = AbsoluteObjectPosition {
                start_pos: last_meta.event_batch_absolute_position,
                end_pos: last_meta.event_batch_absolute_position
                    + last_meta.event_batch_metadata.compressed_size,
            };

            let last_batch_pos_start_pos = last_batch_pos.start_pos;

            let last_batch_bytes = read_objects_absolute::read_objects_absolute(
                &self.event_batches_dma_file,
                file_len_event_batch,
                &[last_batch_pos],
                self.config.max_chunk_size,
            )
            .await?;

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
                        end_pos: second_last_meta.event_batch_absolute_position
                            + second_last_meta.event_batch_metadata.compressed_size,
                    };

                    let second_last_batch_pos_start_pos = second_last_batch_pos.start_pos;

                    let second_last_batch_bytes = read_objects_absolute::read_objects_absolute(
                        &self.event_batches_dma_file,
                        file_len_event_batch,
                        &[second_last_batch_pos],
                        self.config.max_chunk_size,
                    )
                    .await?;

                    let second_last_actual_crc = crc32fast::hash(&second_last_batch_bytes[0]);

                    if second_last_actual_crc == second_last_meta.event_batch_metadata.events_crc {
                        // Second-to-last is valid, trim to that point
                        let trim_metadata_pos = file_len_metadata - rec_size;
                        let trim_event_batch_pos = second_last_meta.event_batch_absolute_position
                            + second_last_meta.event_batch_metadata.compressed_size;

                        self.metadata_dma_file.truncate(trim_metadata_pos).await?;
                        self.event_batches_dma_file
                            .truncate(trim_event_batch_pos)
                            .await?;

                        file_len_metadata = trim_metadata_pos;
                        file_len_event_batch = trim_event_batch_pos;

                        // Remove last entry from uncached_metadata_set if it's there
                        if !uncached_metadata_set.is_empty() {
                            uncached_metadata_set.pop();
                        }

                        //Rebuild the client_id to last client_event_index cache
                        client_event_indexes.clear();
                        for meta in &uncached_metadata_set {
                            client_event_indexes
                                .entry(meta.event_batch_metadata.client_id)
                                .and_modify(|v| {
                                    if meta.event_batch_metadata.max_client_event_index > *v {
                                        *v = meta.event_batch_metadata.max_client_event_index;
                                    }
                                })
                                .or_insert(meta.event_batch_metadata.max_client_event_index);
                        }
                    } else {
                        // Both last and second-to-last are corrupt
                        return Err(ReadError::CorruptEventBatch {
                            expected_crc: second_last_meta.event_batch_metadata.events_crc,
                            actual_crc: second_last_actual_crc,
                            event_batch_index: second_last_meta
                                .event_batch_metadata
                                .event_batch_index,
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

        // Merge cached client indexes
        for c in cached_snapshot.iter() {
            let meta = &c.event_batch_metadata;
            client_event_indexes
                .entry(meta.client_id)
                .and_modify(|v| {
                    if meta.max_client_event_index > *v {
                        *v = meta.max_client_event_index;
                    }
                })
                .or_insert(meta.max_client_event_index);
        }

        // Compute next_* from latest record (prefer cached tail if available)
        let (next_event_index, next_event_batch_index) =
            if let Some(last_cached) = cached_snapshot.last() {
                let m = &last_cached.event_batch_metadata;
                (
                    m.max_event_index.saturating_add(1),
                    m.event_batch_index.saturating_add(1),
                )
            } else if let Some(last_uncached) = uncached_metadata_set.last() {
                let m = &last_uncached.event_batch_metadata;
                (
                    m.max_event_index.saturating_add(1),
                    m.event_batch_index.saturating_add(1),
                )
            } else {
                // Shouldn't happen since file_len_metadata > 0, but be safe
                (1, 1)
            };

        let write_operations_data_requirements = WriteOperationsDataRequirements {
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

    //minimum_available_event_batch_index comes from writer as it only changes during a trim operation
    async fn read(
        &self,
        minimum_available_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
        read_filters: &ReadFilters,
        max_bytes: Option<usize>,
    ) -> Result<CacheableReadResult, ReadError> {
        // Use the helper to get metadata range
        let (uncached_metadata_set, cached_metadata_set_snapshot) = self
            .get_metadata_range(
                minimum_available_event_batch_index,
                read_filters.from_event_batch_index,
                file_len_metadata,
                file_len_event_batch,
                read_filters.to_event_batch_index,
            )
            .await?;

        // Handle empty result
        if uncached_metadata_set.is_empty() && cached_metadata_set_snapshot.is_empty() {
            return Ok(CacheableReadResult {
                uncached_metadata_set: Vec::new(),
                filtered_event_batches: Vec::new(),
                next_event_batch_index: None,
            });
        }

        // Build the complete, contiguous set of metadata entries
        let mut metadata_for_reading: Vec<&MetadataWithAbsolutePosition> =
            Vec::with_capacity(uncached_metadata_set.len() + cached_metadata_set_snapshot.len());
        metadata_for_reading.extend(uncached_metadata_set.iter());
        metadata_for_reading.extend(cached_metadata_set_snapshot.iter());

        // Exclude metadata entries based on filters and apply max_bytes pagination
        let next_event_batch_index: Option<u64> =
            trim_end_if_exceeds_max_bytes(&mut metadata_for_reading, &read_filters, max_bytes)?;

        // Read the actual event batches at specific positions in the file
        let object_positions: Vec<AbsoluteObjectPosition> = metadata_for_reading
            .iter()
            .map(|f| AbsoluteObjectPosition {
                start_pos: f.event_batch_absolute_position,
                end_pos: f.event_batch_absolute_position + f.event_batch_metadata.compressed_size,
            })
            .collect();

        let event_batches_bytes_set = read_objects_absolute::read_objects_absolute(
            &self.event_batches_dma_file,
            file_len_event_batch,
            &object_positions,
            self.config.max_chunk_size,
        )
        .await?;

        // Deserialize and filter event batches
        assert!(event_batches_bytes_set.len() == metadata_for_reading.len());

        let mut index = 0;
        let mut filtered_event_batches: Vec<EventBatchItem> =
            Vec::with_capacity(event_batches_bytes_set.len());
        for event_batch_bytes in event_batches_bytes_set.iter() {
            let metadata = &metadata_for_reading[index].event_batch_metadata;
            let format_version_on_disk = metadata_for_reading[index].format_version_on_disk;
            let actual_crc = crc32fast::hash(&event_batch_bytes);

            if actual_crc != metadata.events_crc {
                return Err(ReadError::CorruptEventBatch {
                    expected_crc: metadata.events_crc,
                    actual_crc,
                    event_batch_index: metadata.event_batch_index,
                    file_pos_event_batch: object_positions[index].start_pos,
                    file_pos_metadata: metadata_for_reading[index].event_batch_absolute_position,
                });
            }

            let compression_type = CompressionType::from_tuple(metadata.compression_type, None);
            let mut event_batch = deserialize_event_batch_versioned(
                &event_batch_bytes,
                compression_type,
                metadata.uncompressed_size as usize,
                format_version_on_disk,
            )?;

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

    fn update_metadata_cache(
        &mut self,
        mut uncached_metadata_set: Vec<MetadataWithAbsolutePosition>,
    ) {
        if uncached_metadata_set.is_empty() {
            return;
        }

        // Find split point - how many items from uncached_metadata_set to keep
        let items_to_keep = if let Some(first_cached) = self.cache_metadata.first() {
            let min_cached_index = first_cached.event_batch_metadata.event_batch_index;
            // Find first item >= min_cached_index and keep everything before it
            uncached_metadata_set
                .iter()
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

async fn get_existing_file_as_dma<P: AsRef<Path>>(
    path: P,
    create_if_not_exists: bool,
) -> Result<DmaFile, ReadError> {
    let dma_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(create_if_not_exists)
        .append(false)
        .dma_open(path)
        .await?;

    Ok(dma_file)
}
