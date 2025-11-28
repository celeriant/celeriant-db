use eventplanedb_structures::{
    compression_type::CompressionType, constants::METADATA_BATCH_SIZE_BYTES, event_batch_item::EventBatchItem, read_filters::ReadFilters, read_result::ReadResult, version_aware_wire_format::{
        deserialize_event_batch_metadata_versioned, deserialize_event_batch_versioned,
    }
};
use glommio::{GlommioError, io::DmaFile};
use std::collections::HashMap;

use crate::{
    files::{
        read_fixed_records_visit_const, read_objects_absolute::{self, AbsoluteObjectPosition}
    },
    read_operations::read_structures::{
        AggregateReadConfig, MetadataWithAbsolutePosition,
        WriteOperationsDataRequirements,
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
    pub metadata_dma_file: Option<DmaFile>,
    pub event_batches_dma_file: Option<DmaFile>,
    pub config: AggregateReadConfig,
}

#[allow(async_fn_in_trait)]
pub trait ReadOperations {

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

    /// Updates internal file handles after a trim or prepend operation.
    ///
    /// Called after the writer has trimmed files to update this reader's
    /// file handles and clear any invalidated cache entries.
    ///
    /// # Parameters
    /// * `metadata_dma_file` - New DMA file handle for metadata
    /// * `event_batches_dma_file` - New DMA file handle for event batches
    async fn replace_dma_files(&mut self, metadata_dma_file: DmaFile, event_batches_dma_file: DmaFile) -> Result<WriteOperationsDataRequirements, ReadError>;

    /// Retrieves state information required by write operations.
    ///
    /// Scans metadata to determine the next event/batch indices, client event indexes,
    /// and validates data integrity. Also primes the metadata cache with recent entries.
    ///
    /// # Returns
    /// `WriteOperationsDataRequirements` containing state for writer
    async fn get_write_operations_data_requirements(
        &self,
    ) -> Result<WriteOperationsDataRequirements, ReadError>;

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
    /// `ReadResult` containing filtered events, and pagination info
    async fn read(
        &self,
        minimum_available_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
        read_filters: &ReadFilters,
        max_bytes: Option<usize>,
    ) -> Result<ReadResult, ReadError>;

    async fn close(&mut self) -> Result<(), GlommioError<()>>;
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
    pub fn new(
        metadata_dma_file: DmaFile,
        event_batches_dma_file: DmaFile,
        aggregate_read_config: AggregateReadConfig,
    ) -> ReadOperationsWithDmaFiles {
        ReadOperationsWithDmaFiles {
            metadata_dma_file: Some(metadata_dma_file),
            event_batches_dma_file: Some(event_batches_dma_file),
            config: aggregate_read_config,
        }
    }

    async fn get_metadata_range(
        &self,
        minimum_available_event_batch_index: u64,
        from_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
        to_event_batch_index: Option<u64>, // None means read to end of file
    ) -> Result<Vec<MetadataWithAbsolutePosition>, ReadError> {

        if self.metadata_dma_file.is_none() {
            return Err(ReadError::NotExists);
        }
        let metadata_dma_file = self.metadata_dma_file.as_ref().unwrap();

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
            return Ok(Vec::new());
        }

        // Calculate read boundaries
        // let metadata_read_to_bytes = if let Some(to_index) = to_event_batch_index {
        //     let to_bytes = (to_index.saturating_sub(minimum_available_event_batch_index) + 1)
        //         * METADATA_BATCH_SIZE_BYTES as u64;
        //     std::cmp::min(to_bytes, file_len_metadata)
        // } else {
        //     file_len_metadata
        // };

        // let actual_read_to = std::cmp::min(metadata_read_to_bytes, file_len_metadata);
        let metadata_count = ((file_len_metadata.saturating_sub(metadata_read_from_bytes))
            / METADATA_BATCH_SIZE_BYTES as u64) as usize;
        let mut metadata_set: Vec<MetadataWithAbsolutePosition> =
            Vec::with_capacity(metadata_count);

        // Absolute position reference from cache
        let mut event_batch_absolute_position = file_len_event_batch;

        // Read uncached metadata from disk
        if metadata_count > 0 {
            read_fixed_records_visit_const::read_fixed_records_visit_const::<
                METADATA_BATCH_SIZE_BYTES,
                ReadError,
            >(
                metadata_dma_file,
                file_len_metadata,
                metadata_read_from_bytes,
                Some(file_len_metadata),
                self.config.max_chunk_size,
                |metadata_bytes| {
                    let (event_batch_metadata, format_version_on_disk) =
                        deserialize_event_batch_metadata_versioned(metadata_bytes)?;
                    metadata_set.push(MetadataWithAbsolutePosition {
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
        for metadata_with_absolute_position in metadata_set.iter_mut().rev() {
            event_batch_absolute_position -= metadata_with_absolute_position
                .event_batch_metadata
                .compressed_size;
            metadata_with_absolute_position.event_batch_absolute_position =
                event_batch_absolute_position;
        }

        // Remove any metadata entries beyond to_event_batch_index
        if let Some(to_index) = to_event_batch_index {
            metadata_set.retain(|m| m.event_batch_metadata.event_batch_index <= to_index);
        }

        Ok(metadata_set)
    }
}

impl ReadOperations for ReadOperationsWithDmaFiles {
    
    async fn close(&mut self) -> Result<(), GlommioError<()>> {
        if let Some(metadata_dma_file) = self.metadata_dma_file.take() {
            metadata_dma_file.close().await?;
        }

        if let Some(event_batches_dma_file) = self.event_batches_dma_file.take() {
            event_batches_dma_file.close().await?;
        }

        Ok(())
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
        let metadata_set = self
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

        for metadata in metadata_set.iter() {
            event_batch_position += metadata.event_batch_metadata.compressed_size;
        }

        Ok(FilePositions {
            metadata_position,
            event_batch_position,
        })
    }

    async fn replace_dma_files(&mut self, metadata_dma_file: DmaFile, event_batches_dma_file: DmaFile) -> Result<WriteOperationsDataRequirements, ReadError> {
        let old_metadata_file = std::mem::replace(&mut self.metadata_dma_file, Some(metadata_dma_file));
        if let Some(old_metadata_file) = old_metadata_file { old_metadata_file.close().await?; }

        let old_event_batch_file = std::mem::replace(&mut self.event_batches_dma_file, Some(event_batches_dma_file));
        if let Some(old_event_batch_file) = old_event_batch_file { old_event_batch_file.close().await?; }

        Ok(self.get_write_operations_data_requirements().await?)
    }

    async fn get_write_operations_data_requirements(
        &self,
    ) -> Result<WriteOperationsDataRequirements, ReadError> {
        if self.metadata_dma_file.is_none() || self.event_batches_dma_file.is_none() {
            return Err(ReadError::NotExists);
        }

        let metadata_dma_file = self.metadata_dma_file.as_ref().unwrap();
        let event_batches_dma_file = self.event_batches_dma_file.as_ref().unwrap();

        let file_len_metadata = metadata_dma_file.file_size().await?;

        // No metadata in file => initial state
        if file_len_metadata == 0 {

            let metadata_buffer = vec![0u8; metadata_dma_file.alignment() as usize];
            let event_batch_buffer = vec![0u8; event_batches_dma_file.alignment() as usize];

            let write_operations_data_requirements = WriteOperationsDataRequirements {
                file_len_event_batch: 0,
                file_len_metadata,
                metadata_buffer,
                event_batch_buffer,
                minimum_available_event_batch_index: 0,
                next_event_index: 1,
                next_event_batch_index: 1,
                client_event_indexes: HashMap::new(),
            };
            return Ok(write_operations_data_requirements);
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

        // Determine uncached prefix size (in bytes) and record count
        let total_records = file_len_metadata / rec_size;

        // The reason we must go through all metadata is for client idempotency checks
        let mut client_event_indexes: HashMap<u128, u64> = HashMap::new();
        let mut metadata_entries: Vec<MetadataWithAbsolutePosition> = Vec::with_capacity(total_records as usize);
        let mut minimum_available_event_batch_index: Option<u64> = None;
        let mut event_batch_absolute_position: u64 = 0;

        read_fixed_records_visit_const::read_fixed_records_visit_const::<
            METADATA_BATCH_SIZE_BYTES,
            ReadError,
        >(
            metadata_dma_file,
            file_len_metadata,
            0,
            Some(file_len_metadata),
            self.config.max_chunk_size,
            |metadata_bytes| {
                let try_deser = deserialize_event_batch_metadata_versioned(metadata_bytes);

                // Check if the metadata_bytes buffer is all zeros
                if try_deser.is_err() {
                    if metadata_bytes.iter().all(|&byte| byte == 0) {
                        // Skip this entry as it is preallocated but unwritten space
                        return Ok(());
                    } else {
                        // Return the deserialization error if the buffer is not all zeros
                        return Err(try_deser.unwrap_err())?;
                    }
                }
                
                let (event_batch_metadata, format_version_on_disk) = try_deser.unwrap();

                if minimum_available_event_batch_index.is_none() {
                    minimum_available_event_batch_index = Some(event_batch_metadata.event_batch_index);
                }

                // Build client map (latest max index per client)
                client_event_indexes
                    .entry(event_batch_metadata.client_id)
                    .and_modify(|v| {
                        if event_batch_metadata.max_client_event_index > *v {
                            *v = event_batch_metadata.max_client_event_index;
                        }
                    })
                    .or_insert(event_batch_metadata.max_client_event_index);

                let compressed_size = event_batch_metadata.compressed_size;

                metadata_entries.push(MetadataWithAbsolutePosition {
                    event_batch_metadata,
                    event_batch_absolute_position,
                    format_version_on_disk,
                });

                event_batch_absolute_position += compressed_size;

                Ok(())
            },
        )
        .await?;

        let last_meta = metadata_entries
            .last()
            .expect("At least one metadata entry should exist");

        // Get remaining bytes from both files for writer buffers to satisfy alignment constraints
        let metadata_aligned_end = metadata_dma_file.align_down(file_len_metadata);
        let metadata_remainder_bytes = file_len_metadata.saturating_sub(metadata_aligned_end) as usize;
        let metadata_buffer = metadata_dma_file.read_at(metadata_aligned_end, metadata_remainder_bytes).await?;

        let last_event_batch_end_position = last_meta.event_batch_absolute_position + last_meta.event_batch_metadata.compressed_size;
        let event_batch_aligned_end = event_batches_dma_file.align_down(last_event_batch_end_position);
        let event_batch_remainder_bytes = last_event_batch_end_position.saturating_sub(event_batch_aligned_end) as usize;
        let event_batch_buffer = event_batches_dma_file.read_at(event_batch_aligned_end, event_batch_remainder_bytes).await?;

        let write_operations_data_requirements = WriteOperationsDataRequirements {
            file_len_event_batch: last_event_batch_end_position,
            file_len_metadata,
            metadata_buffer: metadata_buffer.to_vec(),
            event_batch_buffer: event_batch_buffer.to_vec(),
            minimum_available_event_batch_index: minimum_available_event_batch_index.unwrap_or(1),
            next_event_index: last_meta.event_batch_metadata.max_event_index.saturating_add(1),
            next_event_batch_index: last_meta.event_batch_metadata.event_batch_index.saturating_add(1),
            client_event_indexes,
        };

        Ok(write_operations_data_requirements)
    }

    //minimum_available_event_batch_index comes from writer as it only changes during a trim operation
    async fn read(
        &self,
        minimum_available_event_batch_index: u64,
        file_len_metadata: u64,
        file_len_event_batch: u64,
        read_filters: &ReadFilters,
        max_bytes: Option<usize>,
    ) -> Result<ReadResult, ReadError> {

        if self.event_batches_dma_file.is_none() {
            return Err(ReadError::NotExists);
        }
        let event_batches_dma_file = self.event_batches_dma_file.as_ref().unwrap();

        // Use the helper to get metadata range
        let mut metadata_for_reading = self
            .get_metadata_range(
                minimum_available_event_batch_index,
                read_filters.from_event_batch_index,
                file_len_metadata,
                file_len_event_batch,
                read_filters.to_event_batch_index,
            )
            .await?;

        // Handle empty result
        if metadata_for_reading.is_empty() {
            return Ok(ReadResult {
                event_batches: Vec::new(),
                next_event_batch_index: None,
            });
        }

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
            event_batches_dma_file,
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
            let actual_crc = crc32c::crc32c(&event_batch_bytes);

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

        Ok(ReadResult {
            event_batches: filtered_event_batches,
            next_event_batch_index,
        })
    }
}
