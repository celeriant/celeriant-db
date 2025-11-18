use std::{collections::{HashMap, HashSet, VecDeque}, fmt::Write, path::Path};

use eventplanedb_structures::{
    compression_type::CompressionType, constants::{BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED, METADATA_BATCH_SIZE_BYTES}, event_batch_item::EventBatchItem, event_batch_metadata::{EventBatchMetadata, EventTypesData}, event_item::EventItem, read_filters::ReadFilters, version_aware_wire_format::to_wire_format_fixed_with_version, wire_format::to_wire_format_variable, write_result::WriteResult
};
use fastbloom::BloomFilter;
use glommio::{GlommioError, io::{DmaFile, OpenOptions}};

use crate::{
    read_operations::{
        in_memory_filtering::{apply_event_filters, is_include_batch}, read_structures::{CacheableReadResult, WriteOperationsDataRequirements}
    },
    write_operations::{
        write_error::WriteError,
        write_structures::{AggregateWriteConfig, WriteOptions},
    },
};

pub struct CacheItem {
    event_batch_item: EventBatchItem,
    event_batch_metadata: EventBatchMetadata
}

pub struct WriteOperationsWithDmaFile {
    pub metadata_dma_file: DmaFile,
    pub event_batches_dma_file: DmaFile,
    pub data_cache: VecDeque<CacheItem>,
    total_cache_size_bytes: usize,
    pub minimum_available_event_batch_index: u64,
    next_event_index: u64,
    pub next_event_batch_index: u64,
    client_event_indexes: HashMap<u128, u64>,
    max_data_cache_size_bytes: usize,
    cache_trim_factor: usize,
    max_chunk_size: usize,
    bloom_filter: BloomFilter,
    event_type_dedup: HashSet<u64>,
    append_event_batch_queue: Vec<AppendEventBatchQueueItem>,
    pub file_len_metadata: u64,
    pub file_len_event_batch: u64,
}

impl WriteOperationsWithDmaFile {
    pub fn open(
        metadata_dma_file: DmaFile,
        event_batches_dma_file: DmaFile,
        data_requirements: WriteOperationsDataRequirements,
        aggregate_write_config: AggregateWriteConfig,
    ) -> Result<WriteOperationsWithDmaFile, GlommioError<()>> {
        let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);

        Ok(WriteOperationsWithDmaFile {
            metadata_dma_file,
            event_batches_dma_file,
            data_cache: VecDeque::new(),
            total_cache_size_bytes: 0,
            next_event_batch_index: data_requirements.next_event_batch_index,
            next_event_index: data_requirements.next_event_index,
            minimum_available_event_batch_index: data_requirements
                .minimum_available_event_batch_index,
            client_event_indexes: data_requirements.client_event_indexes,
            max_data_cache_size_bytes: aggregate_write_config.max_data_cache_size_bytes,
            cache_trim_factor: aggregate_write_config.cache_trim_factor,
            max_chunk_size: aggregate_write_config.max_chunk_size,
            bloom_filter,
            event_type_dedup: HashSet::new(),
            append_event_batch_queue: vec![],
            file_len_metadata: data_requirements.file_len_metadata,
            file_len_event_batch: data_requirements.file_len_event_batch,
        })
    }

    fn create_bloom_filter_bytes(&mut self, events: &[EventItem]) -> [u64; BLOOM_BYTES / 8] {
        // Populate bloom filter with multiple event types
        self.bloom_filter.clear();
        self.event_type_dedup.clear();

        for event in events {
            self.event_type_dedup.insert(event.event_type_major);
        }

        for &event_type in self.event_type_dedup.iter() {
            self.bloom_filter.insert(&event_type.to_le_bytes());
        }

        self.bloom_filter
            .as_slice()
            .try_into()
            .expect("Conversion failed")
    }

    async fn sync(&mut self) -> Result<(), WriteError> {
        // Get current file sizes
        let metadata_file_size = self.metadata_dma_file.file_size().await?;
        let event_batches_file_size = self.event_batches_dma_file.file_size().await?;

        // Calculate total sizes
        let total_event_batches_size: usize = self
            .append_event_batch_queue
            .iter()
            .map(|item| item.compressed_event_batch_item.len())
            .sum();
        let total_metadata_size: usize = self
            .append_event_batch_queue
            .iter()
            .map(|item| item.metadata_bytes.len())
            .sum();

        // Allocate contiguous buffers
        let mut event_buf = self
            .event_batches_dma_file
            .alloc_dma_buffer(total_event_batches_size);
        let mut meta_buf = self.metadata_dma_file.alloc_dma_buffer(total_metadata_size);

        // Copy event batches into buffer
        let mut event_offset = 0;
        for item in self.append_event_batch_queue.iter() {
            let len = item.compressed_event_batch_item.len();
            event_buf.as_bytes_mut()[event_offset..event_offset + len]
                .copy_from_slice(&item.compressed_event_batch_item);
            event_offset += len;
        }

        // Copy metadata into buffer
        let mut meta_offset = 0;
        for item in self.append_event_batch_queue.iter() {
            let len = item.metadata_bytes.len();
            meta_buf.as_bytes_mut()[meta_offset..meta_offset + len]
                .copy_from_slice(&item.metadata_bytes);
            meta_offset += len;
        }

        let event_buf_len = event_buf.len() as u64;
        let meta_buf_len = meta_buf.len() as u64;

        // Single write_at per file
        self.event_batches_dma_file
            .write_at(event_buf, event_batches_file_size)
            .await?;
        self.event_batches_dma_file.fdatasync().await?;

        self.metadata_dma_file
            .write_at(meta_buf, metadata_file_size)
            .await?;
        self.metadata_dma_file.fdatasync().await?;

        self.file_len_event_batch += event_buf_len;
        self.file_len_metadata += meta_buf_len;

        // Phase 1: Add new items to cache efficiently
        let queue_len = self.append_event_batch_queue.len();
        if queue_len > 0 {
            // Reserve capacity upfront for better performance
            self.data_cache.reserve(queue_len);

            // Bulk add items and track size
            for item in self.append_event_batch_queue.drain(..) {
                let uncompressed_size = item.event_batch_metadata.uncompressed_size as usize;
                self.total_cache_size_bytes += uncompressed_size;

                self.data_cache.push_back(CacheItem {
                    event_batch_metadata: item.event_batch_metadata,    
                    event_batch_item: item.event_batch_item,
                });
            }
        }

        if self.minimum_available_event_batch_index == 0 {
            //Data in file is now available to read
            self.minimum_available_event_batch_index = 1;
        }

        // Phase 2: Trim old events from front if cache significantly exceeds max size
        // Only trim if we're over by at least 10% to avoid constant trimming overhead
        let trim_threshold = self.max_data_cache_size_bytes
            + (self.max_data_cache_size_bytes / self.cache_trim_factor);

        if self.total_cache_size_bytes > trim_threshold {
            // Calculate how many items to remove in one pass
            let mut items_to_remove = 0;
            let mut size_to_remove = 0;
            let target_size = self.max_data_cache_size_bytes;

            for pair in self.data_cache.iter() {
                if self.total_cache_size_bytes - size_to_remove <= target_size {
                    break;
                }
                size_to_remove += pair.event_batch_metadata.uncompressed_size as usize;
                items_to_remove += 1;
            }

            // Remove all items in one bulk operation
            if items_to_remove > 0 {
                self.data_cache.drain(..items_to_remove);
                self.total_cache_size_bytes -= size_to_remove;
            }
        }

        Ok(())
    }
    async fn trim_and_prepend(
        &mut self,
        event_batches: Vec<PrependEventBatchQueueItem>,
        bytes_to_trim_metadata: u64,
        bytes_to_trim_event_batch: u64,
    ) -> Result<(), WriteError> {
        let metadata_file_path = self.metadata_dma_file.path().unwrap().to_path_buf();
        let event_batch_file_path = self.event_batches_dma_file.path().unwrap().to_path_buf();

        // Create temp file paths
        let temp_path_metadata = format!("{}.tmp", metadata_file_path.display());
        let temp_path_event_batch = format!("{}.tmp", event_batch_file_path.display());

        // Prepare prepend data from event_batches
        let total_prepend_event_batch_size: usize = event_batches
            .iter()
            .map(|item| item.compressed_event_batch_item.len())
            .sum();
        let total_prepend_metadata_size: usize = event_batches
            .iter()
            .map(|item| item.metadata_bytes.len())
            .sum();

        // Trim and prepend metadata file
        {
            let metadata_remaining_size = self
                .file_len_metadata
                .saturating_sub(bytes_to_trim_metadata);
            let temp_metadata_file = DmaFile::create(&temp_path_metadata).await?;

            let mut write_pos = 0u64;

            // First, write the prepended metadata
            if total_prepend_metadata_size > 0 {
                let mut meta_buf = temp_metadata_file.alloc_dma_buffer(total_prepend_metadata_size);
                let mut meta_offset = 0;
                for item in event_batches.iter() {
                    let len = item.metadata_bytes.len();
                    meta_buf.as_bytes_mut()[meta_offset..meta_offset + len]
                        .copy_from_slice(&item.metadata_bytes);
                    meta_offset += len;
                }
                temp_metadata_file.write_at(meta_buf, write_pos).await?;
                write_pos += total_prepend_metadata_size as u64;
            }

            // Then, copy the trimmed original data
            let mut offset = bytes_to_trim_metadata;
            let mut remaining = metadata_remaining_size;

            while remaining > 0 {
                let to_read = std::cmp::min(remaining, self.max_chunk_size as u64);
                let aligned_size = self.metadata_dma_file.align_up(to_read) as usize;

                let chunk = self
                    .metadata_dma_file
                    .read_at_aligned(offset, aligned_size)
                    .await?;
                let actual_read = std::cmp::min(chunk.len(), to_read as usize);

                temp_metadata_file.write_at(chunk.into(), write_pos).await?;

                offset += actual_read as u64;
                write_pos += actual_read as u64;
                remaining -= actual_read as u64;
            }

            temp_metadata_file.fdatasync().await?;
            temp_metadata_file.close().await?;
        }

        // Trim and prepend event batch file
        {
            let event_batch_remaining_size = self
                .file_len_event_batch
                .saturating_sub(bytes_to_trim_event_batch);
            let temp_event_batch_file = DmaFile::create(&temp_path_event_batch).await?;

            let mut write_pos = 0u64;

            // First, write the prepended event batches
            if total_prepend_event_batch_size > 0 {
                let mut event_buf =
                    temp_event_batch_file.alloc_dma_buffer(total_prepend_event_batch_size);
                let mut event_offset = 0;
                for item in event_batches.iter() {
                    let len = item.compressed_event_batch_item.len();
                    event_buf.as_bytes_mut()[event_offset..event_offset + len]
                        .copy_from_slice(&item.compressed_event_batch_item);
                    event_offset += len;
                }
                temp_event_batch_file.write_at(event_buf, write_pos).await?;
                write_pos += total_prepend_event_batch_size as u64;
            }

            // Then, copy the trimmed original data
            let mut offset = bytes_to_trim_event_batch;
            let mut remaining = event_batch_remaining_size;

            while remaining > 0 {
                let to_read = std::cmp::min(remaining, self.max_chunk_size as u64);
                let aligned_size = self.event_batches_dma_file.align_up(to_read) as usize;

                let chunk = self
                    .event_batches_dma_file
                    .read_at_aligned(offset, aligned_size)
                    .await?;
                let actual_read = std::cmp::min(chunk.len(), to_read as usize);

                temp_event_batch_file
                    .write_at(chunk.into(), write_pos)
                    .await?;

                offset += actual_read as u64;
                write_pos += actual_read as u64;
                remaining -= actual_read as u64;
            }

            temp_event_batch_file.fdatasync().await?;
            temp_event_batch_file.close().await?;
        }

        // Now close the OLD files to release their file descriptors
        let old_metadata = std::mem::replace(
            &mut self.metadata_dma_file, 
            DmaFile::open(&temp_path_metadata).await?  // Dummy placeholder
        );
        old_metadata.close().await?;
        
        let old_event_batch = std::mem::replace(
            &mut self.event_batches_dma_file,
            DmaFile::open(&temp_path_event_batch).await?  // Dummy placeholder  
        );
        old_event_batch.close().await?;

        // Commit by renaming temp files over originals
        std::fs::rename(&temp_path_metadata, &metadata_file_path).map_err(|e| {
            WriteError::FileRenameFailure {
                from: temp_path_metadata,
                to: metadata_file_path.to_string_lossy().to_string(),
                error: e,
            }
        })?;
        std::fs::rename(&temp_path_event_batch, &event_batch_file_path).map_err(|e| {
            WriteError::FileRenameFailure {
                from: temp_path_event_batch,
                to: event_batch_file_path.to_string_lossy().to_string(),
                error: e,
            }
        })?;

        // Reopen files and update state
        let new_metadata_file = get_existing_file_as_dma(&metadata_file_path, false).await?;
        let new_event_batch_file = get_existing_file_as_dma(&event_batch_file_path, false).await?;

        // Update cached file lengths (subtract trimmed, add prepended)
        self.file_len_metadata = self
            .file_len_metadata
            .saturating_sub(bytes_to_trim_metadata)
            .saturating_add(total_prepend_metadata_size as u64);
        self.file_len_event_batch = self
            .file_len_event_batch
            .saturating_sub(bytes_to_trim_event_batch)
            .saturating_add(total_prepend_event_batch_size as u64);

        // Update internal file handles
        self.metadata_dma_file = new_metadata_file;
        self.event_batches_dma_file = new_event_batch_file;

        Ok(())
    }
}


pub async fn get_existing_file_as_dma<P: AsRef<Path>>(
    path: P,
    create_if_not_exists: bool,
) -> Result<DmaFile, WriteError> {
    let dma_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(create_if_not_exists)
        .append(false)
        .dma_open(path)
        .await?;

    Ok(dma_file)
}


#[allow(async_fn_in_trait)]
pub trait WriteOperations {
    
    fn update_max_data_cache_size_bytes(&mut self, value: usize);

    // In case of failure during sync, we need to roll back the in-memory state
    async fn sync_with_rollback(&mut self) -> Result<(), WriteError>;

    /// We require the ownership of the events to be transferred, as they will be stored in the in-memory cache
    /// The events are also mutable as we need to filter out events for client idempotency requirements
    fn queue_events_in_memory(
        &mut self,
        events: Vec<EventItem>,
        write_options: &WriteOptions,
    ) -> Result<WriteResult, WriteError>;

    async fn trim_end(
        &mut self,
        new_metadata_len: u64,
        new_event_batch_len: u64,
    ) -> Result<(), WriteError>;

    async fn trim_start(
        &mut self,
        keep_from_event_batch_index: u64,
        bytes_to_trim_metadata: u64,
        bytes_to_trim_event_batch: u64,
    ) -> Result<(), WriteError>;

    async fn prepend_batches(
        &mut self,
        compression_type: CompressionType,
        event_batches: &Vec<EventBatchItem>,
    ) -> Result<(), WriteError>;

    fn maybe_read_cached_events(
        &self,
        filters: &ReadFilters,
        max_bytes: Option<usize>,
    ) -> Result<CacheableReadResult, WriteError>;
}

/// Allows appending new events for an aggregate. Note this doesn't handle fdatasync.
/// Also caches recent events and indexes in memory for fast read access
/// If cached read fails, you should fall back to the AggregateReadFileOperations struct
/// This struct never reads from disk, only appends. So it requires cache data on initialization.
impl WriteOperations for WriteOperationsWithDmaFile {
    
    fn update_max_data_cache_size_bytes(&mut self, value: usize) {
        self.max_data_cache_size_bytes = value;
    
        // Proactively trim cache if it exceeds the new max size
        if self.total_cache_size_bytes > value {
            let mut items_to_remove = 0;
            let mut size_to_remove = 0;

            for pair in self.data_cache.iter() {
                if self.total_cache_size_bytes - size_to_remove <= value {
                    break;
                }
                size_to_remove += pair.event_batch_metadata.uncompressed_size as usize;
                items_to_remove += 1;
            }

            // Remove all items in one bulk operation
            if items_to_remove > 0 {
                self.data_cache.drain(..items_to_remove);
                self.total_cache_size_bytes -= size_to_remove;
            }
        }
    }

    // In case of failure during sync, we need to roll back the in-memory state
    async fn sync_with_rollback(&mut self) -> Result<(), WriteError> {
        match self.sync().await {
            Ok(_) => Ok(()),
            Err(e) => {
                //Pop off items from append_event_batch_queue, inspect metadata to rollback
                while let Some(item) = self.append_event_batch_queue.pop() {
                    self.client_event_indexes
                        .entry(item.event_batch_item.client_id)
                        .and_modify(|e| {
                            *e = item
                                .event_batch_metadata
                                .min_client_event_index
                                .saturating_sub(1);
                        });
                    self.next_event_index = item.event_batch_metadata.min_event_index;
                    self.next_event_batch_index = item.event_batch_metadata.event_batch_index;
                }

                if self.next_event_batch_index == 1 {
                    //First write failed!
                    self.minimum_available_event_batch_index = 0;
                }

                //Still must error out so we notify clients of failure to write
                Err(e)
            }
        }
    }

    /// We require the ownership of the events to be transferred, as they will be stored in the in-memory cache
    /// The events are also mutable as we need to filter out events for client idempotency requirements
    fn queue_events_in_memory(
        &mut self,
        mut events: Vec<EventItem>,
        write_options: &WriteOptions,
    ) -> Result<WriteResult, WriteError> {
        // Make sure we have at least one event to write
        if events.is_empty() {
            return Err(WriteError::EmptyEventsList);
        }

        // Validate that no event uses the sentinel 0 event type
        if let Some(ev) = events.iter().find(|e| e.event_type_major == 0) {
            return Err(WriteError::ZeroEventType {
                client_event_index: ev.client_event_index,
            });
        }

        // If checking idempotency, check if client is providing the same events again using client event index, if so, error
        if write_options.enforce_client_idempotency {
            if let Some(&last_client_event_index) =
                self.client_event_indexes.get(&write_options.client_id)
            {
                let attempted_client_event_index = events
                    .iter()
                    .map(|e| e.client_event_index)
                    .min()
                    .unwrap_or(0);
                if attempted_client_event_index <= last_client_event_index {
                    return Err(WriteError::ClientIdempotencyViolation {
                        client_id: write_options.client_id,
                        last_client_event_index,
                        attempted_client_event_index,
                    });
                }
            }
        }

        // If doing optimistic concurrency, check expected event batch index matches current
        if let Some(expected) = write_options.expected_event_batch_index {
            if expected != self.next_event_batch_index {
                return Err(WriteError::OptimisticConcurrencyViolation {
                    client_id: write_options.client_id,
                    expected_event_batch_index: expected,
                    current_event_batch_index: self.next_event_batch_index,
                });
            }
        }

        // Update events - set event indexes, server timestamp millis. Keep track of last event index assigned to update state later
        let mut next_event_index = self.next_event_index;
        for e in events.iter_mut() {
            e.event_index = next_event_index;
            next_event_index = next_event_index.saturating_add(1);
        }

        // Create EventBatchItem from events with next index, don't increment struct state yet though
        let event_batch_item = EventBatchItem::new(
            self.next_event_batch_index,
            write_options.server_timestamp_millis,
            write_options.client_id,
            write_options.user_id,
            events,
        );

        // Serialize and compress the event data
        let (uncompressed_size, compressed_event_batch_item) =
            to_wire_format_variable(&event_batch_item, write_options.compression_type)?;
        let events_crc = crc32fast::hash(&compressed_event_batch_item);

        // Determine event types data (bloom filter or direct array)
        let (event_types, use_bloom) = extract_unique_event_types(&event_batch_item.events);
        let event_types_data = if use_bloom {
            let bloom_bytes = self.create_bloom_filter_bytes(&event_batch_item.events);
            EventTypesData::Bloom(bloom_bytes)
        } else {
            EventTypesData::Direct(event_types)
        };

        // Create and serialize metadata
        let event_batch_metadata = EventBatchMetadata::from_batch_item(
            &event_batch_item,
            uncompressed_size as u64,
            compressed_event_batch_item.len() as u64,
            write_options.compression_type,
            event_types_data,
            events_crc,
        );

        let latest_client_event_index = event_batch_metadata.max_client_event_index;

        let mut metadata_bytes = [0u8; METADATA_BATCH_SIZE_BYTES];
        to_wire_format_fixed_with_version(&event_batch_metadata, &mut metadata_bytes)?;

        self.append_event_batch_queue
            .push(AppendEventBatchQueueItem {
                compressed_event_batch_item,
                event_batch_item,
                metadata_bytes,
                event_batch_metadata,
            });

        // Update next event index, next event batch index, client event indexes
        self.next_event_index = next_event_index;
        self.next_event_batch_index = self.next_event_batch_index.saturating_add(1);
        self.client_event_indexes
            .insert(write_options.client_id, latest_client_event_index);

        Ok(WriteResult {
            next_event_batch_index: self.next_event_batch_index,
        })
    }

    async fn trim_end(
        &mut self,
        new_metadata_len: u64,
        new_event_batch_len: u64,
    ) -> Result<(), WriteError> {
        let mut truncated = false;

        // Truncate the metadata file
        if self.file_len_metadata > new_metadata_len {
            self.metadata_dma_file.truncate(new_metadata_len).await?;
            self.file_len_metadata = new_metadata_len;
            truncated = true;
        }

        // Truncate the event batches file
        if self.file_len_event_batch > new_event_batch_len {
            self.event_batches_dma_file
                .truncate(new_event_batch_len)
                .await?;
            self.file_len_event_batch = new_event_batch_len;
            truncated = true;
        }

        if truncated {
            // Clear the data cache as it's now invalid
            self.data_cache.clear();
            self.total_cache_size_bytes = 0; // Reset cache size tracking
        }

        Ok(())
    }

    async fn trim_start(
        &mut self,
        keep_from_event_batch_index: u64,
        bytes_to_trim_metadata: u64,
        bytes_to_trim_event_batch: u64,
    ) -> Result<(), WriteError> {
        if bytes_to_trim_metadata == 0 || bytes_to_trim_event_batch == 0 {
            return Ok(());
        }

        let result =self.trim_and_prepend(vec![], bytes_to_trim_metadata, bytes_to_trim_event_batch)
            .await?;

        self.minimum_available_event_batch_index = keep_from_event_batch_index;
        self.data_cache.clear();
        self.total_cache_size_bytes = 0;

        Ok(result)
    }

    async fn prepend_batches(
        &mut self,
        compression_type: CompressionType,
        event_batches: &Vec<EventBatchItem>,
    ) -> Result<(), WriteError> {
        if event_batches.is_empty() {
            return Err(WriteError::EmptyEventsList);
        }

        // Validate contiguous event_batch_indexes
        for i in 1..event_batches.len() {
            let prev_index = event_batches[i - 1].event_batch_index;
            let curr_index = event_batches[i].event_batch_index;

            if curr_index != prev_index + 1 {
                return Err(WriteError::PrependNonContiguousBatches {
                    from_event_batch_index: prev_index,
                    to_event_batch_index: curr_index,
                });
            }
        }

        // Validate that last event_batch_index is exactly one less than minimum_available_event_batch_index
        let last_batch_index = event_batches
            .last()
            .unwrap()
            .event_batch_index;
        if last_batch_index + 1 != self.minimum_available_event_batch_index {
            return Err(WriteError::PrependCreatesEventBatchIndexGap {
                provided_last_batch_index: last_batch_index,
                current_first_event_batch_index: self.minimum_available_event_batch_index,
            });
        }

        // Convert BatchMetadataItemPair to AppendEventBatchQueueItem
        let mut event_batches_queued = Vec::with_capacity(event_batches.len());

        for event_batch_item in event_batches.iter() {

            // Serialize and compress the event data
            let (uncompressed_size, compressed_event_batch_item) =
                to_wire_format_variable(&event_batch_item, compression_type)?;
            let events_crc = crc32fast::hash(&compressed_event_batch_item);

            // Determine event types data (bloom filter or direct array)
            let (event_types, use_bloom) = extract_unique_event_types(&event_batch_item.events);
            let event_types_data = if use_bloom {
                let bloom_bytes = self.create_bloom_filter_bytes(&event_batch_item.events);
                EventTypesData::Bloom(bloom_bytes)
            } else {
                EventTypesData::Direct(event_types)
            };

            // Create and serialize metadata
            let event_batch_metadata = EventBatchMetadata::from_batch_item(
                &event_batch_item,
                uncompressed_size as u64,
                compressed_event_batch_item.len() as u64,
                compression_type,
                event_types_data,
                events_crc,
            );

            let mut metadata_bytes = [0u8; METADATA_BATCH_SIZE_BYTES];
            to_wire_format_fixed_with_version(&event_batch_metadata, &mut metadata_bytes)?;

            event_batches_queued.push(PrependEventBatchQueueItem {
                compressed_event_batch_item,
                metadata_bytes,
            });
        }

        // Perform trim and prepend (with 0 bytes to trim)
        self.trim_and_prepend(event_batches_queued, 0, 0).await?;

        // Update minimum_available_event_batch_index to the first prepended batch
        self.minimum_available_event_batch_index =
            event_batches[0].event_batch_index;

        Ok(())
    }

    fn maybe_read_cached_events(
        &self,
        filters: &ReadFilters,
        max_bytes: Option<usize>,
    ) -> Result<CacheableReadResult, WriteError> {
        // Check if cache is empty
        if self.data_cache.is_empty() {
            return Err(WriteError::CacheMiss {
                missing_from_event_batch_index: filters.from_event_batch_index,
                missing_to_event_batch_index: filters.to_event_batch_index,
            });
        }

        // Get the range of cached event batch indexes
        let cache_min_batch_index = self.data_cache[0].event_batch_item.event_batch_index;

        // Check if requested range is within cache
        if filters.from_event_batch_index < cache_min_batch_index {
            return Err(WriteError::CacheMiss {
                missing_from_event_batch_index: filters.from_event_batch_index,
                missing_to_event_batch_index: Some(cache_min_batch_index.saturating_sub(1)),
            });
        }

        // Collect matching event batches from cache
        let mut filtered_event_batches = Vec::new();
        let mut cumulative_size: u64 = 0;
        let mut next_event_batch_index: Option<u64> = None;

        for pair in self.data_cache.iter() {
            let metadata = &pair.event_batch_metadata;

            // Check if we've exceeded the to_event_batch_index
            if filters
                .to_event_batch_index
                .map_or(false, |to_index| metadata.event_batch_index > to_index)
            {
                break;
            }

            // Check if this batch should be included based on metadata filters
            if !is_include_batch(metadata, filters) {
                continue;
            }

            // Check max_bytes limit if specified
            if let Some(max_bytes) = max_bytes {
                let next_size = cumulative_size + metadata.compressed_size;
                if next_size > max_bytes as u64 {
                    // Mark next batch index for pagination
                    next_event_batch_index = Some(metadata.event_batch_index);
                    break;
                }
                cumulative_size = next_size;
            }

            // Clone the batch and apply event-level filters
            let mut event_batch = pair.event_batch_item.clone();
            apply_event_filters(&mut event_batch, filters);

            // Only include batches that have events after filtering
            if !event_batch.events.is_empty() {
                filtered_event_batches.push(event_batch);
            }
        }

        Ok(CacheableReadResult {
            uncached_metadata_set: Vec::new(),
            filtered_event_batches,
            next_event_batch_index,
        })
    }
}

fn extract_unique_event_types(events: &[EventItem]) -> ([u64; 4], bool) {
    let mut bloom_or_event_types = [u64::MAX, u64::MAX, u64::MAX, u64::MAX];
    let mut use_bloom = false;
    let mut unique_count = 0;

    for event in events {
        let event_type = event.event_type_major;

        // Check if we already have this event type
        if unique_count > 0 && bloom_or_event_types[0] == event_type {
            continue;
        }
        if unique_count > 1 && bloom_or_event_types[1] == event_type {
            continue;
        }
        if unique_count > 2 && bloom_or_event_types[2] == event_type {
            continue;
        }
        if unique_count > 3 && bloom_or_event_types[3] == event_type {
            continue;
        }

        // New unique event type
        if unique_count < 4 {
            bloom_or_event_types[unique_count] = event_type;
            unique_count += 1;
        } else {
            use_bloom = true;
            break;
        }
    }

    (bloom_or_event_types, use_bloom)
}

struct AppendEventBatchQueueItem {
    compressed_event_batch_item: Vec<u8>,
    event_batch_item: EventBatchItem,
    metadata_bytes: [u8; METADATA_BATCH_SIZE_BYTES],
    event_batch_metadata: EventBatchMetadata,
}

struct PrependEventBatchQueueItem {
    compressed_event_batch_item: Vec<u8>,
    metadata_bytes: [u8; METADATA_BATCH_SIZE_BYTES],
}
