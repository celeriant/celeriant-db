use std::{collections::{HashMap, HashSet, VecDeque}};

use celeriant_disk::files::open_dma_files::existing_file_write_only_dma;
use celeriant_msg::{request::{read_filters::ReadFilters, requests::WriteRequest}, response::responses::{ReadResponse, WriteResponse}};
use celeriant_wal::{compression_type::CompressionType, constants::{BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED, METADATA_BATCH_SIZE_BYTES}, wal::{event_batch_item::EventBatchItem, event_batch_metadata::{EventBatchMetadata, EventTypesData}, event_item::EventItem}};
use celeriant_wire::{version_aware_wire_format::to_wire_format_fixed_with_version, wire_format::to_wire_format_variable};
use fastbloom::BloomFilter;
use futures_lite::AsyncWriteExt;
use glommio::{GlommioError, io::{DmaFile, DmaStreamWriterBuilder}};

use crate::{
    read_operations::{in_memory_filtering::{apply_event_filters, is_include_batch}, read_structures::WriteOperationsDataRequirements}, write_operations::{
        write_error::WriteError,
        aggregate_write_config::{AggregateWriteConfig},
    }
};

pub struct CacheItem {
    pub event_batch_item: EventBatchItem,
    pub event_batch_metadata: EventBatchMetadata
}

pub struct WriteOperationsWithDmaFile {
    pub metadata_dma_file: Option<DmaFile>,
    pub event_batches_dma_file: Option<DmaFile>,
    pub metadata_buffer: Vec<u8>,
    pub event_batch_buffer: Vec<u8>,
    pub data_cache: VecDeque<CacheItem>,
    pub total_cache_size_bytes: usize,
    pub minimum_available_event_batch_index: u64,
    pub next_event_index: u64,
    pub next_event_batch_index: u64,
    pub client_event_indexes: HashMap<u128, u64>,
    pub max_data_cache_size_bytes: usize,
    cache_trim_factor: usize,
    max_chunk_size: usize,
    bloom_filter: BloomFilter,
    event_type_dedup: HashSet<u64>,
    append_event_batch_queue: Vec<EventBatchQueueItem>,
    pub file_len_metadata: u64,
    pub file_len_event_batch: u64,
}

impl WriteOperationsWithDmaFile {
    pub fn new(
        metadata_dma_file: DmaFile,
        event_batches_dma_file: DmaFile,
        data_requirements: WriteOperationsDataRequirements,
        aggregate_write_config: AggregateWriteConfig,
    ) -> WriteOperationsWithDmaFile {
        let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);

        WriteOperationsWithDmaFile {
            metadata_dma_file: Some(metadata_dma_file),
            event_batches_dma_file: Some(event_batches_dma_file),
            metadata_buffer: data_requirements.metadata_buffer,
            event_batch_buffer: data_requirements.event_batch_buffer,
            data_cache: VecDeque::new(),
            total_cache_size_bytes: 0,
            next_event_batch_index: data_requirements.next_event_batch_index,
            next_event_index: data_requirements.next_event_index,
            minimum_available_event_batch_index: data_requirements.minimum_available_event_batch_index,
            client_event_indexes: data_requirements.client_event_indexes,
            max_data_cache_size_bytes: aggregate_write_config.max_data_cache_size_bytes,
            cache_trim_factor: aggregate_write_config.cache_trim_factor,
            max_chunk_size: aggregate_write_config.max_chunk_size,
            bloom_filter,
            event_type_dedup: HashSet::new(),
            append_event_batch_queue: vec![],
            file_len_metadata: data_requirements.file_len_metadata,
            file_len_event_batch: data_requirements.file_len_event_batch,
        }
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

        // Check for unexpected close of files
        if self.event_batches_dma_file.is_none() || self.metadata_dma_file.is_none() {
            return Err(WriteError::DmaFileNotInitialized);
        }
        let event_batches_dma_file = self.event_batches_dma_file.as_mut().unwrap();
        let metadata_dma_file = self.metadata_dma_file.as_mut().unwrap();

        // Calculate total sizes of the batches to write
        let event_data_size_bytes: usize = self
            .append_event_batch_queue
            .iter()
            .map(|item| item.compressed_event_batch_item.len())
            .sum();
        let meta_data_size_bytes: usize = self
            .append_event_batch_queue
            .iter()
            .map(|item| item.metadata_bytes.len())
            .sum();

        // Where we start writing from in each file - may include part of previous written batch due to alignment
        let event_write_start_pos = event_batches_dma_file.align_down(self.file_len_event_batch);
        let meta_write_start_pos = metadata_dma_file.align_down(self.file_len_metadata);

        // Pad buffer sizes to alignment, eg may need to write 335 bytes but must pad to 512 for the nvme device
        let mut event_offset = (self.file_len_event_batch - event_write_start_pos) as usize;
        let event_write_buffer_len = event_batches_dma_file.align_up((event_offset + event_data_size_bytes) as u64);
        let mut meta_offset = (self.file_len_metadata - meta_write_start_pos) as usize;
        let meta_write_buffer_len = metadata_dma_file.align_up((meta_offset + meta_data_size_bytes) as u64);

        // Allocate aligned buffers
        let mut event_buf = event_batches_dma_file.alloc_dma_buffer(event_write_buffer_len as usize);
        let mut meta_buf = metadata_dma_file.alloc_dma_buffer(meta_write_buffer_len as usize);

        // Where we must truncate the files to after close
        let event_final_len = self.file_len_event_batch + event_data_size_bytes as u64;
        let metadata_final_len = self.file_len_metadata + meta_data_size_bytes as u64;

        // How much remainder of data we have to include in next write for alignment
        let event_carry_over_len = event_final_len - event_batches_dma_file.align_down(event_final_len);
        let metadata_carry_over_len = metadata_final_len - metadata_dma_file.align_down(metadata_final_len);

        // Copy event batches into the in-memory buffer
        // The first part of the buffer is any carry-over from previous writes due to alignment
        if event_offset > 0 {
            event_buf.as_bytes_mut()[0..event_offset].copy_from_slice(&self.event_batch_buffer[0..event_offset]);        
        }
        for item in self.append_event_batch_queue.iter() {
            let len = item.compressed_event_batch_item.len();
            event_buf.as_bytes_mut()[event_offset..event_offset + len].copy_from_slice(&item.compressed_event_batch_item);
            event_offset += len;
        }

        // Save any data that goes over the last alignment boundary for the next write
        let event_carry_over = if event_carry_over_len > 0 {
            let carry_over_start = event_offset - event_carry_over_len as usize;
            event_buf.as_bytes()[carry_over_start..event_offset].to_vec()
        } else {
            Vec::new()
        };

        // Copy metadata into the in-memory buffer
        // The first part of the buffer is any carry-over from previous writes due to alignment
        if meta_offset > 0 {
            meta_buf.as_bytes_mut()[0..meta_offset].copy_from_slice(&self.metadata_buffer[0..meta_offset]);
        }
        for item in self.append_event_batch_queue.iter() {
            let len = item.metadata_bytes.len();
            meta_buf.as_bytes_mut()[meta_offset..meta_offset + len]
                .copy_from_slice(&item.metadata_bytes);
            meta_offset += len;
        }

        // Save any data that goes over the last alignment boundary for the next write
        let meta_carry_over = if metadata_carry_over_len > 0 {
            let carry_over_start = meta_offset - metadata_carry_over_len as usize;
            meta_buf.as_bytes()[carry_over_start..meta_offset].to_vec()
        } else {
            Vec::new()
        };

        // Write to disk and sync. We always write the event batches first as metadata file is our commit unit
        event_batches_dma_file
            .write_at(event_buf, event_write_start_pos)
            .await?;
        event_batches_dma_file.fdatasync().await?;

        metadata_dma_file
            .write_at(meta_buf, meta_write_start_pos)
            .await?;
        metadata_dma_file.fdatasync().await?;

        // Now that data is safely on disk, update in-memory state
        // This includes updating file lengths, buffers, and moving queued items to the cache
        self.file_len_event_batch = event_final_len;
        self.file_len_metadata = metadata_final_len;
        self.event_batch_buffer = event_carry_over;
        self.metadata_buffer = meta_carry_over;
        
        let queue_len = self.append_event_batch_queue.len();
        if self.max_data_cache_size_bytes > 0 && queue_len > 0 {
            self.data_cache.reserve(queue_len);
            for item in self.append_event_batch_queue.drain(..) {
                let uncompressed_size = item.event_batch_metadata.uncompressed_size as usize;
                self.total_cache_size_bytes += uncompressed_size;
                self.data_cache.push_back(CacheItem {
                    event_batch_metadata: item.event_batch_metadata,
                    event_batch_item: item.event_batch_item,
                });
            }
        } else if self.max_data_cache_size_bytes == 0 {
            self.append_event_batch_queue.clear();
        }

        // Update sentinal value to 1 now that we have written some data
        if self.minimum_available_event_batch_index == 0 {
            self.minimum_available_event_batch_index = 1;
        }

        // Trim cache if it exceeds max size
        // Note these is a trim factor to avoid trimming on every write
        let trim_threshold = self.max_data_cache_size_bytes
            + (self.max_data_cache_size_bytes / self.cache_trim_factor);

        if self.total_cache_size_bytes > trim_threshold {
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

            if items_to_remove > 0 {
                self.data_cache.drain(..items_to_remove);
                self.total_cache_size_bytes -= size_to_remove;
            }
        }

        Ok(())
    }

    async fn trim_and_prepend(
        &mut self,
        event_batches: &Vec<EventBatchQueueItem>,
        source_metadata_dma_file: &DmaFile,
        source_event_batches_dma_file: &DmaFile,
        bytes_to_trim_metadata: u64,
        bytes_to_trim_event_batch: u64,
    ) -> Result<(), WriteError> {
        let metadata_path = source_metadata_dma_file.path().unwrap().to_path_buf();
        let event_batch_path = source_event_batches_dma_file.path().unwrap().to_path_buf();

        let temp_path_metadata = metadata_path.with_extension("tmp");
        let temp_path_event_batch = event_batch_path.with_extension("tmp");

        // create temp output files
        let tmp_meta = DmaFile::create(&temp_path_metadata).await?;
        let tmp_evt  = DmaFile::create(&temp_path_event_batch).await?;

        let mut tmp_meta_writer = DmaStreamWriterBuilder::new(tmp_meta)
            .with_buffer_size(self.max_chunk_size)
            .build();

        let mut tmp_evt_writer = DmaStreamWriterBuilder::new(tmp_evt)
            .with_buffer_size(self.max_chunk_size)
            .build();

        for item in event_batches {
            // prepend metadata
            tmp_meta_writer.write_all(&item.metadata_bytes).await?;

            // prepend event batch bytes
            tmp_evt_writer.write_all(&item.compressed_event_batch_item).await?;
        }

        // copying metadata
        let meta_remaining = self.file_len_metadata.saturating_sub(bytes_to_trim_metadata);
        {
            let mut offset = bytes_to_trim_metadata;
            let mut remaining = meta_remaining;

            while remaining > 0 {
                let chunk = remaining.min(self.max_chunk_size as u64) as usize;

                let read_result = source_metadata_dma_file.read_at(offset, chunk).await?;
                let bytes: &[u8] = &*read_result;

                if bytes.is_empty() {
                    break;
                }

                tmp_meta_writer.write_all(bytes).await?;
                offset += bytes.len() as u64;
                remaining -= bytes.len() as u64;
            }

            tmp_meta_writer.flush().await?;
        }

        // copying event batches
        let evt_remaining = self.file_len_event_batch.saturating_sub(bytes_to_trim_event_batch);
        {
            let mut offset = bytes_to_trim_event_batch;
            let mut remaining = evt_remaining;

            while remaining > 0 {
                let chunk = remaining.min(self.max_chunk_size as u64) as usize;

                let read_result = source_event_batches_dma_file.read_at(offset, chunk).await?;
                let bytes: &[u8] = &*read_result;

                if bytes.is_empty() {
                    break;
                }

                tmp_evt_writer.write_all(bytes).await?;
                offset += bytes.len() as u64;
                remaining -= bytes.len() as u64;
            }

            tmp_evt_writer.flush().await?;
        }

        tmp_meta_writer.sync().await?;
        tmp_evt_writer.sync().await?;

        // drop stream writers so rename can occur
        tmp_meta_writer.close().await?;
        tmp_evt_writer.close().await?;

        drop(tmp_meta_writer);
        drop(tmp_evt_writer);

        // close original files or they will hold the old inode open
        let old_meta_writer = self.metadata_dma_file.take();
        if let Some(file) = old_meta_writer {
            file.close().await?;
        }
        let old_event_writer = self.event_batches_dma_file.take();
        if let Some(file) = old_event_writer {
            file.close().await?;
        }

        // rename temporary files into place
        let temp_meta = existing_file_write_only_dma(&temp_path_metadata).await?;
        temp_meta.rename(&metadata_path).await?;
        self.metadata_dma_file = Some(temp_meta);

        let tmp_evt = existing_file_write_only_dma(&temp_path_event_batch).await?;
        tmp_evt.rename(&event_batch_path).await?;
        self.event_batches_dma_file = Some(tmp_evt);

        // update internal state
        self.file_len_metadata = (event_batches.len() as u64 * METADATA_BATCH_SIZE_BYTES as u64)
            + meta_remaining;

        self.file_len_event_batch = event_batches
            .iter()
            .map(|e| e.compressed_event_batch_item.len() as u64)
            .sum::<u64>()
            + evt_remaining;

        Ok(())
    }
    
    pub fn update_write_operations_data_requirements(&mut self, data_requirements: WriteOperationsDataRequirements) {
        self.metadata_buffer = data_requirements.metadata_buffer;
        self.event_batch_buffer = data_requirements.event_batch_buffer;
        self.file_len_event_batch = data_requirements.file_len_event_batch;
        self.file_len_metadata = data_requirements.file_len_metadata;
        self.next_event_batch_index = data_requirements.next_event_batch_index;
        self.next_event_index = data_requirements.next_event_index;
        self.minimum_available_event_batch_index = data_requirements.minimum_available_event_batch_index;
        self.client_event_indexes = data_requirements.client_event_indexes;
    }
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
        node_id: u128,
        lease_index: u64,
        server_timestamp_ms: u64,
        write_request: &mut WriteRequest,
    ) -> Result<WriteResponse, WriteError>;

    async fn trim_end(
        &mut self,
        new_metadata_len: u64,
        new_event_batch_len: u64,
    ) -> Result<(), WriteError>;

    async fn trim_start(
        &mut self,
        keep_from_event_batch_index: u64,
        source_metadata_dma_file: &DmaFile,
        source_event_batches_dma_file: &DmaFile,
        bytes_to_trim_metadata: u64,
        bytes_to_trim_event_batch: u64,
    ) -> Result<(), WriteError>;

    async fn prepend_batches(
        &mut self,
        compression_type: CompressionType,
        event_batches: &Vec<EventBatchItem>,
        source_metadata_dma_file: &DmaFile,
        source_event_batches_dma_file: &DmaFile,
    ) -> Result<(), WriteError>;

    fn maybe_read_cached_events(
        &self,
        filters: &ReadFilters,
        max_bytes: Option<usize>,
    ) -> Result<ReadResponse, WriteError>;

    async fn close(&mut self) -> Result<(), GlommioError<()>>;
}

/// Allows appending new events for an aggregate. Note this doesn't handle fdatasync.
/// Also caches recent events and indexes in memory for fast read access
/// If cached read fails, you should fall back to the AggregateReadFileOperations struct
/// This struct never reads from disk, only appends. So it requires cache data on initialization.
impl WriteOperations for WriteOperationsWithDmaFile {
    
    async fn close(&mut self) -> Result<(), GlommioError<()>> {
        if let Some(metadata_dma_file) = self.metadata_dma_file.take() {
            metadata_dma_file.truncate(self.file_len_metadata).await?;
            metadata_dma_file.close().await?;
        }

        if let Some(event_batches_dma_file) = self.event_batches_dma_file.take() {
            event_batches_dma_file.truncate(self.file_len_event_batch).await?;
            event_batches_dma_file.close().await?;
        }

        Ok(())
    }

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
        node_id: u128,
        lease_index: u64,
        server_timestamp: u64,
        write_request: &mut WriteRequest,
    ) -> Result<WriteResponse, WriteError> {
        // Make sure we have at least one event to write
        if write_request.events.is_empty() {
            return Err(WriteError::EmptyEventsList);
        }

        // Validate that no event uses the sentinel 0 event type
        if let Some(ev) = write_request.events.iter().find(|e| e.event_type_major == 0) {
            return Err(WriteError::ZeroEventType {
                client_event_index: ev.client_event_index,
            });
        }

        // If checking idempotency, check if client is providing the same events again using client event index, if so, error
        if write_request.enforce_client_idempotency {
            if let Some(&last_client_event_index) =
                self.client_event_indexes.get(&write_request.client_id)
            {
                let attempted_client_event_index = write_request.events
                    .iter()
                    .map(|e| e.client_event_index)
                    .min()
                    .unwrap_or(0);
                if attempted_client_event_index <= last_client_event_index {
                    return Err(WriteError::ClientIdempotencyViolation {
                        client_id: write_request.client_id,
                        last_client_event_index,
                        attempted_client_event_index,
                    });
                }
            }
        }

        // If doing optimistic concurrency, check expected event batch index matches current
        if let Some(expected) = write_request.expected_event_batch_index {
            if expected != self.next_event_batch_index {
                return Err(WriteError::OptimisticConcurrencyViolation {
                    client_id: write_request.client_id,
                    expected_event_batch_index: expected,
                    current_event_batch_index: self.next_event_batch_index,
                });
            }
        }

        let mut write_response = WriteResponse {
            event_batch_index: self.next_event_batch_index,
            start_event_index: self.next_event_index,
            server_timestamp,
            node_id,
            lease_index,
            compressed_size: 0, //Write later after serialization
            events_crc: 0,
        };

        // Update events - set event indexes, server timestamp millis. Keep track of last event index assigned to update state later
        let mut next_event_index = self.next_event_index;
        for e in write_request.events.iter_mut() {
            e.event_index = next_event_index;
            next_event_index = next_event_index.saturating_add(1);
        }

        let events_in_batch = std::mem::take(&mut write_request.events);

        // Create EventBatchItem from events with next index, don't increment struct state yet though
        let event_batch_item = EventBatchItem::new(
            self.next_event_batch_index,
            server_timestamp,
            write_request.client_id,
            write_request.user_id,
            node_id,
            lease_index,
            events_in_batch,
        );

        // Serialize and compress the event data
        let (uncompressed_size, compressed_event_batch_item) =
            to_wire_format_variable(&event_batch_item, write_request.compression_type)?;
        let events_crc = crc32c::crc32c(&compressed_event_batch_item);
        
        write_response.events_crc = events_crc;
        write_response.compressed_size = compressed_event_batch_item.len() as u64;

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
            write_request.compression_type,
            event_types_data,
            events_crc,
        );

        
        let mut metadata_bytes = [0u8; METADATA_BATCH_SIZE_BYTES];
        to_wire_format_fixed_with_version(&event_batch_metadata, &mut metadata_bytes)?;
        
        let latest_client_event_index = event_batch_metadata.max_client_event_index;
        self.append_event_batch_queue
            .push(EventBatchQueueItem {
                compressed_event_batch_item,
                event_batch_item,
                metadata_bytes,
                event_batch_metadata,
            });

        // Update next event index, next event batch index, client event indexes
        self.next_event_index = next_event_index;
        self.next_event_batch_index = self.next_event_batch_index.saturating_add(1);
        self.client_event_indexes
            .insert(write_request.client_id, latest_client_event_index);

        Ok(write_response)
    }

    async fn trim_end(
        &mut self,
        new_metadata_len: u64,
        new_event_batch_len: u64,
    ) -> Result<(), WriteError> {
        let mut truncated = false;

        if self.event_batches_dma_file.is_none() || self.metadata_dma_file.is_none() {
            return Err(WriteError::DmaFileNotInitialized);
        }

        let event_batches_dma_file = self.event_batches_dma_file.as_mut().unwrap();
        let metadata_dma_file = self.metadata_dma_file.as_mut().unwrap();

        // Truncate the metadata file
        if self.file_len_metadata > new_metadata_len {
            metadata_dma_file.truncate(new_metadata_len).await?;
            self.file_len_metadata = new_metadata_len;
            truncated = true;
        }

        // Truncate the event batches file
        if self.file_len_event_batch > new_event_batch_len {
            event_batches_dma_file
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
        source_metadata_dma_file: &DmaFile,
        source_event_batches_dma_file: &DmaFile,
        bytes_to_trim_metadata: u64,
        bytes_to_trim_event_batch: u64,
    ) -> Result<(), WriteError> {
        if bytes_to_trim_metadata == 0 || bytes_to_trim_event_batch == 0 {
            return Ok(());
        }

        let result =self.trim_and_prepend(&vec![], source_metadata_dma_file, source_event_batches_dma_file, bytes_to_trim_metadata, bytes_to_trim_event_batch)
            .await?;

        self.minimum_available_event_batch_index = keep_from_event_batch_index;
        self.data_cache.retain(|v| v.event_batch_metadata.event_batch_index >= keep_from_event_batch_index);
        self.total_cache_size_bytes = self.data_cache.iter().map(|v| v.event_batch_metadata.uncompressed_size as usize).sum();

        Ok(result)
    }

    async fn prepend_batches(
        &mut self,
        compression_type: CompressionType,
        event_batches: &Vec<EventBatchItem>,
        source_metadata_dma_file: &DmaFile,
        source_event_batches_dma_file: &DmaFile,
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
            let events_crc = crc32c::crc32c(&compressed_event_batch_item);

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

            event_batches_queued.push(EventBatchQueueItem {
                event_batch_metadata,
                event_batch_item: event_batch_item.clone(),
                compressed_event_batch_item,
                metadata_bytes,
            });
        }

        // Perform trim and prepend (with 0 bytes to trim)
        self.trim_and_prepend(&event_batches_queued, source_metadata_dma_file, source_event_batches_dma_file, 0, 0).await?;

        // Update minimum_available_event_batch_index to the first prepended batch
        self.minimum_available_event_batch_index =
            event_batches[0].event_batch_index;

        // Check if the last prepended batch joins up with the first batch in the cache
        if let Some(first_cached_batch) = self.data_cache.front() {
            if last_batch_index + 1 == first_cached_batch.event_batch_metadata.event_batch_index {
                // Prepend the new batches to the cache
                for item in event_batches_queued.iter().rev() {

                    let uncompressed_size = item.event_batch_metadata.uncompressed_size as usize;

                    self.data_cache.push_front(CacheItem {
                        event_batch_metadata: item.event_batch_metadata.clone(),
                        event_batch_item: item.event_batch_item.clone(),
                    });

                    self.total_cache_size_bytes += uncompressed_size;
                }
            }
        }

        Ok(())
    }

    fn maybe_read_cached_events(
        &self,
        filters: &ReadFilters,
        max_bytes: Option<usize>,
    ) -> Result<ReadResponse, WriteError> {
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
                let next_size = cumulative_size + metadata.uncompressed_size;
                if next_size > max_bytes as u64 {
                    // If this is the first batch and it doesn't fit, the limit is too small
                    if filtered_event_batches.is_empty() {
                        return Err(WriteError::MaxBytesTooSmall {
                            current_max_bytes: max_bytes as u64,
                            required_max_bytes: metadata.uncompressed_size,
                        });
                    }
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

        Ok(ReadResponse {
            correlation_id: None,
            event_batches: filtered_event_batches,
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

struct EventBatchQueueItem {
    compressed_event_batch_item: Vec<u8>,
    event_batch_item: EventBatchItem,
    metadata_bytes: [u8; METADATA_BATCH_SIZE_BYTES],
    event_batch_metadata: EventBatchMetadata,
}