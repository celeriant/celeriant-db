use std::collections::{HashMap, HashSet, VecDeque};

use eventplanedb_structures::{append_result::AppendResult, batch_metadata_item_pair::BatchMetadataItemPair, compression_type::CompressionType, constants::{BINCODE_CONFIG_FIXED, BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED, METADATA_BATCH_SIZE_BYTES}, event_batch_item::EventBatchItem, event_batch_metadata::{EventBatchMetadata, EventTypesData}, event_item::EventItem, read_filters::ReadFilters, wire_format::to_wire_format_variable};
use fastbloom::BloomFilter;
use glommio::{io::{DmaFile}, GlommioError};

use crate::files::read_operations::{CacheableReadResult, apply_event_filters, is_include_batch};

#[derive(Debug)]
pub enum AppendError {
    IoError(GlommioError<()>),
    OptimisticConcurrencyViolation {
        client_id: u128,
        expected_event_batch_index: u64,
        current_event_batch_index: u64,
    },
    ClientIdempotencyViolation {
        client_id: u128,
        last_client_event_index: u64,
        attempted_client_event_index: u64,
    },
    EmptyEventsList {
        client_id: u128,
    },
    NoEventsToAppend {
        client_id: u128,
        existing_event_index: u64,
    },
    SerializationError {
        message: String,
    },
    WriteError {
        message: String,
    },
}

#[derive(Debug)]
pub enum CacheReadError {
    // Cache data should be contiguous and always present at the latest write
    // So cache miss can only occur for older event batch index ranges
    CacheMiss {
        missing_from_event_batch_index: u64,        
        missing_to_event_batch_index: Option<u64>,
    },
}

impl From<GlommioError<()>> for AppendError {
    fn from(error: GlommioError<()>) -> Self {
        AppendError::IoError(error)
    }
}

#[derive(Debug, Clone)]
pub struct AggregateWriteConfig {
    pub max_data_cache_size_bytes: usize,
    pub max_chunk_size: usize
}

pub struct AppendOptions {
    pub client_id: u128,
    pub user_id: Option<u128>,
    pub expected_event_batch_index: Option<u64>,
    pub enforce_client_idempotency: bool,
    pub server_timestamp_millis: u64,
    pub compression_type: CompressionType,
}

pub struct WriteOperations {
    metadata_dma_file: DmaFile,
    event_batches_dma_file: DmaFile,
    data_cache: VecDeque<BatchMetadataItemPair>,
    total_cache_size_bytes: usize,
    pub minimum_available_event_batch_index: u64,
    next_event_index: u64,
    next_event_batch_index: u64,
    client_event_indexes: HashMap<u128, u64>,
    max_data_cache_size_bytes: usize,
    max_chunk_size: usize,
    bloom_filter: BloomFilter,
    event_type_dedup: HashSet<u64>,
    append_event_batch_queue: Vec<AppendEventBatchQueueItem>,
    file_len_metadata: u64,
    file_len_event_batch: u64,
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

pub struct WriteOperationsDataRequirements {
    pub file_len_metadata: u64,
    pub metadata_dma_file: DmaFile,
    pub file_len_event_batch: u64,
    pub event_batches_dma_file: DmaFile,
    pub data_cache: VecDeque<BatchMetadataItemPair>, 
    pub minimum_available_event_batch_index: u64,
    pub next_event_index: u64, 
    pub next_event_batch_index: u64, 
    pub client_event_indexes: HashMap<u128, u64>,
}

/// Allows appending new events for an aggregate. Note this doesn't handle fdatasync.
/// Also caches recent events and indexes in memory for fast read access
/// If cached read fails, you should fall back to the AggregateReadFileOperations struct
/// This struct never reads from disk, only appends. So it requires cache data on initialization.
impl WriteOperations {

    pub fn open(
        data_requirements: WriteOperationsDataRequirements,
        aggregate_write_config: AggregateWriteConfig,
        ) -> Result<WriteOperations, GlommioError<()>> {

        let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);
        
        // Calculate initial cache size
        let total_cache_size_bytes: usize = data_requirements.data_cache
            .iter()
            .map(|pair| pair.event_batch_metadata.uncompressed_size as usize)
            .sum();

        Ok(WriteOperations {
            metadata_dma_file: data_requirements.metadata_dma_file, 
            event_batches_dma_file: data_requirements.event_batches_dma_file, 
            data_cache: data_requirements.data_cache, 
            total_cache_size_bytes,
            next_event_batch_index: data_requirements.next_event_batch_index, 
            next_event_index: data_requirements.next_event_index, 
            minimum_available_event_batch_index: data_requirements.minimum_available_event_batch_index,
            client_event_indexes: data_requirements.client_event_indexes,
            max_data_cache_size_bytes: aggregate_write_config.max_data_cache_size_bytes,
            max_chunk_size: aggregate_write_config.max_chunk_size,
            bloom_filter,
            event_type_dedup: HashSet::new(),
            append_event_batch_queue: vec![],
            file_len_metadata: data_requirements.file_len_metadata,
            file_len_event_batch: data_requirements.file_len_event_batch
        })
    }

    fn create_bloom_filter_bytes(
        &mut self,
        events: &[EventItem],
    ) -> [u64; BLOOM_BYTES / 8] {
        // Populate bloom filter with multiple event types
        self.bloom_filter.clear();
        self.event_type_dedup.clear();

        for event in events {
            self.event_type_dedup.insert(event.event_type_major);
        }

        for &event_type in self.event_type_dedup.iter() {
            self.bloom_filter.insert(&event_type.to_le_bytes());
        }

        self.bloom_filter.as_slice().try_into().expect("Conversion failed")
    }

    async fn sync(&mut self) -> Result<(), AppendError> {
        // Get current file sizes
        let metadata_file_size = self.metadata_dma_file.file_size().await
            .map_err(|e| AppendError::WriteError { message: format!("metadata file size failed: {}", e) })?;
        let event_batches_file_size = self.event_batches_dma_file.file_size().await
            .map_err(|e| AppendError::WriteError { message: format!("event batches file size failed: {}", e) })?;

        // Calculate total sizes
        let total_event_batches_size: usize = self.append_event_batch_queue.iter()
            .map(|item| item.compressed_event_batch_item.len())
            .sum();
        let total_metadata_size: usize = self.append_event_batch_queue.iter()
            .map(|item| item.metadata_bytes.len())
            .sum();

        // Allocate contiguous buffers
        let mut event_buf = self.event_batches_dma_file.alloc_dma_buffer(total_event_batches_size);
        let mut meta_buf = self.metadata_dma_file.alloc_dma_buffer(total_metadata_size);

        // Copy event batches into buffer
        let mut event_offset = 0;
        for item in self.append_event_batch_queue.iter() {
            let len = item.compressed_event_batch_item.len();
            event_buf.as_bytes_mut()[event_offset..event_offset+len]
                .copy_from_slice(&item.compressed_event_batch_item);
            event_offset += len;
        }

        // Copy metadata into buffer
        let mut meta_offset = 0;
        for item in self.append_event_batch_queue.iter() {
            let len = item.metadata_bytes.len();
            meta_buf.as_bytes_mut()[meta_offset..meta_offset+len]
                .copy_from_slice(&item.metadata_bytes);
            meta_offset += len;
        }

        let event_buf_len = event_buf.len() as u64;
        let meta_buf_len = meta_buf.len() as u64;

        // Single write_at per file
        self.event_batches_dma_file.write_at(event_buf, event_batches_file_size).await
            .map_err(|e| AppendError::WriteError { message: format!("event batch write failed: {}", e) })?;
        self.event_batches_dma_file.fdatasync().await
            .map_err(|e| AppendError::WriteError { message: format!("metadata_dma_file fdatasync failed: {}", e) })?;

        self.metadata_dma_file.write_at(meta_buf, metadata_file_size).await
            .map_err(|e| AppendError::WriteError { message: format!("metadata write failed: {}", e) })?;
        self.metadata_dma_file.fdatasync().await
            .map_err(|e| AppendError::WriteError { message: format!("event_batches_dma_file fdatasync failed: {}", e) })?;

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
                
                self.data_cache.push_back(BatchMetadataItemPair {
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
        let trim_threshold = self.max_data_cache_size_bytes + (self.max_data_cache_size_bytes / 25);
        
        if self.total_cache_size_bytes > trim_threshold {
            // Calculate how many items to remove in one pass
            let mut items_to_remove = 0;
            let mut size_to_remove = 0;
            let target_size = self.max_data_cache_size_bytes;
            
            for pair in self.data_cache.iter() {
                if self.total_cache_size_bytes - size_to_remove <= target_size {
                    break;
                }
                size_to_remove += pair.event_batch_metadata.compressed_size as usize;
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
    
    // In case of failure during sync, we need to roll back the in-memory state
    pub async fn sync_with_rollback(&mut self) -> Result<(), AppendError> {
        match self.sync().await {
            Ok(_) => Ok(()),
            Err(e) => {

                //Pop off items from append_event_batch_queue, inspect metadata to rollback
                while let Some(item) = self.append_event_batch_queue.pop() {
                    
                    self.client_event_indexes
                        .entry(item.event_batch_item.client_id)
                        .and_modify(|e| {
                            *e = item.event_batch_metadata.min_client_event_index.saturating_sub(1);
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
    pub fn queue_events_in_memory(&mut self, mut events: Vec<EventItem>, append_options: &AppendOptions) -> Result<AppendResult, AppendError> {
        // Requires mutable write buffer access
        // Requires mutable access to cache data (events, indexes)

        // Make sure we have at least one event to write
        if events.is_empty() {
            return Err(AppendError::EmptyEventsList {
                client_id: append_options.client_id,
            });
        }

        // If checking idempotency, check if client is providing the same events again using client event index, if so, error
        if append_options.enforce_client_idempotency {
            if let Some(&last_client_event_index) = self.client_event_indexes.get(&append_options.client_id) {
                let attempted_client_event_index = events.iter().map(|e| e.client_event_index).min().unwrap_or(0);
                if attempted_client_event_index <= last_client_event_index {
                    return Err(AppendError::ClientIdempotencyViolation {
                        client_id: append_options.client_id,
                        last_client_event_index,
                        attempted_client_event_index,
                    });
                }
            }
        }

        // If doing optimistic concurrency, check expected event batch index matches current
        if let Some(expected) = append_options.expected_event_batch_index {
            if expected != self.next_event_batch_index {
                return Err(AppendError::OptimisticConcurrencyViolation {
                    client_id: append_options.client_id,
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
            append_options.server_timestamp_millis,
            append_options.client_id,
            append_options.user_id,
            events,
        );

        // Serialize and compress the event data
        let (uncompressed_size, compressed_event_batch_item) =
            to_wire_format_variable(&event_batch_item, append_options.compression_type)
            .map_err(|e| AppendError::SerializationError { 
                message: format!("Failed to serialize event batch: {}", e) 
            })?;
        let events_crc = crc32fast::hash(&compressed_event_batch_item);

        // Determine event types data (bloom filter or direct array)
        let (event_types, use_bloom) = extract_unique_event_types(&event_batch_item.events);
        let event_types_data = if use_bloom {
            let bloom_bytes =
                self.create_bloom_filter_bytes(&event_batch_item.events);
            EventTypesData::Bloom(
                bloom_bytes,
            )
        } else {
            EventTypesData::Direct(
                event_types,
            )
        };

        // Create and serialize metadata
        let event_batch_metadata = EventBatchMetadata::from_batch_item(
            &event_batch_item,
            uncompressed_size as u64,
            compressed_event_batch_item.len() as u64,
            append_options.compression_type,
            event_types_data,
            events_crc,
        );

        let latest_client_event_index = event_batch_metadata.max_client_event_index;

        let mut metadata_bytes = [0u8; METADATA_BATCH_SIZE_BYTES];
        bincode::encode_into_slice(
            &event_batch_metadata, 
            &mut metadata_bytes, 
            BINCODE_CONFIG_FIXED
        ).map_err(|e| AppendError::SerializationError { 
            message: format!("Failed to serialize metadata: {}", e) 
        })?;

        self.append_event_batch_queue.push(AppendEventBatchQueueItem {
            compressed_event_batch_item,
            event_batch_item,
            metadata_bytes,
            event_batch_metadata,
        });

        // Update next event index, next event batch index, client event indexes
        self.next_event_index = next_event_index;
        self.next_event_batch_index = self.next_event_batch_index.saturating_add(1);
        self.client_event_indexes
            .insert(append_options.client_id, latest_client_event_index);

        Ok(AppendResult {
            next_event_batch_index: self.next_event_batch_index,
        })
    }

    pub async fn trim_end(&mut self, new_metadata_len: u64, new_event_batch_len: u64) -> Result<(), AppendError> {
        let mut truncated = false;

        // Truncate the metadata file
        if self.file_len_metadata > new_metadata_len {
            self.metadata_dma_file.truncate(new_metadata_len).await?;
            self.file_len_metadata = new_metadata_len;
            truncated = true;
        }
        
        // Truncate the event batches file
        if self.file_len_event_batch > new_event_batch_len {
            self.event_batches_dma_file.truncate(new_event_batch_len).await?;
            self.file_len_event_batch = new_event_batch_len;
            truncated = true;
        }
        
        if truncated {
            // Clear the data cache as it's now invalid
            self.data_cache.clear();
            self.total_cache_size_bytes = 0;  // Reset cache size tracking
        }
        
        Ok(())
    }

    pub async fn trim_start(
        &mut self, 
        bytes_to_trim_metadata: u64, 
        bytes_to_trim_event_batch: u64,
    ) -> Result<(DmaFile, DmaFile), AppendError> {
        if bytes_to_trim_metadata == 0 || bytes_to_trim_event_batch == 0 {
            return Err(AppendError::WriteError { 
                message: "Cannot trim 0 bytes".to_string() 
            });
        }
        
        let metadata_file_path = self.metadata_dma_file.path().unwrap().to_path_buf();
        let event_batch_file_path = self.event_batches_dma_file.path().unwrap().to_path_buf();

        // Create temp file paths
        let temp_path_metadata = format!("{}.tmp", metadata_file_path.display());
        let temp_path_event_batch = format!("{}.tmp", event_batch_file_path.display());

        // Trim metadata file
        {
            let metadata_remaining_size = self.file_len_metadata.saturating_sub(bytes_to_trim_metadata);
            let temp_metadata_file = DmaFile::create(&temp_path_metadata).await?;
            
            let mut offset = bytes_to_trim_metadata;
            let mut remaining = metadata_remaining_size;
            let mut write_pos = 0u64;
            
            while remaining > 0 {
                let to_read = std::cmp::min(remaining, self.max_chunk_size as u64);
                let aligned_size = self.metadata_dma_file.align_up(to_read) as usize;
                
                let chunk = self.metadata_dma_file.read_at_aligned(offset, aligned_size).await?;
                let actual_read = std::cmp::min(chunk.len(), to_read as usize);
                
                temp_metadata_file.write_at(chunk.into(), write_pos).await?;
                
                offset += actual_read as u64;
                write_pos += actual_read as u64;
                remaining -= actual_read as u64;
            }
            
            temp_metadata_file.fdatasync().await?;
            temp_metadata_file.close().await?;
        }

        // Trim event batch file
        {
            let event_batch_remaining_size = self.file_len_event_batch.saturating_sub(bytes_to_trim_event_batch);
            let temp_event_batch_file = DmaFile::create(&temp_path_event_batch).await?;
            
            let mut offset = bytes_to_trim_event_batch;
            let mut remaining = event_batch_remaining_size;
            let mut write_pos = 0u64;
            
            while remaining > 0 {
                let to_read = std::cmp::min(remaining, self.max_chunk_size as u64);
                let aligned_size = self.event_batches_dma_file.align_up(to_read) as usize;
                
                let chunk = self.event_batches_dma_file.read_at_aligned(offset, aligned_size).await?;
                let actual_read = std::cmp::min(chunk.len(), to_read as usize);
                
                temp_event_batch_file.write_at(chunk.into(), write_pos).await?;
                
                offset += actual_read as u64;
                write_pos += actual_read as u64;
                remaining -= actual_read as u64;
            }
            
            temp_event_batch_file.fdatasync().await?;
            temp_event_batch_file.close().await?;
        }


        // Commit by renaming temp files over originals
        std::fs::rename(&temp_path_metadata, &metadata_file_path)
            .map_err(|e| AppendError::WriteError { message: format!("metadata rename failed: {}", e) })?;
        std::fs::rename(&temp_path_event_batch, &event_batch_file_path)
            .map_err(|e| AppendError::WriteError { message: format!("event batch rename failed: {}", e) })?;

        // Reopen files and update state
        let new_metadata_file = DmaFile::open(&metadata_file_path).await?;
        let new_event_batch_file = DmaFile::open(&event_batch_file_path).await?;

        // Update cached file lengths
        self.file_len_metadata = self.file_len_metadata.saturating_sub(bytes_to_trim_metadata);
        self.file_len_event_batch = self.file_len_event_batch.saturating_sub(bytes_to_trim_event_batch);

        // Duplicate file handles for return
        let dup_metadata_file = new_metadata_file.dup()?;
        let dup_event_batch_file = new_event_batch_file.dup()?;

        // Update internal file handles
        self.metadata_dma_file = new_metadata_file;
        self.event_batches_dma_file = new_event_batch_file;

        Ok((dup_metadata_file, dup_event_batch_file))
    }

    pub fn maybe_read_cached_events(&self, filters: &ReadFilters) -> Result<CacheableReadResult, CacheReadError> {
        // Check if cache is empty
        if self.data_cache.is_empty() {
            return Err(CacheReadError::CacheMiss { 
                missing_from_event_batch_index: filters.from_event_batch_index, 
                missing_to_event_batch_index: filters.to_event_batch_index 
            });
        }

        // Get the range of cached event batch indexes
        let cache_min_batch_index = self.data_cache[0].event_batch_metadata.event_batch_index;

        // Check if requested range is within cache
        if filters.from_event_batch_index < cache_min_batch_index {
            return Err(CacheReadError::CacheMiss { 
                missing_from_event_batch_index: filters.from_event_batch_index, 
                missing_to_event_batch_index: Some(cache_min_batch_index.saturating_sub(1))
            });
        }

        // Collect matching event batches from cache
        let mut filtered_event_batches = Vec::new();
        let mut cumulative_size: u64 = 0;
        let mut next_event_batch_index: Option<u64> = None;

        for pair in self.data_cache.iter() {
            let metadata = &pair.event_batch_metadata;
            
            // Check if we've exceeded the to_event_batch_index
            if filters.to_event_batch_index.map_or(false, |to_index| {
                metadata.event_batch_index > to_index
            }) {
                break;
            }

            // Check if this batch should be included based on metadata filters
            if !is_include_batch(metadata, filters) {
                continue;
            }

            // Check max_bytes limit if specified
            if let Some(max_bytes) = filters.max_bytes {
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
    
    pub fn file_len_metadata(&self) -> u64 {
        self.file_len_metadata
    }
    
    pub fn file_len_event_batch(&self) -> u64 {
        self.file_len_event_batch
    }
}