use std::{collections::{HashMap, HashSet}, path::Path};

use eventplanedb_structures::{append_result::AppendResult, compression_type::CompressionType, constants::{BINCODE_CONFIG_FIXED, BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED, METADATA_BATCH_SIZE_BYTES}, event_batch_item::EventBatchItem, event_batch_metadata::{EventBatchMetadata, EventTypesData}, event_item::EventItem, read_filters::ReadFilters, wire_format::to_wire_format_variable};
use fastbloom::BloomFilter;
use futures_lite::AsyncWriteExt;
use glommio::{io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions}, GlommioError};

use std::{
    cell::RefCell,
    rc::Rc,
    time::Duration,
};
use glommio::{
    timer::sleep,
};

use crate::local_event::LocalEvent;

pub struct BatchMetadataItemPair {
    pub metadata: EventBatchMetadata,
    pub item: EventBatchItem,
}

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
        last_event_index: u64,
        attempted_event_index: u64,
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
        missing_to_event_batch_index: u64,
    },
}

impl From<GlommioError<()>> for AppendError {
    fn from(error: GlommioError<()>) -> Self {
        AppendError::IoError(error)
    }
}

pub struct AggregateWriteConfig {
    pub max_data_cache_size_bytes: usize,
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
    data_cache: Vec<BatchMetadataItemPair>,
    next_event_index: u64,
    next_event_batch_index: u64,
    client_event_indexes: HashMap<u128, u64>,
    max_data_cache_size_bytes: usize,
    bloom_filter: BloomFilter,
    event_type_dedup: HashSet<u64>,
    append_event_batch_queue: Vec<AppendEventBatchQueueItem>,
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
    metadata_bytes: Vec<u8>,
    event_batch_metadata: EventBatchMetadata,
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
        let event_batches_dma_file = append_only_file(path_event_batches).await?;

        let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);
        
        Ok(WriteOperations {
            metadata_dma_file, 
            event_batches_dma_file, 
            data_cache, 
            next_event_batch_index, 
            next_event_index, 
            client_event_indexes,
            max_data_cache_size_bytes: aggregate_write_config.max_data_cache_size_bytes,
            bloom_filter,
            event_type_dedup: HashSet::new(),
            append_event_batch_queue: vec![],
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

        let total_metadata_size: usize = self.append_event_batch_queue.iter()
            .map(|item| item.metadata_bytes.len())
            .sum();
        let total_event_batch_size: usize = self.append_event_batch_queue.iter()
            .map(|item| item.compressed_event_batch_item.len())
            .sum();
        
        let total_metadata_size = self.metadata_dma_file.align_up(total_metadata_size as u64) as usize;
        let total_event_batch_size = self.event_batches_dma_file.align_up(total_event_batch_size as u64) as usize;

        let mut writer_metadata = DmaStreamWriterBuilder::new(self.metadata_dma_file.dup()?)
            .with_buffer_size(total_metadata_size)
            .with_write_behind(1)
            .build();

        let mut writer_event_batch = DmaStreamWriterBuilder::new(self.event_batches_dma_file.dup()?)
            .with_buffer_size(total_event_batch_size)
            .with_write_behind(1)
            .build();

        for item in self.append_event_batch_queue.iter() {
            writer_event_batch.write(&item.compressed_event_batch_item).await
                .map_err(|e| AppendError::WriteError { message: format!("event batch write failed: {}", e) })?;
            writer_metadata.write(&item.metadata_bytes).await
                .map_err(|e| AppendError::WriteError { message: format!("metadata write failed: {}", e) })?;
        }
        writer_event_batch.close().await
            .map_err(|e| AppendError::WriteError { message: format!("event batch close failed: {}", e) })?;
        writer_metadata.close().await
            .map_err(|e| AppendError::WriteError { message: format!("metadata close failed: {}", e) })?;

        for item in self.append_event_batch_queue.drain(..){
            self.data_cache.push(BatchMetadataItemPair {
                metadata: item.event_batch_metadata,
                item: item.event_batch_item,
            });
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
            if let Some(&last_event_index) = self.client_event_indexes.get(&append_options.client_id) {
                let attempted_event_index = events.iter().map(|e| e.client_event_index).min().unwrap_or(0);
                if attempted_event_index <= last_event_index {
                    return Err(AppendError::ClientIdempotencyViolation {
                        client_id: append_options.client_id,
                        last_event_index,
                        attempted_event_index,
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

        let metadata_bytes = bincode::encode_to_vec(&event_batch_metadata, BINCODE_CONFIG_FIXED)
            .map_err(|e| AppendError::SerializationError { 
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
            event_batch_index: self.next_event_batch_index,
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