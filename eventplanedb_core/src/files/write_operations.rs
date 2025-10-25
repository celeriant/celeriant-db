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
    pub event_batches_buffer_size_bytes: usize,
    pub event_batches_write_behind_count: usize,
    pub metadata_write_behind_count: usize,
    pub max_data_cache_size_bytes: usize,
    pub wal_sync_delay_micros: u64,
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
    metadata_stream_writer: DmaStreamWriter,
    event_batches_stream_writer: DmaStreamWriter,
    data_cache: Vec<BatchMetadataItemPair>,
    next_event_index: u64,
    next_event_batch_index: u64,
    client_event_indexes: HashMap<u128, u64>,
    max_data_cache_size_bytes: usize,
    bloom_filter: BloomFilter,
    event_type_dedup: HashSet<u64>,
    wal_sync_delay: Duration,
    wal_sync_event: RefCell<Option<Rc<LocalEvent>>>,
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

fn stream_writer(dma_file: DmaFile, buffer_size: usize, write_behind: usize) -> DmaStreamWriter {
    DmaStreamWriterBuilder::new(dma_file)
        .with_buffer_size(buffer_size)
        .with_write_behind(write_behind)
        .build()
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
        let metadata_stream_writer = stream_writer(
            metadata_dma_file, METADATA_BATCH_SIZE_BYTES, aggregate_write_config.metadata_write_behind_count);

        let event_batches_dma_file = append_only_file(path_event_batches).await?;
        let event_batches_stream_writer = stream_writer(
            event_batches_dma_file, aggregate_write_config.event_batches_buffer_size_bytes, aggregate_write_config.event_batches_write_behind_count);

        let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);
        
        Ok(WriteOperations {
            metadata_stream_writer, 
            event_batches_stream_writer, 
            data_cache, 
            next_event_batch_index, 
            next_event_index, 
            client_event_indexes,
            max_data_cache_size_bytes: aggregate_write_config.max_data_cache_size_bytes,
            bloom_filter,
            event_type_dedup: HashSet::new(),
            wal_sync_delay: Duration::from_micros(aggregate_write_config.wal_sync_delay_micros),
            wal_sync_event: RefCell::new(None),
        })
    }

    pub async fn sync(&mut self) -> Result<(), AppendError> {

        self.event_batches_stream_writer.sync().await
            .map_err(|e| AppendError::WriteError { 
                message: format!("Failed to flush event batch writer: {e}")
            })?;
        self.metadata_stream_writer.sync().await
            .map_err(|e| AppendError::WriteError { 
                message: format!("Failed to flush event batch writer: {e}")
            })?;
        Ok(())
    }

    async fn sync_with_delay(&mut self) -> Result<(), AppendError> {
        // Check if there's already a sync in progress
        let maybe_event = self.wal_sync_event.borrow().as_ref().cloned();
        
        if let Some(event) = maybe_event {
            // A sync is already scheduled, wait for it
            event.listen().await;
        } else {
            // No sync scheduled, create new event and schedule one
            let event = Rc::new(LocalEvent::new());
            self.wal_sync_event.replace(Some(event.clone()));
            
            // Sleep for the delay period
            sleep(self.wal_sync_delay).await;
            
            // Clear the event before sync
            self.wal_sync_event.replace(None);
            
            // Do the actual sync
            self.sync().await?;
            
            // Notify waiters
            event.notify();
        }
        
        Ok(())
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

    /// We require the ownership of the events to be transferred, as they will be stored in the in-memory cache
    /// The events are also mutable as we need to filter out events for client idempotency requirements
    pub async fn append_events(&mut self, mut events: Vec<EventItem>, append_options: &AppendOptions) -> Result<AppendResult, AppendError> {
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

        // Serialize and write event batch and metadata

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

        let events_written = event_batch_item.events.len();

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

        // Write event batch first, because readers use metadata to find event batch locations in the file
        self.event_batches_stream_writer.write_all(&compressed_event_batch_item).await
            .map_err(|e| AppendError::WriteError { 
                message: format!("Failed to write event batch: {e}")
            })?;

        // Write metadata
        self.metadata_stream_writer.write_all(&metadata_bytes).await
            .map_err(|e| AppendError::WriteError { 
                message: format!("Failed to write metadata: {e}")
            })?;

        // If we sync on every append, we kill thoughput. But we don't want to return ack to clients until data is safely on disk.
        // So we must do a periodic sync (eg. every 20ms) and force all writes to wait.
        // If can sync now (haven't synced for > 20ms), do it
        // else, wait until either the remaining delay time has passed then sync;
        // or another task (same glommio executor thread) for this aggregate beats us to the sync

        // Schedule sync and wait for it to complete
        self.sync_with_delay().await?;
        
        // Update in-memory cache with new batch and metadata
        // TODO: enforce max cache size by evicting oldest entries if needed using max_data_cache_size_bytes
        self.data_cache.push(BatchMetadataItemPair {
            metadata: event_batch_metadata,
            item: event_batch_item,
        });

        let event_batch_index = self.next_event_batch_index;

        // Update next event index, next event batch index, client event indexes
        self.next_event_index = next_event_index;
        self.next_event_batch_index = self.next_event_batch_index.saturating_add(1);
        self.client_event_indexes
            .insert(append_options.client_id, latest_client_event_index);

        Ok(AppendResult {
            event_batch_index,
            events_written,
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
            event_batches_buffer_size_bytes: 1024 * 1024,
            event_batches_write_behind_count: 4,
            metadata_write_behind_count: 4,
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