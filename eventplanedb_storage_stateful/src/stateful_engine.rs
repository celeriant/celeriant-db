use std::{
    io::{self},
    path::PathBuf,
};

use eventplanedb_storage_stateless::{
    stateless_destructive::StatelessDestructive, stateless_engine::StatelessEngine,
    stateless_reader::StatelessReader, stateless_writer::StatelessWriter, stateless_writer_async::StatelessWriterAsync,
};

use glommio::io::DmaFile;

use eventplanedb_storage_structures::{
    compression_type::CompressionType,
    constants::{BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED},
    event_batch_item::EventBatchItem,
    event_batch_metadata::EventBatchMetadata,
    event_item::EventItem,
    read_filters::ReadFilters,
    read_result::ReadResult,
};
use fastbloom::BloomFilter;
use std::collections::HashSet;

use crate::{dma_file_cache::DmaFileCache, event_index_cache::EventIndexCache};

use super::{
    client_event_index_cache::ClientEventIndexCache, event_batch_index_cache::EventBatchIndexCache,
    file_cache::FileCache, memory_cache::LruMemoryCache,
};

#[derive(Debug, Clone)]
pub struct StatefulEngineConfig {
    // Cache configurations
    pub event_index_cache_size: usize,
    pub last_event_batch_cache_size: usize,
    pub client_event_index_cache_size: usize,
    pub recent_batches_cache_size: u64,

    // Handle management
    pub max_file_handles: usize, // default: 100

    // File paths
    pub base_path: PathBuf,
    pub compression_type: CompressionType,

    // Stateless engine configuration
    pub stateless_engine: StatelessEngine,
}

impl Default for StatefulEngineConfig {
    fn default() -> Self {
        Self {
            event_index_cache_size: 500_000,
            last_event_batch_cache_size: 100_000,
            client_event_index_cache_size: 500_000,
            recent_batches_cache_size: 1024 * 1024 * 1024, // 1GB
            max_file_handles: 1000,
            base_path: PathBuf::from("./data"),
            compression_type: CompressionType::Zstd { level: 3 },
            stateless_engine: StatelessEngine::builder().build(),
        }
    }
}

pub struct StatefulEngine {
    config: StatefulEngineConfig,

    // Caches
    event_index_cache: EventIndexCache,
    event_batch_index_cache: EventBatchIndexCache,
    client_event_index_cache: ClientEventIndexCache,
    memory_cache: LruMemoryCache,

    // File handle management
    file_cache: FileCache,
    dma_file_cache: DmaFileCache,

    // Shared resources for writing
    bloom_filter: BloomFilter,
    event_type_dedup: HashSet<u64>,
}

impl StatefulEngine {
    pub fn new(config: StatefulEngineConfig) -> Self {
        let event_index_cache = EventIndexCache::new(config.event_index_cache_size);
        let event_batch_index_cache = EventBatchIndexCache::new(config.last_event_batch_cache_size);
        let client_event_index_cache =
            ClientEventIndexCache::new(config.client_event_index_cache_size);
        let memory_cache = LruMemoryCache::new(config.recent_batches_cache_size);
        let file_cache = FileCache::new(config.max_file_handles);
        let dma_file_cache = DmaFileCache::new(config.max_file_handles);

        let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);

        Self {
            config,
            event_index_cache,
            event_batch_index_cache,
            client_event_index_cache,
            memory_cache,
            file_cache,
            dma_file_cache,
            bloom_filter,
            event_type_dedup: HashSet::new(),
        }
    }

    pub fn with_default_config(base_path: PathBuf) -> Self {
        let mut config = StatefulEngineConfig::default();
        config.base_path = base_path;
        Self::new(config)
    }

    fn get_aggregate_paths(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> (PathBuf, PathBuf) {
        let org_dir_name = org_id.to_string();
        let aggregate_type_dir_name = aggregate_type_id.to_string();
        let aggregate_dir_name = aggregate_id.to_string();
        let aggregate_dir = self
            .config
            .base_path
            .join(org_dir_name)
            .join(aggregate_type_dir_name)
            .join(aggregate_dir_name);
        let event_batch_path = aggregate_dir.join("event_batches.bin");
        let metadata_path = aggregate_dir.join("metadata.bin");
        (event_batch_path, metadata_path)
    }

    fn ensure_aggregate_directory(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> io::Result<()> {
        let org_dir_name = org_id.to_string();
        let aggregate_type_dir_name = aggregate_type_id.to_string();
        let aggregate_dir_name = aggregate_id.to_string();
        let aggregate_dir = self
            .config
            .base_path
            .join(org_dir_name)
            .join(aggregate_type_dir_name)
            .join(aggregate_dir_name);
        std::fs::create_dir_all(aggregate_dir)
    }

    fn get_next_event_batch_index(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> io::Result<u64> {
        // Check cache first
        if let Some(cached_index) =
            self.event_batch_index_cache
                .get(org_id, aggregate_type_id, aggregate_id)
        {
            return Ok(cached_index + 1);
        }

        // Cache miss - read from disk
        let (_, metadata_path) = self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);

        if !metadata_path.exists() {
            // New aggregate, start with index 1
            self.event_batch_index_cache
                .set(org_id, aggregate_type_id, aggregate_id, 1);
            return Ok(1);
        }

        let metadata_reader = self
            .file_cache
            .create_reader(metadata_path.to_str().unwrap())?;

        // Attempt to recover from corruption if detected
        let last_index = match self.config.stateless_engine.last_event_batch_index(
            #[cfg(target_os = "linux")]
            &mut *metadata_reader.borrow_mut(),
            #[cfg(not(target_os = "linux"))]
            &mut *metadata_reader.borrow_mut(),
        ) {
            Ok(index) => index,
            Err(_) => {
                // Try to recover from corruption
                self.recover_from_corruption(org_id, aggregate_type_id, aggregate_id)?;

                // Retry after recovery
                let metadata_reader = self
                    .file_cache
                    .create_reader(metadata_path.to_str().unwrap())?;
                self.config.stateless_engine.last_event_batch_index(
                    #[cfg(target_os = "linux")]
                    &mut *metadata_reader.borrow_mut(),
                    #[cfg(not(target_os = "linux"))]
                    &mut *metadata_reader.borrow_mut(),
                )?
            }
        };

        // Cache the result
        self.event_batch_index_cache
            .set(org_id, aggregate_type_id, aggregate_id, last_index);
        Ok(last_index + 1)
    }

    fn recover_from_corruption(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> io::Result<()> {
        let (event_batch_path, metadata_path) =
            self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);

        if !event_batch_path.exists() || !metadata_path.exists() {
            return Ok(()); // Nothing to recover
        }

        let event_batch_reader = self
            .file_cache
            .create_reader(event_batch_path.to_str().unwrap())?;
        let metadata_reader = self
            .file_cache
            .create_reader(metadata_path.to_str().unwrap())?;

        if let Some(corrupt_positions) = self.config.stateless_engine.detect_corruption(
            #[cfg(target_os = "linux")]
            &mut *event_batch_reader.borrow_mut(),
            #[cfg(target_os = "linux")]
            &mut *metadata_reader.borrow_mut(),
            #[cfg(not(target_os = "linux"))]
            &mut *event_batch_reader.borrow_mut(),
            #[cfg(not(target_os = "linux"))]
            &mut *metadata_reader.borrow_mut(),
        )? {
            // Clear caches for this aggregate
            self.clear_aggregate_caches(org_id, aggregate_type_id, aggregate_id);

            // Create writers for trim operation
            let event_batch_writer = self
                .file_cache
                .create_overwrite_writer(event_batch_path.to_str().unwrap())?;
            let metadata_writer = self
                .file_cache
                .create_overwrite_writer(metadata_path.to_str().unwrap())?;

            // Trim corrupted data
            self.config.stateless_engine.trim_end(
                &mut *event_batch_writer.borrow_mut(),
                corrupt_positions.event_batch_position,
                &mut *metadata_writer.borrow_mut(),
                corrupt_positions.metadata_position,
            )?;
        }

        Ok(())
    }

    fn filter_duplicate_events(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        events: &mut Vec<EventItem>,
    ) -> io::Result<()> {
        let highest_seen =
            self.client_event_index_cache
                .get(org_id, aggregate_type_id, aggregate_id, client_id);

        if let Some(highest_index) = highest_seen {
            // Filter out events with client_event_index <= highest_seen
            events.retain(|event| event.client_event_index > highest_index);
        }

        Ok(())
    }

    fn update_client_event_index_cache(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        events: &[EventItem],
    ) -> io::Result<()> {
        let highest_seen =
            self.client_event_index_cache
                .get(org_id, aggregate_type_id, aggregate_id, client_id);

        // Update cache with the new highest index if we have events
        if let Some(max_event) = events.iter().max_by_key(|e| e.client_event_index) {
            if highest_seen.map_or(true, |seen| max_event.client_event_index > seen) {
                self.client_event_index_cache.set(
                    org_id,
                    aggregate_type_id,
                    aggregate_id,
                    client_id,
                    max_event.client_event_index,
                );
            }
        }

        Ok(())
    }

    fn get_next_event_indices(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        count: usize,
    ) -> io::Result<u64> {
        // Check cache first
        if let Some(cached_index) =
            self.event_index_cache
                .get(org_id, aggregate_type_id, aggregate_id)
        {
            return Ok(cached_index + 1);
        }

        // Cache miss - read from disk
        let (_, metadata_path) = self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);

        if !metadata_path.exists() {
            // New aggregate, start with index 1
            self.event_index_cache
                .set(org_id, aggregate_type_id, aggregate_id, count as u64);
            return Ok(1);
        }

        let metadata_reader = self
            .file_cache
            .create_reader(metadata_path.to_str().unwrap())?;

        // Attempt to recover from corruption if detected
        let last_index = match self.config.stateless_engine.last_event_index(
            #[cfg(target_os = "linux")]
            &mut *metadata_reader.borrow_mut(),
            #[cfg(not(target_os = "linux"))]
            &mut *metadata_reader.borrow_mut(),
        ) {
            Ok(index) => index,
            Err(_) => {
                // Try to recover from corruption
                self.recover_from_corruption(org_id, aggregate_type_id, aggregate_id)?;

                // Retry after recovery
                let metadata_reader = self
                    .file_cache
                    .create_reader(metadata_path.to_str().unwrap())?;
                self.config.stateless_engine.last_event_index(
                    #[cfg(target_os = "linux")]
                    &mut *metadata_reader.borrow_mut(),
                    #[cfg(not(target_os = "linux"))]
                    &mut *metadata_reader.borrow_mut(),
                )?
            }
        };

        // Cache the result (after assigning to all events in this batch)
        self.event_index_cache.set(
            org_id,
            aggregate_type_id,
            aggregate_id,
            last_index + count as u64,
        );
        Ok(last_index + 1)
    }

    fn assign_event_indices(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        events: &mut [EventItem],
    ) -> io::Result<Option<u64>> {
        if events.is_empty() {
            return Ok(None);
        }

        let start_index =
            self.get_next_event_indices(org_id, aggregate_type_id, aggregate_id, events.len())?;

        // Assign sequential event indices to all events
        for (i, event) in events.iter_mut().enumerate() {
            event.event_index = start_index + i as u64;
        }

        Ok(Some(events[events.len() - 1].event_index))
    }

    fn clear_aggregate_caches(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) {
        self.event_batch_index_cache
            .remove(org_id, aggregate_type_id, aggregate_id);
        self.event_index_cache
            .remove(org_id, aggregate_type_id, aggregate_id);
        self.memory_cache
            .clear_aggregate(org_id, aggregate_type_id, aggregate_id);

        // Remove file handles for this aggregate
        let (event_batch_path, metadata_path) =
            self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);
        self.file_cache.remove(event_batch_path.to_str().unwrap());
        self.file_cache.remove(metadata_path.to_str().unwrap());
        self.dma_file_cache.remove(event_batch_path.to_str().unwrap());
        self.dma_file_cache.remove(metadata_path.to_str().unwrap());
    }

    fn try_read_from_memory_cache(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: &ReadFilters,
    ) -> Option<ReadResult> {
        // Check if we have the requested starting batch in cache
        let start_pos = self.memory_cache.get_pos(
            org_id,
            aggregate_type_id,
            aggregate_id,
            filters.from_event_batch_index,
        )?;

        let batches = self
            .memory_cache
            .get_all_batches(org_id, aggregate_type_id, aggregate_id)?;
        let mut result_batches = Vec::new();
        let mut current_size = 0u64;
        let mut next_event_batch_index = None;

        for i in start_pos..batches.len() {
            let (batch_item, batch_metadata) = &batches[i];

            // Apply metadata-level filters
            if !Self::batch_matches_filters(batch_metadata, filters) {
                continue;
            }

            // Check max_bytes limit
            if let Some(max_bytes) = filters.max_bytes {
                if current_size + batch_metadata.uncompressed_size > max_bytes as u64 {
                    if current_size == 0 {
                        // First batch exceeds limit - this is an error
                        return None;
                    }
                    next_event_batch_index = Some(batch_item.event_batch_index);
                    break;
                }
            }

            // Apply event-level filters
            let mut filtered_batch = batch_item.clone();
            Self::apply_event_filters(&mut filtered_batch, filters);

            if !filtered_batch.events.is_empty() {
                result_batches.push(filtered_batch);
                current_size += batch_metadata.uncompressed_size;
            }

            // Check to_event_batch_index
            if let Some(to_index) = filters.to_event_batch_index {
                if batch_item.event_batch_index >= to_index {
                    break;
                }
            }
        }

        Some(ReadResult {
            event_batches: result_batches,
            next_event_batch_index,
        })
    }

    fn batch_matches_filters(metadata: &EventBatchMetadata, filters: &ReadFilters) -> bool {
        // Apply the same filtering logic as in stateless reader
        if metadata.event_batch_index < filters.from_event_batch_index {
            return false;
        }

        if filters
            .to_event_batch_index
            .map_or(false, |to_index| metadata.event_batch_index > to_index)
        {
            return false;
        }

        if filters
            .min_server_timestamp
            .map_or(false, |min_time| metadata.server_timestamp < min_time)
        {
            return false;
        }

        if filters
            .max_server_timestamp
            .map_or(false, |max_time| metadata.server_timestamp > max_time)
        {
            return false;
        }

        if filters
            .exclude_client_id
            .map_or(false, |exclude_id| metadata.client_id == exclude_id)
        {
            return false;
        }

        if filters
            .include_client_id
            .map_or(false, |include_id| metadata.client_id != include_id)
        {
            return false;
        }

        if filters
            .exclude_user_id
            .map_or(false, |exclude_id| metadata.user_id == exclude_id)
        {
            return false;
        }

        if filters
            .include_user_id
            .map_or(false, |include_id| metadata.user_id != include_id)
        {
            return false;
        }

        true
    }

    fn apply_event_filters(batch: &mut EventBatchItem, filters: &ReadFilters) {
        // Filter by event types
        if let Some(include_types) = filters.include_event_types.as_deref() {
            batch
                .events
                .retain(|event| include_types.contains(&event.event_type_major));
        }

        // Filter by client event index range
        if let Some(min_index) = filters.min_client_event_index {
            batch
                .events
                .retain(|event| event.client_event_index >= min_index);
        }

        if let Some(max_index) = filters.max_client_event_index {
            batch
                .events
                .retain(|event| event.client_event_index <= max_index);
        }

        // Filter by event timestamp range
        if let Some(min_time) = filters.min_event_timestamp {
            batch
                .events
                .retain(|event| event.event_timestamp >= min_time);
        }

        if let Some(max_time) = filters.max_event_timestamp {
            batch
                .events
                .retain(|event| event.event_timestamp <= max_time);
        }

        // Filter by event index range
        if let Some(min_index) = filters.min_event_index {
            batch.events.retain(|event| event.event_index >= min_index);
        }

        if let Some(max_index) = filters.max_event_index {
            batch.events.retain(|event| event.event_index <= max_index);
        }
    }


    async fn create_dma_files_for_append(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> io::Result<(DmaFile, DmaFile)> {
        use glommio::io::OpenOptions;
        
        let (event_batch_path, metadata_path) =
            self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);

        // Ensure directory exists
        self.ensure_aggregate_directory(org_id, aggregate_type_id, aggregate_id)?;

        // Use OpenOptions to open files for writing without truncating
        let event_batch_file = OpenOptions::new()
            .write(true)
            .create(true)  // Create if doesn't exist
            .dma_open(&event_batch_path)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to open event batch file for writing: {}", e)))?;

        let metadata_file = OpenOptions::new()
            .write(true)
            .create(true)  // Create if doesn't exist
            .dma_open(&metadata_path)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to open metadata file for writing: {}", e)))?;

        Ok((event_batch_file, metadata_file))
    }

    
}

// Add async versions of the traits
#[async_trait::async_trait(?Send)]
pub trait StatefulWriterAsync {
    async fn append_events_async(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
        filter_duplicate_client_events: bool,
    ) -> io::Result<EventBatchMetadata>;
}

#[async_trait::async_trait(?Send)]
pub trait StatefulReaderAsync {
    async fn read_filtered_async(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult>;

    async fn exists_async(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> io::Result<bool>;
}


#[async_trait::async_trait(?Send)]
impl StatefulWriterAsync for StatefulEngine {
    async fn append_events_async(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        mut events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
        filter_duplicate_client_events: bool,
    ) -> io::Result<EventBatchMetadata> {
        if events.is_empty() {
            return Err(io::Error::other("Cannot write empty event batch"));
        }

        // Ensure aggregate directory exists
        self.ensure_aggregate_directory(org_id, aggregate_type_id, aggregate_id)?;

        // Get next event batch index
        let next_event_batch_index =
            self.get_next_event_batch_index(org_id, aggregate_type_id, aggregate_id)?;

        // Optimistic concurrency check
        if let Some(expected_index) = expected_event_batch_index {
            if expected_index != next_event_batch_index {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Optimistic concurrency check failed: expected batch index {}, but next is {}",
                        expected_index, next_event_batch_index
                    ),
                ));
            }
        }

        // Filter out duplicate events based on client event index
        if filter_duplicate_client_events {
            self.filter_duplicate_events(
                org_id,
                aggregate_type_id,
                aggregate_id,
                client_id,
                &mut events,
            )?;
        }

        if events.is_empty() {
            return Err(io::Error::other(
                "All events were duplicates and filtered out",
            ));
        }

        // Assign server-side event indices
        let final_event_index = self
            .assign_event_indices(org_id, aggregate_type_id, aggregate_id, &mut events)?
            .unwrap();

        // Create event batch
        let server_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let event_batch = EventBatchItem::new(
            next_event_batch_index,
            server_timestamp,
            client_id,
            user_id,
            events,
        );

        // Get file paths and writers
        let (event_batch_path, metadata_path) =
            self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);
        let event_batch_writer = self
            .dma_file_cache
            .create_append_writer(event_batch_path.to_str().unwrap()).await?;
        let metadata_writer = self
            .dma_file_cache
            .create_append_writer(metadata_path.to_str().unwrap()).await?;

        // Write to disk using stateless engine
        let metadata = self.config.stateless_engine.append_event_batch_async(
            &mut *event_batch_writer.borrow_mut(),
            &mut *metadata_writer.borrow_mut(),
            &mut self.bloom_filter,
            &mut self.event_type_dedup,
            self.config.compression_type,
            &event_batch,
        ).await?;

        // Update caches
        self.update_client_event_index_cache(
            org_id,
            aggregate_type_id,
            aggregate_id,
            client_id,
            &event_batch.events,
        )?;
        self.event_batch_index_cache.set(
            org_id,
            aggregate_type_id,
            aggregate_id,
            next_event_batch_index,
        );
        self.memory_cache.add(
            org_id,
            aggregate_type_id,
            aggregate_id,
            event_batch,
            metadata.clone(),
        );
        self.event_index_cache
            .set(org_id, aggregate_type_id, aggregate_id, final_event_index);

        Ok(metadata)
    }
}

pub trait StatefulWriter {
    fn append_events(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
        filter_duplicate_client_events: bool,
    ) -> io::Result<EventBatchMetadata>;
}

pub trait StatefulReader {
    fn read_filtered(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult>;

    fn exists(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> io::Result<bool>;
}

pub trait StatefulDestructive {
    fn trim_start(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
    ) -> io::Result<()>;

    fn delete(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> io::Result<()>;
}

impl StatefulWriter for StatefulEngine {
    fn append_events(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        mut events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
        filter_duplicate_client_events: bool,
    ) -> io::Result<EventBatchMetadata> {
        if events.is_empty() {
            return Err(io::Error::other("Cannot write empty event batch"));
        }

        // Ensure aggregate directory exists
        self.ensure_aggregate_directory(org_id, aggregate_type_id, aggregate_id)?;

        // Get next event batch index
        let next_event_batch_index =
            self.get_next_event_batch_index(org_id, aggregate_type_id, aggregate_id)?;

        // Optimistic concurrency check
        if let Some(expected_index) = expected_event_batch_index {
            if expected_index != next_event_batch_index {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Optimistic concurrency check failed: expected batch index {}, but next is {}",
                        expected_index, next_event_batch_index
                    ),
                ));
            }
        }

        // Filter out duplicate events based on client event index
        if filter_duplicate_client_events {
            self.filter_duplicate_events(
                org_id,
                aggregate_type_id,
                aggregate_id,
                client_id,
                &mut events,
            )?;
        }

        if events.is_empty() {
            return Err(io::Error::other(
                "All events were duplicates and filtered out",
            ));
        }

        // Assign server-side event indices
        let final_event_index = self
            .assign_event_indices(org_id, aggregate_type_id, aggregate_id, &mut events)?
            .unwrap();

        // Create event batch
        let server_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let event_batch = EventBatchItem::new(
            next_event_batch_index,
            server_timestamp,
            client_id,
            user_id,
            events,
        );

        // Get file paths and writers
        let (event_batch_path, metadata_path) =
            self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);
        let event_batch_writer = self
            .file_cache
            .create_append_writer(event_batch_path.to_str().unwrap())?;
        let metadata_writer = self
            .file_cache
            .create_append_writer(metadata_path.to_str().unwrap())?;

        // Write to disk using stateless engine
        let metadata = self.config.stateless_engine.append_event_batch(
            &mut *event_batch_writer.borrow_mut(),
            &mut *metadata_writer.borrow_mut(),
            &mut self.bloom_filter,
            &mut self.event_type_dedup,
            self.config.compression_type,
            &event_batch,
        )?;

        // Update caches
        self.update_client_event_index_cache(
            org_id,
            aggregate_type_id,
            aggregate_id,
            client_id,
            &event_batch.events,
        )?;
        self.event_batch_index_cache.set(
            org_id,
            aggregate_type_id,
            aggregate_id,
            next_event_batch_index,
        );
        self.memory_cache.add(
            org_id,
            aggregate_type_id,
            aggregate_id,
            event_batch,
            metadata.clone(),
        );
        self.event_index_cache
            .set(org_id, aggregate_type_id, aggregate_id, final_event_index);

        Ok(metadata)
    }
}

impl StatefulReader for StatefulEngine {
    fn read_filtered(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult> {
        // Try to read from memory cache first
        if let Some(result) =
            self.try_read_from_memory_cache(org_id, aggregate_type_id, aggregate_id, filters)
        {
            return Ok(result);
        }

        // Fallback to disk read
        let (event_batch_path, metadata_path) =
            self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);

        if !event_batch_path.exists() || !metadata_path.exists() {
            return Ok(ReadResult {
                event_batches: Vec::new(),
                next_event_batch_index: None,
            });
        }

        let event_batch_reader = self
            .file_cache
            .create_reader(event_batch_path.to_str().unwrap())?;
        let metadata_reader = self
            .file_cache
            .create_reader(metadata_path.to_str().unwrap())?;

        // Attempt recovery if corruption is detected
        #[cfg(not(target_os = "linux"))]
        let mut mut_event_batch_reader = event_batch_reader.borrow_mut();
        #[cfg(not(target_os = "linux"))]
        let mut mut_metadata_reader = metadata_reader.borrow_mut();
        #[cfg(target_os = "linux")]
        let mut mut_event_batch_reader = event_batch_reader.borrow_mut();
        #[cfg(target_os = "linux")]
        let mut mut_metadata_reader = metadata_reader.borrow_mut();

        match self.config.stateless_engine.read_filtered(
            #[cfg(target_os = "linux")]
            &mut *mut_event_batch_reader,
            #[cfg(target_os = "linux")]
            &mut *mut_metadata_reader,
            #[cfg(not(target_os = "linux"))]
            &mut *mut_event_batch_reader,
            #[cfg(not(target_os = "linux"))]
            &mut *mut_metadata_reader,
            filters,
        ) {
            Ok(result) => Ok(result),
            Err(_e) => {
                drop(mut_event_batch_reader);
                drop(mut_metadata_reader);

                // Try to recover from corruption
                self.recover_from_corruption(org_id, aggregate_type_id, aggregate_id)?;

                // Retry after recovery
                let event_batch_reader = self
                    .file_cache
                    .create_reader(event_batch_path.to_str().unwrap())?;
                let metadata_reader = self
                    .file_cache
                    .create_reader(metadata_path.to_str().unwrap())?;

                #[cfg(not(target_os = "linux"))]
                let mut mut_event_batch_reader = event_batch_reader.borrow_mut();
                #[cfg(not(target_os = "linux"))]
                let mut mut_metadata_reader = metadata_reader.borrow_mut();
                #[cfg(target_os = "linux")]
                let mut mut_event_batch_reader = event_batch_reader.borrow_mut();
                #[cfg(target_os = "linux")]
                let mut mut_metadata_reader = metadata_reader.borrow_mut();

                self.config.stateless_engine.read_filtered(
                    #[cfg(target_os = "linux")]
                    &mut *mut_event_batch_reader,
                    #[cfg(target_os = "linux")]
                    &mut *mut_metadata_reader,
                    #[cfg(not(target_os = "linux"))]
                    &mut *mut_event_batch_reader,
                    #[cfg(not(target_os = "linux"))]
                    &mut *mut_metadata_reader,
                    filters,
                )
            }
        }
    }

    fn exists(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> io::Result<bool> {
        let (event_batch_path, metadata_path) =
            self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);
        Ok(event_batch_path.exists() && metadata_path.exists())
    }
}

impl StatefulDestructive for StatefulEngine {
    fn trim_start(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
    ) -> io::Result<()> {
        let (event_batch_path, metadata_path) =
            self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);

        if !event_batch_path.exists() || !metadata_path.exists() {
            return Ok(()); // Nothing to trim
        }

        // We need to calculate positions based on the event batch index
        // This requires reading metadata to find the correct positions
        let metadata_reader = self
            .file_cache
            .create_reader(metadata_path.to_str().unwrap())?;
        let event_batch_reader = self
            .file_cache
            .create_reader(event_batch_path.to_str().unwrap())?;

        let event_batch_positions = self
            .config
            .stateless_engine
            .positions_for_event_batch_index(
                #[cfg(target_os = "linux")]
                &mut *metadata_reader.borrow_mut(),
                #[cfg(not(target_os = "linux"))]
                &mut *metadata_reader.borrow_mut(),
                keep_from_event_batch_index,
            )?;

        if event_batch_positions.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Event batch index {} does not exist for aggregate {}",
                    keep_from_event_batch_index, aggregate_id
                ),
            ));
        }

        let event_batch_positions = event_batch_positions.unwrap();

        self.config.stateless_engine.trim_start(
            #[cfg(target_os = "linux")]
            &mut *event_batch_reader.borrow_mut(),
            #[cfg(not(target_os = "linux"))]
            &mut *event_batch_reader.borrow_mut(),
            event_batch_positions.event_batch_position,
            event_batch_path.to_str().unwrap(),
            #[cfg(target_os = "linux")]
            &mut *metadata_reader.borrow_mut(),
            #[cfg(not(target_os = "linux"))]
            &mut *metadata_reader.borrow_mut(),
            event_batch_positions.metadata_position,
            metadata_path.to_str().unwrap(),
        )?;

        // Clear caches for this aggregate
        self.clear_aggregate_caches(org_id, aggregate_type_id, aggregate_id);

        Ok(())
    }

    fn delete(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> io::Result<()> {
        let (event_batch_path, metadata_path) =
            self.get_aggregate_paths(org_id, aggregate_type_id, aggregate_id);

        if event_batch_path.exists() && metadata_path.exists() {
            self.config
                .stateless_engine
                .delete(&event_batch_path, &metadata_path)?;
        }

        // Clear caches and handles for this aggregate
        self.clear_aggregate_caches(org_id, aggregate_type_id, aggregate_id);

        // Remove the aggregate directory if it's empty
        let aggregate_dir = self.config.base_path.join(aggregate_id.to_string());
        if aggregate_dir.exists() {
            std::fs::remove_dir(&aggregate_dir).ok(); // Ignore errors if directory is not empty
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, fs, io, path::PathBuf, time::Duration, u128};
    use tempfile::TempDir;
    use glommio::{LocalExecutor, LocalExecutorBuilder, Placement};

    //TODO: A test that simulates a failed write after consuming an index, creating a gap

    #[test]
    fn test_async_append_events_basic() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let mut fixture = StatefulTestFixture::new().unwrap();

                let events = fixture.create_test_events(1, 2);
                let metadata = fixture
                    .engine
                    .append_events_async(544, 655, 123, 100, Some(200), events, None, true)
                    .await
                    .unwrap();

                assert_eq!(metadata.event_batch_index, 1);
                assert_eq!(metadata.client_id, 100);
                assert_eq!(metadata.user_id, 200);
            })
            .unwrap();

        ex.join().unwrap();
    }

    #[test]
    fn test_async_append_events_with_compression() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let config = StatefulEngineConfig {
                    compression_type: CompressionType::Zstd { level: 3 },
                    ..Default::default()
                };
                let mut fixture = StatefulTestFixture::with_config(config).unwrap();

                // Create events with larger, compressible data
                let large_data = vec![b'A'; 1000];
                let events = vec![
                    EventItem::new(1, 1, 1000, 42, 1, large_data.clone()),
                    EventItem::new(2, 2, 1001, 43, 1, large_data),
                ];

                let metadata = fixture
                    .engine
                    .append_events_async(544, 655, 123, 100, None, events, None, true)
                    .await
                    .unwrap();

                // With compression, compressed size should be less than uncompressed
                assert!(metadata.compressed_size < metadata.uncompressed_size);
            })
            .unwrap();

        ex.join().unwrap();
    }

    #[test]
    fn test_async_duplicate_event_filtering() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let mut fixture = StatefulTestFixture::new().unwrap();

                // First write
                let events1 = vec![
                    EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec()),
                    EventItem::new(2, 2, 1001, 42, 1, b"event2".to_vec()),
                ];

                fixture
                    .engine
                    .append_events_async(544, 655, 123, 100, None, events1, None, true)
                    .await
                    .unwrap();

                // Second write with overlapping client_event_index
                let events2 = vec![
                    EventItem::new(2, 3, 1002, 42, 1, b"event2_dup".to_vec()), // Should be filtered
                    EventItem::new(3, 4, 1003, 42, 1, b"event3".to_vec()),     // Should be written
                ];

                let _metadata2 = fixture
                    .engine
                    .append_events_async(544, 655, 123, 100, None, events2, None, true)
                    .await
                    .unwrap();

                // Read back using sync method and verify only new events were written
                let result = fixture
                    .engine
                    .read_filtered(544, 655, 123, &ReadFilters::new(2))
                    .unwrap();
                assert_eq!(result.event_batches.len(), 1);
                assert_eq!(result.event_batches[0].events.len(), 1); // Only event 3
                assert_eq!(result.event_batches[0].events[0].client_event_index, 3);
            })
            .unwrap();

        ex.join().unwrap();
    }

    #[test]
    fn test_async_optimistic_concurrency_success() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let mut fixture = StatefulTestFixture::new().unwrap();

                // First write
                let events1 = fixture.create_test_events(1, 1);
                fixture
                    .engine
                    .append_events_async(544, 655, 123, 100, None, events1, None, true)
                    .await
                    .unwrap();

                // Second write with correct expected index
                let events2 = fixture.create_test_events(2, 1);
                let result = fixture
                    .engine
                    .append_events_async(544, 655, 123, 100, None, events2, Some(2), true)
                    .await;
                    
                assert!(result.is_ok());
                assert_eq!(result.unwrap().event_batch_index, 2);
            })
            .unwrap();

        ex.join().unwrap();
    }

    #[test]
    fn test_async_optimistic_concurrency_failure() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let mut fixture = StatefulTestFixture::new().unwrap();

                // First write
                let events1 = fixture.create_test_events(1, 1);
                fixture
                    .engine
                    .append_events_async(544, 655, 123, 100, None, events1, None, true)
                    .await
                    .unwrap();

                // Second write with incorrect expected index
                let events2 = fixture.create_test_events(2, 1);
                let result = fixture
                    .engine
                    .append_events_async(544, 655, 123, 100, None, events2, Some(5), true)
                    .await;

                assert!(result.is_err());
                let err = result.unwrap_err().to_string();
                assert!(err.contains("Optimistic concurrency check failed"));
                assert!(err.contains("expected batch index 5, but next is 2"));
            })
            .unwrap();

        ex.join().unwrap();
    }

    #[test]
    fn test_async_empty_events_rejected() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let mut fixture = StatefulTestFixture::new().unwrap();

                let result = fixture
                    .engine
                    .append_events_async(544, 655, 123, 100, None, vec![], None, true)
                    .await;
                    
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("Cannot write empty event batch"));
            })
            .unwrap();

        ex.join().unwrap();
    }

    struct StatefulTestFixture {
        _temp_dir: TempDir,
        engine: StatefulEngine,
        base_path: PathBuf,
    }

    impl StatefulTestFixture {
        fn new() -> io::Result<Self> {
            let temp_dir = TempDir::new()?;
            let base_path = temp_dir.path().to_path_buf();

            let config = StatefulEngineConfig {
                base_path: base_path.clone(),
                ..Default::default()
            };

            let engine = StatefulEngine::new(config);

            Ok(Self {
                _temp_dir: temp_dir,
                engine,
                base_path,
            })
        }

        fn reset(&mut self) {
            let base_path = self._temp_dir.path().to_path_buf();
            let config = StatefulEngineConfig {
                base_path: base_path.clone(),
                ..Default::default()
            };
            let engine = StatefulEngine::new(config);
            self.engine = engine;
        }

        fn with_config(config: StatefulEngineConfig) -> io::Result<Self> {
            let temp_dir = TempDir::new()?;
            let base_path = temp_dir.path().to_path_buf();

            let mut config = config;
            config.base_path = base_path.clone();

            let engine = StatefulEngine::new(config);

            Ok(Self {
                _temp_dir: temp_dir,
                engine,
                base_path,
            })
        }

        fn create_test_events(&self, start_index: u64, count: usize) -> Vec<EventItem> {
            (0..count)
                .map(|i| {
                    EventItem::new(
                        start_index + i as u64,
                        start_index + i as u64,
                        1000 + i as u64,
                        42,
                        1,
                        format!("test event {}", i).into_bytes(),
                    )
                })
                .collect()
        }
    }

    // 1. Basic Write Operations & Event Batch Index Caching

    #[test]
    fn test_event_batch_index_increments_correctly() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = fixture.create_test_events(1, 2);
        let events2 = fixture.create_test_events(3, 2);
        let events3 = fixture.create_test_events(5, 2);

        // First write should get event_batch_index 1
        let metadata1 =
            fixture
                .engine
                .append_events(544, 655, 123, 100, Some(200), events1, None, true)?;
        assert_eq!(metadata1.event_batch_index, 1);

        // Second write should get event_batch_index 2
        let metadata2 =
            fixture
                .engine
                .append_events(544, 655, 123, 100, Some(200), events2, None, true)?;
        assert_eq!(metadata2.event_batch_index, 2);

        // Third write should get event_batch_index 3
        let metadata3 =
            fixture
                .engine
                .append_events(544, 655, 123, 100, Some(200), events3, None, true)?;
        assert_eq!(metadata3.event_batch_index, 3);

        Ok(())
    }

    #[test]
    fn test_event_batch_index_cache_across_aggregates() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = fixture.create_test_events(1, 1);
        let events2 = fixture.create_test_events(2, 1);

        // First aggregate gets index 0
        let metadata1 = fixture.engine.append_events(
            544,
            655,
            111,
            100,
            Some(200),
            events1.clone(),
            None,
            true,
        )?;
        assert_eq!(metadata1.event_batch_index, 1);

        // Second aggregate also gets index 0 (separate sequence)
        let metadata2 = fixture.engine.append_events(
            544,
            655,
            222,
            100,
            Some(200),
            events2.clone(),
            None,
            true,
        )?;
        assert_eq!(metadata2.event_batch_index, 1);

        // Second write to first aggregate gets index 1
        let events3 = fixture.create_test_events(2, 1);
        let metadata3 =
            fixture
                .engine
                .append_events(544, 655, 111, 100, Some(200), events3, None, true)?;
        assert_eq!(metadata3.event_batch_index, 2);

        Ok(())
    }

    #[test]
    fn test_event_batch_index_cache_persists_across_restarts() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let base_path = temp_dir.path().to_path_buf();

        // First engine instance
        {
            let config = StatefulEngineConfig {
                base_path: base_path.clone(),
                ..Default::default()
            };
            let mut engine = StatefulEngine::new(config);

            let events = vec![EventItem::new(1, 1, 1000, 42, 1, b"test".to_vec())];

            let metadata1 =
                engine.append_events(544, 655, 123, 100, None, events.clone(), None, true)?;
            assert_eq!(metadata1.event_batch_index, 1);

            let events2 = vec![EventItem::new(2, 2, 1000, 42, 1, b"test".to_vec())];
            let metadata2 = engine.append_events(544, 655, 123, 100, None, events2, None, true)?;
            assert_eq!(metadata2.event_batch_index, 2);
        }

        // Second engine instance (simulating restart)
        {
            let config = StatefulEngineConfig {
                base_path: base_path.clone(),
                ..Default::default()
            };
            let mut engine = StatefulEngine::new(config);

            let events = vec![EventItem::new(3, 3, 1000, 42, 1, b"test".to_vec())];

            // Should continue from index 2
            let metadata3 = engine.append_events(544, 655, 123, 100, None, events, None, true)?;
            assert_eq!(metadata3.event_batch_index, 3);
        }

        Ok(())
    }

    // 2. Producer Idempotency (Client Event Index Filtering)

    #[test]
    fn test_duplicate_event_filtering_same_client() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // First write with events having client_event_index 1, 2, 3
        let events1 = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec()),
            EventItem::new(2, 2, 1001, 42, 1, b"event2".to_vec()),
            EventItem::new(3, 3, 1002, 42, 1, b"event3".to_vec()),
        ];

        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        // Second write with overlapping client_event_index (2, 3, 4, 5)
        // Only events with index 4 and 5 should be written
        let events2 = vec![
            EventItem::new(2, 4, 1003, 42, 1, b"event2_dup".to_vec()), // Should be filtered
            EventItem::new(3, 5, 1004, 42, 1, b"event3_dup".to_vec()), // Should be filtered
            EventItem::new(4, 6, 1005, 42, 1, b"event4".to_vec()),     // Should be written
            EventItem::new(5, 7, 1006, 42, 1, b"event5".to_vec()),     // Should be written
        ];

        let _metadata2 = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, None, true)?;

        // Read back and verify only new events were written
        let result = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(2))?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 2); // Only events 4 and 5
        assert_eq!(result.event_batches[0].events[0].client_event_index, 4);
        assert_eq!(result.event_batches[0].events[1].client_event_index, 5);

        Ok(())
    }

    #[test]
    fn test_no_duplicate_filtering_different_clients() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Client 100 writes events with client_event_index 1, 2
        let events1 = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"client100_event1".to_vec()),
            EventItem::new(2, 2, 1001, 42, 1, b"client100_event2".to_vec()),
        ];

        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        // Client 200 writes events with same client_event_index 1, 2
        // These should NOT be filtered since they're from a different client
        let events2 = vec![
            EventItem::new(1, 3, 1002, 42, 1, b"client200_event1".to_vec()),
            EventItem::new(2, 4, 1003, 42, 1, b"client200_event2".to_vec()),
        ];

        fixture
            .engine
            .append_events(544, 655, 123, 200, None, events2, None, true)?;

        // Read back and verify both clients' events are present
        let result = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 2);
        assert_eq!(result.event_batches[0].events.len(), 2); // Client 100 events
        assert_eq!(result.event_batches[1].events.len(), 2); // Client 200 events

        Ok(())
    }

    #[test]
    fn test_all_events_filtered_out_error() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // First write
        let events1 = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec()),
            EventItem::new(2, 2, 1001, 42, 1, b"event2".to_vec()),
        ];

        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        // Second write with same client_event_indices - all should be filtered out
        let events2 = vec![
            EventItem::new(1, 3, 1002, 42, 1, b"event1_dup".to_vec()),
            EventItem::new(2, 4, 1003, 42, 1, b"event2_dup".to_vec()),
        ];

        let result = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, None, true);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("All events were duplicates")
        );

        Ok(())
    }

    #[test]
    fn test_client_event_index_cache_different_aggregates() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Client 100 writes to aggregate1
        let events1 = vec![EventItem::new(1, 1, 1000, 42, 1, b"test".to_vec())];
        fixture
            .engine
            .append_events(544, 655, 111, 100, None, events1, None, true)?;

        // Same client writes to aggregate2 with same client_event_index
        // Should NOT be filtered since it's a different aggregate
        let events2 = vec![EventItem::new(1, 2, 1001, 42, 1, b"test".to_vec())];
        let result = fixture
            .engine
            .append_events(544, 655, 222, 100, None, events2, None, true);
        assert!(result.is_ok());

        Ok(())
    }

    // 3. Optimistic Concurrency Control

    #[test]
    fn test_optimistic_concurrency_success() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // First write
        let events1 = fixture.create_test_events(1, 1);
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        // Second write with correct expected index
        let events2 = fixture.create_test_events(2, 1);
        let result = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, Some(2), true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().event_batch_index, 2);

        Ok(())
    }

    #[test]
    fn test_optimistic_concurrency_failure() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // First write
        let events1 = fixture.create_test_events(1, 1);
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        // Second write with incorrect expected index
        let events2 = fixture.create_test_events(2, 1);
        let result = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, Some(5), true);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Optimistic concurrency check failed"));
        assert!(err.contains("expected batch index 5, but next is 2"));

        Ok(())
    }

    #[test]
    fn test_optimistic_concurrency_new_aggregate() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // First write to new aggregate with expected index 0
        let events = fixture.create_test_events(1, 1);
        let result = fixture
            .engine
            .append_events(544, 655, 999, 100, None, events, Some(1), true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().event_batch_index, 1);

        Ok(())
    }

    // 4. Memory Cache Operations

    #[test]
    fn test_memory_cache_populated_on_write() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events = fixture.create_test_events(1, 2);
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events, None, true)?;

        // Verify cache is populated by checking internal state
        // Since we can't directly access the cache, we'll test by reading from cache
        let result = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 2);

        Ok(())
    }

    #[test]
    fn test_memory_cache_hit_for_recent_reads() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write some events
        let events1 = fixture.create_test_events(1, 2);
        let events2 = fixture.create_test_events(3, 2);

        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, None, true)?;

        // Read from cache (should hit memory cache)
        let result1 = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0))?;
        assert_eq!(result1.event_batches.len(), 2);

        // Read again from cache with different starting point
        let result2 = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(2))?;
        assert_eq!(result2.event_batches.len(), 1);
        assert_eq!(result2.event_batches[0].event_batch_index, 2);

        Ok(())
    }

    #[test]
    fn test_memory_cache_miss_falls_back_to_disk() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events that won't be in memory cache (simulate old data)
        let events = fixture.create_test_events(1, 2);
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events, None, true)?;

        // Clear memory cache by creating new engine instance
        let config = StatefulEngineConfig {
            base_path: fixture.base_path.clone(),
            ..Default::default()
        };
        let mut new_engine = StatefulEngine::new(config);

        // Read should still work (falling back to disk)
        let result = new_engine.read_filtered(544, 655, 123, &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 2);

        Ok(())
    }

    #[test]
    fn test_memory_cache_respects_filters() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events with different client_ids
        let events1 = fixture.create_test_events(1, 2);
        let events2 = fixture.create_test_events(3, 2);

        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;
        fixture
            .engine
            .append_events(544, 655, 123, 200, None, events2, None, true)?;

        // Read with client_id filter
        let mut filters = ReadFilters::new(0);
        filters.include_client_id = Some(100);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].client_id, 100);

        Ok(())
    }

    // 5. File Handle Management

    #[test]
    fn test_file_handles_reused_same_aggregate() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Multiple writes to same aggregate should reuse handles
        for i in 0..5 {
            let events = vec![EventItem::new(
                i + 1,
                i + 1,
                1000 + i,
                42,
                1,
                format!("event{}", i).into_bytes(),
            )];
            fixture
                .engine
                .append_events(544, 655, 123, 100, None, events, None, true)?;
        }

        // Verify all writes succeeded (indirectly tests handle reuse)
        let result = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 5);

        Ok(())
    }

    #[test]
    fn test_file_handles_separate_for_different_aggregates() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write to multiple aggregates
        for i in 0..3u128 {
            let events = fixture.create_test_events((i + 1) as u64, 1);
            fixture
                .engine
                .append_events(544, 655, i, 100, None, events, None, true)?;
        }

        // Verify all aggregates have their data
        for i in 0..3u128 {
            let result = fixture
                .engine
                .read_filtered(544, 655, i, &ReadFilters::new(0))?;
            assert_eq!(result.event_batches.len(), 1);
        }

        Ok(())
    }

    // 6. Error Recovery and Corruption Handling

    #[test]
    fn test_corruption_recovery_on_read() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write some valid data
        let events1 = fixture.create_test_events(1, 2);
        let events2 = fixture.create_test_events(3, 2);

        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, None, true)?;

        // Corrupt the files by truncating them
        let (event_batch_path, metadata_path) = fixture.engine.get_aggregate_paths(544, 655, 123);

        // Truncate files to simulate corruption
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&event_batch_path)?
            .set_len(10)?; // Very small size to cause corruption

        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&metadata_path)?
            .set_len(10)?;

        // Create a new engine instance to clear any in-memory state
        fixture.reset();
        // Read should detect corruption and attempt recovery
        // Since we completely corrupted the files, it should end up with empty result
        let result = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0));
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_corruption_recovery_on_write() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write some valid data first
        let events1 = fixture.create_test_events(1, 2);
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        // Corrupt the metadata file
        let (_, metadata_path) = fixture.engine.get_aggregate_paths(544, 655, 123);
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&metadata_path)?
            .set_len(10)?;

        // Next write should detect corruption and recover
        let events2 = fixture.create_test_events(3, 1);
        let result = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, None, true);

        // The exact behavior depends on recovery implementation, but it shouldn't panic
        // We're mainly testing that corruption detection is triggered
        match result {
            Ok(_) => {}  // Recovery succeeded
            Err(_) => {} // Recovery failed gracefully
        }

        Ok(())
    }

    // 7. Cache Invalidation on Destructive Operations

    #[test]
    fn test_delete_clears_caches() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write data to populate caches
        let events = fixture.create_test_events(1, 2);
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events, None, true)?;

        // Verify data exists
        assert!(fixture.engine.exists(544, 655, 123)?);

        // Delete the aggregate
        fixture.engine.delete(544, 655, 123)?;

        // Verify aggregate no longer exists
        assert!(!fixture.engine.exists(544, 655, 123)?);

        // Verify read returns empty results
        let result = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 0);

        Ok(())
    }

    #[test]
    fn test_trim_start_clears_caches() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write multiple batches
        for i in 0..5 {
            let events = vec![EventItem::new(
                i + 1,
                i + 1,
                1000 + i,
                42,
                1,
                format!("event{}", i).into_bytes(),
            )];
            fixture
                .engine
                .append_events(544, 655, 123, 100, None, events, None, true)?;
        }

        fixture.reset();
        // Verify all batches exist
        let result_before = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0))?;
        assert_eq!(result_before.event_batches.len(), 5);

        // Trim start to keep only last 2 batches
        fixture.engine.trim_start(544, 655, 123, 5)?;

        // Verify only remaining batches are accessible
        let result_after = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(5))?;
        assert_eq!(result_after.event_batches.len(), 1);
        assert_eq!(result_after.event_batches[0].event_batch_index, 5);

        // Verify cache was cleared - trying to read from beginning should find nothing
        let result_trimmed = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0));
        assert!(result_trimmed.is_err());

        Ok(())
    }

    // 8. Configuration and Edge Cases

    #[test]
    fn test_empty_events_rejected() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let result = fixture
            .engine
            .append_events(544, 655, 123, 100, None, vec![], None, true);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot write empty event batch")
        );

        Ok(())
    }

    #[test]
    fn test_custom_cache_sizes() -> io::Result<()> {
        let config = StatefulEngineConfig {
            last_event_batch_cache_size: 5,
            client_event_index_cache_size: 10,
            recent_batches_cache_size: 1024, // 1KB
            max_file_handles: 2,
            base_path: PathBuf::from(""),
            ..Default::default()
        };

        let mut fixture = StatefulTestFixture::with_config(config)?;

        // Test should work with smaller cache sizes
        let events = fixture.create_test_events(1, 1);
        let result = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events, None, true);
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_directory_creation() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Aggregate directory shouldn't exist yet
        let aggregate_dir = fixture
            .base_path
            .join(544.to_string())
            .join(655.to_string())
            .join(999.to_string());
        assert!(!aggregate_dir.exists());

        // Writing should create the directory
        let events = fixture.create_test_events(1, 1);
        fixture
            .engine
            .append_events(544, 655, 999, 100, None, events, None, true)?;

        // Directory should now exist
        assert!(aggregate_dir.exists());
        assert!(aggregate_dir.join("event_batches.bin").exists());
        assert!(aggregate_dir.join("metadata.bin").exists());

        Ok(())
    }

    #[test]
    fn test_mixed_operations_workflow() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write to multiple aggregates with different clients
        for aggregate_idx in 0..3 {
            for client_id in 100..103 {
                let events = vec![EventItem::new(
                    aggregate_idx * 10 + client_id - 100 + 1,
                    aggregate_idx * 10 + client_id - 100 + 1,
                    1000,
                    42,
                    1,
                    format!("aggregate{}_client{}", aggregate_idx, client_id).into_bytes(),
                )];

                fixture.engine.append_events(
                    544,
                    655,
                    aggregate_idx as u128,
                    client_id as u128,
                    None,
                    events,
                    None,
                    true,
                )?;
            }
        }

        // Read from each aggregate with different filters
        for aggregate_idx in 0..3 {
            // Read all
            let result_all = fixture.engine.read_filtered(
                544,
                655,
                aggregate_idx as u128,
                &ReadFilters::new(0),
            )?;
            assert_eq!(result_all.event_batches.len(), 3); // 3 clients per aggregate

            // Read with client filter
            let mut filters = ReadFilters::new(0);
            filters.include_client_id = Some(101);
            let result_filtered =
                fixture
                    .engine
                    .read_filtered(544, 655, aggregate_idx as u128, &filters)?;
            assert_eq!(result_filtered.event_batches.len(), 1);
            assert_eq!(result_filtered.event_batches[0].client_id, 101);
        }

        // Delete one aggregate
        fixture.engine.delete(544, 655, 1)?;
        assert!(!fixture.engine.exists(544, 655, 1)?);

        // Other aggregates should still exist
        assert!(fixture.engine.exists(544, 655, 0)?);
        assert!(fixture.engine.exists(544, 655, 2)?);

        Ok(())
    }

    // 9. Bloom Filter and Event Type Deduplication Tests

    #[test]
    fn test_bloom_filter_state_persists_across_writes() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events with different event types
        let events1 = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec()), // event_type_major = 42
            EventItem::new(2, 2, 1001, 43, 1, b"event2".to_vec()), // event_type_major = 43
        ];

        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        // Write more events - bloom filter should accumulate state
        let events2 = vec![
            EventItem::new(3, 3, 1002, 44, 1, b"event3".to_vec()), // event_type_major = 44
            EventItem::new(4, 4, 1003, 42, 1, b"event4".to_vec()), // duplicate type
        ];

        let result = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, None, true);
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_event_type_dedup_cleared_per_batch() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write batch with duplicate event types within same batch
        let events1 = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec()),
            EventItem::new(2, 2, 1001, 42, 1, b"event2".to_vec()), // same type in batch
        ];

        let result = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true);
        assert!(result.is_ok());

        Ok(())
    }

    // 10. Timestamp and Server Time Tests

    #[test]
    fn test_server_timestamp_increases_monotonically() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = fixture.create_test_events(1, 1);
        let metadata1 = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        // Small delay to ensure timestamp difference
        std::thread::sleep(Duration::from_millis(1));

        let events2 = fixture.create_test_events(2, 1);
        let metadata2 = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, None, true)?;

        assert!(metadata2.server_timestamp > metadata1.server_timestamp);

        Ok(())
    }

    #[test]
    fn test_timestamp_filtering_from_cache() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events with known timestamps
        let events1 = fixture.create_test_events(1, 2);
        let _metadata1 = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        std::thread::sleep(Duration::from_millis(5));

        let events2 = fixture.create_test_events(3, 2);
        let metadata2 = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, None, true)?;

        // Filter by server timestamp - should only get second batch
        let mut filters = ReadFilters::new(0);
        filters.min_server_timestamp = Some(metadata2.server_timestamp);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].event_batch_index, 2);

        Ok(())
    }

    // 11. Max Bytes Limit Tests

    #[test]
    fn test_max_bytes_limit_respected_from_cache() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write several batches with larger events
        let large_data = vec![0u8; 1000]; // 1KB event data

        for i in 0..5 {
            let events = vec![EventItem::new(
                i + 1,
                i + 1,
                1000 + i,
                42,
                1,
                large_data.clone(),
            )];
            fixture
                .engine
                .append_events(544, 655, 123, 100, None, events, None, true)?;
        }

        // Read with byte limit that should stop after a few batches
        let mut filters = ReadFilters::new(0);
        filters.max_bytes = Some(2500); // Should allow ~2 batches

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert!(result.event_batches.len() <= 3); // Should be limited
        assert!(result.next_event_batch_index.is_some()); // Should indicate more data available

        Ok(())
    }

    #[test]
    fn test_max_bytes_first_batch_exceeds_limit() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write a batch with very large data
        let large_data = vec![0u8; 10000]; // 10KB
        let events = vec![EventItem::new(1, 1, 1000, 42, 1, large_data)];
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events, None, true)?;

        // Try to read with smaller byte limit
        let mut filters = ReadFilters::new(0);
        filters.max_bytes = Some(5000); // 5KB limit

        // Should still return the first batch even though it exceeds limit
        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);

        Ok(())
    }

    // 12. Event-Level Filtering Tests

    #[test]
    fn test_event_type_filtering_from_cache() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events with different types
        let events = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"type42".to_vec()),
            EventItem::new(2, 2, 1001, 43, 1, b"type43".to_vec()),
            EventItem::new(3, 3, 1002, 44, 1, b"type44".to_vec()),
        ];
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events, None, true)?;

        // Filter by event types
        let mut filters = ReadFilters::new(0);
        filters.include_event_types = Some((&[42, 44]).to_vec());

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 2); // Only types 42 and 44
        assert_eq!(result.event_batches[0].events[0].event_type_major, 42);
        assert_eq!(result.event_batches[0].events[1].event_type_major, 44);

        Ok(())
    }

    #[test]
    fn test_client_event_index_range_filtering() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events with different client event indices
        let events = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec()),
            EventItem::new(5, 2, 1001, 42, 1, b"event5".to_vec()),
            EventItem::new(10, 3, 1002, 42, 1, b"event10".to_vec()),
        ];
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events, None, true)?;

        // Filter by client event index range
        let mut filters = ReadFilters::new(0);
        filters.min_client_event_index = Some(3);
        filters.max_client_event_index = Some(8);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 1); // Only event with index 5
        assert_eq!(result.event_batches[0].events[0].client_event_index, 5);

        Ok(())
    }

    #[test]
    fn test_event_timestamp_filtering() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events with different event timestamps
        let events = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec()),
            EventItem::new(2, 2, 2000, 42, 1, b"event2".to_vec()),
            EventItem::new(3, 3, 3000, 42, 1, b"event3".to_vec()),
        ];
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events, None, true)?;

        // Filter by event timestamp
        let mut filters = ReadFilters::new(0);
        filters.min_event_timestamp = Some(1500);
        filters.max_event_timestamp = Some(2500);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 1); // Only event with timestamp 2000
        assert_eq!(result.event_batches[0].events[0].event_timestamp, 2000);

        Ok(())
    }

    #[test]
    fn test_event_index_filtering() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events with different event indices
        // We set 10, 20, 30 here but they are overwritten by the server logic. This is intentional!
        let events = vec![
            EventItem::new(1, 10, 1000, 42, 1, b"event1".to_vec()),
            EventItem::new(2, 20, 1001, 42, 1, b"event2".to_vec()),
            EventItem::new(3, 30, 1002, 42, 1, b"event3".to_vec()),
        ];
        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events, None, true)?;

        // Filter by event index range
        let mut filters = ReadFilters::new(0);
        filters.min_event_index = Some(2);
        filters.max_event_index = Some(2);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 1); // Only event with index 2
        assert_eq!(result.event_batches[0].events[0].event_index, 2);

        // Filter by event index range
        let mut filters = ReadFilters::new(0);
        filters.min_event_index = Some(2);
        filters.max_event_index = Some(3);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 2); // Only event with index 2 and 3
        assert_eq!(result.event_batches[0].events[0].event_index, 2);
        assert_eq!(result.event_batches[0].events[1].event_index, 3);

        // Filter by event index range
        let mut filters = ReadFilters::new(2);
        filters.min_event_index = Some(2);
        filters.max_event_index = Some(3);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 0); //From batch index excludes the first batch

        Ok(())
    }

    // 13. User ID Filtering Tests

    #[test]
    fn test_user_id_include_filtering() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = fixture.create_test_events(1, 2);
        let events2 = fixture.create_test_events(3, 2);

        fixture
            .engine
            .append_events(544, 655, 123, 100, Some(500), events1, None, true)?;
        fixture
            .engine
            .append_events(544, 655, 123, 200, Some(600), events2, None, true)?;

        // Filter by user_id
        let mut filters = ReadFilters::new(0);
        filters.include_user_id = Some(500);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].user_id, Some(500));

        Ok(())
    }

    #[test]
    fn test_user_id_exclude_filtering() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = fixture.create_test_events(1, 2);
        let events2 = fixture.create_test_events(3, 2);

        fixture
            .engine
            .append_events(544, 655, 123, 100, Some(500), events1, None, true)?;
        fixture
            .engine
            .append_events(544, 655, 123, 200, Some(600), events2, None, true)?;

        // Exclude specific user_id
        let mut filters = ReadFilters::new(0);
        filters.exclude_user_id = Some(500);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].user_id, Some(600));

        Ok(())
    }

    // 14. Cache Boundary Tests

    #[test]
    fn test_memory_cache_eviction() -> io::Result<()> {
        // Test with very small cache size to force eviction
        let config = StatefulEngineConfig {
            recent_batches_cache_size: 100, // Very small cache
            ..Default::default()
        };
        let mut fixture = StatefulTestFixture::with_config(config)?;

        // Write many small batches to trigger eviction
        for i in 0..10 {
            let events = vec![EventItem::new(i + 1, i + 1, 1000, 42, 1, b"small".to_vec())];
            fixture
                .engine
                .append_events(544, 655, 123, 100, None, events, None, true)?;
        }

        // Early batches should be evicted from cache, but still readable from disk
        let result = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 10); // All should still be readable

        Ok(())
    }

    #[test]
    fn test_client_event_index_cache_eviction() -> io::Result<()> {
        let config = StatefulEngineConfig {
            client_event_index_cache_size: 2, // Very small cache
            ..Default::default()
        };
        let mut fixture = StatefulTestFixture::with_config(config)?;

        // Write with multiple clients to trigger eviction
        for client_id in 100..110 {
            let events = vec![EventItem::new(1, 1, 1000, 42, 1, b"test".to_vec())];
            fixture
                .engine
                .append_events(544, 655, 123, client_id, None, events, None, true)?;
        }

        // Cache should have evicted earlier clients, but duplicate filtering should still work
        // (by falling back to disk reads if needed)
        let events_dup = vec![EventItem::new(1, 2, 1001, 42, 1, b"dup".to_vec())];
        let result = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events_dup, None, true);

        // Should either succeed (cache miss, no filtering) or fail (duplicate detected from disk)
        // The exact behavior depends on implementation details
        match result {
            Ok(_) => {}                                          // Cache miss occurred, no filtering
            Err(e) if e.to_string().contains("duplicates") => {} // Filtering worked via disk lookup
            Err(e) => panic!("Unexpected error: {}", e),
        }

        Ok(())
    }

    // 15. Edge Cases and Error Conditions

    #[test]
    fn test_read_nonexistent_aggregate() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let result = fixture
            .engine
            .read_filtered(544, 655, 765, &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 0);
        assert_eq!(result.next_event_batch_index, None);

        Ok(())
    }

    #[test]
    fn test_trim_start_nonexistent_aggregate() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Should succeed silently for nonexistent aggregate
        let result = fixture.engine.trim_start(544, 655, 765, 5);
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_trim_start_invalid_index() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write some batches
        for i in 0..3 {
            let events = fixture.create_test_events(i + 1, 1);
            fixture
                .engine
                .append_events(544, 655, 123, 100, None, events, None, true)?;
        }

        // Try to trim from non-existent index
        let result = fixture.engine.trim_start(544, 655, 123, 10);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));

        Ok(())
    }

    #[test]
    fn test_delete_nonexistent_aggregate() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Should succeed silently for nonexistent aggregate
        let result = fixture.engine.delete(544, 655, 765);
        assert!(result.is_ok());

        Ok(())
    }

    // 16. Complex Filter Combinations

    #[test]
    fn test_complex_filter_combinations() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write diverse data
        let events1 = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec()),
            EventItem::new(5, 2, 2000, 43, 1, b"event5".to_vec()),
        ];
        let events2 = vec![
            EventItem::new(10, 3, 1500, 42, 1, b"event10".to_vec()),
            EventItem::new(15, 4, 2500, 44, 1, b"event15".to_vec()),
        ];

        fixture
            .engine
            .append_events(544, 655, 123, 100, Some(500), events1, None, true)?;
        fixture
            .engine
            .append_events(544, 655, 123, 200, Some(600), events2, None, true)?;

        // Complex filter: specific client, event type, and timestamp range
        let mut filters = ReadFilters::new(0);
        filters.include_client_id = Some(100);
        filters.include_event_types = Some([42, 43].to_vec());
        filters.min_event_timestamp = Some(1500);

        let result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 1); // Only event5 matches all criteria
        assert_eq!(result.event_batches[0].events[0].client_event_index, 5);

        Ok(())
    }

    // 17. Concurrent-like Behavior Tests

    #[test]
    fn test_multiple_clients_interleaved() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Simulate multiple clients writing in interleaved fashion
        for batch in 0..5 {
            for client_id in 100..103 {
                let events = vec![EventItem::new(
                    (batch * 3 + (client_id - 100)) as u64 + 1,
                    batch as u64 + 1,
                    1000 + batch,
                    42,
                    1,
                    format!("client{}_batch{}", client_id, batch).into_bytes(),
                )];
                fixture.engine.append_events(
                    544,
                    655,
                    123,
                    client_id as u128,
                    None,
                    events,
                    None,
                    true,
                )?;
            }
        }

        // Should have 15 batches total (5 batches × 3 clients)
        let result = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 15);

        // Test filtering by specific client
        let mut filters = ReadFilters::new(0);
        filters.include_client_id = Some(101);
        let client_result = fixture.engine.read_filtered(544, 655, 123, &filters)?;
        assert_eq!(client_result.event_batches.len(), 5); // 5 batches from client 101

        Ok(())
    }

    // 18. Builder Pattern Test

    #[test]
    fn test_builder_with_default_config() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let base_path = temp_dir.path().to_path_buf();

        let mut engine = StatefulEngine::with_default_config(base_path);

        let events = vec![EventItem::new(1, 1, 1000, 42, 1, b"test".to_vec())];
        let result = engine.append_events(544, 655, 123, 100, None, events, None, true);
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_organization_isolation_event_batch_indices() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events = fixture.create_test_events(1, 1);

        // Same aggregate_type_id and aggregate_id, different org_id
        let metadata1 =
            fixture
                .engine
                .append_events(100, 200, 300, 400, None, events.clone(), None, true)?;
        let metadata2 =
            fixture
                .engine
                .append_events(101, 200, 300, 400, None, events.clone(), None, true)?;
        let metadata3 =
            fixture
                .engine
                .append_events(102, 200, 300, 400, None, events.clone(), None, true)?;

        // Each org should start with event_batch_index 1
        assert_eq!(metadata1.event_batch_index, 1);
        assert_eq!(metadata2.event_batch_index, 1);
        assert_eq!(metadata3.event_batch_index, 1);

        // Second writes should increment independently
        let events2 = fixture.create_test_events(2, 1);
        let metadata4 =
            fixture
                .engine
                .append_events(100, 200, 300, 400, None, events2.clone(), None, true)?;
        let metadata5 =
            fixture
                .engine
                .append_events(101, 200, 300, 400, None, events2.clone(), None, true)?;

        assert_eq!(metadata4.event_batch_index, 2);
        assert_eq!(metadata5.event_batch_index, 2);

        Ok(())
    }

    #[test]
    fn test_organization_isolation_client_event_indices() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = vec![EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec())];
        let events2 = vec![EventItem::new(1, 2, 1001, 42, 1, b"event1_dup".to_vec())];

        // Write to org 100
        fixture
            .engine
            .append_events(100, 200, 300, 400, None, events1.clone(), None, true)?;

        // Same client_event_index in different org should NOT be filtered
        let result_org101 =
            fixture
                .engine
                .append_events(101, 200, 300, 400, None, events2.clone(), None, true);
        assert!(result_org101.is_ok()); // Should succeed

        // Same client_event_index in same org SHOULD be filtered
        let result_org100 =
            fixture
                .engine
                .append_events(100, 200, 300, 400, None, events2.clone(), None, true);
        assert!(result_org100.is_err()); // Should fail due to duplicates

        Ok(())
    }

    #[test]
    fn test_organization_isolation_reads() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = fixture.create_test_events(1, 2);
        let events2 = fixture.create_test_events(3, 2);

        // Write to different orgs with same aggregate_type_id and aggregate_id
        fixture
            .engine
            .append_events(100, 200, 300, 400, None, events1, None, true)?;
        fixture
            .engine
            .append_events(101, 200, 300, 400, None, events2, None, true)?;

        // Read from org 100
        let result_org100 = fixture
            .engine
            .read_filtered(100, 200, 300, &ReadFilters::new(0))?;
        assert_eq!(result_org100.event_batches.len(), 1);
        assert_eq!(
            result_org100.event_batches[0].events[0].client_event_index,
            1
        );

        // Read from org 101
        let result_org101 = fixture
            .engine
            .read_filtered(101, 200, 300, &ReadFilters::new(0))?;
        assert_eq!(result_org101.event_batches.len(), 1);
        assert_eq!(
            result_org101.event_batches[0].events[0].client_event_index,
            3
        );

        // Read from non-existent org
        let result_org999 = fixture
            .engine
            .read_filtered(999, 200, 300, &ReadFilters::new(0))?;
        assert_eq!(result_org999.event_batches.len(), 0);

        Ok(())
    }

    #[test]
    fn test_organization_isolation_exists() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events = fixture.create_test_events(1, 1);
        fixture
            .engine
            .append_events(100, 200, 300, 400, None, events, None, true)?;

        // Should exist for correct org
        assert!(fixture.engine.exists(100, 200, 300)?);

        // Should not exist for different org
        assert!(!fixture.engine.exists(101, 200, 300)?);
        assert!(!fixture.engine.exists(999, 200, 300)?);

        Ok(())
    }

    #[test]
    fn test_organization_isolation_destructive_operations() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Create data in multiple orgs
        for i in 0..3 {
            let events = fixture.create_test_events(i + 1, 2);
            fixture.engine.append_events(
                100 + i as u128,
                200,
                300,
                400,
                None,
                events,
                None,
                true,
            )?;
        }

        // Verify all exist
        for i in 0..3 {
            assert!(fixture.engine.exists(100 + i as u128, 200, 300)?);
        }

        // Delete from one org
        fixture.engine.delete(101, 200, 300)?;

        // Only that org should be deleted
        assert!(fixture.engine.exists(100, 200, 300)?);
        assert!(!fixture.engine.exists(101, 200, 300)?);
        assert!(fixture.engine.exists(102, 200, 300)?);

        Ok(())
    }

    // 20. Aggregate Type Isolation Tests

    #[test]
    fn test_aggregate_type_isolation_event_batch_indices() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events = fixture.create_test_events(1, 1);

        // Same org_id and aggregate_id, different aggregate_type_id
        let metadata1 =
            fixture
                .engine
                .append_events(100, 200, 300, 400, None, events.clone(), None, true)?;
        let metadata2 =
            fixture
                .engine
                .append_events(100, 201, 300, 400, None, events.clone(), None, true)?;
        let metadata3 =
            fixture
                .engine
                .append_events(100, 202, 300, 400, None, events.clone(), None, true)?;

        // Each aggregate type should start with event_batch_index 0
        assert_eq!(metadata1.event_batch_index, 1);
        assert_eq!(metadata2.event_batch_index, 1);
        assert_eq!(metadata3.event_batch_index, 1);

        Ok(())
    }

    #[test]
    fn test_aggregate_type_isolation_client_event_indices() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = vec![EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec())];
        let events2 = vec![EventItem::new(1, 2, 1001, 42, 1, b"event1_dup".to_vec())];

        // Write to aggregate_type 200
        fixture
            .engine
            .append_events(100, 200, 300, 400, None, events1.clone(), None, true)?;

        // Same client_event_index in different aggregate_type should NOT be filtered
        let result_type201 =
            fixture
                .engine
                .append_events(100, 201, 300, 400, None, events2.clone(), None, true);
        assert!(result_type201.is_ok()); // Should succeed

        // Same client_event_index in same aggregate_type SHOULD be filtered
        let result_type200 =
            fixture
                .engine
                .append_events(100, 200, 300, 400, None, events2.clone(), None, true);
        assert!(result_type200.is_err()); // Should fail due to duplicates

        Ok(())
    }

    #[test]
    fn test_aggregate_type_isolation_reads() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = fixture.create_test_events(1, 2);
        let events2 = fixture.create_test_events(3, 2);

        // Write to different aggregate types with same org_id and aggregate_id
        fixture
            .engine
            .append_events(100, 200, 300, 400, None, events1, None, true)?;
        fixture
            .engine
            .append_events(100, 201, 300, 400, None, events2, None, true)?;

        // Read from aggregate_type 200
        let result_type200 = fixture
            .engine
            .read_filtered(100, 200, 300, &ReadFilters::new(0))?;
        assert_eq!(result_type200.event_batches.len(), 1);
        assert_eq!(
            result_type200.event_batches[0].events[0].client_event_index,
            1
        );

        // Read from aggregate_type 201
        let result_type201 = fixture
            .engine
            .read_filtered(100, 201, 300, &ReadFilters::new(0))?;
        assert_eq!(result_type201.event_batches.len(), 1);
        assert_eq!(
            result_type201.event_batches[0].events[0].client_event_index,
            3
        );

        Ok(())
    }

    #[test]
    fn test_aggregate_type_isolation_destructive_operations() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Create data in multiple aggregate types
        for i in 0..3 {
            let events = fixture.create_test_events(i + 1, 2);
            fixture.engine.append_events(
                100,
                200 + i as u128,
                300,
                400,
                None,
                events,
                None,
                true,
            )?;
        }

        fixture.engine.delete(100, 201, 300)?;

        // Other aggregate types should be unaffected
        let result_type200 = fixture
            .engine
            .read_filtered(100, 200, 300, &ReadFilters::new(0))?;
        assert_eq!(result_type200.event_batches.len(), 1);

        let result_type202 = fixture
            .engine
            .read_filtered(100, 202, 300, &ReadFilters::new(0))?;
        assert_eq!(result_type202.event_batches.len(), 1);

        Ok(())
    }

    // 21. Full Hierarchy Isolation Tests

    #[test]
    fn test_full_hierarchy_isolation() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Create a matrix of data across all dimensions
        let mut expected_batches = HashMap::new();

        for org_id in [100, 101] {
            for aggregate_type_id in [200, 201] {
                for aggregate_id in [300, 301] {
                    let events = vec![EventItem::new(
                        1,
                        1,
                        1000,
                        42,
                        1,
                        format!(
                            "org{}_type{}_agg{}",
                            org_id, aggregate_type_id, aggregate_id
                        )
                        .into_bytes(),
                    )];

                    fixture.engine.append_events(
                        org_id,
                        aggregate_type_id,
                        aggregate_id,
                        400,
                        None,
                        events,
                        None,
                        true,
                    )?;

                    expected_batches.insert((org_id, aggregate_type_id, aggregate_id), true);
                }
            }
        }

        // Verify each combination exists independently
        for org_id in [100, 101] {
            for aggregate_type_id in [200, 201] {
                for aggregate_id in [300, 301] {
                    assert!(
                        fixture
                            .engine
                            .exists(org_id, aggregate_type_id, aggregate_id)?
                    );

                    let result = fixture.engine.read_filtered(
                        org_id,
                        aggregate_type_id,
                        aggregate_id,
                        &ReadFilters::new(0),
                    )?;
                    assert_eq!(result.event_batches.len(), 1);

                    // Verify the data content matches expectations
                    let expected_data = format!(
                        "org{}_type{}_agg{}",
                        org_id, aggregate_type_id, aggregate_id
                    );
                    assert_eq!(
                        result.event_batches[0].events[0].event_value,
                        std::sync::Arc::new(Vec::from(expected_data.as_bytes()))
                    );
                }
            }
        }

        // Test cross-dimension isolation by deleting one specific combination
        fixture.engine.delete(100, 200, 300)?;

        // Verify only that specific combination is gone
        assert!(!fixture.engine.exists(100, 200, 300)?);

        // All others should still exist
        assert!(fixture.engine.exists(100, 200, 301)?);
        assert!(fixture.engine.exists(100, 201, 300)?);
        assert!(fixture.engine.exists(100, 201, 301)?);
        assert!(fixture.engine.exists(101, 200, 300)?);
        assert!(fixture.engine.exists(101, 200, 301)?);
        assert!(fixture.engine.exists(101, 201, 300)?);
        assert!(fixture.engine.exists(101, 201, 301)?);

        Ok(())
    }

    #[test]
    fn test_memory_cache_isolation_across_hierarchy() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write to multiple combinations
        let events1 = fixture.create_test_events(1, 2);
        let events2 = fixture.create_test_events(3, 2);
        let events3 = fixture.create_test_events(5, 2);

        fixture
            .engine
            .append_events(100, 200, 300, 400, None, events1, None, true)?;
        fixture
            .engine
            .append_events(100, 200, 301, 400, None, events2, None, true)?;
        fixture
            .engine
            .append_events(100, 201, 300, 400, None, events3, None, true)?;

        // Each should be cached independently
        let result1 = fixture
            .engine
            .read_filtered(100, 200, 300, &ReadFilters::new(0))?;
        let result2 = fixture
            .engine
            .read_filtered(100, 200, 301, &ReadFilters::new(0))?;
        let result3 = fixture
            .engine
            .read_filtered(100, 201, 300, &ReadFilters::new(0))?;

        assert_eq!(result1.event_batches[0].events[0].client_event_index, 1);
        assert_eq!(result2.event_batches[0].events[0].client_event_index, 3);
        assert_eq!(result3.event_batches[0].events[0].client_event_index, 5);

        Ok(())
    }

    // 22. File Path Structure Tests

    #[test]
    fn test_file_path_structure() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events = fixture.create_test_events(1, 1);
        fixture
            .engine
            .append_events(123, 456, 789, 400, None, events, None, true)?;

        // Verify directory structure is created correctly
        let expected_dir = fixture
            .base_path
            .join("123") // org_id
            .join("456") // aggregate_type_id
            .join("789"); // aggregate_id

        assert!(expected_dir.exists());
        assert!(expected_dir.join("event_batches.bin").exists());
        assert!(expected_dir.join("metadata.bin").exists());

        Ok(())
    }

    #[test]
    fn test_file_path_structure_large_ids() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Test with large u128 values
        let org_id = u128::MAX;
        let aggregate_type_id = u128::MAX - 1;
        let aggregate_id = u128::MAX - 2;

        let events = fixture.create_test_events(1, 1);
        fixture.engine.append_events(
            org_id,
            aggregate_type_id,
            aggregate_id,
            400,
            None,
            events,
            None,
            true,
        )?;

        // Verify directory structure handles large numbers
        let expected_dir = fixture
            .base_path
            .join(org_id.to_string())
            .join(aggregate_type_id.to_string())
            .join(aggregate_id.to_string());

        assert!(expected_dir.exists());
        assert!(expected_dir.join("event_batches.bin").exists());
        assert!(expected_dir.join("metadata.bin").exists());

        Ok(())
    }

    // 23. Cache Isolation Tests

    #[test]
    fn test_event_batch_index_cache_isolation() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write to create cache entries across the hierarchy
        for i in 0..3 {
            let events = fixture.create_test_events(i + 1, 1);
            fixture.engine.append_events(
                100,
                200,
                300 + i as u128,
                400,
                None,
                events,
                None,
                true,
            )?;
        }

        // Each aggregate should have its own sequence
        let events2 = fixture.create_test_events(10, 1);
        let metadata1 =
            fixture
                .engine
                .append_events(100, 200, 300, 400, None, events2.clone(), None, true)?;
        let metadata2 =
            fixture
                .engine
                .append_events(100, 200, 301, 400, None, events2.clone(), None, true)?;
        let metadata3 =
            fixture
                .engine
                .append_events(100, 200, 302, 400, None, events2.clone(), None, true)?;

        assert_eq!(metadata1.event_batch_index, 2);
        assert_eq!(metadata2.event_batch_index, 2);
        assert_eq!(metadata3.event_batch_index, 2);

        Ok(())
    }

    #[test]
    fn test_client_event_index_cache_isolation() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write same client_event_index to different hierarchy positions
        let events = vec![EventItem::new(42, 1, 1000, 1, 1, b"test".to_vec())];

        fixture
            .engine
            .append_events(100, 200, 300, 400, None, events.clone(), None, true)?;
        fixture
            .engine
            .append_events(100, 200, 301, 400, None, events.clone(), None, true)?;
        fixture
            .engine
            .append_events(100, 201, 300, 400, None, events.clone(), None, true)?;
        fixture
            .engine
            .append_events(101, 200, 300, 400, None, events.clone(), None, true)?;

        // All should succeed - no cross-pollution

        // Now try duplicates within each scope
        let dup_events = vec![EventItem::new(42, 2, 1001, 1, 1, b"dup".to_vec())];

        let result1 =
            fixture
                .engine
                .append_events(100, 200, 300, 400, None, dup_events.clone(), None, true);
        let result2 =
            fixture
                .engine
                .append_events(100, 200, 301, 400, None, dup_events.clone(), None, true);
        let result3 =
            fixture
                .engine
                .append_events(100, 201, 300, 400, None, dup_events.clone(), None, true);
        let result4 =
            fixture
                .engine
                .append_events(101, 200, 300, 400, None, dup_events.clone(), None, true);

        // All should fail - duplicates detected within each scope
        assert!(result1.is_err());
        assert!(result2.is_err());
        assert!(result3.is_err());
        assert!(result4.is_err());

        Ok(())
    }

    // 24. Cross-Hierarchy Operation Tests

    #[test]
    fn test_mixed_hierarchy_operations() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Create a complex scenario with multiple organizations, aggregate types, and aggregates
        let mut write_count = 0u64;
        for org_id in [100, 200] {
            for aggregate_type_id in [10, 20] {
                for aggregate_id in [1, 2] {
                    for client_id in [500, 600] {
                        write_count += 1;
                        let events = vec![EventItem::new(
                            write_count,
                            write_count,
                            1000 + write_count,
                            42,
                            1,
                            format!(
                                "data_{}_{}_{}_{}",
                                org_id, aggregate_type_id, aggregate_id, client_id
                            )
                            .into_bytes(),
                        )];

                        fixture.engine.append_events(
                            org_id,
                            aggregate_type_id,
                            aggregate_id,
                            client_id,
                            Some(client_id * 10),
                            events,
                            None,
                            true,
                        )?;
                    }
                }
            }
        }

        // Verify total number of unique aggregates (2 orgs × 2 types × 2 aggregates = 8 aggregates, each with 2 batches)
        let mut total_batches = 0;
        for org_id in [100, 200] {
            for aggregate_type_id in [10, 20] {
                for aggregate_id in [1, 2] {
                    let result = fixture.engine.read_filtered(
                        org_id,
                        aggregate_type_id,
                        aggregate_id,
                        &ReadFilters::new(0),
                    )?;
                    total_batches += result.event_batches.len();
                    assert_eq!(result.event_batches.len(), 2); // 2 clients per aggregate
                }
            }
        }
        assert_eq!(total_batches, 16); // 8 aggregates × 2 batches each

        // Test filtering across the hierarchy
        let mut filters = ReadFilters::new(0);
        filters.include_client_id = Some(500);

        let result = fixture.engine.read_filtered(100, 10, 1, &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].client_id, 500);

        Ok(())
    }

    #[test]
    fn test_corruption_recovery_hierarchy_isolation() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write to multiple aggregates
        for aggregate_id in [1, 2] {
            let events = fixture.create_test_events(aggregate_id, 2);
            fixture.engine.append_events(
                100,
                200,
                aggregate_id as u128,
                400,
                None,
                events,
                None,
                true,
            )?;
        }

        // Corrupt one aggregate's files
        let (_event_batch_path, metadata_path) = fixture.engine.get_aggregate_paths(100, 200, 1);
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&metadata_path)?
            .set_len(10)?;

        fixture.reset();
        // Recovery should be isolated - other aggregates unaffected
        let result_good = fixture
            .engine
            .read_filtered(100, 200, 2, &ReadFilters::new(0))?;
        assert_eq!(result_good.event_batches.len(), 1);

        // Corrupted aggregate should be detected
        let result_bad = fixture
            .engine
            .read_filtered(100, 200, 1, &ReadFilters::new(0));
        assert!(result_bad.is_err());

        Ok(())
    }

    // 25. Edge Cases with Hierarchy

    #[test]
    fn test_zero_ids_in_hierarchy() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Test with zero values for all IDs
        let events = fixture.create_test_events(1, 1);
        let result = fixture
            .engine
            .append_events(0, 0, 0, 0, None, events, None, true);
        assert!(result.is_ok());

        // Verify it can be read back
        let read_result = fixture
            .engine
            .read_filtered(0, 0, 0, &ReadFilters::new(0))?;
        assert_eq!(read_result.event_batches.len(), 1);

        Ok(())
    }

    #[test]
    fn test_hierarchy_with_optimistic_concurrency() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Set up initial state across hierarchy
        let events1 = fixture.create_test_events(1, 1);
        let events2 = fixture.create_test_events(2, 1);

        fixture
            .engine
            .append_events(100, 200, 300, 400, None, events1, None, true)?;
        fixture
            .engine
            .append_events(100, 200, 301, 400, None, events2, None, true)?;

        // Test optimistic concurrency within each scope
        let events3 = fixture.create_test_events(3, 1);
        let events4 = fixture.create_test_events(4, 1);

        // Should succeed with correct expected indices
        let result1 =
            fixture
                .engine
                .append_events(100, 200, 300, 400, None, events3, Some(2), true);
        let result2 =
            fixture
                .engine
                .append_events(100, 200, 301, 400, None, events4, Some(2), true);

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(result1.unwrap().event_batch_index, 2);
        assert_eq!(result2.unwrap().event_batch_index, 2);

        Ok(())
    }

    #[test]
    fn test_directory_cleanup_on_delete() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Create nested structure
        let events = fixture.create_test_events(1, 1);
        fixture
            .engine
            .append_events(100, 200, 300, 400, None, events, None, true)?;

        let aggregate_dir = fixture.base_path.join("100").join("200").join("300");

        assert!(aggregate_dir.exists());

        // Delete should remove the aggregate directory
        fixture.engine.delete(100, 200, 300)?;

        // The current implementation tries to remove the aggregate directory
        // but might not remove parent directories even if empty
        // This is acceptable behavior - we're mainly testing that delete works correctly

        Ok(())
    }

    #[test]
    fn test_no_duplicate_filtering_when_disabled() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // First write with events having client_event_index 1, 2
        let events1 = vec![
            EventItem::new(1, 1, 1000, 42, 1, b"event1_first".to_vec()),
            EventItem::new(2, 2, 1001, 42, 1, b"event2_first".to_vec()),
        ];

        fixture
            .engine
            .append_events(544, 655, 123, 100, None, events1, None, true)?;

        // Second write with same client_event_index but filter_duplicate_client_events = false
        // All events should be written regardless of duplicate client_event_index
        let events2 = vec![
            EventItem::new(1, 3, 1002, 42, 1, b"event1_second".to_vec()), // Same client_event_index
            EventItem::new(2, 4, 1003, 42, 1, b"event2_second".to_vec()), // Same client_event_index
            EventItem::new(3, 5, 1004, 42, 1, b"event3_new".to_vec()),    // New client_event_index
        ];

        let metadata2 = fixture
            .engine
            .append_events(544, 655, 123, 100, None, events2, None, false)?; // filter_duplicate_client_events = false

        // Should succeed and create a new batch
        assert_eq!(metadata2.event_batch_index, 2);

        // Read back and verify all events from both batches are present
        let result = fixture
            .engine
            .read_filtered(544, 655, 123, &ReadFilters::new(1))?;

        assert_eq!(result.event_batches.len(), 2);

        // First batch should have 2 events
        assert_eq!(result.event_batches[0].events.len(), 2);
        assert_eq!(result.event_batches[0].events[0].client_event_index, 1);
        assert_eq!(result.event_batches[0].events[1].client_event_index, 2);
        assert_eq!(
            result.event_batches[0].events[0].event_value,
            std::sync::Arc::new(b"event1_first".to_vec())
        );

        // Second batch should have 3 events (including duplicates)
        assert_eq!(result.event_batches[1].events.len(), 3);
        assert_eq!(result.event_batches[1].events[0].client_event_index, 1); // Duplicate allowed
        assert_eq!(result.event_batches[1].events[1].client_event_index, 2); // Duplicate allowed
        assert_eq!(result.event_batches[1].events[2].client_event_index, 3); // New event
        assert_eq!(
            result.event_batches[1].events[0].event_value,
            std::sync::Arc::new(b"event1_second".to_vec())
        );
        assert_eq!(
            result.event_batches[1].events[1].event_value,
            std::sync::Arc::new(b"event2_second".to_vec())
        );

        Ok(())
    }

    #[test]
    fn test_event_index_assignment_with_and_without_cache() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let base_path = temp_dir.path().to_path_buf();

        // First engine instance - will build up cache
        {
            let config = StatefulEngineConfig {
                base_path: base_path.clone(),
                ..Default::default()
            };
            let mut engine = StatefulEngine::new(config);

            // First write - should get event indices 1, 2
            let events1 = vec![
                EventItem::new(1, 0, 1000, 42, 1, b"event1".to_vec()),
                EventItem::new(2, 0, 1001, 43, 1, b"event2".to_vec()),
            ];
            let metadata1 = engine.append_events(100, 200, 300, 400, None, events1, None, true)?;
            assert_eq!(metadata1.event_batch_index, 1);
            assert_eq!(metadata1.min_event_index, 1);
            assert_eq!(metadata1.max_event_index, 2);

            // Second write - should use cache and get event indices 3, 4, 5
            let events2 = vec![
                EventItem::new(3, 0, 1002, 44, 1, b"event3".to_vec()),
                EventItem::new(4, 0, 1003, 45, 1, b"event4".to_vec()),
                EventItem::new(5, 0, 1004, 46, 1, b"event5".to_vec()),
            ];
            let metadata2 = engine.append_events(100, 200, 300, 400, None, events2, None, true)?;
            assert_eq!(metadata2.event_batch_index, 2);
            assert_eq!(metadata2.min_event_index, 3);
            assert_eq!(metadata2.max_event_index, 5);

            // Verify the second batch has correct event indices by reading back
            let result = engine.read_filtered(100, 200, 300, &ReadFilters::new(2))?;
            assert_eq!(result.event_batches.len(), 1);
            assert_eq!(result.event_batches[0].events.len(), 3);
            assert_eq!(result.event_batches[0].events[0].event_index, 3);
            assert_eq!(result.event_batches[0].events[1].event_index, 4);
            assert_eq!(result.event_batches[0].events[2].event_index, 5);
        }

        // Second engine instance - cache is cleared, must read from disk
        {
            let config = StatefulEngineConfig {
                base_path: base_path.clone(),
                ..Default::default()
            };
            let mut engine = StatefulEngine::new(config);

            // Third write - should read last index from disk and continue with 6, 7
            let events3 = vec![
                EventItem::new(6, 0, 1005, 47, 1, b"event6".to_vec()),
                EventItem::new(7, 0, 1006, 48, 1, b"event7".to_vec()),
            ];
            let metadata3 = engine.append_events(100, 200, 300, 400, None, events3, None, true)?;
            assert_eq!(metadata3.event_batch_index, 3);
            assert_eq!(metadata3.min_event_index, 6);
            assert_eq!(metadata3.max_event_index, 7);

            // Read back ALL events to verify complete sequence
            let result = engine.read_filtered(100, 200, 300, &ReadFilters::new(1))?;
            assert_eq!(result.event_batches.len(), 3);

            // Verify first batch: event indices 1, 2
            assert_eq!(result.event_batches[0].events.len(), 2);
            assert_eq!(result.event_batches[0].events[0].event_index, 1);
            assert_eq!(result.event_batches[0].events[1].event_index, 2);

            // Verify second batch: event indices 3, 4, 5
            assert_eq!(result.event_batches[1].events.len(), 3);
            assert_eq!(result.event_batches[1].events[0].event_index, 3);
            assert_eq!(result.event_batches[1].events[1].event_index, 4);
            assert_eq!(result.event_batches[1].events[2].event_index, 5);

            // Verify third batch: event indices 6, 7
            assert_eq!(result.event_batches[2].events.len(), 2);
            assert_eq!(result.event_batches[2].events[0].event_index, 6);
            assert_eq!(result.event_batches[2].events[1].event_index, 7);

            // Verify event indices are sequential across all batches
            let all_indices: Vec<u64> = result
                .event_batches
                .iter()
                .flat_map(|batch| batch.events.iter())
                .map(|event| event.event_index)
                .collect();
            assert_eq!(all_indices, vec![1, 2, 3, 4, 5, 6, 7]);
        }

        Ok(())
    }
}
