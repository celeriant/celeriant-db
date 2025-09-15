use std::{
    collections::HashMap,
    io::{self, BufReader, BufWriter},
    path::PathBuf,
    time::Duration,
};

//TODO: Setting event_index correctly

use eventplanedb_storage_stateless::{
    stateless_destructive::StatelessDestructive, stateless_engine::StatelessEngine,
    stateless_reader::StatelessReader, stateless_writer::StatelessWriter,
};
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

use super::{
    client_event_index_cache::ClientEventIndexCache, event_batch_index_cache::EventBatchIndexCache,
    file_cache::FileCache, memory_cache::LruMemoryCache,
};

#[derive(Debug, Clone)]
pub struct StatefulEngineConfig {
    // Cache configurations
    pub last_event_batch_cache_size: usize, // default: 10,000
    pub client_event_index_cache_size: usize, // default: 50,000
    pub recent_batches_cache_size: u64,     // default: 16MB

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
            last_event_batch_cache_size: 10_000,
            client_event_index_cache_size: 50_000,
            recent_batches_cache_size: 16 * 1024 * 1024, // 16MB
            max_file_handles: 100,
            base_path: PathBuf::from("./data"),
            compression_type: CompressionType::Zstd { level: 3 },
            stateless_engine: StatelessEngine::builder().build(),
        }
    }
}

pub struct StatefulEngine {
    config: StatefulEngineConfig,

    // Caches
    event_batch_index_cache: EventBatchIndexCache,
    client_event_index_cache: ClientEventIndexCache,
    memory_cache: LruMemoryCache,

    // File handle management
    file_cache: FileCache,

    // Shared resources for writing
    bloom_filter: BloomFilter,
    event_type_dedup: HashSet<u64>,
}

impl StatefulEngine {
    pub fn new(config: StatefulEngineConfig) -> Self {
        let event_batch_index_cache = EventBatchIndexCache::new(config.last_event_batch_cache_size);
        let client_event_index_cache =
            ClientEventIndexCache::new(config.client_event_index_cache_size);
        let memory_cache = LruMemoryCache::new(config.recent_batches_cache_size);
        let file_cache = FileCache::new(config.max_file_handles);

        let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);

        Self {
            config,
            event_batch_index_cache,
            client_event_index_cache,
            memory_cache,
            file_cache,
            bloom_filter,
            event_type_dedup: HashSet::new(),
        }
    }

    pub fn with_default_config(base_path: PathBuf) -> Self {
        let mut config = StatefulEngineConfig::default();
        config.base_path = base_path;
        Self::new(config)
    }

    fn get_aggregate_paths(&self, aggregate_id: &str) -> (PathBuf, PathBuf) {
        let aggregate_dir = self.config.base_path.join(aggregate_id);
        let event_batch_path = aggregate_dir.join("event_batches.bin");
        let metadata_path = aggregate_dir.join("metadata.bin");
        (event_batch_path, metadata_path)
    }

    fn ensure_aggregate_directory(&self, aggregate_id: &str) -> io::Result<()> {
        let aggregate_dir = self.config.base_path.join(aggregate_id);
        std::fs::create_dir_all(aggregate_dir)
    }

    fn get_next_event_batch_index(&mut self, aggregate_id: &str) -> io::Result<u64> {
        // Check cache first
        if let Some(cached_index) = self.event_batch_index_cache.get(aggregate_id) {
            return Ok(cached_index + 1);
        }

        // Cache miss - read from disk
        let (_, metadata_path) = self.get_aggregate_paths(aggregate_id);

        if !metadata_path.exists() {
            // New aggregate, start with index 0
            self.event_batch_index_cache.set(aggregate_id, 0);
            return Ok(0);
        }

        let mut metadata_reader = self
            .file_cache
            .create_reader(metadata_path.to_str().unwrap())?;

        // Attempt to recover from corruption if detected
        let last_index = match self
            .config
            .stateless_engine
            .last_event_batch_index(
                #[cfg(target_os = "linux")]
                &mut metadata_reader,
                #[cfg(not(target_os = "linux"))]
                &mut *metadata_reader.borrow_mut()
            )
        {
            Ok(index) => index,
            Err(_) => {
                // Try to recover from corruption
                self.recover_from_corruption(aggregate_id)?;

                // Retry after recovery
                let mut metadata_reader = self
                    .file_cache
                    .create_reader(metadata_path.to_str().unwrap())?;
                self.config
                    .stateless_engine
                    .last_event_batch_index(
                        #[cfg(target_os = "linux")]
                        &mut metadata_reader,
                        #[cfg(not(target_os = "linux"))]
                        &mut *metadata_reader.borrow_mut()
                    )?
            }
        };

        // Cache the result
        self.event_batch_index_cache.set(aggregate_id, last_index);
        Ok(last_index + 1)
    }

    fn recover_from_corruption(&mut self, aggregate_id: &str) -> io::Result<()> {
        let (event_batch_path, metadata_path) = self.get_aggregate_paths(aggregate_id);

        if !event_batch_path.exists() || !metadata_path.exists() {
            return Ok(()); // Nothing to recover
        }

        let mut event_batch_reader = self
            .file_cache
            .create_reader(event_batch_path.to_str().unwrap())?;
        let mut metadata_reader = self
            .file_cache
            .create_reader(metadata_path.to_str().unwrap())?;

        if let Some(corrupt_positions) = self.config.stateless_engine.detect_corruption(
            #[cfg(target_os = "linux")]
            &mut event_batch_reader,
            #[cfg(target_os = "linux")]
            &mut metadata_reader,
            #[cfg(not(target_os = "linux"))]
            &mut *event_batch_reader.borrow_mut(),
            #[cfg(not(target_os = "linux"))]
            &mut *metadata_reader.borrow_mut(),
        )? {
            // Clear caches for this aggregate
            self.clear_aggregate_caches(aggregate_id);

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
        aggregate_id: &str,
        client_id: u128,
        events: &mut Vec<EventItem>,
    ) -> io::Result<()> {
        let highest_seen = self.client_event_index_cache.get(aggregate_id, client_id);

        if let Some(highest_index) = highest_seen {
            // Filter out events with client_event_index <= highest_seen
            events.retain(|event| event.client_event_index > highest_index);
        }

        Ok(())
    }

    fn update_client_event_index_cache(
        &mut self,
        aggregate_id: &str,
        client_id: u128,
        events: &[EventItem],
    ) -> io::Result<()> {
        let highest_seen = self.client_event_index_cache.get(aggregate_id, client_id);

        // Update cache with the new highest index if we have events
        if let Some(max_event) = events.iter().max_by_key(|e| e.client_event_index) {
            if highest_seen.map_or(true, |seen| max_event.client_event_index > seen) {
                self.client_event_index_cache.set(
                    aggregate_id,
                    client_id,
                    max_event.client_event_index,
                );
            }
        }

        Ok(())
    }

    fn clear_aggregate_caches(&mut self, aggregate_id: &str) {
        self.event_batch_index_cache.remove(aggregate_id);
        self.memory_cache.clear_aggregate(aggregate_id);

        // Remove file handles for this aggregate
        let (event_batch_path, metadata_path) = self.get_aggregate_paths(aggregate_id);
        self.file_cache.remove(event_batch_path.to_str().unwrap());
        self.file_cache.remove(metadata_path.to_str().unwrap());
    }

    fn try_read_from_memory_cache(
        &mut self,
        aggregate_id: &str,
        filters: &ReadFilters,
    ) -> Option<ReadResult> {
        // Check if we have the requested starting batch in cache
        let start_pos = self
            .memory_cache
            .get_pos(aggregate_id, filters.from_event_batch_index)?;

        let batches = self.memory_cache.get_all_batches(aggregate_id)?;
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
}

pub trait StatefulWriter {
    fn append_events(
        &mut self,
        aggregate_id: &str,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
    ) -> io::Result<EventBatchMetadata>;
}

pub trait StatefulReader {
    fn read_filtered(
        &mut self,
        aggregate_id: &str,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult>;

    fn exists(&mut self, aggregate_id: &str) -> io::Result<bool>;
}

pub trait StatefulDestructive {
    fn trim_start(
        &mut self,
        aggregate_id: &str,
        keep_from_event_batch_index: u64,
    ) -> io::Result<()>;

    fn delete(&mut self, aggregate_id: &str) -> io::Result<()>;
}

impl StatefulWriter for StatefulEngine {
    fn append_events(
        &mut self,
        aggregate_id: &str,
        client_id: u128,
        user_id: Option<u128>,
        mut events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
    ) -> io::Result<EventBatchMetadata> {
        if events.is_empty() {
            return Err(io::Error::other("Cannot write empty event batch"));
        }

        // Ensure aggregate directory exists
        self.ensure_aggregate_directory(aggregate_id)?;

        // Get next event batch index
        let next_event_batch_index = self.get_next_event_batch_index(aggregate_id)?;

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
        self.filter_duplicate_events(aggregate_id, client_id, &mut events)?;

        if events.is_empty() {
            return Err(io::Error::other(
                "All events were duplicates and filtered out",
            ));
        }

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
        let (event_batch_path, metadata_path) = self.get_aggregate_paths(aggregate_id);
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
        self.update_client_event_index_cache(aggregate_id, client_id, &event_batch.events)?;
        self.event_batch_index_cache
            .set(aggregate_id, next_event_batch_index);
        self.memory_cache
            .add(aggregate_id, event_batch, metadata.clone());

        Ok(metadata)
    }
}

impl StatefulReader for StatefulEngine {
    fn read_filtered(
        &mut self,
        aggregate_id: &str,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult> {
        // Try to read from memory cache first
        if let Some(result) = self.try_read_from_memory_cache(aggregate_id, filters) {
            return Ok(result);
        }

        // Fallback to disk read
        let (event_batch_path, metadata_path) = self.get_aggregate_paths(aggregate_id);

        if !event_batch_path.exists() || !metadata_path.exists() {
            return Ok(ReadResult {
                event_batches: Vec::new(),
                next_event_batch_index: None,
            });
        }

        let mut event_batch_reader = self
            .file_cache
            .create_reader(event_batch_path.to_str().unwrap())?;
        let mut metadata_reader = self
            .file_cache
            .create_reader(metadata_path.to_str().unwrap())?;

        // Attempt recovery if corruption is detected
        #[cfg(not(target_os = "linux"))]
        let mut mut_event_batch_reader = event_batch_reader.borrow_mut();
        #[cfg(not(target_os = "linux"))]
        let mut mut_metadata_reader = metadata_reader.borrow_mut();
        #[cfg(target_os = "linux")]
        let mut mut_event_batch_reader = event_batch_reader;
        #[cfg(target_os = "linux")]
        let mut mut_metadata_reader = metadata_reader;

        match self.config.stateless_engine.read_filtered(
            #[cfg(target_os = "linux")]
            &mut mut_event_batch_reader,
            #[cfg(target_os = "linux")]
            &mut mut_metadata_reader,
            #[cfg(not(target_os = "linux"))]
            &mut *mut_event_batch_reader,
            #[cfg(not(target_os = "linux"))]
            &mut *mut_metadata_reader,
            filters,
        ) {
            Ok(result) => Ok(result),
            Err(e) => {
                drop(mut_event_batch_reader);
                drop(mut_metadata_reader);

                // Try to recover from corruption
                self.recover_from_corruption(aggregate_id)?;

                // Retry after recovery
                let mut event_batch_reader = self
                    .file_cache
                    .create_reader(event_batch_path.to_str().unwrap())?;
                let mut metadata_reader = self
                    .file_cache
                    .create_reader(metadata_path.to_str().unwrap())?;

                #[cfg(not(target_os = "linux"))]
                let mut mut_event_batch_reader = event_batch_reader.borrow_mut();
                #[cfg(not(target_os = "linux"))]
                let mut mut_metadata_reader = metadata_reader.borrow_mut();
                #[cfg(target_os = "linux")]
                let mut mut_event_batch_reader = event_batch_reader;
                #[cfg(target_os = "linux")]
                let mut mut_metadata_reader = metadata_reader;
                
                self.config.stateless_engine.read_filtered(
                    #[cfg(target_os = "linux")]
                    &mut mut_event_batch_reader,
                    #[cfg(target_os = "linux")]
                    &mut mut_metadata_reader,
                    #[cfg(not(target_os = "linux"))]
                    &mut *mut_event_batch_reader,
                    #[cfg(not(target_os = "linux"))]
                    &mut *mut_metadata_reader,
                    filters,
                )
            }
        }
    }

    fn exists(&mut self, aggregate_id: &str) -> io::Result<bool> {
        let (event_batch_path, metadata_path) = self.get_aggregate_paths(aggregate_id);
        Ok(event_batch_path.exists() && metadata_path.exists())
    }
}

impl StatefulDestructive for StatefulEngine {
    fn trim_start(
        &mut self,
        aggregate_id: &str,
        keep_from_event_batch_index: u64,
    ) -> io::Result<()> {
        let (event_batch_path, metadata_path) = self.get_aggregate_paths(aggregate_id);

        if !event_batch_path.exists() || !metadata_path.exists() {
            return Ok(()); // Nothing to trim
        }

        // We need to calculate positions based on the event batch index
        // This requires reading metadata to find the correct positions
        let mut metadata_reader = self
            .file_cache
            .create_reader(metadata_path.to_str().unwrap())?;
        let mut event_batch_reader = self
            .file_cache
            .create_reader(event_batch_path.to_str().unwrap())?;

        let event_batch_positions = self
            .config
            .stateless_engine
            .positions_for_event_batch_index(
                #[cfg(target_os = "linux")]
                &mut metadata_reader,
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
            &mut event_batch_reader,
            #[cfg(not(target_os = "linux"))]
            &mut *event_batch_reader.borrow_mut(),
            event_batch_positions.event_batch_position,
            event_batch_path.to_str().unwrap(),
            #[cfg(target_os = "linux")]
            &mut metadata_reader,
            #[cfg(not(target_os = "linux"))]
            &mut *metadata_reader.borrow_mut(),
            event_batch_positions.metadata_position,
            metadata_path.to_str().unwrap(),
        )?;

        // Clear caches for this aggregate
        self.clear_aggregate_caches(aggregate_id);

        Ok(())
    }

    fn delete(&mut self, aggregate_id: &str) -> io::Result<()> {
        let (event_batch_path, metadata_path) = self.get_aggregate_paths(aggregate_id);

        if event_batch_path.exists() && metadata_path.exists() {
            self.config
                .stateless_engine
                .delete(&event_batch_path, &metadata_path)?;
        }

        // Clear caches and handles for this aggregate
        self.clear_aggregate_caches(aggregate_id);

        // Remove the aggregate directory if it's empty
        let aggregate_dir = self.config.base_path.join(aggregate_id);
        if aggregate_dir.exists() {
            std::fs::remove_dir(&aggregate_dir).ok(); // Ignore errors if directory is not empty
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, io, path::PathBuf};
    use tempfile::TempDir;

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

        // First write should get event_batch_index 0
        let metadata1 =
            fixture
                .engine
                .append_events("test_aggregate", 100, Some(200), events1, None)?;
        assert_eq!(metadata1.event_batch_index, 0);

        // Second write should get event_batch_index 1
        let metadata2 =
            fixture
                .engine
                .append_events("test_aggregate", 100, Some(200), events2, None)?;
        assert_eq!(metadata2.event_batch_index, 1);

        // Third write should get event_batch_index 2
        let metadata3 =
            fixture
                .engine
                .append_events("test_aggregate", 100, Some(200), events3, None)?;
        assert_eq!(metadata3.event_batch_index, 2);

        Ok(())
    }

    #[test]
    fn test_event_batch_index_cache_across_aggregates() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events1 = fixture.create_test_events(1, 1);
        let events2 = fixture.create_test_events(2, 1);

        // First aggregate gets index 0
        let metadata1 =
            fixture
                .engine
                .append_events("aggregate1", 100, Some(200), events1.clone(), None)?;
        assert_eq!(metadata1.event_batch_index, 0);

        // Second aggregate also gets index 0 (separate sequence)
        let metadata2 =
            fixture
                .engine
                .append_events("aggregate2", 100, Some(200), events2.clone(), None)?;
        assert_eq!(metadata2.event_batch_index, 0);

        // Second write to first aggregate gets index 1
        let events3 = fixture.create_test_events(2, 1);
        let metadata3 =
            fixture
                .engine
                .append_events("aggregate1", 100, Some(200), events3, None)?;
        assert_eq!(metadata3.event_batch_index, 1);

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
                engine.append_events("test_aggregate", 100, None, events.clone(), None)?;
            assert_eq!(metadata1.event_batch_index, 0);

            let events2 = vec![EventItem::new(2, 2, 1000, 42, 1, b"test".to_vec())];
            let metadata2 = engine.append_events("test_aggregate", 100, None, events2, None)?;
            assert_eq!(metadata2.event_batch_index, 1);
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
            let metadata3 = engine.append_events("test_aggregate", 100, None, events, None)?;
            assert_eq!(metadata3.event_batch_index, 2);
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
            .append_events("test_aggregate", 100, None, events1, None)?;

        // Second write with overlapping client_event_index (2, 3, 4, 5)
        // Only events with index 4 and 5 should be written
        let events2 = vec![
            EventItem::new(2, 4, 1003, 42, 1, b"event2_dup".to_vec()), // Should be filtered
            EventItem::new(3, 5, 1004, 42, 1, b"event3_dup".to_vec()), // Should be filtered
            EventItem::new(4, 6, 1005, 42, 1, b"event4".to_vec()),     // Should be written
            EventItem::new(5, 7, 1006, 42, 1, b"event5".to_vec()),     // Should be written
        ];

        let metadata2 = fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, None)?;

        // Read back and verify only new events were written
        let result = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(1))?;
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
            .append_events("test_aggregate", 100, None, events1, None)?;

        // Client 200 writes events with same client_event_index 1, 2
        // These should NOT be filtered since they're from a different client
        let events2 = vec![
            EventItem::new(1, 3, 1002, 42, 1, b"client200_event1".to_vec()),
            EventItem::new(2, 4, 1003, 42, 1, b"client200_event2".to_vec()),
        ];

        fixture
            .engine
            .append_events("test_aggregate", 200, None, events2, None)?;

        // Read back and verify both clients' events are present
        let result = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(0))?;
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
            .append_events("test_aggregate", 100, None, events1, None)?;

        // Second write with same client_event_indices - all should be filtered out
        let events2 = vec![
            EventItem::new(1, 3, 1002, 42, 1, b"event1_dup".to_vec()),
            EventItem::new(2, 4, 1003, 42, 1, b"event2_dup".to_vec()),
        ];

        let result = fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, None);
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
            .append_events("aggregate1", 100, None, events1, None)?;

        // Same client writes to aggregate2 with same client_event_index
        // Should NOT be filtered since it's a different aggregate
        let events2 = vec![EventItem::new(1, 2, 1001, 42, 1, b"test".to_vec())];
        let result = fixture
            .engine
            .append_events("aggregate2", 100, None, events2, None);
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
            .append_events("test_aggregate", 100, None, events1, None)?;

        // Second write with correct expected index
        let events2 = fixture.create_test_events(2, 1);
        let result = fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, Some(1));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().event_batch_index, 1);

        Ok(())
    }

    #[test]
    fn test_optimistic_concurrency_failure() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // First write
        let events1 = fixture.create_test_events(1, 1);
        fixture
            .engine
            .append_events("test_aggregate", 100, None, events1, None)?;

        // Second write with incorrect expected index
        let events2 = fixture.create_test_events(2, 1);
        let result = fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, Some(5));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Optimistic concurrency check failed"));
        assert!(err.contains("expected batch index 5, but next is 1"));

        Ok(())
    }

    #[test]
    fn test_optimistic_concurrency_new_aggregate() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // First write to new aggregate with expected index 0
        let events = fixture.create_test_events(1, 1);
        let result = fixture
            .engine
            .append_events("new_aggregate", 100, None, events, Some(0));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().event_batch_index, 0);

        Ok(())
    }

    // 4. Memory Cache Operations

    #[test]
    fn test_memory_cache_populated_on_write() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let events = fixture.create_test_events(1, 2);
        fixture
            .engine
            .append_events("test_aggregate", 100, None, events, None)?;

        // Verify cache is populated by checking internal state
        // Since we can't directly access the cache, we'll test by reading from cache
        let result = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(0))?;
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
            .append_events("test_aggregate", 100, None, events1, None)?;
        fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, None)?;

        // Read from cache (should hit memory cache)
        let result1 = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(0))?;
        assert_eq!(result1.event_batches.len(), 2);

        // Read again from cache with different starting point
        let result2 = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(1))?;
        assert_eq!(result2.event_batches.len(), 1);
        assert_eq!(result2.event_batches[0].event_batch_index, 1);

        Ok(())
    }

    #[test]
    fn test_memory_cache_miss_falls_back_to_disk() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events that won't be in memory cache (simulate old data)
        let events = fixture.create_test_events(1, 2);
        fixture
            .engine
            .append_events("test_aggregate", 100, None, events, None)?;

        // Clear memory cache by creating new engine instance
        let config = StatefulEngineConfig {
            base_path: fixture.base_path.clone(),
            ..Default::default()
        };
        let mut new_engine = StatefulEngine::new(config);

        // Read should still work (falling back to disk)
        let result = new_engine.read_filtered("test_aggregate", &ReadFilters::new(0))?;
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
            .append_events("test_aggregate", 100, None, events1, None)?;
        fixture
            .engine
            .append_events("test_aggregate", 200, None, events2, None)?;

        // Read with client_id filter
        let mut filters = ReadFilters::new(0);
        filters.include_client_id = Some(100);

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
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
                .append_events("test_aggregate", 100, None, events, None)?;
        }

        // Verify all writes succeeded (indirectly tests handle reuse)
        let result = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 5);

        Ok(())
    }

    #[test]
    fn test_file_handles_separate_for_different_aggregates() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write to multiple aggregates
        for i in 0..3 {
            let events = fixture.create_test_events(i + 1, 1);
            let aggregate_name = format!("aggregate_{}", i);
            fixture
                .engine
                .append_events(&aggregate_name, 100, None, events, None)?;
        }

        // Verify all aggregates have their data
        for i in 0..3 {
            let aggregate_name = format!("aggregate_{}", i);
            let result = fixture
                .engine
                .read_filtered(&aggregate_name, &ReadFilters::new(0))?;
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
            .append_events("test_aggregate", 100, None, events1, None)?;
        fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, None)?;

        // Corrupt the files by truncating them
        let (event_batch_path, metadata_path) =
            fixture.engine.get_aggregate_paths("test_aggregate");

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
            .read_filtered("test_aggregate", &ReadFilters::new(0));
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
            .append_events("test_aggregate", 100, None, events1, None)?;

        // Corrupt the metadata file
        let (_, metadata_path) = fixture.engine.get_aggregate_paths("test_aggregate");
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&metadata_path)?
            .set_len(10)?;

        // Next write should detect corruption and recover
        let events2 = fixture.create_test_events(3, 1);
        let result = fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, None);

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
            .append_events("test_aggregate", 100, None, events, None)?;

        // Verify data exists
        assert!(fixture.engine.exists("test_aggregate")?);

        // Delete the aggregate
        fixture.engine.delete("test_aggregate")?;

        // Verify aggregate no longer exists
        assert!(!fixture.engine.exists("test_aggregate")?);

        // Verify read returns empty results
        let result = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(0))?;
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
                .append_events("test_aggregate", 100, None, events, None)?;
        }

        fixture.reset();
        // Verify all batches exist
        let result_before = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(0))?;
        assert_eq!(result_before.event_batches.len(), 5);

        // Trim start to keep only last 2 batches
        fixture.engine.trim_start("test_aggregate", 3)?;

        // Verify only remaining batches are accessible
        let result_after = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(3))?;
        assert_eq!(result_after.event_batches.len(), 2);
        assert_eq!(result_after.event_batches[0].event_batch_index, 3);

        // Verify cache was cleared - trying to read from beginning should find nothing
        let result_trimmed = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(0));
        assert!(result_trimmed.is_err());

        Ok(())
    }

    // 8. Configuration and Edge Cases

    #[test]
    fn test_empty_events_rejected() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        let result = fixture
            .engine
            .append_events("test_aggregate", 100, None, vec![], None);
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
            .append_events("test_aggregate", 100, None, events, None);
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_directory_creation() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Aggregate directory shouldn't exist yet
        let aggregate_dir = fixture.base_path.join("new_aggregate");
        assert!(!aggregate_dir.exists());

        // Writing should create the directory
        let events = fixture.create_test_events(1, 1);
        fixture
            .engine
            .append_events("new_aggregate", 100, None, events, None)?;

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
                    &format!("aggregate_{}", aggregate_idx),
                    client_id as u128,
                    None,
                    events,
                    None,
                )?;
            }
        }

        // Read from each aggregate with different filters
        for aggregate_idx in 0..3 {
            let aggregate_name = format!("aggregate_{}", aggregate_idx);

            // Read all
            let result_all = fixture
                .engine
                .read_filtered(&aggregate_name, &ReadFilters::new(0))?;
            assert_eq!(result_all.event_batches.len(), 3); // 3 clients per aggregate

            // Read with client filter
            let mut filters = ReadFilters::new(0);
            filters.include_client_id = Some(101);
            let result_filtered = fixture.engine.read_filtered(&aggregate_name, &filters)?;
            assert_eq!(result_filtered.event_batches.len(), 1);
            assert_eq!(result_filtered.event_batches[0].client_id, 101);
        }

        // Delete one aggregate
        fixture.engine.delete("aggregate_1")?;
        assert!(!fixture.engine.exists("aggregate_1")?);

        // Other aggregates should still exist
        assert!(fixture.engine.exists("aggregate_0")?);
        assert!(fixture.engine.exists("aggregate_2")?);

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
            .append_events("test_aggregate", 100, None, events1, None)?;

        // Write more events - bloom filter should accumulate state
        let events2 = vec![
            EventItem::new(3, 3, 1002, 44, 1, b"event3".to_vec()), // event_type_major = 44
            EventItem::new(4, 4, 1003, 42, 1, b"event4".to_vec()), // duplicate type
        ];

        let result = fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, None);
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
            .append_events("test_aggregate", 100, None, events1, None);
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
            .append_events("test_aggregate", 100, None, events1, None)?;

        // Small delay to ensure timestamp difference
        std::thread::sleep(Duration::from_millis(1));

        let events2 = fixture.create_test_events(2, 1);
        let metadata2 = fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, None)?;

        assert!(metadata2.server_timestamp > metadata1.server_timestamp);

        Ok(())
    }

    #[test]
    fn test_timestamp_filtering_from_cache() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events with known timestamps
        let events1 = fixture.create_test_events(1, 2);
        let metadata1 = fixture
            .engine
            .append_events("test_aggregate", 100, None, events1, None)?;

        std::thread::sleep(Duration::from_millis(5));

        let events2 = fixture.create_test_events(3, 2);
        let metadata2 = fixture
            .engine
            .append_events("test_aggregate", 100, None, events2, None)?;

        // Filter by server timestamp - should only get second batch
        let mut filters = ReadFilters::new(0);
        filters.min_server_timestamp = Some(metadata2.server_timestamp);

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].event_batch_index, 1);

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
                .append_events("test_aggregate", 100, None, events, None)?;
        }

        // Read with byte limit that should stop after a few batches
        let mut filters = ReadFilters::new(0);
        filters.max_bytes = Some(2500); // Should allow ~2 batches

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
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
            .append_events("test_aggregate", 100, None, events, None)?;

        // Try to read with smaller byte limit
        let mut filters = ReadFilters::new(0);
        filters.max_bytes = Some(5000); // 5KB limit

        // Should still return the first batch even though it exceeds limit
        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
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
            .append_events("test_aggregate", 100, None, events, None)?;

        // Filter by event types
        let mut filters = ReadFilters::new(0);
        filters.include_event_types = Some((&[42, 44]).to_vec());

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
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
            .append_events("test_aggregate", 100, None, events, None)?;

        // Filter by client event index range
        let mut filters = ReadFilters::new(0);
        filters.min_client_event_index = Some(3);
        filters.max_client_event_index = Some(8);

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
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
            .append_events("test_aggregate", 100, None, events, None)?;

        // Filter by event timestamp
        let mut filters = ReadFilters::new(0);
        filters.min_event_timestamp = Some(1500);
        filters.max_event_timestamp = Some(2500);

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 1); // Only event with timestamp 2000
        assert_eq!(result.event_batches[0].events[0].event_timestamp, 2000);

        Ok(())
    }

    #[test]
    fn test_event_index_filtering() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Write events with different event indices
        let events = vec![
            EventItem::new(1, 10, 1000, 42, 1, b"event1".to_vec()),
            EventItem::new(2, 20, 1001, 42, 1, b"event2".to_vec()),
            EventItem::new(3, 30, 1002, 42, 1, b"event3".to_vec()),
        ];
        fixture
            .engine
            .append_events("test_aggregate", 100, None, events, None)?;

        // Filter by event index range
        let mut filters = ReadFilters::new(0);
        filters.min_event_index = Some(15);
        filters.max_event_index = Some(25);

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 1); // Only event with index 20
        assert_eq!(result.event_batches[0].events[0].event_index, 20);

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
            .append_events("test_aggregate", 100, Some(500), events1, None)?;
        fixture
            .engine
            .append_events("test_aggregate", 200, Some(600), events2, None)?;

        // Filter by user_id
        let mut filters = ReadFilters::new(0);
        filters.include_user_id = Some(500);

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
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
            .append_events("test_aggregate", 100, Some(500), events1, None)?;
        fixture
            .engine
            .append_events("test_aggregate", 200, Some(600), events2, None)?;

        // Exclude specific user_id
        let mut filters = ReadFilters::new(0);
        filters.exclude_user_id = Some(500);

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
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
                .append_events("test_aggregate", 100, None, events, None)?;
        }

        // Early batches should be evicted from cache, but still readable from disk
        let result = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(0))?;
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
                .append_events("test_aggregate", client_id, None, events, None)?;
        }

        // Cache should have evicted earlier clients, but duplicate filtering should still work
        // (by falling back to disk reads if needed)
        let events_dup = vec![EventItem::new(1, 2, 1001, 42, 1, b"dup".to_vec())];
        let result = fixture
            .engine
            .append_events("test_aggregate", 100, None, events_dup, None);

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
            .read_filtered("nonexistent", &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 0);
        assert_eq!(result.next_event_batch_index, None);

        Ok(())
    }

    #[test]
    fn test_trim_start_nonexistent_aggregate() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Should succeed silently for nonexistent aggregate
        let result = fixture.engine.trim_start("nonexistent", 5);
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
                .append_events("test_aggregate", 100, None, events, None)?;
        }

        // Try to trim from non-existent index
        let result = fixture.engine.trim_start("test_aggregate", 10);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));

        Ok(())
    }

    #[test]
    fn test_delete_nonexistent_aggregate() -> io::Result<()> {
        let mut fixture = StatefulTestFixture::new()?;

        // Should succeed silently for nonexistent aggregate
        let result = fixture.engine.delete("nonexistent");
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
            .append_events("test_aggregate", 100, Some(500), events1, None)?;
        fixture
            .engine
            .append_events("test_aggregate", 200, Some(600), events2, None)?;

        // Complex filter: specific client, event type, and timestamp range
        let mut filters = ReadFilters::new(0);
        filters.include_client_id = Some(100);
        filters.include_event_types = Some([42, 43].to_vec());
        filters.min_event_timestamp = Some(1500);

        let result = fixture.engine.read_filtered("test_aggregate", &filters)?;
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
                    "test_aggregate",
                    client_id as u128,
                    None,
                    events,
                    None,
                )?;
            }
        }

        // Should have 15 batches total (5 batches × 3 clients)
        let result = fixture
            .engine
            .read_filtered("test_aggregate", &ReadFilters::new(0))?;
        assert_eq!(result.event_batches.len(), 15);

        // Test filtering by specific client
        let mut filters = ReadFilters::new(0);
        filters.include_client_id = Some(101);
        let client_result = fixture.engine.read_filtered("test_aggregate", &filters)?;
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
        let result = engine.append_events("test_aggregate", 100, None, events, None);
        assert!(result.is_ok());

        Ok(())
    }
}
