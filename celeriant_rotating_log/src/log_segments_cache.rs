use celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES;
use lru::LruCache;
use std::{cell::{Cell, RefCell}, num::NonZeroUsize, path::PathBuf, rc::Rc};

use crate::{errors::{open_or_create_error::OpenOrCreateError, ready_up_error::ReadyUpError}, log_segment_file::{log_segment_cursor::LogSegmentCursor, log_segment_file::LogSegmentFile}};

/// Manages DmaFile handles for a shard with LRU caching.
/// "Active File" file is the current log being written to
/// Older logs are opened on-demand and cached with LRU eviction
pub struct LogSegmentsCache {
    /// The file we are currently appending to. It doesn't get added
    /// to the LRU cache.
    active_file: RefCell<Rc<LogSegmentFile>>,

    pub force_immediate: Cell<bool>,

    /// Cache of every open dma file other than the active log file.
    lru_cache: RefCell<LruCache<u64, Rc<LogSegmentFile>>>,

    /// Required as we lazy-load log files on demand in get()
    shard_dir: PathBuf,

    pub preallocate_bytes: u64,
}

impl LogSegmentsCache {

    /// Rollback write position after failed replication (read -> write)
    /// This is done here because it's possible the write is on a new rotated log file
    /// and read is still at the previous log file.
    pub fn rollback_write_position(&self) {
        let active_log_id = self.active_log_id();
        let prev_log_id = active_log_id.saturating_sub(1);

        // Check if read position is on active file
        let read_on_active = {
            let active_file = self.active_file.borrow();
            active_file.metadata.borrow().read.is_some()
        };

        if read_on_active {
            let active_file = self.active_file.borrow();
            let mut metadata = active_file.metadata.borrow_mut();
            metadata.write = metadata.read.clone().unwrap();
            return;
        }

        // Read position is still on a previous log file
        // Get cursor from previous file: prefer read, fallback to write
        let prev_cursor = {
            self.lru_cache.borrow_mut()
                .get(&prev_log_id)
                .map(|prev_file| {
                    let metadata = prev_file.metadata.borrow();
                    metadata.read.clone().unwrap_or_else(|| metadata.write.clone())
                })
        };

        // Reset active file - use previous cursor state if available, otherwise fresh
        {
            let active_file = self.active_file.borrow();
            let mut metadata = active_file.metadata.borrow_mut();
            
            metadata.write = LogSegmentCursor {
                log_id: metadata.log_id,
                metablocks_position: HEADER_BLOCK_SIZE_BYTES as u64,
                datablocks_position: metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64),
                wal_index: prev_cursor.as_ref().map_or(0, |c| c.wal_index),
                aggregate_key_bloom: prev_cursor.as_ref().map_or_else(Default::default, |c| c.aggregate_key_bloom.clone()),
                tip_hash: prev_cursor.as_ref().map_or(Default::default(), |c| c.tip_hash),
            };
        }

        // Reset the previous log file's write position
        if let Some(prev_cursor) = prev_cursor {
            if let Some(prev_file) = self.lru_cache.borrow_mut().get(&prev_log_id) {
                prev_file.metadata.borrow_mut().write = prev_cursor;
            }
        }
    }

    /// Gets the read position metadata - from active file, or if we just rotated and
    /// the read positions are still in the previous log segment file, returns that instead
    pub fn get_latest_read_cursor(&self) -> LogSegmentCursor {
        let (read_cursor, write_cursor, prev_log_id) = {
            let active_file = self.active_file.borrow();
            let metadata = active_file.metadata.borrow();
            (metadata.read.clone(), metadata.write.clone(), metadata.log_id.saturating_sub(1))
        };

        if let Some(read) = read_cursor {
            return read;
        }

        // Read is still on previous log file, fallback to write if not found
        self.lru_cache.borrow_mut()
            .get(&prev_log_id)
            .map(|prev_file| {
                let metadata = prev_file.metadata.borrow();
                metadata.read.clone().unwrap_or_else(|| metadata.write.clone())
            })
            .unwrap_or(write_cursor)
    }

    pub fn active_log_available_space(&self) -> u64 {
        let active_file = self.active_file.borrow();
        let metadata = active_file.metadata.borrow();
        metadata.available_space()
    }

    pub async fn rotate_to_next_log(&self) -> Result<(), OpenOrCreateError> {
        let current_log_id = self.active_log_id();
        let new_log_segment_file = Rc::new(self.active().rotate(&self.shard_dir, self.preallocate_bytes).await?);

        let mut current_log_segment_cache = self.active_file.borrow_mut();
        let old = std::mem::replace(&mut *current_log_segment_cache, new_log_segment_file);

        self.lru_cache.borrow_mut().put(current_log_id, old);

        Ok(())
    }

    /// Called when starting up a shard, ensures we always have an active log file to write to
    pub async fn ready_up(shard_dir: PathBuf, preallocate_bytes: u64, max_cached_files: usize) -> Result<Self, ReadyUpError> {
        if preallocate_bytes <= HEADER_BLOCK_SIZE_BYTES as u64 * 2 || preallocate_bytes % HEADER_BLOCK_SIZE_BYTES as u64 != 0 {
            return Err(ReadyUpError::InvalidPreallocatedBytes(preallocate_bytes));
        }

        std::fs::create_dir_all(&shard_dir)
            .map_err(|source| ReadyUpError::UnableToCreateDirectory {
                directory: shard_dir.to_string_lossy().to_string(),
                source,
            })?;

        let active_log_id = find_latest_log_file(&shard_dir)
            .map_err(|source| ReadyUpError::UnableToAccessDirectory {
                directory: shard_dir.to_string_lossy().to_string(),
                source,
            })?
            .unwrap_or(FIRST_LOG_ID);

        let active_file = LogSegmentFile::open_or_create_first_file_for_shard(&shard_dir, preallocate_bytes, active_log_id, true).await
            .map_err(|source| ReadyUpError::ActiveFileError(source))?;

        let cache_cap = NonZeroUsize::new(max_cached_files.max(1)).unwrap();
        
        Ok(Self {
            active_file: RefCell::new(Rc::new(active_file)),
            lru_cache: RefCell::new(LruCache::new(cache_cap)),
            shard_dir,
            preallocate_bytes,
            force_immediate: Cell::new(false),
        })
    }

    pub fn active_log_id(&self) -> u64 {
        self.active_file.borrow().metadata.borrow().log_id
    }

    pub fn active(&self) -> Rc<LogSegmentFile> {
        self.active_file.borrow().clone()
    }

    /// Close all files and clear cache.
    /// Will wait on other writers to finish their writes and release locks
    /// before closing files.
    pub async fn close(&self) {
        self.active_file.borrow().close().await;

        let cached_files: Vec<Rc<LogSegmentFile>> = {
            let mut cache = self.lru_cache.borrow_mut();
            let mut files = Vec::with_capacity(cache.len());
            while let Some((_, file)) = cache.pop_lru() {
                files.push(file);
            }
            files
        };

        for file in cached_files {
            file.close().await;
        }
    }

    /// Get file by log_id. Returns reader copy of active file or opens/caches older file.
    pub async fn get(&self, log_id: u64) -> Result<Rc<LogSegmentFile>, OpenOrCreateError> {
        // Return the reader duplicate for active file to avoid blocking writers
        if log_id == self.active_log_id() {
            return Ok(self.active());
        }

        // Check cache - borrow is dropped before any await
        {
            let mut cache = self.lru_cache.borrow_mut();
            if let Some(file) = cache.get(&log_id) {
                return Ok(file.clone());
            }
        }

        // Open existing file - this is async
        let log_segment_file = LogSegmentFile::open_existing(&self.shard_dir, log_id).await?;
        let rc_file = Rc::new(log_segment_file);

        // Cache it
        self.lru_cache.borrow_mut().put(log_id, rc_file.clone());

        Ok(rc_file)
    }

    /// Get file by log_id only if already cached (active or in LRU). No I/O.
    pub fn get_if_cached(&self, log_id: u64) -> Option<Rc<LogSegmentFile>> {
        if log_id == self.active_log_id() {
            return Some(self.active());
        }
        self.lru_cache.borrow_mut().get(&log_id).cloned()
    }
}

const FIRST_LOG_ID: u64 = 1;

/// Searches for the most recent log file in the given directory
/// Only matches log_*.wal
fn find_latest_log_file(dir: &PathBuf) -> Result<Option<u64>, std::io::Error> {
    let mut best: Option<u64> = None;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };

        // Expect: log_{id}.wal
        if !name.starts_with("log_") || !name.ends_with(".wal") {
            continue;
        }

        let id_str = &name["log_".len()..name.len() - ".wal".len()];
        if let Ok(id) = id_str.parse::<u64>() {
            match &best {
                None => best = Some(id),
                Some(best_id) if id > *best_id => best = Some(id),
                _ => {}
            }
        }
    }

    Ok(best)
}


#[cfg(test)]
mod tests {
    use super::*;
    use glommio::{LocalExecutorBuilder, Placement};

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    fn test_dir() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shard");
        (tmp, dir)
    }

    const FILE_SIZE: u64 = 1024 * 1024 * 4;

    #[test]
    fn ready_up_creates_dir_and_first_log() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2).await.unwrap();

            assert!(dir.exists());
            assert!(dir.join("log_1.wal").exists());
            assert_eq!(cache.active_log_id(), 1);

            cache.close().await;
        });
    }

    #[test]
    fn ready_up_invalid_preallocate_bytes() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            for bad_size in [0, 512, HEADER_BLOCK_SIZE_BYTES as u64, HEADER_BLOCK_SIZE_BYTES as u64 * 2] {
                let result = LogSegmentsCache::ready_up(dir.clone(), bad_size, 2).await;
                assert!(matches!(result, Err(ReadyUpError::InvalidPreallocatedBytes(_))));
            }
        });
    }

    #[test]
    fn ready_up_finds_latest_existing_log() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            std::fs::create_dir_all(&dir).unwrap();

            use crate::log_segment_file::log_segment_file::LogSegmentFile;
            for id in [1, 3, 7] {
                LogSegmentFile::open_or_create_first_file_for_shard(&dir, FILE_SIZE, id, true).await.unwrap().close().await;
            }

            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2).await.unwrap();
            assert_eq!(cache.active_log_id(), 7);
            cache.close().await;
        });
    }

    #[test]
    fn active_returns_current_file() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2).await.unwrap();

            let active = cache.active();
            assert_eq!(active.metadata.borrow().log_id, 1);

            cache.close().await;
        });
    }

    #[test]
    fn get_returns_active_for_active_id() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2).await.unwrap();

            let file = cache.get(1).await.unwrap();
            assert_eq!(file.metadata.borrow().log_id, 1);

            cache.close().await;
        });
    }

    #[test]
    fn rotate_creates_new_log_and_caches_old() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2).await.unwrap();

            assert_eq!(cache.active_log_id(), 1);
            cache.rotate_to_next_log().await.unwrap();
            assert_eq!(cache.active_log_id(), 2);

            assert!(dir.join("log_2.wal").exists());
            assert!(cache.get_if_cached(1).is_some());

            cache.close().await;
        });
    }

    #[test]
    fn get_opens_uncached_file() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2).await.unwrap();

            cache.rotate_to_next_log().await.unwrap();
            cache.rotate_to_next_log().await.unwrap();
            cache.rotate_to_next_log().await.unwrap();

            assert!(cache.get_if_cached(1).is_none());

            let file = cache.get(1).await.unwrap();
            assert_eq!(file.metadata.borrow().log_id, 1);
            assert!(cache.get_if_cached(1).is_some());

            cache.close().await;
        });
    }

    #[test]
    fn get_if_cached_returns_none_when_not_cached() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2).await.unwrap();

            assert!(cache.get_if_cached(1).is_some());
            assert!(cache.get_if_cached(99).is_none());

            cache.close().await;
        });
    }

    #[test]
    fn lru_eviction() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2).await.unwrap();

            for _ in 0..5 {
                cache.rotate_to_next_log().await.unwrap();
            }

            assert!(cache.get_if_cached(1).is_none());
            assert!(cache.get_if_cached(2).is_none());
            assert!(cache.get_if_cached(3).is_none());
            assert!(cache.get_if_cached(4).is_some());
            assert!(cache.get_if_cached(5).is_some());
            assert!(cache.get_if_cached(6).is_some());

            cache.close().await;
        });
    }

    #[test]
    fn available_space_delegates_to_metadata() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2).await.unwrap();

            let space = cache.active_log_available_space();
            let expected = cache.active().metadata.borrow().available_space();
            assert_eq!(space, expected);

            cache.close().await;
        });
    }

    #[test]
    fn multiple_rotations() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 8).await.unwrap();

            for expected_id in 2..=10 {
                cache.rotate_to_next_log().await.unwrap();
                assert_eq!(cache.active_log_id(), expected_id);
                assert!(dir.join(format!("log_{expected_id}.wal")).exists());
            }

            cache.close().await;
        });
    }

    #[test]
    fn get_nonexistent_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2).await.unwrap();

            let result = cache.get(99).await;
            assert!(result.is_err());

            cache.close().await;
        });
    }

    #[test]
    fn close_is_idempotent() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2).await.unwrap();

            cache.rotate_to_next_log().await.unwrap();

            cache.close().await;
            cache.close().await;
        });
    }
}