use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;
use lru::LruCache;
use std::{cell::{Cell, RefCell}, num::NonZeroUsize, path::PathBuf, rc::Rc};

use crate::{log_segment_file::LogSegmentFile, rotating_log_error::RotatingLogError};

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

    preallocate_bytes: u64,
}

impl LogSegmentsCache {

    pub async fn rotate_to_next_log(&self, required_disk_space: u64) -> Result<bool, RotatingLogError> {

        let (current_log_id, available_space) = {
            let active_file = self.active_file.borrow();
            let active_file_metadata = active_file.metadata.borrow();
            let available_space = active_file_metadata.available_space();
            (active_file_metadata.log_id, available_space)
        };

        if available_space.saturating_sub(required_disk_space) > 0 {
            return Ok(false)
        }

        if self.preallocate_bytes.saturating_sub(required_disk_space).saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64 * 2) == 0 {
            return Err(RotatingLogError::BatchesTooLarge(self.preallocate_bytes));
        }

        let active_log_id = current_log_id.saturating_add(1);

        let new_log_segment_file = Rc::new(LogSegmentFile::open_or_create(&self.shard_dir, self.preallocate_bytes, active_log_id).await?);

        let mut current_log_segment_cache = self.active_file.borrow_mut();
        let old = std::mem::replace(&mut *current_log_segment_cache, new_log_segment_file);

        self.lru_cache.borrow_mut().put(current_log_id, old);

        Ok(true)
    }

    /// Called when starting up a shard, ensures we always have an active log file to write to
    pub async fn ready_up(shard_dir: PathBuf, preallocate_bytes: u64, max_cached_files: usize) -> Result<Self, RotatingLogError> {
        if preallocate_bytes <= FIXED_BLOCK_SIZE_BYTES as u64 * 2 || preallocate_bytes % FIXED_BLOCK_SIZE_BYTES as u64 != 0 {
            return Err(RotatingLogError::InvalidPreallocatedBytes(preallocate_bytes));
        }

        std::fs::create_dir_all(&shard_dir)?;

        let active_log_id = find_latest_log_file(&shard_dir)
            .map_err(|e| RotatingLogError::IoError(e.to_string()))?
            .unwrap_or(FIRST_LOG_ID);

        let active_file = LogSegmentFile::open_or_create(&shard_dir, preallocate_bytes, active_log_id).await?;

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
    pub async fn close(&self) -> Result<(), RotatingLogError> {
        // Close active writer file
        self.active_file.borrow().close().await?;

        // Drain cache by popping all entries
        let cached_files: Vec<Rc<LogSegmentFile>> = {
            let mut cache = self.lru_cache.borrow_mut();
            let mut files = Vec::with_capacity(cache.len());
            while let Some((_, file)) = cache.pop_lru() {
                files.push(file);
            }
            files
        };

        for file in cached_files {
            file.close().await?;
        }

        Ok(())
    }

    /// Get file by log_id. Returns reader copy of active file or opens/caches older file.
    pub async fn get(&self, log_id: u64) -> Result<Rc<LogSegmentFile>, RotatingLogError> {
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
mod log_segments_cache_tests {
    use glommio::{LocalExecutorBuilder, Placement};

    use super::*;

    fn create_test_dir() -> (tempfile::TempDir, PathBuf) {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_dir = tempdir.path().join("test_shard");
        (tempdir, shard_dir)
    }

    #[test]
    fn ready_up_create() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024; // 64 KiB

                let cache = LogSegmentsCache::ready_up(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                // Verify directory and first log file were created
                assert!(shard_dir.exists());
                assert!(shard_dir.join("log_1.wal").exists());

                // Verify file size matches preallocate_bytes
                let metadata = std::fs::metadata(shard_dir.join("log_1.wal")).unwrap();
                assert_eq!(metadata.len(), preallocate_bytes);

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

}