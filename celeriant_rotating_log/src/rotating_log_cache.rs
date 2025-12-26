use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
    path::PathBuf,
    rc::Rc,
};
use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;
use glommio::sync::RwLock;
use lru::LruCache;

use crate::{rotating_log_error::RotatingLogError, rwlock_timeout::write_with_timeout, shard_log_dma_file::ShardLogDmaFile};

/// Manages DmaFile handles for a shard with LRU caching.
///
/// - "Active File" file is the current log being written to
/// - Older logs are opened on-demand and cached with LRU eviction
/// - All files use the same `Rc<RwLock<Option<DmaFile>>>` type for uniform access
pub struct RotatingLogCache {
    
    /// The file we are currently appending to. It doesn't get added
    /// to the LRU cache.
    active_file: Rc<RwLock<ShardLogDmaFile>>,

    /// Duplicate fd of active_file for concurrent reading without blocking writers
    active_file_reader: Rc<RwLock<ShardLogDmaFile>>,
    
    /// We keep active_log_id cached here to avoid taking a read lock
    /// on active_file, allowing readers to search for the right log
    /// file without being blocked by writers to active_file
    active_log_id: Cell<u64>,

    /// Cache of every open dma file other than the active log file.
    lru_cache: RefCell<LruCache<u64, Rc<RwLock<ShardLogDmaFile>>>>,
    
    /// Required as we lazy-load log files on demand in get()
    shard_dir: PathBuf,
}

const FIRST_LOG_ID: u64 = 1;

impl RotatingLogCache {

    /// Get the current active log id without taking a lock
    pub fn active_log_id(&self) -> u64 {
        self.active_log_id.get()
    }
    
    /// Called when starting up a shard, ensures we always have an active log file to write to
    pub async fn new(
        shard_dir: PathBuf,
        preallocate_bytes: u64,
        max_cached_files: usize,
    ) -> Result<Self, RotatingLogError> {

        if preallocate_bytes <= FIXED_BLOCK_SIZE_BYTES as u64 * 2 || preallocate_bytes % FIXED_BLOCK_SIZE_BYTES as u64 != 0 {
            return Err(RotatingLogError::InvalidPreallocatedBytes(preallocate_bytes));
        }

        std::fs::create_dir_all(&shard_dir)?;

        let active_log_id = find_latest_log_file(&shard_dir)
            .map_err(|e| RotatingLogError::IoError(e.to_string()))?
            .unwrap_or(FIRST_LOG_ID);

        let active_dma_file = ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, active_log_id).await?;

        // Create a duplicate fd for concurrent readers
        let reader_dma_file = dup_shard_log_dma_file(&active_dma_file)?;

        let cache_cap = NonZeroUsize::new(max_cached_files.max(1)).unwrap();
        Ok(Self {
            active_file: Rc::new(RwLock::new(active_dma_file)),
            active_file_reader: Rc::new(RwLock::new(reader_dma_file)),
            active_log_id: Cell::new(active_log_id),
            lru_cache: RefCell::new(LruCache::new(cache_cap)),
            shard_dir,
        })
    }

    /// Get file by log_id. Returns reader copy of active file or opens/caches older file.
    pub async fn get(&self, log_id: u64) -> Result<Rc<RwLock<ShardLogDmaFile>>, RotatingLogError> {
        // Return the reader duplicate for active file to avoid blocking writers
        if log_id == self.active_log_id.get() {
            return Ok(self.active_file_reader.clone());
        }

        // Check cache - borrow is dropped before any await
        {
            let mut cache = self.lru_cache.borrow_mut();
            if let Some(file) = cache.get(&log_id) {
                return Ok(file.clone());
            }
        }

        // Open existing file - this is async
        let opened_dma_file = ShardLogDmaFile::open_existing(&self.shard_dir, log_id).await?;
        let rc_file = Rc::new(RwLock::new(opened_dma_file));

        // Cache it
        self.lru_cache.borrow_mut().put(log_id, rc_file.clone());

        Ok(rc_file)
    }

    /// Allows writers to get access to the RwLock, to lock it for writing.
    pub fn active(&self) -> Rc<RwLock<ShardLogDmaFile>> {
        self.active_file.clone()
    }

    /// Update active log id after rotation.
    /// Caller is responsible for placing the new file in the RwLock.
    /// This is done to allow the caller to control the locking semantics.
    pub async fn rotate_to_next_log(
        &self,
        new_active_log_id: u64,
        previous_shard_log_file: ShardLogDmaFile,
    ) -> Result<(), RotatingLogError> {
        // Update active log id (sync Cell operation)
        self.active_log_id.set(new_active_log_id);

        // Push previous file to cache - borrow dropped before any await
        self.lru_cache.borrow_mut().push(
            previous_shard_log_file.log_id,
            Rc::new(RwLock::new(previous_shard_log_file)),
        );

        // Create new reader duplicate from the new active file
        let new_reader = {
            let active_guard = self.active_file.read().await?;
            dup_shard_log_dma_file(&active_guard)?
        };

        // Update the reader file, closing the old duplicate first
        let mut reader_guard = write_with_timeout(&self.active_file_reader, "rotate_reader").await?;
        if let Some(old_file) = reader_guard.dma_file.take() {
            old_file.close().await?;
        }
        *reader_guard = new_reader;

        Ok(())
    }

    /// Close all files and clear cache.
    /// Will wait on other writers to finish their writes and release locks
    /// before closing files.
    pub async fn close(&self) -> Result<(), RotatingLogError> {
        // Close active writer file
        {
            let mut guard = write_with_timeout(&self.active_file, "close_active").await?;
            if let Some(file) = guard.dma_file.take() {
                file.close().await?;
            }
        }

        // Close active reader duplicate
        {
            let mut guard = write_with_timeout(&self.active_file_reader, "close_active_reader").await?;
            if let Some(file) = guard.dma_file.take() {
                file.close().await?;
            }
        }

        // Collect all cached files first - borrow dropped before async operations
        let cached_files: Vec<Rc<RwLock<ShardLogDmaFile>>> = {
            let cache = self.lru_cache.borrow();
            cache.iter().map(|(_id, file_rc)| file_rc.clone()).collect()
        };

        // Clear the cache - borrow dropped immediately
        self.lru_cache.borrow_mut().clear();

        // Now close all collected files
        for file_rc in cached_files {
            let mut guard = write_with_timeout(&file_rc, "close_reader").await?;
            if let Some(file) = guard.dma_file.take() {
                file.close().await?;
            }
        }

        Ok(())
    }
}

/// Creates a duplicate ShardLogDmaFile with a dup'd file descriptor for concurrent access
fn dup_shard_log_dma_file(source: &ShardLogDmaFile) -> Result<ShardLogDmaFile, RotatingLogError> {
    let dup_dma_file = source.dma_file
        .as_ref()
        .ok_or_else(|| RotatingLogError::IoError("No file handle available to dup".to_string()))?
        .dup()?;

    Ok(ShardLogDmaFile {
        dma_file: Some(dup_dma_file),
        log_id: source.log_id,
        file_len: source.file_len,
        shard_log_header: source.shard_log_header.clone(),
    })
}

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