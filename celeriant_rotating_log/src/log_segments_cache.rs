use celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES;
use lru::LruCache;
use std::{cell::RefCell, num::NonZeroUsize, path::{Path, PathBuf}, rc::Rc};

use crate::{errors::{open_or_create_error::OpenOrCreateError, ready_up_error::ReadyUpError}, log_segment_file::{log_segment_cursor::LogSegmentCursor, log_segment_file::{is_zero_dual_header_orphan, log_file_name, LogSegmentFile}}};

/// Manages DmaFile handles for a shard with LRU caching.
/// "Active File" file is the current log being written to
/// Older logs are opened on-demand and cached with LRU eviction
pub struct LogSegmentsCache {
    /// The file we are currently appending to. It doesn't get added
    /// to the LRU cache.
    active_file: RefCell<Rc<LogSegmentFile>>,

    /// Cache of every open dma file other than the active log file.
    lru_cache: RefCell<LruCache<u64, Rc<LogSegmentFile>>>,

    /// Required as we lazy-load log files on demand in get()
    shard_dir: PathBuf,

    pub preallocate_bytes: u64,

    shard_label: [(&'static str, String); 1],
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
        let wal_index_at_rotation = self.active().metadata.borrow().write.wal_index;
        let new_log_segment_file = Rc::new(self.active().rotate(&self.shard_dir, self.preallocate_bytes).await?);
        let new_log_id = new_log_segment_file.metadata.borrow().log_id;

        let mut current_log_segment_cache = self.active_file.borrow_mut();
        let old = std::mem::replace(&mut *current_log_segment_cache, new_log_segment_file);

        self.lru_cache.borrow_mut().put(current_log_id, old);

        metrics::counter!("celeriant_log_rotations_total", &self.shard_label).increment(1);
        metrics::gauge!("celeriant_log_segments_total", &self.shard_label).set((1 + self.lru_cache.borrow().len()) as f64);
        tracing::info!(shard_id = %self.shard_label[0].1, old_log_id = current_log_id, new_log_id, wal_index_at_rotation, "Log segment rotated");

        Ok(())
    }

    /// Called when starting up a shard, ensures we always have an active log file to write to
    pub async fn ready_up(shard_dir: PathBuf, preallocate_bytes: u64, max_cached_files: usize, shard_id: u32) -> Result<Self, ReadyUpError> {
        if preallocate_bytes <= HEADER_BLOCK_SIZE_BYTES as u64 * 2 || preallocate_bytes % HEADER_BLOCK_SIZE_BYTES as u64 != 0 {
            return Err(ReadyUpError::InvalidPreallocatedBytes(preallocate_bytes));
        }

        std::fs::create_dir_all(&shard_dir)
            .map_err(|source| ReadyUpError::UnableToCreateDirectory {
                directory: shard_dir.to_string_lossy().to_string(),
                source,
            })?;

        let mut active_log_id = find_latest_log_file(&shard_dir)
            .map_err(|source| ReadyUpError::UnableToAccessDirectory {
                directory: shard_dir.to_string_lossy().to_string(),
                source,
            })?
            .unwrap_or(FIRST_LOG_ID);

        let active_file = loop {
            match LogSegmentFile::open_or_create_first_file_for_shard(&shard_dir, preallocate_bytes, active_log_id, true).await {
                Ok(f) => break f,
                Err(e @ OpenOrCreateError::LogSegmentFileCorrupted { .. }) => {
                    let orphan_path = shard_dir.join(log_file_name(active_log_id));
                    let is_orphan = match is_zero_dual_header_orphan(&orphan_path) {
                        Ok(v) => v,
                        Err(io_err) => {
                            tracing::warn!(
                                path = %orphan_path.display(),
                                error = ?io_err,
                                "is_zero_dual_header_orphan I/O error during recovery — refusing to delete, treating as fatal"
                            );
                            return Err(ReadyUpError::ActiveFileError(e));
                        }
                    };

                    if !is_orphan {
                        return Err(ReadyUpError::ActiveFileError(e));
                    }

                    tracing::warn!(
                        log_id = active_log_id,
                        path = %orphan_path.display(),
                        "Detected zero-dual-header orphan from crashed rotation; deleting and falling back to log_id-1"
                    );

                    std::fs::remove_file(&orphan_path)
                        .map_err(|source| ReadyUpError::UnableToDeleteOrphanSegment {
                            path: orphan_path.to_string_lossy().to_string(),
                            source,
                        })?;

                    metrics::counter!("celeriant_orphan_segment_recovered_total").increment(1);

                    if active_log_id == FIRST_LOG_ID {
                        // Just removed the floor segment; next iteration creates a fresh one.
                        // If THAT also returns corrupted, the disk is fundamentally broken.
                        match LogSegmentFile::open_or_create_first_file_for_shard(&shard_dir, preallocate_bytes, FIRST_LOG_ID, true).await {
                            Ok(f) => break f,
                            Err(e) => return Err(ReadyUpError::ActiveFileError(e)),
                        }
                    }

                    active_log_id -= 1;
                }
                Err(e) => return Err(ReadyUpError::ActiveFileError(e)),
            }
        };

        let cache_cap = NonZeroUsize::new(max_cached_files.max(1)).unwrap();
        let shard_label = [("shard_id", shard_id.to_string())];

        metrics::gauge!("celeriant_log_segments_total", &shard_label).set(1.0);

        Ok(Self {
            active_file: RefCell::new(Rc::new(active_file)),
            lru_cache: RefCell::new(LruCache::new(cache_cap)),
            shard_dir,
            preallocate_bytes,
            shard_label,
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
                metrics::counter!("celeriant_cache_log_file_hits_total", &self.shard_label).increment(1);
                return Ok(file.clone());
            }
        }

        metrics::counter!("celeriant_cache_log_file_misses_total", &self.shard_label).increment(1);

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

    /// Evict a sealed segment from the LRU cache. No-op if not cached or if it's the active file.
    ///
    /// Drops the cache's `Rc<LogSegmentFile>` reference. File handles are closed when the last
    /// `Rc` drops (i.e., when any in-flight reads also release their references).
    pub fn evict_from_lru(&self, log_id: u64) {
        // Never evict the active file — it receives writes.
        if log_id == self.active_log_id() {
            return;
        }
        self.lru_cache.borrow_mut().pop(&log_id);
    }

    /// Returns the shard data directory path.
    pub fn shard_dir(&self) -> &Path {
        &self.shard_dir
    }

    /// Discard the active segment and any intermediates, making the target
    /// sealed segment the new active write target.
    ///
    /// Used during S3 catchup truncation when the common ancestor lives in a
    /// sealed segment. After return:
    ///
    /// - The segment at `target_log_id` is the active file (removed from LRU)
    /// - All segment files for ids in `(target_log_id, old_active_log_id]`
    ///   are deleted from disk and evicted from the LRU
    ///
    /// The caller must hold the rollback lock to block concurrent writes.
    /// In-flight reads on discarded segments are safe: Linux unlink semantics
    /// keep the fd alive until the last Rc drops.
    ///
    /// The caller is responsible for rewriting `target_log_id`'s dual headers
    /// to reflect the new write cursor.
    pub async fn unwind_active_to_sealed(
        &self,
        target_log_id: u64,
    ) -> Result<(), UnwindActiveError> {
        let current_active_id = self.active_log_id();
        if target_log_id >= current_active_id {
            return Err(UnwindActiveError::NotOlderThanActive {
                target: target_log_id,
                active: current_active_id,
            });
        }

        // Open the target (may already be in the LRU) and remove it from the
        // LRU so we can move it into the active slot without aliasing.
        let target = self
            .get(target_log_id)
            .await
            .map_err(UnwindActiveError::OpenTarget)?;
        self.lru_cache.borrow_mut().pop(&target_log_id);

        // Collect every log_id that must be discarded (target+1..=current_active).
        let discard_ids: Vec<u64> = (target_log_id + 1..=current_active_id).collect();

        // Drop LRU references so file handles close when the old active Rc
        // drops below. Active slot is replaced separately.
        for id in &discard_ids {
            if *id != current_active_id {
                self.lru_cache.borrow_mut().pop(id);
            }
        }

        // Swap the active slot. The old active file is closed here.
        let old_active = {
            let mut slot = self.active_file.borrow_mut();
            std::mem::replace(&mut *slot, target)
        };
        old_active.close().await;
        drop(old_active);

        // Delete the discarded files from disk.
        for id in &discard_ids {
            let path = self.shard_dir.join(log_file_name(*id));
            if let Err(e) = std::fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(UnwindActiveError::DeleteSegment {
                        log_id: *id,
                        source: e.to_string(),
                    });
                }
            }
        }

        metrics::gauge!("celeriant_log_segments_total", &self.shard_label)
            .set((1 + self.lru_cache.borrow().len()) as f64);
        tracing::warn!(
            shard_id = %self.shard_label[0].1,
            target_log_id,
            previous_active_log_id = current_active_id,
            discarded = discard_ids.len(),
            "Unwound active log segment to sealed — discarded newer segments"
        );

        Ok(())
    }
}

#[derive(Debug)]
pub enum UnwindActiveError {
    NotOlderThanActive { target: u64, active: u64 },
    OpenTarget(OpenOrCreateError),
    DeleteSegment { log_id: u64, source: String },
}

impl std::fmt::Display for UnwindActiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotOlderThanActive { target, active } => {
                write!(f, "target log_id {target} must be strictly less than active {active}")
            }
            Self::OpenTarget(e) => write!(f, "failed to open target segment: {e:?}"),
            Self::DeleteSegment { log_id, source } => {
                write!(f, "failed to delete segment log_{log_id}: {source}")
            }
        }
    }
}

impl std::error::Error for UnwindActiveError {}

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
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2, 0).await.unwrap();

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
                let result = LogSegmentsCache::ready_up(dir.clone(), bad_size, 2, 0).await;
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

            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2, 0).await.unwrap();
            assert_eq!(cache.active_log_id(), 7);
            cache.close().await;
        });
    }

    #[test]
    fn active_returns_current_file() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2, 0).await.unwrap();

            let active = cache.active();
            assert_eq!(active.metadata.borrow().log_id, 1);

            cache.close().await;
        });
    }

    #[test]
    fn get_returns_active_for_active_id() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2, 0).await.unwrap();

            let file = cache.get(1).await.unwrap();
            assert_eq!(file.metadata.borrow().log_id, 1);

            cache.close().await;
        });
    }

    #[test]
    fn rotate_creates_new_log_and_caches_old() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2, 0).await.unwrap();

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
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2, 0).await.unwrap();

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
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2, 0).await.unwrap();

            assert!(cache.get_if_cached(1).is_some());
            assert!(cache.get_if_cached(99).is_none());

            cache.close().await;
        });
    }

    #[test]
    fn lru_eviction() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2, 0).await.unwrap();

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
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2, 0).await.unwrap();

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
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 8, 0).await.unwrap();

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
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2, 0).await.unwrap();

            let result = cache.get(99).await;
            assert!(result.is_err());

            cache.close().await;
        });
    }

    #[test]
    fn close_is_idempotent() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 2, 0).await.unwrap();

            cache.rotate_to_next_log().await.unwrap();

            cache.close().await;
            cache.close().await;
        });
    }

    fn make_zero_file(path: &std::path::Path, size: u64) {
        let f = std::fs::OpenOptions::new().create_new(true).write(true).open(path).unwrap();
        f.set_len(size).unwrap();
    }

    #[test]
    fn ready_up_recovers_zero_dual_header_orphans() {
        glommio_test!({
            use crate::log_segment_file::log_segment_file::LogSegmentFile;

            // case 1: single orphan after valid log_1 → falls back to log_1
            {
                let (_tmp, dir) = test_dir();
                std::fs::create_dir_all(&dir).unwrap();
                LogSegmentFile::open_or_create_first_file_for_shard(&dir, FILE_SIZE, 1, true).await.unwrap().close().await;
                make_zero_file(&dir.join("log_2.wal"), FILE_SIZE);

                let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2, 0).await.unwrap();
                assert_eq!(cache.active_log_id(), 1);
                assert!(!dir.join("log_2.wal").exists());
                assert!(dir.join("log_1.wal").exists());
                cache.close().await;
            }

            // case 2: two consecutive orphans after valid log_1 → walks past both
            {
                let (_tmp, dir) = test_dir();
                std::fs::create_dir_all(&dir).unwrap();
                LogSegmentFile::open_or_create_first_file_for_shard(&dir, FILE_SIZE, 1, true).await.unwrap().close().await;
                make_zero_file(&dir.join("log_2.wal"), FILE_SIZE);
                make_zero_file(&dir.join("log_3.wal"), FILE_SIZE);

                let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2, 0).await.unwrap();
                assert_eq!(cache.active_log_id(), 1);
                assert!(!dir.join("log_2.wal").exists());
                assert!(!dir.join("log_3.wal").exists());
                cache.close().await;
            }

            // case 3: orphan at FIRST_LOG_ID → deleted and replaced with fresh segment
            {
                let (_tmp, dir) = test_dir();
                std::fs::create_dir_all(&dir).unwrap();
                make_zero_file(&dir.join("log_1.wal"), FILE_SIZE);

                let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 2, 0).await.unwrap();
                assert_eq!(cache.active_log_id(), FIRST_LOG_ID);
                assert!(dir.join("log_1.wal").exists());
                cache.close().await;
            }
        });
    }

    #[test]
    fn unwind_rejects_active_or_newer() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir, FILE_SIZE, 4, 0).await.unwrap();
            cache.rotate_to_next_log().await.unwrap();
            assert_eq!(cache.active_log_id(), 2);

            // Same as active
            assert!(matches!(
                cache.unwind_active_to_sealed(2).await,
                Err(UnwindActiveError::NotOlderThanActive { target: 2, active: 2 })
            ));
            // Newer than active
            assert!(matches!(
                cache.unwind_active_to_sealed(99).await,
                Err(UnwindActiveError::NotOlderThanActive { target: 99, active: 2 })
            ));

            cache.close().await;
        });
    }

    #[test]
    fn unwind_to_previous_segment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 4, 0).await.unwrap();
            cache.rotate_to_next_log().await.unwrap();
            assert_eq!(cache.active_log_id(), 2);

            cache.unwind_active_to_sealed(1).await.unwrap();

            assert_eq!(cache.active_log_id(), 1);
            assert!(!dir.join("log_2.wal").exists(), "discarded segment should be deleted");
            assert!(dir.join("log_1.wal").exists());

            cache.close().await;
        });
    }

    #[test]
    fn unwind_discards_intermediates() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let cache = LogSegmentsCache::ready_up(dir.clone(), FILE_SIZE, 8, 0).await.unwrap();
            for _ in 0..4 {
                cache.rotate_to_next_log().await.unwrap();
            }
            assert_eq!(cache.active_log_id(), 5);

            cache.unwind_active_to_sealed(2).await.unwrap();

            assert_eq!(cache.active_log_id(), 2);
            assert!(dir.join("log_1.wal").exists());
            assert!(dir.join("log_2.wal").exists());
            for id in 3..=5 {
                assert!(!dir.join(format!("log_{id}.wal")).exists(), "log_{id}.wal should be deleted");
            }
            // LRU should only have log_1 (intermediates evicted)
            assert!(cache.get_if_cached(1).is_some());
            assert!(cache.get_if_cached(3).is_none());

            cache.close().await;
        });
    }
}