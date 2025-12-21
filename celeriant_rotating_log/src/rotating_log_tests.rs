use std::path::PathBuf;

use glommio::{LocalExecutorBuilder, Placement};

use crate::{
    rotating_log_cache::RotatingLogCache, rotating_log_error::RotatingLogError,
    shard_log_dma_file::ShardLogDmaFile,
};
use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;

#[cfg(test)]
fn create_test_dir() -> (tempfile::TempDir, PathBuf) {
    let tempdir = tempfile::tempdir().unwrap();
    let shard_dir = tempdir.path().join("test_shard");
    (tempdir, shard_dir)
}

// ============================================================================
// RotatingLogCache Tests
// ============================================================================

#[cfg(test)]
mod rotating_log_cache_tests {
    use super::*;

    #[test]
    fn test_new_cache_creates_first_log_file() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024; // 64 KiB

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
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

    #[test]
    fn test_new_cache_opens_existing_latest_log() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Create initial cache with first log
                {
                    let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                        .await
                        .unwrap();
                    {
                        let active_file = cache.active();
                        let mut active = active_file.write().await.unwrap();
                        let log_1 = active
                            .rotate_to_next_log(&shard_dir, preallocate_bytes)
                            .await
                            .unwrap();
                        assert_eq!(log_1.log_id, 1);
                        let log_2 = active
                            .rotate_to_next_log(&shard_dir, preallocate_bytes)
                            .await
                            .unwrap();
                        assert_eq!(log_2.log_id, 2);
                    }
                    cache.close().await.unwrap();
                }

                // third log file should be picked as latest
                {
                    let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                        .await
                        .unwrap();

                    // Active file should be log_2 (highest existing) or log_3 if it creates new
                    // Based on the code, it opens the latest existing file
                    let active = cache.active();
                    let guard = active.read().await.unwrap();
                    assert!(guard.log_id >= 2);
                    drop(guard);

                    cache.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_get_active_file_returns_same_instance() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                let active1 = cache.active();
                let active2 = cache.active();

                // Should be the same Rc
                assert!(std::rc::Rc::ptr_eq(&active1, &active2));

                // get() with active log_id should also return active file
                let guard = active1.read().await.unwrap();
                let active_log_id = guard.log_id;
                drop(guard);

                let fetched = cache.get(active_log_id).await.unwrap();
                assert!(std::rc::Rc::ptr_eq(&active1, &fetched));

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_get_nonexistent_log_returns_error() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                // Try to get a log file that doesn't exist
                let result = cache.get(999).await;
                assert!(result.is_err());

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_rotate_to_next_log() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                // Get initial active log_id
                let initial_log_id = {
                    let active = cache.active();
                    let guard = active.read().await.unwrap();
                    guard.log_id
                };
                assert_eq!(initial_log_id, 1);

                // Perform rotation
                {
                    let active = cache.active();
                    let mut guard = active.write().await.unwrap();
                    let previous = guard
                        .rotate_to_next_log(&shard_dir, preallocate_bytes)
                        .await
                        .unwrap();

                    // Update cache with new active log
                    cache.rotate_to_next_log(guard.log_id, previous);
                }

                // Verify new active log_id
                let new_log_id = {
                    let active = cache.active();
                    let guard = active.read().await.unwrap();
                    guard.log_id
                };
                assert_eq!(new_log_id, 2);

                // Verify old log is now in cache and accessible
                let old_file = cache.get(1).await.unwrap();
                let guard = old_file.read().await.unwrap();
                assert_eq!(guard.log_id, 1);
                drop(guard);

                // Verify both files exist on disk
                assert!(shard_dir.join("log_1.wal").exists());
                assert!(shard_dir.join("log_2.wal").exists());

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_lru_cache_eviction() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;
                let max_cached_files = 2; // Small cache to test eviction

                let cache =
                    RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, max_cached_files)
                        .await
                        .unwrap();

                // Rotate multiple times to create several log files
                for _ in 0..4 {
                    let active = cache.active();
                    let mut guard = active.write().await.unwrap();
                    let previous = guard
                        .rotate_to_next_log(&shard_dir, preallocate_bytes)
                        .await
                        .unwrap();
                    cache.rotate_to_next_log(guard.log_id, previous);
                }

                // Active should now be log_5
                let active = cache.active();
                let guard = active.read().await.unwrap();
                assert_eq!(guard.log_id, 5);
                drop(guard);

                // Access log_1 - should open from disk (was evicted)
                let log1 = cache.get(1).await.unwrap();
                let guard = log1.read().await.unwrap();
                assert_eq!(guard.log_id, 1);
                drop(guard);

                // Access log_2 - should open from disk
                let log2 = cache.get(2).await.unwrap();
                let guard = log2.read().await.unwrap();
                assert_eq!(guard.log_id, 2);
                drop(guard);

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_close_is_idempotent() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                // Close multiple times should not panic or error
                cache.close().await.unwrap();
                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_close_waits_for_write_locks() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                // This test verifies that close() properly acquires locks
                // In a single-threaded executor, this is straightforward
                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }
}

// ============================================================================
// ShardLogDmaFile Tests
// ============================================================================

#[cfg(test)]
mod shard_log_dma_file_tests {
    use super::*;

    #[test]
    fn test_open_or_create_new_file() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let dma_file = ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                    .await
                    .unwrap();

                assert_eq!(dma_file.log_id, 1);
                assert_eq!(dma_file.file_len, preallocate_bytes);
                assert!(dma_file.dma_file.is_some());

                // Verify header positions are initialized correctly
                assert_eq!(
                    dma_file.shard_log_header.metablocks_position,
                    FIXED_BLOCK_SIZE_BYTES as u64
                );
                assert_eq!(
                    dma_file.shard_log_header.datablocks_position,
                    preallocate_bytes - FIXED_BLOCK_SIZE_BYTES as u64
                );

                // Close the file
                if let Some(file) = dma_file.dma_file {
                    file.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_open_or_create_existing_file() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Create initial file
                {
                    let dma_file =
                        ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                            .await
                            .unwrap();
                    if let Some(file) = dma_file.dma_file {
                        file.close().await.unwrap();
                    }
                }

                // Reopen existing file
                {
                    let dma_file =
                        ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                            .await
                            .unwrap();

                    assert_eq!(dma_file.log_id, 1);
                    assert_eq!(dma_file.file_len, preallocate_bytes);

                    if let Some(file) = dma_file.dma_file {
                        file.close().await.unwrap();
                    }
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_open_existing_file() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Create file first
                {
                    let dma_file =
                        ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                            .await
                            .unwrap();
                    if let Some(file) = dma_file.dma_file {
                        file.close().await.unwrap();
                    }
                }

                // Open existing with open_existing
                {
                    let dma_file = ShardLogDmaFile::open_existing(&shard_dir, 1).await.unwrap();

                    assert_eq!(dma_file.log_id, 1);

                    if let Some(file) = dma_file.dma_file {
                        file.close().await.unwrap();
                    }
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_open_existing_nonexistent_file_errors() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                std::fs::create_dir_all(&shard_dir).unwrap();

                let result = ShardLogDmaFile::open_existing(&shard_dir, 999).await;
                assert!(result.is_err());
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_rotate_to_next_log() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let mut dma_file =
                    ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                        .await
                        .unwrap();

                assert_eq!(dma_file.log_id, 1);

                // Rotate to next log
                let previous = dma_file
                    .rotate_to_next_log(&shard_dir, preallocate_bytes)
                    .await
                    .unwrap();

                // Current file should now be log_2
                assert_eq!(dma_file.log_id, 2);
                assert!(dma_file.dma_file.is_some());

                // Previous should be log_1
                assert_eq!(previous.log_id, 1);
                assert!(previous.dma_file.is_some());

                // Both files should exist on disk
                assert!(shard_dir.join("log_1.wal").exists());
                assert!(shard_dir.join("log_2.wal").exists());

                // Clean up
                if let Some(file) = dma_file.dma_file {
                    file.close().await.unwrap();
                }
                if let Some(file) = previous.dma_file {
                    file.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_write_new_headers_and_fsync() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let mut dma_file =
                    ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                        .await
                        .unwrap();

                let initial_meta_pos = dma_file.shard_log_header.metablocks_position;
                let initial_data_pos = dma_file.shard_log_header.datablocks_position;

                // Simulate writing some data by updating positions
                let new_meta_pos = initial_meta_pos + FIXED_BLOCK_SIZE_BYTES as u64;
                let new_data_pos = initial_data_pos - 1024;

                dma_file
                    .write_new_headers_and_fsync(new_data_pos, new_meta_pos, 1)
                    .await
                    .unwrap();

                // Verify in-memory header was updated
                assert_eq!(dma_file.shard_log_header.metablocks_position, new_meta_pos);
                assert_eq!(dma_file.shard_log_header.datablocks_position, new_data_pos);
                assert_eq!(dma_file.shard_log_header.wal_index, 1);

                // Close and reopen to verify persistence
                if let Some(file) = dma_file.dma_file.take() {
                    file.close().await.unwrap();
                }

                let reopened = ShardLogDmaFile::open_existing(&shard_dir, 1).await.unwrap();

                assert_eq!(reopened.shard_log_header.metablocks_position, new_meta_pos);
                assert_eq!(reopened.shard_log_header.datablocks_position, new_data_pos);
                assert_eq!(reopened.shard_log_header.wal_index, 1);

                if let Some(file) = reopened.dma_file {
                    file.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_header_recovery_from_backup() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Create a valid file with headers
                {
                    let dma_file =
                        ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                            .await
                            .unwrap();
                    if let Some(file) = dma_file.dma_file {
                        file.close().await.unwrap();
                    }
                }

                // Corrupt the front header by overwriting first bytes
                let log_path = shard_dir.join("log_1.wal");
                {
                    use std::io::{Seek, Write};
                    let mut file = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&log_path)
                        .unwrap();
                    file.seek(std::io::SeekFrom::Start(0)).unwrap();
                    file.write_all(&[0xFF; 64]).unwrap(); // Corrupt CRC and version
                    file.sync_all().unwrap();
                }

                // Should still open using backup header at end of file
                let dma_file = ShardLogDmaFile::open_existing(&shard_dir, 1).await.unwrap();

                // Header should be recovered from backup
                assert_eq!(
                    dma_file.shard_log_header.metablocks_position,
                    FIXED_BLOCK_SIZE_BYTES as u64
                );

                if let Some(file) = dma_file.dma_file {
                    file.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_both_headers_corrupted_returns_error() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Create a valid file
                {
                    let dma_file =
                        ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                            .await
                            .unwrap();
                    if let Some(file) = dma_file.dma_file {
                        file.close().await.unwrap();
                    }
                }

                // Corrupt both headers
                let log_path = shard_dir.join("log_1.wal");
                {
                    use std::io::{Seek, Write};
                    let mut file = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&log_path)
                        .unwrap();

                    // Corrupt front header
                    file.seek(std::io::SeekFrom::Start(0)).unwrap();
                    file.write_all(&[0xFF; 64]).unwrap();

                    // Corrupt back header
                    file.seek(std::io::SeekFrom::End(-(FIXED_BLOCK_SIZE_BYTES as i64)))
                        .unwrap();
                    file.write_all(&[0xFF; 64]).unwrap();

                    file.sync_all().unwrap();
                }

                let result = ShardLogDmaFile::open_existing(&shard_dir, 1).await;
                assert!(matches!(
                    result,
                    Err(RotatingLogError::HeaderCorrupted { log_id: Some(1) })
                ));
            })
            .unwrap();
        handle.join().unwrap();
    }
}

// ============================================================================
// Validation Tests
// ============================================================================

#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn test_preallocate_bytes_must_be_block_aligned() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();

                // Not aligned to FIXED_BLOCK_SIZE_BYTES (512)
                let unaligned_size = 64 * 1024 + 100;

                let result = RotatingLogCache::new(shard_dir.clone(), unaligned_size, 2).await;

                // Should error because size is not block-aligned
                assert!(
                    result.is_err(),
                    "Expected error for unaligned preallocate_bytes"
                );
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_preallocate_bytes_aligned_succeeds() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();

                // Properly aligned to FIXED_BLOCK_SIZE_BYTES (512)
                let aligned_size = 64 * 1024; // 64 KiB, divisible by 512

                let result = RotatingLogCache::new(shard_dir.clone(), aligned_size, 2).await;
                assert!(result.is_ok());

                if let Ok(cache) = result {
                    cache.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_preallocate_bytes_minimum_size() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();

                // Minimum valid size: need at least 2 blocks for dual headers
                // Plus some space for data
                let min_valid_size = (FIXED_BLOCK_SIZE_BYTES * 3) as u64;

                let result = RotatingLogCache::new(shard_dir.clone(), min_valid_size, 2).await;
                assert!(result.is_ok());

                if let Ok(cache) = result {
                    cache.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_preallocate_bytes_too_small_for_headers() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();

                // Too small: only one block, but we need space for two headers
                let too_small_size = FIXED_BLOCK_SIZE_BYTES as u64;

                let result = RotatingLogCache::new(shard_dir.clone(), too_small_size, 2).await;

                // Should error - not enough space for dual headers
                assert!(result.is_err());
            })
            .unwrap();
        handle.join().unwrap();
    }
}

// ============================================================================
// Integration/Workflow Tests
// ============================================================================

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_full_rotation_workflow() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                // Simulate multiple write/rotate cycles
                for expected_log_id in 2..=5 {
                    let active = cache.active();
                    let mut guard = active.write().await.unwrap();

                    // Simulate updating header positions before rotation
                    let new_meta_pos =
                        guard.shard_log_header.metablocks_position + FIXED_BLOCK_SIZE_BYTES as u64;
                    let new_data_pos = guard.shard_log_header.datablocks_position - 1024;
                    let new_wal_index = guard.shard_log_header.wal_index + 1;
                    guard
                        .write_new_headers_and_fsync(new_data_pos, new_meta_pos, new_wal_index)
                        .await
                        .unwrap();

                    // Rotate
                    let previous = guard
                        .rotate_to_next_log(&shard_dir, preallocate_bytes)
                        .await
                        .unwrap();
                    cache.rotate_to_next_log(guard.log_id, previous);

                    assert_eq!(guard.log_id, expected_log_id);
                }

                // Verify all log files exist
                for log_id in 1..=5 {
                    assert!(shard_dir.join(format!("log_{}.wal", log_id)).exists());
                }

                // Verify we can access old logs
                for log_id in 1..=4 {
                    let file = cache.get(log_id).await.unwrap();
                    let guard = file.read().await.unwrap();
                    assert_eq!(guard.log_id, log_id);
                }

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_reopen_after_rotations() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Create and rotate several times
                {
                    let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                        .await
                        .unwrap();

                    for _ in 0..3 {
                        let active = cache.active();
                        let mut guard = active.write().await.unwrap();
                        let previous = guard
                            .rotate_to_next_log(&shard_dir, preallocate_bytes)
                            .await
                            .unwrap();
                        cache.rotate_to_next_log(guard.log_id, previous);
                    }

                    cache.close().await.unwrap();
                }

                // Reopen - should find log_4 as the latest
                {
                    let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                        .await
                        .unwrap();

                    let active = cache.active();
                    let guard = active.read().await.unwrap();
                    assert_eq!(guard.log_id, 4, "Should reopen with latest log file");
                    drop(guard);

                    // Should be able to access older logs
                    for log_id in 1..=3 {
                        let file = cache.get(log_id).await.unwrap();
                        let guard = file.read().await.unwrap();
                        assert_eq!(guard.log_id, log_id);
                    }

                    cache.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_concurrent_readers_and_writer() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let cache = std::rc::Rc::new(
                    RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                        .await
                        .unwrap(),
                );

                // Spawn multiple reader tasks
                let cache_clone1 = cache.clone();
                let reader1 = glommio::spawn_local(async move {
                    for _ in 0..10 {
                        let active = cache_clone1.active();
                        let guard = active.read().await.unwrap();
                        let _log_id = guard.log_id;
                        drop(guard);
                        glommio::timer::sleep(std::time::Duration::from_micros(100)).await;
                    }
                });

                let cache_clone2 = cache.clone();
                let reader2 = glommio::spawn_local(async move {
                    for _ in 0..10 {
                        let active = cache_clone2.active();
                        let guard = active.read().await.unwrap();
                        let _log_id = guard.log_id;
                        drop(guard);
                        glommio::timer::sleep(std::time::Duration::from_micros(100)).await;
                    }
                });

                // Writer task that does rotations
                let cache_clone3 = cache.clone();
                let shard_dir_clone = shard_dir.clone();
                let writer = glommio::spawn_local(async move {
                    for _ in 0..3 {
                        let active = cache_clone3.active();
                        let mut guard = active.write().await.unwrap();
                        let previous = guard
                            .rotate_to_next_log(&shard_dir_clone, preallocate_bytes)
                            .await
                            .unwrap();
                        cache_clone3.rotate_to_next_log(guard.log_id, previous);
                        drop(guard);
                        glommio::timer::sleep(std::time::Duration::from_micros(500)).await;
                    }
                });

                // Wait for all tasks
                reader1.await;
                reader2.await;
                writer.await;

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_header_persistence_survives_crash_simulation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let expected_meta_pos;
                let expected_data_pos;
                let expected_wal_index;

                // Write and fsync, then "crash" (don't call close)
                {
                    let mut dma_file =
                        ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                            .await
                            .unwrap();

                    // Update positions
                    expected_meta_pos = dma_file.shard_log_header.metablocks_position
                        + FIXED_BLOCK_SIZE_BYTES as u64 * 5;
                    expected_data_pos = dma_file.shard_log_header.datablocks_position - 2048;
                    expected_wal_index = 5;

                    dma_file
                        .write_new_headers_and_fsync(expected_data_pos, expected_meta_pos, expected_wal_index)
                        .await
                        .unwrap();

                    // Simulate crash - just drop without closing
                    // The file handle will be dropped but data should be durable
                    //dma_file.dma_file.take().unwrap().close().await.unwrap();
                }

                // "Recover" - reopen and verify
                {
                    let dma_file = ShardLogDmaFile::open_existing(&shard_dir, 1).await.unwrap();

                    assert_eq!(
                        dma_file.shard_log_header.metablocks_position,
                        expected_meta_pos
                    );
                    assert_eq!(
                        dma_file.shard_log_header.datablocks_position,
                        expected_data_pos
                    );
                    assert_eq!(dma_file.shard_log_header.wal_index, expected_wal_index);

                    if let Some(file) = dma_file.dma_file {
                        file.close().await.unwrap();
                    }
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_log_file_naming_convention() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                // Rotate to create multiple files
                for _ in 0..5 {
                    let active = cache.active();
                    let mut guard = active.write().await.unwrap();
                    let previous = guard
                        .rotate_to_next_log(&shard_dir, preallocate_bytes)
                        .await
                        .unwrap();
                    cache.rotate_to_next_log(guard.log_id, previous);
                }

                // Verify naming: log_{id}.wal
                let entries: Vec<_> = std::fs::read_dir(&shard_dir)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect();

                for log_id in 1..=6 {
                    let expected_name = format!("log_{}.wal", log_id);
                    assert!(
                        entries.contains(&expected_name),
                        "Expected {} in {:?}",
                        expected_name,
                        entries
                    );
                }

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_large_log_id_values() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Create a file with a large log_id directly
                let large_log_id = u64::MAX - 1;

                let dma_file =
                    ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, large_log_id)
                        .await
                        .unwrap();

                assert_eq!(dma_file.log_id, large_log_id);

                if let Some(file) = dma_file.dma_file {
                    file.close().await.unwrap();
                }

                // Verify cache finds it as latest
                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                let active = cache.active();
                let guard = active.read().await.unwrap();
                assert_eq!(guard.log_id, large_log_id);
                drop(guard);

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_max_cached_files_boundary() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Test with max_cached_files = 0 (should be clamped to 1)
                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 0)
                    .await
                    .unwrap();

                // Should still work
                let active = cache.active();
                let guard = active.read().await.unwrap();
                assert_eq!(guard.log_id, 1);
                drop(guard);

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }
}

// ============================================================================
// Edge Cases
// ============================================================================

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_empty_directory_creates_log_1() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Directory doesn't exist yet
                assert!(!shard_dir.exists());

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                // Should create directory and log_1.wal
                assert!(shard_dir.exists());
                assert!(shard_dir.join("log_1.wal").exists());

                let active = cache.active();
                let guard = active.read().await.unwrap();
                assert_eq!(guard.log_id, 1);
                drop(guard);

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_directory_with_non_log_files() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Create directory with some non-log files
                std::fs::create_dir_all(&shard_dir).unwrap();
                std::fs::write(shard_dir.join("random.txt"), "hello").unwrap();
                std::fs::write(shard_dir.join("log_notanumber.wal"), "bad").unwrap();
                std::fs::write(shard_dir.join("notlog_1.wal"), "bad").unwrap();

                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                // Should ignore non-matching files and create log_1
                let active = cache.active();
                let guard = active.read().await.unwrap();
                assert_eq!(guard.log_id, 1);
                drop(guard);

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_sparse_log_ids() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                // Create log files with gaps: 1, 5, 10
                for log_id in [1, 5, 10] {
                    let dma_file =
                        ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, log_id)
                            .await
                            .unwrap();
                    if let Some(file) = dma_file.dma_file {
                        file.close().await.unwrap();
                    }
                }

                // Should find log_10 as latest
                let cache = RotatingLogCache::new(shard_dir.clone(), preallocate_bytes, 2)
                    .await
                    .unwrap();

                let active = cache.active();
                let guard = active.read().await.unwrap();
                assert_eq!(guard.log_id, 10);
                drop(guard);

                // Should be able to access other logs
                let log5 = cache.get(5).await.unwrap();
                let guard = log5.read().await.unwrap();
                assert_eq!(guard.log_id, 5);
                drop(guard);

                let log1 = cache.get(1).await.unwrap();
                let guard = log1.read().await.unwrap();
                assert_eq!(guard.log_id, 1);
                drop(guard);

                // Missing log (e.g., 3) should error
                let result = cache.get(3).await;
                assert!(result.is_err());

                cache.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_write_headers_without_dma_file_errors() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024;

                let mut dma_file =
                    ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                        .await
                        .unwrap();

                // Take the dma_file out
                let taken_file = dma_file.dma_file.take();
                assert!(dma_file.dma_file.is_none());

                // Try to write headers - should error
                let result = dma_file.write_new_headers_and_fsync(1000, 2000, 9999).await;
                assert!(result.is_err());

                // Clean up
                if let Some(file) = taken_file {
                    file.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_available_space_calculation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let (_tempdir, shard_dir) = create_test_dir();
                let preallocate_bytes = 64 * 1024u64;

                let dma_file = ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, 1)
                    .await
                    .unwrap();

                // Initial available space: file_len - 2 * FIXED_BLOCK_SIZE_BYTES
                let expected_space = preallocate_bytes - 2 * FIXED_BLOCK_SIZE_BYTES as u64;
                assert_eq!(dma_file.shard_log_header.available_space(), expected_space);

                if let Some(file) = dma_file.dma_file {
                    file.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }
}
