use std::{cell::RefCell, path::{Path, PathBuf}, rc::Rc};

use celeriant_disk::files::{open_dma_files::{create_file_dma, existing_file_dma}, rwlock_timeout::{LockTimeoutError, read_with_timeout, write_with_timeout}};
use celeriant_wal::{
    constants::{AGGREGATE_BLOOM_BYTES, GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_SHARD_LOG_HEADER},
    shard_log_header::ShardLogHeader,
};
use celeriant_wire::disk::{
    versioned_block::{deserialise_shard_log_header, serialize_versioned_message},
};
use glommio::{
    GlommioError,
    io::DmaFile,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{
    errors::{open_or_create_error::OpenOrCreateError, write_dual_header_error::WriteDualHeaderError}, log_segment_file::log_segment_file_metadata::LogSegmentFileMetadata
};

/// Represents a physical log file on disk, with its associated metadata.
/// Here we are flexible in terms of locking, allowing read/write of metadata during concurrent writes & reads
pub struct LogSegmentFile {
    /// Active, open fd to the log file. Optional type allows to take
    /// ownership when closing the file or rotating the active log file.
    writer: RwLock<Option<Rc<DmaFile>>>,

    /// Duplicate DmaFile for a reader, so we can do reads while writing without blocking
    reader: RwLock<Option<Rc<DmaFile>>>,

    // Metadata directly associated with the log segment file structure
    pub metadata: RefCell<LogSegmentFileMetadata>,
}

impl LogSegmentFile {
    pub async fn lock_reader(&self, location: &'static str) -> Result<RwLockReadGuard<'_, Option<Rc<DmaFile>>>, LockTimeoutError> {
        read_with_timeout(&self.reader, location).await
    }

    pub async fn lock_writer(&self, location: &'static str) -> Result<RwLockWriteGuard<'_, Option<Rc<DmaFile>>>, LockTimeoutError> {
        write_with_timeout(&self.writer, location).await
    }

    pub async fn close(&self) {
        if let Ok(mut guard) = write_with_timeout(&self.writer, "close_writer").await {
            if let Some(rc) = guard.take() {
                if let Ok(writer) = Rc::try_unwrap(rc) {
                    let _ = writer.close().await;
                }
            }
        }
        if let Ok(mut guard) = write_with_timeout(&self.reader, "close_reader").await {
            if let Some(rc) = guard.take() {
                if let Ok(reader) = Rc::try_unwrap(rc) {
                    let _ = reader.close().await;
                }
            }
        }
    }

    pub async fn rotate(&self, shard_dir: &PathBuf, preallocate_bytes: u64) -> Result<Self, OpenOrCreateError> {
        let (new_log_id, wal_index, tip_hash, last_received_replication_wal_index) = {
            let meta = self.metadata.borrow();
            (meta.log_id + 1, meta.write.wal_index, meta.write.tip_hash, meta.last_received_replication_wal_index)
        };

        let log_path = shard_dir.join(log_file_name(new_log_id));

        if log_path.exists() {
            match is_zero_dual_header_orphan(&log_path) {
                Ok(true) => {
                    tracing::warn!(
                        path = %log_path.display(),
                        new_log_id,
                        "Rotation target exists with zero dual-headers — deleting orphan from prior aborted rotation"
                    );
                    metrics::counter!("celeriant_orphan_segment_recovered_total").increment(1);
                    std::fs::remove_file(&log_path).map_err(|e| OpenOrCreateError::RotationTargetUnsafe {
                        log_id: new_log_id,
                        path: log_path.to_string_lossy().into_owned(),
                        source: format!("orphan delete failed: {e}"),
                    })?;
                }
                Ok(false) => {
                    return Err(OpenOrCreateError::RotationTargetUnsafe {
                        log_id: new_log_id,
                        path: log_path.to_string_lossy().into_owned(),
                        source: "file exists with non-zero header(s) — possible live data".into(),
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        path = %log_path.display(),
                        error = ?e,
                        "Failed to inspect rotation target — refusing to overwrite"
                    );
                    return Err(OpenOrCreateError::RotationTargetUnsafe {
                        log_id: new_log_id,
                        path: log_path.to_string_lossy().into_owned(),
                        source: format!("header inspection failed: {e}"),
                    });
                }
            }
        }

        let writer = match create_file_dma(&log_path, Some(preallocate_bytes)).await {
            Ok(w) => w,
            Err(e) => {
                if log_path.exists() {
                    if let Err(rm_err) = std::fs::remove_file(&log_path) {
                        tracing::warn!(
                            path = %log_path.display(),
                            error = ?rm_err,
                            "Failed to remove partial file left by failed create_file_dma"
                        );
                    }
                }
                let io_err: Option<&std::io::Error> = match &e {
                    GlommioError::IoError(io_err) => Some(io_err),
                    GlommioError::EnhancedIoError { source, .. } => Some(source),
                    _ => None,
                };
                if let Some(io_err) = io_err {
                    if io_err.kind() == std::io::ErrorKind::StorageFull {
                        panic!(
                            "rotation failed: ENOSPC on {} (preallocate {} bytes) — physical NVMe is not resizable, requires operator intervention",
                            log_path.display(),
                            preallocate_bytes
                        );
                    }
                }
                return Err(OpenOrCreateError::UnableToCreateLogSegmentFile {
                    log_id: new_log_id,
                    path: log_path.to_string_lossy().into_owned(),
                    preallocate_bytes,
                    source: e.to_string(),
                });
            }
        };

        create_new_file(new_log_id, &log_path, writer, preallocate_bytes, shard_dir, false, wal_index, tip_hash, last_received_replication_wal_index).await
    }

    /// Open a log file, if it doesn't exist, create it.
    /// Used for opening the active writing last log file.
    /// Errors if the file is corrupt and needs repair.
    pub async fn open_or_create_first_file_for_shard(shard_dir: &PathBuf, preallocate_bytes: u64, log_id: u64, advance_read: bool) -> Result<Self, OpenOrCreateError> {
        let log_path = shard_dir.join(log_file_name(log_id));
        let exists = log_path.exists();

        let (writer, file_len) = if exists {
            existing_file_dma(&log_path)
                .await
                .map_err(|e| OpenOrCreateError::UnableToOpenExistingFile {
                    log_id,
                    path: log_path.to_string_lossy().into_owned(),
                    source: e.to_string(),
                })?
        } else {
            (
                create_file_dma(&log_path, Some(preallocate_bytes))
                    .await
                    .map_err(|e| OpenOrCreateError::UnableToCreateLogSegmentFile {
                        log_id,
                        path: log_path.to_string_lossy().into_owned(),
                        preallocate_bytes,
                        source: e.to_string(),
                    })?,
                preallocate_bytes,
            )
        };

        if exists {
            load_existing_file(log_id, &log_path, writer, file_len, advance_read).await
        } else {
            create_new_file(log_id, &log_path, writer, preallocate_bytes, shard_dir, advance_read, 0, GENESIS_HASH, 0).await
        }
    }

    /// Open an existing log file. Errors if it doesn't exist
    /// or the file is corrupt and needs repair.
    pub async fn open_existing(shard_dir: &PathBuf, log_id: u64) -> Result<Self, OpenOrCreateError> {
        let log_path = shard_dir.join(log_file_name(log_id));
        let (writer, file_len) = existing_file_dma(&log_path)
            .await
            .map_err(|e| OpenOrCreateError::UnableToOpenExistingFile {
                log_id,
                path: log_path.to_string_lossy().into_owned(),
                source: e.to_string(),
            })?;

        load_existing_file(log_id, &log_path, writer, file_len, true).await
    }
}


async fn build_log_segment(
    log_id: u64,
    log_path: &PathBuf,
    writer: DmaFile,
    file_len: u64,
    header: &ShardLogHeader,
    advance_read: bool,
) -> Result<LogSegmentFile, OpenOrCreateError> {
    let reader = writer.dup().map_err(|e| OpenOrCreateError::UnableToDuplicateWriterFD {
        log_id,
        path: log_path.to_string_lossy().into_owned(),
        source: e.to_string(),
    })?;

    let datablocks_carry_over = read_datablocks_carry_over_bytes(&reader, header.datablocks_position).await
        .map_err(|source| OpenOrCreateError::LogSegmentFileReadError {
            log_id,
            source: source.to_string(),
            step: "read_datablocks_carry_over_bytes".into(),
        })?;

    Ok(LogSegmentFile {
        writer: RwLock::new(Some(Rc::new(writer))),
        reader: RwLock::new(Some(Rc::new(reader))),
        metadata: RefCell::new(LogSegmentFileMetadata::new(
            log_id, file_len, datablocks_carry_over, header, advance_read,
        )),
    })
}

async fn load_existing_file(
    log_id: u64,
    log_path: &PathBuf,
    mut writer: DmaFile,
    file_len: u64,
    advance_read: bool,
) -> Result<LogSegmentFile, OpenOrCreateError> {
    let header = load_header_detecting_corruption(&mut writer, file_len, log_id).await?;
    build_log_segment(log_id, log_path, writer, file_len, &header, advance_read).await
}

async fn create_new_file(
    log_id: u64,
    log_path: &PathBuf,
    mut writer: DmaFile,
    file_len: u64,
    dir_path: &PathBuf,
    advance_read: bool,
    wal_index: u64,
    tip_hash: [u8; 32],
    last_received_replication_wal_index: u64,
) -> Result<LogSegmentFile, OpenOrCreateError> {
    let header = setup_new_file(&mut writer, log_id, dir_path, file_len, wal_index, tip_hash, last_received_replication_wal_index).await?;
    build_log_segment(log_id, log_path, writer, file_len, &header, advance_read).await
}

pub(crate) fn log_file_name(log_id: u64) -> String {
    format!("log_{log_id}.wal")
}

pub fn is_zero_dual_header_orphan(path: &Path) -> Result<bool, std::io::Error> {
    use std::io::{Read, Seek, SeekFrom};

    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };

    let file_len = f.metadata()?.len();
    if file_len < HEADER_BLOCK_SIZE_BYTES as u64 * 2 {
        return Ok(true);
    }

    let mut buf = vec![0u8; HEADER_BLOCK_SIZE_BYTES];

    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut buf)?;
    if !buf.iter().all(|&b| b == 0) {
        return Ok(false);
    }

    f.seek(SeekFrom::Start(file_len - HEADER_BLOCK_SIZE_BYTES as u64))?;
    f.read_exact(&mut buf)?;
    Ok(buf.iter().all(|&b| b == 0))
}

/// Tries to load the header. Will try the front of the file first,
/// then the end of the file if the front is corrupted.
async fn load_header_detecting_corruption(dma_file: &mut DmaFile, file_len: u64, log_id: u64) -> Result<ShardLogHeader, OpenOrCreateError> {
    let header_bytes = dma_file
        .read_at(0, HEADER_BLOCK_SIZE_BYTES)
        .await
        .map_err(|source| OpenOrCreateError::LogSegmentFileReadError {
            log_id,
            source: source.to_string(),
            step: "read_header_front".into(),
        })?;

    match deserialise_shard_log_header(&header_bytes) {
        Ok(shard_log_header) => Ok(shard_log_header),
        Err(_primary_err) => {
            // Primary header failed, try backup at end of file
            let header_bytes = dma_file
                .read_at(file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64), HEADER_BLOCK_SIZE_BYTES)
                .await
                .map_err(|source| OpenOrCreateError::LogSegmentFileReadError {
                    log_id,
                    source: source.to_string(),
                    step: "read_header_rear".into(),
                })?;

            match deserialise_shard_log_header(&header_bytes) {
                Ok(shard_log_header) => Ok(shard_log_header),
                Err(source) => Err(OpenOrCreateError::LogSegmentFileCorrupted { log_id, source }),
            }
        }
    }
}

/// Setup a new file, writing the header to the start and end of the file
/// Assumes the file already is preallocated to file_len. Will fsync the
/// file and fsync the parent directory.
async fn setup_new_file(dma_file: &mut DmaFile, log_id: u64, dir_path: &PathBuf, file_len: u64, wal_index: u64, tip_hash: [u8; 32], last_received_replication_wal_index: u64) -> Result<ShardLogHeader, OpenOrCreateError> {
    let header = ShardLogHeader {
        metablocks_position: HEADER_BLOCK_SIZE_BYTES as u64,
        datablocks_position: file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64),
        wal_index,
        aggregate_bloom: vec![0u64; AGGREGATE_BLOOM_BYTES / 8],
        tip_hash,
        last_received_replication_wal_index,
    };

    write_dual_shard_log_header(dma_file, header.datablocks_position, &header)
        .await
        .map_err(|source| OpenOrCreateError::LogSegmentFileHeaderWriteFailure { log_id, source })?;

    dma_file
        .fdatasync()
        .await
        .map_err(|source| OpenOrCreateError::FSyncErrorOnNewFile { log_id, source: source.to_string() })?;

    // Folder fsync - fsync the parent directory to ensure the file entry is durable
    let dir = glommio::io::Directory::open(dir_path)
        .await
        .map_err(|source| OpenOrCreateError::DirectoryFSyncErrorOnNewFile {
            log_id,
            source: source.to_string(),
            path: dir_path.to_string_lossy().into_owned(),
            step: "dir_open".into(),
        })?;

    dir.sync().await.map_err(|source| OpenOrCreateError::DirectoryFSyncErrorOnNewFile {
        log_id,
        source: source.to_string(),
        path: dir_path.to_string_lossy().into_owned(),
        step: "dir_sync".into(),
    })?;

    dir.close().await.map_err(|source| OpenOrCreateError::DirectoryFSyncErrorOnNewFile {
        log_id,
        source: source.to_string(),
        path: dir_path.to_string_lossy().into_owned(),
        step: "dir_close".into(),
    })?;

    Ok(header)
}

pub async fn write_dual_shard_log_header(dma_file: &DmaFile, header_end_start_pos: u64, header: &ShardLogHeader) -> Result<(), WriteDualHeaderError> {
    let mut header_bytes = dma_file.alloc_dma_buffer(HEADER_BLOCK_SIZE_BYTES);

    serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, header_bytes.as_bytes_mut())?;

    let header_bytes = Rc::new(header_bytes);

    dma_file
        .write_rc_at(header_bytes.clone(), 0)
        .await
        .map_err(|_| WriteDualHeaderError::FileWriteError { from_back: false })?;
    dma_file
        .write_rc_at(header_bytes, header_end_start_pos)
        .await
        .map_err(|_| WriteDualHeaderError::FileWriteError { from_back: true })?;

    Ok(())
}

pub async fn read_datablocks_carry_over_bytes(dma_file: &DmaFile, datablocks_position: u64) -> Result<Option<Vec<u8>>, GlommioError<()>> {
    let alignment = (dma_file.alignment() as u64).max(celeriant_wal::constants::MIN_WRITE_ALIGNMENT);
    let datablocks_carry_over_size = celeriant_wal::constants::align_up(datablocks_position, alignment).saturating_sub(datablocks_position);

    if datablocks_carry_over_size > 0 {
        return Ok(Some(
            dma_file.read_at(datablocks_position, datablocks_carry_over_size as usize).await?.to_vec(),
        ));
    } else {
        return Ok(None);
    }
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
        std::fs::create_dir_all(&dir).unwrap();
        (tmp, dir)
    }

    const MIN_FILE_SIZE: u64 = HEADER_BLOCK_SIZE_BYTES as u64 * 3;

    #[test]
    fn create_new_file() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let file = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap();

            let meta = file.metadata.borrow();
            assert_eq!(meta.log_id, 1);
            assert_eq!(meta.file_len, MIN_FILE_SIZE);
            assert_eq!(meta.write.metablocks_position, HEADER_BLOCK_SIZE_BYTES as u64);
            assert_eq!(meta.write.datablocks_position, MIN_FILE_SIZE - HEADER_BLOCK_SIZE_BYTES as u64);
            assert!(meta.read.is_some());

            file.close().await;
        });
    }

    #[test]
    fn create_then_reopen() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 5, true).await.unwrap().close().await;

            let file = LogSegmentFile::open_existing(&dir, 5).await.unwrap();
            assert_eq!(file.metadata.borrow().log_id, 5);
            file.close().await;
        });
    }

    #[test]
    fn open_nonexistent_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let result = LogSegmentFile::open_existing(&dir, 99).await;
            assert!(matches!(result, Err(OpenOrCreateError::UnableToOpenExistingFile { log_id: 99, .. })));
        });
    }

    #[test]
    fn advance_read_flag() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            let with_advance = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap();
            assert!(with_advance.metadata.borrow().read.is_some());
            with_advance.close().await;

            let without_advance = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 2, false).await.unwrap();
            assert!(without_advance.metadata.borrow().read.is_none());
            without_advance.close().await;
        });
    }

    #[test]
    fn dual_fd_independence() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let file = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap();

            let reader = file.lock_reader("test").await.unwrap();
            let writer = file.lock_writer("test").await.unwrap();

            assert!(reader.is_some());
            assert!(writer.is_some());

            drop(reader);
            drop(writer);
            file.close().await;
        });
    }

    #[test]
    fn multiple_log_ids() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            for log_id in [1, 5, 100, u64::MAX - 1] {
                let file = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, log_id, true).await.unwrap();
                assert_eq!(file.metadata.borrow().log_id, log_id);
                assert!(dir.join(format!("log_{log_id}.wal")).exists());
                file.close().await;
            }
        });
    }

    #[test]
    fn header_corruption_recovery() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let path = dir.join("log_1.wal");

            LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap().close().await;

            {
                let mut contents = std::fs::read(&path).unwrap();
                contents[0..64].fill(0xFF);
                std::fs::write(&path, contents).unwrap();
            }

            let file = LogSegmentFile::open_existing(&dir, 1).await.unwrap();
            assert_eq!(file.metadata.borrow().log_id, 1);
            file.close().await;
        });
    }

    #[test]
    fn both_headers_corrupted_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let path = dir.join("log_1.wal");

            LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap().close().await;

            {
                let mut contents = std::fs::read(&path).unwrap();
                let len = contents.len();
                contents[0..64].fill(0xFF);
                contents[len - 64..].fill(0xFF);
                std::fs::write(&path, contents).unwrap();
            }

            let result = LogSegmentFile::open_existing(&dir, 1).await;
            assert!(matches!(result, Err(OpenOrCreateError::LogSegmentFileCorrupted { log_id: 1, .. })));
        });
    }

    #[test]
    fn close_is_idempotent() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let file = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap();

            file.close().await;
            file.close().await;
            file.close().await;
        });
    }

    #[test]
    fn rotate_carries_wal_state() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let file = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap();

            let tip: [u8; 32] = [0xAB; 32];
            {
                let mut meta = file.metadata.borrow_mut();
                meta.write.wal_index = 42;
                meta.write.tip_hash = tip;
            }

            let rotated = file.rotate(&dir, MIN_FILE_SIZE).await.unwrap();
            let meta = rotated.metadata.borrow();

            assert_eq!(meta.log_id, 2);
            assert_eq!(meta.write.wal_index, 42);
            assert_eq!(meta.write.tip_hash, tip);
            assert_eq!(meta.write.metablocks_position, HEADER_BLOCK_SIZE_BYTES as u64);
            assert!(meta.read.is_none());

            // old file stays open
            assert!(file.lock_reader("test").await.unwrap().is_some());

            file.close().await;
            rotated.close().await;
        });
    }

    fn make_zero_file(path: &std::path::Path, size: u64) {
        let f = std::fs::OpenOptions::new().create_new(true).write(true).open(path).unwrap();
        f.set_len(size).unwrap();
    }

    fn write_nonzero_at(path: &std::path::Path, offset: u64) {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        let mut buf = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        buf[0] = 0xAB;
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.write_all(&buf).unwrap();
    }

    #[test]
    fn is_zero_dual_header_orphan_classifies_all_layouts() {
        let (_tmp, dir) = test_dir();
        let front_off = 0;
        let rear_off = MIN_FILE_SIZE - HEADER_BLOCK_SIZE_BYTES as u64;

        // (size, writes, expected, label)
        let cases: &[(u64, &[u64], bool, &str)] = &[
            (MIN_FILE_SIZE, &[], true, "zero/zero is orphan"),
            (MIN_FILE_SIZE, &[front_off], false, "front nonzero is not orphan"),
            (MIN_FILE_SIZE, &[rear_off], false, "rear nonzero is not orphan"),
            (MIN_FILE_SIZE, &[front_off, rear_off], false, "both nonzero is not orphan"),
            (HEADER_BLOCK_SIZE_BYTES as u64, &[], true, "too-short is orphan"),
            (0, &[], true, "0-byte is orphan"),
        ];

        for (i, (size, writes, expected, label)) in cases.iter().enumerate() {
            let path = dir.join(format!("log_{i}.wal"));
            make_zero_file(&path, *size);
            for off in *writes {
                write_nonzero_at(&path, *off);
            }
            assert_eq!(is_zero_dual_header_orphan(&path).unwrap(), *expected, "{label}");
        }

        let missing = dir.join("does_not_exist.wal");
        assert_eq!(is_zero_dual_header_orphan(&missing).unwrap(), false, "missing file is not orphan");
    }

    #[test]
    fn rotate_handles_existing_target() {
        glommio_test!({
            // case 1: orphan at target is deleted, rotation succeeds
            {
                let (_tmp, dir) = test_dir();
                let file = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap();
                make_zero_file(&dir.join("log_2.wal"), MIN_FILE_SIZE);

                let rotated = file.rotate(&dir, MIN_FILE_SIZE).await.unwrap();
                assert_eq!(rotated.metadata.borrow().log_id, 2);
                file.close().await;
                rotated.close().await;
            }

            // case 2: target with non-zero header is refused, file preserved
            {
                let (_tmp, dir) = test_dir();
                let file = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap();
                let target = dir.join("log_2.wal");
                make_zero_file(&target, MIN_FILE_SIZE);
                write_nonzero_at(&target, 0);

                let result = file.rotate(&dir, MIN_FILE_SIZE).await;
                assert!(matches!(result, Err(OpenOrCreateError::RotationTargetUnsafe { log_id: 2, .. })));
                assert!(target.exists());
                file.close().await;
            }

            // case 3: inspection failure (target is a directory) is refused
            {
                let (_tmp, dir) = test_dir();
                let file = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap();
                let target = dir.join("log_2.wal");
                std::fs::create_dir_all(&target).unwrap();

                let result = file.rotate(&dir, MIN_FILE_SIZE).await;
                assert!(matches!(result, Err(OpenOrCreateError::RotationTargetUnsafe { log_id: 2, .. })));
                assert!(target.exists());
                file.close().await;
            }
        });
    }

    #[test]
    fn file_persists_across_opens() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            {
                let file = LogSegmentFile::open_or_create_first_file_for_shard(&dir, MIN_FILE_SIZE, 1, true).await.unwrap();
                file.metadata.borrow_mut().write.wal_index = 42;
                file.close().await;
            }

            let file = LogSegmentFile::open_existing(&dir, 1).await.unwrap();
            assert_eq!(file.metadata.borrow().write.wal_index, 0);
            file.close().await;
        });
    }
}
