use std::{cell::RefCell, path::PathBuf, rc::Rc};

use celeriant_disk::files::open_dma_files::{existing_file_dma, open_or_create_file_dma};
use celeriant_wal::{
    constants::{AGGREGATE_BLOOM_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_SHARD_LOG_HEADER},
    shard_log_header::ShardLogHeader,
};
use celeriant_wire::version_aware_wire_format::{deserialize_versioned_shard_log_header, serialize_versioned_message};
use glommio::{GlommioError, io::DmaFile, sync::{RwLock, RwLockReadGuard, RwLockWriteGuard}};

use crate::{log_segment_file_metadata::LogSegmentFileMetadata, rotating_log_error::RotatingLogError, rwlock_timeout::{LockTimeoutError, read_with_timeout, write_with_timeout}};

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

    pub async fn close(&self) -> Result<(), RotatingLogError> {

        Ok(())
    }

    /// Open a log file, if it doesn't exist, create it.
    /// Used for opening the active writing last log file.
    /// Errors if the file is corrupt and needs repair.
    pub async fn open_or_create(shard_dir: &PathBuf, preallocate_bytes: u64, log_id: u64) -> Result<Self, RotatingLogError> {
        let log_path = shard_dir.join(log_file_name(log_id));
        let (mut writer, file_len, exists) = open_or_create_file_dma(&log_path, Some(preallocate_bytes)).await?;
        let reader = writer.dup()?;
        let shard_log_header = load_log_file(&mut writer, file_len, exists, log_id).await?;
        
        let datablocks_carry_over = read_datablocks_carry_over_bytes(&reader, shard_log_header.datablocks_position).await?;

        Ok(Self {
            writer: RwLock::new(Some(Rc::new(writer))),
            reader: RwLock::new(Some(Rc::new(reader))),
            metadata: RefCell::new(LogSegmentFileMetadata::new(log_id, file_len, datablocks_carry_over, &shard_log_header)),
        })
    }

    /// Open an existing log file. Errors if it doesn't exist
    /// or the file is corrupt and needs repair.
    pub async fn open_existing(shard_dir: &PathBuf, log_id: u64) -> Result<Self, RotatingLogError> {
        let log_path = shard_dir.join(log_file_name(log_id));
        let (mut writer, file_len) = existing_file_dma(&log_path).await?;
        let reader = writer.dup()?;
        let shard_log_header = load_log_file(&mut writer, file_len, true, log_id).await?;
        
        let datablocks_carry_over = read_datablocks_carry_over_bytes(&reader, shard_log_header.datablocks_position).await?;

        Ok(Self {
            writer: RwLock::new(Some(Rc::new(writer))),
            reader: RwLock::new(Some(Rc::new(reader))),
            metadata: RefCell::new(LogSegmentFileMetadata::new(log_id, file_len, datablocks_carry_over, &shard_log_header)),
        })
    }
}

fn log_file_name(log_id: u64) -> String {
    format!("log_{log_id}.wal")
}

/// Either load an existing file or setup a new one that already has a preallocated size
async fn load_log_file(mut dma_file: &mut DmaFile, file_len: u64, existing_file: bool, log_id: u64) -> Result<ShardLogHeader, RotatingLogError> {
    if existing_file {
        load_header_detecting_corruption(dma_file, file_len, log_id).await
    } else {
        setup_new_file(&mut dma_file, file_len).await
    }
}

/// Tries to load the header. Will try the front of the file first,
/// then the end of the file if the front is corrupted.
async fn load_header_detecting_corruption(dma_file: &mut DmaFile, file_len: u64, log_id: u64) -> Result<ShardLogHeader, RotatingLogError> {
    let header_bytes = dma_file.read_at(0, HEADER_BLOCK_SIZE_BYTES).await?;
    match deserialize_versioned_shard_log_header(&header_bytes) {
        Ok((shard_log_header, _version)) => Ok(shard_log_header),
        Err(_primary_err) => {
            // Primary header failed, try backup at end of file
            let header_bytes = dma_file
                .read_at(file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64), HEADER_BLOCK_SIZE_BYTES)
                .await?;
            match deserialize_versioned_shard_log_header(&header_bytes) {
                Ok((shard_log_header, _version)) => Ok(shard_log_header),
                Err(_backup_err) => Err(RotatingLogError::HeaderCorrupted { log_id: Some(log_id) }),
            }
        }
    }
}

/// Setup a new file, writing the header to the start and end of the file
/// Assumes the file already is preallocated to file_len. Will fsync the
/// file and fsync the parent directory.
async fn setup_new_file(dma_file: &mut DmaFile, file_len: u64) -> Result<ShardLogHeader, RotatingLogError> {
    let header = ShardLogHeader {
        metablocks_position: HEADER_BLOCK_SIZE_BYTES as u64,
        datablocks_position: file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64),
        wal_index: 0,
        aggregate_bloom: vec![0u64; AGGREGATE_BLOOM_BYTES / 8],
    };

    write_dual_shard_log_header(dma_file, header.datablocks_position, &header).await?;

    dma_file.fdatasync().await?;

    // Folder fsync - fsync the parent directory to ensure the file entry is durable
    let dir_path = dma_file
        .path()
        .ok_or_else(|| RotatingLogError::IoError("Cannot get dma file path".to_string()))?
        .parent()
        .ok_or_else(|| RotatingLogError::IoError("Cannot get dma file path".to_string()))?
        .to_path_buf();

    let dir = glommio::io::Directory::open(dir_path).await?;
    dir.sync().await?;
    dir.close().await?;

    Ok(header)
}

/// Writes the provided header to both start and end header positions
pub async fn write_dual_shard_log_header(dma_file: &DmaFile, header_end_start_pos: u64, header: &ShardLogHeader) -> Result<(), RotatingLogError> {
    let mut header_bytes = dma_file.alloc_dma_buffer(HEADER_BLOCK_SIZE_BYTES);
    serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, header_bytes.as_bytes_mut())?;
    dma_file.write_at(header_bytes, 0).await?;

    let mut header_bytes = dma_file.alloc_dma_buffer(HEADER_BLOCK_SIZE_BYTES);
    serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, header_bytes.as_bytes_mut())?;
    dma_file.write_at(header_bytes, header_end_start_pos).await?;

    Ok(())
}

async fn read_datablocks_carry_over_bytes(dma_file: &DmaFile, datablocks_position: u64) -> Result<Option<Vec<u8>>, GlommioError<()>> {
    let datablocks_carry_over_size = dma_file.align_up(datablocks_position).saturating_sub(datablocks_position);

    if datablocks_carry_over_size > 0 {
        return Ok(Some(
            dma_file.read_at(datablocks_position, datablocks_carry_over_size as usize).await?.to_vec(),
        ));
    } else {
        return Ok(None);
    }
}