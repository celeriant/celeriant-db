use std::path::PathBuf;

use celeriant_disk::files::open_dma_files::{existing_file_dma, open_or_create_file_dma};
use celeriant_wal::{constants::FIXED_BLOCK_SIZE_BYTES, shard_log_header::ShardLogHeader};
use celeriant_wire::version_aware_wire_format::{deserialize_versioned_shard_log_header, serialize_versioned_message};
use glommio::io::DmaFile;

use crate::rotating_log_error::RotatingLogError;

/// Represents a physical log file on disk, with its associated metadata.
pub struct ShardLogDmaFile {
    /// Active, open fd to the log file. Optional type allows to take
    /// ownership when closing the file or rotating the active log file.
    pub dma_file: Option<DmaFile>,
    /// One based incremented log id
    pub log_id: u64,
    /// Size of log file, typically fixed but could be smaller if log is being truncated
    pub file_len: u64,
    /// Header that contains positions where next writes can be placed. Stored at
    /// front and back of the file, taking 512 bytes each.
    pub shard_log_header: ShardLogHeader,
}

impl ShardLogDmaFile {

    /// Open a log file, if it doesn't exist, create it.
    /// Used for opening the active writing last log file.
    /// Errors if the file is corrupt and needs repair.
    pub async fn open_or_create(
        shard_dir: &PathBuf,
        preallocate_bytes: u64,
        log_id: u64,
    ) -> Result<Self, RotatingLogError> {
        std::fs::create_dir_all(&shard_dir)?;
        let log_path = shard_dir.join(log_file_name(log_id));
        let (mut dma_file, file_len, exists) = open_or_create_file_dma(&log_path, Some(preallocate_bytes)).await?;
        let shard_log_header = load_log_file(&mut dma_file, file_len, exists, log_id).await?;

        Ok(Self {
            dma_file: Some(dma_file),
            log_id,
            file_len,
            shard_log_header,
        })
    }

    /// Open an existing log file. Errors if it doesn't exist
    /// or the file is corrupt and needs repair.
    pub async fn open_existing(
        shard_dir: &PathBuf,
        log_id: u64,
    ) -> Result<Self, RotatingLogError> {
        let log_path = shard_dir.join(log_file_name(log_id));
        let (mut dma_file, file_len) = existing_file_dma(&log_path).await?;
        let shard_log_header = load_log_file(&mut dma_file, file_len, true, log_id).await?;

        Ok(Self {
            dma_file: Some(dma_file),
            log_id,
            file_len,
            shard_log_header,
        })
    }

    /// Creates a new log file with log_id+1, opens it and takes over this ShardLogDmaFile.
    /// The previous file is kept open and returned, so it can be placed in the reader file cache
    pub async fn rotate_to_next_log(&mut self, shard_dir: &PathBuf, preallocate_bytes: u64) -> Result<Self, RotatingLogError> {

        let old_dma_file = self.dma_file.take();
        let old_log_id = self.log_id;
        let old_file_len = self.file_len;
        let old_datablocks_position = self.shard_log_header.datablocks_position;
        let old_metablocks_position = self.shard_log_header.metablocks_position;

        self.log_id = self.log_id.saturating_add(1);

        let log_path = shard_dir.join(log_file_name(self.log_id));
        let (mut new_dma_file, active_log_len, exists) = open_or_create_file_dma(&log_path, Some(preallocate_bytes)).await?;
        
        self.shard_log_header = load_log_file(&mut new_dma_file, active_log_len, exists, self.log_id).await?;
        self.file_len = active_log_len;
        self.dma_file = Some(new_dma_file);

        Ok(Self {
            dma_file: old_dma_file,
            log_id: old_log_id,
            file_len: old_file_len,
            shard_log_header: ShardLogHeader {
                datablocks_position: old_datablocks_position,
                metablocks_position: old_metablocks_position,
            },
        })
    }

    /// After a successful write of new entries to the WAL, update the headers
    /// and perform a durable write using fdatasync. Ensures the shard_log_header
    /// stays up to date with the file contents.
    pub async fn write_new_headers_and_fsync(
        &mut self,
        datablocks_position: u64,
        metablocks_position: u64,
    ) -> Result<(), RotatingLogError> {
        let header = ShardLogHeader {
            metablocks_position,
            datablocks_position,
        };

        let mut dma_file = self.dma_file.as_mut()
            .ok_or(RotatingLogError::IoError("No file handle available to execute write_new_headers_and_fsync".to_string()))?;

        let header_end_start_pos = self.file_len.saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64);

        write_dual_shard_log_header(&mut dma_file, header_end_start_pos, &header).await?;

        dma_file.fdatasync().await?;

        // Success! Update our cached header in memory
        self.shard_log_header = header;

        Ok(())
    }
}

fn log_file_name(log_id: u64) -> String {
    format!("log_{log_id}.wal")
}

/// Either load an existing file or setup a new one that already has a preallocated size
async fn load_log_file(
    mut dma_file: &mut DmaFile,
    file_len: u64,
    existing_file: bool,
    log_id: u64,
) -> Result<ShardLogHeader, RotatingLogError> {
    if existing_file {
        load_header_detecting_corruption(dma_file, file_len, log_id).await
    } else {
        setup_new_file(&mut dma_file, file_len).await
    }
}

/// Tries to load the header. Will try the front of the file first, 
/// then the end of the file if the front is corrupted.
async fn load_header_detecting_corruption(
    dma_file: &mut DmaFile,
    file_len: u64,
    log_id: u64,
) -> Result<ShardLogHeader, RotatingLogError> {
    let header_bytes = dma_file.read_at(0, FIXED_BLOCK_SIZE_BYTES).await?;
    match deserialize_versioned_shard_log_header(&header_bytes) {
        Ok((shard_log_header, _version)) => Ok(shard_log_header),
        Err(_primary_err) => {
            // Primary header failed, try backup at end of file
            let header_bytes = dma_file
                .read_at(
                    file_len.saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64),
                    FIXED_BLOCK_SIZE_BYTES,
                )
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
async fn setup_new_file(
    dma_file: &mut DmaFile,
    file_len: u64,
) -> Result<ShardLogHeader, RotatingLogError> {
    let header = ShardLogHeader {
        metablocks_position: FIXED_BLOCK_SIZE_BYTES as u64,
        datablocks_position: file_len.saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64),
    };

    write_dual_shard_log_header(dma_file, header.datablocks_position, &header).await?;

    dma_file.fdatasync().await?;

    // Folder fsync - fsync the parent directory to ensure the file entry is durable
    let dir_path = dma_file.path()
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
async fn write_dual_shard_log_header(
    dma_file: &mut DmaFile,
    header_end_start_pos: u64,
    header: &ShardLogHeader,
) -> Result<(), RotatingLogError> {
    let mut header_bytes = dma_file.alloc_dma_buffer(FIXED_BLOCK_SIZE_BYTES);
    serialize_versioned_message(
        &header,
        celeriant_wal::shard_log_header::CURRENT_VERSION,
        header_bytes.as_bytes_mut(),
    )?;
    dma_file.write_at(header_bytes, 0).await?;

    let mut header_bytes = dma_file.alloc_dma_buffer(FIXED_BLOCK_SIZE_BYTES);
    serialize_versioned_message(
        &header,
        celeriant_wal::shard_log_header::CURRENT_VERSION,
        header_bytes.as_bytes_mut(),
    )?;
    dma_file
        .write_at(header_bytes, header_end_start_pos)
        .await?;

    Ok(())
}
