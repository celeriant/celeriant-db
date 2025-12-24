use std::rc::Rc;

use celeriant_disk::files::read_fixed_records_visit_const::read_fixed_records_visit_const;
use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;
use glommio::sync::RwLock;

use crate::{rotating_log_cache::RotatingLogCache, rotating_log_error::RotatingLogError, shard_log_dma_file::ShardLogDmaFile};

/// Scans metablocks in reverse order across all log files.
/// Starts from the active log and works backwards through older logs.
pub struct ReverseMetablockScanner<'a> {
    log_cache: &'a RotatingLogCache,
    current_log_id: u64,
    chunk_size: u64,
}

impl<'a> ReverseMetablockScanner<'a> {
    pub fn new(log_cache: &'a RotatingLogCache, starting_log_id: u64, chunk_size: u64) -> Self {
        Self {
            log_cache,
            current_log_id: starting_log_id,
            chunk_size,
        }
    }

    /// Scan metablocks in reverse, calling visitor for each 512-byte block.
    /// Visitor returns:
    /// - Ok(Some(result)) to stop and return the result
    /// - Ok(None) to continue scanning
    /// - Err(e) to abort with error
    pub async fn scan<T, E>(
        &mut self,
        mut visitor: impl FnMut(&[u8; FIXED_BLOCK_SIZE_BYTES]) -> Result<Option<T>, E>,
    ) -> Result<Option<T>, RotatingLogError>
    where
        E: std::fmt::Debug,
    {
        while self.current_log_id >= 1 {
            let result = self.scan_single_log(&mut visitor).await?;
            
            if let Some(found) = result {
                return Ok(Some(found));
            }

            if self.current_log_id == 1 {
                break;
            }
            self.current_log_id -= 1;
        }

        Ok(None)
    }

    async fn scan_single_log<T, E>(
        &self,
        visitor: &mut impl FnMut(&[u8; FIXED_BLOCK_SIZE_BYTES]) -> Result<Option<T>, E>,
    ) -> Result<Option<T>, RotatingLogError>
    where
        E: std::fmt::Debug,
    {
        let file_rc: Rc<RwLock<ShardLogDmaFile>> = self.log_cache.get(self.current_log_id).await?;
        let guard = file_rc.read().await?;

        let dma_file = guard.dma_file.as_ref()
            .ok_or_else(|| RotatingLogError::IoError("No file handle".to_string()))?;

        let metablocks_start = FIXED_BLOCK_SIZE_BYTES as u64;
        let metablocks_end = guard.shard_log_header.metablocks_position;

        if metablocks_end <= metablocks_start {
            return Ok(None); // No metablocks in this log
        }

        let mut found: Option<T> = None;

        read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, RotatingLogError>(
            dma_file,
            true, // reverse
            metablocks_start,
            metablocks_end,
            self.chunk_size,
            |block| {
                match visitor(block) {
                    Ok(Some(result)) => {
                        found = Some(result);
                        Ok(true) // stop scanning
                    }
                    Ok(None) => Ok(false), // continue
                    Err(_) => Err(RotatingLogError::IoError("Visitor error".to_string())),
                }
            },
        )
        .await
        .map_err(|e| RotatingLogError::IoError(format!("{:?}", e)))?;

        Ok(found)
    }
}