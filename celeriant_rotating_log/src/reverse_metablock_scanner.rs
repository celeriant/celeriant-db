use celeriant_disk::files::read_fixed_records_visit_const::read_fixed_records_visit_const;
use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;

use crate::{rotating_log_cache::RotatingLogCache, rotating_log_error::RotatingLogError, rwlock_timeout::read_with_timeout};

/// Scans metablocks in reverse order across all log files.
/// Starts from the active log and works backwards through older logs.
pub struct ReverseMetablockScanner<'a> {
    log_cache: &'a RotatingLogCache,
    current_log_id: u64,
    chunk_size: u64,
    start_from_position: Option<u64>,
}

impl<'a> ReverseMetablockScanner<'a> {
    pub fn new(log_cache: &'a RotatingLogCache, starting_log_id: u64, start_from_position: Option<u64>, chunk_size: u64) -> Self {
        Self {
            log_cache,
            current_log_id: starting_log_id,
            chunk_size,
            start_from_position
        }
    }

    /// Scan metablocks in reverse, calling visitor for each 512-byte block.
    /// Visitor returns:
    /// - Ok(Some(result)) to stop and return the result
    /// - Ok(None) to continue scanning
    /// - Err(e) to abort with error
    pub async fn scan<T, E>(
        &mut self,
        mut visitor: impl FnMut(u64, u64, &[u8; FIXED_BLOCK_SIZE_BYTES]) -> Result<Option<T>, E>,
    ) -> Result<Option<T>, RotatingLogError>
    where
        E: std::fmt::Debug,
    {
        // Use start_from_position only for the first log, then clear it
        let mut override_end = self.start_from_position.take();

        while self.current_log_id >= 1 {
            let result = self.scan_single_log(&mut visitor, override_end).await?;
            override_end = None; // Only applies to first log
            
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
        visitor: &mut impl FnMut(u64, u64, &[u8; FIXED_BLOCK_SIZE_BYTES]) -> Result<Option<T>, E>,
        override_end: Option<u64>,
    ) -> Result<Option<T>, RotatingLogError>
    where
        E: std::fmt::Debug,
    {
        let file_rc = self.log_cache.get(self.current_log_id).await?;
        let guard = read_with_timeout(&file_rc, "scan_single_log").await?;

        let dma_file = guard.dma_file.as_ref()
        .ok_or_else(|| RotatingLogError::IoError("No file handle".to_string()))?;
    
        let metablocks_start = FIXED_BLOCK_SIZE_BYTES as u64;
        let metablocks_end = override_end
            .unwrap_or(guard.shard_log_header.metablocks_position)
            .min(guard.shard_log_header.metablocks_position);

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
            |pos, block| {
                match visitor(self.current_log_id, pos, block) {
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