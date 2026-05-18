use celeriant_disk::files::read_fixed_records_visit_const::{read_fixed_records_visit_const, ReadVisitError};
use celeriant_wal::{aggregate_key::AggregateKey, constants::HEADER_BLOCK_SIZE_BYTES};
use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;

use crate::errors::scan_error::ScanError;
use crate::log_segments_cache::LogSegmentsCache;

/// Scans metablocks in reverse order across all log files.
/// Starts from the active log and works backwards through older logs.
pub struct ReverseMetablockScanner<'a> {
    log_cache: &'a LogSegmentsCache,
    current_log_id: u64,
    chunk_size: u64,
    start_from_position: Option<u64>,
    /// Optional hash for bloom filter optimization.
    /// When set, log segments where the bloom filter says "definitely not present" are skipped.
    bloom_filter_hash: Option<[u8; 8]>,
    /// When true, scan up to the write cursor (uncommitted region included)
    use_write_cursor: bool,
}

impl<'a> ReverseMetablockScanner<'a> {
    pub fn new(log_cache: &'a LogSegmentsCache, starting_log_id: u64, start_from_position: Option<u64>, chunk_size: u64) -> Self {
        Self {
            log_cache,
            current_log_id: starting_log_id,
            chunk_size,
            start_from_position,
            bloom_filter_hash: None,
            use_write_cursor: false,
        }
    }

    /// Enable bloom filter optimization for a specific aggregate key.
    /// Log segments where the bloom filter says "definitely not present" will be skipped entirely.
    #[must_use]
    pub fn with_bloom_filter(mut self, aggregate_key: &AggregateKey) -> Self {
        self.bloom_filter_hash = Some(aggregate_key.hash_bytes());
        self
    }

    /// Enable bloom filter optimization using a pre-computed hash.
    /// Log segments where the bloom filter says "definitely not present" will be skipped entirely.
    #[must_use]
    pub fn with_bloom_filter_hash(mut self, hash_bytes: [u8; 8]) -> Self {
        self.bloom_filter_hash = Some(hash_bytes);
        self
    }

    #[must_use]
    pub fn with_write_cursor_upper_bound(mut self) -> Self {
        self.use_write_cursor = true;
        self
    }

    /// Scan metablocks in reverse, calling visitor for each 512-byte block.
    /// Visitor returns:
    /// - Ok(Some(result)) to stop and return the result
    /// - Ok(None) to continue scanning
    /// - Err(e) to abort with error
    pub async fn scan<T, E: std::fmt::Debug>(
        &mut self,
        mut visitor: impl FnMut(u64, u64, &[u8; FIXED_BLOCK_SIZE_BYTES]) -> Result<Option<T>, E>,
    ) -> Result<Option<T>, ScanError<E>> {
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

    async fn scan_single_log<T, E: std::fmt::Debug>(
        &self,
        visitor: &mut impl FnMut(u64, u64, &[u8; FIXED_BLOCK_SIZE_BYTES]) -> Result<Option<T>, E>,
        override_end: Option<u64>,
    ) -> Result<Option<T>, ScanError<E>> {
        let log_id = self.current_log_id;
        let log_segment_file = self.log_cache.get(log_id).await?;

        let metablock_position = {
            let metadata = log_segment_file.metadata.borrow();
            let (position, bloom) = if self.use_write_cursor {
                (
                    metadata.write.metablocks_position,
                    &metadata.write.aggregate_key_bloom,
                )
            } else {
                let read = match &metadata.read {
                    Some(r) => r,
                    None => return Ok(None),
                };
                (read.metablocks_position, &read.aggregate_key_bloom)
            };

            // Check bloom filter - skip entire log segment if key definitely not present
            if let Some(hash) = &self.bloom_filter_hash {
                if !bloom.may_contain_hash(hash) {
                    tracing::trace!(log_id, "Bloom filter skip");
                    return Ok(None);
                }
            }

            position
        };

        let guard = log_segment_file.lock_reader("scan_single_log").await?;
        let dma_file = guard.as_ref().ok_or(ScanError::NoFileHandle { log_id })?;
        let metablocks_start = HEADER_BLOCK_SIZE_BYTES as u64;
        let metablocks_end = override_end.unwrap_or(metablock_position).min(metablock_position);

        if metablocks_end <= metablocks_start {
            return Ok(None);
        }

        let mut found: Option<T> = None;

        let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, ScanError<E>>(
            dma_file,
            true,
            metablocks_start,
            metablocks_end,
            self.chunk_size,
            |pos, block| {
                match visitor(log_id, pos, block) {
                    Ok(Some(result)) => {
                        found = Some(result);
                        Ok(true)
                    }
                    Ok(None) => Ok(false),
                    Err(e) => Err(ScanError::Visitor(e)),
                }
            },
        )
        .await;

        match result {
            Ok(_) => Ok(found),
            Err(ReadVisitError::Io(source)) => {
                tracing::error!(log_id, error = %source, "DMA read failed during metablock scan");
                Err(ScanError::Io { log_id, source: source.to_string() })
            }
            Err(ReadVisitError::Visitor(scan_err)) => Err(scan_err),
        }
    }
}