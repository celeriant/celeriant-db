use std::collections::HashMap;

use celeriant_disk::files::read_fixed_records_visit_const::{ReadVisitError, read_fixed_records_visit_const};
use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;
use celeriant_wal::{aggregate_key::AggregateKey, constants::HEADER_BLOCK_SIZE_BYTES};
use celeriant_wire::disk::metablock_bytes;

use crate::errors::scan_error::ScanError;
use crate::log_segments_cache::LogSegmentsCache;

/// Caller-supplied per-segment knowledge for chain scans
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentHint {
    /// The target chain has no member in this segment: skip it entirely.
    Skip,
    /// The target chain's newest member sits at this absolute position.
    SeekTo(u64),
}

/// Scans metablocks in reverse order across all log files.
/// Starts from the active log and works backwards through older logs.
pub struct ReverseMetablockScanner<'a> {
    log_cache: &'a LogSegmentsCache,
    current_log_id: u64,
    chunk_size: u64,
    start_from_position: Option<u64>,
    /// Optional hash for bloom filter optimization.
    /// When set, log segments where the bloom filter says "definitely not present" are skipped.
    bloom_filter_hash: Option<u64>,
    /// Optional client_id bloom hash for a NEGATIVE client-seq lookup. When set, a segment
    /// whose client bloom says "client definitely absent" is skipped
    client_bloom_filter_hash: Option<u64>,
    /// When true, scan up to the write cursor (uncommitted region included)
    use_write_cursor: bool,
    /// When set, follow per-aggregate backlinks to skip foreign metablocks instead
    /// of reading every block. The visitor is invoked only on this aggregate's chain.
    chain_aggregate_key: Option<AggregateKey>,
    /// Read-window size for backlink following. One block = read only the target's
    /// metablocks (minimal bytes, best when many aggregates are interleaved); larger
    /// batches consecutive in-window hops (fewer IOs when the chain is dense).
    chain_follow_window: u64,
    /// Optional per-segment hints for chain scans; segments absent from the map
    /// scan exactly as without hints. Composes with (runs after) the bloom checks.
    segment_hints: Option<&'a HashMap<u64, SegmentHint>>,
    /// Oldest log_id to visit (default 1). Callers driving a per-segment loop set
    /// it to the starting log_id so one scanner covers exactly one segment.
    min_log_id: u64,
}

impl<'a> ReverseMetablockScanner<'a> {
    pub fn new(log_cache: &'a LogSegmentsCache, starting_log_id: u64, start_from_position: Option<u64>, chunk_size: u64) -> Self {
        Self {
            log_cache,
            current_log_id: starting_log_id,
            chunk_size,
            start_from_position,
            bloom_filter_hash: None,
            client_bloom_filter_hash: None,
            use_write_cursor: false,
            chain_aggregate_key: None,
            chain_follow_window: FIXED_BLOCK_SIZE_BYTES as u64,
            segment_hints: None,
            min_log_id: 1,
        }
    }

    /// Stop after scanning down to `min_log_id` instead of log 1.
    #[must_use]
    pub fn with_min_log_id(mut self, min_log_id: u64) -> Self {
        self.min_log_id = min_log_id.max(1);
        self
    }

    /// Attach per-segment hints for chain scans (see [`SegmentHint`]).
    #[must_use]
    pub fn with_segment_hints(mut self, hints: &'a HashMap<u64, SegmentHint>) -> Self {
        self.segment_hints = Some(hints);
        self
    }

    #[must_use]
    pub fn with_aggregate_chain(mut self, aggregate_key: AggregateKey, follow_window_bytes: u64) -> Self {
        self.bloom_filter_hash = Some(aggregate_key.bloom_hash());
        self.chain_aggregate_key = Some(aggregate_key);
        let blocks = (follow_window_bytes / FIXED_BLOCK_SIZE_BYTES as u64).max(1);
        self.chain_follow_window = blocks * FIXED_BLOCK_SIZE_BYTES as u64;
        self
    }

    /// Enable bloom filter optimization for a specific aggregate key.
    /// Log segments where the bloom filter says "definitely not present" will be skipped entirely.
    #[must_use]
    pub fn with_bloom_filter(mut self, aggregate_key: &AggregateKey) -> Self {
        self.bloom_filter_hash = Some(aggregate_key.bloom_hash());
        self
    }

    /// Enable bloom filter optimization using a pre-computed hash.
    /// Log segments where the bloom filter says "definitely not present" will be skipped entirely.
    #[must_use]
    pub fn with_bloom_filter_hash(mut self, hash: u64) -> Self {
        self.bloom_filter_hash = Some(hash);
        self
    }

    /// Enable the client_id negative short-circuit: segments whose client bloom says the
    /// client is absent are skipped without reading their metablocks
    #[must_use]
    pub fn with_client_bloom_filter_hash(mut self, hash: u64) -> Self {
        self.client_bloom_filter_hash = Some(hash);
        self
    }

    #[must_use]
    pub fn with_write_cursor_upper_bound(mut self) -> Self {
        self.use_write_cursor = true;
        self
    }

    /// Scan metablocks in reverse, calling visitor for each FIXED_BLOCK_SIZE_BYTES block.
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

        let chain_key = self.chain_aggregate_key.clone();

        while self.current_log_id >= self.min_log_id {
            let result = match &chain_key {
                Some(key) => self.scan_chain_single_log(&mut visitor, override_end, key).await?,
                None => self.scan_single_log(&mut visitor, override_end).await?,
            };
            override_end = None; // Only applies to first log

            if let Some(found) = result {
                return Ok(Some(found));
            }

            if self.current_log_id == self.min_log_id {
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
            let (position, bloom, client_bloom) = if self.use_write_cursor {
                (metadata.write.metablocks_position, &metadata.write.aggregate_key_bloom, &metadata.write.client_id_bloom)
            } else {
                let read = match &metadata.read {
                    Some(r) => r,
                    None => return Ok(None),
                };
                (read.metablocks_position, &read.aggregate_key_bloom, &read.client_id_bloom)
            };

            // Check bloom filter - skip entire log segment if key definitely not present
            if let Some(hash) = self.bloom_filter_hash {
                metrics::counter!("celeriant_read_bloom_gate_total").increment(1);
                let bloom = bloom.borrow();
                // An absent bloom answers "maybe" for every key, so the segment is walked
                // for reasons the bloom never had a say in. Counted apart from a real hit.
                if bloom.is_absent() {
                    metrics::counter!("celeriant_read_bloom_absent_total").increment(1);
                } else if !bloom.may_contain_hash(hash) {
                    metrics::counter!("celeriant_read_bloom_short_circuit_total").increment(1);
                    tracing::trace!(log_id, "Bloom filter skip");
                    return Ok(None);
                }
            }

            // Negative client short-circuit: client absent here
            if let Some(hash) = self.client_bloom_filter_hash {
                if !client_bloom.borrow().may_contain_hash(hash) {
                    metrics::counter!("celeriant_read_client_bloom_short_circuit_total").increment(1);
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

        if self.bloom_filter_hash.is_some() {
            metrics::counter!("celeriant_read_segments_walked_total").increment(1);
        }

        let mut found: Option<T> = None;

        let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, ScanError<E>>(
            dma_file,
            true,
            metablocks_start,
            metablocks_end,
            self.chunk_size,
            |pos, block| match visitor(log_id, pos, block) {
                Ok(Some(result)) => {
                    found = Some(result);
                    Ok(true)
                }
                Ok(None) => Ok(false),
                Err(e) => Err(ScanError::Visitor(e)),
            },
        )
        .await;

        match result {
            Ok(_) => Ok(found),
            Err(ReadVisitError::Io(source)) => {
                tracing::error!(log_id, error = %source, "DMA read failed during metablock scan");
                Err(ScanError::Io {
                    log_id,
                    source: source.to_string(),
                })
            }
            Err(ReadVisitError::ShortRead { pos, requested, got }) => {
                tracing::error!(log_id, pos, requested, got, "short read during metablock scan");
                Err(ScanError::ShortRead { log_id, pos, requested, got })
            }
            Err(ReadVisitError::Visitor(scan_err)) => Err(scan_err),
        }
    }

    /// Chain-following scan of one segment: locate the aggregate's newest metablock
    /// at-or-below the upper bound, then walk its in-segment backlinks, skipping all
    /// foreign metablocks. The visitor is invoked only on this aggregate's chain.
    async fn scan_chain_single_log<T, E: std::fmt::Debug>(
        &self,
        visitor: &mut impl FnMut(u64, u64, &[u8; FIXED_BLOCK_SIZE_BYTES]) -> Result<Option<T>, E>,
        override_end: Option<u64>,
        key: &AggregateKey,
    ) -> Result<Option<T>, ScanError<E>> {
        let log_id = self.current_log_id;
        let log_segment_file = self.log_cache.get(log_id).await?;

        let metablock_position = {
            let metadata = log_segment_file.metadata.borrow();
            let (position, bloom, client_bloom) = if self.use_write_cursor {
                (metadata.write.metablocks_position, &metadata.write.aggregate_key_bloom, &metadata.write.client_id_bloom)
            } else {
                let read = match &metadata.read {
                    Some(r) => r,
                    None => return Ok(None),
                };
                (read.metablocks_position, &read.aggregate_key_bloom, &read.client_id_bloom)
            };

            if let Some(hash) = self.bloom_filter_hash {
                metrics::counter!("celeriant_read_bloom_gate_total").increment(1);
                let bloom = bloom.borrow();
                if bloom.is_absent() {
                    metrics::counter!("celeriant_read_bloom_absent_total").increment(1);
                } else if !bloom.may_contain_hash(hash) {
                    metrics::counter!("celeriant_read_bloom_short_circuit_total").increment(1);
                    return Ok(None);
                }
            }

            // Negative client short-circuit (see scan_single_log).
            if let Some(hash) = self.client_bloom_filter_hash {
                if !client_bloom.borrow().may_contain_hash(hash) {
                    metrics::counter!("celeriant_read_client_bloom_short_circuit_total").increment(1);
                    return Ok(None);
                }
            }
            position
        };

        let start = HEADER_BLOCK_SIZE_BYTES as u64;
        let end = override_end.unwrap_or(metablock_position).min(metablock_position);
        if end <= start {
            return Ok(None);
        }

        let hint = self.segment_hints.and_then(|h| h.get(&log_id)).copied();
        if hint == Some(SegmentHint::Skip) {
            metrics::counter!("celeriant_read_segment_hint_skip_total").increment(1);
            return Ok(None);
        }

        let guard = log_segment_file.lock_reader("scan_chain_single_log").await?;
        let dma_file = guard.as_ref().ok_or(ScanError::NoFileHandle { log_id })?;

        // Counted only once the read can actually happen, matching scan_single_log:
        // a lock timeout or a missing handle is not a walk in either function.
        if self.bloom_filter_hash.is_some() {
            metrics::counter!("celeriant_read_segments_walked_total").increment(1);
        }

        let window = self.chain_follow_window;
        let mut win_start = 0u64;
        let mut win: Option<glommio::io::ReadResult> = None;

        // Try the hinted seek target first: verify the block belongs to this chain
        // (a compaction may have moved blocks since the hint was computed — the
        // block then at-or-below the old tip position can only be foreign, never an
        // older member of the same chain, so a mismatch is detectable and safe).
        // The verifying read seeds the follow window below, costing no extra IO.
        let mut pos: Option<u64> = None;
        if let Some(SegmentHint::SeekTo(hint_pos)) = hint {
            if hint_pos >= start && hint_pos.checked_add(FIXED_BLOCK_SIZE_BYTES as u64).is_some_and(|e| e <= end) {
                let buf = dma_file.read_at(hint_pos, FIXED_BLOCK_SIZE_BYTES).await.map_err(|e| ScanError::Io {
                    log_id,
                    source: e.to_string(),
                })?;
                let block: &[u8; FIXED_BLOCK_SIZE_BYTES] = buf
                    .get(..FIXED_BLOCK_SIZE_BYTES)
                    .and_then(|b| b.try_into().ok())
                    .ok_or(ScanError::ShortRead {
                        log_id,
                        pos: hint_pos,
                        requested: FIXED_BLOCK_SIZE_BYTES,
                        got: buf.len(),
                    })?;
                if metablock_bytes::read_chain_aggregate_key(block).as_ref() == Some(key) {
                    metrics::counter!("celeriant_read_segment_hint_seek_total").increment(1);
                    win_start = hint_pos;
                    win = Some(buf);
                    pos = Some(hint_pos);
                }
            }
        }

        // No (usable) hint: locate the aggregate's newest metablock by reverse scan
        // (bloom may be a false positive, in which case there is none and we fall
        // through to older segments).
        let resolved = match pos {
            Some(p) => Some(p),
            None => self.find_first_chain_member(dma_file, log_id, start, end, key).await?,
        };
        let Some(mut pos) = resolved else {
            return Ok(None);
        };

        // Follow backlinks. A window covers in-range hops without re-reading (dense
        // chains); jumps beyond it read a fresh window, skipping interleaved foreign
        // metablocks. Window size trades bytes-read against IO count (see builder).
        loop {
            let covered = matches!(&win, Some(buf)
                if pos >= win_start && pos + FIXED_BLOCK_SIZE_BYTES as u64 <= win_start + buf.len() as u64);
            if !covered {
                let win_end = pos + FIXED_BLOCK_SIZE_BYTES as u64;
                win_start = win_end.saturating_sub(window).max(start);
                let len = (win_end - win_start) as usize;
                let buf = dma_file.read_at(win_start, len).await.map_err(|e| ScanError::Io {
                    log_id,
                    source: e.to_string(),
                })?;
                if buf.len() < len {
                    return Err(ScanError::ShortRead { log_id, pos: win_start, requested: len, got: buf.len() });
                }
                win = Some(buf);
            }
            let buf = win.as_ref().unwrap();
            let off = (pos - win_start) as usize;
            let block: &[u8; FIXED_BLOCK_SIZE_BYTES] = buf
                .get(off..off + FIXED_BLOCK_SIZE_BYTES)
                .and_then(|b| b.try_into().ok())
                .ok_or(ScanError::ShortRead {
                    log_id,
                    pos,
                    requested: FIXED_BLOCK_SIZE_BYTES,
                    got: buf.len().saturating_sub(off),
                })?;

            // Only this aggregate's metablocks carry a backlink we may follow.
            if metablock_bytes::read_chain_aggregate_key(block).as_ref() != Some(key) {
                break;
            }
            if let Some(result) = visitor(log_id, pos, block).map_err(ScanError::Visitor)? {
                return Ok(Some(result));
            }
            let backlink = metablock_bytes::read_previous_aggregate_metablock_pos(block);
            // 0 = no predecessor in this segment; guard against any out-of-range link.
            if backlink < start || backlink >= pos {
                break;
            }
            pos = backlink;
        }

        Ok(None)
    }

    /// Reverse-scan for the newest metablock belonging to `key` in [start, end).
    /// Returns its absolute position, or None if the segment holds none.
    async fn find_first_chain_member<E: std::fmt::Debug>(
        &self,
        dma_file: &glommio::io::DmaFile,
        log_id: u64,
        start: u64,
        end: u64,
        key: &AggregateKey,
    ) -> Result<Option<u64>, ScanError<E>> {
        let mut found: Option<u64> = None;
        let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, ()>(dma_file, true, start, end, self.chunk_size, |pos, block| {
            if metablock_bytes::read_chain_aggregate_key(block).as_ref() == Some(key) {
                found = Some(pos);
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .await;

        match result {
            Ok(_) => Ok(found),
            Err(ReadVisitError::Io(source)) => Err(ScanError::Io {
                log_id,
                source: source.to_string(),
            }),
            Err(ReadVisitError::ShortRead { pos, requested, got }) => {
                Err(ScanError::ShortRead { log_id, pos, requested, got })
            }
            Err(ReadVisitError::Visitor(())) => Ok(found),
        }
    }
}
#[cfg(test)]
mod tests {
    //! Tests for [`ReverseMetablockScanner`]. The `Harness` fabricates real log
    //! segments on disk: real `DmaFile`, real metablock encoding, nothing stubbed.

    use super::ReverseMetablockScanner;
    use std::collections::HashMap;
    use std::rc::Rc;

    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK};
    use celeriant_wal::metablocks::metablock::Metablock;
    use celeriant_wal::metablocks::metablock_kind::MetablockKind;
    use celeriant_wire::disk::metablock_bytes;
    use celeriant_wire::disk::versioned_block::serialize_versioned_message;
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::log_segments_cache::LogSegmentsCache;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    /// 6 header blocks, ~2 MiB of metablock space per segment. Preallocate must
    /// exceed 2 header blocks and be a multiple of one.
    const SEGMENT_BYTES: u64 = HEADER_BLOCK_SIZE_BYTES as u64 * 6;
    const CHUNK_SIZE: u64 = 32 * 1024;

    fn key(org: u128, atype: u128, id: u128) -> AggregateKey {
        AggregateKey::new(org, atype, id)
    }

    /// Aggregate key carried by a metablock.
    fn block_key(block: &[u8; FIXED_BLOCK_SIZE_BYTES]) -> AggregateKey {
        metablock_bytes::read_event_batch_aggregate_key(block)
    }

    /// Aggregate version carried by a metablock.
    fn block_version(block: &[u8; FIXED_BLOCK_SIZE_BYTES]) -> u64 {
        metablock_bytes::read_event_batch_aggregate_version(block)
    }

    /// Fabricates WAL log segments for scanner tests. Logical layer auto-maintains
    /// per-aggregate, per-segment backlinks; low-level layer writes raw blocks.
    struct Harness {
        _tmp: tempfile::TempDir,
        cache: LogSegmentsCache,
        /// Newest metablock position per aggregate within the *active* segment.
        last_pos: HashMap<AggregateKey, u64>,
        wal_seq: u64,
    }

    impl Harness {
        async fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path().join("shard");
            let cache = LogSegmentsCache::ready_up(dir, SEGMENT_BYTES, 64, 1).await.unwrap();
            Self {
                _tmp: tmp,
                cache,
                last_pos: HashMap::new(),
                wal_seq: 0,
            }
        }

        fn active_log_id(&self) -> u64 {
            self.cache.active_log_id()
        }

        /// Append one metablock for `key` at `version`, auto-linked to this aggregate's
        /// previous metablock in the active segment. Returns its absolute position.
        async fn write_version(&mut self, key: &AggregateKey, version: u64) -> u64 {
            let backlink = self.last_pos.get(key).copied().unwrap_or(0);
            let pos = self.write_raw(key, version, backlink).await;
            self.last_pos.insert(key.clone(), pos);
            pos
        }

        /// Append a metablock for a different aggregate (its own one-element chain).
        async fn write_foreign(&mut self, key: &AggregateKey) -> u64 {
            self.write_version(key, 0).await
        }

        /// Write a raw `EventBatchMetadata` block with an explicit backlink (0 = none).
        /// Bumps the write cursor and bloom; skips the logical link tracking.
        async fn write_raw(&mut self, key: &AggregateKey, version: u64, previous_aggregate_metablock_pos: u64) -> u64 {
            let active = self.cache.active();
            let pos = active.metadata.borrow().write.metablocks_position;
            self.wal_seq += 1;

            let mut mb = Metablock::default_inline_event_batch_metadata(key.clone());
            mb.wal_seq = self.wal_seq;
            mb.previous_aggregate_metablock_pos = previous_aggregate_metablock_pos;
            if let MetablockKind::EventBatchMetadata(ref mut eb) = mb.wal_metablock_type {
                eb.aggregate_version = version;
            }

            {
                let guard = active.lock_writer("harness").await.unwrap();
                let dma = guard.as_ref().unwrap();
                let mut buf = dma.alloc_dma_buffer(FIXED_BLOCK_SIZE_BYTES);
                serialize_versioned_message(&mb, WIRE_VERSION_WAL_METABLOCK, buf.as_bytes_mut()).unwrap();
                dma.write_rc_at(Rc::new(buf), pos).await.unwrap();
            }

            let mut meta = active.metadata.borrow_mut();
            meta.write.metablocks_position = pos + FIXED_BLOCK_SIZE_BYTES as u64;
            meta.write.aggregate_key_bloom.borrow_mut().insert(key);
            meta.write.wal_seq = self.wal_seq;
            pos
        }

        /// Force a hash into the active segment's write bloom.
        fn bloom_insert_hash(&self, hash: u64) {
            self.cache.active().metadata.borrow_mut().write.aggregate_key_bloom.borrow_mut().insert_hash(hash);
        }

        /// Start a new, higher segment. Chain links are per-segment, so tracking resets.
        async fn rotate(&mut self) {
            self.cache.rotate_to_next_log().await.unwrap();
            self.last_pos.clear();
        }

        /// Publish each segment's write cursor as its read cursor, so a default
        /// (read-cursor) scan observes everything written.
        async fn commit(&self) {
            for log_id in 1..=self.cache.active_log_id() {
                let f = self.cache.get(log_id).await.unwrap();
                let mut meta = f.metadata.borrow_mut();
                meta.read = Some(meta.write.clone());
            }
        }

        /// Publish the active segment's read cursor at the first `blocks` metablocks
        /// only, leaving the rest as an uncommitted write-cursor tail.
        async fn commit_blocks(&self, blocks: u64) {
            let f = self.cache.get(self.cache.active_log_id()).await.unwrap();
            let mut meta = f.metadata.borrow_mut();
            let mut read = meta.write.clone();
            read.metablocks_position = HEADER_BLOCK_SIZE_BYTES as u64 + blocks * FIXED_BLOCK_SIZE_BYTES as u64;
            meta.read = Some(read);
        }

        fn scanner(&self, start_from: Option<u64>) -> ReverseMetablockScanner<'_> {
            ReverseMetablockScanner::new(&self.cache, self.cache.active_log_id(), start_from, CHUNK_SIZE)
        }
    }

    /// Chain-scan for `target`, recording every visited (log_id, pos, version) in order.
    async fn collect_chain(h: &Harness, target: &AggregateKey, window: u64) -> Vec<(u64, u64, u64)> {
        let mut seen: Vec<(u64, u64, u64)> = Vec::new();
        h.scanner(None)
            .with_aggregate_chain(target.clone(), window)
            .scan::<(), ()>(|log_id, pos, block| {
                seen.push((log_id, pos, block_version(block)));
                Ok(None)
            })
            .await
            .unwrap();
        seen
    }

    /// Harness self-check: chain mode visits exactly the target's versions, newest
    /// first, skipping interleaved foreign metablocks.
    #[test]
    fn harness_smoke_chain_skips_foreign_newest_first() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            let foreign = key(1, 1, 999);

            h.write_version(&target, 1).await;
            for _ in 0..5 {
                h.write_foreign(&foreign).await;
            }
            h.write_version(&target, 2).await;
            for _ in 0..5 {
                h.write_foreign(&foreign).await;
            }
            h.write_version(&target, 3).await;
            h.commit().await;

            let seen = collect_chain(&h, &target, FIXED_BLOCK_SIZE_BYTES as u64).await;
            let versions: Vec<u64> = seen.iter().map(|(_, _, v)| *v).collect();
            assert_eq!(versions, vec![3, 2, 1], "must visit only target versions, newest first");
            for (_, pos, _) in &seen {
                let block = read_block(&h, h.active_log_id(), *pos).await;
                assert_eq!(block_key(&block), target, "no foreign block should be visited");
            }
        });
    }

    /// Read a single block back from disk.
    async fn read_block(h: &Harness, log_id: u64, pos: u64) -> [u8; FIXED_BLOCK_SIZE_BYTES] {
        let f = h.cache.get(log_id).await.unwrap();
        let guard = f.lock_reader("test").await.unwrap();
        let dma = guard.as_ref().unwrap();
        let buf = dma.read_at(pos, FIXED_BLOCK_SIZE_BYTES).await.unwrap();
        buf[..FIXED_BLOCK_SIZE_BYTES].try_into().unwrap()
    }

    /// Chain visited-set and order are identical for a one-block window vs. a large
    /// multi-block window.
    #[test]
    fn chain_visited_set_invariant_under_window_size() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            let foreign = key(1, 1, 999);

            for v in 1..=10 {
                h.write_version(&target, v).await;
                for _ in 0..3 {
                    h.write_foreign(&foreign).await;
                }
            }
            h.commit().await;

            let small = collect_chain(&h, &target, FIXED_BLOCK_SIZE_BYTES as u64).await;
            let large = collect_chain(&h, &target, FIXED_BLOCK_SIZE_BYTES as u64 * 4096).await;
            assert_eq!(small, large, "visited (log_id,pos,version) set and order must not depend on window");
            let versions: Vec<u64> = small.iter().map(|(_, _, v)| *v).collect();
            assert_eq!(versions, vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
        });
    }

    /// A chain spanning two segments is followed across the boundary, newest segment
    /// first.
    #[test]
    fn chain_followed_across_segment_boundary() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            let foreign = key(1, 1, 999);

            h.write_version(&target, 1).await;
            h.write_foreign(&foreign).await;
            h.write_version(&target, 2).await;
            let old_log = h.active_log_id();

            h.rotate().await;
            h.write_version(&target, 3).await;
            h.write_foreign(&foreign).await;
            h.write_version(&target, 4).await;
            let new_log = h.active_log_id();
            h.commit().await;

            let seen = collect_chain(&h, &target, FIXED_BLOCK_SIZE_BYTES as u64).await;
            let versions: Vec<u64> = seen.iter().map(|(_, _, v)| *v).collect();
            assert_eq!(versions, vec![4, 3, 2, 1], "chain continues into older segment");
            let logs: Vec<u64> = seen.iter().map(|(l, _, _)| *l).collect();
            assert_eq!(logs, vec![new_log, new_log, old_log, old_log], "newer segment exhausted first");
        });
    }

    /// Bloom "definitely not present" skips an entire segment; a false positive is
    /// scanned but yields no match.
    #[test]
    fn bloom_skips_absent_segment_but_scans_false_positive() {
        glommio_test!({
            let mut h = Harness::new().await;
            let present = key(2, 2, 2);
            let absent = key(3, 3, 3);

            // Segment 1: contains `present`; its bloom does NOT know `absent`.
            h.write_version(&present, 1).await;
            let seg1 = h.active_log_id();

            // Segment 2: a foreign block, but force `absent` into the bloom (false positive).
            h.rotate().await;
            let other = key(4, 4, 4);
            h.write_version(&other, 1).await;
            h.bloom_insert_hash(absent.bloom_hash());
            let seg2 = h.active_log_id();
            h.commit().await;

            // Scan for `absent`: seg2's bloom lies (scanned, no match), seg1's bloom is honest (skipped).
            let mut visited_logs: Vec<u64> = Vec::new();
            let found = h
                .scanner(None)
                .with_bloom_filter(&absent)
                .scan::<(), ()>(|log_id, _, _| {
                    visited_logs.push(log_id);
                    Ok(None)
                })
                .await
                .unwrap();
            assert!(found.is_none(), "no matching block exists");
            assert!(visited_logs.contains(&seg2), "false-positive segment must be scanned");
            assert!(!visited_logs.contains(&seg1), "honest negative segment must be skipped entirely");
        });
    }

    /// Visitor `Ok(Some(_))` stops at the first match; nothing further is visited.
    #[test]
    fn visitor_some_stops_immediately() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            for v in 1..=5 {
                h.write_version(&target, v).await;
            }
            h.commit().await;

            let mut count = 0;
            let stop = h
                .scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .scan::<u64, ()>(|_, _, block| {
                    count += 1;
                    Ok(Some(block_version(block)))
                })
                .await
                .unwrap();
            assert_eq!(stop, Some(5), "returns the newest version's value");
            assert_eq!(count, 1, "must not visit any block past the stop");
        });
    }

    /// Visitor `Err(_)` aborts the scan and propagates the error.
    #[test]
    fn visitor_err_aborts() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            h.write_version(&target, 1).await;
            h.write_version(&target, 2).await;
            h.commit().await;

            let result = h
                .scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .scan::<(), &'static str>(|_, _, _| Err("boom"))
                .await;
            assert!(result.is_err(), "visitor error must propagate as ScanError");
        });
    }

    /// `start_from_position` bounds only the first (active) segment; older segments
    /// scan in full.
    #[test]
    fn start_from_position_bounds_only_first_segment() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);

            h.write_version(&target, 1).await;
            h.write_version(&target, 2).await;
            h.rotate().await;
            let p3 = h.write_version(&target, 3).await;
            h.write_version(&target, 4).await;
            h.commit().await;

            // Bound just below v4: excluded in the active segment, but v2/v1 in the older one scan.
            let mut versions: Vec<u64> = Vec::new();
            h.scanner(Some(p3 + FIXED_BLOCK_SIZE_BYTES as u64))
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .scan::<(), ()>(|_, _, block| {
                    versions.push(block_version(block));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(versions, vec![3, 2, 1], "bound excludes v4 in active segment only");
        });
    }

    /// The uncommitted write tail is visible only with `with_write_cursor_upper_bound()`.
    #[test]
    fn write_cursor_reveals_uncommitted_tail() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            for v in 1..=5 {
                h.write_version(&target, v).await;
            }
            // Commit only the first 3 blocks; v4/v5 remain an uncommitted tail.
            h.commit_blocks(3).await;

            let mut read_versions: Vec<u64> = Vec::new();
            h.scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .scan::<(), ()>(|_, _, block| {
                    read_versions.push(block_version(block));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(read_versions, vec![3, 2, 1], "read-cursor scan stops at the committed prefix");

            let mut write_versions: Vec<u64> = Vec::new();
            h.scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .with_write_cursor_upper_bound()
                .scan::<(), ()>(|_, _, block| {
                    write_versions.push(block_version(block));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(write_versions, vec![5, 4, 3, 2, 1], "write-cursor scan includes the uncommitted tail");
        });
    }

    /// A segment with no read cursor contributes nothing on a default scan.
    #[test]
    fn no_read_cursor_segment_yields_nothing() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            for v in 1..=3 {
                h.write_version(&target, v).await;
            }
            // Deliberately do NOT commit: read cursor stays absent.

            let seen = collect_chain(&h, &target, FIXED_BLOCK_SIZE_BYTES as u64).await;
            assert!(seen.is_empty(), "no read cursor, nothing visited");
        });
    }

    /// Non-chain scan visits every metablock, foreign included, newest-first across a
    /// segment boundary. Covers the plain scan path.
    #[test]
    fn plain_scan_visits_all_blocks_newest_first() {
        glommio_test!({
            let mut h = Harness::new().await;
            let a = key(1, 1, 1);
            let b = key(2, 2, 2);

            let p1 = h.write_version(&a, 1).await;
            let p2 = h.write_foreign(&b).await;
            let old_log = h.active_log_id();
            h.rotate().await;
            let p3 = h.write_version(&a, 2).await;
            let p4 = h.write_foreign(&b).await;
            let new_log = h.active_log_id();
            h.commit().await;

            let mut seen: Vec<(u64, u64)> = Vec::new();
            h.scanner(None)
                .scan::<(), ()>(|log_id, pos, _| {
                    seen.push((log_id, pos));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(
                seen,
                vec![(new_log, p4), (new_log, p3), (old_log, p2), (old_log, p1)],
                "plain scan visits all blocks newest-first across segments",
            );
        });
    }

    /// Chain walk halts on an out-of-range backlink instead of dereferencing it; the
    /// target's newest block is still visited, the unreachable predecessor is not.
    #[test]
    fn chain_stops_at_out_of_range_backlink() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);

            h.write_version(&target, 1).await;
            h.write_raw(&target, 2, 7).await; // backlink 7 < metablocks start
            h.commit().await;

            let seen = collect_chain(&h, &target, FIXED_BLOCK_SIZE_BYTES as u64).await;
            let versions: Vec<u64> = seen.iter().map(|(_, _, v)| *v).collect();
            assert_eq!(versions, vec![2], "out-of-range backlink stops the walk; v1 not reached");
        });
    }

    // ── Segment hints (chain scans) ──

    use super::SegmentHint;

    /// A `Skip` hint suppresses the segment entirely — the segment's chain
    /// members are not visited, older segments still are.
    #[test]
    fn hint_skip_suppresses_segment_but_not_older_ones() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);

            h.write_version(&target, 1).await;
            let old_log = h.active_log_id();
            h.rotate().await;
            h.write_version(&target, 2).await;
            let new_log = h.active_log_id();
            h.commit().await;

            let hints = HashMap::from([(new_log, SegmentHint::Skip)]);
            let mut seen: Vec<(u64, u64)> = Vec::new();
            h.scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .with_segment_hints(&hints)
                .scan::<(), ()>(|log_id, _, block| {
                    seen.push((log_id, block_version(block)));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(seen, vec![(old_log, 1)], "hinted segment skipped, older segment still walked");
        });
    }

    /// A valid `SeekTo` hint starts the chain walk at the hinted block: the
    /// find-first reverse hunt over newer foreign blocks never runs, and the
    /// visited set equals the unhinted walk's.
    #[test]
    fn hint_seek_to_visits_same_chain_as_full_walk() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            let foreign = key(1, 1, 999);

            h.write_version(&target, 1).await;
            for _ in 0..5 {
                h.write_foreign(&foreign).await;
            }
            let tip = h.write_version(&target, 2).await;
            for _ in 0..5 {
                h.write_foreign(&foreign).await;
            }
            h.commit().await;

            let unhinted = collect_chain(&h, &target, FIXED_BLOCK_SIZE_BYTES as u64).await;

            let hints = HashMap::from([(h.active_log_id(), SegmentHint::SeekTo(tip))]);
            let mut hinted: Vec<(u64, u64, u64)> = Vec::new();
            h.scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .with_segment_hints(&hints)
                .scan::<(), ()>(|log_id, pos, block| {
                    hinted.push((log_id, pos, block_version(block)));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(hinted, unhinted, "seek hint must visit exactly the chain the full walk visits");
        });
    }

    /// A stale `SeekTo` pointing at a foreign block (compaction moved blocks)
    /// must fall back to the full reverse hunt — never silently miss the chain.
    #[test]
    fn hint_seek_to_foreign_block_falls_back_to_full_hunt() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            let foreign = key(1, 1, 999);

            h.write_version(&target, 1).await;
            let foreign_pos = h.write_foreign(&foreign).await;
            h.write_version(&target, 2).await;
            h.commit().await;

            let hints = HashMap::from([(h.active_log_id(), SegmentHint::SeekTo(foreign_pos))]);
            let mut versions: Vec<u64> = Vec::new();
            h.scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .with_segment_hints(&hints)
                .scan::<(), ()>(|_, _, block| {
                    versions.push(block_version(block));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(versions, vec![2, 1], "stale hint must degrade to the full hunt, not a miss");
        });
    }

    /// An out-of-range `SeekTo` (past the readable end / below the region start)
    /// is ignored: full hunt as today.
    #[test]
    fn hint_seek_to_out_of_range_falls_back() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            h.write_version(&target, 1).await;
            h.commit().await;

            for bad in [7u64, u64::MAX - FIXED_BLOCK_SIZE_BYTES as u64] {
                let hints = HashMap::from([(h.active_log_id(), SegmentHint::SeekTo(bad))]);
                let mut versions: Vec<u64> = Vec::new();
                h.scanner(None)
                    .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                    .with_segment_hints(&hints)
                    .scan::<(), ()>(|_, _, block| {
                        versions.push(block_version(block));
                        Ok(None)
                    })
                    .await
                    .unwrap();
                assert_eq!(versions, vec![1], "out-of-range hint {bad} must not break the walk");
            }
        });
    }

    /// Segments without a hint entry scan exactly as before.
    #[test]
    fn absent_hint_entry_scans_normally() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            h.write_version(&target, 1).await;
            h.write_version(&target, 2).await;
            h.commit().await;

            let hints: HashMap<u64, SegmentHint> = HashMap::new();
            let mut versions: Vec<u64> = Vec::new();
            h.scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .with_segment_hints(&hints)
                .scan::<(), ()>(|_, _, block| {
                    versions.push(block_version(block));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(versions, vec![2, 1]);
        });
    }

    /// A scanner bounded to its starting segment must not descend into older
    /// segments (the per-segment dedup consult drives one scanner per segment).
    #[test]
    fn min_log_id_bounds_the_scan_to_one_segment() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            h.write_version(&target, 1).await;
            h.rotate().await;
            h.write_version(&target, 2).await;
            h.commit().await;

            let top = h.active_log_id();
            let mut seen: Vec<(u64, u64)> = Vec::new();
            h.scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .with_min_log_id(top)
                .scan::<(), ()>(|log_id, _, block| {
                    seen.push((log_id, block_version(block)));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(seen, vec![(top, 2)], "the bounded scan must stop at its own segment");
        });
    }

    /// Chain walk halts when a backlink points at a foreign aggregate's block: the
    /// foreign block is never visited.
    #[test]
    fn chain_stops_when_backlink_points_to_foreign() {
        glommio_test!({
            let mut h = Harness::new().await;
            let target = key(1, 1, 1);
            let foreign = key(2, 2, 2);

            let foreign_pos = h.write_foreign(&foreign).await;
            h.write_raw(&target, 5, foreign_pos).await; // backlink into the foreign block
            h.commit().await;

            let mut seen: Vec<(AggregateKey, u64)> = Vec::new();
            h.scanner(None)
                .with_aggregate_chain(target.clone(), FIXED_BLOCK_SIZE_BYTES as u64)
                .scan::<(), ()>(|_, _, block| {
                    seen.push((block_key(block), block_version(block)));
                    Ok(None)
                })
                .await
                .unwrap();
            assert_eq!(seen, vec![(target.clone(), 5)], "walk stops at foreign backlink; foreign not visited");
        });
    }
}
