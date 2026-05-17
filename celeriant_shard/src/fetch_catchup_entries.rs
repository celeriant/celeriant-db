use std::rc::Rc;

use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_rotating_log::reverse_metablock_scanner::ReverseMetablockScanner;
use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::disk::disk_format_error::DiskFormatError;
use celeriant_wire::disk::metablock_bytes;
use celeriant_wire::disk::versioned_block::deserialise_metablock;

use crate::collect_from_disk::{EventBatchFromLogSegmentFile, fetch_datablocks_for_metablocks};
use crate::error::fetch_catchup_entries_error::FetchCatchupEntriesError;

/// Reverse-scan the WAL for metablocks that are to be sent to the follower
pub(crate) async fn fetch_catchup_entries(
    log_segments_cache: &Rc<LogSegmentsCache>,
    follower_wal_seq: u64,
    leader_wal_seq: u64,
    max_size_bytes: Option<u64>,
    read_max_chunk_size: u64,
    dict_codec: &DictCodec,
) -> Result<Vec<EventBatchFromLogSegmentFile>, FetchCatchupEntriesError> {
    let current_log_id = log_segments_cache.active_log_id();
    let mut scanner = ReverseMetablockScanner::new(log_segments_cache, current_log_id, None, read_max_chunk_size);

    let mut replication_items: Vec<EventBatchFromLogSegmentFile> = vec![];
    let mut accumulated_size = 0u64;

    let _scan_result = scanner
        .scan(|log_id, _pos, bytes| {
            let wal_seq = metablock_bytes::read_wal_seq(bytes);

            // Stop if we've gone too far back
            if wal_seq <= follower_wal_seq {
                return Ok(Some(()));
            }

            // Include if in range
            if wal_seq < leader_wal_seq {
                let metablock = deserialise_metablock(bytes)?;

                // Estimate size (metablock + potential datablock)
                let size_estimate = metablock.uncompressed_size.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64);
                accumulated_size += size_estimate;

                replication_items.push(EventBatchFromLogSegmentFile {
                    log_id,
                    metablock,
                    datablock: None,
                });

                // Stop if size limit exceeded
                if max_size_bytes.is_some_and(|cap| accumulated_size > cap) {
                    return Ok(Some(()));
                }
            }

            Ok::<Option<()>, DiskFormatError>(None)
        })
        .await
        .map_err(FetchCatchupEntriesError::MetablockDiscoveryError)?;

    // If nothing collected, return empty
    // This is still valid because the leader no longer has the entries
    // So the follower doesn't need them either (hard deleted)
    if replication_items.is_empty() {
        return Ok(vec![]);
    }

    if max_size_bytes.is_some_and(|cap| accumulated_size > cap) {
        return Err(FetchCatchupEntriesError::FollowerTooFarBehind);
    }

    // Reverse to get chronological order
    replication_items.reverse();

    fetch_datablocks_for_metablocks(&mut replication_items, read_max_chunk_size, log_segments_cache, dict_codec)
        .await
        .map_err(FetchCatchupEntriesError::FetchDatablockError)?;

    Ok(replication_items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::rc::Rc;

    use glommio::{LocalExecutorBuilder, Placement};

    use celeriant_rotating_log::log_segment_file::log_segment_cursor::LogSegmentCursor;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK};
    use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
    use celeriant_wal::metablocks::metablock::Metablock;
    use celeriant_wire::disk::versioned_block::serialize_versioned_message;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    const FILE_SIZE: u64 = 4 * 1024 * 1024;

    fn test_dir() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shard");
        (tmp, dir)
    }

    /// Build a metablock with `DatablockStorageKind::None` so the datablock-fetch step is a
    /// no-op. `uncompressed_size` controls how much budget the entry consumes during the
    /// budget-exhaustion test.
    fn metablock_at(wal_seq: u64, uncompressed_size: u64) -> Metablock {
        let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::default());
        mb.wal_seq = wal_seq;
        mb.uncompressed_size = uncompressed_size;
        mb.datablock = DatablockStorageKind::None;
        mb
    }

    /// Write `metablocks` consecutively to the active segment starting at the metablocks
    /// region offset, then advance the read cursor so the scanner sees them as committed.
    /// Tests need real bytes on disk because `ReverseMetablockScanner` reads via DMA.
    async fn write_metablocks_and_set_read_cursor(
        lsc: &Rc<LogSegmentsCache>,
        metablocks: &[Metablock],
    ) {
        let active = lsc.active();
        let guard = active.lock_writer("test_write_metablocks").await.unwrap();
        let dma = guard.as_ref().unwrap();

        // DMA writes must be alignment-sized. Pad up to nearest alignment.
        let alignment = dma.alignment() as usize;
        let metablocks_bytes = metablocks.len() * FIXED_BLOCK_SIZE_BYTES;
        let padded = ((metablocks_bytes + alignment - 1) / alignment) * alignment;
        let mut buffer = dma.alloc_dma_buffer(padded);
        let slice = buffer.as_bytes_mut();

        for (i, mb) in metablocks.iter().enumerate() {
            let mut block = [0u8; FIXED_BLOCK_SIZE_BYTES];
            serialize_versioned_message(mb, WIRE_VERSION_WAL_METABLOCK, &mut block).unwrap();
            let start = i * FIXED_BLOCK_SIZE_BYTES;
            slice[start..start + FIXED_BLOCK_SIZE_BYTES].copy_from_slice(&block);
        }

        dma.write_at(buffer, HEADER_BLOCK_SIZE_BYTES as u64).await.unwrap();
        dma.fdatasync().await.unwrap();
        drop(guard);

        // Advance the read cursor so the scanner sees these metablocks. The scanner uses
        // `read.metablocks_position` as its upper bound; without `read = Some(..)` it skips
        // the segment entirely.
        let metablocks_end = HEADER_BLOCK_SIZE_BYTES as u64 + metablocks_bytes as u64;
        let last_wal = metablocks.last().map(|m| m.wal_seq).unwrap_or(0);
        let cursor = LogSegmentCursor {
            log_id: lsc.active_log_id(),
            metablocks_position: metablocks_end,
            datablocks_position: FILE_SIZE.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64),
            wal_seq: last_wal,
            aggregate_key_bloom: Default::default(),
            tip_hash: [0u8; 32],
        };
        let mut metadata = active.metadata.borrow_mut();
        metadata.read = Some(cursor.clone());
        metadata.write = cursor;
    }

    fn test_codec() -> DictCodec {
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile")
    }

    /// Happy path: writes 5 metablocks at wal=1..=5 and asks for the half-open range
    /// (follower=2, leader=5). Result must be wal=3 and wal=4 in chronological order.
    #[test]
    fn returns_entries_in_chronological_order() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let lsc = Rc::new(LogSegmentsCache::ready_up(dir, FILE_SIZE, 4, 0).await.unwrap());

            let blocks: Vec<Metablock> = (1..=5).map(|i| metablock_at(i, 0)).collect();
            write_metablocks_and_set_read_cursor(&lsc, &blocks).await;

            let entries = fetch_catchup_entries(&lsc, 2, 5, Some(1024 * 1024), 64 * 1024, &test_codec()).await.unwrap();

            let wal_seqs: Vec<u64> = entries.iter().map(|e| e.metablock.wal_seq).collect();
            assert_eq!(wal_seqs, vec![3, 4], "should return (follower, leader) exclusive on both ends");

            lsc.close().await;
        });
    }

    /// Empty range (follower == leader == anything in the middle) returns an empty vec —
    /// not an error. Same applies if the follower is already at the leader's tip.
    #[test]
    fn returns_empty_when_follower_caught_up() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let lsc = Rc::new(LogSegmentsCache::ready_up(dir, FILE_SIZE, 4, 0).await.unwrap());

            let blocks: Vec<Metablock> = (1..=3).map(|i| metablock_at(i, 0)).collect();
            write_metablocks_and_set_read_cursor(&lsc, &blocks).await;

            let entries = fetch_catchup_entries(&lsc, 3, 4, Some(1024 * 1024), 64 * 1024, &test_codec()).await.unwrap();
            assert!(entries.is_empty(), "no entries strictly between follower and leader");

            lsc.close().await;
        });
    }

    /// Empty result when the leader no longer holds the gap range (e.g. compacted away).
    /// Distinct from the caught-up case: here the leader's wal is much higher than what's
    /// on disk. The function returns `Ok(vec![])` — signal to the caller that the
    /// follower's position is implicitly current and no replay is needed.
    #[test]
    fn returns_empty_when_leader_no_longer_has_entries() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let lsc = Rc::new(LogSegmentsCache::ready_up(dir, FILE_SIZE, 4, 0).await.unwrap());

            // Disk has wal=10..=12 only (older entries were trimmed).
            let blocks: Vec<Metablock> = (10..=12).map(|i| metablock_at(i, 0)).collect();
            write_metablocks_and_set_read_cursor(&lsc, &blocks).await;

            // Follower is way behind at wal=2, leader claims wal=5. Disk has nothing in
            // (2, 5) — scanner stops at wal=10 and the in-range filter never matches.
            let entries = fetch_catchup_entries(&lsc, 2, 5, Some(1024 * 1024), 64 * 1024, &test_codec()).await.unwrap();
            assert!(entries.is_empty());

            lsc.close().await;
        });
    }

    /// Budget exhaustion: each metablock claims 64 KiB. With 4 entries in range and a
    /// 100 KiB budget, the scan must trip the limit and return `FollowerTooFarBehind`.
    #[test]
    fn returns_follower_too_far_behind_when_budget_exceeded() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let lsc = Rc::new(LogSegmentsCache::ready_up(dir, FILE_SIZE, 4, 0).await.unwrap());

            let blocks: Vec<Metablock> = (1..=5).map(|i| metablock_at(i, 64 * 1024)).collect();
            write_metablocks_and_set_read_cursor(&lsc, &blocks).await;

            let result = fetch_catchup_entries(&lsc, 0, 5, Some(100 * 1024), 64 * 1024, &test_codec()).await;
            assert!(matches!(result, Err(FetchCatchupEntriesError::FollowerTooFarBehind)),
                "expected FollowerTooFarBehind, got {:?}", result.err());

            lsc.close().await;
        });
    }

    /// Half-open semantics: the boundary metablocks (wal == follower, wal == leader) must
    /// NOT appear in the result. Wal=follower is the entry the follower already has;
    /// wal=leader is the one that triggered the catchup and is sent on the next attempt.
    #[test]
    fn excludes_boundary_wal_seqs() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let lsc = Rc::new(LogSegmentsCache::ready_up(dir, FILE_SIZE, 4, 0).await.unwrap());

            let blocks: Vec<Metablock> = (1..=5).map(|i| metablock_at(i, 0)).collect();
            write_metablocks_and_set_read_cursor(&lsc, &blocks).await;

            let entries = fetch_catchup_entries(&lsc, 1, 4, Some(1024 * 1024), 64 * 1024, &test_codec()).await.unwrap();
            let wal_seqs: Vec<u64> = entries.iter().map(|e| e.metablock.wal_seq).collect();
            assert_eq!(wal_seqs, vec![2, 3], "wal=1 (follower) and wal=4 (leader) excluded");

            lsc.close().await;
        });
    }

}
