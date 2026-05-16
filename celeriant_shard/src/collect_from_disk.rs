use std::collections::{HashMap};

use celeriant_disk::files::read_objects_absolute::{AbsoluteObjectPosition, read_objects_absolute};
use celeriant_rotating_log::{log_segments_cache::LogSegmentsCache};
use celeriant_wal::{
    datablocks::datablock::Datablock,
    metablocks::{datablock_storage_kind::DatablockStorageKind, metablock::Metablock},
};
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::disk::{serialised_datablock::deserialise_datablock};

use crate::error::fetch_datablock_error::FetchDatablockError;

/// Metablock and datablock pair in the context of which
/// log segment file it came from
pub struct EventBatchFromLogSegmentFile {
    pub log_id: u64,
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
}

#[derive(Default)]
struct LogSegmentDatablockPositions {
    metablock_indexes: Vec<usize>,
    datablock_positions: Vec<AbsoluteObjectPosition>,
}

impl LogSegmentDatablockPositions {
    pub fn push(&mut self, metablock_idx: usize, datablock_start_pos: u64, datablock_end_pos: u64) {
        self.metablock_indexes.push(metablock_idx);
        self.datablock_positions.push(AbsoluteObjectPosition {
            start_pos: datablock_start_pos,
            end_pos: datablock_end_pos,
        });
    }

    /// Datablock positions are collected in chronological (metablock) order, which is
    /// monotonically descending because the datablocks region grows downward and each
    /// sync batch writes its datablocks in reverse queue order. A simple reverse
    /// produces the ascending position order that read_objects_absolute requires.
    fn reverse_positions(&mut self) {
        self.metablock_indexes.reverse();
        self.datablock_positions.reverse();
    }
}

/// Populates the datablock in-memory representations in kept_metablocks,
/// either a simple deserialise or by pulling it efficiently from disk.
pub async fn fetch_datablocks_for_metablocks(
    kept_metablocks: &mut [EventBatchFromLogSegmentFile],
    read_max_chunk_size: u64,
    log_segments_cache: &LogSegmentsCache,
    dict_codec: &DictCodec,
) -> Result<(), FetchDatablockError> {
    let mut disk_fetches_by_log_id: HashMap<u64, LogSegmentDatablockPositions> = HashMap::new();

    for (metablock_idx, kept) in kept_metablocks.iter_mut().enumerate() {
        if kept.datablock.is_some() {
            continue;
        }

        match &kept.metablock.datablock {
            DatablockStorageKind::None => {
                continue;
            }
            DatablockStorageKind::Inline(_) => {
                let datablock = deserialise_datablock(
                    kept.metablock.uncompressed_size,
                    kept.metablock.compressed_size,
                    kept.metablock.datablock_version,
                    kept.metablock.datablock_compression_type,
                    &kept.metablock.datablock,
                    None,
                    dict_codec,
                )
                .map_err(|source| FetchDatablockError::DatablockError {
                    log_id: kept.log_id,
                    wal_index: kept.metablock.wal_index,
                    source,
                    is_inline: true
                })?;

                kept.datablock = Some(datablock);
            }
            DatablockStorageKind::Block(_) => {
                disk_fetches_by_log_id.entry(kept.log_id).or_default().push(
                    metablock_idx,
                    kept.metablock.datablock_position,
                    kept.metablock.datablock_position.saturating_add(kept.metablock.compressed_size),
                );
            }
        }
    }

    for (log_id, mut log_fetches) in disk_fetches_by_log_id {
        log_fetches.reverse_positions();

        let blobs = {
            let log_segment_file = log_segments_cache.get(log_id).await.map_err(FetchDatablockError::LogSegmentFileError)?;
            let file_len = log_segment_file.metadata.borrow().file_len;
            let guard = log_segment_file
                .lock_reader("fetch_datablocks")
                .await
                .map_err(|_| FetchDatablockError::LogSegmentFileReaderContention)?;
            let dma = guard.as_ref().ok_or_else(|| FetchDatablockError::LogSegmentFileUnavailable { log_id })?;

            read_objects_absolute(dma, file_len, &log_fetches.datablock_positions, read_max_chunk_size)
                .await
                .map_err(|e| {
                    tracing::error!(log_id, error = %e, datablock_count = log_fetches.datablock_positions.len(), "DMA read failed fetching datablocks");
                    FetchDatablockError::DatablockReadError(e.to_string())
                })?
        };

        if blobs.len() != log_fetches.metablock_indexes.len() {
            return Err(FetchDatablockError::MissingDatablocksOnDisk);
        }

        for (idx, blob) in blobs.iter().enumerate() {
            let kept = &mut kept_metablocks[log_fetches.metablock_indexes[idx]];

            let datablock = deserialise_datablock(
                kept.metablock.uncompressed_size,
                kept.metablock.compressed_size,
                kept.metablock.datablock_version,
                kept.metablock.datablock_compression_type,
                &kept.metablock.datablock,
                Some(blob),
                dict_codec,
            )
            .map_err(|source| FetchDatablockError::DatablockError {
                log_id: kept.log_id,
                wal_index: kept.metablock.wal_index,
                source,
                is_inline: false
            })?;

            kept.datablock = Some(datablock);
        }
    }

    Ok(())
}
