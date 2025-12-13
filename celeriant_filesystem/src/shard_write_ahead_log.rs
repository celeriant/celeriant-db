use std::{cell::Cell, collections::HashMap, path::{Path, PathBuf}};

use celeriant_disk::files::open_dma_files::{create_file_dma, existing_file_dma};
use celeriant_msg::{request::requests::WriteRequest, response::responses::WriteResponse};
use celeriant_wal::{constants::METADATA_BATCH_SIZE_BYTES, shard_log::{shard_log_checkpoint::ShardLogCheckpoint, shard_log_header::ShardLogHeader}, wal::{event_batch_item::EventBatchItem, event_batch_metadata::EventBatchMetadata}};
use celeriant_wire::version_aware_wire_format::{deserialize_shard_log_checkpoint_versioned, deserialize_shard_log_header_versioned, to_wire_format_fixed_with_version};
use glommio::{io::DmaFile, sync::RwLock};

use crate::{local_event::LocalEvent, shard_config::ShardConfig, shard_log_file::ShardLogFile, shard_log_write_error::ShardLogWriteError, shard_mem_cache::ShardMemCache};

/// Represents the WAL for each shard. Operations we perform:
///   - Append batches to in-memory queue
///   - Write batches from queue to disk
///   - Read batches either from hot cache or from disk
/// We use RwLock here to allow concurrent, interleaved tasks in the shard.
///   - We can read from cache while appending to queue or writing to disk
///   - We can append to queue while writing to disk
/// The goal is to hold the shortest write locks possible.
pub struct ShardWriteAheadLog {
    shard_logs: RwLock<HashMap<u64, ShardLogFile>>,
    shard_mem_cache: RwLock<ShardMemCache>,
    pending_append_queue: RwLock<Vec<EventBatchQueueItem>>,
    wal_sync_event: RwLock<Option<LocalEvent<SyncResult>>>,
    has_pending_sync_error: Cell<bool>,
}

impl ShardWriteAheadLog {
    pub async fn new(shard_id: usize, data_root: &PathBuf) -> Result<Self, ShardLogWriteError> {
        Self::new_with_config(shard_id, data_root, ShardConfig::default()).await
    }

    pub async fn close(&self) -> Result<(), ShardLogWriteError> {
        let mut shard_logs = self.shard_logs.try_write()?;
        
        for shard_log_file in shard_logs.values_mut() {
            if let Some(dma_file) = shard_log_file.dma_file.take() {
                dma_file.close().await?;
            }
        }
        
        Ok(())
    }

    pub async fn new_with_config(
        shard_id: usize,
        data_root: &PathBuf,
        config: ShardConfig,
    ) -> Result<Self, ShardLogWriteError> {
        let shard_dir = data_root.join(format!("shard_{shard_id}"));
        std::fs::create_dir_all(&shard_dir)?;

        // Find latest log file id (or create first).
        let (log_id, log_path) = find_latest_log_file(&shard_dir)
            .map_err(|e| ShardLogWriteError::IoError(e.to_string()))?
            .unwrap_or((1, shard_dir.join(log_file_name(1))));
        
        let exists = log_path.exists();
        let mut dma_file = if exists { 
            existing_file_dma(&log_path).await?
        } 
        else 
        { 
            create_file_dma(&log_path, Some(config.preallocate_bytes)).await? 
        };

        let alignment = dma_file.alignment();
        let checkpoint_reserved_bytes = alignment * config.checkpoint_reserved_bytes_multiple;

        let (header, checkpoint) = if exists {
            load_header_and_checkpoint(&mut dma_file, alignment).await?
            //TODO: This could fail due to crc or torn writes, we can repair by going through the metadata and other logs.
        } else {
            setup_new_file(&mut dma_file, &config, alignment, checkpoint_reserved_bytes).await?
        };

        let shard_log_file = ShardLogFile {
            shard_log_header: header.clone(),
            dma_file: Some(dma_file),
        };

        let mut shard_logs = HashMap::new();
        shard_logs.insert(log_id, shard_log_file);

        let shard_mem_cache = ShardMemCache {
            active_shard_log_checkpoint: checkpoint,
            aggregate_metadata: HashMap::new(),
            aggregate_client_event_indexes: HashMap::new(),
            recent_writes_cache: HashMap::new(),
        };

        Ok(Self {
            shard_logs: RwLock::new(shard_logs),
            shard_mem_cache: RwLock::new(shard_mem_cache),
            pending_append_queue: RwLock::new(Vec::new()),
            wal_sync_event: RwLock::new(None),
            has_pending_sync_error: Cell::new(false),
        })
    }

    pub async fn write(
        &self,
        lease_index: u64,
        mut request: WriteRequest,
    ) -> Result<WriteResponse, ShardLogWriteError> {
        todo!()
    }
}

pub type SyncResult = Result<(), ShardLogWriteError>;

/// In-memory queue of data waiting to be written to disk + fsync'd
/// We include the structs here too as they go into the cache after fsync
struct EventBatchQueueItem {
    pub compressed_event_batch_item: Vec<u8>,
    pub event_batch_item: EventBatchItem,
    pub metadata_bytes: [u8; METADATA_BATCH_SIZE_BYTES],
    pub event_batch_metadata: EventBatchMetadata,
}

fn log_file_name(log_id: u64) -> String {
    format!("log_{log_id}.wal")
}

fn find_latest_log_file(dir: &Path) -> Result<Option<(u64, PathBuf)>, std::io::Error> {
    let mut best: Option<(u64, PathBuf)> = None;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };

        // Expect: log_{id}.wal
        if !name.starts_with("log_") || !name.ends_with(".wal") {
            continue;
        }

        let id_str = &name["log_".len()..name.len() - ".wal".len()];
        if let Ok(id) = id_str.parse::<u64>() {
            match &best {
                None => best = Some((id, path)),
                Some((best_id, _)) if id > *best_id => best = Some((id, path)),
                _ => {}
            }
        }
    }

    Ok(best)
}

async fn load_header_and_checkpoint(
    dma_file: &mut DmaFile,
    header_region_len: u64,
) -> Result<(ShardLogHeader, ShardLogCheckpoint), ShardLogWriteError> {
    
    let header_bytes = dma_file.read_at(0, header_region_len as usize).await?;
    let (header, _ver) = deserialize_shard_log_header_versioned(&header_bytes)?;

    let file_len = dma_file.file_size().await?;

    let header_bytes = dma_file.read_at(header.shard_log_checkpoint_start_pos, file_len.saturating_sub(header.shard_log_checkpoint_start_pos) as usize).await?;
    let (checkpoint, _ver) = deserialize_shard_log_checkpoint_versioned(&header_bytes)?;

    Ok((header, checkpoint))
}

async fn setup_new_file(
    dma_file: &mut DmaFile,
    config: &ShardConfig,
    header_region_len: u64,
    checkpoint_region_len: u64,
) -> Result<(ShardLogHeader, ShardLogCheckpoint), ShardLogWriteError> {
    let checkpoint_start_pos = config
        .preallocate_bytes
        .saturating_sub(checkpoint_region_len);

    let header = ShardLogHeader {
        shard_log_version: config.shard_log_version,
        shard_log_checkpoint_start_pos: checkpoint_start_pos,
    };

    let checkpoint = ShardLogCheckpoint::new(config.preallocate_bytes, header_region_len, checkpoint_start_pos);
 
    let mut buffer = dma_file.alloc_dma_buffer(header_region_len as usize);
    to_wire_format_fixed_with_version(&header, buffer.as_bytes_mut())?;
    dma_file.write_at(buffer, 0).await?;
 
    let mut buffer = dma_file.alloc_dma_buffer(checkpoint_region_len as usize);
    to_wire_format_fixed_with_version(&checkpoint, buffer.as_bytes_mut())?;
    dma_file.write_at(buffer, checkpoint_start_pos).await?;

    dma_file.fdatasync().await?;

    Ok((header, checkpoint))
}

#[cfg(test)]
mod TestShardWriteAheadLog {
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{shard_config::ShardConfig, shard_write_ahead_log::ShardWriteAheadLog};

    #[test]
    fn test_create_new_shard_wal() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_path_buf();

                let wal = ShardWriteAheadLog::new(0, &data_root).await.unwrap();
                
                // Verify the shard directory was created
                let shard_dir = data_root.join("shard_0");
                assert!(shard_dir.exists());
                
                // Verify the log file was created
                let log_file = shard_dir.join("log_1.wal");
                assert!(log_file.exists());

                wal.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_reopen_existing_shard_wal() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_path_buf();

                // Create and close the WAL
                {
                    let wal = ShardWriteAheadLog::new(0, &data_root).await.unwrap();
                    wal.close().await.unwrap();
                }

                // Reopen the WAL - should load existing file
                {
                    let wal = ShardWriteAheadLog::new(0, &data_root).await.unwrap();
                    wal.close().await.unwrap();
                }

                // Verify only one log file exists (no duplicates created)
                let shard_dir = data_root.join("shard_0");
                let log_files: Vec<_> = std::fs::read_dir(&shard_dir)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().map(|ext| ext == "wal").unwrap_or(false))
                    .collect();
                assert_eq!(log_files.len(), 1);
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_create_with_custom_config() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_path_buf();

                let config = ShardConfig {
                    preallocate_bytes: 64 * 1024 * 1024, // 64 MiB
                    shard_log_version: 2,
                    checkpoint_reserved_bytes_multiple: 50,
                };

                let wal = ShardWriteAheadLog::new_with_config(0, &data_root, config.clone())
                    .await
                    .unwrap();

                // Verify file was preallocated to the configured size
                let log_file = data_root.join("shard_0").join("log_1.wal");
                let metadata = std::fs::metadata(&log_file).unwrap();
                assert_eq!(metadata.len(), config.preallocate_bytes);

                wal.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_multiple_shards() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_path_buf();

                let wal_0 = ShardWriteAheadLog::new(0, &data_root).await.unwrap();
                let wal_1 = ShardWriteAheadLog::new(1, &data_root).await.unwrap();
                let wal_2 = ShardWriteAheadLog::new(2, &data_root).await.unwrap();

                // Verify separate directories created
                assert!(data_root.join("shard_0").exists());
                assert!(data_root.join("shard_1").exists());
                assert!(data_root.join("shard_2").exists());

                wal_0.close().await.unwrap();
                wal_1.close().await.unwrap();
                wal_2.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_close_is_idempotent() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_path_buf();

                let wal = ShardWriteAheadLog::new(0, &data_root).await.unwrap();
                
                // Close multiple times should not error
                wal.close().await.unwrap();
                wal.close().await.unwrap();
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_header_and_checkpoint_persisted_correctly() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempfile::tempdir().unwrap();
                let data_root = tempdir.path().to_path_buf();

                let config = ShardConfig {
                    preallocate_bytes: 32 * 1024 * 1024,
                    shard_log_version: 42,
                    checkpoint_reserved_bytes_multiple: 25,
                };

                // Create WAL with specific config
                {
                    let wal = ShardWriteAheadLog::new_with_config(0, &data_root, config.clone())
                        .await
                        .unwrap();
                    wal.close().await.unwrap();
                }

                // Reopen and verify config was persisted
                {
                    let wal = ShardWriteAheadLog::new_with_config(0, &data_root, config.clone())
                        .await
                        .unwrap();
                    
                    let shard_logs = wal.shard_logs.read().await.unwrap();
                    let shard_log = shard_logs.values().next().unwrap();
                    
                    assert_eq!(shard_log.shard_log_header.shard_log_version, 42);
                    
                    wal.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }
}