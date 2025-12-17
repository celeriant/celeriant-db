use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    rc::Rc, time::Duration,
};

use celeriant_disk::files::open_dma_files::{create_file_dma, existing_file_dma};
use celeriant_msg::{request::requests::{ReadRequest, WriteRequest}, response::responses::{ReadResponse, WriteResponse}};
use celeriant_wal::{
    aggregate_key::AggregateKey, constants::{
        BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED, FIXED_BLOCK_SIZE_BYTES,
        MINIBATCH_SIZE_BYTES,
    }, datablocks::{
        event_batch_item::EventBatchItem, event_item::EventItem, wal_datablock::WalDatablock,
    }, metablocks::{
        datablock_style::DatablockStyle, event_batch_metadata::{EventBatchMetadata, EventTypesData}, wal_metablock::WalMetablock
    }, shard_log_header::ShardLogHeader
};
use celeriant_wire::{
    version_aware_wire_format::{
        deserialize_shard_log_header_versioned, serialize_fixed_len_with_version,
    },
    wire_format::to_wire_format_variable,
};
use fastbloom::BloomFilter;
use glommio::{io::DmaFile, sync::RwLock};

use crate::{
    in_memory_cache::{shard_log_queue_item::ShardLogQueueItem, shard_mem_cache::{self, ShardMemCache, SyncPositionsSnapshot}},
    local_event::LocalEvent,
    shard_config::ShardConfig,
    shard_log_write_error::ShardLogWriteError, watch::{aggregate_watch_event::AggregateWatchEvent, watched_aggregates::WatchedAggregates},
};

pub type SyncResult = Result<(), ShardLogWriteError>;

/// Represents the WAL for each shard. Operations we perform:
///   - Append batches to in-memory queue
///   - Write batches from queue to disk
///   - Read batches either from hot cache or from disk
/// We use RwLock here to allow concurrent, interleaved tasks in the shard.
///   - We can read from cache while appending to queue or writing to disk
///   - We can append to queue while writing to disk
/// The goal is to hold the shortest write locks possible.
pub struct ShardWriteAheadLog {
    /// We never hold write locks over async boundaries so only need RefCell for shard_mem_cache
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    bloom_filter: RefCell<BloomFilter>,
    event_type_dedup: RefCell<HashSet<u64>>,
    pub watched_aggregates: Rc<WatchedAggregates>,
    wal_sync_event: Rc<RwLock<Option<Rc<LocalEvent<SyncResult>>>>>,
    dma_file: Rc<RwLock<Option<DmaFile>>>,
    active_log_id: u64,
    active_log_len: u64,
    config: ShardConfig,
}

impl ShardWriteAheadLog {
    pub async fn new(shard_id: usize, data_root: &PathBuf) -> Result<Self, ShardLogWriteError> {
        Self::new_with_config(shard_id, data_root, ShardConfig::default()).await
    }

    pub async fn read(&self, read_request: &ReadRequest) -> Result<ReadResponse, ShardLogWriteError> {
        todo!()
    }

    pub async fn close(&self) -> Result<(), ShardLogWriteError> {
        let mut disk_writer = self.dma_file.write().await?;

        if let Some(dma_file) = disk_writer.take() {
            dma_file.close().await?;
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
        let (active_log_id, log_path) = find_latest_log_file(&shard_dir)
            .map_err(|e| ShardLogWriteError::IoError(e.to_string()))?
            .unwrap_or((1, shard_dir.join(log_file_name(1))));

        let exists = log_path.exists();
        let (mut dma_file, active_log_len) = if exists {
            existing_file_dma(&log_path).await?
        } else {
            (
                create_file_dma(&log_path, Some(config.preallocate_bytes)).await?,
                config.preallocate_bytes,
            )
        };

        let header = if exists {
            load_header(&mut dma_file, active_log_len).await?
            //TODO: This could fail due to crc or torn writes, we can repair by going through the metadata and other logs.
        } else {
            setup_new_file(&mut dma_file, active_log_len).await?
        };

        let shard_mem_cache = ShardMemCache::new(active_log_len, header.metablocks_position, header.datablocks_position);

        let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);

        Ok(Self {
            active_log_len,
            active_log_id,
            shard_mem_cache: Rc::new(RefCell::new(shard_mem_cache)),
            dma_file: Rc::new(RwLock::new(Some(dma_file))),
            wal_sync_event: Rc::new(RwLock::new(None)),
            config,
            bloom_filter: RefCell::new(bloom_filter),
            event_type_dedup: RefCell::new(HashSet::new()),
            watched_aggregates: Rc::new(WatchedAggregates::new())
        })
    }

    pub async fn write(
        &self,
        lease_index: u64,
        mut write_request: WriteRequest,
    ) -> Result<WriteResponse, ShardLogWriteError> {
        // Make sure we have at least one event to write
        if write_request.events.is_empty() {
            return Err(ShardLogWriteError::EmptyEventsList);
        }

        // Validate that no event uses the sentinel 0 event type
        if let Some(ev) = write_request
            .events
            .iter()
            .find(|e| e.event_type_major == 0)
        {
            return Err(ShardLogWriteError::ZeroEventType {
                client_event_index: ev.client_event_index,
            });
        }

        let (write_response, force_durable) = {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

            // If checking idempotency, check if client is providing the same events again using client event index, if so, error
            if write_request.enforce_client_idempotency {
                if let Some(last_client_event_index) = shard_mem_cache
                    .get_client_event_index(
                        &write_request.aggregate_key,
                        write_request.client_id,
                    )
                {
                    let attempted_client_event_index = write_request
                        .events
                        .iter()
                        .map(|e| e.client_event_index)
                        .min()
                        .unwrap_or(0);
                    if attempted_client_event_index <= last_client_event_index {
                        return Err(ShardLogWriteError::ClientIdempotencyViolation {
                            client_id: write_request.client_id,
                            last_client_event_index,
                            attempted_client_event_index,
                        });
                    }
                }
            }

            let aggregate_current_indexes =
                shard_mem_cache.get_event_indexes(&write_request.aggregate_key);

            // If doing optimistic concurrency, check expected event batch index matches current
            if let Some(expected) = write_request.expected_event_batch_index {
                if expected != aggregate_current_indexes.event_batch_index {
                    return Err(ShardLogWriteError::OptimisticConcurrencyViolation {
                        expected_event_batch_index: expected,
                        current_event_batch_index: aggregate_current_indexes.event_batch_index,
                    });
                }
            }

            let server_timestamp = get_server_timestamp_ms();

            // Update events - set event indexes, server timestamp millis. Keep track of last event index assigned to update state later
            let mut event_index = aggregate_current_indexes.event_index;
            let event_batch_index = aggregate_current_indexes.event_batch_index.saturating_add(1);

            for e in write_request.events.iter_mut() {
                event_index = event_index.saturating_add(1);
                e.event_index = event_index;
            }

            let mut write_response = WriteResponse {
                correlation_id: write_request.correlation_id,
                event_batch_index,
                start_event_index: aggregate_current_indexes.event_index.saturating_add(1),
                server_timestamp,
                node_id: self.config.node_id,
                lease_index,
                compressed_size: 0, //Write later after serialization
                events_crc: 0,
            };

            let events_in_batch = std::mem::take(&mut write_request.events);

            // Create EventBatchItem from events with next index, don't increment struct state yet though
            let event_batch_item = EventBatchItem::new(
                write_response.event_batch_index,
                server_timestamp,
                write_request.client_id,
                write_request.user_id,
                self.config.node_id,
                lease_index,
                events_in_batch,
            );

            // Determine event types data (bloom filter or direct array)
            let (event_types, use_bloom) = extract_unique_event_types(&event_batch_item.events);
            let event_types_data = if use_bloom {
                let bloom_bytes = self.create_bloom_filter_bytes(&event_batch_item.events);
                EventTypesData::Bloom(bloom_bytes)
            } else {
                EventTypesData::Direct(event_types)
            };

            let datablock = WalDatablock::EventBatch(event_batch_item);

            // Serialize and compress the wal_datablock
            let (uncompressed_size, datablock_bytes) =
                to_wire_format_variable(&datablock, write_request.compression_type)?;
            let events_crc = crc32c::crc32c(&datablock_bytes);

            write_response.events_crc = events_crc;
            write_response.compressed_size = datablock_bytes.len() as u64;

            let minibatch = if datablock_bytes.len() <= MINIBATCH_SIZE_BYTES {
                let mut batch = [0u8; MINIBATCH_SIZE_BYTES];
                batch[..datablock_bytes.len()].copy_from_slice(&datablock_bytes);
                Some(batch)
            } else {
                None
            };

            // Extract reference from datablock instead of cloning
            let event_batch_item_ref = match &datablock {
                WalDatablock::EventBatch(item) => item,
                _ => unreachable!("We just created an EventBatch variant"),
            };

            // Create and serialize metadata
            let event_batch_metadata = EventBatchMetadata::from_batch_item(
                write_request.aggregate_key.clone(),
                event_batch_item_ref,
                0, //Don't need to set datablock position yet
                uncompressed_size as u64,
                datablock_bytes.len() as u64,
                events_crc,
                write_request.compression_type,
                event_types_data,
                minibatch,
            );
            let latest_client_event_index = event_batch_metadata.max_client_event_index;
            let metablock = WalMetablock::EventBatchMetadata(event_batch_metadata);

            let shard_log_queue_item = ShardLogQueueItem {
                datablock_bytes: if minibatch.is_none() {
                    Some(datablock_bytes)
                } else {
                    None
                },
                datablock: if minibatch.is_none(){ Some(datablock) } else { None },
                metablock,
            };

            // Update next event index, next event batch index, client event indexes
            shard_mem_cache.add_to_pending_append_queue(
                &write_request.aggregate_key,
                event_index,
                event_batch_index,
                write_request.client_id,
                latest_client_event_index,
                shard_log_queue_item,
            );

            (write_response, shard_mem_cache.force_durable_on_next_write())
        };

        // Now we wait on disk write before ack to client
        if force_durable {
            sync_with_delay(self.wal_sync_event.clone(), self.dma_file.clone(), self.shard_mem_cache.clone(), None, self.watched_aggregates.clone()).await?;
        } else if let Some(delay_us) = self.config.durable_write_with_delay_us {
            sync_with_delay(self.wal_sync_event.clone(), self.dma_file.clone(), self.shard_mem_cache.clone(), Some(Duration::from_micros(delay_us)), self.watched_aggregates.clone()).await?;
        } else {
            let async_flush_ms = self.config.async_flush_ms;
            let watched_aggregates = self.watched_aggregates.clone();
            let shard_mem_cache = self.shard_mem_cache.clone();
            let wal_sync_event = self.wal_sync_event.clone();
            let dma_file = self.dma_file.clone();

            //TODO: do we really need another spawn_local? Just don't await?
            glommio::spawn_local(async move {
                let _ = sync_with_delay(wal_sync_event, dma_file, shard_mem_cache.clone(), Some(Duration::from_millis(async_flush_ms)), watched_aggregates).await;
            })
            .detach();
        }


        Ok(write_response)
    }

    fn create_bloom_filter_bytes(&self, events: &[EventItem]) -> [u64; BLOOM_BYTES / 8] {
        let mut bloom_filter = self.bloom_filter.borrow_mut();
        let mut event_type_dedup = self.event_type_dedup.borrow_mut();

        // Populate bloom filter with multiple event types
        bloom_filter.clear();
        event_type_dedup.clear();

        for event in events {
            event_type_dedup.insert(event.event_type_major);
        }

        for &event_type in event_type_dedup.iter() {
            bloom_filter.insert(&event_type.to_le_bytes());
        }

        bloom_filter
            .as_slice()
            .try_into()
            .expect("Conversion failed")
    }
}

async fn sync_with_delay(
    wal_sync_event: Rc<RwLock<Option<Rc<LocalEvent<SyncResult>>>>>,
    dma_file: Rc<RwLock<Option<DmaFile>>>,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    wal_sync_delay: Option<Duration>, 
    watched_aggregates: Rc<WatchedAggregates>) -> SyncResult {

    if wal_sync_delay.is_none() || wal_sync_delay.unwrap().as_micros() == 0 {
        // No delay - do immediate sync
        return Ok(sync_with_rollback(dma_file, shard_mem_cache, watched_aggregates).await?);
    }

    let wal_sync_delay = wal_sync_delay.unwrap();

    loop {
        match wal_sync_event.try_write() {
            Ok(mut maybe_event) => {
                // We won! Check if sync is already in progress
                if let Some(event) = maybe_event.as_ref() {
                    // Another task beat us between our check and lock acquisition
                    let event = event.clone();
                    drop(maybe_event); // Release lock before awaiting
                    return event.listen().await;
                }

                // We're the coordinator - create the event
                let event = Rc::new(LocalEvent::new());
                *maybe_event = Some(event.clone());
                drop(maybe_event); // Release lock while sleeping

                // Sleep for the delay period
                glommio::timer::sleep(wal_sync_delay).await;

                // Clear the event before sync (need write lock again)
                wal_sync_event.write().await.unwrap().take();

                // Do the actual sync
                let sync_result = {
                    sync_with_rollback(dma_file, shard_mem_cache, watched_aggregates).await?
                };

                // Notify all waiters
                event.notify(Ok(sync_result.clone()));

                return Ok(sync_result);
            }
            Err(_) => {
                let maybe_event = wal_sync_event.read().await.unwrap();
                if let Some(event) = maybe_event.as_ref() {
                    let event = event.clone();
                    drop(maybe_event);
                    return event.listen().await;
                }
                // Retry - coordinator cleared event while we were waiting
                drop(maybe_event);
                continue;
            }
        }
    }
    
}

async fn sync_with_rollback(
    dma_file: Rc<RwLock<Option<DmaFile>>>,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    watched_aggregates: Rc<WatchedAggregates>) -> SyncResult {
    
    // Lock the writer before we take the snapshot of the queue
    let mut guard_writer = dma_file.write().await?;
    let dma_file_writer = guard_writer.as_mut();

    let mut dma_file_writer = if let Some(dma_file_writer) = dma_file_writer {
        dma_file_writer
    } else {
        return Err(ShardLogWriteError::DmaFileNotInitialized);
    };

    let mut sync_positions_snapshot = {
        let mut shard_mem_cache = shard_mem_cache.borrow_mut();
        if !shard_mem_cache.requires_write() {
            // Queue is empty - either another coordinator synced our items,
            // or a previous sync failed and cleared the queue
            if shard_mem_cache.force_durable_on_next_write() {
                return Err(ShardLogWriteError::IoError("Disk sync failure forced queue clear".to_string()));
            }
            // Another coordinator successfully synced our items
            return Ok(());
        }
        
        shard_mem_cache.take_sync_positions_snapshot()
    };

    match sync(&mut dma_file_writer, &mut sync_positions_snapshot).await {
        Ok(_) => {
            let mut shard_mem_cache = shard_mem_cache.borrow_mut();

            let mut pending_append_queue = vec![];
            std::mem::swap(&mut pending_append_queue, &mut sync_positions_snapshot.pending_append_queue);

            shard_mem_cache.commit_sync_positions_snapshot(sync_positions_snapshot);

            let mut write_events: HashMap<AggregateKey, AggregateWatchEvent> = HashMap::new();
            for queue_item in pending_append_queue {

                match queue_item.metablock {
                    WalMetablock::EventBatchMetadata(event_batch_metadata) => {

                        //TODO: update in-memory cache

                        // Build watched_aggregates events
                        write_events
                            .entry(event_batch_metadata.aggregate_key)
                            .and_modify(|event| {
                                if let AggregateWatchEvent::Write { from_event_batch_index, to_event_batch_index } = event {
                                    if event_batch_metadata.event_batch_index < *from_event_batch_index {
                                        *from_event_batch_index = event_batch_metadata.event_batch_index;
                                    }
                                    if event_batch_metadata.event_batch_index > *to_event_batch_index {
                                        *to_event_batch_index = event_batch_metadata.event_batch_index;
                                    }
                                }
                            })
                            .or_insert(AggregateWatchEvent::Write {
                                from_event_batch_index: event_batch_metadata.event_batch_index,
                                to_event_batch_index: event_batch_metadata.event_batch_index,
                            });
                    },
                    WalMetablock::SnapshotOrg(_snapshot_org) => {},
                    WalMetablock::SnapshotAggregatSnapshotAggregateType(_snapshot_aggregate_type) => {},
                    WalMetablock::SnapshotAggregate(_snapshot_aggregate) => {},
                }
            }

            for (aggregate_key, watch_event) in write_events {
                watched_aggregates.notify(&aggregate_key, watch_event);
            }

            return Ok(());
        },
        Err(e) => {
            let mut shard_mem_cache = shard_mem_cache.borrow_mut();
            shard_mem_cache.rollback_queue_positions();
            return Err(e);
        }
    }

}

async fn sync(dma_file_writer: &mut DmaFile, sync_positions_snapshot: &mut SyncPositionsSnapshot) -> Result<(), ShardLogWriteError> {
    // Write datablocks first so we can get the positions to include into metablocks
    let buffer_size_datablocks: u64 = sync_positions_snapshot.pending_append_queue
        .iter()
        .map(|item| item.datablock_bytes.as_ref().map_or(0, |bytes| bytes.len() as u64))
        .sum();
    let mut datablocks_absolute_write_positions: Vec<u64> = Vec::with_capacity(sync_positions_snapshot.pending_append_queue.len());
    let mut new_datablocks_position = sync_positions_snapshot.datablocks_position;
    let mut datablocks_carry_over: Option<Vec<u8>> = sync_positions_snapshot.datablocks_carry_over.take();
    if buffer_size_datablocks > 0 {

        let write_to_pos = dma_file_writer.align_up(sync_positions_snapshot.datablocks_position);
        let write_from_pos =  dma_file_writer.align_down(sync_positions_snapshot.datablocks_position.saturating_sub(buffer_size_datablocks));
        let aligned_buffer_size_datablocks = write_to_pos.saturating_sub(write_from_pos);

        let mut buffer_datablocks = dma_file_writer.alloc_dma_buffer(aligned_buffer_size_datablocks as usize);
        let buffer_datablocks_slice = buffer_datablocks.as_bytes_mut();

        let end_carry_over = dma_file_writer.align_up(sync_positions_snapshot.datablocks_position).saturating_sub(sync_positions_snapshot.datablocks_position);

        if end_carry_over > 0 {
            if datablocks_carry_over.is_none() || 
                datablocks_carry_over.as_ref().unwrap().len() != end_carry_over as usize {
                return Err(ShardLogWriteError::DatablocksCarryOverBufferNotPresent);
            }
            buffer_datablocks_slice[(aligned_buffer_size_datablocks.saturating_sub(end_carry_over)) as usize..].copy_from_slice(&datablocks_carry_over.as_ref().unwrap());    
        }

        new_datablocks_position = sync_positions_snapshot.datablocks_position.saturating_sub(buffer_size_datablocks);
        let front_carry_over = new_datablocks_position.saturating_sub(dma_file_writer.align_down(new_datablocks_position)) as usize;
        if front_carry_over > 0 {
            buffer_datablocks_slice[..front_carry_over].fill(0);   
        }

        let mut position = 0usize;
        for item in &sync_positions_snapshot.pending_append_queue {
            if let Some(datablock_bytes) = &item.datablock_bytes {
                let len = datablock_bytes.len();
                datablocks_absolute_write_positions.push(new_datablocks_position + position as u64);
                buffer_datablocks_slice[front_carry_over + position..front_carry_over + position + len].copy_from_slice(datablock_bytes);
                position += len;
            }
        }

        let datablocks_carry_over_size = dma_file_writer.align_up(new_datablocks_position).saturating_sub(new_datablocks_position);
        if datablocks_carry_over_size > 0 {
            datablocks_carry_over = Some(buffer_datablocks_slice[front_carry_over..(front_carry_over+datablocks_carry_over_size as usize)].to_vec());
        }
        
        dma_file_writer.write_at(buffer_datablocks, new_datablocks_position.saturating_sub(front_carry_over as u64)).await?;
    }

    let buffer_size_metablocks: u64 = (sync_positions_snapshot.pending_append_queue.len() * FIXED_BLOCK_SIZE_BYTES) as u64;
    let mut buffer_metablocks = dma_file_writer.alloc_dma_buffer(buffer_size_metablocks as usize);
    let buffer_metablocks_slice = buffer_metablocks.as_bytes_mut();
    let mut position = 0usize;
    let mut index = 0;
    for item in &mut sync_positions_snapshot.pending_append_queue {
        
        if item.datablock.is_some() {
            match &mut item.metablock {
                WalMetablock::EventBatchMetadata(event_batch_metadata) => {
                    if let DatablockStyle::Block { datablock_position, .. } = &mut event_batch_metadata.datablock {
                        *datablock_position = datablocks_absolute_write_positions[index];
                    }
                },
                _ => {},
            }
            index += 1;
        }

        let mut metablock_bytes = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_fixed_len_with_version(
            &item.metablock,
            celeriant_wal::metablocks::wal_metablock::CURRENT_VERSION,
            &mut metablock_bytes,
        )?;

        //let metablock_bytes: [u8; FIXED_BLOCK_SIZE_BYTES]
        buffer_metablocks_slice[position..position + FIXED_BLOCK_SIZE_BYTES].copy_from_slice(&metablock_bytes);
        position += FIXED_BLOCK_SIZE_BYTES;
    }
    
    //Write header front & back
    let new_metablocks_position = sync_positions_snapshot.metablocks_position + buffer_metablocks.len() as u64;
    let shard_log_header = ShardLogHeader {
        datablocks_position: new_datablocks_position,
        metablocks_position: new_metablocks_position
    };
    
    dma_file_writer.write_at(buffer_metablocks, sync_positions_snapshot.metablocks_position).await?;

    write_dual_shard_log_header(
        dma_file_writer, 
        sync_positions_snapshot.file_len.saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64), 
        &shard_log_header).await?;

    dma_file_writer.fdatasync().await?;

    sync_positions_snapshot.datablocks_position = new_datablocks_position;
    sync_positions_snapshot.datablocks_carry_over = datablocks_carry_over;
    sync_positions_snapshot.metablocks_position = new_metablocks_position;

    Ok(())
}

fn extract_unique_event_types(events: &[EventItem]) -> ([u64; 4], bool) {
    let mut bloom_or_event_types = [u64::MAX, u64::MAX, u64::MAX, u64::MAX];
    let mut use_bloom = false;
    let mut unique_count = 0;

    for event in events {
        let event_type = event.event_type_major;

        // Check if we already have this event type
        if unique_count > 0 && bloom_or_event_types[0] == event_type {
            continue;
        }
        if unique_count > 1 && bloom_or_event_types[1] == event_type {
            continue;
        }
        if unique_count > 2 && bloom_or_event_types[2] == event_type {
            continue;
        }
        if unique_count > 3 && bloom_or_event_types[3] == event_type {
            continue;
        }

        // New unique event type
        if unique_count < 4 {
            bloom_or_event_types[unique_count] = event_type;
            unique_count += 1;
        } else {
            use_bloom = true;
            break;
        }
    }

    (bloom_or_event_types, use_bloom)
}

fn get_server_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
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

async fn load_header(
    dma_file: &mut DmaFile,
    file_len: u64,
) -> Result<ShardLogHeader, ShardLogWriteError> {
    let header_bytes = dma_file.read_at(0, FIXED_BLOCK_SIZE_BYTES).await?;
    match deserialize_shard_log_header_versioned(&header_bytes) {
        Ok((shard_log_header, _version)) => return Ok(shard_log_header),
        Err(_) => {
            let header_bytes = dma_file
                .read_at(
                    file_len.saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64),
                    FIXED_BLOCK_SIZE_BYTES,
                )
                .await?;
            return Ok(deserialize_shard_log_header_versioned(&header_bytes)?.0);
        }
    }
}

async fn write_dual_shard_log_header(
    dma_file: &mut DmaFile,
    header_end_start_pos: u64,
    header: &ShardLogHeader,
) -> Result<(), ShardLogWriteError> {
    let mut header_bytes = dma_file.alloc_dma_buffer(FIXED_BLOCK_SIZE_BYTES);
    serialize_fixed_len_with_version(
        &header,
        celeriant_wal::shard_log_header::CURRENT_VERSION,
        header_bytes.as_bytes_mut(),
    )?;
    dma_file.write_at(header_bytes, 0).await?;

    let mut header_bytes = dma_file.alloc_dma_buffer(FIXED_BLOCK_SIZE_BYTES);
    serialize_fixed_len_with_version(
        &header,
        celeriant_wal::shard_log_header::CURRENT_VERSION,
        header_bytes.as_bytes_mut(),
    )?;
    dma_file
        .write_at(header_bytes, header_end_start_pos)
        .await?;

    Ok(())
}

async fn setup_new_file(
    dma_file: &mut DmaFile,
    file_len: u64,
) -> Result<ShardLogHeader, ShardLogWriteError> {
    let header = ShardLogHeader {
        metablocks_position: FIXED_BLOCK_SIZE_BYTES as u64,
        datablocks_position: file_len.saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64),
    };

    write_dual_shard_log_header(dma_file, header.datablocks_position, &header).await?;

    dma_file.fdatasync().await?;

    //TODO: Folder fsync

    Ok(header)
}

#[cfg(test)]
mod test_shard_write_ahead_log {
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
                    .filter(|e| {
                        e.path()
                            .extension()
                            .map(|ext| ext == "wal")
                            .unwrap_or(false)
                    })
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
                    ..Default::default()
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
                    ..Default::default()
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

                    wal.close().await.unwrap();
                }
            })
            .unwrap();
        handle.join().unwrap();
    }
}
