use fastbloom::BloomFilter;
use io_uring::{IoUring, opcode, types};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::io::AsRawFd;

use crate::structures::compression_type::CompressionType;
use crate::structures::constants::{BINCODE_CONFIG_FIXED, BLOOM_HASH_COUNT};
use crate::structures::event_batch_item::EventBatchItem;
use crate::structures::event_batch_metadata::EventTypesData;
use crate::structures::{
    constants::{BLOOM_BYTES, METADATA_BATCH_SIZE_BYTES},
    event_batch_metadata::EventBatchMetadata,
    read_filters::ReadFilters,
    read_result::ReadResult,
};

/// Reads event batches from a binary stream with filtering and pagination support
///
/// # Arguments
/// * `event_batch_reader` - Any readable and seekable source (File, Cursor<Vec<u8>>, etc.) which contains all the event batches
/// * `metadata_reader` - Any readable and seekable source (File, Cursor<Vec<u8>>, etc.) which contains all the metadata for each batch
/// * `filters` - Filtering and pagination options
///
/// # Returns
/// * `ReadResult` containing filtered event batches and optional pagination token
///
/// # Errors
/// If corruption is detected, an error is raised.
pub fn filtered_read<R: Read + Seek + AsRawFd>(
    event_batch_reader: &mut R,
    metadata_reader: &mut R,
    filters: &ReadFilters,
) -> io::Result<ReadResult> {
    let file_len_event_batch = event_batch_reader.seek(SeekFrom::End(0))?;
    let file_len_metadata = metadata_reader.seek(SeekFrom::End(0))?;

    // Read first metadata entry at start of metadata file. Find our minimum server_id.
    // If we don't have the required server_ids (file was trimmed), error back to caller. They need to pull from S3 instead perhaps.
    let minimum_available_server_id =
        get_minimum_available_server_id(metadata_reader, filters.from_server_id)?;

    // Calculate the offset in the metadata file to start reading metadata chunks
    let start_reading_metadata_from_offset_position =
        (filters.from_server_id - minimum_available_server_id) * METADATA_BATCH_SIZE_BYTES as u64;

    // Calculate how many metadata entries we can read
    let remaining_metadata_bytes =
        file_len_metadata.saturating_sub(start_reading_metadata_from_offset_position);
    let max_metadata_entries = (remaining_metadata_bytes / METADATA_BATCH_SIZE_BYTES as u64) as usize;

    // Handle scenario where client requests server_id that hasn't been written yet
    if max_metadata_entries == 0 {
        return Ok(ReadResult {
            event_batches: Vec::new(),
            next_server_id: None,
        });
    }

    // Use io_uring to pull all the metadata entries we need into memory into EventBatchMetadata structs
    // Mark each event batch as 'include' or 'exclude' based on the client's filter requirements
    // Also keep track of next_server_id - we might not be able to return all event batches as we exceeded the max_bytes limit!
    // Also calculate the absolute position (start and end pos) for each event batch blob in the event_batch file (metadata only stores batch lengths)
    // We can do all this inside io_uring collect phase as although its concurrent, the order is maintained (queue design)

    // Phase 1: Read and filter metadata entries using io_uring
    let mut batches = read_metadata_entries_with_uring(
        metadata_reader,
        start_reading_metadata_from_offset_position,
        max_metadata_entries,
        filters,
    )?;

    calculate_absolute_positions(file_len_event_batch, &mut batches);

    let next_server_id: Option<u64> = trim_end_if_exceeds_max_bytes(&mut batches, filters.max_bytes);

    if batches.is_empty() {
        return Ok(ReadResult {
            event_batches: Vec::new(),
            next_server_id: None,
        });
    }

    // Now we know which event batches to pull, and where they are in the file, and their size in bytes. Pull them all into memory using io_uring
    // Decompress and deserialize within the collect phase of io_uring.
    // Note also we must do a final filter out of individual events if we need to filter by event type.
    // The bloom filter is not 100% accurate and metadata only stores 'in' types, not exclusive

    // Phase 3: Read event batches using io_uring
    let event_batches = read_event_batches_with_uring(event_batch_reader, &batches, filters)?;

    Ok(ReadResult {
        event_batches,
        next_server_id,
    })
}

fn trim_end_if_exceeds_max_bytes(
    batches: &mut Vec<MetadataBatchInfo>,
    max_bytes: Option<usize>,
) -> Option<u64> {
    // If no max_bytes limit is specified, we don't need to trim
    let max_bytes = match max_bytes {
        Some(limit) => limit as u64,
        None => return None,
    };

    // Only keep batches where include is true
    batches.retain(|batch| batch.include);

    // If after filtering we don't have any batches, return None
    if batches.is_empty() {
        return None;
    }

    // Calculate cumulative compressed size
    let mut cumulative_size: u64 = 0;
    let mut cut_index: Option<usize> = None;

    // Batches are sorted by server_id (ascending)
    for (index, batch) in batches.iter().enumerate() {
        cumulative_size += batch.compressed_size;
        
        // If we exceed the max_bytes limit, store this index as the cut point
        if cumulative_size > max_bytes {
            cut_index = Some(index);
            break;
        }
    }

    // If we need to trim
    if let Some(index) = cut_index {
        // Get the server_id of the first batch we're trimming
        let next_server_id = if index < batches.len() {
            Some(batches[index].server_id)
        } else {
            None
        };

        // Keep only the batches that fit within the max_bytes limit
        batches.truncate(index);
        
        next_server_id
    } else {
        // No trimming needed, all batches fit within the limit
        None
    }
}

#[derive(Debug)]
struct MetadataBatchInfo {
    server_id: u64,
    uncompressed_size: u64,
    compressed_size: u64,
    compression_type: u8,
    events_crc: u32,
    file_offset: u64,
    include: bool,
}

impl Default for MetadataBatchInfo {
    fn default() -> Self {
        Self { server_id: 0, uncompressed_size: 0, compressed_size: 0, compression_type: 0, events_crc: 0, file_offset: 0, include: false }
    }
}

fn read_metadata_entries_with_uring<R: Read + Seek + AsRawFd>(
    metadata_reader: &mut R,
    start_offset: u64,
    max_entries: usize,
    filters: &ReadFilters,
) -> io::Result<Vec<MetadataBatchInfo>> {
    let mut ring = IoUring::new(32)?;
    let fd = types::Fd(metadata_reader.as_raw_fd());

    // Prepare buffers for metadata entries
    let mut buffer_pool = Vec::with_capacity(max_entries);
    for _ in 0..max_entries {
        buffer_pool.push([0u8; METADATA_BATCH_SIZE_BYTES]);
    }

    // Submit read operations
    let mut submission_count = 0;
    for (i, buffer) in buffer_pool.iter_mut().enumerate().take(max_entries) {
        let offset = start_offset + (i as u64 * METADATA_BATCH_SIZE_BYTES as u64);
        let read_op = opcode::Read::new(fd, buffer.as_mut_ptr(), buffer.len() as u32)
            .offset(offset)
            .build()
            .user_data(i as u64);

        unsafe {
            if ring.submission().push(&read_op).is_err() {
                break; // Queue full, submit what we have
            }
        }
        submission_count += 1;
    }

    ring.submit_and_wait(submission_count)?;

    // Pre-allocate the result vector with the exact size
    let mut batch_entries = Vec::with_capacity(submission_count);
    for _ in 0..submission_count {
        // We'll create actual entries based on the order of completion
        batch_entries.push(MetadataBatchInfo::default());
    }

    // Collect results
    for _ in 0..submission_count {
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| io::Error::other("Missing completion entry"))?;

        let buffer_index = cqe.user_data() as usize;
        let bytes_read = cqe.result();

        if (bytes_read as usize) < METADATA_BATCH_SIZE_BYTES {
            return Err(io::Error::other(format!("Read error: {}", bytes_read)));
        }

        let metadata: EventBatchMetadata = bincode::decode_from_slice(
            &buffer_pool[buffer_index],
            BINCODE_CONFIG_FIXED,
        )
        .map_err(|e| io::Error::other(e.to_string()))?
        .0;

        batch_entries[buffer_index].include = is_include_batch(&metadata, filters);
        batch_entries[buffer_index].server_id = metadata.server_id;
        batch_entries[buffer_index].uncompressed_size = metadata.uncompressed_size;
        batch_entries[buffer_index].compressed_size = metadata.compressed_size;
        batch_entries[buffer_index].compression_type = metadata.compression_type;
        batch_entries[buffer_index].events_crc = metadata.events_crc;
    }

    Ok(batch_entries)
}

fn is_include_batch(metadata: &EventBatchMetadata, filters: &ReadFilters) -> bool {
    if metadata.server_id < filters.from_server_id {
        return false;
    }

    if filters.to_server_id.map_or(false, |to_server_id| metadata.server_id > to_server_id) {
        return false;
    }

    if filters.before_server_time.map_or(false, |before_server_time| metadata.server_time < before_server_time) {
        return false;
    }

    if filters.after_server_time.map_or(false, |after_server_time| metadata.server_time > after_server_time) {
        return false;
    }

    if filters.exclude_client_id.map_or(false, |exclude_client_id| metadata.client_id == exclude_client_id) {
        return false;
    }

    if filters.include_client_id.map_or(false, |include_client_id| metadata.client_id != include_client_id) {
        return false;
    }

    if filters.exclude_user_id.map_or(false, |exclude_user_id| metadata.user_id == exclude_user_id) {
        return false;
    }

    if filters.include_user_id.map_or(false, |include_user_id| metadata.user_id != include_user_id) {
        return false;
    }

    if filters.min_local_index.map_or(false, |min_index| metadata.max_local_index < min_index) {
        return false;
    }

    if filters.max_local_index.map_or(false, |max_index| metadata.min_local_index > max_index) {
        return false;
    }

    if filters.min_event_time.map_or(false, |min_time| metadata.max_event_time < min_time) {
        return false;
    }

    if filters.max_event_time.map_or(false, |max_time| metadata.min_event_time > max_time) {
        return false;
    }

    if filters.include_event_types.map_or(false, |include_event_types| {
        //Is there at least one of the include_event_types in the event batch? If not, return true to skip
        let at_least_one_match = check_event_types_match(&metadata.event_types_data, include_event_types);
        !at_least_one_match
    }) {
        return false;
    }

    true
}

fn calculate_absolute_positions(
    event_batches_file_len: u64,
    batches: &mut [MetadataBatchInfo],
) {
    let mut current_offset = 0u64;

    for batch in batches.iter_mut().rev() {
        current_offset += batch.compressed_size;
        batch.file_offset = event_batches_file_len - current_offset;
    }
}

fn check_event_types_match(event_types_data: &EventTypesData, include_event_types: &[u64]) -> bool {
    match event_types_data {
        EventTypesData::Direct(event_types) => {
            // Check if any of the required types are in the direct array
            if event_types.len() < include_event_types.len() {
                event_types.iter().any(|&batch_type| include_event_types.contains(&batch_type))
            } else {
                include_event_types.iter().any(|&include_event_type| event_types.contains(&include_event_type))
            }
        }
        EventTypesData::Bloom(bloom_bytes) => {
            // Create bloom filter and test each required type
            let bloom = bloom_filter_from_bytes(bloom_bytes, BLOOM_HASH_COUNT);
            include_event_types.iter().any(|&include_event_type| bloom.contains(&include_event_type))
        }
    }
}

fn read_event_batches_with_uring<R: Read + Seek + AsRawFd>(
    event_batch_reader: &mut R,
    batch_info: &[MetadataBatchInfo],
    filters: &ReadFilters,
) -> io::Result<Vec<EventBatchItem>> {
    let included_batches: Vec<&MetadataBatchInfo> = batch_info.iter().filter(|b| b.include).collect();

    if included_batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut ring = IoUring::new(32)?;
    let fd = types::Fd(event_batch_reader.as_raw_fd());

    // Prepare buffers
    let mut buffers: Vec<Vec<u8>> = included_batches
        .iter()
        .map(|batch| vec![0u8; batch.compressed_size as usize])
        .collect();

    // Submit read operations
    let mut submission_count = 0;
    for (i, (buffer, batch)) in buffers.iter_mut().zip(&included_batches).enumerate() {
        let read_op = opcode::Read::new(fd, buffer.as_mut_ptr(), buffer.len() as u32)
            .offset(batch.file_offset)
            .build()
            .user_data(i as u64);

        unsafe {
            if ring.submission().push(&read_op).is_err() {
                break;
            }
        }
        submission_count += 1;
    }

    ring.submit_and_wait(submission_count)?;

    // Collect and decompress results
    let mut event_batches = Vec::with_capacity(submission_count);
    for _ in 0..submission_count {
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| io::Error::other("Missing completion entry"))?;

        let buffer_index = cqe.user_data() as usize;
        let bytes_read = cqe.result();

        if bytes_read < 0 {
            return Err(io::Error::other(format!("Read error: {}", bytes_read)));
        }

        let batch = &included_batches[buffer_index];
        let buffer = &buffers[buffer_index];

        // Verify CRC
        let crc = crc32fast::hash(&buffer[..bytes_read as usize]);
        if crc != batch.events_crc {
            return Err(io::Error::other("CRC mismatch in event batch"));
        }

        // Decompress and deserialize
        let compression_type = CompressionType::from_tuple(batch.compression_type, None);
        let mut event_batch = EventBatchItem::from_wire_format(
            &buffer[..bytes_read as usize],
            compression_type,
            batch.uncompressed_size as usize,
        )?;

        // Final event type filtering (bloom filter might have false positives)
        if let Some(event_types) = filters.include_event_types {
            event_batch
                .events
                .retain(|event| event_types.contains(&event.event_type_major));
        }

        // Final filtering (metadata only contains min/max ranges)
        if let Some(event_types) = filters.include_event_types {
            event_batch
                .events
                .retain(|event| event_types.contains(&event.event_type_major));
        }

        // Final filtering for local_index
        if let Some(min_local_index) = filters.min_local_index {
            event_batch
                .events
                .retain(|event| event.local_index >= min_local_index);
        }

        if let Some(max_local_index) = filters.max_local_index {
            event_batch
                .events
                .retain(|event| event.local_index <= max_local_index);
        }

        // Final filtering for event_time
        if let Some(min_event_time) = filters.min_event_time {
            event_batch
                .events
                .retain(|event| event.event_time >= min_event_time);
        }

        if let Some(max_event_time) = filters.max_event_time {
            event_batch
                .events
                .retain(|event| event.event_time <= max_event_time);
        }

        if !event_batch.events.is_empty() {
            event_batches.push(event_batch);
        }
    }

    // Sort by server_id to maintain order
    event_batches.sort_by_key(|batch| batch.server_id);

    Ok(event_batches)
}

fn get_minimum_available_server_id<R: Read + Seek>(
    metadata_reader: &mut R,
    requested_server_id: u64,
) -> io::Result<u64> {
    metadata_reader.seek(SeekFrom::Start(0))?;
    let mut buffer = vec![0u8; METADATA_BATCH_SIZE_BYTES as usize];
    let bytes_read = metadata_reader.read(&mut buffer)?;

    if bytes_read < METADATA_BATCH_SIZE_BYTES {
        return Err(io::Error::other(
            "Insufficient metadata to determine minimum server ID",
        ));
    }

    let first_metadata: EventBatchMetadata =
        bincode::decode_from_slice(&buffer, BINCODE_CONFIG_FIXED)
            .map_err(|e| io::Error::other(e.to_string()))?
            .0;

    if first_metadata.server_id > requested_server_id {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "Requested server_id {} is not available, minimum is {}",
                requested_server_id, first_metadata.server_id
            ),
        ));
    }

    Ok(first_metadata.server_id)
}

/// Get the most recent server id used when storing events.
///
/// # Arguments
/// * `reader` - Any readable and seekable source (File, Cursor<Vec<u8>>, etc.)
///
/// # Returns
/// * Server id (u64) for the last event batch save
pub fn last_server_id<R: Read + Seek>(reader: &mut R) -> io::Result<u64> {
    Ok(0)
}

/// For a provided client, get the most recent local id they used when storing events.
///
/// # Arguments
/// * `reader` - Any readable and seekable source (File, Cursor<Vec<u8>>, etc.)
///
/// # Returns
/// * None if the client never saved any event batches
/// * Local id (u64) for the last event batch save that the client used
pub fn last_local_id<R: Read + Seek>(reader: &mut R) -> io::Result<Option<u64>> {
    Ok(None)
}

/// Find the first position in the file where the data is corrupt.
///
/// # Arguments
/// * `reader` - Any readable and seekable source (File, Cursor<Vec<u8>>, etc.)
///
/// # Returns
/// * None if the file is not corrupt
/// * File position where the data in the file is beginning to be corrupt
pub fn detect_corruption<R: Read + Seek>(reader: &mut R) -> io::Result<Option<u64>> {
    Ok(None)
}

fn bloom_filter_from_bytes(bloom_bytes: &[u8; BLOOM_BYTES], num_hashes: u32) -> BloomFilter {
    // Convert bytes back to Vec<u64>
    let mut u64_vec = Vec::with_capacity(BLOOM_BYTES / 8); // eg. 128 bytes = 16 u64s

    for chunk in bloom_bytes.chunks_exact(8) {
        let u64_val = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        u64_vec.push(u64_val);
    }

    BloomFilter::from_vec(u64_vec).hashes(num_hashes)
}
