use fastbloom::BloomFilter;
use io_uring::{IoUring, opcode, types};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::io::AsRawFd;

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
pub fn filtered_read<R: Read + Seek>(
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
    let max_metadata_entries = remaining_metadata_bytes / METADATA_BATCH_SIZE_BYTES as u64;

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

    // Phase 1: Read metadata entries using io_uring
    let metadata_entries = read_metadata_entries_with_uring(
        metadata_reader,
        start_reading_metadata_from_offset_position,
        max_metadata_entries,
        filters,
    )?;

    // Phase 2: Filter metadata and calculate event batch positions
    let batch_info = filter_and_calculate_positions(&metadata_entries, filters)?;

    if batch_info.is_empty() {
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
    let event_batches = read_event_batches_with_uring(event_batch_reader, &batch_info, filters)?;

    let next_server_id = if let Some(last_batch) = event_batches.last() {
        Some(last_batch.server_id + 1)
    } else {
        None
    };

    Ok(ReadResult {
        event_batches,
        next_server_id,
    })
}

#[derive(Debug)]
struct BatchInfo {
    metadata: EventBatchMetadata,
    file_offset: u64,
    include: bool,
}

fn read_metadata_entries_with_uring<R: Read + Seek + AsRawFd>(
    metadata_reader: &mut R,
    start_offset: u64,
    max_entries: usize,
    filters: &ReadFilters,
) -> io::Result<Vec<EventBatchMetadata>> {
    let mut ring = IoUring::new(32)?;
    let fd = types::Fd(metadata_reader.as_raw_fd());

    // Prepare buffers for metadata entries
    let mut buffers: Vec<Vec<u8>> = (0..max_entries)
        .map(|_| vec![0u8; METADATA_BATCH_SIZE_BYTES as usize])
        .collect();

    // Submit read operations
    let mut submission_count = 0;
    for (i, buffer) in buffers.iter_mut().enumerate().take(max_entries) {
        let offset = start_offset + (i as u64 * METADATA_BATCH_SIZE_BYTES);
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

    // Collect results
    let mut metadata_entries = Vec::new();
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

        if bytes_read as usize >= std::mem::size_of::<EventBatchMetadata>() {
            let metadata = bincode::decode_from_slice(
                &buffers[buffer_index][..bytes_read as usize],
                bincode::config::standard(),
            )
            .map_err(|e| io::Error::other(e.to_string()))?
            .0;

            metadata_entries.push(metadata);
        }
    }

    // Sort by server_id to maintain order
    metadata_entries.sort_by_key(|m| m.server_id);

    Ok(metadata_entries)
}

fn filter_and_calculate_positions(
    metadata_entries: &[EventBatchMetadata],
    filters: &ReadFilters,
) -> io::Result<Vec<BatchInfo>> {
    let mut batch_info = Vec::new();
    let mut current_offset = 0u64;
    let mut total_bytes = 0usize;

    for metadata in metadata_entries {
        // Check server_id range
        if let Some(to_server_id) = filters.to_server_id {
            if metadata.server_id > to_server_id {
                break;
            }
        }

        // Apply filters
        let mut include = true;

        // Client ID filters
        if let Some(exclude_client) = filters.exclude_client_id {
            if metadata.client_id == exclude_client {
                include = false;
            }
        }
        if let Some(include_client) = filters.include_client_id {
            if metadata.client_id != include_client {
                include = false;
            }
        }

        // User ID filters
        if let Some(exclude_user) = filters.exclude_user_id {
            if metadata.user_id == exclude_user {
                include = false;
            }
        }
        if let Some(include_user) = filters.include_user_id {
            if metadata.user_id != include_user {
                include = false;
            }
        }

        // Time filters
        if let Some(after_time) = filters.after_server_time {
            if metadata.server_time <= after_time {
                include = false;
            }
        }
        if let Some(before_time) = filters.before_server_time {
            if metadata.server_time >= before_time {
                include = false;
            }
        }

        // Event type filter (preliminary check using bloom filter or direct array)
        if let Some(event_types) = filters.include_event_types {
            include = include && check_event_types_match(&metadata.event_types_data, event_types);
        }

        // Check max_bytes limit
        if include {
            if let Some(max_bytes) = filters.max_bytes {
                if total_bytes + metadata.compressed_size as usize > max_bytes {
                    break;
                }
            }
            total_bytes += metadata.compressed_size as usize;
        }

        batch_info.push(BatchInfo {
            metadata: metadata.clone(),
            file_offset: current_offset,
            include,
        });

        current_offset += metadata.compressed_size;
    }

    Ok(batch_info)
}

fn check_event_types_match(event_types_data: &EventTypesData, filter_types: &[u64]) -> bool {
    match event_types_data {
        EventTypesData::Direct(types) => {
            // Check if any of the required types are in the direct array
            filter_types.iter().any(|&filter_type| {
                types
                    .iter()
                    .any(|&batch_type| batch_type != u64::MAX && batch_type == filter_type)
            })
        }
        EventTypesData::Bloom(bloom_bytes) => {
            // Create bloom filter and test each required type
            let mut bloom = BloomFilter::with_num_bits(BLOOM_BYTES * 8);
            // Copy bloom bytes into filter (implementation depends on fastbloom API)
            // This is a preliminary check - we'll do final filtering after decompression
            true // For now, include all bloom filter batches for final filtering
        }
    }
}

fn read_event_batches_with_uring<R: Read + Seek + AsRawFd>(
    event_batch_reader: &mut R,
    batch_info: &[BatchInfo],
    filters: &ReadFilters,
) -> io::Result<Vec<EventBatchItem>> {
    let included_batches: Vec<&BatchInfo> = batch_info.iter().filter(|b| b.include).collect();

    if included_batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut ring = IoUring::new(32)?;
    let fd = types::Fd(event_batch_reader.as_raw_fd());

    // Prepare buffers
    let mut buffers: Vec<Vec<u8>> = included_batches
        .iter()
        .map(|batch| vec![0u8; batch.metadata.compressed_size as usize])
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
    let mut event_batches = Vec::new();
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
        if crc != batch.metadata.events_crc {
            return Err(io::Error::other("CRC mismatch in event batch"));
        }

        // Decompress and deserialize
        let compression_type = CompressionType::from_tuple((batch.metadata.compression_type, 0));
        let mut event_batch = EventBatchItem::from_wire_format(
            &buffer[..bytes_read as usize],
            compression_type,
            batch.metadata.uncompressed_size as usize,
        )?;

        // Final event type filtering (bloom filter might have false positives)
        if let Some(event_types) = filters.include_event_types {
            event_batch
                .events
                .retain(|event| event_types.contains(&event.event_type_major));
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
        bincode::decode_from_slice(&buffer, bincode::config::standard())
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
