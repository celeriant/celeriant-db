use fastbloom::BloomFilter;

#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};

use std::io::{self, Read, Seek, SeekFrom};

// Platform-specific raw file descriptor traits
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

use crate::stateless::stateless_engine::StatelessEngine;
use crate::structures::compression_type::CompressionType;
use crate::structures::constants::{BINCODE_CONFIG_FIXED, BLOOM_HASH_COUNT, BLOOM_HASH_SEED};
use crate::structures::event_batch_item::EventBatchItem;
use crate::structures::event_batch_metadata::EventTypesData;
use crate::structures::{
    constants::{BLOOM_BYTES, METADATA_BATCH_SIZE_BYTES},
    event_batch_metadata::EventBatchMetadata,
    read_filters::ReadFilters,
    read_result::ReadResult,
};

pub trait StatelessReader {
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
    #[cfg(unix)]
    fn read_filtered<R: Read + Seek + AsRawFd>(
        &self,
        event_batch_reader: &mut R,
        metadata_reader: &mut R,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult>;

    #[cfg(windows)]
    fn read_filtered<R: Read + Seek + AsRawHandle>(
        &self,
        event_batch_reader: &mut R,
        metadata_reader: &mut R,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult>;

    /// Reads event batches from a binary stream with filtering and pagination support. This reads without using io_uring.
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
    fn read_filtered_standard<R: Read + Seek>(
        &self,
        event_batch_reader: &mut R,
        metadata_reader: &mut R,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult>;

    /// Get the most recent server id used when storing events.
    ///
    /// # Arguments
    /// * `reader` - Any readable and seekable source (File, Cursor<Vec<u8>>, etc.)
    ///
    /// # Returns
    /// * Server id (u64) for the last event batch save
    fn last_event_batch_index<R: Read + Seek>(&self, metadata_reader: &mut R) -> io::Result<u64>;

    /// For a provided client, get the most recent local id they used when storing events.
    ///
    /// # Arguments
    /// * `reader` - Any readable and seekable source (File, Cursor<Vec<u8>>, etc.)
    ///
    /// # Returns
    /// * None if the client never saved any event batches
    /// * Local id (u64) for the last event batch save that the client used
    fn last_local_index<R: Read + Seek>(&self, metadata_reader: &mut R) -> io::Result<u64>;

    /// Find the first position in the files where the data is corrupt.
    ///
    /// # Arguments
    /// * `reader` - Any readable and seekable source (File, Cursor<Vec<u8>>, etc.)
    ///
    /// # Returns
    /// * None if the file is not corrupt
    /// * File positions where the data in the file is beginning to be corrupt
    fn detect_corruption<R: Read + Seek>(
        &self,
        event_batch_reader: &mut R,
        metadata_reader: &mut R,
    ) -> io::Result<Option<CorruptPositions>>;
}

pub struct CorruptPositions {
    pub metadata_position: u64,
    pub event_batch_position: u64,
}

impl StatelessReader for StatelessEngine {
    fn last_event_batch_index<R: Read + Seek>(&self, metadata_reader: &mut R) -> io::Result<u64> {
        // Get file length to find the last metadata entry
        let file_len = metadata_reader.seek(SeekFrom::End(0))?;

        // Check if file has at least one metadata entry
        if file_len < METADATA_BATCH_SIZE_BYTES as u64 {
            return Err(io::Error::other(
                "Metadata file is too small to contain any entries",
            ));
        }

        // Seek to the start of the last metadata entry
        let last_entry_offset = file_len - METADATA_BATCH_SIZE_BYTES as u64;
        metadata_reader.seek(SeekFrom::Start(last_entry_offset))?;

        // Read the last metadata entry
        let mut buffer = [0u8; METADATA_BATCH_SIZE_BYTES];
        let bytes_read = metadata_reader.read(&mut buffer)?;

        if bytes_read < METADATA_BATCH_SIZE_BYTES {
            return Err(io::Error::other("Failed to read complete metadata entry"));
        }

        // Deserialize the metadata
        let metadata: EventBatchMetadata =
            bincode::decode_from_slice(&buffer, BINCODE_CONFIG_FIXED)
                .map_err(|e| io::Error::other(e.to_string()))?
                .0;

        Ok(metadata.event_batch_index)
    }

    fn last_local_index<R: Read + Seek>(&self, metadata_reader: &mut R) -> io::Result<u64> {
        // Get file length to check if we have any metadata entries
        let file_len = metadata_reader.seek(SeekFrom::End(0))?;

        // Check if file has at least one metadata entry
        if file_len < METADATA_BATCH_SIZE_BYTES as u64 {
            return Err(io::Error::other(
                "Metadata file is too small to contain any entries",
            ));
        }

        // Seek to the start of the last metadata entry
        let last_entry_offset = file_len - METADATA_BATCH_SIZE_BYTES as u64;
        metadata_reader.seek(SeekFrom::Start(last_entry_offset))?;

        // Read the last metadata entry
        let mut buffer = [0u8; METADATA_BATCH_SIZE_BYTES];
        let bytes_read = metadata_reader.read(&mut buffer)?;

        if bytes_read < METADATA_BATCH_SIZE_BYTES {
            return Err(io::Error::other(
                "Metadata file is too small to contain any entries",
            ));
        }

        // Deserialize the metadata
        let metadata: EventBatchMetadata =
            bincode::decode_from_slice(&buffer, BINCODE_CONFIG_FIXED)
                .map_err(|e| io::Error::other(e.to_string()))?
                .0;

        Ok(metadata.max_client_event_index)
    }

    fn detect_corruption<R: Read + Seek>(
        &self,
        event_batch_reader: &mut R,
        metadata_reader: &mut R,
    ) -> io::Result<Option<CorruptPositions>> {
        // Check if either file has size 0
        let metadata_file_len = metadata_reader.seek(SeekFrom::End(0))?;
        let event_batch_file_len = event_batch_reader.seek(SeekFrom::End(0))?;

        if metadata_file_len == 0 || event_batch_file_len == 0 {
            return Ok(Some(CorruptPositions {
                metadata_position: 0,
                event_batch_position: 0,
            }));
        }

        // Reset to start of metadata file
        metadata_reader.seek(SeekFrom::Start(0))?;

        let mut last_valid_metadata_position = 0u64;
        let mut last_valid_event_batch_position = 0u64;
        let mut current_event_batch_position = 0u64;

        let mut buffer = [0u8; METADATA_BATCH_SIZE_BYTES];

        loop {
            let current_metadata_position = metadata_reader.stream_position()?;

            // Try to read metadata entry
            let bytes_read = metadata_reader.read(&mut buffer)?;

            // Check if we have enough bytes for a complete metadata entry
            if bytes_read < METADATA_BATCH_SIZE_BYTES {
                // If we're at the end of file and read some bytes but not enough, it's corrupt
                if bytes_read > 0 {
                    return Ok(Some(CorruptPositions {
                        metadata_position: last_valid_metadata_position,
                        event_batch_position: last_valid_event_batch_position,
                    }));
                }
                // If we read 0 bytes, we've reached the end successfully
                break;
            }

            // Try to deserialize metadata
            let metadata: EventBatchMetadata =
                match bincode::decode_from_slice(&buffer, BINCODE_CONFIG_FIXED) {
                    Ok((metadata, _)) => metadata,
                    Err(_) => {
                        // Metadata deserialization failed - corruption detected
                        return Ok(Some(CorruptPositions {
                            metadata_position: last_valid_metadata_position,
                            event_batch_position: last_valid_event_batch_position,
                        }));
                    }
                };

            // Check if we have enough bytes in the event batch file for this batch
            if current_event_batch_position + metadata.compressed_size > event_batch_file_len {
                // Not enough bytes in event batch file - corruption detected
                return Ok(Some(CorruptPositions {
                    metadata_position: last_valid_metadata_position,
                    event_batch_position: last_valid_event_batch_position,
                }));
            }

            // Seek to the event batch position and read the compressed data
            event_batch_reader.seek(SeekFrom::Start(current_event_batch_position))?;
            let mut event_batch_buffer = vec![0u8; metadata.compressed_size as usize];
            let event_bytes_read = event_batch_reader.read(&mut event_batch_buffer)?;

            // Check if we read the expected number of bytes
            if event_bytes_read != metadata.compressed_size as usize {
                // Not enough bytes read from event batch - corruption detected
                return Ok(Some(CorruptPositions {
                    metadata_position: last_valid_metadata_position,
                    event_batch_position: last_valid_event_batch_position,
                }));
            }

            // Verify CRC of the event batch
            let calculated_crc = crc32fast::hash(&event_batch_buffer);
            if calculated_crc != metadata.events_crc {
                // CRC mismatch - corruption detected
                return Ok(Some(CorruptPositions {
                    metadata_position: last_valid_metadata_position,
                    event_batch_position: last_valid_event_batch_position,
                }));
            }

            // If we've made it here, this batch is valid
            last_valid_metadata_position =
                current_metadata_position + METADATA_BATCH_SIZE_BYTES as u64;
            last_valid_event_batch_position =
                current_event_batch_position + metadata.compressed_size;
            current_event_batch_position += metadata.compressed_size;
        }

        // No corruption detected
        Ok(None)
    }

    #[cfg(unix)]
    fn read_filtered<R: Read + Seek + AsRawFd>(
        &self,
        event_batch_reader: &mut R,
        metadata_reader: &mut R,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult> {
        #[cfg(target_os = "linux")]
        {
            if self.is_io_uring_available() {
                return internal_read_filtered_io_uring(
                    event_batch_reader,
                    metadata_reader,
                    filters,
                    self.io_uring_queue_depth
                );
            }
        }

        self.read_filtered_standard(event_batch_reader, metadata_reader, filters)
    }

    #[cfg(windows)]
    fn read_filtered<R: Read + Seek + AsRawHandle>(
        &self,
        event_batch_reader: &mut R,
        metadata_reader: &mut R,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult> {
        // On Windows, always fall back to standard reading
        self.read_filtered_standard(event_batch_reader, metadata_reader, filters)
    }

    fn read_filtered_standard<R: Read + Seek>(
        &self,
        event_batch_reader: &mut R,
        metadata_reader: &mut R,
        filters: &ReadFilters,
    ) -> io::Result<ReadResult> {
        internal_read_filtered_standard(event_batch_reader, metadata_reader, filters)
    }
}

#[cfg(target_os = "linux")]
fn internal_read_filtered_io_uring<R: Read + Seek + AsRawFd>(
    event_batch_reader: &mut R,
    metadata_reader: &mut R,
    filters: &ReadFilters,
    io_uring_queue_depth: u32,
) -> io::Result<ReadResult> {
    let file_len_event_batch = event_batch_reader.seek(SeekFrom::End(0))?;
    let file_len_metadata = metadata_reader.seek(SeekFrom::End(0))?;

    // Read first metadata entry at start of metadata file. Find our minimum server_id.
    // If we don't have the required server_ids (file was trimmed), error back to caller. They need to pull from S3 instead perhaps.
    let minimum_available_server_id =
        get_minimum_available_server_id(metadata_reader, filters.from_event_batch_index)?;

    // Calculate the offset in the metadata file to start reading metadata chunks
    let start_reading_metadata_from_offset_position =
        (filters.from_event_batch_index - minimum_available_server_id) * METADATA_BATCH_SIZE_BYTES as u64;

    // Calculate how many metadata entries we can read
    let remaining_metadata_bytes =
        file_len_metadata.saturating_sub(start_reading_metadata_from_offset_position);
    let max_metadata_entries =
        (remaining_metadata_bytes / METADATA_BATCH_SIZE_BYTES as u64) as usize;

    // Handle scenario where client requests server_id that hasn't been written yet
    if max_metadata_entries == 0 {
        return Ok(ReadResult {
            event_batches: Vec::new(),
            next_event_batch_index: None,
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
        io_uring_queue_depth
    )?;

    calculate_absolute_positions(file_len_event_batch, &mut batches);

    let next_server_id: Option<u64> =
        trim_end_if_exceeds_max_bytes(&mut batches, filters.max_bytes);

    if batches.is_empty() {
        return Ok(ReadResult {
            event_batches: Vec::new(),
            next_event_batch_index: None,
        });
    }

    // Now we know which event batches to pull, and where they are in the file, and their size in bytes. Pull them all into memory using io_uring
    // Decompress and deserialize within the collect phase of io_uring.
    // Note also we must do a final filter out of individual events if we need to filter by event type.
    // The bloom filter is not 100% accurate and metadata only stores 'in' types, not exclusive

    // Phase 3: Read event batches using io_uring
    let event_batches = read_event_batches_with_uring(event_batch_reader, &batches, filters, io_uring_queue_depth)?;

    Ok(ReadResult {
        event_batches,
        next_event_batch_index: next_server_id,
    })
}

fn bloom_filter_from_bytes(bloom_bytes: &[u64; BLOOM_BYTES/8]) -> BloomFilter {
    BloomFilter::from_vec(bloom_bytes.to_vec()).seed(&BLOOM_HASH_SEED).hashes(BLOOM_HASH_COUNT)
}

fn trim_end_if_exceeds_max_bytes(
    batches: &mut Vec<MetadataBatchInfo>,
    max_bytes: Option<usize>,
) -> Option<u64> {

    // Only keep batches where include is true
    batches.retain(|batch| batch.include);

    // If no max_bytes limit is specified, we don't need to trim
    let max_bytes = match max_bytes {
        Some(limit) => limit as u64,
        None => return None,
    };

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
        Self {
            server_id: 0,
            uncompressed_size: 0,
            compressed_size: 0,
            compression_type: 0,
            events_crc: 0,
            file_offset: 0,
            include: false,
        }
    }
}

#[cfg(target_os = "linux")]
fn read_metadata_entries_with_uring<R: Read + Seek + AsRawFd>(
    metadata_reader: &mut R,
    start_offset: u64,
    max_entries: usize,
    filters: &ReadFilters,
    io_uring_queue_depth: u32,
) -> io::Result<Vec<MetadataBatchInfo>> {
    let mut ring = IoUring::new(io_uring_queue_depth)?;
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

        let metadata: EventBatchMetadata =
            bincode::decode_from_slice(&buffer_pool[buffer_index], BINCODE_CONFIG_FIXED)
                .map_err(|e| io::Error::other(e.to_string()))?
                .0;

        batch_entries[buffer_index].include = is_include_batch(&metadata, filters);
        batch_entries[buffer_index].server_id = metadata.event_batch_index;
        batch_entries[buffer_index].uncompressed_size = metadata.uncompressed_size;
        batch_entries[buffer_index].compressed_size = metadata.compressed_size;
        batch_entries[buffer_index].compression_type = metadata.compression_type;
        batch_entries[buffer_index].events_crc = metadata.events_crc;
    }

    Ok(batch_entries)
}

fn is_include_batch(metadata: &EventBatchMetadata, filters: &ReadFilters) -> bool {
    if metadata.event_batch_index < filters.from_event_batch_index {
        return false;
    }

    if filters
        .to_event_batch_index
        .map_or(false, |to_server_id| metadata.event_batch_index > to_server_id)
    {
        return false;
    }

    if filters
        .min_server_timestamp
        .map_or(false, |before_server_time| {
            metadata.server_timestamp < before_server_time
        })
    {
        return false;
    }

    if filters
        .max_server_timestamp
        .map_or(false, |after_server_time| {
            metadata.server_timestamp > after_server_time
        })
    {
        return false;
    }

    if filters
        .exclude_client_id
        .map_or(false, |exclude_client_id| {
            metadata.client_id == exclude_client_id
        })
    {
        return false;
    }

    if filters
        .include_client_id
        .map_or(false, |include_client_id| {
            metadata.client_id != include_client_id
        })
    {
        return false;
    }

    if filters
        .exclude_user_id
        .map_or(false, |exclude_user_id| metadata.user_id == exclude_user_id)
    {
        return false;
    }

    if filters
        .include_user_id
        .map_or(false, |include_user_id| metadata.user_id != include_user_id)
    {
        return false;
    }

    if filters
        .min_client_event_index
        .map_or(false, |min_index| metadata.max_client_event_index < min_index)
    {
        return false;
    }

    if filters
        .max_client_event_index
        .map_or(false, |max_index| metadata.min_client_event_index > max_index)
    {
        return false;
    }

    if filters
        .min_event_timestamp
        .map_or(false, |min_time| metadata.max_event_timestamp < min_time)
    {
        return false;
    }

    if filters
        .max_event_timestamp
        .map_or(false, |max_time| metadata.min_event_timestamp > max_time)
    {
        return false;
    }

    if filters
        .min_event_index
        .map_or(false, |min_index| metadata.max_event_index < min_index)
    {
        return false;
    }

    if filters
        .max_event_index
        .map_or(false, |max_index| metadata.min_event_index > max_index)
    {
        return false;
    }

    if filters
        .include_event_types
        .map_or(false, |include_event_types| {
            //Is there at least one of the include_event_types in the event batch? If not, return true to skip
            let at_least_one_match =
                check_event_types_match(&metadata.event_types_data, include_event_types);
            !at_least_one_match
        })
    {
        return false;
    }

    true
}

fn calculate_absolute_positions(event_batches_file_len: u64, batches: &mut [MetadataBatchInfo]) {
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
                event_types
                    .iter()
                    .any(|&batch_type| include_event_types.contains(&batch_type))
            } else {
                include_event_types
                    .iter()
                    .any(|&include_event_type| event_types.contains(&include_event_type))
            }
        }
        EventTypesData::Bloom(bloom_bytes) => {
            // Create bloom filter and test each required type
            let bloom = bloom_filter_from_bytes(bloom_bytes);
            include_event_types
                .iter()
                .any(|&include_event_type| bloom.contains(&include_event_type.to_le_bytes()))
        }
    }
}

#[cfg(target_os = "linux")]
fn read_event_batches_with_uring<R: Read + Seek + AsRawFd>(
    event_batch_reader: &mut R,
    batch_info: &[MetadataBatchInfo],
    filters: &ReadFilters,
    io_uring_queue_depth: u32,
) -> io::Result<Vec<EventBatchItem>> {
    let included_batches: Vec<&MetadataBatchInfo> =
        batch_info.iter().filter(|b| b.include).collect();

    if included_batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut ring = IoUring::new(io_uring_queue_depth)?;
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
        if let Some(min_local_index) = filters.min_client_event_index {
            event_batch
                .events
                .retain(|event| event.client_event_index >= min_local_index);
        }

        if let Some(max_local_index) = filters.max_client_event_index {
            event_batch
                .events
                .retain(|event| event.client_event_index <= max_local_index);
        }

        // Final filtering for event_time
        if let Some(min_event_time) = filters.min_event_timestamp {
            event_batch
                .events
                .retain(|event| event.event_timestamp >= min_event_time);
        }

        if let Some(max_event_time) = filters.max_event_timestamp {
            event_batch
                .events
                .retain(|event| event.event_timestamp <= max_event_time);
        }

        // Final filtering for event index
        if let Some(min_client_event_index) = filters.min_client_event_index {
            event_batch
                .events
                .retain(|event| event.client_event_index >= min_client_event_index);
        }

        if let Some(max_client_event_index) = filters.max_client_event_index {
            event_batch
                .events
                .retain(|event| event.client_event_index <= max_client_event_index);
        }

        if !event_batch.events.is_empty() {
            event_batches.push(event_batch);
        }
    }

    // Sort by server_id to maintain order
    event_batches.sort_by_key(|batch| batch.event_batch_index);

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

    if first_metadata.event_batch_index > requested_server_id {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "Requested server_id {} is not available, minimum is {}",
                requested_server_id, first_metadata.event_batch_index
            ),
        ));
    }

    Ok(first_metadata.event_batch_index)
}

fn internal_read_filtered_standard<R: Read + Seek>(
    event_batch_reader: &mut R,
    metadata_reader: &mut R,
    filters: &ReadFilters,
) -> io::Result<ReadResult> {
    let file_len_event_batch = event_batch_reader.seek(SeekFrom::End(0))?;
    let file_len_metadata = metadata_reader.seek(SeekFrom::End(0))?;

    // Read first metadata entry at start of metadata file. Find our minimum server_id.
    // If we don't have the required server_ids (file was trimmed), error back to caller. They need to pull from S3 instead perhaps.
    let minimum_available_server_id =
        get_minimum_available_server_id(metadata_reader, filters.from_event_batch_index)?;

    // Calculate the offset in the metadata file to start reading metadata chunks
    let start_reading_metadata_from_offset_position =
        (filters.from_event_batch_index - minimum_available_server_id) * METADATA_BATCH_SIZE_BYTES as u64;

    // Calculate how many metadata entries we can read
    let remaining_metadata_bytes =
        file_len_metadata.saturating_sub(start_reading_metadata_from_offset_position);
    let max_metadata_entries =
        (remaining_metadata_bytes / METADATA_BATCH_SIZE_BYTES as u64) as usize;

    // Handle scenario where client requests server_id that hasn't been written yet
    if max_metadata_entries == 0 {
        return Ok(ReadResult {
            event_batches: Vec::new(),
            next_event_batch_index: None,
        });
    }

    // Phase 1: Read and filter metadata entries using standard I/O
    let mut batches = read_metadata_entries_standard(
        metadata_reader,
        start_reading_metadata_from_offset_position,
        max_metadata_entries,
        filters,
    )?;

    calculate_absolute_positions(file_len_event_batch, &mut batches);

    let next_server_id: Option<u64> =
        trim_end_if_exceeds_max_bytes(&mut batches, filters.max_bytes);

    if batches.is_empty() {
        return Ok(ReadResult {
            event_batches: Vec::new(),
            next_event_batch_index: None,
        });
    }

    // Phase 2: Read event batches using standard I/O
    let event_batches = read_event_batches_standard(event_batch_reader, &batches, filters)?;

    Ok(ReadResult {
        event_batches,
        next_event_batch_index: next_server_id,
    })
}

fn read_metadata_entries_standard<R: Read + Seek>(
    metadata_reader: &mut R,
    start_offset: u64,
    max_entries: usize,
    filters: &ReadFilters,
) -> io::Result<Vec<MetadataBatchInfo>> {
    metadata_reader.seek(SeekFrom::Start(start_offset))?;

    let mut batch_entries = Vec::with_capacity(max_entries);
    let mut buffer = [0u8; METADATA_BATCH_SIZE_BYTES];

    for _ in 0..max_entries {
        let bytes_read = metadata_reader.read(&mut buffer)?;

        if bytes_read < METADATA_BATCH_SIZE_BYTES {
            break; // End of file or insufficient data
        }

        let metadata: EventBatchMetadata =
            bincode::decode_from_slice(&buffer, BINCODE_CONFIG_FIXED)
                .map_err(|e| io::Error::other(e.to_string()))?
                .0;

        let batch_info = MetadataBatchInfo {
            include: is_include_batch(&metadata, filters),
            server_id: metadata.event_batch_index,
            uncompressed_size: metadata.uncompressed_size,
            compressed_size: metadata.compressed_size,
            compression_type: metadata.compression_type,
            events_crc: metadata.events_crc,
            file_offset: 0, // Will be calculated later
        };

        batch_entries.push(batch_info);
    }

    Ok(batch_entries)
}

fn read_event_batches_standard<R: Read + Seek>(
    event_batch_reader: &mut R,
    batch_info: &[MetadataBatchInfo],
    filters: &ReadFilters,
) -> io::Result<Vec<EventBatchItem>> {
    let included_batches: Vec<&MetadataBatchInfo> =
        batch_info.iter().filter(|b| b.include).collect();

    if included_batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut event_batches = Vec::with_capacity(included_batches.len());

    for batch in included_batches {
        // Seek to the batch position
        event_batch_reader.seek(SeekFrom::Start(batch.file_offset))?;

        // Read the compressed data
        let mut buffer = vec![0u8; batch.compressed_size as usize];
        let bytes_read = event_batch_reader.read(&mut buffer)?;

        if bytes_read != batch.compressed_size as usize {
            return Err(io::Error::other(format!(
                "Expected {} bytes, but read {} bytes",
                batch.compressed_size, bytes_read
            )));
        }

        // Verify CRC
        let crc = crc32fast::hash(&buffer);
        if crc != batch.events_crc {
            return Err(io::Error::other("CRC mismatch in event batch"));
        }

        // Decompress and deserialize
        let compression_type = CompressionType::from_tuple(batch.compression_type, None);
        let mut event_batch = EventBatchItem::from_wire_format(
            &buffer,
            compression_type,
            batch.uncompressed_size as usize,
        )?;

        // Final event type filtering (bloom filter might have false positives)
        if let Some(event_types) = filters.include_event_types {
            event_batch
                .events
                .retain(|event| event_types.contains(&event.event_type_major));
        }

        // Final filtering for local_index
        if let Some(min_local_index) = filters.min_client_event_index {
            event_batch
                .events
                .retain(|event| event.client_event_index >= min_local_index);
        }

        if let Some(max_local_index) = filters.max_client_event_index {
            event_batch
                .events
                .retain(|event| event.client_event_index <= max_local_index);
        }

        // Final filtering for event_time
        if let Some(min_event_time) = filters.min_event_timestamp {
            event_batch
                .events
                .retain(|event| event.event_timestamp >= min_event_time);
        }

        if let Some(max_event_time) = filters.max_event_timestamp {
            event_batch
                .events
                .retain(|event| event.event_timestamp <= max_event_time);
        }

        if !event_batch.events.is_empty() {
            event_batches.push(event_batch);
        }
    }

    // Sort by server_id to maintain order
    event_batches.sort_by_key(|batch| batch.event_batch_index);

    Ok(event_batches)
}
