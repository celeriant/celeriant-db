use std::io::{self, Read, Seek, SeekFrom};

use fastbloom::BloomFilter;

use crate::structures::{
    constants::BLOOM_BYTES, read_filters::ReadFilters, read_result::ReadResult,
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
    let start_reading_metadata_from_offset_postion =
        (filters.from_server_id - minimum_available_server_id) * METADATA_BATCH_SIZE_BYTES;

    // Use io_uring to pull all the metadata entries we need into memory into EventBatchMetadata structs
    // Mark each event batch as 'include' or 'exclude' based on the client's filter requirements
    // Also keep track of next_server_id - we might not be able to return all event batches as we exceeded the max_bytes limit!
    // Also calculate the absolute position (start and end pos) for each event batch blob in the event_batch file (metadata only stores batch lengths)
    // We can do all this inside io_uring collect phase as although its concurrent, the order is maintained (queue design)

    // Now we know which event batches to pull, and where they are in the file, and their size in bytes. Pull them all into memory using io_uring
    // Decompress and deserialize within the collect phase of io_uring.
    // Note also we must do a final filter out of individual events if we need to filter by event type.
    // The bloom filter is not 100% accurate and metadata only stores 'in' types, not exclusive

    Ok(ReadResult {
        event_batches: Vec::new(),
        next_server_id: None,
    })
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
