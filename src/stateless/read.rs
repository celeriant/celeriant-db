use std::io::{self, Read, Seek, SeekFrom};

use fastbloom::BloomFilter;

use crate::structures::{
    constants::BLOOM_BYTES, read_filters::ReadFilters, read_result::ReadResult,
};

/// Reads event batches from a binary stream with filtering and pagination support
///
/// # Arguments
/// * `reader` - Any readable and seekable source (File, Cursor<Vec<u8>>, etc.)
/// * `filters` - Filtering and pagination options
///
/// # Returns
/// * `ReadResult` containing filtered event batches and optional pagination token
pub fn filtered_read<R: Read + Seek>(
    reader: &mut R,
    filters: &ReadFilters,
) -> io::Result<ReadResult> {
    // Get file length using seek
    let file_len = reader.seek(SeekFrom::End(0))?;
    reader.seek(SeekFrom::Start(0))?; // Reset to beginning

    //TODO: Implementation

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
