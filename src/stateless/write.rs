use std::{
    collections::HashSet,
    io::{self, Write},
    u64,
};

use crc32fast::Hasher;
use fastbloom::BloomFilter;

use crate::structures::{
    compression_type::CompressionType,
    constants::{BLOOM_BYTES, HEAD_BATCH_START_SIZE, MAGIC_NUMBER, TAIL_BATCH_METADATA_SIZE},
    event_batch_item::EventBatchItem,
    event_item::EventItem,
};

/// Writes an event batch item to a binary stream
///
/// # Arguments
/// * `writer` - Any writable destination (BufWriter<File>, Vec<u8>, Cursor<Vec<u8>>, etc.)
/// * `event_batch_item` - The event batch item to serialize and write
///
/// # Returns
/// * `usize` - The size of the uncompressed event_batch_item data in bytes
pub fn append_event_batch<W: Write>(
    writer: &mut W,
    bloom_filter: &mut BloomFilter,
    event_type_dedup: &mut HashSet<u64>,
    compression_type: CompressionType,
    event_batch_item: &EventBatchItem,
) -> io::Result<usize> {
    if event_batch_item.events.is_empty() {
        return Err(io::Error::other("Cannot write empty event batch"));
    }

    let mut hasher = Hasher::new();

    // Serialize and compress
    let (uncompressed_size, compressed_event_batch_item) =
        event_batch_item.to_wire_format(compression_type)?;

    // Write everything in one go to minimize syscalls
    let compressed_event_batch_item_length = compressed_event_batch_item.len();
    let total_size =
        HEAD_BATCH_START_SIZE + compressed_event_batch_item_length + TAIL_BATCH_METADATA_SIZE;
    let mut write_buffer = Vec::with_capacity(total_size);

    //Write the length of the compressed event batch item first for forward-based file recovery
    let compressed_len_bytes = (compressed_event_batch_item_length as u64).to_le_bytes();
    write_buffer.extend_from_slice(&compressed_len_bytes);
    hasher.update(&compressed_len_bytes);

    //Now we can write the actual event_batch_item data
    write_buffer.extend_from_slice(&compressed_event_batch_item);
    hasher.update(&compressed_event_batch_item);

    // We need the original size of the event in bytes before compression to create the right buffer size
    let uncompressed_size_bytes = (uncompressed_size as u64).to_le_bytes();
    write_buffer.extend_from_slice(&uncompressed_size_bytes);
    hasher.update(&uncompressed_size_bytes);

    let (event_types, use_bloom) = extract_unique_event_types(&event_batch_item.events);
    write_buffer.extend_from_slice(&(use_bloom as u8).to_le_bytes());

    if use_bloom {
        let bloom_bytes =
            create_bloom_filter_bytes(bloom_filter, event_type_dedup, &event_batch_item.events);
        write_buffer.extend_from_slice(&bloom_bytes);
        hasher.update(&bloom_bytes);
    } else {
        for &event_type in &event_types {
            let event_type_bytes = event_type.to_le_bytes();
            write_buffer.extend_from_slice(&event_type_bytes);
            hasher.update(&event_type_bytes);
        }
    }

    // Write the clients last local index (8 bytes)
    // This helps the server to stop the client from writing duplicate events
    let last_local_index = event_batch_item.events.last().map_or(0, |e| e.local_index);
    let last_local_index_bytes = last_local_index.to_le_bytes();
    write_buffer.extend_from_slice(&last_local_index_bytes);
    hasher.update(&last_local_index_bytes);

    // We need the si of the first event in the batch to determine which batch to start the catch-up process from
    let server_id_bytes = event_batch_item.server_id.to_le_bytes();
    write_buffer.extend_from_slice(&server_id_bytes);
    hasher.update(&server_id_bytes);

    // Write the client id who created the event batch
    // Note we don't write the user_id as it's size is not fixed
    let client_id_bytes = event_batch_item.client_id.to_le_bytes();
    write_buffer.extend_from_slice(&client_id_bytes);
    hasher.update(&client_id_bytes);

    let user_id_bytes = event_batch_item.user_id.unwrap_or(0).to_le_bytes();
    write_buffer.extend_from_slice(&user_id_bytes);
    hasher.update(&user_id_bytes);

    let server_time_bytes = event_batch_item.server_time.to_le_bytes();
    write_buffer.extend_from_slice(&server_time_bytes);
    hasher.update(&server_time_bytes);

    // Each batch is variable in length, so we need to know where it starts when reading backwards through the file
    write_buffer.extend_from_slice(&compressed_len_bytes);
    hasher.update(&compressed_len_bytes);

    // What compression algorithm we used
    let compression_type_bytes = compression_type.to_tuple().0.to_le_bytes();
    write_buffer.extend_from_slice(&compression_type_bytes);
    hasher.update(&compression_type_bytes);

    // Finalize checksum and add it
    let checksum = hasher.finalize();
    write_buffer.extend_from_slice(&checksum.to_le_bytes());

    // Magic number for corruption detection
    write_buffer.extend_from_slice(&MAGIC_NUMBER.to_le_bytes());

    writer.write_all(&write_buffer)?;

    // Force write to the disk to ensure we don't lose data if the program crashes
    writer.flush()?;

    Ok(uncompressed_size)
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

fn create_bloom_filter_bytes(
    filter: &mut BloomFilter,
    event_type_dedup: &mut HashSet<u64>,
    events: &[EventItem],
) -> [u8; BLOOM_BYTES] {
    let mut bloom_bytes = [0u8; BLOOM_BYTES];

    // Populate bloom filter with multiple event types
    filter.clear();
    event_type_dedup.clear();

    for event in events {
        event_type_dedup.insert(event.event_type_major);
    }

    for &event_type in event_type_dedup.iter() {
        filter.insert(&event_type.to_le_bytes());
    }

    // Get filter bytes
    let filter_slice = filter.as_slice(); // Returns &[u64]
    let filter_bytes = unsafe {
        core::slice::from_raw_parts(
            filter_slice.as_ptr() as *const u8,
            BLOOM_BYTES, // Convert u64 count to byte count
        )
    };
    if filter_bytes.len() >= BLOOM_BYTES {
        bloom_bytes.copy_from_slice(&filter_bytes[..BLOOM_BYTES]);
    } else {
        bloom_bytes[..filter_bytes.len()].copy_from_slice(filter_bytes);
    }

    bloom_bytes
}
