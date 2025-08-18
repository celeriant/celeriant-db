use std::{
    collections::HashSet,
    io::{self, Write},
};

use crc32fast::Hasher;
use fastbloom::BloomFilter;

use crate::structures::{
    compression_type::CompressionType,
    constants::{
        BLOOM_BITS, BLOOM_BYTES, BLOOM_HASH_COUNT, HEAD_BATCH_START_SIZE, MAGIC_NUMBER,
        TAIL_BATCH_METADATA_SIZE,
    },
    event_batch_item::EventBatchItem,
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

    // Write the event type if there is only a single event in the batch (for filtering later)
    let mut tp = if event_batch_item.events.len() == 1 {
        event_batch_item.events[0].event_type_major
    } else {
        u64::MAX
    };

    // 128 bytes for a bloom filter (always written for fixed header size)
    let mut bloom_bytes = [0u8; BLOOM_BYTES];

    if event_batch_item.events.len() > 1 {
        let event_types_major: HashSet<_> = event_batch_item
            .events
            .iter()
            .map(|f| f.event_type_major)
            .collect();

        if event_types_major.len() == 1 {
            tp = *event_types_major.iter().next().unwrap();
        } else {
            // Populate bloom filter with multiple event types
            let mut filter = BloomFilter::with_num_bits(BLOOM_BITS).hashes(BLOOM_HASH_COUNT);

            for event_type in &event_types_major {
                filter.insert(&event_type.to_le_bytes());
            }

            // Get filter bytes (256 bits = 32 bytes)
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
        }
    }

    let tp_bytes = tp.to_le_bytes();
    write_buffer.extend_from_slice(&tp_bytes);
    hasher.update(&tp_bytes);

    write_buffer.extend_from_slice(&bloom_bytes);
    hasher.update(&bloom_bytes);

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
