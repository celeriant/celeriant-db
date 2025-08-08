use crate::catchup_result::CatchupResult;
use crate::event_batch_item::EventBatchItem;
use crate::wire_format::{compress_data, decompress_data, deserialize_event_batch_item, serialize_event_batch_item};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

/// Append a batch of events with compression
pub fn append_event_batch(writer: &mut BufWriter<File>, event_batch_item: &EventBatchItem) -> io::Result<usize> {
    if event_batch_item.events.is_empty() {
        return Ok(0);
    }

    // Always write new batches at end of the file
    let batch_start_pos = writer.get_ref().metadata()?.len();

    // Serialize and compress
    let encoded_event_batch_item = serialize_event_batch_item(event_batch_item)?;
    let compressed_event_batch_item = compress_data(&encoded_event_batch_item)?;

    // Write everything in one go to minimize syscalls
    let total_size = BATCH_START_SIZE as usize + compressed_event_batch_item.len() + BATCH_METADATA_SIZE as usize;
    let mut write_buffer = Vec::with_capacity(total_size);

    //Write the length of the compressed event batch item first for forward-based file recovery
    write_buffer.extend_from_slice(&(compressed_event_batch_item.len() as u64).to_le_bytes());

    //Now we can write the actual event_batch_item data
    write_buffer.extend_from_slice(&compressed_event_batch_item);

    // We need the original size of the event in bytes before compression to create the right buffer size
    write_buffer.extend_from_slice(&(encoded_event_batch_item.len() as u64).to_le_bytes());

    // Write the event type if there is only a single event in the batch (for filtering later)
    let tp = if event_batch_item.events.len() == 1 {
        event_batch_item.events[0].event_type
    } else {
        u64::MAX
    };
    write_buffer.extend_from_slice(&tp.to_le_bytes());

    // Write the clients last local index (8 bytes)
    // This helps the server to stop the client from writing duplicate events
    let last_local_index = event_batch_item.events.last().map_or(0, |e| e.local_index);
    write_buffer.extend_from_slice(&last_local_index.to_le_bytes());

    // We need the si of the first event in the batch to determine which batch to start the catch-up process from
    write_buffer.extend_from_slice(&event_batch_item.server_id.to_le_bytes());

    // Write the client id who created the event batch (16 bytes)
    write_buffer.extend_from_slice(&event_batch_item.client_id.to_le_bytes());

    // Each batch is variable in length, so we need to know where it starts when reading backwards through the file
    write_buffer.extend_from_slice(&batch_start_pos.to_le_bytes());

    // Magic number for corruption detection
    write_buffer.extend_from_slice(&MAGIC_NUMBER.to_le_bytes());

    writer.write_all(&write_buffer)?;

    // Force write to the disk to ensure we don't lose data if the program crashes
    writer.flush()?;

    Ok(total_size)
}

fn read_u64_at_offset(reader: &mut BufReader<File>, current_pos: u64, offset: u64) -> io::Result<u64> {
    reader.seek(SeekFrom::Start(current_pos - offset))?;

    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;

    Ok(u64::from_le_bytes(bytes))
}

fn read_u128_at_offset(reader: &mut BufReader<File>, current_pos: u64, offset: u64) -> io::Result<u128> {
    reader.seek(SeekFrom::Start(current_pos - offset))?;

    let mut bytes = [0u8; 16];
    reader.read_exact(&mut bytes)?;

    Ok(u128::from_le_bytes(bytes))
}

fn seek_to_and_read_exact(reader: &mut BufReader<File>, seek_to: u64, data_size: usize) -> io::Result<Vec<u8>> {
    reader.seek(SeekFrom::Start(seek_to))?;

    let mut data = vec![0; data_size];
    reader.read_exact(&mut data)?;

    Ok(data)
}

const MAGIC_NUMBER: u64 = 0xDEADBEEFCAFEBABE;

const BATCH_START_SIZE: u64 = 8;
const BATCH_METADATA_SIZE: u64 = UNCOMPRESSED_BATCH_SIZE_OFFSET;

const UNCOMPRESSED_BATCH_SIZE_OFFSET: u64 = 64;
const EVENT_TYPE_OFFSET: u64 = 56;
const LOCAL_INDEX_OFFSET: u64 = 48;
const SERVER_ID_OFFSET: u64 = 40;
const CLIENT_ID_OFFSET: u64 = 32;
const BATCH_START_POS_OFFSET: u64 = 16;
const MAGIC_NUMBER_OFFSET: u64 = 8;

fn read_batch_at_position(reader: &mut BufReader<File>, current_pos: u64, batch_start_pos: u64) -> io::Result<EventBatchItem> {
    let original_size = read_u64_at_offset(reader, current_pos, UNCOMPRESSED_BATCH_SIZE_OFFSET)?;
    let data_size = (current_pos - BATCH_START_SIZE - BATCH_METADATA_SIZE - batch_start_pos) as usize;
    let compressed_data = seek_to_and_read_exact(reader, batch_start_pos + BATCH_START_SIZE, data_size)?;
    let decompressed_data = decompress_data(&compressed_data, original_size as usize)?;
    let event_batch_item = deserialize_event_batch_item(&decompressed_data)?;
    Ok(event_batch_item)
}

pub fn find_last_valid_event_batch(mut reader: &mut BufReader<File>) -> io::Result<u64> {
    let file_size = reader.get_ref().metadata()?.len();

    if file_size < BATCH_START_SIZE + BATCH_METADATA_SIZE {
        return Ok(0); // File too small to contain any valid batch
    }

    let mut current_pos = 0u64;
    let mut last_valid_pos = 0u64;

    while current_pos + BATCH_START_SIZE + BATCH_METADATA_SIZE <= file_size {
        // Read the compressed data size from the beginning of the batch
        let compressed_size = read_u64_at_offset(&mut reader, current_pos, 0)?;

        // Calculate where this batch should end
        let batch_end_pos = current_pos + BATCH_START_SIZE + compressed_size + BATCH_METADATA_SIZE;

        // Check if the batch would extend beyond the file
        if batch_end_pos > file_size {
            break;
        }

        // Check if the magic number is correct at the expected end position
        if !is_batch_corrupt(&mut reader, batch_end_pos) {
            last_valid_pos = batch_end_pos;
            current_pos = batch_end_pos;
        } else {
            break; // Found corruption, stop here
        }
    }

    Ok(last_valid_pos)
}

pub fn find_last_si(mut reader: &mut BufReader<File>) -> io::Result<Option<u64>> {
    let file_size = reader.get_ref().metadata()?.len();

    if file_size < BATCH_METADATA_SIZE || is_batch_corrupt(&mut reader, file_size) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Batch data is corrupt, file recovery needed"));
    }

    let last_si = read_u64_at_offset(&mut reader, file_size, SERVER_ID_OFFSET)?;

    Ok(Some(last_si))
}

pub fn find_last_li(mut reader: &mut BufReader<File>, client_id: u128) -> io::Result<Option<u64>> {
    let file_size = reader.get_ref().metadata()?.len();

    if file_size < BATCH_METADATA_SIZE {
        return Ok(None);
    }

    let mut current_pos = file_size;

    // Scan backwards through the file to find the last batch from this client
    while current_pos >= BATCH_METADATA_SIZE {
        if is_batch_corrupt(&mut reader, current_pos) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Batch data is corrupt, file recovery needed"));
        }

        let batch_client_id = read_u128_at_offset(reader, current_pos, CLIENT_ID_OFFSET)?;

        if batch_client_id == client_id {
            let last_li = read_u64_at_offset(&mut reader, current_pos, LOCAL_INDEX_OFFSET)?;
            return Ok(Some(last_li));
        }

        let batch_start_pos = read_u64_at_offset(&mut reader, current_pos, BATCH_START_POS_OFFSET)?;
        current_pos = batch_start_pos;
    }

    Ok(None)
}

fn is_batch_corrupt(mut reader: &mut BufReader<File>, current_pos: u64) -> bool {
    let magic = match read_u64_at_offset(&mut reader, current_pos, MAGIC_NUMBER_OFFSET) {
        Ok(magic) => magic,
        Err(_) => 0, // Can't read, assume corruption
    };

    magic != MAGIC_NUMBER
}

/// Read events starting from a specific si (efficient catchup)
pub fn read_from_si(
    mut reader: &mut BufReader<File>,
    target_server_id: u64,
    max_bytes: usize,
    event_type_filter: Option<&[u64]>,
    exclude_client_id_filter: Option<u128>,
) -> io::Result<CatchupResult> {
    let file_size = reader.get_ref().metadata()?.len();

    if file_size < BATCH_METADATA_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Batch data is corrupt, file recovery needed"));
    }

    let mut batch_positions = Vec::new();
    let mut current_pos = file_size;

    // Collect batch positions until we find the target batch (scanning backwards)
    while current_pos >= BATCH_METADATA_SIZE {
        if is_batch_corrupt(&mut reader, current_pos) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Batch data is corrupt, file recovery needed"));
        }

        let batch_start_pos = read_u64_at_offset(&mut reader, current_pos, BATCH_START_POS_OFFSET)?;

        // Read first si of this batch to check if we've reached our target
        let batch_si = read_u64_at_offset(&mut reader, current_pos, SERVER_ID_OFFSET)?;

        // Stop if this batch might contain our target_si
        if batch_si < target_server_id {
            break;
        }

        batch_positions.push((batch_start_pos, current_pos));

        current_pos = batch_start_pos;
    }

    // Reverse to get chronological order (oldest to newest)
    batch_positions.reverse();

    let mut event_batches: Vec<Arc<EventBatchItem>> = Vec::new();
    let mut total_bytes = 0;

    for (batch_start_pos, batch_end_pos) in batch_positions.iter() {
        // If there is a tp_filter first check if this batch matches this tp
        if let Some(tp_filter) = event_type_filter {
            let batch_tp = read_u64_at_offset(reader, *batch_end_pos, EVENT_TYPE_OFFSET)?;
            let mut matched: bool = false;
            for tp in tp_filter {
                if batch_tp == *tp {
                    matched = true;
                    break;
                }
            }
            if !matched {
                continue;
            }
        }

        if let Some(exclude_client_id_filter) = exclude_client_id_filter {
            let batch_client_id = read_u128_at_offset(reader, *batch_end_pos, CLIENT_ID_OFFSET)?;
            if batch_client_id == exclude_client_id_filter {
                continue;
            }
        }

        let events = read_batch_at_position(reader, *batch_end_pos, *batch_start_pos)?;
        event_batches.push(Arc::new(events));

        let compressed_data_size = (batch_end_pos - batch_start_pos) as usize;
        total_bytes += compressed_data_size + BATCH_START_SIZE as usize + BATCH_METADATA_SIZE as usize;

        // Check if adding this batch would exceed our limit
        if total_bytes > max_bytes {
            // We've hit our limit, return what we have
            let next_server_id = Some(event_batches.last().unwrap().server_id + 1);
            return Ok(CatchupResult { event_batches, next_server_id });
        }
    }

    Ok(CatchupResult {
        event_batches,
        next_server_id: None,
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::event_item::EventItem;
    use crate::event_item::tests::{create_minimal_event_item, create_test_event_item};
    use crate::file_cache::{create_append_writer, create_reader};
    use std::fs::OpenOptions;
    use std::usize;
    use tempfile::TempDir;

    pub fn create_event_batch_item(server_id: u64, client_id: u128, user_id: Option<String>, server_date: u64, events: Vec<EventItem>) -> EventBatchItem {
        EventBatchItem {
            server_id,
            client_id,
            user_id,
            server_date,
            events,
        }
    }

    //TODO: Tests for exclude_client_id_filter

    #[test]
    fn test_corrupt_file() {
        let events_batch_1 = create_event_batch_item(
            0,
            0,
            None,
            123,
            vec![create_test_event_item(), create_minimal_event_item(), create_test_event_item()],
        );
        let events_batch_2 = create_event_batch_item(1, 0, None, 456, vec![create_test_event_item(), create_minimal_event_item()]);
        let events_batch_3 = create_event_batch_item(2, 0, None, 789, vec![create_minimal_event_item()]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();
        append_event_batch(&mut writer, &events_batch_2).unwrap();

        let reader = create_reader(events_bin.to_str().unwrap()).unwrap();
        let current_file_size = reader.get_ref().metadata().unwrap().len();

        append_event_batch(&mut writer, &events_batch_3).unwrap();

        //Truncate the file back
        let file = OpenOptions::new().write(true).open(events_bin.to_str().unwrap()).unwrap();

        // Set the file length.
        file.set_len(current_file_size + 99).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();
        let catchup_result = read_from_si(&mut reader, 0, usize::MAX, None, None);

        assert!(catchup_result.is_err());
        let error = catchup_result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let catchup_result = find_last_si(&mut reader);
        assert!(catchup_result.is_err());
        let error = catchup_result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_max_bytes_limit() {
        let events_batch_1 = create_event_batch_item(
            0,
            0,
            None,
            123,
            vec![create_test_event_item(), create_minimal_event_item(), create_test_event_item()],
        );
        let events_batch_2 = create_event_batch_item(1, 0, None, 456, vec![create_test_event_item(), create_minimal_event_item()]);
        let events_batch_3 = create_event_batch_item(2, 0, None, 789, vec![create_minimal_event_item()]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();
        append_event_batch(&mut writer, &events_batch_2).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        let catchup_result = read_from_si(&mut reader, 0, 1, None, None).unwrap();

        assert_eq!(catchup_result.event_batches.len(), 1);
        assert_eq!(catchup_result.next_server_id, Some(1));

        let current_file_size = reader.get_ref().metadata().unwrap().len();

        let catchup_result = read_from_si(&mut reader, 0, current_file_size as usize + 86, None, None).unwrap();

        assert_eq!(catchup_result.event_batches.len(), 2);
        assert_eq!(catchup_result.next_server_id, Some(2));

        append_event_batch(&mut writer, &events_batch_3).unwrap();

        let catchup_result = read_from_si(&mut reader, 0, current_file_size as usize + 86, None, None).unwrap();

        assert_eq!(catchup_result.event_batches.len(), 2);
        assert_eq!(catchup_result.next_server_id, Some(2));

        let catchup_result = read_from_si(&mut reader, 0, current_file_size as usize + 396, None, None).unwrap();

        assert_eq!(catchup_result.event_batches.len(), 3);
        assert_eq!(catchup_result.next_server_id, None);
    }

    #[test]
    fn test_read_write_with_event_storage_format() {
        let events_batch_1 = create_event_batch_item(
            0,
            0,
            None,
            123,
            vec![create_test_event_item(), create_minimal_event_item(), create_test_event_item()],
        );
        let events_batch_2 = create_event_batch_item(1, 0, None, 456, vec![create_test_event_item(), create_minimal_event_item()]);
        let events_batch_3 = create_event_batch_item(2, 0, None, 789, vec![create_minimal_event_item()]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        let result_events_batch_1 = read_from_si(&mut reader, 0, usize::MAX, None, None).unwrap().event_batches;

        assert_eq!(result_events_batch_1.len(), 1);
        assert_eq!(events_batch_1.server_id, result_events_batch_1[0].server_id);

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(0));

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_2).unwrap();

        let result_events_batches = read_from_si(&mut reader, 0, usize::MAX, None, None).unwrap().event_batches;

        assert_eq!(result_events_batches.len(), 2);
        assert_eq!(events_batch_1.server_id, result_events_batches[0].server_id);
        assert_eq!(events_batch_2.server_id, result_events_batches[1].server_id);

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(1));

        append_event_batch(&mut writer, &events_batch_3).unwrap();

        let result_events_batches = read_from_si(&mut reader, 0, usize::MAX, None, None).unwrap().event_batches;

        assert_eq!(result_events_batches.len(), 3);
        assert_eq!(events_batch_1.server_id, result_events_batches[0].server_id);
        assert_eq!(events_batch_2.server_id, result_events_batches[1].server_id);
        assert_eq!(events_batch_3.server_id, result_events_batches[2].server_id);

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(2));
    }

    #[test]
    fn test_invalid_catch_up() {
        let events_batch_1 = create_event_batch_item(
            0,
            0,
            None,
            123,
            vec![create_test_event_item(), create_minimal_event_item(), create_test_event_item()],
        );

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        let invalid_si_over = read_from_si(&mut reader, 1000, usize::max_value(), None, None).unwrap();

        assert_eq!(invalid_si_over.event_batches.len(), 0);
        assert_eq!(invalid_si_over.next_server_id, Option::None);
    }

    #[test]
    fn test_valid_catchup_scenarios() {
        let events_batch_1 = create_event_batch_item(
            0,
            0,
            None,
            123,
            vec![create_test_event_item(), create_minimal_event_item(), create_test_event_item()],
        );
        let events_batch_2 = create_event_batch_item(1, 0, None, 456, vec![create_test_event_item(), create_minimal_event_item()]);
        let events_batch_3 = create_event_batch_item(2, 0, None, 789, vec![create_minimal_event_item()]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(0));

        let si_0_result = read_from_si(&mut reader, 0, usize::max_value(), None, None).unwrap();

        assert_eq!(si_0_result.event_batches.len(), 1);
        assert_eq!(si_0_result.event_batches[0].events.len(), 3);
        assert_eq!(events_batch_1.server_id, si_0_result.event_batches[0].server_id);

        append_event_batch(&mut writer, &events_batch_2).unwrap();

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(1));

        let read_result = read_from_si(&mut reader, 0, usize::max_value(), None, None).unwrap();

        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(events_batch_1.server_id, read_result.event_batches[0].server_id);
        assert_eq!(events_batch_2.server_id, read_result.event_batches[1].server_id);

        let read_result = read_from_si(&mut reader, 1, usize::max_value(), None, None).unwrap();

        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(events_batch_2.server_id, read_result.event_batches[0].server_id);

        append_event_batch(&mut writer, &events_batch_3).unwrap();

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(2));

        let read_result = read_from_si(&mut reader, 2, usize::max_value(), None, None).unwrap();

        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(read_result.event_batches[0].events.len(), 1);
        assert_eq!(events_batch_3.server_id, read_result.event_batches[0].server_id);

        let read_result = read_from_si(&mut reader, 1, usize::max_value(), None, None).unwrap();
        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(events_batch_2.server_id, read_result.event_batches[0].server_id);
        assert_eq!(events_batch_3.server_id, read_result.event_batches[1].server_id);
    }

    #[test]
    fn test_find_last_valid_event_batch_corrupted_file() {
        let events_batch_1 = create_event_batch_item(0, 0, None, 123, vec![create_test_event_item(), create_minimal_event_item()]);
        let events_batch_2 = create_event_batch_item(1, 0, None, 456, vec![create_test_event_item()]);
        let events_batch_3 = create_event_batch_item(2, 0, None, 789, vec![create_minimal_event_item()]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();
        append_event_batch(&mut writer, &events_batch_2).unwrap();

        // Get the position after the first two valid batches
        let reader = create_reader(events_bin.to_str().unwrap()).unwrap();
        let valid_end_pos = reader.get_ref().metadata().unwrap().len();

        // Add a third batch
        append_event_batch(&mut writer, &events_batch_3).unwrap();

        // Corrupt the file by truncating it in the middle of the third batch
        let file = OpenOptions::new().write(true).open(events_bin.to_str().unwrap()).unwrap();
        file.set_len(valid_end_pos + 50).unwrap(); // Truncate partway through third batch

        // Test that find_last_valid_event_batch returns position after second batch
        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();
        let last_valid_pos = find_last_valid_event_batch(&mut reader).unwrap();

        assert_eq!(last_valid_pos, valid_end_pos);
    }

    #[test]
    fn test_find_last_valid_event_batch_uncorrupted_file() {
        let events_batch_1 = create_event_batch_item(0, 0, None, 123, vec![create_test_event_item(), create_minimal_event_item()]);
        let events_batch_2 = create_event_batch_item(1, 0, None, 456, vec![create_test_event_item()]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();
        append_event_batch(&mut writer, &events_batch_2).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();
        let file_size = reader.get_ref().metadata().unwrap().len();

        let last_valid_pos = find_last_valid_event_batch(&mut reader).unwrap();

        // Should return the exact end of file position since no corruption
        assert_eq!(last_valid_pos, file_size);
    }

    #[test]
    fn test_find_last_valid_event_batch_completely_corrupt_file() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        // Create a file with random garbage data
        let mut file = File::create(events_bin.to_str().unwrap()).unwrap();
        let garbage_data = vec![0x42u8; 1000]; // 1000 bytes of garbage
        file.write_all(&garbage_data).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();
        let last_valid_pos = find_last_valid_event_batch(&mut reader).unwrap();

        // Should return 0 since no valid batches found
        assert_eq!(last_valid_pos, 0);
    }

    #[test]
    fn test_find_last_li_not_found() {
        let events_batch_1 = create_event_batch_item(0, 123, None, 123, vec![create_test_event_item()]);
        let events_batch_2 = create_event_batch_item(1, 456, None, 456, vec![create_test_event_item()]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();
        append_event_batch(&mut writer, &events_batch_2).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        // Try to find last local index for a client that doesn't exist
        let last_li = find_last_li(&mut reader, 999).unwrap();
        assert_eq!(last_li, None);
    }

    #[test]
    fn test_li_simple() {
        let mut event1 = create_test_event_item();
        event1.local_index = 10;
        let events_batch_1 = create_event_batch_item(0, 123, None, 100, vec![event1]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        // Find last local index for client 123 - should return 40 (from the last batch)
        let last_li = find_last_li(&mut reader, 123).unwrap();
        assert_eq!(last_li, Some(10));
    }

    #[test]
    fn test_find_last_li_multiple_clients() {
        // Create events with different local indexes for different clients
        let mut event1 = create_test_event_item();
        event1.local_index = 10;
        let mut event2 = create_test_event_item();
        event2.local_index = 20;
        let mut event3 = create_test_event_item();
        event3.local_index = 30;

        let mut event4 = create_test_event_item();
        event4.local_index = 15;
        let mut event5 = create_test_event_item();
        event5.local_index = 25;

        // Client 123 batches
        let events_batch_1 = create_event_batch_item(0, 123, None, 100, vec![event1, event2]);
        let events_batch_2 = create_event_batch_item(1, 123, None, 200, vec![event3.clone()]);

        // Client 456 batches
        let events_batch_3 = create_event_batch_item(2, 456, None, 150, vec![event4]);
        let events_batch_4 = create_event_batch_item(3, 456, None, 250, vec![event5.clone()]);

        // Another batch from client 123 (should be the last one found)
        let mut event6 = create_test_event_item();
        event6.local_index = 40;
        let events_batch_5 = create_event_batch_item(4, 123, None, 300, vec![event6.clone()]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();
        append_event_batch(&mut writer, &events_batch_2).unwrap();
        append_event_batch(&mut writer, &events_batch_3).unwrap();
        append_event_batch(&mut writer, &events_batch_4).unwrap();
        append_event_batch(&mut writer, &events_batch_5).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        // Find last local index for client 123 - should return 40 (from the last batch)
        let last_li = find_last_li(&mut reader, 123).unwrap();
        assert_eq!(last_li, Some(40));

        // Find last local index for client 456 - should return 25
        let last_li = find_last_li(&mut reader, 456).unwrap();
        assert_eq!(last_li, Some(25));
    }

    #[test]
    fn test_exclude_client_id_filter() {
        let mut event1 = create_test_event_item();
        event1.local_index = 10;
        let mut event2 = create_test_event_item();
        event2.local_index = 20;
        let mut event3 = create_test_event_item();
        event3.local_index = 30;
        let mut event4 = create_test_event_item();
        event4.local_index = 40;

        // Create batches from different clients
        let events_batch_1 = create_event_batch_item(0, 123, None, 100, vec![event1]); // Client 123
        let events_batch_2 = create_event_batch_item(1, 456, None, 200, vec![event2]); // Client 456
        let events_batch_3 = create_event_batch_item(2, 123, None, 300, vec![event3]); // Client 123 again
        let events_batch_4 = create_event_batch_item(3, 789, None, 400, vec![event4]); // Client 789

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();
        append_event_batch(&mut writer, &events_batch_2).unwrap();
        append_event_batch(&mut writer, &events_batch_3).unwrap();
        append_event_batch(&mut writer, &events_batch_4).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        // Test excluding client 123 - should return only batches from clients 456 and 789
        let result = read_from_si(&mut reader, 0, usize::MAX, None, Some(123)).unwrap();

        assert_eq!(result.event_batches.len(), 2);
        assert_eq!(result.event_batches[0].client_id, 456);
        assert_eq!(result.event_batches[0].server_id, 1);
        assert_eq!(result.event_batches[1].client_id, 789);
        assert_eq!(result.event_batches[1].server_id, 3);

        // Test excluding client 456 - should return batches from clients 123 and 789
        let result = read_from_si(&mut reader, 0, usize::MAX, None, Some(456)).unwrap();

        assert_eq!(result.event_batches.len(), 3);
        assert_eq!(result.event_batches[0].client_id, 123);
        assert_eq!(result.event_batches[0].server_id, 0);
        assert_eq!(result.event_batches[1].client_id, 123);
        assert_eq!(result.event_batches[1].server_id, 2);
        assert_eq!(result.event_batches[2].client_id, 789);
        assert_eq!(result.event_batches[2].server_id, 3);

        // Test with no exclusion - should return all batches
        let result = read_from_si(&mut reader, 0, usize::MAX, None, None).unwrap();

        assert_eq!(result.event_batches.len(), 4);
        assert_eq!(result.event_batches[0].client_id, 123);
        assert_eq!(result.event_batches[1].client_id, 456);
        assert_eq!(result.event_batches[2].client_id, 123);
        assert_eq!(result.event_batches[3].client_id, 789);

        // Test excluding non-existent client - should return all batches
        let result = read_from_si(&mut reader, 0, usize::MAX, None, Some(999)).unwrap();

        assert_eq!(result.event_batches.len(), 4);
    }
}
