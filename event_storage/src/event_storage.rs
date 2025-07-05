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
        event_batch_item.events[0].tp
    } else {
        u64::MAX
    };
    write_buffer.extend_from_slice(&tp.to_le_bytes());

    // We need the si of the first event in the batch to determine which batch to start the catch-up process from
    write_buffer.extend_from_slice(&event_batch_item.si.to_le_bytes());

    // Each batch is variable in length, so we need to know where it starts when reading backwards through the file
    write_buffer.extend_from_slice(&batch_start_pos.to_le_bytes());
    
    // Magic number for corruption detection
    write_buffer.extend_from_slice(&MAGIC_NUMBER.to_le_bytes());

    writer.write_all(&write_buffer)?;

    // Force write to the disk to ensure we don't lose data if the program crashes
    writer.flush()?;

    Ok(total_size)
}

fn read_u64_at_offset(
    reader: &mut BufReader<File>,
    current_pos: u64,
    offset: u64,
) -> io::Result<u64> {
    reader.seek(SeekFrom::Start(current_pos - offset))?;

    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;

    Ok(u64::from_le_bytes(bytes))
}

fn seek_to_and_read_exact(
    reader: &mut BufReader<File>,
    seek_to: u64,
    data_size: usize
) -> io::Result<Vec<u8>> {
    reader.seek(SeekFrom::Start(seek_to))?;

    // Pre-allocate with known capacity and avoid zeroing
    let mut data = Vec::with_capacity(data_size);
    unsafe {
        data.set_len(data_size);
    }
    reader.read_exact(&mut data)?;

    Ok(data)
}

const BATCH_START_SIZE: u64 = 8;
const BATCH_METADATA_SIZE: u64 = 40;
const BATCH_START_POS_OFFSET: u64 = 16;
const TP_OFFSET: u64 = 32;
const SI_OFFSET: u64 = 24;
const ORIGINAL_SIZE_OFFSET: u64 = 40;
const MAGIC_NUMBER_OFFSET: u64 = 8;
const MAGIC_NUMBER: u64 = 0xDEADBEEFCAFEBABE;

fn read_batch_at_position(reader: &mut BufReader<File>, current_pos: u64, batch_start_pos: u64) -> io::Result<EventBatchItem> {
    let original_size = read_u64_at_offset(reader, current_pos, ORIGINAL_SIZE_OFFSET)?;
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
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Batch data is corrupt, file recovery needed"
        ));
    }

    let last_si = read_u64_at_offset(&mut reader, file_size, SI_OFFSET)?;

    Ok(Some(last_si))
}

fn is_batch_corrupt(mut reader: &mut BufReader<File>, current_pos: u64) -> bool {
    let magic = match read_u64_at_offset(&mut reader, current_pos, MAGIC_NUMBER_OFFSET) {
        Ok(magic) => magic,
        Err(_) => 0, // Can't read, assume corruption
    };

    magic != MAGIC_NUMBER
}

/// Read events starting from a specific si (efficient catchup)
pub fn read_from_si(mut reader: &mut BufReader<File>, target_si: u64, max_bytes: usize, tp_filter: Option<u64>) -> io::Result<CatchupResult> {
    let file_size = reader.get_ref().metadata()?.len();

    if file_size < BATCH_METADATA_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Batch data is corrupt, file recovery needed"
        ));
    }

    let mut batch_positions = Vec::new();
    let mut current_pos = file_size;

    // Collect batch positions until we find the target batch (scanning backwards)
    while current_pos >= BATCH_METADATA_SIZE {
        if is_batch_corrupt(&mut reader, current_pos) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Batch data is corrupt, file recovery needed"
            ));
        }
        
        let batch_start_pos = read_u64_at_offset(&mut reader, current_pos, BATCH_START_POS_OFFSET)?;

        // Read first si of this batch to check if we've reached our target
        let batch_si = read_u64_at_offset(&mut reader, current_pos, SI_OFFSET)?;

        // Stop if this batch might contain our target_si
        if batch_si < target_si {
            break;
        }

        batch_positions.push((batch_start_pos, current_pos));

        current_pos = batch_start_pos;
    }

    // Reverse to get chronological order (oldest to newest)
    batch_positions.reverse();

    let mut event_batches: Vec<Arc<EventBatchItem>> = Vec::new();
    let mut total_bytes = 0;
    
    for (i, (batch_start_pos, batch_end_pos)) in batch_positions.iter().enumerate() {

        // If there is a tp_filter first check if this batch matches this tp
        if let Some(tp_filter) = tp_filter {
            let batch_tp = read_u64_at_offset(reader, *batch_end_pos, TP_OFFSET)?;
            if batch_tp != tp_filter {
                continue;
            }
        }

        
        let events = read_batch_at_position(&mut reader, *batch_end_pos, *batch_start_pos)?;        
        event_batches.push(Arc::new(events));
                
        let compressed_data_size = (batch_end_pos - batch_start_pos) as usize;
        total_bytes += compressed_data_size + BATCH_START_SIZE as usize + BATCH_METADATA_SIZE as usize;

        // Check if adding this batch would exceed our limit
        if total_bytes > max_bytes {
            // We've hit our limit, return what we have
            let next_si = Some(event_batches.last().unwrap().si + 1);
            return Ok(CatchupResult {
                event_batches: event_batches,
                next_si: next_si
            });
        }
    }

    Ok(CatchupResult {
        event_batches: event_batches,
        next_si: None,
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::event_item::tests::{create_minimal_event_item, create_test_event_item};
    use crate::event_item::EventItem;
    use crate::file_cache::{create_append_writer, create_reader};
    use std::fs::OpenOptions;
    use std::usize;
    use tempfile::TempDir;

    pub fn create_event_batch_item(
        si: u64,
        cb: Option<String>,
        sd: u64,
        events: Vec<EventItem>,
    ) -> EventBatchItem {
        EventBatchItem {
            si,
            cb,
            sd,
            events,
        }
    }

    #[test]
    fn test_corrupt_file() {
        let events_batch_1 = create_event_batch_item(0, None, 123, vec![
            create_test_event_item(),
            create_minimal_event_item(),
            create_test_event_item(),
        ]);
        let events_batch_2 = create_event_batch_item(1, None, 456, vec![
            create_test_event_item(),
            create_minimal_event_item(),
        ]);
        let events_batch_3 = create_event_batch_item(2, None, 789, vec![
            create_minimal_event_item(),
        ]);

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
        let file = OpenOptions::new()
            .write(true)
            .open(events_bin.to_str().unwrap())
            .unwrap();

        // Set the file length.
        file.set_len(current_file_size + 99).unwrap();
        
        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();
        let catchup_result = read_from_si(&mut reader, 0, usize::MAX, None);
        
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
        let events_batch_1 = create_event_batch_item(0, None, 123, vec![
            create_test_event_item(),
            create_minimal_event_item(),
            create_test_event_item(),
        ]);
        let events_batch_2 = create_event_batch_item(1, None, 456, vec![
            create_test_event_item(),
            create_minimal_event_item(),
        ]);
        let events_batch_3 = create_event_batch_item(2, None, 789, vec![
            create_minimal_event_item(),
        ]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();
        append_event_batch(&mut writer, &events_batch_2).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        let catchup_result = read_from_si(&mut reader, 0, 1, None).unwrap();

        assert_eq!(catchup_result.event_batches.len(), 1);
        assert_eq!(catchup_result.next_si, Some(1));

        let current_file_size = reader.get_ref().metadata().unwrap().len();

        let catchup_result = read_from_si(&mut reader, 0, current_file_size as usize + 58, None).unwrap();

        assert_eq!(catchup_result.event_batches.len(), 2);
        assert_eq!(catchup_result.next_si, Some(2));

        append_event_batch(&mut writer, &events_batch_3).unwrap();

        let catchup_result = read_from_si(&mut reader, 0, current_file_size as usize + 58, None).unwrap();

        assert_eq!(catchup_result.event_batches.len(), 2);
        assert_eq!(catchup_result.next_si, Some(2));

        let catchup_result = read_from_si(&mut reader, 0, current_file_size as usize + 316, None).unwrap();

        assert_eq!(catchup_result.event_batches.len(), 3);
        assert_eq!(catchup_result.next_si, None);
    }

    #[test]
    fn test_read_write_with_event_storage_format() {
        let events_batch_1 = create_event_batch_item(0, None, 123, vec![
            create_test_event_item(),
            create_minimal_event_item(),
            create_test_event_item(),
        ]);
        let events_batch_2 = create_event_batch_item(1, None, 456, vec![
            create_test_event_item(),
            create_minimal_event_item(),
        ]);
        let events_batch_3 = create_event_batch_item(2, None, 789, vec![
            create_minimal_event_item(),
        ]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        let result_events_batch_1 = read_from_si(&mut reader, 0, usize::MAX, None).unwrap().event_batches;

        assert_eq!(result_events_batch_1.len(), 1);
        assert_eq!(events_batch_1.si, result_events_batch_1[0].si);

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(0));

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_2).unwrap();

        let result_events_batches = read_from_si(&mut reader, 0, usize::MAX, None).unwrap().event_batches;

        assert_eq!(result_events_batches.len(), 2);
        assert_eq!(events_batch_1.si, result_events_batches[0].si);
        assert_eq!(events_batch_2.si, result_events_batches[1].si);

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(1));

        append_event_batch(&mut writer, &events_batch_3).unwrap();

        let result_events_batches = read_from_si(&mut reader, 0, usize::MAX, None).unwrap().event_batches;

        assert_eq!(result_events_batches.len(), 3);
        assert_eq!(events_batch_1.si, result_events_batches[0].si);
        assert_eq!(events_batch_2.si, result_events_batches[1].si);
        assert_eq!(events_batch_3.si, result_events_batches[2].si);

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(2));
    }

    #[test]
    fn test_invalid_catch_up() {
        let events_batch_1 = create_event_batch_item(0, None, 123, vec![
            create_test_event_item(),
            create_minimal_event_item(),
            create_test_event_item(),
        ]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        let invalid_si_over = read_from_si(&mut reader, 1000, usize::max_value(), None).unwrap();

        assert_eq!(invalid_si_over.event_batches.len(), 0);
        assert_eq!(invalid_si_over.next_si, Option::None);
    }

    #[test]
    fn test_valid_catchup_scenarios() {
        let events_batch_1 = create_event_batch_item(0, None, 123, vec![
            create_test_event_item(),
            create_minimal_event_item(),
            create_test_event_item(),
        ]);
        let events_batch_2 = create_event_batch_item(1, None, 456, vec![
            create_test_event_item(),
            create_minimal_event_item(),
        ]);
        let events_batch_3 = create_event_batch_item(2, None, 789, vec![
            create_minimal_event_item(),
        ]);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let mut writer = create_append_writer(events_bin.to_str().unwrap()).unwrap();

        append_event_batch(&mut writer, &events_batch_1).unwrap();

        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(0));

        let si_0_result = read_from_si(&mut reader, 0, usize::max_value(), None).unwrap();

        assert_eq!(si_0_result.event_batches.len(), 1);
        assert_eq!(si_0_result.event_batches[0].events.len(), 3);
        assert_eq!(events_batch_1.si, si_0_result.event_batches[0].si);

        append_event_batch(&mut writer, &events_batch_2).unwrap();

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(1));

        let read_result = read_from_si(&mut reader, 0, usize::max_value(), None).unwrap();

        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(events_batch_1.si, read_result.event_batches[0].si);
        assert_eq!(events_batch_2.si, read_result.event_batches[1].si);

        let read_result = read_from_si(&mut reader, 1, usize::max_value(), None).unwrap();

        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(events_batch_2.si, read_result.event_batches[0].si);

        append_event_batch(&mut writer, &events_batch_3).unwrap();

        let last_si = find_last_si(&mut reader).unwrap();
        assert_eq!(last_si, Some(2));

        let read_result = read_from_si(&mut reader, 2, usize::max_value(), None).unwrap();

        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(read_result.event_batches[0].events.len(), 1);
        assert_eq!(events_batch_3.si, read_result.event_batches[0].si);

        let read_result = read_from_si(&mut reader, 1, usize::max_value(), None).unwrap();
        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(events_batch_2.si, read_result.event_batches[0].si);
        assert_eq!(events_batch_3.si, read_result.event_batches[1].si);
    }

    #[test]
    fn test_find_last_valid_event_batch_corrupted_file() {
        let events_batch_1 = create_event_batch_item(0, None, 123, vec![
            create_test_event_item(),
            create_minimal_event_item(),
        ]);
        let events_batch_2 = create_event_batch_item(1, None, 456, vec![
            create_test_event_item(),
        ]);
        let events_batch_3 = create_event_batch_item(2, None, 789, vec![
            create_minimal_event_item(),
        ]);

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
        let file = OpenOptions::new()
            .write(true)
            .open(events_bin.to_str().unwrap())
            .unwrap();
        file.set_len(valid_end_pos + 50).unwrap(); // Truncate partway through third batch

        // Test that find_last_valid_event_batch returns position after second batch
        let mut reader = create_reader(events_bin.to_str().unwrap()).unwrap();
        let last_valid_pos = find_last_valid_event_batch(&mut reader).unwrap();
        
        assert_eq!(last_valid_pos, valid_end_pos);
    }

    #[test]
    fn test_find_last_valid_event_batch_uncorrupted_file() {
        let events_batch_1 = create_event_batch_item(0, None, 123, vec![
            create_test_event_item(),
            create_minimal_event_item(),
        ]);
        let events_batch_2 = create_event_batch_item(1, None, 456, vec![
            create_test_event_item(),
        ]);

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

}