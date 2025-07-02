use std::{fs, io};
use crate::{event_batch_item::EventBatchItem};

pub fn serialize_event_batch_item(events: &EventBatchItem) -> io::Result<Vec<u8>> {
    bincode::encode_to_vec(events, bincode::config::standard())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    // postcard::to_allocvec(events)
    //     .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
}

pub fn compress_data(data: &[u8]) -> io::Result<Vec<u8>> {
    zstd::bulk::compress(data, 6)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
}

pub fn decompress_data(data: &[u8], capacity: usize) -> io::Result<Vec<u8>> {
    zstd::bulk::decompress(data, capacity)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
}

pub fn deserialize_event_batch_item(data: &[u8]) -> io::Result<EventBatchItem> {
    bincode::decode_from_slice(data, bincode::config::standard())
        .map(|(events, _)| events)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    // postcard::from_bytes(data)
    //     .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
}

pub fn load_event_batch_item_from_json(path: &str) -> Result<EventBatchItem, Box<dyn std::error::Error>> {
    let mut json_content = fs::read(path)?; // Note: read bytes, not string
    let storage: EventBatchItem = simd_json::from_slice(&mut json_content)?;

    Ok(storage)
}

pub fn save_event_batch_item_to_json(event_item: &EventBatchItem, path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let json_content = simd_json::to_string(event_item)?;
    fs::write(path, json_content)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};
    use tempfile::TempDir;
    use crate::{event_item::tests::{create_minimal_event_item, create_test_event_item}, file_cache::create_append_writer};
    use super::*;

    #[test]
    fn test_write_event_batch_item() {

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.cb = Some("test".to_string());
        event_batch_item.si = 0;
        event_batch_item.sd = 23432;
        event_batch_item.events.push(create_test_event_item());
        event_batch_item.events.push(create_minimal_event_item());

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();

        let event_batch_bin = temp_path.join("event_batch.bin");
        let event_batch_json = temp_path.join("event_batch.json");

        let mut writer =
            create_append_writer(event_batch_bin.to_str().unwrap()).expect("Open writer to event_batch.bin");
        let encoded_events = serialize_event_batch_item(&event_batch_item).expect("Serialize event_batch");
        let compressed_events = compress_data(&encoded_events).expect("Compress event_batch");
        writer
            .write_all(&compressed_events)
            .expect("write event_batch to bin");
        writer.flush().expect("flush event_batch bin");

        save_event_batch_item_to_json(&event_batch_item, event_batch_json.to_str().unwrap()).expect("Failed to write event_batch to json");

        // Print file sizes
        let bin_size = fs::metadata(&event_batch_bin).expect("Failed to get bin file metadata").len();
        let json_size = fs::metadata(&event_batch_json).expect("Failed to get event_batch json metadata").len();
        
        println!("File sizes:");
        println!("  event_batch.bin (compressed): {} bytes", bin_size);
        println!("  event_batch.json: {} bytes", json_size);
        println!("  Original serialized size: {} bytes", encoded_events.len());
        println!("  Compression ratio: {:.2}%", (bin_size as f64 / encoded_events.len() as f64) * 100.0);
        println!("  JSON vs Binary ratio: {:.2}%", (json_size as f64 / bin_size as f64) * 100.0);

        let compressed_data = fs::read(&event_batch_bin).expect("Failed to read event_batch.bin");
        let original_size = u64::from_le_bytes((encoded_events.len() as u64).to_le_bytes());
        let decompressed_data = decompress_data(&compressed_data, original_size as usize)
            .expect("Failed to decompress data");
        let deserialized_events =
            deserialize_event_batch_item(&decompressed_data).expect("Failed to deserialize event_batch");

        assert_eq!(
            2,
            deserialized_events.events.len(),
            "Event count mismatch"
        );

        // Compare event1
        assert_eq!(deserialized_events.cb, event_batch_item.cb);
        assert_eq!(deserialized_events.si, event_batch_item.si);
        assert_eq!(deserialized_events.events[0].ed, event_batch_item.events[0].ed);
        assert_eq!(deserialized_events.events[0].tp, event_batch_item.events[0].tp);
        assert_eq!(deserialized_events.events[0].int_values, event_batch_item.events[0].int_values);
        assert_eq!(deserialized_events.events[0].f32_values, event_batch_item.events[0].f32_values);
        assert_eq!(
            deserialized_events.events[0].string_values,
            event_batch_item.events[0].string_values
        );
        assert_eq!(deserialized_events.events[0].byte_arrays, event_batch_item.events[0].byte_arrays);

        // Compare event2
        assert_eq!(deserialized_events.events[1].ed, event_batch_item.events[1].ed);
        assert_eq!(deserialized_events.events[1].tp, event_batch_item.events[1].tp);
        assert_eq!(deserialized_events.events[1].int_values, event_batch_item.events[1].int_values);
        assert_eq!(deserialized_events.events[1].f32_values, event_batch_item.events[1].f32_values);
        assert_eq!(
            deserialized_events.events[1].string_values,
            event_batch_item.events[1].string_values
        );

        // Validation: Read back JSON files and compare
        let event1_from_json = load_event_batch_item_from_json(event_batch_json.to_str().unwrap()).expect("Failed to read event_batch from JSON");

        // Compare event1
        assert_eq!(event1_from_json.cb, event_batch_item.cb);
        assert_eq!(event1_from_json.si, event_batch_item.si);
        assert_eq!(event1_from_json.events[0].ed, event_batch_item.events[0].ed);
        assert_eq!(event1_from_json.events[0].tp, event_batch_item.events[0].tp);
        assert_eq!(event1_from_json.events[0].int_values, event_batch_item.events[0].int_values);
        assert_eq!(event1_from_json.events[0].f32_values, event_batch_item.events[0].f32_values);
        assert_eq!(
            event1_from_json.events[0].string_values,
            event_batch_item.events[0].string_values
        );
        assert_eq!(event1_from_json.events[0].byte_arrays, event_batch_item.events[0].byte_arrays);

        // Compare event2
        assert_eq!(event1_from_json.events[1].ed, event_batch_item.events[1].ed);
        assert_eq!(event1_from_json.events[1].tp, event_batch_item.events[1].tp);
        assert_eq!(event1_from_json.events[1].int_values, event_batch_item.events[1].int_values);
        assert_eq!(event1_from_json.events[1].f32_values, event_batch_item.events[1].f32_values);
        assert_eq!(
            event1_from_json.events[1].string_values,
            event_batch_item.events[1].string_values
        );
    }
}