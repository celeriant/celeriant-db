use std::{fs::{metadata, File}, io::{self, Read, Seek, SeekFrom, Write}, sync::Arc};

use crate::{catchup_result::{CatchupResult}, event_batch_item::EventBatchItem, event_storage::{append_event_batch, find_last_si, read_from_si}, file_cache::FileCache, last_si_cache::LastSiCache, memory_cache::MemoryCache};

pub struct EventStorageCache {
    file_cache: FileCache,
    last_si_cache: LastSiCache,
    memory_cache: MemoryCache,
}

impl EventStorageCache {
    pub fn new(memory_cache_ttl_secs: u64, last_si_cache_max_size: usize, file_cache_max_handles: usize) -> Self {
        Self {
            file_cache: FileCache::new(file_cache_max_handles),
            last_si_cache: LastSiCache::new(last_si_cache_max_size),
            memory_cache: MemoryCache::new(memory_cache_ttl_secs),
        }
    }

    fn get_transaction_path(&self, file_path: &str) -> String {
        format!("{}.transaction", file_path)
    }

    fn open_transaction(&mut self, file_path: &str) -> io::Result<()> {
        let transaction_path = self.get_transaction_path(file_path);
        
        // Get current file length
        let file_length = match metadata(file_path) {
            Ok(metadata) => metadata.len(),
            Err(_) => 0, // File doesn't exist yet
        };
        
        // Write file length to transaction file
        self.write_transaction_value(&transaction_path, file_length)
    }

    fn write_transaction_value(&mut self, transaction_path: &str, file_length: u64) -> io::Result<()> {
        let writer_ref = self.file_cache.create_overwrite_writer(&transaction_path)?;
        let mut writer = writer_ref.borrow_mut();

        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(&file_length.to_le_bytes())?;
        writer.flush()?;
        Ok(())
    }
    
    fn has_transaction(&mut self, file_path: &str) -> io::Result<Option<u64>> {
        let transaction_path = self.get_transaction_path(file_path);
        
        match self.file_cache.create_reader(&transaction_path) {
            Ok(reader_ref) => {
                let mut reader = reader_ref.borrow_mut();
                reader.seek(SeekFrom::Start(0))?;

                // Check if file has content by trying to read 8 bytes
                let mut buffer = [0u8; 8];
                match reader.read_exact(&mut buffer) {
                    Ok(_) => {
                        let file_length = u64::from_le_bytes(buffer);
                        Ok(Some(file_length))
                    },
                    Err(_) => Ok(None), // File is empty or can't read 8 bytes
                }
            }
            Err(_) => Ok(None), // File doesn't existc
        }
    }
    
    fn commit_transaction(&mut self, file_path: &str) -> io::Result<()> {
        let transaction_path = self.get_transaction_path(file_path);
        
        // Empty out the file but keep it
        let writer_ref = self.file_cache.create_overwrite_writer(&transaction_path)?;
        let mut writer = writer_ref.borrow_mut();

        writer.seek(std::io::SeekFrom::Start(0))?;
        writer.get_mut().set_len(0)?;
        writer.flush()?;

        Ok(())
    }

    fn recover_from_transaction_file(&mut self, file_path: &str) {
        // Check for existing transaction and recover if needed
        if let Ok(Some(original_file_length)) = self.has_transaction(file_path) {
            // Truncate file to original length to recover from crash
            if let Ok(file) = std::fs::OpenOptions::new()
                .write(true)
                .open(file_path)
            {
                let _ = file.set_len(original_file_length);
            }
            
            // Clear the caches for this file
            self.file_cache.remove(file_path);
            self.last_si_cache.remove(file_path);
        }
    }

    fn recover_file_using_magic_number(&mut self, reader: &mut std::cell::RefMut<'_, io::BufReader<File>>, file_path: &str) -> bool {
        todo!()
    }

    pub fn get_last_si(&mut self, file_path: &str) -> io::Result<Option<u64>> {
        self.recover_from_transaction_file(file_path);
        self.get_last_si_internal(file_path, true)
    }

    fn get_last_si_internal(&mut self, file_path: &str, use_cache: bool) -> io::Result<Option<u64>> {
        let mut last_si: Option<u64> = None;
        
        if use_cache {
            self.last_si_cache.get(file_path);
        }
        
        if last_si == None {
            match self.file_cache.create_reader(file_path) {
                Ok(reader_ref) => {

                    //File exists, try to read it
                    let mut reader = reader_ref.borrow_mut();

                    match find_last_si(&mut reader) {
                        Ok(si) => last_si = si,
                        Err(_) => {
                            //File is corrupted, try to recover it
                            if self.recover_file_using_magic_number(&mut reader, file_path) {
                                match find_last_si(&mut reader) {
                                    Ok(si) => last_si = si,
                                    Err(_) => {
                                        return Err(io::Error::new(io::ErrorKind::Other, "File is corrupted and cannot be recovered"));
                                    },
                                }
                            } else {
                                return Err(io::Error::new(io::ErrorKind::Other, "File is corrupted and cannot be recovered"));
                            }
                        }
                    }
                },
                Err(_) => { 
                    //File doesn't exist yet, keep si as None
                }
            };
        }

        Ok(last_si)
    }

    pub fn write(&mut self, file_path: &str, allow_create: bool, mut event_batch_item: EventBatchItem) -> io::Result<u64> {

        self.recover_from_transaction_file(file_path);

        // Get the last SI from cache or load from file
        let last_si = self.get_last_si_internal(file_path, false)?;

        if last_si.is_none() && !allow_create {
            return Err(io::Error::new(io::ErrorKind::NotFound, "File not found"));
        }

        event_batch_item.si = last_si.map_or(0, |si| si + 1);

        self.open_transaction(file_path)?;
        
        // Write the batch to disk
        let writer_ref = self.file_cache.create_append_writer(file_path)?;
        let mut writer = writer_ref.borrow_mut();

        let compressed_batch_size = append_event_batch(
            &mut writer, 
            &event_batch_item)?;

        // Update cache and return the last SI that was assigned
        let si = event_batch_item.si;

        self.last_si_cache.update(file_path, si);
        self.memory_cache.put(file_path, si, Arc::new(event_batch_item), compressed_batch_size);

        self.commit_transaction(file_path)?;

        Ok(si)
    }

    pub fn read(&mut self, file_path: &str, from_si: u64, max_bytes: usize) -> io::Result<CatchupResult> {
        let mut event_batches = Vec::new();
        let mut current_si = from_si;
        let mut number_bytes: usize = 0;
        let mut more_batches_to_go: bool = false;

        // First, try to use the memory cache to get events if within the TTL
        loop {
            if let Some((cached_event_batch_item, compressed_batch_size)) = self.memory_cache.get(file_path, current_si) {
                current_si = cached_event_batch_item.si + 1;
                event_batches.push(cached_event_batch_item);
                number_bytes += compressed_batch_size;
                more_batches_to_go = number_bytes >= max_bytes;
                if more_batches_to_go {
                    break;
                }
            } else {
                break;
            }
        }

        if more_batches_to_go {
            let next_batch_cannot_return = self.memory_cache.get(file_path, current_si);
            more_batches_to_go = next_batch_cannot_return.is_some();
        }        

        if !event_batches.is_empty() {
            return Ok(CatchupResult {
                event_batches: event_batches,
                next_si: if more_batches_to_go { Some(current_si) } else { None },
            });
        }
        
        self.recover_from_transaction_file(file_path);

        match self.file_cache.create_reader(file_path) {
            Ok(reader_mut) => {
                let mut reader = reader_mut.borrow_mut();

                match read_from_si(&mut reader, from_si, max_bytes) {
                    Ok(catchup_result) => return Ok(catchup_result),
                    Err(_) => {
                        //File is corrupted, try to recover it
                        if self.recover_file_using_magic_number(&mut reader, file_path) {
                            match read_from_si(&mut reader, from_si, max_bytes) {
                                Ok(catchup_result) => return Ok(catchup_result),
                                Err(_) => {
                                    return Err(io::Error::new(io::ErrorKind::Other, "File is corrupted and cannot be recovered"));
                                },
                            }
                        } else {
                            return Err(io::Error::new(io::ErrorKind::Other, "File is corrupted and cannot be recovered"));
                        }
                    }
                }
                
            },
            Err(_) => {
                return Err(io::Error::new(io::ErrorKind::NotFound, "File not found"));
            }
        }
    }

    pub fn delete(&mut self, file_path: &str) -> io::Result<bool> {        
        // Check if file exists
        if !std::path::Path::new(file_path).exists() {
                return Err(io::Error::new(io::ErrorKind::NotFound, "File not found"));
        }
        
        // Remove the actual file (this is atomic)
        match std::fs::remove_file(file_path) {
            Ok(_) => {
                // Clear caches for this file
                self.file_cache.remove(file_path);
                self.last_si_cache.remove(file_path);
                self.memory_cache.invalidate_file(file_path);
                
                // Clean up any leftover transaction file
                let transaction_path = self.get_transaction_path(file_path);
                let _ = std::fs::remove_file(transaction_path); // Ignore errors
                
                Ok(true)
            },
            Err(e) => Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use crate::{event_item::{tests::{create_minimal_event_item, create_test_event_item}, EventItem}, event_storage::tests::create_event_batch_item};

    use super::*;

    #[test]
    fn test_delete_file() {
        let mut storage = EventStorageCache::new(30, 1000000, 10000);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");
        let file_path = events_bin.to_str().unwrap();

        // Write some events
        let events_batch = vec![create_test_event_item(), create_minimal_event_item()];
        let event_batch_item = create_event_batch_item(0, None, 0, events_batch);
        let last_si = storage.write(file_path, true, event_batch_item).unwrap();
        assert_eq!(last_si, 0);

        // Verify file exists and has content
        assert!(std::path::Path::new(file_path).exists());
        let events = storage.read(file_path, 0, 1000).unwrap();
        assert_eq!(events.flatten_events().len(), 2);

        // Delete the file
        let deleted = storage.delete(file_path).unwrap();
        assert!(deleted);

        // Verify file is gone
        assert!(!std::path::Path::new(file_path).exists());

        // Verify reading returns empty result
        let result_read = storage.read(file_path, 0, 1000);
        assert!(result_read.is_err());
        assert_eq!(result_read.unwrap_err().kind(), io::ErrorKind::NotFound);

        // Verify transaction file is also cleaned up
        let transaction_path = storage.get_transaction_path(file_path);
        assert!(!std::path::Path::new(&transaction_path).exists());
    }

    #[test]
    fn test_delete_nonexistent_file() {
        let mut storage = EventStorageCache::new(30, 1000000, 10000);
        
        let result = storage.delete("nonexistent_file.bin");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }
    
    #[test]
    fn test_round_trip() {
        let mut storage = EventStorageCache::new(30, 1000000, 10000);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let events_batch_1 = vec![create_test_event_item(), create_minimal_event_item(), create_test_event_item()];
        let events_batch_2 = vec![create_test_event_item(), create_minimal_event_item()];
        let events_batch_3 = vec![create_minimal_event_item()];

        let str_compare_value = events_batch_2[0].string_values.as_ref().unwrap()[4].as_ref().unwrap().clone();

        let event_batch_item_1 = create_event_batch_item(0, None, 0, events_batch_1);
        let event_batch_item_2 = create_event_batch_item(0, None, 0, events_batch_2);
        let event_batch_item_3 = create_event_batch_item(0, None, 0, events_batch_3);

        let last_si = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_1).unwrap();
        assert_eq!(last_si, 0);

        let last_si = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_2).unwrap();
        assert_eq!(last_si, 1);

        let last_si = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_3).unwrap();
        assert_eq!(last_si, 2);

        let events_from_1 = storage.read(&events_bin.to_str().unwrap(), 1, 40000).unwrap();

        assert_eq!(events_from_1.flatten_events().len(), 3);
        assert_eq!(events_from_1.next_si, None);
        assert_eq!(events_from_1.event_batches.len(), 2);
        assert_eq!(events_from_1.event_batches[0].si, 1);
        assert_eq!(events_from_1.event_batches[1].si, 2);

        assert_eq!(events_from_1.flatten_events()[1].string_values.as_ref().unwrap()[1].as_ref().unwrap(), "World");

        assert_eq!(str_compare_value, *events_from_1.flatten_events()[0].string_values.as_ref().unwrap()[4].as_ref().unwrap());
    }

    #[test]
    fn test_read_file_not_exists() {
        let mut storage = EventStorageCache::new(30, 1000000, 10000);
        let events_from_3 = storage.read("unknownfile.bin", 0, 40000);
        assert!(events_from_3.is_err());
        assert_eq!(events_from_3.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn test_multi_file() {
        let mut storage = EventStorageCache::new(30, 1000000, 10000);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let temp_dir_2 = TempDir::new().expect("Failed to create temp directory");
        let temp_path_2 = temp_dir_2.path();
        let events_bin_2 = temp_path_2.join("events.bin");

        let events_batch_1 = vec![create_test_event_item(), create_minimal_event_item(), create_test_event_item()];
        let events_batch_2 = vec![create_test_event_item(), create_minimal_event_item()];

        let event_batch_item_1 = create_event_batch_item(0, None, 0, events_batch_1);
        let event_batch_item_2 = create_event_batch_item(0, None, 0, events_batch_2);

        let last_si_1 = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_1).unwrap();
        assert_eq!(last_si_1, 0);

        let last_si_2 = storage.write(&events_bin_2.to_str().unwrap(), true, event_batch_item_2).unwrap();
        assert_eq!(last_si_2, 0);

        let file1 = storage.read(&events_bin.to_str().unwrap(), 0, 100).unwrap();
        let file2 = storage.read(&events_bin_2.to_str().unwrap(), 0, 100).unwrap();

        assert_eq!(file1.flatten_events().len(), 3);
        assert_eq!(file2.flatten_events().len(), 2);

    }

    // Ensure the read also does a recovery on crash check
    #[test]
    fn test_file_recovery_basic() {
        let mut storage = EventStorageCache::new(30, 1000000, 10000);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let events_batch_1 = vec![create_test_event_item(), create_minimal_event_item(), create_test_event_item()];
        let events_batch_2 = vec![create_test_event_item(), create_minimal_event_item()];

        let event_batch_item_1 = create_event_batch_item(0, None, 0, events_batch_1);
        let event_batch_item_2 = create_event_batch_item(0, None, 0, events_batch_2);

        let last_si = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_1).unwrap();
        assert_eq!(last_si, 0);

        let file_length = fs::metadata(&events_bin).unwrap().len();

        let last_si = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_2).unwrap();
        assert_eq!(last_si, 1);

        // simulate crash by writing the transaction file with the prev len
        let transaction_path = storage.get_transaction_path(events_bin.to_str().unwrap());
        storage.write_transaction_value(&transaction_path, file_length).unwrap();

        storage.memory_cache.invalidate_file(events_bin.to_str().unwrap());

        let events_from_0 = storage.read(&events_bin.to_str().unwrap(), 0, 100).unwrap();
        assert_eq!(events_from_0.flatten_events().len(), 3);
        
    }

    // Ensure the write does a recovery on crash check (re-write scenario)
    #[test]
    fn test_file_recovery() {
        let mut storage = EventStorageCache::new(30, 1000000, 10000);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let events_batch_1 = vec![create_test_event_item(), create_minimal_event_item(), create_test_event_item()];
        let events_batch_2 = vec![create_test_event_item(), create_minimal_event_item()];
        let str_compare_value = events_batch_2[0].string_values.as_ref().unwrap()[4].as_ref().unwrap().clone();
        let events_batch_3 = vec![create_minimal_event_item()];

        let event_batch_item_1 = create_event_batch_item(0, None, 0, events_batch_1);
        let event_batch_item_2 = create_event_batch_item(0, None, 0, events_batch_2);
        let event_batch_item_3 = create_event_batch_item(0, None, 0, events_batch_3);

        let last_si = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_1).unwrap();
        assert_eq!(last_si, 0);

        // store file len here
        let file_length = fs::metadata(&events_bin).unwrap().len();

        let last_si = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_2.clone()).unwrap();
        assert_eq!(last_si, 1);

        // simulate crash by writing the transaction file with the prev len
        let transaction_path = storage.get_transaction_path(events_bin.to_str().unwrap());
        storage.write_transaction_value(&transaction_path, file_length).unwrap();

        // try write again
        let last_si = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_2).unwrap();
        assert_eq!(last_si, 1);

        let last_si = storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_3).unwrap();
        assert_eq!(last_si, 2);


        // Same assertions as above round trip test, no duplicate write
        let events_from_3 = storage.read(&events_bin.to_str().unwrap(), 1, 40000).unwrap();

        assert_eq!(events_from_3.flatten_events().len(), 3);
        assert_eq!(events_from_3.next_si, None);
        assert_eq!(events_from_3.event_batches.len(), 2);
        assert_eq!(events_from_3.event_batches[0].si, 1);
        assert_eq!(events_from_3.event_batches[1].si, 2);

        assert_eq!(events_from_3.flatten_events()[1].string_values.as_ref().unwrap()[1].as_ref().unwrap(), "World");

        assert_eq!(str_compare_value, *events_from_3.flatten_events()[0].string_values.as_ref().unwrap()[4].as_ref().unwrap());

    }

    #[test]
    fn test_memory_cache_read() {
        let mut storage = EventStorageCache::new(3000, 1000000, 10000);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();
        let events_bin = temp_path.join("events.bin");

        let events_batch_1 = vec![create_test_event_item(), create_minimal_event_item()];
        let events_batch_2 = vec![create_test_event_item()];
        
        let event_batch_item_1 = create_event_batch_item(0, None, 0, events_batch_1);
        let event_batch_item_2 = create_event_batch_item(0, None, 0, events_batch_2);

        storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_1.clone()).unwrap();
        storage.write(&events_bin.to_str().unwrap(), true, event_batch_item_2.clone()).unwrap();

        // Read events from memory cache
        let cached_events = storage.read(&events_bin.to_str().unwrap(), 0, 1000000).unwrap();
        assert_eq!(cached_events.flatten_events().len(), 3);

        // Test max_bytes limit
        let partial_events = storage.read(&events_bin.to_str().unwrap(), 0, 1).unwrap();
        assert_eq!(partial_events.next_si, Some(1)); // Should continue from SI 1 after reaching the limit
        assert_eq!(partial_events.flatten_events().len(), 2);

        //Test in-mem from si 1
        let partial_events = storage.read(&events_bin.to_str().unwrap(), 1, 1).unwrap();
        assert_eq!(partial_events.next_si, None); // Should continue from SI 1 after reaching the limit
        assert_eq!(partial_events.flatten_events().len(), 1);
    }
}