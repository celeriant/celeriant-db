use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter},
    rc::Rc,
};

struct FileHandle {
    file_path: String,
    is_reader: bool,
}

pub struct FileCache {
    readers: HashMap<String, Rc<RefCell<BufReader<File>>>>,
    writers: HashMap<String, Rc<RefCell<BufWriter<File>>>>,
    #[cfg(target_os = "linux")]
    raw_readers: HashMap<String, Rc<RefCell<File>>>,
    handle_queue: VecDeque<FileHandle>,
    max_handles: usize,
}

impl FileCache {
    pub fn new(max_handles: usize) -> Self {
        Self {
            readers: HashMap::new(),
            writers: HashMap::new(),
            #[cfg(target_os = "linux")]
            raw_readers: HashMap::new(),
            handle_queue: VecDeque::new(),
            max_handles,
        }
    }

    fn evict(&mut self) {
        #[cfg(target_os = "linux")]
        let mut total_handles = self.readers.len() + self.writers.len() + self.raw_readers.len();
        #[cfg(not(target_os = "linux"))]
        let mut total_handles = self.readers.len() + self.writers.len();

        while total_handles >= self.max_handles {
            if let Some(handle) = self.handle_queue.pop_front() {
                if handle.is_reader {
                    #[cfg(target_os = "linux")]
                    if self.raw_readers.remove(&handle.file_path).is_some() {
                        total_handles -= 1;
                    }
                    #[cfg(not(target_os = "linux"))]
                    if self.readers.remove(&handle.file_path).is_some() {
                        total_handles -= 1;
                    }
                } else {
                    if self.writers.remove(&handle.file_path).is_some() {
                        total_handles -= 1;
                    }
                }
            } else {
                // Queue is empty, but we're still over the limit. This shouldn't happen,
                // but we need a way to avoid an infinite loop.  Possibly log an error.
                break;
            }
        }
    }

    fn track_handle(&mut self, file_path: String, is_reader: bool) {
        self.handle_queue.push_back(FileHandle {
            file_path,
            is_reader,
        });
    }

    pub fn create_overwrite_writer(
        &mut self,
        file_path: &str,
    ) -> io::Result<Rc<RefCell<BufWriter<File>>>> {
        self.evict();

        // Check if exists and clone the Rc in a separate scope
        if let Some(existing) = self.writers.get(file_path).cloned() {
            return Ok(existing);
        }

        let buf = create_overwrite_writer(file_path)?;
        let rc_buf = Rc::new(RefCell::new(buf));
        self.writers
            .insert(file_path.to_string(), Rc::clone(&rc_buf));

        self.track_handle(file_path.to_string(), false);
        Ok(rc_buf)
    }

    pub fn create_append_writer(
        &mut self,
        file_path: &str,
    ) -> io::Result<Rc<RefCell<BufWriter<File>>>> {
        self.evict();

        // Check if exists and clone the Rc in a separate scope
        if let Some(existing) = self.writers.get(file_path).cloned() {
            return Ok(existing);
        }

        let buf = create_append_writer(file_path)?;
        let rc_buf = Rc::new(RefCell::new(buf));
        self.writers
            .insert(file_path.to_string(), Rc::clone(&rc_buf));

        self.track_handle(file_path.to_string(), false);
        Ok(rc_buf)
    }

    #[cfg(target_os = "linux")]
    pub fn create_reader(&mut self, file_path: &str) -> io::Result<Rc<RefCell<File>>> {
        self.evict();

        // Check if exists and clone the Rc in a separate scope
        if let Some(existing) = self.raw_readers.get(file_path).cloned() {
            return Ok(existing);
        }

        let file = OpenOptions::new().read(true).open(file_path)?;
        let rc_file = Rc::new(RefCell::new(file));
        self.raw_readers
            .insert(file_path.to_string(), Rc::clone(&rc_file));

        self.track_handle(file_path.to_string(), true);
        Ok(rc_file)
    }

    #[cfg(not(target_os = "linux"))]
    pub fn create_reader(&mut self, file_path: &str) -> io::Result<Rc<RefCell<BufReader<File>>>> {
        self.evict();

        // Check if exists and clone the Rc in a separate scope
        if let Some(existing) = self.readers.get(file_path).cloned() {
            return Ok(existing);
        }

        let buf = create_reader(file_path)?;
        let rc_buf = Rc::new(RefCell::new(buf));
        self.readers
            .insert(file_path.to_string(), Rc::clone(&rc_buf));

        self.track_handle(file_path.to_string(), true);
        Ok(rc_buf)
    }

    pub fn remove(&mut self, file_path: &str) {
        self.writers.remove(file_path);
        self.readers.remove(file_path);
        #[cfg(target_os = "linux")]
        self.raw_readers.remove(file_path);
        self.handle_queue
            .retain(|handle| handle.file_path != file_path);
    }
}

pub fn create_overwrite_writer(file_path: &str) -> io::Result<BufWriter<File>> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)?;
    Ok(BufWriter::new(file))
}

pub fn create_append_writer(file_path: &str) -> io::Result<BufWriter<File>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;
    Ok(BufWriter::new(file))
}

pub fn create_reader(file_path: &str) -> io::Result<BufReader<File>> {
    let file = OpenOptions::new().read(true).open(file_path)?;
    Ok(BufReader::new(file))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_eviction_logic() {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = FileCache::new(2); // Set max handles to 2

        // Create test files
        let file1 = temp_dir.path().join("test1.txt");
        let file2 = temp_dir.path().join("test2.txt");
        let file3 = temp_dir.path().join("test3.txt");

        // Create files
        fs::write(&file1, "test1").unwrap();
        fs::write(&file2, "test2").unwrap();
        fs::write(&file3, "test3").unwrap();

        let file1_str = file1.to_str().unwrap();
        let file2_str = file2.to_str().unwrap();
        let file3_str = file3.to_str().unwrap();

        // Add first reader - should not trigger eviction
        cache.create_reader(file1_str).unwrap();
        #[cfg(target_os = "linux")]
        {
            assert_eq!(cache.raw_readers.len(), 1);
            assert_eq!(cache.writers.len(), 0);
            assert_eq!(cache.handle_queue.len(), 1);
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(cache.readers.len(), 1);
            assert_eq!(cache.writers.len(), 0);
            assert_eq!(cache.handle_queue.len(), 1);
        }

        // Add second reader - should not trigger eviction (at limit)
        cache.create_reader(file2_str).unwrap();
        #[cfg(target_os = "linux")]
        {
            assert_eq!(cache.raw_readers.len(), 2);
            assert_eq!(cache.writers.len(), 0);
            assert_eq!(cache.handle_queue.len(), 2);
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(cache.readers.len(), 2);
            assert_eq!(cache.writers.len(), 0);
            assert_eq!(cache.handle_queue.len(), 2);
        }

        // Add third reader - should trigger eviction of first reader
        cache.create_reader(file3_str).unwrap();
        #[cfg(target_os = "linux")]
        {
            assert_eq!(cache.raw_readers.len(), 2);
            assert_eq!(cache.writers.len(), 0);
            assert_eq!(cache.handle_queue.len(), 2);

            // file1 should be evicted, file2 and file3 should remain
            assert!(!cache.raw_readers.contains_key(file1_str));
            assert!(cache.raw_readers.contains_key(file2_str));
            assert!(cache.raw_readers.contains_key(file3_str));
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(cache.readers.len(), 2);
            assert_eq!(cache.writers.len(), 0);
            assert_eq!(cache.handle_queue.len(), 2);

            // file1 should be evicted, file2 and file3 should remain
            assert!(!cache.readers.contains_key(file1_str));
            assert!(cache.readers.contains_key(file2_str));
            assert!(cache.readers.contains_key(file3_str));
        }
    }

    #[test]
    fn test_eviction_mixed_readers_writers() {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = FileCache::new(3); // Set max handles to 3

        let file1 = temp_dir.path().join("test1.txt");
        let file2 = temp_dir.path().join("test2.txt");
        let file3 = temp_dir.path().join("test3.txt");
        let file4 = temp_dir.path().join("test4.txt");

        let file1_str = file1.to_str().unwrap();
        let file2_str = file2.to_str().unwrap();
        let file3_str = file3.to_str().unwrap();
        let file4_str = file4.to_str().unwrap();

        // Add reader, writer, reader (fills capacity)
        fs::write(&file1, "test1").unwrap();
        cache.create_reader(file1_str).unwrap();
        cache.create_overwrite_writer(file2_str).unwrap();

        fs::write(&file3, "test3").unwrap();
        cache.create_reader(file3_str).unwrap();

        #[cfg(target_os = "linux")]
        assert_eq!(cache.raw_readers.len() + cache.writers.len(), 3);
        #[cfg(not(target_os = "linux"))]
        assert_eq!(cache.readers.len() + cache.writers.len(), 3);

        // Add fourth handle - should evict the first (oldest) handle
        cache.create_append_writer(file4_str).unwrap();

        #[cfg(target_os = "linux")]
        {
            assert_eq!(cache.raw_readers.len() + cache.writers.len(), 3);
            assert_eq!(cache.handle_queue.len(), 3);

            // file1 (first reader) should be evicted
            assert!(!cache.raw_readers.contains_key(file1_str));
            assert!(cache.writers.contains_key(file2_str));
            assert!(cache.raw_readers.contains_key(file3_str));
            assert!(cache.writers.contains_key(file4_str));
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(cache.readers.len() + cache.writers.len(), 3);
            assert_eq!(cache.handle_queue.len(), 3);

            // file1 (first reader) should be evicted
            assert!(!cache.readers.contains_key(file1_str));
            assert!(cache.writers.contains_key(file2_str));
            assert!(cache.readers.contains_key(file3_str));
            assert!(cache.writers.contains_key(file4_str));
        }
    }

    #[test]
    fn test_no_eviction_when_under_limit() {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = FileCache::new(5); // Set max handles to 5

        let file1 = temp_dir.path().join("test1.txt");
        let file2 = temp_dir.path().join("test2.txt");

        fs::write(&file1, "test1").unwrap();
        fs::write(&file2, "test2").unwrap();

        let file1_str = file1.to_str().unwrap();
        let file2_str = file2.to_str().unwrap();

        // Add handles under the limit
        cache.create_reader(file1_str).unwrap();
        cache.create_append_writer(file2_str).unwrap();

        #[cfg(target_os = "linux")]
        {
            assert_eq!(cache.raw_readers.len(), 1);
            assert_eq!(cache.writers.len(), 1);
            assert_eq!(cache.handle_queue.len(), 2);

            // Both should still be present
            assert!(cache.raw_readers.contains_key(file1_str));
            assert!(cache.writers.contains_key(file2_str));
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(cache.readers.len(), 1);
            assert_eq!(cache.writers.len(), 1);
            assert_eq!(cache.handle_queue.len(), 2);

            // Both should still be present
            assert!(cache.readers.contains_key(file1_str));
            assert!(cache.writers.contains_key(file2_str));
        }
    }
}
