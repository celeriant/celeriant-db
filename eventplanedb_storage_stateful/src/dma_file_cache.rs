use std::{cell::RefCell, collections::{HashMap, VecDeque}, io, rc::Rc};
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder};

struct FileHandle {
    file_path: String,
}

pub struct DmaFileCache {
    writers: HashMap<String, Rc<RefCell<DmaStreamWriter>>>,
    handle_queue: VecDeque<FileHandle>,
    max_handles: usize,
}

impl DmaFileCache {
    pub(crate) fn new(max_file_handles: usize) -> Self {
        Self {
            writers: HashMap::new(),
            handle_queue: VecDeque::new(),
            max_handles: max_file_handles,
        }
    }

    fn evict(&mut self) {
        let total_handles = self.writers.len();

        while total_handles >= self.max_handles {
            if let Some(handle) = self.handle_queue.pop_front() {
                if self.writers.remove(&handle.file_path).is_some() {
                    break;
                }
            } else {
                // Queue is empty but we're still over limit
                break;
            }
        }
    }

    fn track_handle(&mut self, file_path: String) {
        self.handle_queue.push_back(FileHandle { file_path });
    }

    pub async fn create_append_writer(&mut self, file_path: &str) -> io::Result<Rc<RefCell<DmaStreamWriter>>> {
        self.evict();

        // Check if exists and clone the Rc in a separate scope
        if let Some(existing) = self.writers.get(file_path).cloned() {
            return Ok(existing);
        }

        // Create DMA file and writer
        let file = DmaFile::create(file_path).await?;
        let writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(4096)
            .with_write_behind(2)
            .build();

        let rc_writer = Rc::new(RefCell::new(writer));
        self.writers.insert(file_path.to_string(), Rc::clone(&rc_writer));
        self.track_handle(file_path.to_string());

        Ok(rc_writer)
    }

    pub fn remove(&mut self, file_path: &str) {
        self.writers.remove(file_path);
        self.handle_queue.retain(|handle| handle.file_path != file_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glommio::{LocalExecutorBuilder, Placement};
    use std::fs;
    use tempfile::TempDir;
    use futures_lite::io::AsyncWriteExt; // for write_all

    #[test]
    fn test_dma_file_cache() {
        let builder = LocalExecutorBuilder::new(Placement::Fixed(0));
        let handle = builder.spawn(|| async move {
            let temp_dir = TempDir::new().unwrap();
            let mut cache = DmaFileCache::new(2);

            // Create test files
            let file1 = temp_dir.path().join("test1.txt");
            let file2 = temp_dir.path().join("test2.txt");
            let file3 = temp_dir.path().join("test3.txt");

            let file1_str = file1.to_str().unwrap();
            let file2_str = file2.to_str().unwrap();
            let file3_str = file3.to_str().unwrap();

            // Add first writer
            let writer1 = cache.create_append_writer(file1_str).await.unwrap();
            writer1.borrow_mut().write_all(b"test1").await.unwrap();
            assert_eq!(cache.writers.len(), 1);

            // Add second writer
            let writer2 = cache.create_append_writer(file2_str).await.unwrap();
            writer2.borrow_mut().write_all(b"test2").await.unwrap();
            assert_eq!(cache.writers.len(), 2);

            // Add third writer - should evict first
            let writer3 = cache.create_append_writer(file3_str).await.unwrap();
            writer3.borrow_mut().write_all(b"test3").await.unwrap();
            assert_eq!(cache.writers.len(), 2);

            // First writer should be evicted
            assert!(!cache.writers.contains_key(file1_str));
            assert!(cache.writers.contains_key(file2_str));
            assert!(cache.writers.contains_key(file3_str));

            // Close all writers
            for (_, writer) in cache.writers.iter() {
                writer.borrow_mut().close().await.unwrap();
            }

            // Verify file contents
            assert_eq!(fs::read_to_string(file2).unwrap(), "test2");
            assert_eq!(fs::read_to_string(file3).unwrap(), "test3");
        }).unwrap();

        handle.join().unwrap();
    }
}