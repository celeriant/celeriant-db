use std::collections::{HashMap, hash_map::Entry};

pub struct LocalIndexCache {
    cache: HashMap<String, HashMap<u128, u64>>,
    max_files: usize,
}

impl LocalIndexCache {
    pub fn new(max_files: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_files,
        }
    }

    pub fn get(&mut self, file_path: &str, client_id: u128) -> Option<u64> {
        self.cache.get(file_path).and_then(|client_map| client_map.get(&client_id)).copied()
    }

    pub fn update(&mut self, file_path: &str, client_id: u128, last_write_index: u64) {
        if self.cache.len() >= self.max_files {
            self.cache.clear();
        }

        match self.cache.entry(file_path.to_string()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(client_id, last_write_index);
            }
            Entry::Vacant(entry) => {
                let mut client_map = HashMap::new();
                client_map.insert(client_id, last_write_index);
                entry.insert(client_map);
            }
        }
    }

    pub fn remove(&mut self, file_path: &str) {
        self.cache.remove(file_path);
    }
}
