use std::{collections::{hash_map::Entry, HashMap}};

pub struct LastSiCache {
    cache: HashMap<String, u64>,
    max_size: usize,
}

impl LastSiCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_size
        }
    }

    pub fn get(&mut self, file_path: &str) -> Option<u64>
    {
        match self.cache.entry(file_path.to_string()) {
            Entry::Occupied(entry) => Some(*entry.get()),
            Entry::Vacant(_) => None
        }
    }

    pub fn update(&mut self, file_path: &str, last_si: u64) {
        if self.cache.len() >= self.max_size {
            self.cache.clear();
        }
        self.cache.insert(file_path.to_string(), last_si);
    }

    pub fn remove(&mut self, file_path: &str) {
        self.cache.remove(file_path);
    }
}