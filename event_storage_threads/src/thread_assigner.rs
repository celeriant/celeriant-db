use ahash::AHasher;
use std::hash::{Hash, Hasher};

pub fn hash_string_to_index(id: &str, num_threads: usize) -> usize {
    let mut hasher = AHasher::default();
    id.hash(&mut hasher);
    (hasher.finish() as usize) % num_threads
}
