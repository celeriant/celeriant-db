use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::{AGGREGATE_BLOOM_BYTES, AGGREGATE_BLOOM_HASH_COUNT, AGGREGATE_BLOOM_HASH_SEED};
use fastbloom::BloomFilter;
use std::cell::RefCell;

/// Bloom filter cache for aggregate keys in log segment headers.
///
/// Uses the pre-computed hash from AggregateKey for efficient insertion/lookup.
/// This struct is not thread-safe - designed for single-threaded shard access.
pub struct AggregateKeyBloom {
    bloom_filter: RefCell<BloomFilter>,
}

impl Default for AggregateKeyBloom {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateKeyBloom {
    /// Create a new aggregate key bloom filter with standard parameters.
    #[must_use]
    pub fn new() -> Self {
        let bloom_filter = BloomFilter::with_num_bits(AGGREGATE_BLOOM_BYTES * 8)
            .seed(&AGGREGATE_BLOOM_HASH_SEED)
            .hashes(AGGREGATE_BLOOM_HASH_COUNT);

        Self {
            bloom_filter: RefCell::new(bloom_filter),
        }
    }

    /// Create from existing bloom bytes (e.g., loaded from disk).
    #[must_use]
    pub fn from_bytes(bytes: &[u64; AGGREGATE_BLOOM_BYTES / 8]) -> Self {
        let bloom_filter = BloomFilter::from_vec(bytes.to_vec())
            .seed(&AGGREGATE_BLOOM_HASH_SEED)
            .hashes(AGGREGATE_BLOOM_HASH_COUNT);

        Self {
            bloom_filter: RefCell::new(bloom_filter),
        }
    }

    /// Clear the bloom filter for reuse.
    pub fn clear(&self) {
        self.bloom_filter.borrow_mut().clear();
    }

    /// Insert an aggregate key into the bloom filter.
    pub fn insert(&self, aggregate_key: &AggregateKey) {
        // Use the pre-computed hash from AggregateKey
        self.bloom_filter
            .borrow_mut()
            .insert(&aggregate_key.hash_bytes());
    }

    /// Insert multiple aggregate keys.
    pub fn insert_all(&self, aggregate_keys: &[AggregateKey]) {
        let mut bloom = self.bloom_filter.borrow_mut();
        for key in aggregate_keys {
            bloom.insert(&key.hash_bytes());
        }
    }

    /// Check if an aggregate key might be in the set.
    /// Returns `false` if definitely not in set, `true` if possibly in set.
    #[must_use]
    pub fn may_contain(&self, aggregate_key: &AggregateKey) -> bool {
        self.bloom_filter
            .borrow()
            .contains(&aggregate_key.hash_bytes())
    }

    /// Export the bloom filter as bytes for storage in header.
    #[must_use]
    pub fn to_bytes(&self) -> [u64; AGGREGATE_BLOOM_BYTES / 8] {
        self.bloom_filter
            .borrow()
            .as_slice()
            .try_into()
            .expect("Bloom filter size mismatch")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_check() {
        let bloom = AggregateKeyBloom::new();
        let key1 = AggregateKey::new(1, 2, 3);
        let key2 = AggregateKey::new(4, 5, 6);
        let key3 = AggregateKey::new(7, 8, 9);

        bloom.insert(&key1);
        bloom.insert(&key2);

        assert!(bloom.may_contain(&key1));
        assert!(bloom.may_contain(&key2));
        // key3 was not inserted - might still return true (false positive) but unlikely
    }

    #[test]
    fn test_from_bytes_roundtrip() {
        let bloom = AggregateKeyBloom::new();
        let key = AggregateKey::new(100, 200, 300);
        bloom.insert(&key);

        let bytes = bloom.to_bytes();
        let restored = AggregateKeyBloom::from_bytes(&bytes);

        assert!(restored.may_contain(&key));
    }

    #[test]
    fn test_clear() {
        let bloom = AggregateKeyBloom::new();
        let key = AggregateKey::new(1, 2, 3);
        bloom.insert(&key);
        assert!(bloom.may_contain(&key));

        bloom.clear();
        // After clear, should not contain (unless false positive)
        let bytes = bloom.to_bytes();
        assert!(bytes.iter().all(|&b| b == 0));
    }
}