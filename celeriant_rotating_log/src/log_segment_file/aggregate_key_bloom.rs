use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::{AGGREGATE_BLOOM_BYTES, AGGREGATE_BLOOM_HASH_COUNT, AGGREGATE_BLOOM_HASH_SEED};
use fastbloom::BloomFilter;

/// Bloom filter cache for aggregate keys in log segment headers.
///
/// Uses the pre-computed hash from AggregateKey for efficient insertion/lookup.
/// This struct is not thread-safe - designed for single-threaded shard access.
#[derive(Clone)]
pub struct AggregateKeyBloom {
    bloom_filter: BloomFilter,
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
        
        Self { bloom_filter }
    }

    /// Create from existing bloom bytes (e.g., loaded from disk).
    #[must_use]
    pub fn from_bytes(bytes: &[u64]) -> Self {
        let bloom_filter = BloomFilter::from_vec(bytes.to_vec())
            .seed(&AGGREGATE_BLOOM_HASH_SEED)
            .hashes(AGGREGATE_BLOOM_HASH_COUNT);

        Self { bloom_filter }
    }

    /// Clear the bloom filter for reuse.
    pub fn clear(&mut self) {
        self.bloom_filter.clear();
    }

    /// Insert an aggregate key into the bloom filter.
    pub fn insert(&mut self, aggregate_key: &AggregateKey) {
        // Use the pre-computed hash from AggregateKey
        self.bloom_filter.insert(&aggregate_key.hash_bytes());
    }

    /// Insert multiple aggregate keys.
    pub fn insert_all(&mut self, aggregate_keys: &[AggregateKey]) {
        for key in aggregate_keys {
            self.bloom_filter.insert(&key.hash_bytes());
        }
    }

    /// Check if an aggregate key might be in the set.
    /// Returns `false` if definitely not in set, `true` if possibly in set.
    #[must_use]
    pub fn may_contain(&self, aggregate_key: &AggregateKey) -> bool {
        self.bloom_filter.contains(&aggregate_key.hash_bytes())
    }

    /// Insert a hash directly into the bloom filter.
    /// Used for non-AggregateKey types (e.g., SchemaKey) that have pre-computed hashes.
    pub fn insert_hash(&mut self, hash_bytes: &[u8; 8]) {
        self.bloom_filter.insert(hash_bytes);
    }

    /// Check if a hash might be in the set.
    /// Returns `false` if definitely not in set, `true` if possibly in set.
    #[must_use]
    pub fn may_contain_hash(&self, hash_bytes: &[u8; 8]) -> bool {
        self.bloom_filter.contains(hash_bytes)
    }

    /// Export the bloom filter as bytes for storage in header.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u64> {
        self.bloom_filter.iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_check() {
        let mut bloom = AggregateKeyBloom::new();
        let key1 = AggregateKey::new(1, 2, 3);
        let key2 = AggregateKey::new(4, 5, 6);
        let _key3 = AggregateKey::new(7, 8, 9);

        bloom.insert(&key1);
        bloom.insert(&key2);

        assert!(bloom.may_contain(&key1));
        assert!(bloom.may_contain(&key2));
        // assert!(!bloom.may_contain(&key3));
        // key3 was not inserted - might still return true (false positive) but unlikely
    }

    #[test]
    fn test_from_bytes_roundtrip() {
        let mut bloom = AggregateKeyBloom::new();
        let key = AggregateKey::new(100, 200, 300);
        bloom.insert(&key);

        let bytes = bloom.to_bytes();
        let restored = AggregateKeyBloom::from_bytes(&bytes);

        assert!(restored.may_contain(&key));
    }

    #[test]
    fn test_clear() {
        let mut bloom = AggregateKeyBloom::new();
        let key = AggregateKey::new(1, 2, 3);
        bloom.insert(&key);
        assert!(bloom.may_contain(&key));

        bloom.clear();
        // After clear, should not contain (unless false positive)
        let bytes = bloom.to_bytes();
        assert!(bytes.iter().all(|&b| b == 0));
    }
}
