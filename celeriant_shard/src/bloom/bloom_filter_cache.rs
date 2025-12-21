
use celeriant_wal::constants::{BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED};
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use fastbloom::BloomFilter;
use std::cell::RefCell;
use std::collections::HashSet;

/// Reusable bloom filter cache to avoid repeated allocations.
///
/// The bloom filter and dedup set are cleared and reused for each batch.
/// This struct is not thread-safe - designed for single-threaded shard access.
pub struct BloomFilterCache {
    bloom_filter: RefCell<BloomFilter>,
    dedup_set: RefCell<HashSet<u64>>,
}

impl Default for BloomFilterCache {
    fn default() -> Self {
        Self::new()
    }
}

impl BloomFilterCache {
    /// Create a new bloom filter cache with standard WAL parameters.
    #[must_use]
    pub fn new() -> Self {
        let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);

        Self {
            bloom_filter: RefCell::new(bloom_filter),
            dedup_set: RefCell::new(HashSet::new()),
        }
    }

    /// Create bloom filter bytes for the given events.
    ///
    /// Extracts unique event types and populates a bloom filter.
    /// Returns the bloom filter as a fixed-size byte array suitable
    /// for storage in `EventTypesKind::Bloom`.
    #[must_use]
    pub fn create_bloom_bytes(&self, events: &[DatablockAggregateEvent]) -> [u64; BLOOM_BYTES / 8] {
        let mut bloom_filter = self.bloom_filter.borrow_mut();
        let mut dedup_set = self.dedup_set.borrow_mut();

        bloom_filter.clear();
        dedup_set.clear();

        // Collect unique event types
        for event in events {
            dedup_set.insert(event.event_type_major);
        }

        // Populate bloom filter
        for &event_type in dedup_set.iter() {
            bloom_filter.insert(&event_type.to_le_bytes());
        }

        bloom_filter
            .as_slice()
            .try_into()
            .expect("Bloom filter size mismatch")
    }
}