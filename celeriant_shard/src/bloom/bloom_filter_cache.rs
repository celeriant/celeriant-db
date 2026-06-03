use celeriant_wal::constants::BLOOM_BYTES;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::sbbf;
use std::cell::RefCell;
use std::collections::HashSet;

#[inline]
pub fn event_type_hash(event_type: u64) -> u64 {
    xxhash_rust::xxh3::xxh3_64(&event_type.to_le_bytes())
}

/// Reusable cache for building the per-batch event-type bloom.
pub struct BloomFilterCache {
    dedup_set: RefCell<HashSet<u64>>,
}

impl Default for BloomFilterCache {
    fn default() -> Self {
        Self::new()
    }
}

impl BloomFilterCache {
    #[must_use]
    pub fn new() -> Self {
        Self { dedup_set: RefCell::new(HashSet::new()) }
    }

    /// Build the event-type bloom bytes for `events`.
    ///
    /// Extracts unique event types into the owned split-block bloom and returns
    /// it as a fixed-size array for storage in `EventTypesKind::Bloom`
    #[must_use]
    pub fn create_bloom_bytes(&self, events: &[DatablockAggregateEvent]) -> [u64; BLOOM_BYTES / 8] {
        let mut dedup_set = self.dedup_set.borrow_mut();
        dedup_set.clear();
        for event in events {
            dedup_set.insert(event.event_type_major);
        }

        let mut words = [0u64; BLOOM_BYTES / 8];
        for &event_type in dedup_set.iter() {
            sbbf::insert(&mut words, event_type_hash(event_type));
        }
        words
    }
}
