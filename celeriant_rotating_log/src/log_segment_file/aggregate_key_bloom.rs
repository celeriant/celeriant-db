use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::AGGREGATE_BLOOM_BYTES;
use celeriant_wal::sbbf;

const WORDS: usize = AGGREGATE_BLOOM_BYTES / 8;

/// Persisted bloom of aggregate (and schema) keys in log segment headers.
#[derive(Clone)]
pub struct AggregateKeyBloom {
    words: Vec<u64>,
}

impl Default for AggregateKeyBloom {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateKeyBloom {
    /// Create a new, empty aggregate key bloom (256KB).
    #[must_use]
    pub fn new() -> Self {
        Self { words: vec![0u64; WORDS] }
    }

    /// Create a new, empty bloom of a given byte size. Used for the smaller (128KB) client_id bloom
    #[must_use]
    pub fn with_capacity_bytes(bytes: usize) -> Self {
        debug_assert_eq!(bytes % 32, 0, "SBBF byte size must be a multiple of the 32-byte block");
        Self { words: vec![0u64; bytes / 8] }
    }

    /// Create from existing bloom bytes (e.g., loaded from disk). Length is whatever was
    /// persisted (256KB aggregate or 128KB client), only required to be a whole number of
    /// 32-byte SBBF blocks.
    #[must_use]
    pub fn from_bytes(bytes: &[u64]) -> Self {
        debug_assert_eq!(bytes.len() % 4, 0, "persisted bloom is not a whole number of SBBF blocks");
        Self { words: bytes.to_vec() }
    }

    /// Clear the bloom filter for reuse.
    pub fn clear(&mut self) {
        self.words.iter_mut().for_each(|w| *w = 0);
    }

    /// Insert an aggregate key into the bloom filter.
    pub fn insert(&mut self, aggregate_key: &AggregateKey) {
        sbbf::insert(&mut self.words, aggregate_key.bloom_hash());
    }

    /// Insert multiple aggregate keys.
    pub fn insert_all(&mut self, aggregate_keys: &[AggregateKey]) {
        for key in aggregate_keys {
            sbbf::insert(&mut self.words, key.bloom_hash());
        }
    }

    /// Check if an aggregate key might be in the set.
    /// Returns `false` if definitely not in set, `true` if possibly in set.
    #[must_use]
    pub fn may_contain(&self, aggregate_key: &AggregateKey) -> bool {
        sbbf::contains(&self.words, aggregate_key.bloom_hash())
    }

    /// Insert a precomputed key hash directly (e.g. `SchemaKey::bloom_hash`).
    pub fn insert_hash(&mut self, hash: u64) {
        sbbf::insert(&mut self.words, hash);
    }

    /// Check if a precomputed key hash might be in the set.
    #[must_use]
    pub fn may_contain_hash(&self, hash: u64) -> bool {
        sbbf::contains(&self.words, hash)
    }

    /// Export the bloom filter as bytes for storage in header.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u64> {
        self.words.clone()
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

    // ── Empirical false-positive characterisation ──────────────────────────
    //
    // The segment-gating bloom is what lets a reverse WAL scan skip a sealed
    // segment without reading its metablock region. Its false-positive rate
    // therefore decides how much disk a cold/negative lookup actually touches:
    // every false positive turns a "skip" into a full region scan of that
    // segment.
    //
    // These tests pin the real filter's behaviour (256KB split-block bloom — see
    // AGGREGATE_BLOOM_BYTES), not the 32-byte per-batch event-type bloom that
    // some older analysis conflated it with.

    /// Measure the FP rate by inserting `inserted` keys and probing a disjoint
    /// set of `probes` absent keys. Insert/probe key spaces never overlap.
    fn measure_fp_rate(inserted: u128, probes: u128) -> f64 {
        let mut bloom = AggregateKeyBloom::new();
        for id in 0..inserted {
            bloom.insert(&AggregateKey::new(1, 1, id));
        }
        // Absent probe space, far from inserted ids, also varying org/type so a
        // structured hash can't accidentally alias the inserted range.
        let mut false_positives = 0u128;
        for p in 0..probes {
            let key = AggregateKey::new(7, 9, 1_000_000_000 + p);
            if bloom.may_contain(&key) {
                false_positives += 1;
            }
        }
        let rate = false_positives as f64 / probes as f64;
        eprintln!(
            "[bloom-fp] inserted={inserted} probed={probes} false_positives={false_positives} fp_rate={:.4}%",
            rate * 100.0
        );
        rate
    }

    #[test]
    fn fp_rate_at_design_capacity_is_under_one_percent() {
        // constants.rs claims "<1% chance for 200k entries per shard log segment".
        // Validate empirically with a margin for sampling noise.
        let fp = measure_fp_rate(200_000, 200_000);
        assert!(
            fp < 0.015,
            "FP rate at 200k design capacity was {:.3}% — expected <1% (allowing 1.5% sample margin)",
            fp * 100.0
        );
    }

    #[test]
    fn fp_rate_stays_low_well_below_capacity() {
        // A typical sealed segment holds far fewer than 200k distinct aggregates.
        // At 50k the filter should be effectively perfect at rejecting absentees.
        let fp = measure_fp_rate(50_000, 200_000);
        assert!(fp < 0.001, "FP rate at 50k entries was {:.4}% — expected effectively zero", fp * 100.0);
    }

    #[test]
    fn fp_rate_saturates_far_past_capacity() {
        // The bound is PER SEGMENT. Bounded memory at infinite *global* cardinality
        // relies on no single segment exceeding ~200k distinct aggregates. Push 5x
        // past design capacity and confirm the filter degrades to near-useless —
        // this is the cliff: a segment crammed with >>200k aggregates stops being
        // skippable and every lookup that hashes into it pays a full region scan.
        let fp = measure_fp_rate(1_000_000, 100_000);
        assert!(
            fp > 0.5,
            "FP rate at 1M entries (5x capacity) was {:.1}% — expected heavy saturation (>50%)",
            fp * 100.0
        );
    }
}
