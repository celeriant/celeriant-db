use std::collections::HashSet;

use crate::{sbbf, segment_summary::client_set::sized_bloom_from_hashes};

pub(super) const SCHEMA_BLOOM_MAX_BYTES: usize = 8 * 1024;
pub(super) const SCHEMA_ACCUMULATOR_MAX_HASHES: usize = 8 * 1024;

/// This is located here because it has bound invariants with the second summary.
/// only used in the shard_mem_cache.
#[derive(Debug, Default)]
pub struct SchemaHashAccumulator {
    hashes: HashSet<u64>,
    overflow_bloom: Option<Vec<u64>>,
}

impl SchemaHashAccumulator {
    pub fn insert(&mut self, hash: u64) {
        if let Some(words) = &mut self.overflow_bloom {
            sbbf::insert(words, hash);
            return;
        }
        self.hashes.insert(hash);
        if self.hashes.len() > SCHEMA_ACCUMULATOR_MAX_HASHES {
            let mut words = vec![0u64; SCHEMA_BLOOM_MAX_BYTES / 8];
            for h in &self.hashes {
                sbbf::insert(&mut words, *h);
            }
            self.hashes = HashSet::new();
            self.overflow_bloom = Some(words);
        }
    }

    pub fn may_contain(&self, hash: u64) -> bool {
        match &self.overflow_bloom {
            Some(words) => sbbf::contains(words, hash),
            None => self.hashes.contains(&hash),
        }
    }

    pub fn merge(&mut self, other: SchemaHashAccumulator) {
        match other.overflow_bloom {
            Some(mut words) => match &mut self.overflow_bloom {
                Some(mine) => {
                    debug_assert_eq!(mine.len(), words.len(), "overflow blooms are always the fixed cap size");
                    for (m, w) in mine.iter_mut().zip(words) {
                        *m |= w;
                    }
                }
                None => {
                    for hash in &self.hashes {
                        sbbf::insert(&mut words, *hash);
                    }
                    self.hashes = HashSet::new();
                    self.overflow_bloom = Some(words);
                }
            },
            None => {
                for hash in other.hashes {
                    self.insert(hash);
                }
            }
        }
    }

    /// only set complete to true if have seen every single entry in the segment.
    /// returns either a clone of the overflow bloom or a dynamically sized bloom based on the hash entry count
    pub fn to_schema_bloom(&self, complete: bool) -> Option<Vec<u64>> {
        if !complete {
            return None;
        }
        if let Some(words) = &self.overflow_bloom {
            return Some(words.clone());
        }
        Some(sized_bloom_from_hashes(self.hashes.len(), self.hashes.iter().copied(), SCHEMA_BLOOM_MAX_BYTES))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_summary::tests::hashes;

    #[test]
    fn schema_accumulator_incomplete_persists_none() {
        let mut acc = SchemaHashAccumulator::default();
        acc.insert(7);
        assert_eq!(acc.to_schema_bloom(false), None, "a subset may never claim absence");
        assert!(acc.to_schema_bloom(true).is_some());
    }

    #[test]
    fn schema_accumulator_merge_unions_across_exact_and_overflowed_forms() {
        // exact + exact
        let mut a = SchemaHashAccumulator::default();
        a.insert(1);
        let mut b = SchemaHashAccumulator::default();
        b.insert(2);
        a.merge(b);
        assert!(a.may_contain(1) && a.may_contain(2));
        assert!(!a.may_contain(3));

        // exact + overflowed: every hash from both survives
        let mut big = SchemaHashAccumulator::default();
        let h = hashes(SCHEMA_ACCUMULATOR_MAX_HASHES as u64 + 5);
        for hash in &h {
            big.insert(*hash);
        }
        let mut small = SchemaHashAccumulator::default();
        small.insert(0xAB);
        small.merge(big);
        assert!(small.may_contain(0xAB), "exact side must be replayed into the adopted bloom");
        for hash in &h {
            assert!(small.may_contain(*hash), "no false absent across a merge");
        }
    }
}