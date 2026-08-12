use std::collections::HashSet;
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};
use crate::sbbf;

const EXACT_CLIENT_SET_MAX: usize = 32;
const CLIENT_SET_BLOOM_BITS_PER_KEY: usize = 10;
const SBBF_BLOCK_BYTES: usize = 32;
const CLIENT_SET_BLOOM_MAX_BYTES: usize = 8 * 1024;

/// Prepare a bloom for save to disk and dynamically size it based on the number of entries
pub fn sized_bloom_from_hashes<I>(count: usize, hashes: I, max_bytes: usize) -> Vec<u64>
where
    I: IntoIterator<Item = u64>,
{
    let bytes = (count * CLIENT_SET_BLOOM_BITS_PER_KEY)
        .div_ceil(8)
        .max(SBBF_BLOCK_BYTES)
        .next_multiple_of(SBBF_BLOCK_BYTES)
        .min(max_bytes);
    let mut words = vec![0u64; bytes / 8];
    for hash in hashes {
        sbbf::insert(&mut words, hash);
    }
    words
}

/// Is client X definitely absent from THIS aggregate in this segment
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub enum ClientSet {
    Unknown,
    Exact(Vec<u64>),
    Bloom(Vec<u64>),
}

impl ClientSet {
    /// convert the accumulated in-memory hashes of client IDs into a Bloom filter to save the disk.
    /// If there are not many clients, just save them directly instead of using bloom
    pub fn from_client_hashes(hashes: &HashSet<u64>) -> Self {
        if hashes.is_empty() {
            return Self::Unknown;
        }
        if hashes.len() <= EXACT_CLIENT_SET_MAX {
            let mut sorted: Vec<u64> = hashes.iter().copied().collect();
            sorted.sort_unstable();
            return Self::Exact(sorted);
        }
        Self::Bloom(sized_bloom_from_hashes(hashes.len(), hashes.iter().copied(), CLIENT_SET_BLOOM_MAX_BYTES))
    }

    /// `false` = definitely absent (safe to skip); `true` = maybe present (scan).
    pub fn may_contain_hash(&self, hash: u64) -> bool {
        match self {
            Self::Unknown => true,
            Self::Exact(sorted) => sorted.binary_search(&hash).is_ok(),
            // A malformed word count can't answer soundly — treat as maybe-present.
            Self::Bloom(words) => words.is_empty() || words.len() % 4 != 0 || sbbf::contains(words, hash),
        }
    }

    pub fn cardinality(&self) -> Option<usize> {
        match self {
            Self::Unknown | Self::Bloom(_) => None,
            Self::Exact(sorted) => Some(sorted.len()),
        }
    }

    /// Serialized size: u32 discriminant + (u64 len prefix + words) for the Vec variants.
    pub fn wire_size(&self) -> u64 {
        4 + match self {
            Self::Unknown => 0,
            Self::Exact(v) | Self::Bloom(v) => 8 + 8 * v.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_summary::tests::hashes;

    #[test]
    fn client_set_empty_is_unknown() {
        assert_eq!(ClientSet::from_client_hashes(&HashSet::new()), ClientSet::Unknown);
    }

    #[test]
    fn client_set_exact_up_to_threshold() {
        let h = hashes(EXACT_CLIENT_SET_MAX as u64);
        let set = ClientSet::from_client_hashes(&h);
        let ClientSet::Exact(sorted) = &set else { panic!("32 clients must stay Exact, got {set:?}") };
        assert_eq!(sorted.len(), 32);
        assert!(sorted.is_sorted());
        for hash in &h {
            assert!(set.may_contain_hash(*hash));
        }
        assert!(!set.may_contain_hash(0xDEAD_BEEF), "exact set answers definite absence");
    }

    #[test]
    fn client_set_bloom_above_threshold() {
        let h = hashes(EXACT_CLIENT_SET_MAX as u64 + 1);
        let set = ClientSet::from_client_hashes(&h);
        let ClientSet::Bloom(words) = &set else { panic!("33 clients must become Bloom, got {set:?}") };
        // 33 keys × 10 bits = 330 bits = 42 bytes → next multiple of 32 = 64 bytes = 8 words.
        assert_eq!(words.len(), 8);
        for hash in &h {
            assert!(set.may_contain_hash(*hash), "no false absent allowed");
        }
    }

    #[test]
    fn client_set_bloom_capped_at_max_bytes() {
        let h = hashes(20_000);
        let set = ClientSet::from_client_hashes(&h);
        let ClientSet::Bloom(words) = &set else { panic!() };
        assert_eq!(words.len() * 8, CLIENT_SET_BLOOM_MAX_BYTES, "20k clients must hit the 8 KiB cap");
        for hash in &h {
            assert!(set.may_contain_hash(*hash), "no false absent allowed even at cap");
        }
    }

    #[test]
    fn client_set_unknown_and_malformed_bloom_are_maybe_present() {
        assert!(ClientSet::Unknown.may_contain_hash(42));
        assert!(ClientSet::Bloom(vec![]).may_contain_hash(42));
        assert!(ClientSet::Bloom(vec![0; 3]).may_contain_hash(42), "non-block-multiple word count must not claim absence");
    }

    /// The persisted bloom byte size is a FORMAT commitment: for n keys,
    /// max(32, ceil(n×10/8) rounded up to 32) capped. Pinned so a formula
    /// drift shows up as a test failure, not silently fatter/leaner sidecars.
    #[test]
    fn sized_bloom_bytes_pinned_to_formula() {
        for (n, expected_bytes, cap) in [
            (0usize, 32usize, 8 * 1024),
            (1, 32, 8 * 1024),
            (25, 32, 8 * 1024),   // 250 bits = 32 B exactly
            (26, 64, 8 * 1024),   // 260 bits = 33 B → next block
            (300, 384, 1024 * 1024), // the oracle test's cardinality class
            (200_000, 250_016, 256 * 1024), // design capacity still fits under the old fixed size
            (300_000, 256 * 1024, 256 * 1024), // beyond capacity pins the cap
            (1_000_000, 8 * 1024, 8 * 1024),   // saturating small cap
        ] {
            let words = sized_bloom_from_hashes(n, hashes(n as u64).into_iter(), cap);
            assert_eq!(words.len() * 8, expected_bytes, "n={n} cap={cap}");
        }
    }

    #[test]
    fn sized_bloom_never_answers_false_absent_even_saturated() {
        let h = hashes(20_000);
        let words = sized_bloom_from_hashes(h.len(), h.iter().copied(), 8 * 1024);
        assert_eq!(words.len() * 8, 8 * 1024);
        for hash in &h {
            assert!(sbbf::contains(&words, *hash), "no false absent allowed at cap saturation");
        }
    }
}