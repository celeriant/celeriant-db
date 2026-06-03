//! Split-Block Bloom Filter (SBBF) — owned, scalar, dependency-free.
//!
//! This is the persisted bloom format for Celeriant. It is owned in-tree rather
//! than pulled from a crate because the bit layout is written to disk and trusted
//! on read, so it must NEVER change across Rust versions, crate versions, or CPU
//! architecture. A ~30-line scalar implementation of the Apache Parquet
//! split-block spec gives that guarantee outright (verified byte-identical on
//! x86 and aarch64 real hardware).
//!
//! Storage-agnostic by design: these are free functions over `[u64]`, operating
//! directly on the persisted representation. The caller owns the backing memory,
//! so a small, frequently-built filter (the per-metablock event-type bloom) uses
//! a fixed-size stack array with zero heap churn, while the large per-segment
//! aggregate bloom uses a long-lived `Vec` — both share this exact bit math.
//!
//! Layout: 32-byte blocks = 4×u64 = 8×u32 lanes. A key is one u64 hash (compute
//! it with xxh3, also a frozen spec). High 32 bits pick the block; low 32 bits
//! set one bit per lane via the fixed salt table. `words.len()` must be a
//! multiple of 4 (i.e. byte length a multiple of 32).

/// The 8 fixed salts from the Parquet SBBF spec. Changing these changes the
/// on-disk format — do not touch.
pub const SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424c, 0x9efc4947, 0x5c6bfb31,
];

const U64_PER_BLOCK: usize = 4; // 32 bytes / 8

#[inline]
fn block_index(hash: u64, num_blocks: usize) -> usize {
    (((hash >> 32) * num_blocks as u64) >> 32) as usize
}

/// Set one bit per u32 lane within the chosen block, packed into the block's
/// 4 u64 words (lane `w` lives in the `w&1` half of u64 `w/2`).
#[inline]
pub fn insert(words: &mut [u64], hash: u64) {
    debug_assert!(words.len() % U64_PER_BLOCK == 0);
    let base = block_index(hash, words.len() / U64_PER_BLOCK) * U64_PER_BLOCK;
    let lo = hash as u32;
    for lane in 0..8 {
        let bit = 1u64 << ((lo.wrapping_mul(SALT[lane])) >> 27);
        words[base + lane / 2] |= bit << (32 * (lane & 1));
    }
}

#[inline]
pub fn contains(words: &[u64], hash: u64) -> bool {
    debug_assert!(words.len() % U64_PER_BLOCK == 0);
    let base = block_index(hash, words.len() / U64_PER_BLOCK) * U64_PER_BLOCK;
    let lo = hash as u32;
    (0..8).all(|lane| {
        let bit = 1u64 << ((lo.wrapping_mul(SALT[lane])) >> 27);
        words[base + lane / 2] & (bit << (32 * (lane & 1))) != 0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(i: u64) -> u64 {
        xxhash_rust::xxh3::xxh3_64(&i.to_le_bytes())
    }

    #[test]
    fn no_false_negatives_stack_backed() {
        // Fixed-size stack array — the no-heap path used by the event-type bloom.
        let mut words = [0u64; (64 * 1024) / 8];
        for i in 0..5000u64 {
            insert(&mut words, h(i));
        }
        for i in 0..5000u64 {
            assert!(contains(&words, h(i)), "false negative for {i}");
        }
    }

    #[test]
    fn false_positive_rate_is_reasonable() {
        let mut words = vec![0u64; (64 * 1024) / 8];
        for i in 0..5000u64 {
            insert(&mut words, h(i));
        }
        let fp = (0..50_000u64).filter(|&i| contains(&words, h(1_000_000 + i))).count();
        assert!(fp < 1000, "fp rate too high: {fp}/50000");
    }

    /// FROZEN persisted-format pin. The serialized bits for a fixed key set must
    /// never change (Rust/crate/arch). Verified byte-identical on x86 and aarch64
    /// real hardware; a trip here means the on-disk bloom format moved.
    #[test]
    fn serialized_bits_are_pinned() {
        let mut words = [0u64; 4096 / 8];
        for i in 0..1000u64 {
            insert(&mut words, h(i));
        }
        let mut fp = 0xcbf29ce484222325u64;
        for w in &words {
            for byte in w.to_le_bytes() {
                fp ^= byte as u64;
                fp = fp.wrapping_mul(0x100000001b3);
            }
        }
        assert_eq!(fp, 5083834396628355009, "SBBF on-disk format changed");
    }
}
