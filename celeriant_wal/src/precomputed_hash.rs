use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::OnceLock;

/// `BuildHasher` for keys that already carry a strong precomputed hash.
///
/// `AggregateKey`, `AggregateClientKey`, `SchemaKey` and `AggregateTypeKey` each compute an
/// xxh3-64 once at construction and store it, and each one's `Hash` impl writes exactly that
/// single `u64`. Putting them in a map with std's default `RandomState` therefore runs
/// SipHash-1-3 over eight bytes of an already well-distributed hash — the work is real and the
/// distribution it buys has already been paid for.
///
/// This applies one splitmix64 finalising round instead: four instructions against SipHash's
/// forty-odd.
///
/// **The seed is not decoration.** Aggregate ids are client-supplied and the precomputed xxh3 is
/// unkeyed, so feeding it to the map unmixed would let a caller pick keys that all land in one
/// bucket. `RandomState` is per-process random, so an attacker cannot predict which precomputed
/// hashes collide after mixing. Do not "simplify" this to an identity hasher.
#[derive(Clone, Copy, Debug)]
pub struct PrecomputedBuildHasher {
    seed: u64,
}

/// `HashMap` keyed by a type that carries its own precomputed hash.
pub type PrecomputedMap<K, V> = std::collections::HashMap<K, V, PrecomputedBuildHasher>;

/// `HashSet` of a type that carries its own precomputed hash.
pub type PrecomputedSet<K> = std::collections::HashSet<K, PrecomputedBuildHasher>;

impl Default for PrecomputedBuildHasher {
    fn default() -> Self {
        Self { seed: process_seed() }
    }
}

impl BuildHasher for PrecomputedBuildHasher {
    type Hasher = PrecomputedHasher;

    #[inline]
    fn build_hasher(&self) -> PrecomputedHasher {
        PrecomputedHasher { state: self.seed }
    }
}

pub struct PrecomputedHasher {
    state: u64,
}

impl PrecomputedHasher {
    #[inline]
    fn absorb(&mut self, value: u64) {
        self.state = splitmix64(self.state.rotate_left(23) ^ value);
    }
}

impl Hasher for PrecomputedHasher {
    /// Fallback for anything that is not one of the specialised widths below. Correct but not
    /// fast — the point of this hasher is that the keys it is used with never take this path.
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.absorb(u64::from_ne_bytes(chunk.try_into().unwrap()));
        }
        let mut tail = 0u64;
        for &b in chunks.remainder() {
            tail = (tail << 8) | u64::from(b);
        }
        self.absorb(tail);
    }

    #[inline]
    fn write_u8(&mut self, n: u8) {
        self.absorb(u64::from(n));
    }
    #[inline]
    fn write_u16(&mut self, n: u16) {
        self.absorb(u64::from(n));
    }
    #[inline]
    fn write_u32(&mut self, n: u32) {
        self.absorb(u64::from(n));
    }
    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.absorb(n);
    }
    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.absorb(n as u64);
    }
    #[inline]
    fn write_u128(&mut self, n: u128) {
        self.absorb(n as u64);
        self.absorb((n >> 64) as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.state
    }
}

/// splitmix64's finalising mix. Full avalanche on 64 bits, three multiplies and three shifts.
#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn process_seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| RandomState::new().hash_one(0xC0FF_EEu64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate_key::AggregateKey;
    use std::collections::HashMap;

    fn hash_of<T: std::hash::Hash>(build: &PrecomputedBuildHasher, value: &T) -> u64 {
        build.hash_one(value)
    }

    #[test]
    fn same_key_hashes_the_same_within_one_build_hasher() {
        let build = PrecomputedBuildHasher::default();
        let key = AggregateKey::new(1, 2, 3);
        assert_eq!(hash_of(&build, &key), hash_of(&build, &key.clone()));
    }

    /// The failure this guards: an `absorb` that ignores its input, or a `finish` that returns
    /// the seed. Both would make every key hash identically and turn every map into a list —
    /// correct, and catastrophically slow. Verified to FAIL when `absorb` is stubbed to a no-op.
    #[test]
    fn distinct_keys_do_not_collapse() {
        let build = PrecomputedBuildHasher::default();
        let hashes: std::collections::HashSet<u64> =
            (0..10_000u128).map(|i| hash_of(&build, &AggregateKey::new(7, 9, i))).collect();
        assert_eq!(hashes.len(), 10_000, "hash collapsed distinct keys");
    }

    /// The bucket index is taken from the hash's HIGH bits, so a hasher that avalanches only the
    /// low bits passes the test above and still puts every key in one bucket.
    #[test]
    fn high_bits_vary() {
        let build = PrecomputedBuildHasher::default();
        let top: std::collections::HashSet<u64> =
            (0..1_000u128).map(|i| hash_of(&build, &AggregateKey::new(7, 9, i)) >> 57).collect();
        assert!(top.len() > 100, "only {} distinct top-7-bit values in 1000 keys", top.len());
    }

    #[test]
    fn map_round_trips() {
        let mut map: HashMap<AggregateKey, u128, PrecomputedBuildHasher> = HashMap::default();
        for i in 0..5_000u128 {
            map.insert(AggregateKey::new(1, 1, i), i);
        }
        assert_eq!(map.len(), 5_000);
        for i in 0..5_000u128 {
            assert_eq!(map.get(&AggregateKey::new(1, 1, i)), Some(&i));
        }
        assert_eq!(map.get(&AggregateKey::new(1, 1, 5_000)), None);
    }

    /// Two `PrecomputedBuildHasher`s in one process share the seed, so a key moved between two
    /// maps hashes consistently. (The seed varies per process, which is the DoS defence.)
    #[test]
    fn seed_is_stable_within_a_process() {
        let key = AggregateKey::new(4, 5, 6);
        assert_eq!(
            hash_of(&PrecomputedBuildHasher::default(), &key),
            hash_of(&PrecomputedBuildHasher::default(), &key)
        );
    }

    #[test]
    fn byte_fallback_is_length_sensitive() {
        let build = PrecomputedBuildHasher::default();
        assert_ne!(build.hash_one(b"abc".as_slice()), build.hash_one(b"abcd".as_slice()));
    }
}
