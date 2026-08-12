//! Per-aggregate negative-lookup client bloom (design of record:
//! celeriant-notes/db/idempotency-negative-lookup.md).
//!
//! Answers the one question the produce hot path asks on a per-client LRU miss:
//! "is this client_id definitely absent from this aggregate?" A definite absent
//! makes a first write scan-free. The single correctness obligation, verbatim
//! from the notes: never let the bloom become a subset of the true client set.
//! Supersets are always safe (a phantom member costs one scan, never
//! correctness), so eviction, trim, delete, truncate and rollback need no
//! invalidation — staleness only ever drifts in the safe direction.
//!
//! Build protocol (install-empty-then-populate): the entry is installed in
//! `Building` state BEFORE the historical scan starts, so insert-on-write from
//! concurrent commits lands in the same object during the scan's awaits. Only
//! after an exhaustive walk of the aggregate's history is it marked complete;
//! `Building` never answers "absent".

use std::collections::HashSet;

use celeriant_wal::sbbf;

/// Main bloom cap per aggregate — mirrors the sealed-sidecar per-aggregate
/// `CLIENT_SET_BLOOM_MAX_BYTES` rationale: ~6500 clients at 10 bits/key before
/// the FP rate climbs, and degradation is graceful (an FP costs a scan).
pub const NEGATIVE_BLOOM_MAX_BYTES: usize = 8 * 1024;
const NEGATIVE_BLOOM_BITS_PER_KEY: usize = 10;
const SBBF_BLOCK_BYTES: usize = 32;
/// Past this many distinct clients the sized bloom is at cap anyway, so the
/// build spills the collected set into a cap-size bloom and inserts directly.
const SPILL_THRESHOLD: usize = NEGATIVE_BLOOM_MAX_BYTES * 8 / NEGATIVE_BLOOM_BITS_PER_KEY;
/// Cap on sealed-sidecar bloom words carried per entry (see `aux` field). An
/// aggregate needing more degrades to today's scan-per-new-client behaviour
/// (its build never completes) — bounded memory beats a rare optimisation.
pub const AUX_MAX_TOTAL_BYTES: usize = 64 * 1024;
/// Fixed estimate for key + struct + LRU node overhead in the byte budget.
pub const ENTRY_BASE_BYTES: u64 = 160;
/// Estimated bytes per collected hash while Building (8B value + set overhead).
const HASH_SET_BYTES_PER_ENTRY: u64 = 16;

/// Client membership storage. While Building the distinct hashes are collected
/// so the bloom can be sized from the true count at completion; a build that
/// exceeds `SPILL_THRESHOLD` switches to a cap-size bloom mid-build (inserting
/// into a bloom loses no members, so the superset invariant holds either way).
enum Members {
    Collecting(HashSet<u64>),
    Bloom(Vec<u64>),
}

pub struct NegativeClientBloom {
    members: Members,
    /// True until an exhaustive build marks the entry complete. A Building
    /// entry never answers "definitely absent".
    building: bool,
    /// True while a build scan is in flight for this entry. Acts as the
    /// one-build-per-aggregate latch: it is set synchronously (single-threaded
    /// executor, no await between check and set), so at most one builder runs.
    /// While true the entry is also pinned against eviction.
    builder_active: bool,
    /// Which builder owns this entry, stamped from the cache-wide monotonic
    /// counter at begin-build (0 = never adopted by a builder, e.g. seeded).
    /// Builder-identity-SENSITIVE mutators — finish/park, which flip `building`
    /// or clear the latch — must verify this token first: a dead builder's late
    /// finish against a successor entry would mark a half-built bloom Complete
    /// (subset — false absents) or unlatch the live builder (whose aux a resume
    /// then wipes). Plain member/aux INSERTS stay generation-blind by design:
    /// inserting into ANY resident entry only grows the set — superset-safe.
    build_generation: u64,
    /// Sealed-sidecar `ClientSet::Bloom` words unioned in at build time. The
    /// sidecar blooms use the same `client_id_bloom_hash` + SBBF math but are
    /// sized per segment, so their words cannot be OR-merged into a
    /// differently-sized filter; keeping them verbatim and OR-ing the CONTAINS
    /// answers is exactly set-union semantics.
    aux: Vec<Vec<u64>>,
}

impl NegativeClientBloom {
    pub fn new_building() -> Self {
        Self {
            members: Members::Collecting(HashSet::new()),
            building: true,
            builder_active: false,
            build_generation: 0,
            aux: Vec::new(),
        }
    }

    pub fn is_complete(&self) -> bool {
        !self.building
    }

    pub fn is_builder_active(&self) -> bool {
        self.builder_active
    }

    pub fn build_generation(&self) -> u64 {
        self.build_generation
    }

    /// Latch the entry for a builder identified by `generation`.
    pub fn set_builder(&mut self, generation: u64) {
        self.builder_active = true;
        self.build_generation = generation;
    }

    /// Drop unioned sidecar words at (re)build start: the resuming build
    /// re-unions them, and duplicates would grow `aux` unbounded across
    /// truncated build attempts. Safe — Building never answers absent.
    pub fn reset_aux(&mut self) {
        self.aux.clear();
    }

    /// Insert a committed (or about-to-commit) client hash. Phantoms from
    /// rolled-back writes are supersets, i.e. safe.
    pub fn insert_hash(&mut self, hash: u64) {
        match &mut self.members {
            Members::Collecting(set) => {
                set.insert(hash);
                if set.len() > SPILL_THRESHOLD {
                    let mut words = vec![0u64; NEGATIVE_BLOOM_MAX_BYTES / 8];
                    for h in set.iter() {
                        sbbf::insert(&mut words, *h);
                    }
                    self.members = Members::Bloom(words);
                }
            }
            Members::Bloom(words) => sbbf::insert(words, hash),
        }
    }

    /// Union sealed-sidecar bloom words. `false` = the aux cap would overflow;
    /// the caller must then treat the build as non-exhaustive.
    pub fn try_union_bloom_words(&mut self, words: &[u64]) -> bool {
        if words.is_empty() || words.len() % 4 != 0 {
            // Malformed words can't be trusted for absence — refuse, caller
            // degrades to scanning that segment's history (build incomplete).
            return false;
        }
        let aux_bytes: usize = self.aux.iter().map(|w| w.len() * 8).sum();
        if aux_bytes + words.len() * 8 > AUX_MAX_TOTAL_BYTES {
            return false;
        }
        self.aux.push(words.to_vec());
        true
    }

    /// `complete=true` requires the caller to have provably walked the
    /// aggregate's entire history (union of scan + complete sidecar sets +
    /// concurrent insert-on-write). Sizes the bloom from the collected count
    /// with headroom. `complete=false` just parks the builder; collected
    /// members are kept for a later resumed build.
    pub fn finish_build(&mut self, complete: bool) {
        self.builder_active = false;
        if !complete {
            return;
        }
        if let Members::Collecting(set) = &self.members {
            let bits = set.len().max(1) * NEGATIVE_BLOOM_BITS_PER_KEY * 2; // 2x headroom for future clients
            let bytes = bits
                .div_ceil(8)
                .next_multiple_of(SBBF_BLOCK_BYTES)
                .min(NEGATIVE_BLOOM_MAX_BYTES);
            let mut words = vec![0u64; bytes / 8];
            for h in set.iter() {
                sbbf::insert(&mut words, *h);
            }
            self.members = Members::Bloom(words);
        }
        self.building = false;
    }

    /// `false` = definitely absent. Only trustworthy when `is_complete()`;
    /// while Building this conservatively answers `true`.
    pub fn may_contain_hash(&self, hash: u64) -> bool {
        if self.building {
            return true;
        }
        let in_main = match &self.members {
            Members::Collecting(set) => set.contains(&hash),
            Members::Bloom(words) => sbbf::contains(words, hash),
        };
        in_main || self.aux.iter().any(|w| sbbf::contains(w, hash))
    }

    /// Estimated bytes for the LRU byte budget.
    pub fn byte_cost(&self) -> u64 {
        let members = match &self.members {
            Members::Collecting(set) => set.len() as u64 * HASH_SET_BYTES_PER_ENTRY,
            Members::Bloom(words) => words.len() as u64 * 8,
        };
        let aux: u64 = self.aux.iter().map(|w| w.len() as u64 * 8).sum();
        ENTRY_BASE_BYTES + members + aux
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(i: u64) -> u64 {
        celeriant_wal::aggregate_client_key::client_id_bloom_hash(i as u128)
    }

    #[test]
    fn building_never_answers_absent() {
        let bloom = NegativeClientBloom::new_building();
        assert!(!bloom.is_complete());
        assert!(bloom.may_contain_hash(h(1)), "Building must answer maybe-present for everything");
    }

    #[test]
    fn complete_answers_absent_for_unseen_and_maybe_for_members() {
        let mut bloom = NegativeClientBloom::new_building();
        for i in 0..100 {
            bloom.insert_hash(h(i));
        }
        bloom.finish_build(true);
        assert!(bloom.is_complete());
        for i in 0..100 {
            assert!(bloom.may_contain_hash(h(i)), "no false absent for member {i}");
        }
        assert!(!bloom.may_contain_hash(h(999_999)), "unseen client must be definitely absent");
    }

    #[test]
    fn incomplete_finish_keeps_building_state() {
        let mut bloom = NegativeClientBloom::new_building();
        bloom.set_builder(1);
        bloom.insert_hash(h(1));
        bloom.finish_build(false);
        assert!(!bloom.is_complete(), "a truncated build must never become complete");
        assert!(!bloom.is_builder_active());
        assert!(bloom.may_contain_hash(h(2)), "still answers maybe-present");
    }

    #[test]
    fn inserts_after_completion_land_in_the_bloom() {
        let mut bloom = NegativeClientBloom::new_building();
        bloom.finish_build(true);
        assert!(!bloom.may_contain_hash(h(7)));
        bloom.insert_hash(h(7));
        assert!(bloom.may_contain_hash(h(7)), "post-completion insert-on-write must land");
    }

    #[test]
    fn spill_at_threshold_loses_no_members() {
        let mut bloom = NegativeClientBloom::new_building();
        let n = (SPILL_THRESHOLD + 100) as u64;
        for i in 0..n {
            bloom.insert_hash(h(i));
        }
        bloom.finish_build(true);
        for i in 0..n {
            assert!(bloom.may_contain_hash(h(i)), "spill dropped member {i} — subset, unsound");
        }
        assert_eq!(bloom.byte_cost(), ENTRY_BASE_BYTES + NEGATIVE_BLOOM_MAX_BYTES as u64, "spilled bloom is cap-sized");
    }

    #[test]
    fn aux_union_members_are_maybe_present() {
        let mut sidecar = vec![0u64; 8];
        sbbf::insert(&mut sidecar, h(42));
        let mut bloom = NegativeClientBloom::new_building();
        assert!(bloom.try_union_bloom_words(&sidecar));
        bloom.finish_build(true);
        assert!(bloom.may_contain_hash(h(42)), "sidecar member must stay maybe-present");
        assert!(!bloom.may_contain_hash(h(43)), "non-member of both blooms is absent");
    }

    #[test]
    fn aux_cap_overflow_and_malformed_words_are_refused() {
        let mut bloom = NegativeClientBloom::new_building();
        assert!(!bloom.try_union_bloom_words(&[]), "empty words prove nothing");
        assert!(!bloom.try_union_bloom_words(&[0u64; 3]), "non-block-multiple words prove nothing");
        let big = vec![0u64; NEGATIVE_BLOOM_MAX_BYTES / 8];
        for _ in 0..(AUX_MAX_TOTAL_BYTES / NEGATIVE_BLOOM_MAX_BYTES) {
            assert!(bloom.try_union_bloom_words(&big));
        }
        assert!(!bloom.try_union_bloom_words(&big), "aux beyond the cap must be refused");
    }

    #[test]
    fn sized_bloom_is_capped() {
        let mut bloom = NegativeClientBloom::new_building();
        for i in 0..5000u64 {
            bloom.insert_hash(h(i));
        }
        bloom.finish_build(true);
        // 5000 keys × 10 bits × 2 = 12500 bytes → capped at 8 KiB.
        assert_eq!(bloom.byte_cost(), ENTRY_BASE_BYTES + NEGATIVE_BLOOM_MAX_BYTES as u64);
    }

    #[test]
    fn empty_complete_build_answers_universal_absence() {
        // Unlike the sidecar's None-bloom rule, an EMPTY bloom here is sound:
        // completion asserts the whole history was walked and held no clients.
        let mut bloom = NegativeClientBloom::new_building();
        bloom.finish_build(true);
        assert!(!bloom.may_contain_hash(h(1)));
    }

    #[test]
    fn reset_aux_clears_prior_unions() {
        let mut sidecar = vec![0u64; 8];
        sbbf::insert(&mut sidecar, h(42));
        let mut bloom = NegativeClientBloom::new_building();
        assert!(bloom.try_union_bloom_words(&sidecar));
        bloom.reset_aux();
        assert_eq!(bloom.byte_cost(), ENTRY_BASE_BYTES, "aux bytes must be released");
    }
}
