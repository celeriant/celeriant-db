//! Per-task birth ledger for the non-stationary cardinality workload.
//!
//! The workload mints aggregates continuously and never stops, so the interesting
//! question is what a *dormant* aggregate costs to touch as a function of how long
//! it has been dormant. Answering that needs a sample of old aggregates, and the
//! sample has to survive a five-hour run on a build machine — which rules out
//! remembering every id.
//!
//! So the ledger keeps birth-time cohorts, each holding a bounded reservoir sample.
//! Retained memory is `cohorts x reservoir_per_cohort` regardless of how many ids
//! were minted; `births_total` keeps counting. Cohorts older than `max_tracked_age`
//! are dropped whole.
//!
//! Time is a caller-supplied `now_ms` rather than `Instant::now()`. A five-hour age
//! spread is not something a test can wait for.

use std::time::Duration;

/// One minted identity. A fresh client per aggregate is deliberate: it is the
/// worst case for the server's per-aggregate client-set map, which is the
/// structure under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Member {
    pub aggregate_id: u128,
    pub client_id: u128,
}

/// Age bands the reheat probe samples across, youngest first. The reheat cost
/// curve is reported per bucket; a flat curve means cardinality scales.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AgeBucket {
    #[serde(rename = "5m")]
    FiveMin,
    #[serde(rename = "30m")]
    ThirtyMin,
    #[serde(rename = "2h")]
    TwoHour,
    #[serde(rename = "4h")]
    FourHour,
}

impl AgeBucket {
    pub const ALL: [AgeBucket; 4] =
        [AgeBucket::FiveMin, AgeBucket::ThirtyMin, AgeBucket::TwoHour, AgeBucket::FourHour];

    /// Half-open `[lo, hi)` age range. Contiguous and non-overlapping, so a given
    /// age belongs to at most one bucket and the curve has no double-counted point.
    pub fn range(self) -> (Duration, Duration) {
        let m = |n: u64| Duration::from_secs(n * 60);
        match self {
            AgeBucket::FiveMin => (m(5), m(30)),
            AgeBucket::ThirtyMin => (m(30), m(120)),
            AgeBucket::TwoHour => (m(120), m(240)),
            AgeBucket::FourHour => (m(240), Duration::MAX),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            AgeBucket::FiveMin => "5m",
            AgeBucket::ThirtyMin => "30m",
            AgeBucket::TwoHour => "2h",
            AgeBucket::FourHour => "4h",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PopulationConfig {
    pub task_id: u32,
    pub data_shards: u32,
    pub cohort_slice: Duration,
    pub max_tracked_age: Duration,
    pub reservoir_per_cohort: usize,
    pub seed: u64,
}

struct Cohort {
    /// Index of the `cohort_slice`-wide window this cohort covers, from run start.
    slice: u64,
    /// Births that landed in this cohort, including those the reservoir dropped.
    /// Algorithm R needs the true count, not the retained count.
    seen: u64,
    members: Vec<Member>,
}

pub struct Population {
    cfg: PopulationConfig,
    /// Ascending by `slice`. Births arrive in non-decreasing time, so the live
    /// cohort is the last element and expiry drains from the front.
    cohorts: Vec<Cohort>,
    births_total: u64,
    rng: u64,
}

impl Population {
    pub fn new(cfg: PopulationConfig) -> Self {
        assert!(cfg.data_shards > 0, "data_shards must be > 0");
        assert!(cfg.reservoir_per_cohort > 0, "reservoir_per_cohort must be > 0");
        assert!(!cfg.cohort_slice.is_zero(), "cohort_slice must be > 0");
        Self {
            cohorts: Vec::new(),
            births_total: 0,
            // Mixing task_id in keeps two tasks on the same seed from drawing the
            // same reservoir decisions, which would correlate their samples.
            rng: cfg.seed ^ (cfg.task_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
            cfg,
        }
    }

    pub fn births_total(&self) -> u64 {
        self.births_total
    }

    pub fn retained(&self) -> usize {
        self.cohorts.iter().map(|c| c.members.len()).sum()
    }

    /// Mint a fresh aggregate and client, and offer the pair to the current cohort.
    ///
    /// The aggregate id is constructed so that `aggregate_id % data_shards` equals
    /// this task's shard. Routing is `RoutingRule::AggregateId`, and an id landing
    /// on another shard migrates the whole TCP stream across the glommio mesh — a
    /// full mesh channel answers SERVER_BUSY with no retry. So this is not a
    /// preference, it is the difference between a workload and an outage.
    pub fn birth(&mut self, now_ms: u64) -> Member {
        let n = self.births_total;
        self.births_total += 1;

        let shards = self.cfg.data_shards as u128;
        let lane = (self.cfg.task_id % self.cfg.data_shards) as u128;
        // Distinct tasks occupy disjoint id space: the ordinal is namespaced by
        // task_id before being scaled onto the shard lane, so no two tasks can
        // collide however long either runs.
        let ordinal = (self.cfg.task_id as u128) << 64 | n as u128;
        let aggregate_id = ordinal * shards + lane;
        let client_id = Self::mix_u128(aggregate_id);
        let member = Member { aggregate_id, client_id };

        self.expire(now_ms);
        self.offer(member, now_ms);
        member
    }

    /// Drop cohorts that have aged out. Exposed because a task that stops minting
    /// would otherwise hold its last cohorts for the rest of the run.
    pub fn expire(&mut self, now_ms: u64) {
        let slice_ms = self.cfg.cohort_slice.as_millis() as u64;
        let max_age_ms = self.cfg.max_tracked_age.as_millis().min(u64::MAX as u128) as u64;
        let cutoff = now_ms.saturating_sub(max_age_ms);
        // A cohort is expired once its whole window sits below the cutoff, i.e. its
        // newest possible member is older than max_tracked_age.
        let keep_from = cutoff / slice_ms;
        self.cohorts.retain(|c| c.slice >= keep_from);
    }

    /// Sample a retained member whose age at `now_ms` lands inside `bucket`.
    ///
    /// Uniform over the retained members of every eligible cohort, weighted by
    /// retained size rather than by cohort — a cohort holding 3 members should not
    /// contribute as heavily as one holding 32.
    pub fn sample_by_age(&mut self, now_ms: u64, bucket: AgeBucket) -> Option<Member> {
        let slice_ms = self.cfg.cohort_slice.as_millis() as u64;
        let (lo, hi) = bucket.range();
        let lo_ms = lo.as_millis() as u64;
        let hi_ms = hi.as_millis().min(u64::MAX as u128) as u64;

        // Age is measured from the cohort's newest edge, so every member of an
        // eligible cohort is at least `lo` old. Erring young would report a cheap
        // read in an old bucket and flatten the curve — the one artefact that would
        // fake the result this scenario exists to find.
        let mut total = 0usize;
        for c in &self.cohorts {
            if Self::cohort_in_bucket(c.slice, slice_ms, now_ms, lo_ms, hi_ms) {
                total += c.members.len();
            }
        }
        if total == 0 {
            return None;
        }

        let mut pick = (self.next_rand() % total as u64) as usize;
        for c in &self.cohorts {
            if !Self::cohort_in_bucket(c.slice, slice_ms, now_ms, lo_ms, hi_ms) {
                continue;
            }
            if pick < c.members.len() {
                return Some(c.members[pick]);
            }
            pick -= c.members.len();
        }
        None
    }

    /// True when every member of the cohort is at least `lo_ms` old and the cohort's
    /// oldest member is under `hi_ms`.
    fn cohort_in_bucket(slice: u64, slice_ms: u64, now_ms: u64, lo_ms: u64, hi_ms: u64) -> bool {
        let newest_born = slice.saturating_mul(slice_ms) + slice_ms.saturating_sub(1);
        let oldest_born = slice.saturating_mul(slice_ms);
        let min_age = now_ms.saturating_sub(newest_born);
        let max_age = now_ms.saturating_sub(oldest_born);
        now_ms >= newest_born && min_age >= lo_ms && max_age < hi_ms
    }

    /// Algorithm R over the cohort: the first `k` are kept, and the `n`th birth
    /// (n > k) replaces a uniformly chosen slot with probability `k/n`. Every
    /// birth in the cohort therefore has equal probability of being retained,
    /// which is what makes the reheat curve a measurement rather than an artefact
    /// of when the sample was taken.
    fn offer(&mut self, member: Member, now_ms: u64) {
        let slice_ms = self.cfg.cohort_slice.as_millis() as u64;
        let slice = now_ms / slice_ms;
        let k = self.cfg.reservoir_per_cohort;

        if self.cohorts.last().map(|c| c.slice) != Some(slice) {
            match self.cohorts.binary_search_by_key(&slice, |c| c.slice) {
                Ok(_) => {}
                Err(pos) => self
                    .cohorts
                    .insert(pos, Cohort { slice, seen: 0, members: Vec::with_capacity(k.min(64)) }),
            }
        }
        let idx = match self.cohorts.binary_search_by_key(&slice, |c| c.slice) {
            Ok(i) => i,
            Err(_) => return,
        };

        let seen = {
            let c = &mut self.cohorts[idx];
            c.seen += 1;
            c.seen
        };
        if seen <= k as u64 {
            self.cohorts[idx].members.push(member);
            return;
        }
        let j = self.next_rand() % seen;
        if j < k as u64 {
            self.cohorts[idx].members[j as usize] = member;
        }
    }

    /// SplitMix64. Deterministic, allocation-free, and no dependency — the crate
    /// deliberately carries no `rand`.
    fn next_rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Spread an aggregate id across 128 bits so client ids do not share the
    /// aggregate's shard-lane structure. Client identity has its own bloom on the
    /// server; correlating the two would mask a false positive in either.
    fn mix_u128(x: u128) -> u128 {
        let lo = (x as u64) ^ 0xD6E8_FEB8_6659_FD93;
        let hi = ((x >> 64) as u64) ^ 0xA076_1D64_78BD_642F;
        let mix = |mut z: u64| {
            z = (z ^ (z >> 32)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
            z = (z ^ (z >> 32)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
            z ^ (z >> 32)
        };
        ((mix(hi) as u128) << 64) | mix(lo) as u128
    }
}
