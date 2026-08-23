//! Client-side drivers for the `cardinality_pressure` scenario.
//!
//! Everything here is the *workload*: the non-stationary population loop, the
//! contention loop, the age-stratified read probe, and the pure arithmetic that
//! turns CLI flags into rates. `scenario.rs` owns the cluster orchestration.
//!
//! Three rules are load-bearing and every function here is written around them:
//!
//! 1. **The birth rate is cluster-wide, divided across tasks.** A 16k-connection
//!    run must not mint aggregates sixteen times faster than a 1k-connection
//!    run, or their reheat curves are not comparable and the connection dial
//!    stops being an A/B.
//! 2. **Task `t` touches only ids `≡ t (mod data_shards)`.** `Population::birth`
//!    guarantees it; a task rotating ids freely trips `check_client_redirect`
//!    and migrates the whole TCP stream across the glommio mesh.
//! 3. **`iv` stays `None`.** The server skips schema validation entirely for any
//!    event carrying an IV. `account_event` already does this.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use celeriant_bench::account_workload::{
    ALL_MAJORS, AckKey, AckLedger, MAJOR_STATEMENT_ATTACHED, NOMINAL_DATABLOCK_BYTES,
    STATEMENT_MAX_BYTES, STATEMENT_MIN_BYTES, account_event, account_key, aggregates_per_segment,
    derive_large_event_fraction,
};
use celeriant_bench::population::{AgeBucket, Member, Population, PopulationConfig};
use celeriant_bench::read_workload::{ReadErrorKind, ReheatCostCurve};
use celeriant_bench::{
    ClientError, HistoryRecorder, Pool, ReadFilters, ServerError, WriteError, WriteEventsOptions,
};

/// Data shards on the cluster: 4 cores with `RESERVE_COORDINATOR_SHARD=true`,
/// so shard 0 coordinates and 1..=3 hold data. `--tasks` must be a multiple of
/// this or shard affinity distributes unevenly and one executor carries more of
/// the population than the others.
pub const DATA_SHARDS: u32 = 3;
/// Shard 0 is reserved for coordination and never rotates a data segment, so it
/// is excluded from `RotationsReached`.
pub const COORDINATOR_SHARD_ID: u32 = 0;

/// Design point the aggregate bloom was sized for: under 1% false positives at
/// 200k aggregates per segment.
pub const BLOOM_DESIGN_POINT_AGGS_PER_SEGMENT: u64 = 200_000;

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// Fill budget presets. The third column of goal.md's table is the point: the
/// oldest measurable dormancy age is bounded by how long the fill ran, so five
/// hours is not a longer version of one hour — it reaches a part of the reheat
/// curve one hour cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Preset {
    /// 10 min. No useful age spread; proves the harness assembles. Inconclusive
    /// by construction, which is the correct verdict for it.
    Smoke,
    /// 1 hour — reaches ~50 min of dormancy.
    Short,
    /// 5 hours — reaches ~4.5 hours.
    Deep,
}

impl Preset {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "smoke" => Ok(Self::Smoke),
            "short" => Ok(Self::Short),
            "deep" => Ok(Self::Deep),
            other => Err(format!("unknown preset '{other}' (expected smoke|short|deep)")),
        }
    }

    pub fn fill_budget(self) -> Duration {
        match self {
            Self::Smoke => Duration::from_secs(10 * 60),
            Self::Short => Duration::from_secs(60 * 60),
            Self::Deep => Duration::from_secs(5 * 60 * 60),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Short => "short",
            Self::Deep => "deep",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CardinalityParams {
    /// Run phase 6 (SIGKILL the leader, measure promotion and the write gap).
    ///
    /// Off by default, and that is a disambiguation decision rather than
    /// convenience. Failover under a behind-follower is a CONFIRMED separate
    /// defect with its own dedicated red test (`failover_pressure_matrix`), and
    /// it panics a shard — which cascades into ExactlyOneLeader,
    /// EventualConvergence, AckedSubsetDurableBothNodes and JournalNoPanics.
    /// Leaving it on means every run reports FAIL for a reason that has nothing
    /// to do with memory or cardinality, and the question this scenario exists
    /// to answer can never report on its own terms.
    ///
    /// Phase 4's stop/start of BOTH nodes is graceful and does NOT trip it (6.0s,
    /// clean, in both recorded runs), so the cold-restart delta — the actual
    /// deliverable — is unaffected by this switch.
    pub failover_phase: bool,
    pub preset: Preset,
    /// Overrides the preset's budget when set.
    pub fill_duration: Option<Duration>,
    pub disk_high_water_pct: u64,
    pub segment_bytes: u64,
    pub memory_percent: u64,
    pub target_aggs_per_segment: u64,
    /// R distinct replicas per account in the contention phase.
    pub contention_factor: usize,
    /// Cluster-wide births per second, divided across tasks. Never per-task.
    pub birth_rate_per_sec: f64,
    /// Cluster-wide reheat probes per second, divided across tasks.
    pub reheat_rate_per_sec: f64,
}

impl Default for CardinalityParams {
    fn default() -> Self {
        Self {
            failover_phase: false,
            preset: Preset::Smoke,
            fill_duration: None,
            disk_high_water_pct: 70,
            segment_bytes: 256 * 1024 * 1024,
            memory_percent: 20,
            target_aggs_per_segment: BLOOM_DESIGN_POINT_AGGS_PER_SEGMENT,
            contention_factor: 8,
            birth_rate_per_sec: 50.0,
            reheat_rate_per_sec: 5.0,
        }
    }
}

impl CardinalityParams {
    pub fn fill_budget(&self) -> Duration {
        self.fill_duration.unwrap_or_else(|| self.preset.fill_budget())
    }

    /// The payload mix, **derived** from segment size and the target density.
    /// Never taken raw: hold the mix constant while walking 256MB then 1GB and
    /// the two stages are not comparable, because bloom load is set by the mix
    /// relative to the segment size rather than by either alone.
    pub fn large_event_fraction(&self) -> f64 {
        derive_large_event_fraction(
            self.segment_bytes,
            self.target_aggs_per_segment,
            NOMINAL_DATABLOCK_BYTES,
        )
    }

    /// What the derived mix actually achieves. Differs from the target when the
    /// clamp bit — below the metablock floor, or past an all-large payload — and
    /// the report must quote this rather than the request.
    pub fn achieved_aggs_per_segment(&self) -> u64 {
        aggregates_per_segment(self.segment_bytes, self.large_event_fraction(), NOMINAL_DATABLOCK_BYTES)
    }

    /// Declared memory budget on a node with `ram_bytes` of physical RAM.
    pub fn declared_budget_bytes(&self, ram_bytes: u64) -> u64 {
        ram_bytes / 100 * self.memory_percent
    }
}

/// `--tasks` must be a positive multiple of `DATA_SHARDS`.
///
/// Not a style preference. Task `t` owns the id lane `t % data_shards`, so a
/// task count that is not a multiple leaves one lane with more tasks than the
/// others — that executor then carries more of the population and every
/// per-shard figure in the report is measuring an unequal split.
pub fn validate_tasks(tasks: usize) -> Result<(), String> {
    if tasks == 0 {
        return Err("--tasks must be > 0".into());
    }
    if tasks % DATA_SHARDS as usize != 0 {
        return Err(format!(
            "--tasks must be a multiple of {DATA_SHARDS} (the data shard count), got {tasks}; \
             otherwise shard affinity distributes unevenly and one executor carries more of the population"
        ));
    }
    Ok(())
}

/// Turn a **cluster-wide** rate into the interval one task waits between its own
/// firings. `tasks / rate` seconds per task, so `tasks * (1/interval)` is the
/// cluster rate again for any task count — which is what makes a 500-connection
/// run and a 20,000-connection run comparable.
///
/// `None` disables the process entirely (rate <= 0).
// `!(rate > 0.0)` rather than `rate <= 0.0` on purpose: NaN must land in the
// disabled branch. Clippy's suggested rewrite would let a NaN rate through to
// `from_secs_f64`, which panics on it.
#[allow(clippy::neg_cmp_op_on_partial_ord)]
pub fn per_task_interval(cluster_rate_per_sec: f64, tasks: usize) -> Option<Duration> {
    if !(cluster_rate_per_sec > 0.0) || tasks == 0 {
        return None;
    }
    // A day is longer than any preset, so the clamp only catches rates so small
    // the process would never fire anyway — and it keeps `from_secs_f64` away
    // from its overflow panic.
    Some(Duration::from_secs_f64((tasks as f64 / cluster_rate_per_sec).min(86_400.0)))
}

/// Why the fill stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillStop {
    TimeBudget,
    /// Data filesystem crossed the high-water mark. A clean, reported outcome —
    /// the alternative is real ENOSPC, which turns the tail of the run into
    /// garbage.
    DiskHighWater(u64),
}

impl FillStop {
    pub fn label(self) -> String {
        match self {
            Self::TimeBudget => "time budget reached".into(),
            Self::DiskHighWater(pct) => format!("data filesystem at {pct}% — disk high-water"),
        }
    }
}

/// Evaluated every fill tick. Disk is checked first: when both conditions land
/// in the same tick the space figure is the one that constrains the next run's
/// budget, so that is what gets reported.
pub fn fill_stop(
    elapsed: Duration,
    budget: Duration,
    data_fs_used_pct: u64,
    high_water_pct: u64,
) -> Option<FillStop> {
    if high_water_pct > 0 && data_fs_used_pct >= high_water_pct {
        return Some(FillStop::DiskHighWater(data_fs_used_pct));
    }
    (elapsed >= budget).then_some(FillStop::TimeBudget)
}

/// Retained-member budget for the birth ledgers, split across tasks and cohorts.
/// The ledger has to survive a five-hour run on a build machine, so total
/// retention is capped rather than left to grow with the run.
const TARGET_RETAINED_MEMBERS: usize = 500_000;
const MAX_COHORTS: u64 = 64;

/// Cohort geometry for a fill of `budget`. Slices are wide enough that a long
/// run keeps a bounded cohort count, and the per-cohort reservoir shrinks as
/// tasks multiply so total retention stays inside `TARGET_RETAINED_MEMBERS`.
pub fn population_config(task_id: u32, tasks: usize, budget: Duration, seed: u64) -> PopulationConfig {
    // Ages are still sampled after the fill ends (phases 3 and 5), so track past
    // the fill rather than exactly to it.
    let max_tracked_age = budget + Duration::from_secs(30 * 60);
    let slice_secs = (max_tracked_age.as_secs() / MAX_COHORTS).max(60);
    let reservoir = (TARGET_RETAINED_MEMBERS / (tasks.max(1) * MAX_COHORTS as usize)).clamp(4, 64);
    PopulationConfig {
        task_id,
        data_shards: DATA_SHARDS,
        cohort_slice: Duration::from_secs(slice_secs),
        max_tracked_age,
        reservoir_per_cohort: reservoir,
        seed,
    }
}

// ---------------------------------------------------------------------------
// Small shared primitives
// ---------------------------------------------------------------------------

/// SplitMix64. Same generator as `Population` and `AckLedger` — deterministic,
/// allocation-free, and the bench crates deliberately carry no `rand`.
#[derive(Debug, Clone, Copy)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Bounded uniform reservoir of millisecond latencies.
///
/// `run_benchmark` accumulates every sample, which is fine for 60 seconds and
/// not for five hours across thousands of tasks. Uniform rather than truncated
/// because the population is deliberately non-stationary: a late- or
/// early-biased sample would report one phase of the run as if it were all of it.
#[derive(Debug, Clone)]
pub struct LatencyReservoir {
    cap: usize,
    samples: Vec<u64>,
    count: u64,
    rng: Rng,
}

impl LatencyReservoir {
    pub fn new(cap: usize, seed: u64) -> Self {
        Self { cap: cap.max(1), samples: Vec::new(), count: 0, rng: Rng::new(seed) }
    }

    pub fn record(&mut self, ms: u64) {
        self.count += 1;
        if self.samples.len() < self.cap {
            self.samples.push(ms);
            return;
        }
        let j = self.rng.next_u64() % self.count;
        if j < self.cap as u64 {
            self.samples[j as usize] = ms;
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn into_samples(self) -> Vec<u64> {
        self.samples
    }
}

/// Client-visible write availability across a leader kill.
///
/// The suite's `FailoverWithinBudget` counts scrape ticks where neither node
/// holds leadership, and its own doc comment concedes the resolution is bounded
/// by the 500ms scraper interval — asserting a 1.6s budget with ±500ms of
/// measurement error asserts nothing. This measures the thing a caller actually
/// experiences, at microsecond resolution, from the client side.
pub struct AvailabilityClock {
    origin: Instant,
    /// Microseconds from `origin` at which the kill was issued.
    /// `u64::MAX` = not yet marked.
    ///
    /// NOT 0 as the sentinel: `mark_kill` stores `origin.elapsed()`, which is
    /// genuinely 0 when the kill lands inside the first microsecond of the
    /// window. That collides with "unmarked", and every subsequent ack is then
    /// filed on the *before* side — so the outage vanishes and the gap reads
    /// None. Rare in a real run, certain in a fast test, and silent either way.
    kill_us: AtomicU64,
    /// Watermarks in microseconds from `origin`. `saw_ok_before` is a separate
    /// flag rather than a zero sentinel: an ack landing in the first
    /// microsecond of the window is a real observation, not a missing one.
    saw_ok_before: AtomicBool,
    last_ok_before_us: AtomicU64,
    first_ok_after_us: AtomicU64,
    /// Largest interval between two consecutive acks anywhere in the window,
    /// and the timestamp of the most recent ack, both in microseconds.
    ///
    /// This is the number to trust. The anchored gap below depends on knowing
    /// when the kill landed, and the orchestrator cannot know that: `make
    /// kill-*` is a blocking ssh, the SIGKILL fires early inside it, and the
    /// call returns long after. Stamping on return classifies acks served
    /// during the outage as "before" and collapses the reported gap; stamping
    /// on entry misclassifies acks the still-live leader served. The largest
    /// observed ack-to-ack gap needs no anchor at all, and under a continuous
    /// writer it IS the client-visible unavailability.
    max_ack_gap_us: AtomicU64,
    prev_ok_us: AtomicU64,
}

impl AvailabilityClock {
    pub fn new(origin: Instant) -> Self {
        Self {
            origin,
            kill_us: AtomicU64::new(u64::MAX),
            saw_ok_before: AtomicBool::new(false),
            last_ok_before_us: AtomicU64::new(0),
            first_ok_after_us: AtomicU64::new(u64::MAX),
            max_ack_gap_us: AtomicU64::new(0),
            prev_ok_us: AtomicU64::new(u64::MAX),
        }
    }

    pub fn mark_kill(&self) {
        self.kill_us.store(self.origin.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    /// Record a successful write. Ops completing before the kill instant raise
    /// the "last ok before" watermark; ops at or after it lower the "first ok
    /// after" one. An op can only land in the second branch once the kill is
    /// marked AND its own completion is at or past it, so the gap can never
    /// come out negative.
    pub fn record_ok(&self) {
        self.record_at_us(self.origin.elapsed().as_micros() as u64);
    }

    /// Injection point for tests: real recording goes through `record_ok`.
    fn record_at_us(&self, us: u64) {

        // Anchor-free: widen the largest observed silence between two acks.
        // `swap`, not `fetch_max` — with a u64::MAX "no predecessor" sentinel a
        // fetch_max never stores, so the sentinel would survive forever and the
        // gap would always read None. Concurrent writers can swap out of order;
        // the `us > prev` guard drops those rather than recording a negative.
        let prev = self.prev_ok_us.swap(us, Ordering::Relaxed);
        if prev != u64::MAX && us > prev {
            self.max_ack_gap_us.fetch_max(us - prev, Ordering::Relaxed);
        }

        let kill = self.kill_us.load(Ordering::Relaxed);
        if kill != u64::MAX && us >= kill {
            self.first_ok_after_us.fetch_min(us, Ordering::Relaxed);
        } else {
            self.last_ok_before_us.fetch_max(us, Ordering::Relaxed);
            self.saw_ok_before.store(true, Ordering::Relaxed);
        }
    }

    /// Largest silence between consecutive acks over the whole window. `None`
    /// until at least two acks landed. Independent of when the kill was marked,
    /// which is why it is the number the report leads with.
    pub fn max_ack_gap(&self) -> Option<Duration> {
        match self.max_ack_gap_us.load(Ordering::Relaxed) {
            0 => None,
            us => Some(Duration::from_micros(us)),
        }
    }

    /// Last `Ok` before the kill → first `Ok` after it. `None` when the window
    /// never saw both, which must not be reported as a zero gap.
    pub fn gap(&self) -> Option<Duration> {
        let before = self.last_ok_before_us.load(Ordering::Relaxed);
        let after = self.first_ok_after_us.load(Ordering::Relaxed);
        if self.kill_us.load(Ordering::Relaxed) == u64::MAX
            || !self.saw_ok_before.load(Ordering::Relaxed)
            || after == u64::MAX
        {
            return None;
        }
        Some(Duration::from_micros(after.saturating_sub(before)))
    }
}

// ---------------------------------------------------------------------------
// Shared write mechanics
// ---------------------------------------------------------------------------

/// One event's worth of payload choice. `f` of the events are settlement
/// statements, which serialise past `MINIBATCH_SIZE_BYTES` and therefore take
/// the datablock path; the rest live inline in the metablock.
fn pick_payload(rng: &mut Rng, large_event_fraction: f64) -> (u64, usize) {
    if rng.unit() < large_event_fraction {
        let span = STATEMENT_MAX_BYTES - STATEMENT_MIN_BYTES;
        let bytes = STATEMENT_MIN_BYTES + (rng.next_u64() as usize % span.max(1));
        (MAJOR_STATEMENT_ATTACHED, bytes)
    } else {
        // Majors 1-4 are the small inline shapes.
        (ALL_MAJORS[(rng.next_u64() % 4) as usize], 0)
    }
}

fn write_opts(expected_version: Option<u64>) -> WriteEventsOptions {
    WriteEventsOptions { allow_create: true, expected_version, enforce_client_idempotency: true }
}

/// The OCC conflict signal, carrying the server's current version when it gave
/// one. Anything else is left to the caller's error path.
fn occ_conflict_version(e: &ClientError) -> Option<u64> {
    match e {
        ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { current_aggregate_version, .. },
            ..
        }) => *current_aggregate_version,
        _ => None,
    }
}

/// What the fill loop should do with a failed write.
///
/// Pure, so the retry discipline is testable without a cluster: every arm of
/// the loop's error path is one of these plus its counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillRetry {
    /// The server named its current version. Resync to it and retry the same
    /// `client_seq` immediately.
    Resync(u64),
    /// The server has already committed this `(client_id, client_seq)`, so the
    /// write IS durable and only the client is stuck. Carries the server's
    /// `last_client_seq` when it named one.
    AlreadyCommitted(Option<u64>),
    /// An idempotency rejection with no ambiguous attempt behind it: the local
    /// `client_seq` is genuinely behind the server's. A finding, not a retry.
    UnexpectedDuplicate,
    /// Nothing learned about whether the write landed. Back off and retry.
    Backoff,
}

/// Classify a failed write. `unacked` says whether the immediately preceding
/// attempt at this same `client_seq` ended without an ack — the only condition
/// under which a duplicate rejection is benign.
///
/// Splitting on that flag is what keeps the fix from masking the defect it
/// would otherwise hide: a duplicate on a first attempt means the server holds
/// a seq this task never sent twice, which is a real exactly-once finding.
fn classify_write_failure(e: &ClientError, unacked: bool) -> FillRetry {
    if let Some(current) = occ_conflict_version(e) {
        return FillRetry::Resync(current);
    }
    match e {
        ClientError::Server(ServerError::Write {
            kind: WriteError::ClientIdempotencyViolation { last_client_seq, .. },
            ..
        }) => {
            if unacked {
                FillRetry::AlreadyCommitted(*last_client_seq)
            } else {
                FillRetry::UnexpectedDuplicate
            }
        }
        _ => FillRetry::Backoff,
    }
}

/// Read an aggregate from `from_version` and report `(bytes, max_version,
/// max_client_seq_for_client)`.
///
/// This is the microservice's catch-up: the numbers it needs to fold and to
/// pick the next `client_seq`. Bytes are counted because a reheat curve that
/// rises in latency with a flat byte count is a queueing artefact, while one
/// that rises in both is the reverse scan genuinely walking further.
async fn catch_up(
    pool: &Arc<Pool>,
    aggregate_id: u128,
    client_id: u128,
    from_version: u64,
) -> Result<(u64, u64, u64), ClientError> {
    let iter = pool.read_all(account_key(aggregate_id), Some(ReadFilters::new(from_version))).await?;
    let batches = iter.collect().await?;
    let mut bytes = 0u64;
    let mut max_version = from_version.saturating_sub(1);
    let mut max_seq = 0u64;
    for b in &batches {
        max_version = max_version.max(b.aggregate_version);
        for e in &b.events {
            bytes += e.event_value.len() as u64;
            if b.client_id == client_id {
                max_seq = max_seq.max(e.client_seq);
            }
        }
    }
    Ok((bytes, max_version, max_seq))
}

// ---------------------------------------------------------------------------
// Phase 1 — fill
// ---------------------------------------------------------------------------

/// An aggregate currently spending its lifetime write budget.
struct Active {
    member: Member,
    version: u64,
    client_seq: u64,
    remaining: u32,
    next_write_ms: u64,
    /// Grows after every write. This is the decay: no aggregate stays hot, so
    /// the cold tail deepens continuously.
    gap_ms: u64,
    /// The last attempt at `client_seq` failed without telling us whether it
    /// committed. Cleared by any progress, so it describes the current seq
    /// only — which is what makes it a sound test for a benign duplicate.
    unacked: bool,
}

impl Active {
    /// Move past a `client_seq` known to be durable, and decay.
    ///
    /// Shared by the `Ok` arm and the already-committed arm, because those two
    /// differ only in how the client learned the write landed.
    fn advance(&mut self, now_ms: u64, committed_seq: u64) {
        self.client_seq = committed_seq + 1;
        self.unacked = false;
        self.remaining = self.remaining.saturating_sub(1);
        self.gap_ms = (self.gap_ms * 2).min(MAX_GAP_MS);
        self.next_write_ms = now_ms + self.gap_ms;
    }

    /// Retry the same `client_seq` later, having learned nothing.
    fn defer(&mut self, now_ms: u64) {
        self.unacked = true;
        self.next_write_ms = now_ms + RETRY_GAP_MS;
    }
}

/// Aggregates a task keeps hot at once. Bounded so a task's working set does
/// not grow with the run; the evicted ones are exactly the dormant population
/// the reheat probe exists to sample.
const ACTIVE_CAP: usize = 64;
const NURSERY_GAP_MS: u64 = 50;
const MAX_GAP_MS: u64 = 60_000;
/// Pause before retrying a write that failed for an unknown reason.
const RETRY_GAP_MS: u64 = 100;
/// Writes bursted onto a reheated aggregate before it decays again.
const REHEAT_BURST: u32 = 4;

#[derive(Debug, Default, Clone, Copy)]
pub struct FillTotals {
    pub writes_ok: u64,
    pub write_errors: u64,
    /// Retries the server answered with "already applied". Durable writes, but
    /// deliberately NOT in `writes_ok`: this run never saw an ack for them, so
    /// counting them as throughput would credit the cluster for work it can
    /// only be taken at its word for. Kept out of `write_errors` too — they
    /// make progress, and lumping them in was what hid the retry storm.
    pub duplicate_acks: u64,
    pub occ_retries: u64,
    pub reheats_ok: u64,
    pub reheat_errors: u64,
    /// `write_errors + reheat_errors` broken out by kind. Both arms feed it,
    /// matching `FillCounters::write_errors`, which has always summed the two.
    pub errors_by_kind: FillErrorTally,
}

pub struct FillOutcome {
    pub population: Population,
    pub ledger: AckLedger,
    pub latencies: Vec<u64>,
    pub totals: FillTotals,
}

pub struct FillConfig {
    pub task_id: u32,
    pub tasks: usize,
    pub seed: u64,
    pub large_event_fraction: f64,
    pub budget: Duration,
    /// Per-task intervals, already divided down from the cluster-wide rates.
    pub birth_interval: Option<Duration>,
    pub reheat_interval: Option<Duration>,
    pub ledger_capacity: usize,
}

/// What a failed fill write actually was.
///
/// The fill used to drop the `ClientError` on the floor and increment one
/// undifferentiated counter, and run 1787056102 is what that costs: **93,024
/// write errors out of 115,527 writes, exactly 0 for 180s and then a cliff**,
/// with no artifact anywhere naming a single one of them. A count without a
/// kind cannot distinguish a client-side deadline (which censors, and means the
/// numbers around it are survivorship-biased) from a server rejection (which is
/// a finding) from a retry-later shed (which is neither).
///
/// Write-shaped rather than reusing `ReadErrorKind`: that enum collapses every
/// server response into one `Server`, and the three write rejections that most
/// plausibly produce a cliff — `ReplicationBackpressure`, `InflightDuplicateWrite`
/// and `ClientIdempotencyViolation` — are all inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillWriteError {
    Transport,
    ConnectTimeout,
    RequestTimeout,
    Protocol,
    NotLeader,
    ServerBusy,
    /// `WriteError::ReplicationBackpressure` — the shard is shedding, documented
    /// as "retry later". Counted as an error because the workload does not
    /// retry it; naming it is what makes that a decision rather than an accident.
    Backpressure,
    /// `WriteError::InflightDuplicateWrite` — fsynced but not yet replicated.
    InflightDuplicate,
    /// `WriteError::ClientIdempotencyViolation`. In the fill's write loop this
    /// now means the *unexpected* kind only — a duplicate with no ambiguous
    /// attempt behind it. The benign retry-after-ambiguity case is progress and
    /// lands in `FillTotals::duplicate_acks`, so a non-zero count here is a
    /// finding rather than the harness retrying its own committed write.
    Idempotency,
    /// An OCC violation the server did not attach a current version to, so
    /// `occ_conflict_version` cannot resync and the write lands here instead of
    /// in `occ_retries`. Session-v1's reheat storm arrived by this door.
    OccNoVersion,
    ServerOther,
    /// The write read a response bound to a different request. Like its read-side
    /// twin this cannot be produced by load, partition or restart — it is proof
    /// of stream desync, and the one kind here that is never a survivorship
    /// artefact.
    CorrelationMismatch,
    Other,
}

impl FillWriteError {
    pub const ALL: [FillWriteError; 13] = [
        FillWriteError::Transport,
        FillWriteError::ConnectTimeout,
        FillWriteError::RequestTimeout,
        FillWriteError::Protocol,
        FillWriteError::NotLeader,
        FillWriteError::ServerBusy,
        FillWriteError::Backpressure,
        FillWriteError::InflightDuplicate,
        FillWriteError::Idempotency,
        FillWriteError::OccNoVersion,
        FillWriteError::ServerOther,
        FillWriteError::CorrelationMismatch,
        FillWriteError::Other,
    ];

    pub fn label(self) -> &'static str {
        match self {
            FillWriteError::Transport => "transport",
            FillWriteError::ConnectTimeout => "connect-timeout",
            FillWriteError::RequestTimeout => "request-timeout",
            FillWriteError::Protocol => "protocol",
            FillWriteError::NotLeader => "not-leader",
            FillWriteError::ServerBusy => "server-busy",
            FillWriteError::Backpressure => "replication-backpressure",
            FillWriteError::InflightDuplicate => "inflight-duplicate",
            FillWriteError::Idempotency => "client-idempotency",
            FillWriteError::OccNoVersion => "occ-without-version",
            FillWriteError::ServerOther => "server-other",
            FillWriteError::CorrelationMismatch => "correlation-mismatch",
            FillWriteError::Other => "other",
        }
    }

    pub fn of(e: &ClientError) -> Self {
        match e {
            ClientError::ConnectionFailed(_) | ClientError::WireError(_) | ClientError::ReadError(_) => {
                FillWriteError::Transport
            }
            ClientError::ConnectionTimeout => FillWriteError::ConnectTimeout,
            ClientError::RequestTimeout => FillWriteError::RequestTimeout,
            ClientError::ProtocolError => FillWriteError::Protocol,
            ClientError::NotLeader { .. } => FillWriteError::NotLeader,
            ClientError::ServerBusy => FillWriteError::ServerBusy,
            ClientError::Server(ServerError::Write { kind, .. }) => match kind {
                WriteError::ReplicationBackpressure => FillWriteError::Backpressure,
                WriteError::InflightDuplicateWrite { .. } => FillWriteError::InflightDuplicate,
                WriteError::ClientIdempotencyViolation { .. } => FillWriteError::Idempotency,
                WriteError::OptimisticConcurrencyViolation { .. } => FillWriteError::OccNoVersion,
                _ => FillWriteError::ServerOther,
            },
            ClientError::Server(_) => FillWriteError::ServerOther,
            ClientError::CorrelationMismatch { .. } => FillWriteError::CorrelationMismatch,
            _ => FillWriteError::Other,
        }
    }
}

/// Per-kind error tally. Fixed-width — one slot per `FillWriteError` — so the
/// fill's hot loop never allocates and the tally cannot grow with the run.
#[derive(Debug, Default, Clone, Copy)]
pub struct FillErrorTally([u64; FillWriteError::ALL.len()]);

impl FillErrorTally {
    pub fn record(&mut self, kind: FillWriteError) {
        self.0[kind as usize] += 1;
    }

    pub fn merge(&mut self, other: &FillErrorTally) {
        for (slot, add) in self.0.iter_mut().zip(other.0.iter()) {
            *slot += add;
        }
    }

    pub fn get(&self, kind: FillWriteError) -> u64 {
        self.0[kind as usize]
    }

    /// `kind=count` for every kind that fired, in enum order. Empty string when
    /// nothing failed — the caller prints nothing rather than "no errors".
    pub fn summary(&self) -> String {
        FillWriteError::ALL
            .iter()
            .filter(|k| self.get(**k) > 0)
            .map(|k| format!("{}={}", k.label(), self.get(*k)))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Counters the fill phase's monitor reads while the tasks run. Exact, because
/// every mint is a fresh id by construction — unlike `AckLedger::ack_offers`,
/// which is only an upper bound on distinct keys.
#[derive(Default)]
pub struct FillCounters {
    pub births: AtomicU64,
    pub clients: AtomicU64,
    pub writes_ok: AtomicU64,
    pub write_errors: AtomicU64,
    /// Live twin of `FillTotals::duplicate_acks`, so the progress line can show
    /// ambiguity volume while the fill runs rather than only at the end.
    pub duplicate_acks: AtomicU64,
    /// Live per-kind tally, so the 60s progress line can name the errors while
    /// the fill is still running instead of only at the end. A wedged run never
    /// reaches the end.
    by_kind: [AtomicU64; FillWriteError::ALL.len()],
}

impl FillCounters {
    pub fn record_error(&self, kind: FillWriteError) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
        self.by_kind[kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot of the live tally. Slots are read independently, so a
    /// concurrent increment can land between two of them — fine for a progress
    /// line, and the authoritative figures come from the joined `FillTotals`.
    pub fn tally(&self) -> FillErrorTally {
        let mut out = FillErrorTally::default();
        for (slot, atomic) in out.0.iter_mut().zip(self.by_kind.iter()) {
            *slot = atomic.load(Ordering::Relaxed);
        }
        out
    }
}

/// One simulated service replica for the whole fill: births, decay, and
/// age-stratified reheats interleaved on a single connection.
///
/// The reheat probe runs *here*, throughout the fill, rather than once against
/// a quiesced cluster — that is the more honest measurement, and it is the only
/// way to reach the older age buckets at all.
pub async fn run_fill_task(
    pool: Arc<Pool>,
    cfg: FillConfig,
    stop: Arc<AtomicBool>,
    counters: Arc<FillCounters>,
    curve: Arc<std::sync::Mutex<ReheatCostCurve>>,
    start: Instant,
) -> FillOutcome {
    let mut population = Population::new(population_config(cfg.task_id, cfg.tasks, cfg.budget, cfg.seed));
    let mut ledger = AckLedger::new(cfg.ledger_capacity, cfg.seed ^ cfg.task_id as u64);
    let mut rng = Rng::new(cfg.seed ^ ((cfg.task_id as u64) << 32));
    let mut latencies = LatencyReservoir::new(2048, cfg.seed ^ cfg.task_id as u64);
    let mut totals = FillTotals::default();
    let mut active: Vec<Active> = Vec::with_capacity(ACTIVE_CAP);

    // Stagger the first birth across tasks so a few thousand replicas do not
    // mint their first aggregate in the same instant.
    let mut next_birth_ms = cfg.birth_interval.map(|i| {
        (i.as_millis() as u64).saturating_mul(cfg.task_id as u64) / cfg.tasks.max(1) as u64
    });
    // Staggered by task_id like the birth process. Unstaggered, every task's
    // first reheat falls due at the same instant and `due + interval` keeps them
    // in permanent lockstep — at 3000 tasks that is a synchronised burst of the
    // most expensive operation in the system, and its queueing delay lands
    // directly in the reheat cost curve, which is the deliverable.
    let mut next_reheat_ms = cfg.reheat_interval.map(|i| {
        (i.as_millis() as u64).saturating_mul(cfg.task_id as u64) / cfg.tasks.max(1) as u64
    });
    let mut bucket_cursor = 0usize;
    let mut reheat_flavour = 0u32;

    while !stop.load(Ordering::Relaxed) {
        let now_ms = start.elapsed().as_millis() as u64;

        if let (Some(due), Some(interval)) = (next_birth_ms, cfg.birth_interval) {
            if now_ms >= due {
                let member = population.birth(now_ms);
                counters.births.fetch_add(1, Ordering::Relaxed);
                counters.clients.fetch_add(1, Ordering::Relaxed);
                if active.len() >= ACTIVE_CAP {
                    // Evict the most decayed: it is on its way to dormancy
                    // anyway, and that is the population the probe wants.
                    if let Some(i) = (0..active.len()).max_by_key(|&i| active[i].next_write_ms) {
                        active.swap_remove(i);
                    }
                }
                active.push(Active {
                    member,
                    version: 0,
                    client_seq: 1,
                    remaining: lifetime_budget(&mut rng),
                    next_write_ms: now_ms,
                    gap_ms: NURSERY_GAP_MS,
                    unacked: false,
                });
                next_birth_ms = Some(due + interval_ms_at_least_one(interval));
                continue;
            }
        }

        if let (Some(due), Some(interval)) = (next_reheat_ms, cfg.reheat_interval) {
            if now_ms >= due {
                next_reheat_ms = Some(due + interval_ms_at_least_one(interval));
                if let Some((bucket, member)) =
                    sample_reheat(&mut population, now_ms, &mut bucket_cursor, &active)
                {
                    let returning = reheat_flavour % 2 == 0;
                    reheat_flavour += 1;
                    match reheat_once(&pool, member, returning, reheat_nonce(cfg.task_id, reheat_flavour as u64), cfg.large_event_fraction, &mut rng).await {
                        Ok((micros, bytes, state)) => {
                            totals.reheats_ok += 1;
                            totals.writes_ok += 1;
                            counters.writes_ok.fetch_add(1, Ordering::Relaxed);
                            if !returning {
                                counters.clients.fetch_add(1, Ordering::Relaxed);
                            }
                            latencies.record(micros / 1000);
                            if let Ok(mut c) = curve.lock() {
                                c.record(bucket, micros, bytes);
                            }
                            ledger.record(
                                AckKey { aggregate_id: member.aggregate_id, client_id: state.client_id },
                                state.client_seq,
                            );
                            // Reheated aggregates go back to the nursery: they
                            // are hot again, now with a chain spanning a
                            // multi-hour gap in segment space.
                            if active.len() < ACTIVE_CAP {
                                active.push(Active {
                                    member: Member {
                                        aggregate_id: member.aggregate_id,
                                        client_id: state.client_id,
                                    },
                                    version: state.version,
                                    client_seq: state.client_seq + 1,
                                    remaining: REHEAT_BURST,
                                    next_write_ms: now_ms + NURSERY_GAP_MS,
                                    gap_ms: NURSERY_GAP_MS,
                                    unacked: false,
                                });
                            }
                        }
                        Err(e) => {
                            totals.reheat_errors += 1;
                            totals.errors_by_kind.record(FillWriteError::of(&e));
                            counters.record_error(FillWriteError::of(&e));
                            if let Ok(mut c) = curve.lock() {
                                c.record_error(bucket, ReadErrorKind::of(&e));
                            }
                        }
                    }
                }
                continue;
            }
        }

        let due_idx = (0..active.len())
            .filter(|&i| active[i].next_write_ms <= now_ms)
            .min_by_key(|&i| active[i].next_write_ms);
        let Some(i) = due_idx else {
            // Nothing due. Sleep to the next deadline rather than spinning; a
            // 5-hour fill on a build machine cannot afford a busy loop per task.
            let next = [
                next_birth_ms,
                next_reheat_ms,
                active.iter().map(|a| a.next_write_ms).min(),
            ]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(now_ms + 50);
            tokio::time::sleep(Duration::from_millis(next.saturating_sub(now_ms).clamp(1, 250))).await;
            continue;
        };

        let (major, statement_bytes) = pick_payload(&mut rng, cfg.large_event_fraction);
        let (member, version, client_seq) = (active[i].member, active[i].version, active[i].client_seq);
        let event = account_event(major, client_seq, rng.next_u64(), statement_bytes);
        let t0 = Instant::now();
        let res = pool
            .write_events_with(
                account_key(member.aggregate_id),
                vec![event],
                member.client_id,
                write_opts(Some(version)),
            )
            .await;
        match res {
            Ok(_) => {
                latencies.record(t0.elapsed().as_millis() as u64);
                totals.writes_ok += 1;
                counters.writes_ok.fetch_add(1, Ordering::Relaxed);
                ledger.record(
                    AckKey { aggregate_id: member.aggregate_id, client_id: member.client_id },
                    client_seq,
                );
                let a = &mut active[i];
                a.version += 1;
                a.advance(now_ms, client_seq);
                if a.remaining == 0 {
                    active.swap_remove(i);
                }
            }
            Err(e) => match classify_write_failure(&e, active[i].unacked) {
                FillRetry::Resync(current) => {
                    // Single writer per aggregate here, so a conflict means the
                    // local version drifted. Resync and retry immediately.
                    active[i].version = current;
                    totals.occ_retries += 1;
                }
                FillRetry::AlreadyCommitted(last) => {
                    // The retry after an ambiguous failure. The server has this
                    // seq, so the write is durable and the only thing stuck is
                    // us — advance exactly as `Ok` does. Holding `client_seq`
                    // here instead is what made this loop retry one committed
                    // write forever, at ~500/s for the length of the run.
                    //
                    // `version` needs no resync: OCC is checked before
                    // idempotency, so reaching this arm at all means the local
                    // version already matches the server's. If that ever fails
                    // to hold, the next attempt takes the `Resync` arm.
                    //
                    // Not recorded in `ledger`: an ack ledger entry is a claim
                    // this run witnessed durability, and here it did not.
                    totals.duplicate_acks += 1;
                    counters.duplicate_acks.fetch_add(1, Ordering::Relaxed);
                    let a = &mut active[i];
                    let committed = last.unwrap_or(a.client_seq).max(a.client_seq);
                    a.advance(now_ms, committed);
                    if a.remaining == 0 {
                        active.swap_remove(i);
                    }
                }
                FillRetry::UnexpectedDuplicate => {
                    // No ambiguous attempt behind this one, so the server holds
                    // a seq this task never sent twice. Named in the tally, and
                    // the aggregate is retired rather than retried: this task's
                    // `client_seq` for it is wrong and no retry fixes that.
                    totals.write_errors += 1;
                    totals.errors_by_kind.record(FillWriteError::Idempotency);
                    counters.record_error(FillWriteError::Idempotency);
                    active.swap_remove(i);
                }
                FillRetry::Backoff => {
                    totals.write_errors += 1;
                    totals.errors_by_kind.record(FillWriteError::of(&e));
                    counters.record_error(FillWriteError::of(&e));
                    active[i].defer(now_ms);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
        }
    }

    FillOutcome { population, ledger, latencies: latencies.into_samples(), totals }
}

/// Heavy-tailed lifetime write budget: 1..=64 with a uniform exponent, so most
/// aggregates are written a handful of times and a few are written far more.
fn lifetime_budget(rng: &mut Rng) -> u32 {
    1u32 << (rng.next_u64() % 7)
}

/// Round-robin across age buckets so the curve fills evenly, falling through to
/// the next bucket when the run has not aged into this one yet.
/// Draw a dormant aggregate, skipping any that is currently hot.
///
/// `sample_by_age` samples with replacement and never removes, so without this
/// filter an aggregate already resident in `active` can be drawn again and
/// pushed in a second time. Two entries for one aggregate then write under
/// different `client_id`s while each tracks its own `version`, and they
/// OCC-conflict with each other forever — a self-inflicted retry storm that
/// grows as reheats accumulate.
///
/// Measured before this filter existed: errors sat at exactly 0 for the first
/// 360s of a fill and then ran away — 1,747 → 8,005 → 17,679 → 28,832 — tracking
/// the moment aggregates aged into the first reheat bucket. 28,841 errors across
/// 1,260 reheats is ~23 failed writes per reheat, none of them the reheat itself.
///
/// It is also the right semantics regardless: reheating an aggregate that is
/// already hot does not measure a cold touch, so it would pollute the curve even
/// if every write succeeded.
/// Deadline advance in whole milliseconds, never zero.
///
/// The fill loop tracks deadlines as u64 millis, so any interval under 1ms
/// truncates to a non-advancing deadline. Both the birth and reheat branches
/// then `continue` with no `.await` in them, so the task stops yielding: it
/// mints at CPU speed instead of the configured rate and, with tasks >= worker
/// count, starves the scraper and host poller off the runtime entirely.
/// Reachable whenever the cluster-wide rate exceeds 1000 x tasks.
fn interval_ms_at_least_one(interval: std::time::Duration) -> u64 {
    (interval.as_millis() as u64).max(1)
}

fn sample_reheat(
    population: &mut Population,
    now_ms: u64,
    cursor: &mut usize,
    active: &[Active],
) -> Option<(AgeBucket, Member)> {
    const DRAW_ATTEMPTS: usize = 4;
    for _ in 0..DRAW_ATTEMPTS {
        for _ in 0..AgeBucket::ALL.len() {
            let bucket = AgeBucket::ALL[*cursor % AgeBucket::ALL.len()];
            *cursor += 1;
            if let Some(m) = population.sample_by_age(now_ms, bucket) {
                if !active.iter().any(|a| a.member.aggregate_id == m.aggregate_id) {
                    return Some((bucket, m));
                }
            }
        }
    }
    None
}

/// State a reheat leaves behind so the aggregate can rejoin the nursery.
struct ReheatState {
    client_id: u128,
    client_seq: u64,
    version: u64,
}

/// The primary probe: catch up on a dormant aggregate and write to it.
///
/// This is the worst case in the system and it hits the write path, not just
/// the read path — the snapshot is out of the LRU, the negative-lookup bloom
/// entry is evicted, and the segments are long gone from the 16-slot summary
/// cache. Timed end to end, because that whole cost is what a caller pays.
///
/// Two flavours, deliberately different code paths: the original client
/// returning forces a `client_seq` continuity check (exhaustive history walk),
/// while a new client touching an old aggregate puts the client bloom
/// short-circuit in play instead.
async fn reheat_once(
    pool: &Arc<Pool>,
    member: Member,
    returning_client: bool,
    new_client_nonce: u128,
    large_event_fraction: f64,
    rng: &mut Rng,
) -> Result<(u64, u64, ReheatState), ClientError> {
    let client_id = if returning_client {
        member.client_id
    } else {
        // Mixed with a per-reheat nonce, not a fixed constant. A constant makes
        // the "new client" id a pure function of the member, so a second reheat
        // of the same aggregate reuses it — which both inflates the distinct
        // client count and, worse, stops the probe being a FIRST write for that
        // client. The client bloom short-circuit is the whole reason this
        // flavour exists, and it is only in play on a genuinely new client.
        member.client_id
            ^ new_client_nonce.wrapping_mul(0xA076_1D64_78BD_642F)
            ^ 0xD6E8_FEB8_6659_FD93
    };
    let t0 = Instant::now();
    let (bytes, version, max_seq) = catch_up(pool, member.aggregate_id, client_id, 1).await?;
    let client_seq = max_seq + 1;
    let (major, statement_bytes) = pick_payload(rng, large_event_fraction);
    let event = account_event(major, client_seq, rng.next_u64(), statement_bytes);
    pool.write_events_with(
        account_key(member.aggregate_id),
        vec![event],
        client_id,
        write_opts(Some(version)),
    )
    .await?;
    Ok((
        t0.elapsed().as_micros() as u64,
        bytes,
        ReheatState { client_id, client_seq, version: version + 1 },
    ))
}

// ---------------------------------------------------------------------------
// Phases 3 and 5 — age-stratified reads
// ---------------------------------------------------------------------------

/// A fixed set of (bucket, member) pairs, drawn once at the end of the fill.
///
/// Phases 3 and 5 read the SAME keys at the SAME concurrency so the only thing
/// that changed between them is that a restart emptied every cache. Bucket
/// labels are the ages at draw time and stay fixed: re-bucketing by age at read
/// time would move keys between rows and the before/after delta would no longer
/// be comparing like with like.
pub type ReadPlan = Vec<Vec<(AgeBucket, Member)>>;

pub fn build_read_plan(populations: &mut [Population], now_ms: u64, per_bucket: usize) -> ReadPlan {
    populations
        .iter_mut()
        .map(|p| {
            let mut plan: Vec<(AgeBucket, Member)> = Vec::new();
            let mut chosen: std::collections::HashSet<u128> = std::collections::HashSet::new();
            for bucket in AgeBucket::ALL {
                let mut kept = 0usize;
                // Bounded retries rather than a while-loop: a cohort holding
                // fewer distinct members than `per_bucket` would otherwise spin
                // forever, and a short slice is a fine outcome.
                for _ in 0..per_bucket * DRAW_RETRIES {
                    if kept == per_bucket {
                        break;
                    }
                    let Some(m) = p.sample_by_age(now_ms, bucket) else { break };
                    // Distinct keys only. `sample_by_age` draws with replacement,
                    // and a duplicate is worse than a short slice: phase 5 reads
                    // the same plan as phase 3, so a key appearing twice is warm
                    // by its second read. That drags the COLD p50 down, which is
                    // the one direction that fakes a flat curve — the exact
                    // artefact this scenario exists to rule out.
                    if chosen.insert(m.aggregate_id) {
                        plan.push((bucket, m));
                        kept += 1;
                    }
                }
            }
            plan
        })
        .collect()
}

/// Draw attempts per wanted key when de-duplicating a read plan.
const DRAW_RETRIES: usize = 8;

/// Nonce for a new-client reheat, injective over the whole `(u32, u64)` domain
/// so no two reheats anywhere in the run can mint the same client id.
///
/// Returns `u128` rather than packing into `u64`: `task_id << 40` loses the top
/// 8 bits of a `u32`, so task 0xFFFFFFFF and task 0x00FFFFFF collide. Real task
/// counts never reach there, but a nonce whose uniqueness depends on nobody
/// passing a large id is not unique, it is lucky.
pub fn reheat_nonce(task_id: u32, ordinal: u64) -> u128 {
    ((task_id as u128) << 64) | ordinal as u128
}

/// Read every key in one task's slice of the plan, recording cost by bucket.
pub async fn run_read_probe_task(
    pool: Arc<Pool>,
    plan: Vec<(AgeBucket, Member)>,
    curve: Arc<std::sync::Mutex<ReheatCostCurve>>,
) {
    // One untimed round trip first. Phase 5 runs against a cluster that has just
    // restarted, so without this the TCP+TLS handshake and the shard redirect
    // would land inside the first measured sample and inflate the cold side of
    // the delta with a cost that has nothing to do with the segment path.
    //
    // The key is in this task's lane so the redirect settles on the right shard,
    // but is one nothing has ever written: warming a key from the plan would
    // populate the very cache phase 5 exists to measure cold.
    if let Some((_, first)) = plan.first() {
        let settle = settle_aggregate_id(first.aggregate_id % DATA_SHARDS as u128);
        let _ = celeriant_bench::read_max_aggregate_version(&pool, &account_key(settle)).await;
    }
    for (bucket, member) in plan {
        let t0 = Instant::now();
        match catch_up(&pool, member.aggregate_id, member.client_id, 1).await {
            Ok((bytes, _, _)) => {
                let micros = t0.elapsed().as_micros() as u64;
                if let Ok(mut c) = curve.lock() {
                    c.record(bucket, micros, bytes);
                }
            }
            Err(e) => {
                if let Ok(mut c) = curve.lock() {
                    c.record_error(bucket, ReadErrorKind::of(&e));
                }
            }
        }
    }
}

/// Ordinal for the routing-settle key. `Population::birth` mints
/// `((task_id as u128) << 64 | n) * DATA_SHARDS + lane`, so any ordinal above
/// the `u32 << 64 | u64` range it can reach is unreachable by the workload.
const SETTLE_ORDINAL: u128 = 1 << 100;

/// A never-written aggregate id routing to `lane`'s shard.
pub fn settle_aggregate_id(lane: u128) -> u128 {
    SETTLE_ORDINAL * DATA_SHARDS as u128 + lane
}

// ---------------------------------------------------------------------------
// Phases 2 and 6 — hot writers
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, Copy)]
pub struct HotWriterStats {
    pub ok: u64,
    pub errors: u64,
    pub occ_retries: u64,
    /// Highest `client_seq` this writer got acked, and the client it wrote as.
    ///
    /// Carried out so the contention phase can hand the checkers a real ack
    /// list. Without it that phase passes an empty slice, and every content
    /// oracle — monotonicity, final-read parity, payload round-trip — reports
    /// PASS having verified nothing, in the one window where R clients race on
    /// a single aggregate and duplicate acceptance could actually be caught.
    pub aggregate_id: u128,
    pub client_id: u128,
    pub max_acked_client_seq: u64,
}

pub struct HotWriterConfig {
    pub member: Member,
    /// Replica index. Each replica gets its own `client_id` and its own tokio
    /// task (hence its own pooled connection): R unsynchronised replicas on one
    /// account, which is what produces real OCC churn.
    pub replica: u32,
    pub process: u32,
    pub duration: Duration,
    pub seed: u64,
    pub large_event_fraction: f64,
}

/// The read-modify-write loop a real microservice runs: catch up, write under
/// OCC and idempotency, and on conflict re-read from the conflict version and
/// retry with the `client_seq` held constant.
///
/// Bounded in time and used only by phases 2 and 6, both of which record full
/// history — the fill cannot, because the recorder drops on a full 65536-slot
/// channel and `check_idempotency` fails closed on any drop.
pub async fn run_hot_writer(
    pool: Arc<Pool>,
    cfg: HotWriterConfig,
    history: Option<Arc<HistoryRecorder>>,
    availability: Option<Arc<AvailabilityClock>>,
) -> (HotWriterStats, Vec<u64>) {
    let client_id = cfg.member.client_id ^ ((cfg.replica as u128 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let key = account_key(cfg.member.aggregate_id);
    let mut max_acked_client_seq = 0u64;
    let mut rng = Rng::new(cfg.seed ^ ((cfg.replica as u64) << 40) ^ cfg.member.aggregate_id as u64);
    let mut latencies = LatencyReservoir::new(1024, cfg.seed ^ cfg.replica as u64);
    let mut stats = HotWriterStats::default();

    // Catch up before the first write, exactly as the service does on cold start.
    let (mut version, mut client_seq) =
        match catch_up(&pool, cfg.member.aggregate_id, client_id, 1).await {
            Ok((_, v, s)) => (v, s + 1),
            Err(_) => (0, 1),
        };

    let deadline = Instant::now() + cfg.duration;
    while Instant::now() < deadline {
        let (major, statement_bytes) = pick_payload(&mut rng, cfg.large_event_fraction);
        let event = account_event(major, client_seq, rng.next_u64(), statement_bytes);
        let t0 = Instant::now();
        let res = pool
            .write_events_with(key.clone(), vec![event], client_id, write_opts(Some(version)))
            .await;
        if let Some(h) = &history {
            h.record_op(
                cfg.process,
                &key,
                client_id,
                client_seq,
                Some(version),
                &res,
                tokio::time::Instant::from_std(t0),
            );
        }
        match res {
            Ok(_) => {
                latencies.record(t0.elapsed().as_millis() as u64);
                stats.ok += 1;
                max_acked_client_seq = max_acked_client_seq.max(client_seq);
                if let Some(a) = &availability {
                    a.record_ok();
                }
                version += 1;
                client_seq += 1;
            }
            Err(e) => {
                if let Some(current) = occ_conflict_version(&e) {
                    stats.occ_retries += 1;
                    // Re-read only the tail the conflict revealed — the same
                    // delta catch-up the service does — then retry the same
                    // client_seq.
                    match catch_up(&pool, cfg.member.aggregate_id, client_id, version + 1).await {
                        Ok((_, v, s)) => {
                            version = v.max(current);
                            client_seq = client_seq.max(s + 1);
                        }
                        Err(_) => version = current,
                    }
                    continue;
                }
                stats.errors += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    stats.aggregate_id = cfg.member.aggregate_id;
    stats.client_id = client_id;
    stats.max_acked_client_seq = max_acked_client_seq;
    (stats, latencies.into_samples())
}

// ---------------------------------------------------------------------------
// Phase 6 — failover timing, from a 10Hz window
// ---------------------------------------------------------------------------

/// One 10Hz observation of a node's role during the failover window.
///
/// The suite's 2Hz scraper cannot resolve a 1.6s budget: ±500ms of measurement
/// error against a 1600ms bound asserts nothing. This is the raised-rate window,
/// armed for the kill only — a five-hour fill does not need the volume.
#[derive(Debug, Clone)]
pub struct RoleSample {
    pub t_ms: u64,
    pub host: String,
    pub node_role: f64,
    pub ok: bool,
    /// Shards reporting a WAL sequence. Zero means the process answers HTTP but
    /// has not opened its shards yet, which is not "ready".
    pub shards_reporting: usize,
}

/// Survivor takes leadership. Expected to be cardinality-independent: the
/// promotion reconcile keys on the tip metablock's `node_id`, which is
/// tip-local work.
pub fn promotion_latency_ms(samples: &[RoleSample], killed_host: &str, _kill_ms: u64) -> Option<u64> {
    // Measured between two OBSERVATIONS, not from a stamped instant: the last
    // tick on which the dying node still held leadership, and the first tick on
    // which the survivor does.
    //
    // `kill_ms` is deliberately unused. It is stamped when the blocking
    // `make kill-*` ssh RETURNS, while the SIGKILL fires early inside that call,
    // so a >= kill_ms filter discards the real promotion tick and the first
    // surviving tick — where the survivor is already leader — reports 0ms. A
    // genuine 1.4s promotion came back as Some(0). Both endpoints here come from
    // the same 10Hz scrape, so the result is bounded by scrape resolution rather
    // than by an ssh round trip.
    let last_old_leader = samples
        .iter()
        .filter(|s| s.ok && s.host == killed_host && s.node_role >= 0.5)
        .map(|s| s.t_ms)
        .max()?;
    samples
        .iter()
        .filter(|s| s.ok && s.host != killed_host && s.node_role >= 0.5 && s.t_ms > last_old_leader)
        .map(|s| s.t_ms - last_old_leader)
        .min()
}

/// Killed node rejoins and serves. Expected to grow with segment size rather
/// than cardinality: `ShardWal::open` rebuilds the active segment's chain tips
/// before the node serves, which at 1GB is up to a gigabyte of metablock
/// scanning. Readiness here means the process is answering AND its shards are
/// reporting a WAL sequence — HTTP alone comes back well before that.
pub fn restart_ready_ms(samples: &[RoleSample], restarted_host: &str, _restart_ms: u64) -> Option<u64> {
    // Anchored on the observed outage, not on the stamped restart instant. The
    // node must first be seen DOWN — unreachable, or reachable with no shard
    // reporting a WAL sequence — and readiness is measured from that last down
    // tick. Measuring from `restart_ms` reported **60ms** on a real run, because
    // the dying process was still answering when the stamp was taken.
    //
    // `None` when the node was never observed down: nothing was measured, and
    // saying so beats reporting a number that describes the previous process.
    let last_down = samples
        .iter()
        .filter(|s| s.host == restarted_host && (!s.ok || s.shards_reporting == 0))
        .map(|s| s.t_ms)
        .max()?;
    samples
        .iter()
        .filter(|s| s.ok && s.host == restarted_host && s.shards_reporting > 0 && s.t_ms > last_down)
        .map(|s| s.t_ms - last_down)
        .min()
}

/// Parse `shard_1 12` lines from the per-shard segment count over ssh.
///
/// The coordinator shard is dropped: with `RESERVE_COORDINATOR_SHARD=true` it
/// holds no data and never rotates, so counting it would make
/// `RotationsReached` permanently inconclusive. Its raw count is still printed
/// by the caller so a run with the reserve turned off is visible.
pub fn parse_shard_segment_counts(text: &str) -> Vec<(u32, u64)> {
    let mut out: Vec<(u32, u64)> = text
        .lines()
        .filter_map(|l| {
            let (name, count) = l.trim().split_once(char::is_whitespace)?;
            let shard: u32 = name.trim().strip_prefix("shard_")?.parse().ok()?;
            (shard != COORDINATOR_SHARD_ID).then_some((shard, count.trim().parse().ok()?))
        })
        .collect();
    out.sort_unstable();
    out
}

/// Per-shard maximum across hosts. Both nodes hold a full copy, so a follower
/// mid-catchup must not drag the rotation count below what the cluster reached.
pub fn merge_shard_counts(per_host: &[Vec<(u32, u64)>]) -> Vec<(u32, u64)> {
    let mut merged: std::collections::BTreeMap<u32, u64> = std::collections::BTreeMap::new();
    for host in per_host {
        for (shard, n) in host {
            let e = merged.entry(*shard).or_insert(0);
            *e = (*e).max(*n);
        }
    }
    merged.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_err(kind: WriteError) -> ClientError {
        ClientError::Server(ServerError::Write { kind, error_message: String::new() })
    }

    #[test]
    fn every_failed_fill_write_is_named_by_a_kind_the_report_can_print() {
        // One row per way a fill write can fail. The three that matter most are
        // the retry-later rejections: the fill does not retry them, so they land
        // in `write_errors`, and until they were named an 80.5% error rate could
        // not be told apart from a client-side deadline.
        let cases: [(ClientError, FillWriteError); 12] = [
            (ClientError::ConnectionFailed(std::io::Error::from(std::io::ErrorKind::ConnectionReset)), FillWriteError::Transport),
            (ClientError::ConnectionTimeout, FillWriteError::ConnectTimeout),
            (ClientError::RequestTimeout, FillWriteError::RequestTimeout),
            (ClientError::ProtocolError, FillWriteError::Protocol),
            (ClientError::NotLeader { leader_address: None, error_message: String::new() }, FillWriteError::NotLeader),
            (ClientError::ServerBusy, FillWriteError::ServerBusy),
            (write_err(WriteError::ReplicationBackpressure), FillWriteError::Backpressure),
            (write_err(WriteError::InflightDuplicateWrite { last_client_seq: None, attempted_client_seq: None }), FillWriteError::InflightDuplicate),
            (write_err(WriteError::ClientIdempotencyViolation { last_client_seq: None, attempted_client_seq: None }), FillWriteError::Idempotency),
            (write_err(WriteError::OptimisticConcurrencyViolation { expected_version: Some(3), current_aggregate_version: None }), FillWriteError::OccNoVersion),
            (write_err(WriteError::FsyncError), FillWriteError::ServerOther),
            // Never the catch-all: this is the one kind here that no injected
            // fault can produce, so it must stay distinguishable.
            (ClientError::CorrelationMismatch { sent: Some(1), received: Some(2) }, FillWriteError::CorrelationMismatch),
        ];

        let mut tally = FillErrorTally::default();
        for (err, want) in &cases {
            assert_eq!(FillWriteError::of(err), *want, "{err}");
            tally.record(*want);
        }

        // Distinct labels, or two causes render as one line in the report.
        let mut labels: Vec<&str> = FillWriteError::ALL.iter().map(|k| k.label()).collect();
        labels.sort_unstable();
        let before = labels.len();
        labels.dedup();
        assert_eq!(labels.len(), before, "two kinds share a label");

        // Only what fired, and every count is 1 here.
        let summary = tally.summary();
        assert!(!summary.contains("occ-without-version=0"), "{summary}");
        assert!(summary.contains("replication-backpressure=1"), "{summary}");
        assert_eq!(FillErrorTally::default().summary(), "", "a clean fill prints no breakdown");
    }

    fn nursery(client_seq: u64, remaining: u32) -> Active {
        Active {
            member: Member { aggregate_id: 7, client_id: 9 },
            version: 10,
            client_seq,
            remaining,
            next_write_ms: 0,
            gap_ms: NURSERY_GAP_MS,
            unacked: false,
        }
    }

    #[test]
    fn a_failed_write_is_classified_by_what_it_says_about_the_commit() {
        let occ_current = WriteError::OptimisticConcurrencyViolation {
            expected_version: Some(10),
            current_aggregate_version: Some(11),
        };
        let occ_bare = WriteError::OptimisticConcurrencyViolation {
            expected_version: Some(10),
            current_aggregate_version: None,
        };
        let dup = || WriteError::ClientIdempotencyViolation {
            last_client_seq: Some(5),
            attempted_client_seq: Some(5),
        };
        let cases: [(&str, ClientError, bool, FillRetry); 6] = [
            ("occ with a version resyncs", write_err(occ_current), false, FillRetry::Resync(11)),
            ("occ without one is opaque", write_err(occ_bare), true, FillRetry::Backoff),
            ("duplicate after ambiguity is a commit", write_err(dup()), true, FillRetry::AlreadyCommitted(Some(5))),
            ("duplicate on a first attempt is a finding", write_err(dup()), false, FillRetry::UnexpectedDuplicate),
            ("transport says nothing", ClientError::RequestTimeout, true, FillRetry::Backoff),
            ("backpressure says nothing", write_err(WriteError::ReplicationBackpressure), true, FillRetry::Backoff),
        ];
        for (name, err, unacked, want) in &cases {
            assert_eq!(classify_write_failure(err, *unacked), *want, "{name}");
        }
    }

    #[test]
    fn a_duplicate_after_an_ambiguous_failure_progresses_instead_of_spinning() {
        // The measured storm: one committed write, retried ~500/s forever.
        let mut a = nursery(5, 3);

        // 1. The write commits on the server; the ack never arrives.
        let lost = ClientError::ConnectionFailed(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        ));
        assert_eq!(classify_write_failure(&lost, a.unacked), FillRetry::Backoff);
        a.defer(0);
        assert_eq!(a.client_seq, 5, "a retry must hold client_seq constant");
        assert!(a.unacked, "the ambiguity has to be remembered or the fix cannot fire");

        // 2. The retry trips OCC first — the server checks it before
        //    idempotency — and resyncs the version it names.
        let occ = write_err(WriteError::OptimisticConcurrencyViolation {
            expected_version: Some(10),
            current_aggregate_version: Some(11),
        });
        assert_eq!(classify_write_failure(&occ, a.unacked), FillRetry::Resync(11));
        a.version = 11;

        // 3. Now the server says "already applied". Before the fix this was a
        //    hard error: re-arm at +100ms, same seq, same version, forever.
        let dup = write_err(WriteError::ClientIdempotencyViolation {
            last_client_seq: Some(5),
            attempted_client_seq: Some(5),
        });
        assert_eq!(classify_write_failure(&dup, a.unacked), FillRetry::AlreadyCommitted(Some(5)));
        a.advance(100, 5);

        assert_eq!(a.client_seq, 6, "the committed seq must be passed, not re-sent");
        assert!(!a.unacked);
        assert_eq!(a.remaining, 2, "a durable write spends a unit of the lifetime budget");
        assert_eq!(a.next_write_ms, 100 + NURSERY_GAP_MS * 2, "decayed, not re-armed at the retry gap");
        assert_eq!(a.version, 11, "resynced from the server, never guessed");
    }

    #[test]
    fn the_server_last_seq_wins_when_it_is_ahead_of_the_local_one() {
        // More than one attempt landed before the client noticed. Advancing by
        // one would leave the next write duplicate-rejected all over again.
        let mut a = nursery(5, 4);
        a.advance(0, 8);
        assert_eq!(a.client_seq, 9);
    }

    #[test]
    fn a_budget_spent_by_a_duplicate_retires_the_aggregate() {
        // The slot has to come back either way, or a stuck aggregate holds one
        // of ACTIVE_CAP for the rest of the run.
        let mut a = nursery(5, 1);
        a.advance(0, 5);
        assert_eq!(a.remaining, 0, "the loop retires an entry at remaining == 0");
    }

    #[test]
    fn merging_task_tallies_sums_slot_by_slot() {
        let (mut a, mut b) = (FillErrorTally::default(), FillErrorTally::default());
        a.record(FillWriteError::RequestTimeout);
        a.record(FillWriteError::Backpressure);
        b.record(FillWriteError::Backpressure);
        a.merge(&b);
        assert_eq!(a.get(FillWriteError::Backpressure), 2);
        assert_eq!(a.get(FillWriteError::RequestTimeout), 1);
        assert_eq!(a.get(FillWriteError::Transport), 0);
    }

    #[test]
    fn the_settle_key_routes_to_its_lane_and_is_never_written() {
        for lane in 0..DATA_SHARDS as u128 {
            assert_eq!(settle_aggregate_id(lane) % DATA_SHARDS as u128, lane);
        }

        // `Population::birth` mints `((task_id as u128) << 64 | n) * shards + lane`.
        // A settle key inside that ordinal range would collide with a real
        // aggregate and warm the cache phase 5 exists to measure cold.
        let max_minted_ordinal = ((u32::MAX as u128) << 64) | u64::MAX as u128;
        assert!(SETTLE_ORDINAL > max_minted_ordinal);
        assert!(settle_aggregate_id(DATA_SHARDS as u128 - 1) < u128::MAX);
    }

    #[test]
    fn presets_map_to_the_documented_budgets() {
        assert_eq!(Preset::parse("smoke").unwrap().fill_budget(), Duration::from_secs(600));
        assert_eq!(Preset::parse("short").unwrap().fill_budget(), Duration::from_secs(3600));
        assert_eq!(Preset::parse("deep").unwrap().fill_budget(), Duration::from_secs(18_000));
        assert!(Preset::parse("overnight").is_err());

        // An explicit --fill-duration wins over the preset.
        let p = CardinalityParams { fill_duration: Some(Duration::from_secs(42)), ..Default::default() };
        assert_eq!(p.fill_budget(), Duration::from_secs(42));
        assert_eq!(CardinalityParams::default().fill_budget(), Duration::from_secs(600));
    }

    #[test]
    fn tasks_must_divide_evenly_across_the_data_shards() {
        // Not style: a leftover task adds a second replica to one id lane, so
        // that executor carries more of the population than the others.
        assert!(validate_tasks(3).is_ok());
        assert!(validate_tasks(3000).is_ok());
        assert!(validate_tasks(0).is_err());
        assert!(validate_tasks(4000).is_err());
        assert!(validate_tasks(1).is_err());
    }

    #[test]
    fn the_birth_rate_is_cluster_wide_however_many_connections_run() {
        // The whole point of the rule: two runs at wildly different connection
        // counts must mint at the same cluster rate, or their reheat curves are
        // not comparable and the connection dial stops being an A/B.
        for tasks in [3usize, 300, 3000, 16_000] {
            let i = per_task_interval(30.0, tasks).unwrap();
            let cluster_rate = tasks as f64 / i.as_secs_f64();
            assert!((cluster_rate - 30.0).abs() < 1e-6, "tasks={tasks} rate={cluster_rate}");
        }
        // A task count that doubles doubles the per-task interval.
        assert_eq!(per_task_interval(30.0, 30).unwrap(), Duration::from_secs(1));
        assert_eq!(per_task_interval(30.0, 60).unwrap(), Duration::from_secs(2));
        // Zero, negative or NaN disables the process rather than dividing by
        // zero or panicking inside `from_secs_f64`.
        assert!(per_task_interval(0.0, 300).is_none());
        assert!(per_task_interval(-1.0, 300).is_none());
        assert!(per_task_interval(f64::NAN, 300).is_none());
        assert!(per_task_interval(30.0, 0).is_none());
        // A rate small enough to overflow the duration clamps instead.
        assert_eq!(per_task_interval(f64::MIN_POSITIVE, 300), Some(Duration::from_secs(86_400)));
    }

    #[test]
    fn the_fill_stops_on_whichever_limit_arrives_first() {
        let budget = Duration::from_secs(600);
        assert!(fill_stop(Duration::from_secs(100), budget, 20, 70).is_none());
        assert_eq!(fill_stop(Duration::from_secs(600), budget, 20, 70), Some(FillStop::TimeBudget));
        // Space runs out long before the clock: a clean, reported outcome.
        assert_eq!(
            fill_stop(Duration::from_secs(100), budget, 70, 70),
            Some(FillStop::DiskHighWater(70))
        );
        assert_eq!(
            fill_stop(Duration::from_secs(100), budget, 91, 70),
            Some(FillStop::DiskHighWater(91))
        );
        // Both at once reports the space figure — that is what constrains the
        // next run's budget.
        assert_eq!(
            fill_stop(Duration::from_secs(700), budget, 80, 70),
            Some(FillStop::DiskHighWater(80))
        );
        // A zero high-water disarms the watchdog rather than stopping instantly.
        assert!(fill_stop(Duration::from_secs(1), budget, 0, 0).is_none());
    }

    #[test]
    fn the_payload_mix_is_derived_and_the_achieved_density_is_reported() {
        // goal.md's own sizing table: 256MB wants ~4% large events to hit the
        // 200k design point, 1GB wants ~53%.
        let small = CardinalityParams { segment_bytes: 256 * 1024 * 1024, ..Default::default() };
        let big = CardinalityParams { segment_bytes: 1024 * 1024 * 1024, ..Default::default() };
        assert!((small.large_event_fraction() - 0.039).abs() < 0.005, "{}", small.large_event_fraction());
        assert!((big.large_event_fraction() - 0.53).abs() < 0.01, "{}", big.large_event_fraction());
        // Both land back on the design point, which is the property that makes
        // the two segment sizes comparable at all.
        for p in [small, big] {
            let achieved = p.achieved_aggs_per_segment();
            assert!(achieved.abs_diff(200_000) < 1_000, "{achieved}");
        }
    }

    #[test]
    fn population_retention_stays_bounded_as_tasks_multiply() {
        for tasks in [3usize, 300, 3000] {
            let cfg = population_config(0, tasks, Duration::from_secs(5 * 3600), 1);
            let cohorts = cfg.max_tracked_age.as_secs() / cfg.cohort_slice.as_secs();
            let retained = tasks * cohorts as usize * cfg.reservoir_per_cohort;
            assert!(retained <= 4 * TARGET_RETAINED_MEMBERS, "tasks={tasks} retained={retained}");
            assert!(cfg.reservoir_per_cohort >= 4);
            assert_eq!(cfg.data_shards, DATA_SHARDS);
        }
    }

    #[test]
    fn the_availability_gap_is_measured_across_the_kill_not_around_it() {
        let origin = Instant::now();
        let clock = AvailabilityClock::new(origin);
        // No kill yet: no gap to report, and it must not read as zero.
        clock.record_ok();
        assert!(clock.gap().is_none());

        clock.mark_kill();
        // Still nothing after the kill — an unfinished window is not a 0ms gap.
        assert!(clock.gap().is_none());

        std::thread::sleep(Duration::from_millis(5));
        clock.record_ok();
        let gap = clock.gap().expect("both sides recorded");
        assert!(gap >= Duration::from_millis(4), "{gap:?}");
        // Later acks do not widen an already-measured gap.
        std::thread::sleep(Duration::from_millis(5));
        clock.record_ok();
        assert_eq!(clock.gap(), Some(gap));
    }

    #[test]
    fn the_latency_reservoir_is_bounded_but_keeps_counting() {
        let mut r = LatencyReservoir::new(100, 7);
        for v in 0..10_000u64 {
            r.record(v);
        }
        assert_eq!(r.count(), 10_000);
        let s = r.into_samples();
        assert_eq!(s.len(), 100);
        assert!(s.iter().any(|&v| v < 2_000), "no early samples retained");
        assert!(s.iter().any(|&v| v >= 8_000), "no late samples retained");
    }

    fn role(t_ms: u64, host: &str, node_role: f64, shards: usize) -> RoleSample {
        RoleSample { t_ms, host: host.into(), node_role, ok: true, shards_reporting: shards }
    }

    #[test]
    fn failover_timings_are_derived_from_observations_not_from_stamped_instants() {
        // Both figures used to be measured from the orchestrator's stamped kill
        // and restart instants. Those stamps come from a BLOCKING ssh that
        // returns long after the signal lands, so the anchored form reported
        // Some(0) for a genuine 1400ms promotion and 60ms for a real restart.
        // Both endpoints now come from the same 10Hz scrape, which bounds the
        // error at one scrape interval instead of one ssh round trip.
        //
        // The fixture keeps cs1 leader samples right up to the kill, as a real
        // scrape would; a sparse fixture would flatter whichever form is used.
        let samples = vec![
            role(0, "cs1", 1.0, 3),
            role(0, "cs2", 0.0, 3),
            role(900, "cs1", 1.0, 3), // last tick the dying leader still led
            // killed somewhere after 900; the ssh returns at 1550
            role(1100, "cs2", 0.0, 3),
            role(1400, "cs2", 1.0, 3), // survivor leads: 500ms after 900
            role(1900, "cs2", 1.0, 3),
            // cs1 restarted: HTTP answers before the shards open, and answering
            // HTTP is not being ready.
            RoleSample { t_ms: 2200, host: "cs1".into(), node_role: 0.0, ok: true, shards_reporting: 0 },
            role(2600, "cs1", 0.0, 3),
        ];

        // The stamped instant is now irrelevant: wildly late, early, and exact
        // must all agree.
        for stamp in [0, 1000, 1550, 99_999] {
            assert_eq!(promotion_latency_ms(&samples, "cs1", stamp), Some(500), "stamp {stamp}");
            assert_eq!(restart_ready_ms(&samples, "cs1", stamp), Some(400), "stamp {stamp}");
        }

        // Leadership the survivor held BEFORE the kill is not a promotion.
        assert_eq!(promotion_latency_ms(&samples, "cs2", 1000), None);

        // An unfinished window reports nothing rather than zero — expressed by
        // truncating the observations, since the stamp no longer gates it.
        let unfinished = &samples[..4];
        assert_eq!(promotion_latency_ms(unfinished, "cs1", 1000), None);
        assert_eq!(restart_ready_ms(unfinished, "cs1", 2000), None);
    }

    #[test]
    fn shard_counts_drop_the_coordinator_and_take_the_cluster_maximum() {
        let leader = parse_shard_segment_counts("shard_0 1\nshard_1 14\nshard_2 12\nshard_3 11\n");
        assert_eq!(leader, vec![(1, 14), (2, 12), (3, 11)]);
        // A follower mid-catchup must not drag the count below what the
        // cluster actually reached.
        let follower = parse_shard_segment_counts("shard_1 9\nshard_2 12\nshard_3 13\n");
        assert_eq!(merge_shard_counts(&[leader, follower]), vec![(1, 14), (2, 12), (3, 13)]);
        // Garbage lines are dropped, not guessed at.
        assert!(parse_shard_segment_counts("no such file\n\n").is_empty());
    }

    #[test]
    fn the_payload_mix_lands_on_the_requested_fraction() {
        let mut rng = Rng::new(99);
        let large = (0..10_000).filter(|_| pick_payload(&mut rng, 0.25).0 == MAJOR_STATEMENT_ATTACHED).count();
        assert!((2_200..2_800).contains(&large), "{large}");
        // A zero fraction must never take the datablock path.
        let mut rng = Rng::new(1);
        assert!((0..1_000).all(|_| pick_payload(&mut rng, 0.0).0 != MAJOR_STATEMENT_ATTACHED));
    }
}

#[cfg(test)]
mod availability_tests {
    use super::*;

    /// The anchored gap collapses when the kill instant is stamped after the
    /// blocking ssh, because acks served during the outage are classified as
    /// "before". The anchor-free gap must survive that, since it is the number
    /// the report leads with.
    #[test]
    fn the_max_ack_gap_survives_a_kill_instant_stamped_too_late() {
        let origin = Instant::now();
        let clock = AvailabilityClock::new(origin);

        // Two acks, then a long silence, then recovery acks — all recorded
        // BEFORE the kill is marked, exactly as a late ssh return produces.
        clock.record_at_us(1_000);
        clock.record_at_us(2_000);
        clock.record_at_us(900_000); // the outage
        clock.record_at_us(901_000);
        clock.mark_kill();

        let max = clock.max_ack_gap().expect("two acks landed");
        assert!(
            max >= Duration::from_micros(890_000),
            "anchor-free gap lost the outage: {max:?}"
        );
        // The anchored number is the one that goes blind here; it is reported
        // only for comparison.
        assert!(clock.gap().is_none() || clock.gap().unwrap() < max);
    }

    #[test]
    fn a_single_ack_reports_no_gap_rather_than_zero() {
        let clock = AvailabilityClock::new(Instant::now());
        assert_eq!(clock.max_ack_gap(), None);
        clock.record_at_us(5_000);
        assert_eq!(clock.max_ack_gap(), None, "one ack has no predecessor to measure against");
        clock.record_at_us(6_500);
        assert_eq!(clock.max_ack_gap(), Some(Duration::from_micros(1_500)));
    }
}
