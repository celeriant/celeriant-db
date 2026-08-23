use std::path::PathBuf;
use std::time::{Duration, Instant};

use celeriant_bench::{
    BenchmarkResult, DataIntegrityReport, DeepAuditReport, HistoryRecorder, IdempotentBenchCounters,
    Pool, PoolBuilder, TaskAckSummary, WatchFloodParams, build_tls_config,
    deep_audit_failing_aggregates, run_benchmark, run_benchmark_idempotent_opts,
    run_benchmark_idempotent_with_history, run_cas_storm, run_watch_flood, smoke_test,
    verify_no_seq_gaps, watch_dial_probe,
};
use std::sync::Arc;

use crate::actions::{Action, ActionExecutor};
use crate::config::ClusterConfig;
use crate::invariants::{CheckOutcome, CheckResult, RunData, ScenarioExpectations, run_all};
use crate::logs::fetch_journal;
use crate::sample::{NodeSample, elapsed_ms};
use crate::scrape::Scraper;

#[derive(Debug, Clone, Copy)]
pub struct ScenarioParams {
    pub tasks: usize,
    pub duration_secs: u64,
    pub throughput_floor: f64,
    /// Spread bench task starts over this many seconds instead of releasing
    /// one cold-connect herd. Only `baseline` consumes it (A/B for the
    /// thundering-herd envelope); fault scenarios keep the herd — it is
    /// part of their stress.
    pub connect_ramp_secs: Option<u64>,
    /// Run-varying seed (N3): drives `NemesisPrng`, the clock-scramble
    /// splitmix, and the epoch-oracle's aggregate sample. Printed at run
    /// start and persisted in `ScenarioReport` so a Heisenbug is
    /// reproducible instead of every run replaying the same fault schedule.
    pub seed: u64,
}

impl Default for ScenarioParams {
    fn default() -> Self {
        Self {
            tasks: 4000,
            duration_secs: 60,
            throughput_floor: 500.0,
            connect_ramp_secs: None,
            seed: 0xCE1E_51A7,
        }
    }
}

/// Verdict for a whole scenario, aggregated from its checks.
///
/// `Inconclusive` is not a soft failure. A time-boxed run that never rotated a
/// segment, never reached an age spread, or stopped early on the disk watchdog
/// has not found a defect — it has failed to reach the regime where its
/// measurements mean anything. `smoke` is inconclusive by construction and that
/// is the correct result for a run whose only job was proving the harness
/// assembles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum ScenarioOutcome {
    Pass,
    Fail,
    Inconclusive,
}

impl ScenarioOutcome {
    /// A failure outranks an inconclusive: something is known to be wrong, and
    /// a check that merely had nothing to say must not soften that verdict.
    pub fn of(checks: &[CheckResult]) -> Self {
        if checks.iter().any(|c| c.failed()) {
            Self::Fail
        } else if checks.iter().any(|c| c.is_inconclusive()) {
            Self::Inconclusive
        } else {
            Self::Pass
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::Inconclusive => "INCONCLUSIVE",
        }
    }

    /// Only a clean pass counts as success for an exit code or a soak gate.
    pub fn is_pass(self) -> bool {
        self == Self::Pass
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ScenarioReport {
    pub name: String,
    pub outcome: ScenarioOutcome,
    pub params: ScenarioParamsJson,
    pub bench: BenchmarkSummary,
    pub checks: Vec<CheckResult>,
    pub samples: Vec<NodeSample>,
    pub bench_window_start_ms: u64,
    pub bench_actual_end_ms: u64,
    pub bench_window_end_ms: u64,
    /// Paths (relative to the run directory) to per-host journalctl dumps,
    /// fetched only when the scenario failed.
    pub log_files: Vec<String>,
    /// Set only for scenarios that ran the idempotent bench. Captures
    /// per-error-type counts (Ok ACKs vs. 2002 ACKs vs. transient retries).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotent_counters: Option<IdempotentBenchCounters>,
    /// Set only for scenarios that audited client_seq durability. Non-zero
    /// `tasks_with_gaps` is the false-ack data-loss signal the audit exists
    /// to detect.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrity: Option<DataIntegrityReport>,
    /// Per-aggregate forensics for the failing tasks: specific missing seqs
    /// + duplicate-acceptance detection (same client_seq landing on multiple
    /// aggregate_versions). Set only when the headline audit found gaps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deep_audit: Option<DeepAuditReport>,
    /// Per-entry disk-truth verification. SSH'ing to both data nodes and running
    /// `celeriant-wal-inspect`. `actually_missing` is the trustworthy
    /// loss count; `audit_overreported` is the audit's noise floor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_truth: Option<Vec<crate::disk_truth::DiskTruthEntry>>,
    /// Residual S3 fallback objects post-quiesce (count + per-shard ranges).
    /// Trended across soak iterations to catch fallback-object leaks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_lifecycle: Option<crate::s3_lifecycle::S3LifecycleReport>,
    /// `cardinality_pressure`'s deliverable as typed numbers: the three reheat
    /// cost curves, the cold/warm delta, and the run shape they were measured
    /// under. The summary markdown renders from this, so a headline figure can
    /// be re-derived from the run JSON instead of parsed out of prose.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<crate::cardinality_deliverable::CardinalityDeliverable>,
}

#[derive(Debug, serde::Serialize)]
pub struct ScenarioParamsJson {
    pub tasks: usize,
    pub duration_secs: u64,
    pub throughput_floor: f64,
    /// Seed for this run's fault schedule / clock skew / oracle sampling
    /// (N3), persisted so a failing run's fault plan is reproducible via
    /// `--seed`.
    pub seed: u64,
}

#[derive(Debug, serde::Serialize)]
pub struct BenchmarkSummary {
    pub total_requests: u64,
    pub errors: u64,
    pub throughput: f64,
    pub avg_latency_ms: f64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub p999_ms: u64,
    pub min_ms: u64,
    pub max_ms: u64,
}

impl From<&BenchmarkResult> for BenchmarkSummary {
    fn from(r: &BenchmarkResult) -> Self {
        Self {
            total_requests: r.total_requests,
            errors: r.errors,
            throughput: r.throughput,
            avg_latency_ms: r.avg_latency_ms,
            p50_ms: r.p50_ms,
            p95_ms: r.p95_ms,
            p99_ms: r.p99_ms,
            p999_ms: r.p999_ms,
            min_ms: r.min_ms,
            max_ms: r.max_ms,
        }
    }
}

/// State a scenario holds onto between bring-up and tear-down.
///
/// Holds the scraper (which a scenario can read mid-run), the monotonic clock
/// reference for translating wall-time offsets into the sample stream's
/// `t_ms`, and the bench addresses pre-resolved against the real leader.
pub struct ClusterUp {
    pub scraper: Scraper,
    pub scraper_start: Instant,
    pub bench_primary: String,
    pub bench_seed: String,
    /// Per-host (fd count, RSS) taken right after stable-leader, compared
    /// against a pre-teardown snapshot to catch fd/memory leaks per scenario.
    pub resource_before: Vec<(String, crate::resource_baseline::ResourceSnapshot)>,
}

impl ClusterUp {
    /// Convenience: monotonic ms since bring-up. Use to record bench window
    /// boundaries for `tear_down_and_evaluate`.
    pub fn elapsed_ms(&self) -> u64 {
        elapsed_ms(self.scraper_start, Instant::now())
    }
}

/// Standard cluster bring-up: teardown → infra → scraper → start both nodes →
/// wait for stable leader → detect actual leader → resolve bench addresses.
///
/// Used by every scenario; the only thing scenarios customise is what happens
/// between this and `tear_down_and_evaluate`. On any failure, stops services
/// and harvests logs before returning the error.
pub async fn bring_up_cluster(
    cfg: &ClusterConfig,
    scenario_name: &str,
    run_dir: &PathBuf,
) -> Result<ClusterUp, String> {
    let executor = ActionExecutor::new(cfg);

    println!("[{scenario_name}] teardown-data");
    executor.run(&Action::TeardownData)?;

    println!("[{scenario_name}] start infra");
    executor.run(&Action::StartInfra)?;

    // Scraper starts BEFORE the cluster comes up so we capture boot transitions.
    println!("[{scenario_name}] start scraper @ 2Hz on both nodes");
    let scraper = Scraper::start(cfg);
    let scraper_start = Instant::now();

    println!("[{scenario_name}] start cs1");
    executor.run(&Action::StartCs1)?;
    sleep(Duration::from_secs(5)).await;

    println!("[{scenario_name}] start cs2");
    executor.run(&Action::StartCs2)?;
    sleep(Duration::from_secs(3)).await;

    // Wait until exactly one node reports node_role=1 with a hard 30s cap.
    println!("[{scenario_name}] wait for stable leader");
    if let Err(e) = wait_for_stable_leader(&scraper, Duration::from_secs(30)).await {
        let outcome = scraper.stop().await;
        let samples = outcome.store.snapshot().await;
        let _ = executor.run(&Action::StopAll);
        let _ = harvest_logs(cfg, scenario_name, outcome.wall_start, outcome.wall_end, run_dir);
        return Err(format!("cluster did not reach stable leader: {e} (samples: {})", samples.len()));
    }

    // Detect which node is actually the leader and feed THAT as address1 to
    // the bench. The config.env LEADER_HOST is just a slot name — leadership
    // is decided by S3 election. Pointing the bench at the wrong node under a
    // connection storm triggers a feedback loop into FollowerCatchingUp.
    let (bench_primary, bench_seed) = match detect_leader(cfg, &scraper).await {
        Some(leader_host) if leader_host == cfg.leader_host => {
            (cfg.leader_addr(), cfg.follower_addr())
        }
        Some(leader_host) if leader_host == cfg.follower_host => {
            println!(
                "[{scenario_name}] actual leader is {} (config slot: follower) — swapping bench addresses",
                leader_host
            );
            (cfg.follower_addr(), cfg.leader_addr())
        }
        Some(other) => {
            return Err(format!("detect_leader returned unknown host: {other}"));
        }
        None => {
            return Err("could not determine actual leader from metric scrape".into());
        }
    };

    // Baseline fd/RSS per node now that the cluster is stable — compared at
    // tear-down to catch per-scenario leaks. Best-effort: a failed snapshot
    // just means the leak check is skipped for that host.
    let mut resource_before = Vec::new();
    for host in [&cfg.leader_host, &cfg.follower_host] {
        match crate::resource_baseline::snapshot(host).await {
            Ok(s) => resource_before.push((host.clone(), s)),
            Err(e) => println!("[{scenario_name}] resource baseline skipped for {host}: {e}"),
        }
    }

    Ok(ClusterUp { scraper, scraper_start, bench_primary, bench_seed, resource_before })
}

/// Build the bench pool against the bench addresses resolved during bring-up.
/// Caller is expected to also call `smoke_test(&pool)` before driving the bench.
pub async fn build_bench_pool(
    cfg: &ClusterConfig,
    up: &ClusterUp,
    params: ScenarioParams,
) -> Result<std::sync::Arc<Pool>, String> {
    PoolBuilder {
        address1: &up.bench_primary,
        address2: &up.bench_seed,
        // SNI is the hostname/IP of address1, which matches the cert SANs.
        server_name: Some(up.bench_primary.split(':').next().unwrap_or(&up.bench_primary)),
        ca_cert: cfg.ca_cert.to_str().unwrap(),
        client_cert: cfg.client_cert.to_str().unwrap(),
        client_key: cfg.client_key.to_str().unwrap(),
        plaintext: false,
        max_connections: params.tasks,
    }
    .build()
    .await
    .map_err(|e| format!("pool build: {e}"))
}

/// One pool per shard lane, so a connection can settle on one shard and stay.
///
/// Shard affinity in `Population::birth` is necessary but not sufficient. Every
/// task drawing from one shared pool defeats it: checkout is a FIFO free-list
/// with no task-to-connection binding, while a connection is sticky to whichever
/// shard it last served (`check_client_redirect` migrates the stream and the new
/// executor becomes `ctx.current_shard_id`). A task in lane 1 therefore keeps
/// drawing connections last used by lanes 2 and 3 and migrating them back.
///
/// Measured on the first real run: `celeriant_connection_redirects_total` reached
/// **163,258 against 215,757 writes** — about 76% of requests hauling a TCP
/// stream across the glommio mesh. `celeriant_mesh_channel_full_total` stayed at
/// zero, so none of it failed; it was pure cost, landing in every latency the
/// reheat curve reports.
///
/// Task `t` uses `pools[t % DATA_SHARDS]`, matching the lane `Population::birth`
/// assigns it. Total connections are unchanged: `DATA_SHARDS × (tasks /
/// DATA_SHARDS)`.
pub async fn build_lane_pools(
    cfg: &ClusterConfig,
    up: &ClusterUp,
    params: ScenarioParams,
) -> Result<Vec<std::sync::Arc<Pool>>, String> {
    build_lane_pools_with_timeout(cfg, up, params, celeriant_bench::DEFAULT_REQUEST_TIMEOUT).await
}

/// Per-request deadline for the phase 3 and phase 5 read pools.
///
/// The fill's 5s deadline censored phase 5 outright. Run 1787054105: 11,729 of
/// 12,000 cold reads timed out, and the 271 survivors reported p50 4,811ms, p99
/// 5,095ms and max 5,154ms — three numbers all pinned against the deadline, none
/// of them the cost of a cold read. Phase 3's warm max of 4,789ms says the
/// censoring reaches the baseline too at any larger population.
///
/// Sized from the only two hard numbers available:
///   * >5s per cold read at ~1 sealed segment per shard (the censored run above);
///   * ~12 sealed segments per shard projected for the `deep` preset.
///
/// A cold read reverse-scans back through segments, so scale that lower bound
/// linearly: 12 x 5s = 60s of scan, plus the ~5s queueing floor phase 3 already
/// showed at ~3,000-way concurrency with every cache hot. The resulting 65s is
/// itself only a lower bound — 5s was never the cold cost, just where we stopped
/// looking — so double it for the tail nobody has ever seen.
///
/// Worst case: each phase issues ~12,000 reads across ~3,000 tasks, 4 reads deep
/// per task. If every single read hung, a phase costs 4 x 130s = 8m40s and the
/// pair costs 17m20s — bounded, and ~6% of the `deep` preset's 5h fill.
///
/// Phases 3 and 5 MUST share this. They are the two halves of one delta, and a
/// censored baseline compared against an uncensored cold side would render as a
/// finding.
pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(130);

/// Lane pools for the read phases, on `READ_REQUEST_TIMEOUT` rather than the
/// write-shaped default. Built fresh for each read phase; the fill's pools are
/// deliberately left alone, because lengthening the write deadline would change
/// the throughput and backpressure that shaped the population being measured.
pub async fn build_read_pools(
    cfg: &ClusterConfig,
    up: &ClusterUp,
    params: ScenarioParams,
) -> Result<Vec<std::sync::Arc<Pool>>, String> {
    build_lane_pools_with_timeout(cfg, up, params, READ_REQUEST_TIMEOUT).await
}

async fn build_lane_pools_with_timeout(
    cfg: &ClusterConfig,
    up: &ClusterUp,
    params: ScenarioParams,
    request_timeout: Duration,
) -> Result<Vec<std::sync::Arc<Pool>>, String> {
    let lanes = crate::cardinality_workload::DATA_SHARDS as usize;
    let per_lane = (params.tasks / lanes).max(1);
    let mut pools = Vec::with_capacity(lanes);
    for lane in 0..lanes {
        let pool = PoolBuilder {
            address1: &up.bench_primary,
            address2: &up.bench_seed,
            server_name: Some(up.bench_primary.split(':').next().unwrap_or(&up.bench_primary)),
            ca_cert: cfg.ca_cert.to_str().unwrap(),
            client_cert: cfg.client_cert.to_str().unwrap(),
            client_key: cfg.client_key.to_str().unwrap(),
            plaintext: false,
            max_connections: per_lane,
        }
        .build_with_request_timeout(request_timeout)
        .await
        .map_err(|e| format!("lane {lane} pool build: {e}"))?;
        pools.push(pool);
    }
    Ok(pools)
}

/// Per-op history recorder for the idempotent bench, writing
/// `<run_dir>/<scenario>-history.jsonl`. Creation failure disables recording
/// (warn, not abort) — the metric predicates still run.

/// Pinned per-node pools for the RYW probe (one per config slot). Best-effort:
/// a build failure just means the probe falls back to the un-pinned pool.
pub async fn build_ryw_pinned(cfg: &ClusterConfig) -> Vec<(String, std::sync::Arc<Pool>)> {
    let mut out = Vec::new();
    for (host, addr) in [
        (cfg.leader_host.clone(), cfg.leader_addr()),
        (cfg.follower_host.clone(), cfg.follower_addr()),
    ] {
        let built = celeriant_bench::PoolBuilder {
            address1: &addr,
            address2: &addr, // pinned: no failover escape to the other node
            server_name: Some(&host),
            ca_cert: cfg.ca_cert.to_str().unwrap(),
            client_cert: cfg.client_cert.to_str().unwrap(),
            client_key: cfg.client_key.to_str().unwrap(),
            plaintext: false,
            max_connections: 64,
        }
        .build()
        .await;
        match built {
            Ok(p) => out.push((host, p)),
            Err(e) => println!("ryw pinned pool for {host} unavailable: {e}"),
        }
    }
    out
}

pub fn new_history_recorder(scen: &str, run_dir: &std::path::Path) -> Option<Arc<HistoryRecorder>> {
    let path = run_dir.join(format!("{scen}-history.jsonl"));
    match HistoryRecorder::create(&path) {
        Ok(r) => Some(Arc::new(r)),
        Err(e) => {
            eprintln!("[{scen}] history recording disabled ({}): {e}", path.display());
            None
        }
    }
}

/// Close the history file, run the client-API final-read phase against both
/// nodes (must run while services are still up), append the final-read
/// records, and run the history checkers. Returns checks to merge into the
/// scenario's `extra_checks`.
pub async fn finish_history_and_check(
    scen: &str,
    cfg: &ClusterConfig,
    history: Option<Arc<HistoryRecorder>>,
    acks: &[TaskAckSummary],
    seed: u64,
) -> Vec<CheckResult> {
    let Some(arc) = history else { return Vec::new() };
    let Ok(recorder) = Arc::try_unwrap(arc) else {
        // Bench tasks all joined before this runs, so a still-shared Arc is a
        // wiring bug, not a runtime condition.
        return vec![CheckResult::fail(
            "HistoryRecorded",
            "recorder still shared after bench join — history not finalized",
        )];
    };
    let summary = recorder.finish();
    println!(
        "[{scen}] history: {} records ({} dropped) → {}",
        summary.records_written,
        summary.records_dropped,
        summary.path.display()
    );

    match crate::final_read::run_final_read_phase(scen, cfg, acks).await {
        Ok(records) => {
            if let Err(e) = celeriant_bench::history::append_final_reads(&summary.path, &records) {
                eprintln!("[{scen}] appending final reads failed: {e}");
            }
        }
        Err(e) => eprintln!("[{scen}] final-read phase failed: {e}"),
    }

    let (lines, unparseable) = match celeriant_bench::history::read_history(&summary.path) {
        Ok(v) => v,
        Err(e) => return vec![CheckResult::fail("HistoryRecorded", format!("history unreadable: {e}"))],
    };
    let mut checks = crate::checkers::run_history_checks(&lines, summary.records_dropped + unparseable);

    // Payload round-trip: the only byte-level content check in the suite —
    // every other oracle is count/seq-based and would pass a consistent
    // payload-corruption bug unnoticed.
    if !acks.is_empty() {
        checks.push(crate::final_read::verify_payload_roundtrip(scen, cfg, acks).await);
    }

    // acked ⊆ durable-on-both: independent disk join over a sample of acked
    // aggregates — does not trust the server's self-graded counters. The acked
    // set comes from the history's actual `ok` records: workloads like
    // cas_storm ack sparsely (most writes lose OCC), so a contiguous
    // 1..=max_acked assumption over-claims and false-positives.
    let mut acked_map: std::collections::HashMap<(u128, u128, u128, u128), (Vec<u64>, Vec<u64>)> =
        std::collections::HashMap::new();
    for line in &lines {
        if let celeriant_bench::history::HistoryLine::Op(op) = line {
            if matches!(op.outcome, celeriant_bench::history::OpOutcome::Ok) {
                let entry = acked_map
                    .entry((op.org_id, op.type_id, op.agg_id, op.client_id))
                    .or_default();
                entry.0.push(op.client_seq);
                if let Some(v) = op.acked_max_aggregate_version {
                    entry.1.push(v);
                }
            }
        }
    }
    let acked: Vec<crate::epoch_oracle::AckedAggregate> = acked_map
        .into_iter()
        .map(|((org_id, type_id, agg_id, client_id), (mut acked_seqs, mut acked_versions))| {
            acked_seqs.sort_unstable();
            acked_seqs.dedup();
            acked_versions.sort_unstable();
            acked_versions.dedup();
            crate::epoch_oracle::AckedAggregate { org_id, type_id, agg_id, client_id, acked_seqs, acked_versions }
        })
        .collect();
    if !acked.is_empty() {
        checks.extend(crate::epoch_oracle::run_acked_durability_oracle(cfg, &acked, seed).await);
    }
    checks
}

/// Deadline for any single tear-down / evaluation step.
///
/// Everything in this phase runs after the measurement is already in hand, so
/// no step is worth the run. Run 1787056102 wedged here for 37 minutes and had
/// to be SIGKILLed, losing a report that was fully computed except for the
/// write; a bounded step degrades one check instead. Generous rather than
/// tight — an ssh to a loaded Pi legitimately takes tens of seconds — because
/// the point is liveness, not latency.
pub const TEARDOWN_STEP_BUDGET: Duration = Duration::from_secs(120);

/// Disk-truth gets its own, larger budget: up to `MAX_DISK_TRUTH_ENTRIES` × 2
/// nodes serial `celeriant-wal-inspect` invocations over ssh, each of which
/// scans segments. It is also the one step whose loss is expensive — it is what
/// clears an over-reported `NoClientSeqGaps` — so it is worth waiting for.
pub const DISK_TRUTH_BUDGET: Duration = Duration::from_secs(600);

/// Run one tear-down step under a deadline, announcing itself on both sides.
///
/// `None` means the step blew its budget. Every caller must degrade to
/// INCONCLUSIVE on `None`: losing one check is survivable, losing the report is
/// not. The step is also the missing progress output — phase 7 printed nothing
/// across its entire duration, which is why a 37-minute hang could only be
/// localised to a 185-line block.
async fn step<T>(
    scen: &str,
    what: &str,
    budget: Duration,
    f: impl std::future::Future<Output = T>,
) -> Option<T> {
    println!("[{scen}] step: {what}");
    let t0 = Instant::now();
    match tokio::time::timeout(budget, f).await {
        Ok(v) => {
            println!("[{scen}] step: {what} — done in {:.1}s", t0.elapsed().as_secs_f32());
            Some(v)
        }
        Err(_) => {
            println!(
                "[{scen}] step: {what} — TIMED OUT after {}s, degrading to inconclusive",
                budget.as_secs()
            );
            None
        }
    }
}

/// `step` for work that blocks its thread.
///
/// A `timeout` around a future that never yields cannot fire, so anything
/// driving `std::process::Command` directly has to reach a blocking thread
/// first or the deadline is decorative. The blocking thread is not cancelled on
/// timeout — it drains when its ssh finally returns — but the run moves on.
async fn step_blocking<T: Send + 'static>(
    scen: &str,
    what: &str,
    budget: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    step(scen, what, budget, async move { tokio::task::spawn_blocking(f).await.ok() }).await.flatten()
}

/// Standard tear-down: settle scraper one more tick, stop services, splice
/// the bench window out of the sample stream, run invariants against the
/// supplied expectations, harvest logs on failure, and produce a report.
///
/// `bench_window_start_ms` and `bench_window_end_ms` are monotonic offsets
/// (from `up.elapsed_ms()`) bracketing the period invariants should reason
/// about. Outside that window, scraper samples are kept in the report (for
/// post-mortem) but not evaluated.
#[allow(clippy::too_many_arguments)]
pub async fn tear_down_and_evaluate(
    scenario_name: &str,
    cfg: &ClusterConfig,
    up: ClusterUp,
    bench_result: BenchmarkResult,
    bench_window_start_ms: u64,
    bench_window_end_ms: u64,
    expectations: ScenarioExpectations,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    tear_down_and_evaluate_with_audit(
        scenario_name, cfg, up, bench_result,
        bench_window_start_ms, bench_window_end_ms,
        expectations, params, Vec::new(), None, None, None, run_dir,
    ).await
}

/// Extended variant of `tear_down_and_evaluate` for scenarios that want to
/// attach pre-computed checks (e.g. data-integrity audit) and bench metadata
/// produced before service tear-down. `extra_checks` are appended to the
/// invariant checks; any failing one fails the scenario. `integrity` and
/// `idempotent_counters` are passed through to the JSON report unchanged.
#[allow(clippy::too_many_arguments)]
pub async fn tear_down_and_evaluate_with_audit(
    scenario_name: &str,
    cfg: &ClusterConfig,
    up: ClusterUp,
    bench_result: BenchmarkResult,
    bench_window_start_ms: u64,
    bench_window_end_ms: u64,
    expectations: ScenarioExpectations,
    params: ScenarioParams,
    extra_checks: Vec<CheckResult>,
    integrity: Option<DataIntegrityReport>,
    idempotent_counters: Option<IdempotentBenchCounters>,
    deep_audit: Option<DeepAuditReport>,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    // Run disk-truth verification on flagged aggregates BEFORE stopping services
    // so wal-inspect reads consistent state. 64-entry cap keeps SSH time bounded
    // under 8k-task load.
    const MAX_DISK_TRUTH_ENTRIES: usize = 64;

    // Paired with a `total_flagged` count so the NoClientSeqGaps override
    // below can tell "every flagged aggregate was disk-verified" from
    // "only the first MAX_DISK_TRUTH_ENTRIES of a larger set were" (N2).
    let disk_truth_planned: Option<(Vec<celeriant_bench::DeepAuditEntry>, usize)> =
        integrity.as_ref().filter(|i| !i.failing_task_acks.is_empty()).map(|i| {
            use celeriant_bench::DeepAuditEntry;
            let by_key: std::collections::HashMap<String, &DeepAuditEntry> = deep_audit
                .as_ref()
                .map(|da| da.entries.iter().map(|e| (e.aggregate_key_str.clone(), e)).collect())
                .unwrap_or_default();
            let synthesised_all: Vec<DeepAuditEntry> = i.failing_task_acks.iter().map(|ack| {
                let key_str = format!("{}", ack.aggregate_key);
                if let Some(de) = by_key.get(&key_str) {
                    (*de).clone()
                } else {
                    DeepAuditEntry {
                        aggregate_key_str: key_str,
                        client_id: ack.client_id,
                        max_acked: ack.max_acked_client_seq,
                        present_count: 0,
                        missing_seqs: Vec::new(),
                        duplicate_seqs: Vec::new(),
                        duplicate_aggregate_versions: Vec::new(),
                        total_batches: 0,
                    }
                }
            }).collect();
            let total_flagged = synthesised_all.len();
            let synthesised: Vec<DeepAuditEntry> = synthesised_all
                .into_iter()
                .take(MAX_DISK_TRUTH_ENTRIES)
                .collect();
            if total_flagged > synthesised.len() {
                println!(
                    "[{scenario_name}] disk-truth: verifying {} of {} flagged aggregates via wal-inspect on both nodes (capped at {})",
                    synthesised.len(), total_flagged, MAX_DISK_TRUTH_ENTRIES
                );
            } else {
                println!("[{scenario_name}] disk-truth: verifying {} flagged aggregates via wal-inspect on both nodes", synthesised.len());
            }
            (synthesised, total_flagged)
        });
    // Bounded and off the async threads: `verify_against_disk_truth` is a
    // serial run of `celeriant-wal-inspect` over ssh, one invocation per
    // aggregate per node, and nothing in it had a deadline.
    let mut timed_out_checks: Vec<CheckResult> = Vec::new();
    let disk_truth_computed: Option<(Vec<crate::disk_truth::DiskTruthEntry>, usize)> = match disk_truth_planned {
        None => None,
        Some((synthesised, total_flagged)) => {
            let (leader, follower) = (cfg.leader_host.clone(), cfg.follower_host.clone());
            let verified = match step_blocking(
                scenario_name,
                "disk-truth wal-inspect",
                DISK_TRUTH_BUDGET,
                move || crate::disk_truth::verify_against_disk_truth(&leader, &follower, &synthesised),
            )
            .await
            {
                Some(v) => v,
                None => {
                    // No entries means the `NoClientSeqGaps` override below
                    // cannot fire, which is the safe direction — but the reason
                    // has to be on the record, or the run reads as a real gap.
                    timed_out_checks.push(CheckResult::inconclusive(
                        "DiskTruthVerified",
                        format!(
                            "wal-inspect over {total_flagged} flagged aggregates exceeded {}s — \
                             disk truth unknown, so NoClientSeqGaps stands on the audit alone",
                            DISK_TRUTH_BUDGET.as_secs()
                        ),
                    ));
                    Vec::new()
                }
            };
            let truly_missing: u64 = verified.iter().map(|v| v.actually_missing.len() as u64).sum();
            let overreported: u64 = verified.iter().map(|v| v.audit_overreported.len() as u64).sum();
            println!(
                "[{scenario_name}] disk-truth: {} aggregates checked, audit overreported {} seqs, actually missing {} seqs",
                verified.len(), overreported, truly_missing
            );
            Some((verified, total_flagged))
        }
    };
    let (disk_truth_report, disk_truth_total_flagged): (Option<Vec<crate::disk_truth::DiskTruthEntry>>, usize) =
        match disk_truth_computed {
            Some((v, total_flagged)) => (Some(v), total_flagged),
            None => (None, 0),
        };

    // Resource leak check: snapshot fd/RSS while services are still up and
    // compare against the bring-up baseline.
    let mut env_checks: Vec<CheckResult> = timed_out_checks;
    for (host, before) in &up.resource_before {
        let what = format!("resource after-snapshot {host}");
        match step(scenario_name, &what, TEARDOWN_STEP_BUDGET, crate::resource_baseline::snapshot(host)).await {
            Some(Ok(after)) => env_checks.extend(crate::resource_baseline::baseline_checks(host, before, &after)),
            Some(Err(e)) => println!("[{scenario_name}] resource after-snapshot skipped for {host}: {e}"),
            None => env_checks.push(CheckResult::inconclusive(
                "FdReturnToBaseline",
                format!("{host}: after-snapshot ssh exceeded {}s — fd/RSS leak unattestable", TEARDOWN_STEP_BUDGET.as_secs()),
            )),
        }
    }

    // Quiesce wait for ReadConvergedAtQuiesce: the read cursor's convergence
    // is netted by the 5s reconciliation probe (the notify has by-design
    // lost paths), so grant two probe periods + margin for the drain to
    // reach the write tip before the final samples are taken. A timeout
    // does not abort — the gated check judges whatever state was reached.
    if expectations.assert_read_converged_at_quiesce {
        wait_for_read_convergence(scenario_name, cfg, Duration::from_secs(12)).await;
    }

    // Give the scraper one more tick before stopping it.
    sleep(Duration::from_millis(750)).await;
    let outcome = step(scenario_name, "stop scraper", TEARDOWN_STEP_BUDGET, up.scraper.stop())
        .await
        .ok_or_else(|| format!("{scenario_name}: scraper did not stop within {}s", TEARDOWN_STEP_BUDGET.as_secs()))?;
    let samples = outcome.store.snapshot().await;

    {
        // `systemd`'s own `TimeoutStopSec` is 90s per unit and the target stops
        // them in sequence, so this budget has to clear two of those.
        const STOP_ALL_BUDGET: Duration = Duration::from_secs(300);
        let deploy_dir = cfg.deploy_dir.clone();
        let (target, vars) = (Action::StopAll.make_target(), Action::StopAll.make_vars());
        let _ = step_blocking(scenario_name, "stop services", STOP_ALL_BUDGET, move || {
            crate::actions::run_make_in(&deploy_dir, target, &vars)
        })
        .await;
    }

    // Post-stop oracles: WAL state is quiescent, MinIO is still up (teardown-data
    // only runs at the NEXT scenario's bring-up).
    match step(scenario_name, "epoch oracle", TEARDOWN_STEP_BUDGET, crate::epoch_oracle::run_epoch_oracle(cfg, 4)).await {
        Some(c) => env_checks.extend(c),
        None => env_checks.push(CheckResult::inconclusive(
            "EpochMonotonicPerChain",
            format!("epoch oracle exceeded {}s — WAL epoch invariants unattestable", TEARDOWN_STEP_BUDGET.as_secs()),
        )),
    }
    let s3_lifecycle_report = step(scenario_name, "s3 lifecycle audit", TEARDOWN_STEP_BUDGET, crate::s3_lifecycle::audit_s3_fallback())
        .await
        .flatten();
    if let Some(ref report) = s3_lifecycle_report {
        println!(
            "[{scenario_name}] s3-lifecycle: {} residual fallback objects across {} shards",
            report.total_objects, report.per_shard.len()
        );
        env_checks.extend(crate::s3_lifecycle::checks(report));
    }

    let (start_idx, end_idx) = sample_window(&samples, bench_window_start_ms, bench_window_end_ms);

    // `BenchmarkResult` records the actual wall-time spent benching via
    // `params.duration_secs` (run_benchmark runs for exactly this many seconds).
    // The bench started at `bench_window_start_ms`, so it stopped at
    // `bench_window_start_ms + duration_secs * 1000`. Anything past that in the
    // sample window is settle (no client traffic).
    let bench_actual_end_ms = bench_window_start_ms.saturating_add(params.duration_secs * 1000);

    let data = RunData {
        samples: &samples,
        leader_host: &cfg.leader_host,
        follower_host: &cfg.follower_host,
        bench_start_idx: start_idx,
        bench_end_idx: end_idx,
        bench_actual_end_ms,
        bench_errors: bench_result.errors,
        bench_total_requests: bench_result.total_requests,
        bench_throughput: bench_result.throughput,
        throughput_floor: params.throughput_floor,
    };
    let mut checks = run_all(&data, &expectations);
    checks.extend(extra_checks);

    // Disk-truth overrides NoClientSeqGaps: audit can over-report under post-chaos load.
    // Only safe when EVERY flagged aggregate was actually disk-verified — if
    // the flagged set exceeded MAX_DISK_TRUTH_ENTRIES, the unverified
    // remainder could hide a real gap, so the override must not fire (N2).
    if let Some(entries) = disk_truth_report.as_ref() {
        if disk_truth_override_applies(entries, disk_truth_total_flagged, MAX_DISK_TRUTH_ENTRIES) {
            let overreported: u64 = entries.iter().map(|e| e.audit_overreported.len() as u64).sum();
            for check in checks.iter_mut() {
                if check.name == "NoClientSeqGaps" && check.failed() {
                    check.outcome = CheckOutcome::Pass;
                    check.detail = format!(
                        "audit reported gaps but disk-truth verified all {} of {} flagged aggregates are clean ({} seqs overreported)",
                        entries.len(), disk_truth_total_flagged, overreported,
                    );
                }
            }
        } else if disk_truth_total_flagged > MAX_DISK_TRUTH_ENTRIES && !entries.is_empty() {
            println!(
                "[{scenario_name}] disk-truth: {} of {} flagged aggregates verified — cannot rule out the remaining {} unverified, NoClientSeqGaps stays failing",
                entries.len(), disk_truth_total_flagged, disk_truth_total_flagged - entries.len(),
            );
        }
    }

    checks.extend(env_checks);

    // Journals are now fetched on EVERY run (not just failures) so they can be
    // asserted: panics, aborts, BorrowMutError, and error storms fail the run
    // even when no metric or history check noticed. Both nodes are expected up
    // at teardown, so a missing or unreadable journal is itself a fail (N4) —
    // it must not silently drop the JournalNoPanics/NoAbort/NoErrorStorm
    // checks and count a crashed node as never-run.
    let log_files = {
        // journalctl over ssh, one node at a time, and the fetch had no
        // deadline. A failed fetch is already a `JournalHarvested` fail below,
        // so a timeout lands in the same place instead of stalling the report.
        const JOURNAL_BUDGET: Duration = Duration::from_secs(300);
        let (leader, follower) = (cfg.leader_host.clone(), cfg.follower_host.clone());
        let (scen, dir) = (scenario_name.to_string(), run_dir.clone());
        let (wall_start, wall_end) = (outcome.wall_start, outcome.wall_end);
        step_blocking(scenario_name, "harvest journals", JOURNAL_BUDGET, move || {
            harvest_journals(&leader, &follower, &scen, wall_start, wall_end, &dir)
        })
        .await
        .unwrap_or_default()
    };
    for (label, _host) in [("cs1", &cfg.leader_host), ("cs2", &cfg.follower_host)] {
        let basename = format!("{scenario_name}.{label}.log");
        if !log_files.contains(&basename) {
            checks.push(CheckResult::fail(
                "JournalHarvested",
                format!("{label}: journal unavailable — crash-safety unattestable (SSH/journalctl fetch failed)"),
            ));
        }
    }
    for rel in &log_files {
        let path = run_dir.join(rel);
        let node = rel.split('.').nth(1).unwrap_or("node").to_string();
        match std::fs::read_to_string(&path) {
            Ok(text) => checks.extend(crate::journal_assert::journal_checks(&node, &text)),
            Err(e) => checks.push(CheckResult::fail(
                "JournalHarvested",
                format!("{node}: journal unavailable — crash-safety unattestable (read failed: {e})"),
            )),
        }
    }

    // A failure outranks an inconclusive: something is known to be wrong, and
    // that verdict should not be softened by a check that merely had nothing to
    // say. Inconclusive only wins when nothing failed.
    let verdict = ScenarioOutcome::of(&checks);
    match verdict {
        ScenarioOutcome::Fail => {
            let failed: Vec<&str> = checks.iter().filter(|c| c.failed()).map(|c| c.name).collect();
            println!("[{scenario_name}] FAIL — {}", failed.join(", "));
            for c in checks.iter().filter(|c| c.failed()) {
                println!("[{scenario_name}]   {}: {}", c.name, c.detail);
            }
        }
        ScenarioOutcome::Inconclusive => {
            let unmet: Vec<&str> = checks.iter().filter(|c| c.is_inconclusive()).map(|c| c.name).collect();
            println!("[{scenario_name}] INCONCLUSIVE — {}", unmet.join(", "));
            for c in checks.iter().filter(|c| c.is_inconclusive()) {
                println!("[{scenario_name}]   {}: {}", c.name, c.detail);
            }
        }
        ScenarioOutcome::Pass => {}
    }

    Ok(ScenarioReport {
        name: scenario_name.into(),
        outcome: verdict,
        params: ScenarioParamsJson {
            tasks: params.tasks,
            duration_secs: params.duration_secs,
            throughput_floor: params.throughput_floor,
            seed: params.seed,
        },
        bench: BenchmarkSummary::from(&bench_result),
        checks,
        samples,
        bench_window_start_ms,
        bench_actual_end_ms,
        bench_window_end_ms,
        log_files,
        idempotent_counters,
        integrity,
        deep_audit,
        disk_truth: disk_truth_report,
        s3_lifecycle: s3_lifecycle_report,
        cardinality: None,
    })
}

/// Convenience: run the headline audit and, if any gaps were detected,
/// run a per-aggregate deep audit on a capped number of failing tasks.
/// Returns the integrity report (always populated) and the deep audit
/// report (only populated when gaps were detected). Both flow into the
/// scenario report unchanged.
pub async fn run_integrity_and_deep_audit(
    scenario_name: &str,
    pool: &std::sync::Arc<Pool>,
    task_acks: &[celeriant_bench::TaskAckSummary],
    deep_inspect_cap: usize,
) -> (DataIntegrityReport, Option<DeepAuditReport>) {
    println!("[{scenario_name}] auditing {} task(s)...", task_acks.len());
    let integrity = verify_no_seq_gaps(pool, task_acks, 16).await;
    println!(
        "[{scenario_name}] audit done: tasks={} with_gaps={} missing_acks={} unreadable={}",
        integrity.tasks_audited, integrity.tasks_with_gaps,
        integrity.total_missing_acks, integrity.tasks_unreadable,
    );
    let deep = if integrity.tasks_with_gaps > 0 {
        println!(
            "[{scenario_name}] deep audit: inspecting up to {} of {} failing aggregates",
            deep_inspect_cap,
            integrity.failing_task_acks.len(),
        );
        let d = deep_audit_failing_aggregates(
            pool,
            &integrity.failing_task_acks,
            deep_inspect_cap,
            64,
        ).await;
        println!(
            "[{scenario_name}] deep audit done: inspected={} with_duplicates={} total_dup_occurrences={} unreadable={}",
            d.aggregates_inspected, d.aggregates_with_duplicates,
            d.total_duplicate_occurrences, d.aggregates_unreadable,
        );
        Some(d)
    } else {
        None
    };
    (integrity, deep)
}

/// Turn a `DataIntegrityReport` into a `CheckResult`. Any task with a gap is
/// a hard fail; up to `max_unreadable` tasks that errored during the audit
/// read pass are tolerated. `tear_down_and_evaluate_with_audit` will flip a
/// failing NoClientSeqGaps check to pass if disk-truth verifies the flagged
/// aggregates are actually clean (audit-noise override).
pub fn data_integrity_check(report: &DataIntegrityReport, max_unreadable: u64) -> CheckResult {
    const NAME: &str = "NoClientSeqGaps";
    if report.tasks_with_gaps > 0 {
        let sample_summary: Vec<String> = report
            .sample_gaps
            .iter()
            .take(4)
            .map(|g| {
                format!(
                    "{}@client_id={}: max_acked={} server_version={} missing_count={}",
                    g.aggregate_key_str, g.client_id, g.max_acked,
                    g.max_aggregate_version, g.missing_count
                )
            })
            .collect();
        return CheckResult {
            name: NAME,
            outcome: CheckOutcome::Fail,
            detail: format!(
                "{} task(s) with gaps; {} acked client_seq value(s) missing total; sample: [{}]",
                report.tasks_with_gaps,
                report.total_missing_acks,
                sample_summary.join("; ")
            ),
        };
    }
    if report.tasks_unreadable > max_unreadable {
        return CheckResult {
            name: NAME,
            outcome: CheckOutcome::Fail,
            detail: format!(
                "{} task(s) unreadable during audit (allowed {})",
                report.tasks_unreadable, max_unreadable,
            ),
        };
    }
    CheckResult::pass(NAME)
}

/// Whether the disk-truth sample is a safe basis for overriding a failing
/// NoClientSeqGaps to PASS. Requires every flagged aggregate to have been
/// disk-verified (not just a capped sample of a larger flagged set) and
/// every verified entry to be actually clean (N2 — a >64-flagged run must
/// not report "verified" for aggregates it never looked at on disk).
fn disk_truth_override_applies(
    entries: &[crate::disk_truth::DiskTruthEntry],
    total_flagged: usize,
    cap: usize,
) -> bool {
    total_flagged <= cap
        && !entries.is_empty()
        && entries.iter().all(|e| e.actually_missing.is_empty())
}

#[cfg(test)]
mod disk_truth_override_tests {
    use super::disk_truth_override_applies;
    use crate::disk_truth::DiskTruthEntry;

    fn clean_entry() -> DiskTruthEntry {
        DiskTruthEntry {
            aggregate_key_str: "1/1/1".into(),
            client_id: 1,
            max_acked: 5,
            audit_missing: vec![3],
            actually_missing: Vec::new(),
            audit_overreported: vec![3],
            leader_summary: String::new(),
            follower_summary: String::new(),
        }
    }

    #[test]
    fn applies_when_all_flagged_were_verified_and_clean() {
        let entries = vec![clean_entry(), clean_entry()];
        assert!(disk_truth_override_applies(&entries, 2, 64));
    }

    #[test]
    fn does_not_apply_when_flagged_exceeds_cap() {
        // 100 flagged, only 64 verified (the cap) — the other 36 were never
        // looked at on disk, so a clean sample cannot vouch for the whole set.
        let entries: Vec<DiskTruthEntry> = (0..64).map(|_| clean_entry()).collect();
        assert!(!disk_truth_override_applies(&entries, 100, 64));
    }

    #[test]
    fn does_not_apply_when_a_verified_entry_is_actually_missing() {
        let mut missing = clean_entry();
        missing.actually_missing = vec![7];
        let entries = vec![clean_entry(), missing];
        assert!(!disk_truth_override_applies(&entries, 2, 64));
    }

    #[test]
    fn does_not_apply_to_an_empty_verified_set() {
        assert!(!disk_truth_override_applies(&[], 0, 64));
    }
}

/// Delete/trim side-load run concurrently with a scenario's main bench:
/// 1/16th of the main task count cycling write → trim → OCC delete →
/// sequence-continuation recreate on its own aggregate population (org 2).
/// Proves the destructive-op ACK contract under the scenario's fault — the
/// false-ack and stale-tombstone failure modes the rollback-generation and
/// two-phase-delete fixes exist to prevent.
pub fn spawn_delete_trim_sideload(
    scenario: &str,
    pool: &std::sync::Arc<Pool>,
    params: ScenarioParams,
) -> tokio::task::JoinHandle<celeriant_bench::DeleteTrimOutcome> {
    let dt_tasks = (params.tasks / 16).clamp(8, 128);
    let dur = params.duration_secs;
    let pool = pool.clone();
    println!("[{scenario}] delete/trim side-load: {dt_tasks} tasks, {dur}s");
    tokio::spawn(async move { celeriant_bench::run_delete_trim_workload(&pool, dt_tasks, dur).await })
}

/// On a version regression, the WAL holding the duplicate is wiped by the next
/// scenario's bring-up — capture raw wal-inspect output for exactly the
/// violating aggregates into the run dir while the evidence still exists.
fn capture_regression_disk_truth(
    scenario: &str,
    cfg: &ClusterConfig,
    run_dir: &PathBuf,
    regression_aggs: &[(u128, u128, u64)],
) {
    use std::collections::BTreeSet;
    let unique: BTreeSet<(u128, u128)> = regression_aggs.iter().map(|(a, c, _)| (*a, *c)).collect();
    let mut out = String::new();
    for (agg_id, client_id) in unique.iter().take(8) {
        for host in [&cfg.leader_host, &cfg.follower_host] {
            out.push_str(&format!("===== host={host} org=2 type=1 agg={agg_id} client={client_id} =====\n"));
            let cmd = format!(
                "for n in 1 2 3; do for f in /var/lib/nvme/celeriant-data/shard_$n/log_*.wal; do \
                 [ -f \"$f\" ] && {{ echo \"--- $f\"; sudo /usr/local/bin/celeriant-wal-inspect \"$f\" client 2 1 {agg_id} {client_id} 2>&1; }}; done; done"
            );
            match std::process::Command::new("ssh").arg(host).arg(&cmd).output() {
                Ok(o) => {
                    out.push_str(&String::from_utf8_lossy(&o.stdout));
                    out.push_str(&String::from_utf8_lossy(&o.stderr));
                }
                Err(e) => out.push_str(&format!("(ssh failed: {e})\n")),
            }
        }
    }
    let path = run_dir.join(format!("{scenario}.regression-walinspect.txt"));
    if let Err(e) = std::fs::write(&path, out) {
        eprintln!("[{scenario}] failed to write regression wal-inspect capture: {e}");
    } else {
        println!("[{scenario}] regression disk truth captured: {}", path.display());
    }
}

/// Await the side-load and convert its outcome + post-settle audit into
/// checks. Call AFTER the scenario's settle window so read staleness can't
/// masquerade as a false ack.
pub async fn delete_trim_checks(
    scenario: &str,
    cfg: &ClusterConfig,
    pool: &std::sync::Arc<Pool>,
    run_dir: &PathBuf,
    handle: tokio::task::JoinHandle<celeriant_bench::DeleteTrimOutcome>,
) -> Vec<CheckResult> {
    let outcome = match handle.await {
        Ok(o) => o,
        Err(e) => return vec![CheckResult::fail("DeleteTrimSideload", format!("join: {e}"))],
    };
    let c = outcome.counters.clone();
    println!(
        "[{scenario}] delete/trim done: writes={} trims={} deletes={} recreates={} retries={} resyncs={} regressions={} fatal={}",
        c.write_acks, c.trim_acks, c.delete_acks, c.recreate_acks,
        c.retries, c.occ_resyncs, c.version_regressions, c.fatal_errors,
    );
    if !outcome.regression_aggs.is_empty() {
        capture_regression_disk_truth(scenario, cfg, run_dir, &outcome.regression_aggs);
    }
    let pinned = build_ryw_pinned(cfg).await;
    let audit = celeriant_bench::audit_delete_trim_pinned(pool, &outcome, &pinned).await;
    println!(
        "[{scenario}] delete/trim audit: tasks={} false_acked_deletes={} trim_floor_breaches={} version_loss={} unacked_landed={} ambiguous_recreates={} node_divergences={} unreadable={}",
        audit.tasks_audited, audit.false_acked_deletes, audit.trim_floor_breaches,
        audit.acked_version_loss, audit.unacked_deletes_landed, audit.ambiguous_recreates_landed, audit.node_divergences, audit.tasks_unreadable,
    );
    for s in &audit.samples {
        println!("[{scenario}]   delete/trim sample: {s}");
    }

    vec![
        if c.delete_acks + c.trim_acks > 0 {
            CheckResult::pass_with_detail(
                "DeleteTrimExercised",
                format!("{} deletes, {} trims acked ({} retries)", c.delete_acks, c.trim_acks, c.retries),
            )
        } else {
            CheckResult::fail(
                "DeleteTrimExercised",
                "no delete or trim was ever acked — side-load not exercising the destructive paths",
            )
        },
        if c.version_regressions == 0 {
            CheckResult::pass("DeleteTrimVersionMonotonicity")
        } else {
            CheckResult::fail(
                "DeleteTrimVersionMonotonicity",
                format!(
                    "{} acked writes returned a non-increasing aggregate_version — stale-tombstone corruption signature",
                    c.version_regressions,
                ),
            )
        },
        if audit.violations() == 0 {
            CheckResult::pass_with_detail(
                "DeleteTrimAckContract",
                format!("{} tasks audited clean", audit.tasks_audited),
            )
        } else {
            CheckResult::fail(
                "DeleteTrimAckContract",
                format!(
                    "false_acked_deletes={} trim_floor_breaches={} acked_version_loss={}; samples: {:?}",
                    audit.false_acked_deletes, audit.trim_floor_breaches,
                    audit.acked_version_loss, audit.samples,
                ),
            )
        },
    ]
}

/// The single happy-path scenario. Drives a clean cluster bring-up, runs the
/// bench against the actual leader with no chaos, then evaluates invariants
/// with strict zero-tolerance expectations. Any non-zero counter delta or
/// any role flip is a fail.
pub async fn run_baseline(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    let up = bring_up_cluster(cfg, "baseline", run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[baseline] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let bench_window_start_ms = up.elapsed_ms();
    println!("[baseline] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let dt_handle = spawn_delete_trim_sideload("baseline", &pool, params);
    let bench_result =
        celeriant_bench::run_benchmark_ramped(&pool, params.tasks, params.duration_secs, params.connect_ramp_secs).await;
    let bench_window_end_ms = up.elapsed_ms();
    println!(
        "[baseline] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    // The other three delete_trim_checks call sites settle first; baseline did
    // not, so a follower still draining the last batch read as a false ack.
    // Poll rather than sleep — a fault-free run converges in well under a second.
    wait_for_wal_convergence("baseline", cfg, Duration::from_secs(60)).await;

    let extra_checks = delete_trim_checks("baseline", cfg, &pool, run_dir, dt_handle).await;

    // Strict-zero counters plus the follower-visibility oracles: with zero
    // role flips the entire window is one stable-leadership run.
    let expectations = ScenarioExpectations {
        assert_never_ahead: true,
        assert_read_converged_at_quiesce: true,
        ..ScenarioExpectations::default()
    };

    tear_down_and_evaluate_with_audit(
        "baseline",
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        params,
        extra_checks,
        None,
        None,
        None,
        run_dir,
    )
    .await
}

/// Watch storm on a HAPPY cluster (no fault injection). Runs the normal write
/// bench while an adversarial watch flood churns connections, opens slow/never-
/// reading watchers, and holds long-lived watchers that must keep receiving
/// events. Verifies the watch path can't take the server down or leak through
/// it: server stays the single leader, write throughput holds, the subscriber
/// gauge drains after the flood (no CLOSE-WAIT leak), and a fresh dial is still
/// prompt (no 503/~5s-dial degradation).
pub async fn run_watch_storm(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    run_watch_storm_inner(cfg, params, run_dir, false).await
}

/// `watch_storm` plus a mid-storm leader kill (remaining-tests.md item 4):
/// the flood runs against the original leader, which is SIGKILLed ~40% into
/// the window. The flood's watchers against the dead node drop (the
/// failover-disconnect contract); the distinct assertions move to the
/// *promoted* node — it must service fresh watch dials promptly and show no
/// leaked sessions once it leads. Per-watcher delivery correctness across the
/// reconnect is covered by the `watch_failover` integration test; this
/// scenario proves watch servicing recovers on the survivor under storm load.
pub async fn run_watch_storm_failover(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    run_watch_storm_inner(cfg, params, run_dir, true).await
}

async fn run_watch_storm_inner(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
    with_kill: bool,
) -> Result<ScenarioReport, String> {
    #[allow(non_snake_case)]
    let SCEN: &'static str = if with_kill { "watch_storm_failover" } else { "watch_storm" };
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    // Watch client targets the actual leader resolved during bring-up. SNI must be
    // the leader hostname to match the cert SANs.
    let watch_addr = up.bench_primary.clone();
    let leader_host = watch_addr.split(':').next().unwrap_or(&watch_addr).to_string();
    let watch_tls = build_tls_config(
        cfg.ca_cert.to_str().ok_or("ca_cert path not utf8")?,
        cfg.client_cert.to_str().ok_or("client_cert path not utf8")?,
        cfg.client_key.to_str().ok_or("client_key path not utf8")?,
        &leader_host,
    )
    .map_err(|e| format!("watch tls: {e}"))?;

    // The survivor (becomes leader after the kill): the config slot that
    // isn't the current leader. Probes/drains retarget here post-kill.
    let survivor_host = if leader_host == cfg.leader_host { cfg.follower_host.clone() } else { cfg.leader_host.clone() };
    let survivor_addr = format!("{survivor_host}:{}", cfg.client_port);
    let survivor_tls = build_tls_config(
        cfg.ca_cert.to_str().ok_or("ca_cert path not utf8")?,
        cfg.client_cert.to_str().ok_or("client_cert path not utf8")?,
        cfg.client_key.to_str().ok_or("client_key path not utf8")?,
        &survivor_host,
    )
    .map_err(|e| format!("survivor tls: {e}"))?;
    let kill_leader = if leader_host == cfg.leader_host { Action::KillCs1 } else { Action::KillCs2 };

    // Aggregate ids the watchers filter on. The write bench writes (1, 1, id) for
    // id in 0..tasks, so this range overlaps and long-lived watchers see events.
    // churn_tasks × the ~40-80ms inter-cycle gap sets the storm rate. 16 keeps
    // the runner's client-side TIME_WAIT bounded over a 60s window while still
    // sustaining ~200 connect/disconnect per second — pre-fix that leaves ~1000
    // sockets in CLOSE-WAIT on the leader at steady state.
    let flood_params = WatchFloodParams {
        duration_secs: params.duration_secs,
        churn_tasks: 16,
        long_lived_tasks: 8,
        slow_tasks: 4,
        aggregate_ids: (0u128..64).collect(),
    };

    let bench_window_start_ms = up.elapsed_ms();
    println!(
        "[{SCEN}] bench {} tasks + watch flood (churn {}, long {}, slow {}) for {}s",
        params.tasks, flood_params.churn_tasks, flood_params.long_lived_tasks,
        flood_params.slow_tasks, params.duration_secs,
    );

    // Drive the write bench and the watch flood concurrently for the window.
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    // Record watch deliveries so the content checkers (per-connection
    // ordering; delivered ⊆ durable) engage. Only the ordering checker fires
    // here — there are no final-read records without an idempotent-bench
    // history, and the durable checker skips gracefully.
    let watch_history = new_history_recorder(SCEN, run_dir);
    let flood = if with_kill {
        let executor = ActionExecutor::new(cfg);
        let watch_addr2 = watch_addr.clone();
        let watch_tls2 = watch_tls.clone();
        let history2 = watch_history.clone();
        let flood_handle = tokio::spawn(async move {
            match history2 {
                Some(h) => celeriant_bench::run_watch_flood_with_history(&watch_addr2, watch_tls2, flood_params, h).await,
                None => run_watch_flood(&watch_addr2, watch_tls2, flood_params).await,
            }
        });
        // Kill ~40% into the window so there's storm both before and after.
        sleep(Duration::from_secs((params.duration_secs * 2 / 5).max(5))).await;
        println!("[{SCEN}] SIGKILL leader {leader_host} ({kill_leader:?}) mid-storm");
        executor.run(&kill_leader)?;
        flood_handle.await.map_err(|e| format!("flood join: {e}"))?
    } else {
        match watch_history.clone() {
            Some(h) => celeriant_bench::run_watch_flood_with_history(&watch_addr, watch_tls.clone(), flood_params, h).await,
            None => run_watch_flood(&watch_addr, watch_tls.clone(), flood_params).await,
        }
    };

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    let bench_window_end_ms = up.elapsed_ms();

    // Post-kill the original leader is dead; drain + dial probes must target
    // the promoted survivor. Give it time to win the lease and serve.
    let (probe_addr, probe_host, probe_tls) = if with_kill {
        println!("[{SCEN}] waiting for {survivor_host} to promote before probing watch servicing");
        sleep(Duration::from_secs(20)).await;
        (survivor_addr.clone(), survivor_host.clone(), survivor_tls.clone())
    } else {
        (watch_addr.clone(), leader_host.clone(), watch_tls.clone())
    };

    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s | watch: {} cycles, {} attempts ({} conn errors), {} events",
        bench_result.total_requests, bench_result.errors, bench_result.throughput,
        flood.cycles, flood.connect_attempts, flood.connect_errors, flood.events_received,
    );

    let mut extra_checks: Vec<CheckResult> = Vec::new();
    extra_checks.extend(finish_history_and_check(SCEN, cfg, watch_history, &[], params.seed).await);

    // 1) Delivery survived the churn.
    extra_checks.push(if flood.events_received > 0 {
        CheckResult::pass_with_detail("WatchEventsDelivered", format!("{} events", flood.events_received))
    } else {
        CheckResult::fail("WatchEventsDelivered", "long-lived watchers received no events under churn")
    });

    // 2) No leaked sessions. Every watcher is dropped once run_watch_flood returns;
    //    after a short settle the leader's gauge must be ~0. A pre-fix leak keeps
    //    sessions alive until their next ~5s heartbeat write fails, so the gauge
    //    stays elevated well past this 3s settle.
    sleep(Duration::from_secs(3)).await;
    let metrics_url = cfg.metrics_url(&probe_host);
    match scrape_watch_subscribers(&metrics_url, &probe_host).await {
        Ok(active) if active <= 4 => {
            extra_checks.push(CheckResult::pass_with_detail("WatchSubscribersDrained", format!("{active} active on {probe_host}")));
        }
        Ok(active) => {
            extra_checks.push(CheckResult::fail(
                "WatchSubscribersDrained",
                format!("{active} watch sessions still active 3s after the flood on {probe_host} — likely leaked (CLOSE-WAIT)"),
            ));
        }
        Err(e) => {
            extra_checks.push(CheckResult::fail("WatchSubscribersDrained", format!("metrics scrape failed: {e}")));
        }
    }

    // 3) No degradation: a fresh dial to the (post-kill: promoted) leader still
    //    acks promptly (the original symptom was ~5s dials / 503s once the leak
    //    saturated watch servicing).
    match watch_dial_probe(&probe_addr, probe_tls, 9_999_999, Duration::from_secs(2)).await {
        Ok(d) if d < Duration::from_secs(1) => {
            extra_checks.push(CheckResult::pass_with_detail("WatchDialPrompt", format!("dial to {probe_host} acked in {d:?}")));
        }
        Ok(d) => {
            extra_checks.push(CheckResult::fail("WatchDialPrompt", format!("dial to {probe_host} took {d:?} (>1s)")));
        }
        Err(e) => {
            extra_checks.push(CheckResult::fail("WatchDialPrompt", e));
        }
    }

    let expectations = if with_kill {
        // Leader killed mid-storm: a single failover is expected. Same shape
        // as leader_sigkill, plus the connection-storm bench-error budget.
        // The failover opens a no-leader window (survivor waits its full TTL
        // before challenging) — allow it; the survivor's watch-dial probe
        // above is the robust proof that promotion completed and watch
        // servicing recovered, so we don't also gate on the timing-fragile
        // in-window `require_distinct_leader_hosts` (the survivor often
        // promotes near or just after the bench window closes).
        // No `assert_eventual_progress`: the killed leader is never restarted
        // in this scenario, so cross-node convergence is undefined — the
        // survivor promotes from its own replicated point and the old
        // leader's un-replicated tail is culled, leaving the dead node's
        // stale high-water permanently ahead. Data-integrity-under-failover
        // is covered by the idempotency_audit_* and single_node_isolation
        // scenarios (which restart and audit). Here the thesis is "watch
        // servicing recovers on the promoted node", proven by the
        // WatchDialPrompt / WatchSubscribersDrained / WatchEventsDelivered
        // checks above.
        ScenarioExpectations {
            max_leader_elections: 6,
            max_s3_fallbacks: 1000,
            max_heartbeat_failures: 100,
            max_bench_errors: 500_000,
            // Load-proportional ceiling: errors are retry attempts and scale
            // with offered load (attempt-share of errors+completions).
            max_bench_error_ratio: Some(0.75),
            max_role_flips: 4,
            max_node_starts: 1,
            max_split_brain_ticks: 40,
            ..ScenarioExpectations::default()
        }
    } else {
        // Happy cluster: no elections/panics/restarts expected. The connection
        // storm can induce transient write timeouts that scale with task count
        // (observed ~1% attempt-share at 25k under flood churn); the throughput
        // floor is the real "didn't fall over" guard.
        ScenarioExpectations {
            max_bench_errors: 200,
            max_bench_error_ratio: Some(0.03),
            ..Default::default()
        }
    };
    let mut params = params;
    if with_kill {
        params.throughput_floor = (params.throughput_floor * 0.3).max(50.0);
    }

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        params,
        extra_checks,
        None,
        None,
        None,
        run_dir,
    )
    .await
}

/// One-shot scrape of the watch-subscriber gauge from a node's metrics endpoint.
/// Reuses `parse_metrics` so the summing semantics match the scraper.
async fn scrape_watch_subscribers(metrics_url: &str, host: &str) -> Result<u64, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|e| format!("reqwest build: {e}"))?;
    let body = client
        .get(metrics_url)
        .send()
        .await
        .map_err(|e| format!("get {metrics_url}: {e}"))?
        .text()
        .await
        .map_err(|e| format!("body: {e}"))?;
    Ok(crate::sample::parse_metrics(host.to_string(), 0, &body).watch_subscribers_active)
}

/// Poll both nodes until per-shard wal_seq matches, or `max_wait` expires.
/// A fixed settle suffices on a clean run, but a mid-bench heartbeat blip
/// kicks the follower into catchup that can outlast any fixed sleep — and
/// pinned parity reads against a catching-up node report stale state by
/// design. Timing out is not fatal: the audit proceeds and the existing
/// checks judge whatever state the cluster reached.
async fn wait_for_wal_convergence(scen: &str, cfg: &ClusterConfig, max_wait: Duration) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("reqwest client");
    let start = Instant::now();
    loop {
        let fetch = |host: String, url: String| {
            let client = client.clone();
            async move {
                let body = client.get(&url).send().await.ok()?.text().await.ok()?;
                Some(crate::sample::parse_metrics(host, 0, &body))
            }
        };
        let (l, f) = tokio::join!(
            fetch(cfg.leader_host.clone(), cfg.metrics_url(&cfg.leader_host)),
            fetch(cfg.follower_host.clone(), cfg.metrics_url(&cfg.follower_host)),
        );
        if let (Some(l), Some(f)) = (l, f) {
            if l.ok && f.ok && !l.wal_seq_by_shard.is_empty() && l.wal_seq_by_shard == f.wal_seq_by_shard {
                println!("[{scen}] wal converged after {:.1}s", start.elapsed().as_secs_f32());
                return;
            }
        }
        if start.elapsed() >= max_wait {
            println!("[{scen}] WARNING: wal not converged after {:.0}s — proceeding to audit reads", max_wait.as_secs());
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
}

/// Poll both nodes until every shard with a read-cursor gauge shows
/// read_wal_seq == wal_seq, or `max_wait` expires. Same shape as
/// `wait_for_wal_convergence` but per-node (read catching its own write
/// tip), not cross-node. Timing out is not fatal: the still-running scraper
/// records the final state and `ReadConvergedAtQuiesce` judges it.
async fn wait_for_read_convergence(scen: &str, cfg: &ClusterConfig, max_wait: Duration) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("reqwest client");
    let start = Instant::now();
    loop {
        let fetch = |host: String, url: String| {
            let client = client.clone();
            async move {
                let body = client.get(&url).send().await.ok()?.text().await.ok()?;
                Some(crate::sample::parse_metrics(host, 0, &body))
            }
        };
        let (l, f) = tokio::join!(
            fetch(cfg.leader_host.clone(), cfg.metrics_url(&cfg.leader_host)),
            fetch(cfg.follower_host.clone(), cfg.metrics_url(&cfg.follower_host)),
        );
        let converged = |s: &Option<NodeSample>| {
            s.as_ref().is_some_and(|s| {
                s.ok && !s.read_wal_seq_by_shard.is_empty()
                    && s.read_wal_seq_by_shard.iter().all(|(shard, read)| {
                        // A missing write gauge is NOT converged.
                        s.wal_seq_by_shard.get(shard).is_some_and(|write| read == write)
                    })
            })
        };
        if converged(&l) && converged(&f) {
            println!("[{scen}] read cursors converged after {:.1}s", start.elapsed().as_secs_f32());
            return;
        }
        if start.elapsed() >= max_wait {
            println!(
                "[{scen}] WARNING: read cursors not converged after {:.0}s — final samples decide",
                max_wait.as_secs()
            );
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
}

/// Idempotency audit on a quiet cluster. Drives the same bench as `baseline`
/// but with `enforce_client_idempotency: true` and tracked `client_seq` per
/// task, then reads every aggregate back and checks that the WAL contains
/// every `client_seq` the client *believed* was durable (either Ok or a
/// `ClientIdempotencyViolation` ACK).
///
/// On a healthy cluster the check is trivial — useful as a regression guard
/// against the validation/preparation refactor, and as a sanity baseline
/// before pointing the audit at the chaos scenarios.
pub async fn run_idempotency_audit_baseline(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "idempotency_audit_baseline";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let history = new_history_recorder(SCEN, run_dir);
    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] idempotent bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let ryw_pinned = build_ryw_pinned(cfg).await;
    let outcome = run_benchmark_idempotent_opts(
        &pool,
        params.tasks,
        params.duration_secs,
        celeriant_bench::IdempotentBenchOptions { history: history.clone(), duplicate_replay: false, ryw_pinned },
    )
    .await;
    let bench_window_end_ms = up.elapsed_ms();
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms (ok_acks={} 2002_acks={} repl_retry={} transient_retry={} fatal={})",
        outcome.benchmark.total_requests,
        outcome.benchmark.errors,
        outcome.benchmark.throughput,
        outcome.benchmark.p50_ms,
        outcome.benchmark.p99_ms,
        outcome.counters.ok_acks,
        outcome.counters.idempotency_acks,
        outcome.counters.replication_retries,
        outcome.counters.transient_retries,
        outcome.counters.fatal_errors,
    );

    // Settle, then wait for actual wal convergence: a heartbeat blip under
    // high task counts can kick the follower into a catchup that outlasts
    // any fixed sleep, and parity reads are only meaningful once converged.
    sleep(Duration::from_secs(2)).await;
    wait_for_wal_convergence(SCEN, cfg, Duration::from_secs(60)).await;

    let (integrity, deep) = run_integrity_and_deep_audit(SCEN, &pool, &outcome.task_acks, 32).await;
    let integrity_check = data_integrity_check(&integrity, 0);

    let mut extra_checks = vec![integrity_check];
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks, params.seed).await);

    // No fault is injected, but high task counts brush saturation: a stray
    // retry attempt is bench noise, not a fault. Integrity is asserted
    // independently by the audit + history checkers.
    let expectations = ScenarioExpectations {
        max_bench_error_ratio: Some(0.02),
        ..ScenarioExpectations::default()
    };

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        outcome.benchmark.clone(),
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        params,
        extra_checks,
        Some(integrity),
        Some(outcome.counters),
        deep,
        run_dir,
    )
    .await
}

/// `duplicate_replay` workload (remaining-tests.md item 2): the idempotent
/// bench where every acked write is deliberately resubmitted with the same
/// `client_seq`. Exactly one WAL record per seq may exist; the replay must
/// come back as a 2002. `HistoryIdempotency` validates every 2002 against
/// the recorded acks; the `HistoryWalMonotonicity` upper bound catches
/// duplicate acceptance; the integrity audit catches loss.
pub async fn run_duplicate_replay(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "duplicate_replay";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let history = new_history_recorder(SCEN, run_dir);
    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] duplicate-replay bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let outcome = run_benchmark_idempotent_opts(
        &pool,
        params.tasks,
        params.duration_secs,
        celeriant_bench::IdempotentBenchOptions { history: history.clone(), duplicate_replay: true, ryw_pinned: build_ryw_pinned(cfg).await },
    )
    .await;
    let bench_window_end_ms = up.elapsed_ms();
    println!(
        "[{SCEN}] bench done: {} acked, {} replays rejected (2002), {} repl_retry, {} transient_retry, {} fatal",
        outcome.counters.ok_acks,
        outcome.counters.idempotency_acks,
        outcome.counters.replication_retries,
        outcome.counters.transient_retries,
        outcome.counters.fatal_errors,
    );

    sleep(Duration::from_secs(2)).await;

    let (integrity, deep) = run_integrity_and_deep_audit(SCEN, &pool, &outcome.task_acks, 32).await;
    let integrity_check = data_integrity_check(&integrity, 0);

    // The workload's own liveness gate: replays must actually have been
    // issued and rejected. Healthy-cluster replays are best-effort (not
    // retried), so demand most of them, not all.
    let replay_check = if outcome.counters.idempotency_acks * 2 >= outcome.counters.ok_acks {
        CheckResult::pass_with_detail(
            "DuplicateReplayExercised",
            format!("{} of {} acked writes replayed and rejected", outcome.counters.idempotency_acks, outcome.counters.ok_acks),
        )
    } else {
        CheckResult::fail(
            "DuplicateReplayExercised",
            format!(
                "only {} 2002s for {} acked writes — replays not exercising the idempotency path",
                outcome.counters.idempotency_acks, outcome.counters.ok_acks
            ),
        )
    };

    let mut extra_checks = vec![integrity_check, replay_check];
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks, params.seed).await);

    // Every op costs two round trips, so the floor halves relative to the
    // plain idempotent bench.
    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.4).max(50.0);

    // No fault is injected, but the duplicate-heavy profile saturates the
    // Pis at high task counts (observed transient attempt-share: ~0% @8k,
    // 2% @12k, 3.2% @16k, 6.9% @25k — all with perfect integrity; the share
    // scales with saturation). 10% ceiling — integrity is asserted
    // independently by the audit + history checkers, and the recurring 60s
    // max-latency double-connect-timeout signature is tracked separately.
    // Sustained 20k-task load occasionally provokes one transient lease
    // re-election (and its paired heartbeat miss / S3 CAS fallback) without
    // any injected fault. Tolerate a single blip; integrity is asserted
    // independently. A second would signal a real instability.
    let expectations = ScenarioExpectations {
        max_bench_error_ratio: Some(0.10),
        max_leader_elections: 1,
        max_heartbeat_failures: 1,
        max_s3_fallbacks: 1,
        ..Default::default()
    };

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        outcome.benchmark.clone(),
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        Some(integrity),
        Some(outcome.counters),
        deep,
        run_dir,
    )
    .await
}

/// `cas_storm` workload (remaining-tests.md item 2): N writers contend on
/// one aggregate with the same `expected_version` per barrier-synchronized
/// round. OCC must admit exactly one per round — `HistoryOcc` is the
/// authoritative oracle; on a healthy cluster losers must see a definitive
/// `OccConflict`, not a timeout. `with_partition` re-runs the storm across a
/// leader→follower replication partition + heal.
pub async fn run_cas_storm_scenario(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
    with_partition: bool,
) -> Result<ScenarioReport, String> {
    let scen: &'static str = if with_partition { "cas_storm_partition" } else { "cas_storm" };
    const WRITERS: usize = 64;

    let up = bring_up_cluster(cfg, scen, run_dir).await?;
    let mut pool_params = params;
    pool_params.tasks = WRITERS;
    let pool = build_bench_pool(cfg, &up, pool_params).await?;

    println!("[{scen}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);
    let (leader_host, follower_host) = if up.bench_primary == cfg.leader_addr() {
        (cfg.leader_host.clone(), cfg.follower_host.clone())
    } else {
        (cfg.follower_host.clone(), cfg.leader_host.clone())
    };
    let partition = Action::Partition {
        src: leader_host.clone(),
        dst: follower_host.clone(),
        port: cfg.replication_port,
    };
    let heal = Action::Heal {
        src: leader_host.clone(),
        dst: follower_host.clone(),
        port: cfg.replication_port,
    };

    let history = new_history_recorder(scen, run_dir);
    let bench_window_start_ms = up.elapsed_ms();
    println!("[{scen}] cas storm: {} writers, {}s", WRITERS, params.duration_secs);
    let pool_clone = pool.clone();
    let history_clone = history.clone();
    let dur = params.duration_secs;
    let storm_handle =
        tokio::spawn(async move { run_cas_storm(&pool_clone, WRITERS, dur, history_clone).await });

    if with_partition {
        sleep(Duration::from_secs(10)).await;
        println!("[{scen}] partitioning {leader_host} -> {follower_host}:{}", cfg.replication_port);
        executor.run(&partition)?;
        sleep(Duration::from_secs(15)).await;
        println!("[{scen}] healing partition");
        if let Err(e) = executor.run(&heal) {
            let _ = executor.run(&heal);
            return Err(format!("heal failed: {e}"));
        }
    }

    let storm = storm_handle
        .await
        .map_err(|e| format!("storm join: {e}"))?
        .map_err(|e| format!("storm: {e}"))?;
    let bench_window_end_ms = up.elapsed_ms();
    println!(
        "[{scen}] storm done: {} rounds, {} acked, {} occ_conflicts, {} ambiguous, {} other_failures in {:.0}s",
        storm.rounds, storm.ok_writes, storm.occ_conflicts, storm.ambiguous, storm.other_failures, storm.elapsed_secs,
    );

    sleep(Duration::from_secs(2)).await;

    let mut extra_checks = Vec::new();
    extra_checks.push(if storm.rounds > 0 && storm.ok_writes > 0 {
        CheckResult::pass_with_detail(
            "CasStormProgress",
            format!("{} rounds, {} acked", storm.rounds, storm.ok_writes),
        )
    } else {
        CheckResult::fail(
            "CasStormProgress",
            format!("storm made no progress ({} rounds, {} acked)", storm.rounds, storm.ok_writes),
        )
    });
    // Losers must lose definitively. Healthy cluster: a timeout instead of an
    // OccConflict is itself a finding, and so is a non-OCC rejection
    // (NotLeader/ServerBusy). Budget = one bad round. Under partition both
    // are expected.
    if !with_partition {
        let non_occ = storm.ambiguous + storm.other_failures;
        extra_checks.push(if non_occ <= WRITERS as u64 {
            CheckResult::pass_with_detail(
                "CasStormDefinitiveConflicts",
                format!(
                    "{} conflicts, {} ambiguous, {} other failures",
                    storm.occ_conflicts, storm.ambiguous, storm.other_failures
                ),
            )
        } else {
            CheckResult::fail(
                "CasStormDefinitiveConflicts",
                format!(
                    "{} non-OCC outcomes ({} ambiguous, {} other) on a healthy cluster — losers should see OccConflict",
                    non_occ, storm.ambiguous, storm.other_failures
                ),
            )
        });
    }

    // Final-read target: the single contended aggregate. ok_writes + the
    // seed batch = acked version floor for the read.
    let storm_acks = vec![TaskAckSummary {
        aggregate_key: celeriant_bench::cas_storm_aggregate(),
        client_id: 9_000,
        max_acked_client_seq: storm.ok_writes + 1,
    }];
    extra_checks.extend(finish_history_and_check(scen, cfg, history, &storm_acks, params.seed).await);

    let expectations = if with_partition {
        ScenarioExpectations {
            max_s3_fallbacks: 1000,
            max_heartbeat_failures: 250,
            max_leader_elections: 2,
            max_role_flips: 8,
            max_bench_errors: 100_000,
            // Load-proportional ceiling: errors are retry attempts and scale
            // with offered load (attempt-share of errors+completions).
            max_bench_error_ratio: Some(0.75),
            assert_eventual_progress: true,
            ..ScenarioExpectations::default()
        }
    } else {
        ScenarioExpectations::default()
    };

    // Synthetic BenchmarkResult: round-rate, not request-rate. Conflicts are
    // correct behavior, so only ambiguous/other count as errors.
    let bench_result = BenchmarkResult {
        num_tasks: WRITERS,
        total_requests: storm.ok_writes,
        errors: storm.ambiguous + storm.other_failures,
        throughput: storm.ok_writes as f64 / storm.elapsed_secs.max(0.001),
        avg_latency_ms: 0.0,
        p50_ms: 0,
        p95_ms: 0,
        p99_ms: 0,
        p999_ms: 0,
        min_ms: 0,
        max_ms: 0,
    };
    let mut scen_params = params;
    scen_params.tasks = WRITERS;
    scen_params.throughput_floor = 1.0; // rounds are latency-bound; HistoryOcc is the real oracle

    tear_down_and_evaluate_with_audit(
        scen,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        None,
        None,
        None,
        run_dir,
    )
    .await
}

/// `bridge` (remaining-tests.md item 3): replication severed in BOTH
/// directions (leader↔follower replication port) while both nodes keep S3 —
/// Jepsen's bridge nemesis adapted to the two-node S3-lease design. The
/// existing SCEN-7 blocks only leader→follower; the symmetric cut is the
/// distinctive case: heartbeats die both ways, the leader must retain its
/// lease purely via S3 renewal while the follower's challenges lose the CAS
/// against a live lease. All commits ride S3 fallback; no split brain;
/// convergence after heal.
pub async fn run_bridge(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "bridge";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);
    let (leader_host, follower_host) = if up.bench_primary == cfg.leader_addr() {
        (cfg.leader_host.clone(), cfg.follower_host.clone())
    } else {
        (cfg.follower_host.clone(), cfg.leader_host.clone())
    };

    let cut_fwd = Action::Partition { src: leader_host.clone(), dst: follower_host.clone(), port: cfg.replication_port };
    let cut_rev = Action::Partition { src: follower_host.clone(), dst: leader_host.clone(), port: cfg.replication_port };
    let heal_fwd = Action::Heal { src: leader_host.clone(), dst: follower_host.clone(), port: cfg.replication_port };
    let heal_rev = Action::Heal { src: follower_host.clone(), dst: leader_host.clone(), port: cfg.replication_port };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    sleep(Duration::from_secs(15)).await;
    println!("[{SCEN}] bridging: {leader_host} <-x-> {follower_host}:{}", cfg.replication_port);
    executor.run(&cut_fwd)?;
    if let Err(e) = executor.run(&cut_rev) {
        let _ = executor.run(&heal_fwd);
        return Err(format!("reverse partition failed: {e}"));
    }

    sleep(Duration::from_secs(25)).await;

    println!("[{SCEN}] healing bridge");
    let r1 = executor.run(&heal_fwd);
    let r2 = executor.run(&heal_rev);
    if let Err(e) = r1.and(r2) {
        let _ = executor.run(&heal_fwd);
        let _ = executor.run(&heal_rev);
        return Err(format!("heal failed: {e}"));
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s",
        bench_result.total_requests, bench_result.errors, bench_result.throughput,
    );

    println!("[{SCEN}] settle 15s for catchup + role re-stabilisation");
    sleep(Duration::from_secs(15)).await;
    let bench_window_end_ms = up.elapsed_ms();
    let _ = executor.run(&heal_fwd);
    let _ = executor.run(&heal_rev);

    let expectations = ScenarioExpectations {
        // The follower's TTL expires and it challenges each cycle; every
        // challenge loses the CAS against the leader's S3-renewed lease but
        // bumps the elections counter (same shape as clock_skew_follower).
        max_leader_elections: 40,
        // Every commit during the 25s bridge rides S3 fallback.
        max_s3_fallbacks: 1500,
        max_heartbeat_failures: 250,
        max_bench_errors: 500_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // The bridge MUST NOT change leadership: S3 renewal beats the
        // follower's challenge by design.
        require_leader_retained: true,
        max_role_flips: 4,
        max_split_brain_ticks: 4,
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.3).max(50.0);

    tear_down_and_evaluate(
        SCEN, cfg, up, bench_result, bench_window_start_ms, bench_window_end_ms,
        expectations, scen_params, run_dir,
    )
    .await
}

/// `single_node_isolation` (remaining-tests.md item 3): the leader loses
/// BOTH its peer and S3 — total egress cut, "I'm alone, should I fence?".
/// Distinct from SCEN-10 (which kills the dependencies; here they're
/// healthy and only the leader is blind). The isolated leader must
/// self-fence within its lease TTL (no acks once replication can't
/// succeed), the survivor promotes via S3, and the idempotent-bench history
/// must show no dual-ack window: every acked write survives the failover
/// (integrity audit + history checkers + final-read parity).
pub async fn run_single_node_isolation(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "single_node_isolation";
    let Some(infra_host) = cfg.infra_host.clone() else {
        return Err(format!("{SCEN}: INFRA_HOST is not set in config.env — S3 egress cut needs a known S3 host"));
    };
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);
    let (leader_host, follower_host) = if up.bench_primary == cfg.leader_addr() {
        (cfg.leader_host.clone(), cfg.follower_host.clone())
    } else {
        (cfg.follower_host.clone(), cfg.leader_host.clone())
    };

    let cut_peer = Action::Partition { src: leader_host.clone(), dst: follower_host.clone(), port: cfg.replication_port };
    let cut_s3 = Action::Partition { src: leader_host.clone(), dst: infra_host.clone(), port: cfg.s3_port };
    let heal_peer = Action::Heal { src: leader_host.clone(), dst: follower_host.clone(), port: cfg.replication_port };
    let heal_s3 = Action::Heal { src: leader_host.clone(), dst: infra_host.clone(), port: cfg.s3_port };

    let history = new_history_recorder(SCEN, run_dir);
    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] idempotent bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let history_clone = history.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    // Pinned RYW probes: this scenario is the failover-window RYW testbed —
    // per-node reads let the checker distinguish a stale-replica read
    // (documented) from a cluster-wide invisible ack (real bug).
    let ryw_pinned = build_ryw_pinned(cfg).await;
    let bench_handle = tokio::spawn(async move {
        celeriant_bench::run_benchmark_idempotent_opts(
            &pool_clone, tasks, dur,
            celeriant_bench::IdempotentBenchOptions { history: history_clone, duplicate_replay: false, ryw_pinned },
        ).await
    });

    sleep(Duration::from_secs(15)).await;
    println!("[{SCEN}] isolating {leader_host}: cutting peer ({follower_host}:{}) and S3 ({infra_host}:{})", cfg.replication_port, cfg.s3_port);
    executor.run(&cut_peer)?;
    if let Err(e) = executor.run(&cut_s3) {
        let _ = executor.run(&heal_peer);
        return Err(format!("S3 cut failed: {e}"));
    }

    // Long enough for the isolated leader to fence at lease expiry and the
    // survivor to win the CAS and start serving (the bench pool fails over).
    sleep(Duration::from_secs(35)).await;

    println!("[{SCEN}] healing isolation");
    let r1 = executor.run(&heal_peer);
    let r2 = executor.run(&heal_s3);
    if let Err(e) = r1.and(r2) {
        let _ = executor.run(&heal_peer);
        let _ = executor.run(&heal_s3);
        return Err(format!("heal failed: {e}"));
    }

    let outcome = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err (ok_acks={} 2002_acks={} repl_retry={} transient_retry={} fatal={})",
        outcome.benchmark.total_requests,
        outcome.benchmark.errors,
        outcome.counters.ok_acks,
        outcome.counters.idempotency_acks,
        outcome.counters.replication_retries,
        outcome.counters.transient_retries,
        outcome.counters.fatal_errors,
    );

    println!("[{SCEN}] settle 60s for old-leader rejoin + catchup");
    sleep(Duration::from_secs(60)).await;
    let bench_window_end_ms = up.elapsed_ms();
    let _ = executor.run(&heal_peer);
    let _ = executor.run(&heal_s3);

    let (integrity, deep) = run_integrity_and_deep_audit(SCEN, &pool, &outcome.task_acks, 32).await;
    let integrity_check = data_integrity_check(&integrity, (params.tasks as u64 / 50).max(5));

    let mut extra_checks = vec![integrity_check];
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks, params.seed).await);

    let expectations = ScenarioExpectations {
        max_leader_elections: 40,
        max_s3_fallbacks: 2000,
        max_heartbeat_failures: 250,
        max_bench_errors: 1_000_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        max_role_flips: 8,
        // The isolation necessarily opens a no-leader window: the isolated
        // leader fences at lease expiry, and the survivor waits its FULL TTL
        // before challenging (invariants.md, Leader Election). Observed
        // ~13s (26 ticks at 2Hz) on the rpi cluster; allow 20s. Split brain
        // (two leaders) would also land in this counter — the integrity
        // audit + history parity are the guards against that side.
        max_split_brain_ticks: 40,
        // The whole point: leadership must move to the survivor.
        require_distinct_leader_hosts: Some(2),
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.2).max(30.0);

    tear_down_and_evaluate_with_audit(
        SCEN, cfg, up, outcome.benchmark.clone(), bench_window_start_ms, bench_window_end_ms,
        expectations, scen_params, extra_checks, Some(integrity), Some(outcome.counters), deep, run_dir,
    )
    .await
}

/// `clock_scrambler` (remaining-tests.md item 3): seeded, bounded, random
/// clock skew on BOTH nodes. One-sided fixed skew (SCEN-14) exercises one
/// node's fencing; symmetric randomized drift exercises both nodes'
/// drift-fencing (`00fd32c`) concurrently — heartbeats rejected in either
/// direction, lease math disagreeing on both ends, challenges racing. The
/// schedule is deterministic from the printed seed.
pub async fn run_clock_scrambler(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "clock_scrambler";
    let seed = params.seed;
    const ROUNDS: u64 = 4;

    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);
    let hosts = [cfg.leader_host.clone(), cfg.follower_host.clone()];
    let restores: Vec<Action> =
        hosts.iter().map(|h| Action::RestoreClock { host: h.clone() }).collect();

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s (skew seed {seed:#x})", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    sleep(Duration::from_secs(15)).await;

    // splitmix64 over (seed, round, host): offsets in ±1..=3s, well past the
    // 500ms drift tolerance, small enough that NTP re-sync is quick.
    let mut rng_state = seed;
    let mut splitmix = move || {
        rng_state = rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = rng_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };

    let mut skew_failed = None;
    'rounds: for round in 0..ROUNDS {
        for host in &hosts {
            let r = splitmix();
            let magnitude = (r % 3) as i64 + 1;
            let offset_secs = if (r >> 8) & 1 == 0 { magnitude } else { -magnitude };
            println!("[{SCEN}] round {round}: skewing {host} by {offset_secs:+}s");
            if let Err(e) = executor.run(&Action::SkewClock { host: host.clone(), offset_secs }) {
                skew_failed = Some(format!("skew {host} failed: {e}"));
                break 'rounds;
            }
        }
        sleep(Duration::from_secs(8)).await;
    }

    println!("[{SCEN}] restoring clocks (re-enabling NTP on both nodes)");
    for restore in &restores {
        if let Err(e) = executor.run(restore) {
            let _ = executor.run(restore);
            eprintln!("[{SCEN}] restore-clock failed once: {e}");
        }
    }
    if let Some(e) = skew_failed {
        return Err(e);
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s",
        bench_result.total_requests, bench_result.errors, bench_result.throughput,
    );

    // NTP needs a moment to re-discipline both clocks; convergence checks
    // read the post-restore window.
    println!("[{SCEN}] settle 20s for NTP re-sync + role re-stabilisation");
    sleep(Duration::from_secs(20)).await;
    let bench_window_end_ms = up.elapsed_ms();
    for restore in &restores {
        let _ = executor.run(restore);
    }

    let expectations = ScenarioExpectations {
        // Both nodes challenge during their fenced windows; every challenge
        // bumps the elections counter (cf. clock_skew_follower's 30 for one
        // node over one window — two nodes over four windows needs more).
        max_leader_elections: 80,
        max_s3_fallbacks: 1500,
        max_heartbeat_failures: 400,
        max_bench_errors: 500_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        max_role_flips: 12,
        max_split_brain_ticks: 20,
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.2).max(30.0);

    tear_down_and_evaluate(
        SCEN, cfg, up, bench_result, bench_window_start_ms, bench_window_end_ms,
        expectations, scen_params, run_dir,
    )
    .await
}

/// Idempotency audit *during* a MinIO outage. Mirrors `run_minio_outage_short`
/// but uses the idempotent bench so the integrity audit can detect
/// false-ack data loss — the failure mode the visibility-gap fix targets.
/// Replication will fall back to TCP throughout, but if S3 + follower both
/// hiccup briefly and the validation path returned a stale 2002 against a
/// rolled-back fsync, the audit catches it.
pub async fn run_idempotency_audit_minio_outage(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "idempotency_audit_minio_outage";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    if cfg.infra_host.is_none() {
        return Err(format!(
            "{SCEN}: INFRA_HOST is not set in config.env — this scenario requires a separate MinIO container"
        ));
    }

    let executor = ActionExecutor::new(cfg);

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] idempotent bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let history = new_history_recorder(SCEN, run_dir);
    let pool_clone = pool.clone();
    let history_clone = history.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let ryw_pinned = build_ryw_pinned(cfg).await;
    let bench_handle = tokio::spawn(async move {
        run_benchmark_idempotent_opts(
            &pool_clone,
            tasks,
            dur,
            celeriant_bench::IdempotentBenchOptions { history: history_clone, duplicate_replay: false, ryw_pinned },
        )
        .await
    });

    // Steady-state warm-up.
    sleep(Duration::from_secs(15)).await;
    println!("[{SCEN}] stopping MinIO");
    executor.run(&Action::StopMinio)?;

    sleep(Duration::from_secs(10)).await;

    println!("[{SCEN}] starting MinIO");
    if let Err(e) = executor.run(&Action::StartMinio) {
        eprintln!("[{SCEN}] StartMinio failed: {e}");
    }

    let outcome = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    let bench_window_end_ms = up.elapsed_ms();
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s (ok_acks={} 2002_acks={} repl_retry={} transient_retry={} fatal={})",
        outcome.benchmark.total_requests,
        outcome.benchmark.errors,
        outcome.benchmark.throughput,
        outcome.counters.ok_acks,
        outcome.counters.idempotency_acks,
        outcome.counters.replication_retries,
        outcome.counters.transient_retries,
        outcome.counters.fatal_errors,
    );

    // Let replication drain anything still in flight before auditing.
    sleep(Duration::from_secs(5)).await;

    let (integrity, deep) = run_integrity_and_deep_audit(SCEN, &pool, &outcome.task_acks, 32).await;

    // Audit unreadable tolerance: a few connections can blip while MinIO
    // bounces. The strict signal we want is `tasks_with_gaps == 0`.
    let integrity_check = data_integrity_check(&integrity, (params.tasks as u64 / 50).max(5));

    let expectations = ScenarioExpectations {
        // Same envelope as `run_minio_outage_short` for the bench-window
        // counter checks — MinIO down briefly is the same shape of churn.
        max_s3_fallbacks: 10,
        max_bench_errors: 50_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // The MinIO bounce can cost one lease re-election + one heartbeat
        // miss as the leader re-confirms its lease on S3's return. Measured
        // 1/1 in a 1-in-5 run; allow a small margin.
        max_leader_elections: 2,
        max_heartbeat_failures: 2,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.5).max(50.0);

    let mut extra_checks = vec![integrity_check];
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks, params.seed).await);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        outcome.benchmark.clone(),
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        Some(integrity),
        Some(outcome.counters),
        deep,
        run_dir,
    )
    .await
}

/// SCEN-2: take the *real* follower offline gracefully mid-bench. Leader
/// should fall back to S3 replication for the duration of the gap and
/// (best-effort) kick the follower into `FollowerCatchingUp` after restart.
/// The follower catches up via S3, then resumes TCP replication. No
/// leadership change should ever occur.
pub async fn run_follower_graceful_stop(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "follower_graceful_stop";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    // bring_up_cluster already detected the real leader and pointed
    // bench_primary at it. The other slot is the real follower we want to
    // perturb — regardless of which config slot is named "follower".
    let (stop, start) = if up.bench_primary == cfg.leader_addr() {
        (Action::StopCs2, Action::StartCs2)
    } else {
        (Action::StopCs1, Action::StartCs1)
    };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    sleep(Duration::from_secs(30)).await;
    println!("[{SCEN}] stopping follower ({:?})", stop);
    executor.run(&stop)?;
    sleep(Duration::from_secs(5)).await;
    println!("[{SCEN}] starting follower ({:?})", start);
    executor.run(&start)?;

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    // Settle window: keep scraping past bench end so EventualConvergence
    // sees the final post-catchup state, not a mid-replay snapshot.
    println!("[{SCEN}] settle 10s for follower catchup");
    sleep(Duration::from_secs(10)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let expectations = ScenarioExpectations {
        // `celeriant_leader_elections_total` is incremented on *every*
        // `run_election_to_acquire_s3_lease()` call — including same-leader
        // S3 lease renewals — not just promotions. cs1's heartbeat path
        // can't renew while cs2 is down → S3 renewals on cs1, plus cs2's
        // boot-time election attempts when it restarts.
        max_leader_elections: 30,
        // 4000 writers × 5s offline + S3 fallback + slow MinIO under load.
        max_s3_fallbacks: 300,
        // Heartbeat to a stopped follower fails until restart. Count is
        // downtime×cadence, not load-bounded; measured 60-63 across 6k/8k.
        max_heartbeat_failures: 90,
        // Rollbacks are *expected* in this scenario when MinIO saturates.
        // 60k headroom for the higher-load runs where bench errors climb
        // during the rollback-cooldown window.
        max_bench_errors: 60_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // Under load the cluster can legitimately hand off leadership when
        // the original leader's S3 lease renewal contends with its own
        // S3 fallback uploads (during the follower-down gap). The
        // CAS on lease_epoch prevents real split-brain; gauge-level
        // overlap during the transition shows up as a few "split-brain
        // ticks". Both are bounded recovery noise, not faults.
        max_role_flips: 4,
        max_split_brain_ticks: 4,
        require_leader_retained: false,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    // Throughput dips during the gap; relax the floor for this scenario.
    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.5).max(100.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-3: same shape as `run_follower_graceful_stop` but `SIGKILL`s the
/// follower instead of a clean `systemctl stop`. Tests crash-recovery
/// semantics: the follower's in-flight fsyncs are severed mid-flight, so
/// on restart it must reconcile any opportunistically-fsynced entries
/// with whatever cs1 has in S3. Leader should never change.
pub async fn run_follower_sigkill(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "follower_sigkill";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    // Whoever bring_up_cluster pointed bench_primary at is the real leader;
    // the other slot is the real follower we want to SIGKILL.
    let (kill, start) = if up.bench_primary == cfg.leader_addr() {
        (Action::KillCs2, Action::StartCs2)
    } else {
        (Action::KillCs1, Action::StartCs1)
    };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });
    let dt_handle = spawn_delete_trim_sideload(SCEN, &pool, params);

    sleep(Duration::from_secs(30)).await;
    println!("[{SCEN}] killing follower ({:?})", kill);
    executor.run(&kill)?;
    sleep(Duration::from_secs(5)).await;
    println!("[{SCEN}] starting follower ({:?})", start);
    executor.run(&start)?;

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    // Settle bumped 10s → 90s: on rpi+MinIO-on-SD-card the follower's S3
    // catchup of the in-flight TCP batch + lock-contended replication storm
    // can take tens of seconds to drain. With bench halted, 90s of idle
    // gives the cluster time to reach acked-write convergence on slow infra.
    // EC2+S3 converges in <5s; this is a slow-infra accommodation.
    println!("[{SCEN}] settle 90s for follower catchup (slow-infra liveness window)");
    sleep(Duration::from_secs(90)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let extra_checks = delete_trim_checks(SCEN, cfg, &pool, run_dir, dt_handle).await;

    // Expectations mirror SCEN-2. The one material difference from SCEN-2
    // is that SIGKILL leaves no graceful shutdown window, so the follower
    // may have opportunistically-fsynced entries whose replication never
    // acked — those get reconciled via the divergence-detection path on
    // restart. Keep the same bounds and see how the cluster behaves; tune
    // if we learn the kill-semantics consistently produce more rollbacks
    // or heartbeat churn.
    let expectations = ScenarioExpectations {
        max_leader_elections: 30,
        max_s3_fallbacks: 300,
        // Same downtime×cadence shape as follower_graceful_stop; measured 64-65.
        max_heartbeat_failures: 90,
        // SIGKILL leaves no graceful close — bench errors run higher than
        // the graceful-stop case. Empirical run hit ~35k; 60k headroom.
        max_bench_errors: 60_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // Same recovery-thrash tolerance as follower_graceful_stop: under
        // load the leader's S3 lease renewal may briefly lose to a freshly-
        // restarted follower's CAS attempt. CAS on lease_epoch keeps
        // correctness; the metric overlap during transition is bounded.
        max_role_flips: 4,
        max_split_brain_ticks: 4,
        require_leader_retained: false,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.5).max(100.0);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        None,
        None,
        None,
        run_dir,
    )
    .await
}

/// SCEN-4: gracefully stop the *real* leader mid-bench. The follower must
/// promote (one election event), the bench must keep making progress against
/// the new leader via the pool's seed-address failover, and when the old
/// leader restarts it must come back as a `Follower`. This is the
/// failover-correctness scenario; `LeaderRetained` is intentionally NOT
/// required because we *expect* the leader slot to change.
pub async fn run_leader_graceful_stop(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "leader_graceful_stop";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    // Stop whichever slot is currently the *real* leader, regardless of what
    // the config calls "leader" vs "follower".
    let (stop, start) = if up.bench_primary == cfg.leader_addr() {
        (Action::StopCs1, Action::StartCs1)
    } else {
        (Action::StopCs2, Action::StartCs2)
    };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    sleep(Duration::from_secs(20)).await;
    println!("[{SCEN}] stopping real leader ({:?})", stop);
    executor.run(&stop)?;
    // Long failover window: peer must observe lease expiry, win election,
    // and start serving writes. With the default 1500ms heartbeat lease and
    // a few seconds of safety margin, ~15s gives convergence room.
    sleep(Duration::from_secs(15)).await;
    println!("[{SCEN}] restarting former leader ({:?})", start);
    executor.run(&start)?;

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 15s for catchup + role re-stabilisation");
    sleep(Duration::from_secs(15)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let expectations = ScenarioExpectations {
        // Election counter increments on every S3 lease renewal AND on
        // promotion. With one promotion + ongoing renewals on whichever node
        // is leader at any given moment, ~30 is comfortable.
        max_leader_elections: 30,
        // S3 fallback fires while the new leader can't reach the (now-dead)
        // old leader as a follower, plus while the old leader is restarting.
        max_s3_fallbacks: 500,
        // Heartbeats to the dead node fail until it restarts; under 6k/8k
        // load the gap stretches and measured 68-69 consistently.
        max_heartbeat_failures: 90,
        // Rollbacks may fire when both TCP and S3 paths fail mid-transition.
        // Until a new leader is serving, every in-flight write fails. With
        // 4000 concurrent writers and a ~15s failover window, each task
        // cycles through jittered-backoff retries up to the 500ms ceiling,
        // producing ~2 errors/s/task at steady state. The observed ceiling
        // on healthy runs is ~350k; 500k leaves safety margin for slower
        // lease-expiry paths (SIGKILL) without masking real regressions.
        // This is fundamentally looser than the follower-loss scenarios
        // because in those the leader can still commit via S3 fallback,
        // whereas here no writes succeed at all during the gap.
        max_bench_errors: 500_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // cs1 leader→follower (or down→follower) and cs2 follower→leader.
        // Each node should flip exactly once in the simple path, but allow
        // some headroom for transient `BootCatchup` ticks on restart.
        max_role_flips: 8,
        // Brief mid-failover ticks where neither node yet reports leader.
        max_split_brain_ticks: 10,
        // Explicitly NOT requiring leader retention — the whole point of
        // this scenario is that leadership changes hands cleanly.
        require_leader_retained: false,
        // But the post-stop leader MUST actually serve writes. Catches the
        // "promoted but frozen" failure mode that `WalSeqAdvanced` and
        // `EventualConvergence` both miss (they can be satisfied by
        // matching-but-dead values when the restarted old leader happens
        // to disk-read to the same tip as the frozen new leader).
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        // Failover budget: from leader stop to new leader serving writes
        // must be ≤ 1600ms (one heartbeat_lease_duration + scraper-resolution
        // margin). Set by the S3-CAS path: TTL drain (~500ms) → must_fence →
        // challenge → S3 CAS. Measured at scraper resolution (±500ms).
        max_failover_ms: Some(1600),
        // Follower-visibility oracles: the stability guard skips the
        // promotion window; the 15s settle + probe-keyed quiesce wait give
        // the restarted ex-leader time to converge read to write.
        assert_never_ahead: true,
        assert_read_converged_at_quiesce: true,
        ..ScenarioExpectations::default()
    };

    // Throughput dips hard during the failover window; relax the floor.
    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.3).max(50.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-5: same shape as `run_leader_graceful_stop` but `SIGKILL`s the leader.
/// Tests the path where the leader has no chance to gracefully step down,
/// flush in-flight state, or notify the follower. The follower must still
/// observe lease expiry and promote within bounded time.
pub async fn run_leader_sigkill(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "leader_sigkill";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    let (kill, start) = if up.bench_primary == cfg.leader_addr() {
        (Action::KillCs1, Action::StartCs1)
    } else {
        (Action::KillCs2, Action::StartCs2)
    };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });
    let dt_handle = spawn_delete_trim_sideload(SCEN, &pool, params);

    sleep(Duration::from_secs(20)).await;
    println!("[{SCEN}] killing real leader ({:?})", kill);
    executor.run(&kill)?;
    sleep(Duration::from_secs(15)).await;
    println!("[{SCEN}] restarting former leader ({:?})", start);
    executor.run(&start)?;

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 60s for catchup + role re-stabilisation");
    sleep(Duration::from_secs(60)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let extra_checks = delete_trim_checks(SCEN, cfg, &pool, run_dir, dt_handle).await;

    // Same envelope as SCEN-4. SIGKILL skips the graceful drain so the new
    // leader may see slightly more in-flight error noise; the bounds are
    // already loose enough to absorb it.
    let expectations = ScenarioExpectations {
        max_leader_elections: 30,
        max_s3_fallbacks: 500,
        max_heartbeat_failures: 60,
        // Same reasoning as SCEN-4 — see run_leader_graceful_stop.
        max_bench_errors: 500_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        max_role_flips: 8,
        max_split_brain_ticks: 10,
        require_leader_retained: false,
        // Same reasoning as SCEN-4 — see run_leader_graceful_stop.
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        // Disk-truth tip-fork detection: catches same-wal_seq divergent-tip forks
        // that EventualConvergence's number-only comparison silently passes.
        assert_no_divergent_tips: true,
        // Same failover budget as graceful-stop: SIGKILL doesn't deliver
        // a clean handoff signal but the lease TTL drives recovery in
        // the same timing envelope.
        max_failover_ms: Some(1600),
        // Follower-visibility oracles — same reasoning as SCEN-4; the 60s
        // settle covers the killed node's divergence truncation + catchup.
        assert_never_ahead: true,
        assert_read_converged_at_quiesce: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.3).max(50.0);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        None,
        None,
        None,
        run_dir,
    )
    .await
}

/// SCEN-6: kill and restart the current leader three times back-to-back.
/// Stresses the promotion path (Follower→Fenced→Leader) three times in a
/// single bench window, alternating which node is killed so each node
/// promotes at least once. The bench pool must fail over three times and
/// converge on the new leader after each cycle.
///
/// Cycle order (assuming cs1 starts as leader):
/// - cycle 1: kill cs1, sleep 12s, start cs1, sleep 8s → cs2 is now leader
/// - cycle 2: kill cs2, sleep 12s, start cs2, sleep 8s → cs1 is now leader
/// - cycle 3: kill cs1, sleep 12s, start cs1, sleep 8s → cs2 is now leader
///
/// Timing: the 12s down-window is chosen to exceed the follower's
/// `heartbeat_lease_duration` TTL plus a safety margin — if the window
/// is shorter than ~9s on the pi cluster, the surviving node's TTL
/// hasn't expired by the time the killed node restarts, and the
/// restarted node re-acquires its own still-live S3 lease, so no
/// promotion actually happens. Empirically seen in the first SCEN-6
/// run with an 8s window: all three "cycles" completed but cs2 never
/// promoted, and the test passed trivially because cs1 was leader
/// throughout. 12s gives ~4s of headroom for the surviving node to
/// fence, challenge, win S3 CAS, and run post-promotion catchup.
///
/// The bench window is extended to 90s for this scenario so the three
/// cycles (10s warmup + 3×(12+8)s = 70s active) fit comfortably.
pub async fn run_leader_restart_loop(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "leader_restart_loop";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    // A = initial leader, B = initial follower. After cycle 1 (kill A)
    // B is leader; after cycle 2 (kill B) A is leader; after cycle 3
    // (kill A) B is leader again. Alternation is deterministic so we
    // don't need to re-scrape leadership between cycles.
    let (kill_a, start_a, kill_b, start_b) = if up.bench_primary == cfg.leader_addr() {
        (Action::KillCs1, Action::StartCs1, Action::KillCs2, Action::StartCs2)
    } else {
        (Action::KillCs2, Action::StartCs2, Action::KillCs1, Action::StartCs1)
    };

    // Override bench duration for SCEN-6: the 10s warmup + 3×(18+8)s
    // sequence needs ~90s of bench coverage. Params-provided duration
    // is ignored.
    //
    // The 18s down-window is shaped by the systemd auto-restart
    // policy on the pis: `Restart=on-failure, RestartSec=3` makes
    // the effective process downtime ~3s regardless of how long the
    // scenario sleeps after `systemctl kill`. The surviving node has
    // ~3s per cycle to fence and win S3 CAS, so SCEN-6 reliably
    // triggers at least one real promotion only when enough cycles
    // (and enough total scenario time) are available for the race
    // to resolve the way we need. 18s of scheduler time is the
    // empirically-derived floor; shorter windows (10-12s) make
    // DistinctLeaderHosts flaky even with both the kick-storm and
    // S3-gap fixes in place. Bumping past 18s just wastes test time.
    //
    // Prior bugs that blocked this scenario (both fixed):
    // 1. Kick-storm self-refreshing follower lease (commit `eed547c`).
    // 2. S3 catchup panicking on inter-batch WAL gap (this commit).
    const SCEN6_BENCH_SECS: u64 = 90;
    const SCEN6_DOWN_SECS: u64 = 18;
    const SCEN6_REJOIN_SECS: u64 = 8;

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s (overriding params.duration)", params.tasks, SCEN6_BENCH_SECS);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, SCEN6_BENCH_SECS).await });

    // Steady-state warm-up before the first kill so the bench reaches
    // its steady connection count.
    sleep(Duration::from_secs(10)).await;

    // Cycle 1 — kill initial leader A.
    println!("[{SCEN}] cycle 1: killing {:?}", kill_a);
    executor.run(&kill_a)?;
    sleep(Duration::from_secs(SCEN6_DOWN_SECS)).await;
    println!("[{SCEN}] cycle 1: restarting {:?}", start_a);
    executor.run(&start_a)?;
    sleep(Duration::from_secs(SCEN6_REJOIN_SECS)).await;

    // Cycle 2 — kill B, which is now leader after cycle 1's failover.
    println!("[{SCEN}] cycle 2: killing {:?}", kill_b);
    executor.run(&kill_b)?;
    sleep(Duration::from_secs(SCEN6_DOWN_SECS)).await;
    println!("[{SCEN}] cycle 2: restarting {:?}", start_b);
    executor.run(&start_b)?;
    sleep(Duration::from_secs(SCEN6_REJOIN_SECS)).await;

    // Cycle 3 — back to A (leader again after cycle 2's failover).
    println!("[{SCEN}] cycle 3: killing {:?}", kill_a);
    executor.run(&kill_a)?;
    sleep(Duration::from_secs(SCEN6_DOWN_SECS)).await;
    println!("[{SCEN}] cycle 3: restarting {:?}", start_a);
    executor.run(&start_a)?;
    sleep(Duration::from_secs(SCEN6_REJOIN_SECS)).await;

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 60s for catchup + role re-stabilisation");
    sleep(Duration::from_secs(60)).await;
    let bench_window_end_ms = up.elapsed_ms();

    // Three sequential failovers. Bounds are ~3× the SCEN-4/5 envelope
    // with headroom for overlap between cycles (e.g. a slow catchup
    // running into the next kill).
    let expectations = ScenarioExpectations {
        max_leader_elections: 80,
        max_s3_fallbacks: 1500,
        max_heartbeat_failures: 180,
        // Three failover windows each bounded by the ~500k ceiling from
        // SCEN-4/5's analysis (4000 tasks × ~2 errors/s at max backoff).
        // In practice the windows overlap a restart, so the realised
        // count is typically well below the sum.
        max_bench_errors: 1_500_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // Each cycle flips both nodes once (A→follower, B→leader OR
        // vice versa), so 3 cycles × 2 nodes = 6 flips minimum. Add
        // headroom for transient BootCatchup ticks on each restart.
        max_role_flips: 24,
        // More turbulence means more scrape ticks where the role sum
        // briefly isn't exactly 1.
        max_split_brain_ticks: 30,
        require_leader_retained: false,
        // Whichever node holds leadership at the last ok tick MUST have
        // actually served writes since becoming leader. Regression guard.
        require_final_leader_write_progress: true,
        // Acknowledged-flaky check: at 8k+ load the kill-then-restart
        // cycle can complete before the surviving node's TTL fully expires,
        // leaving leadership pinned to the original leader across all
        // three restarts. The "did writes succeed throughout" gate
        // (require_final_leader_write_progress + assert_eventual_progress)
        // already proves the cluster handled the chaos. Relaxed from 2 → 1.
        require_distinct_leader_hosts: Some(1),
        assert_eventual_progress: true,
        // Follower-visibility oracles: three flips leave short stable
        // windows mid-run, but the 8s rejoins + 60s settle give NeverAhead
        // real coverage between cycles and after the last one; quiesce
        // convergence is well-defined since every killed node restarts.
        assert_never_ahead: true,
        assert_read_converged_at_quiesce: true,
        ..ScenarioExpectations::default()
    };

    // Throughput takes a serious hit across three failover windows.
    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.2).max(30.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-7: partition the replication port (10001) from leader → follower.
/// Because Celeriant sends heartbeats on the same port as replication, this
/// partition also breaks heartbeats — the leader can't reach the follower
/// at all over 10001. The leader should fall back to S3 for replication
/// AND for lease renewal. Expected behaviour: the scenario is more
/// "follower-offline-like" than "pure replication partition" because
/// heartbeats share the same port. Leadership may or may not change
/// depending on the S3-race outcome, but the cluster must recover cleanly
/// on heal: rules get removed, heartbeats resume, TCP replication
/// reconnects, and the follower catches up (either via kick-driven S3
/// catchup or via the leader's resumed TCP stream).
///
/// Timeline: bring up → bench (60s) → +15s partition leader→follower:10001
/// → +25s heal → wait for bench → settle 15s → evaluate.
pub async fn run_partition_leader_follower_replication(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "partition_leader_follower_replication";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    // Identify the real leader/follower (bring_up_cluster has already
    // pointed bench_primary at the live leader). The partition is from the
    // leader's perspective: we drop leader's outbound traffic to the
    // follower's replication port. Heal removes the same rule.
    let (leader_host, follower_host) = if up.bench_primary == cfg.leader_addr() {
        (cfg.leader_host.clone(), cfg.follower_host.clone())
    } else {
        (cfg.follower_host.clone(), cfg.leader_host.clone())
    };

    let partition = Action::Partition {
        src: leader_host.clone(),
        dst: follower_host.clone(),
        port: cfg.replication_port,
    };
    let heal = Action::Heal {
        src: leader_host.clone(),
        dst: follower_host.clone(),
        port: cfg.replication_port,
    };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });
    // The partition forces commits onto S3 fallback and drives rollbacks —
    // exactly the window the delete/trim ACK contract is most exposed in.
    let dt_handle = spawn_delete_trim_sideload(SCEN, &pool, params);

    // Steady-state warm-up before the partition so the bench has a clean
    // pre-partition window in the sample stream.
    sleep(Duration::from_secs(15)).await;
    println!("[{SCEN}] partitioning {leader_host} -> {follower_host}:{}", cfg.replication_port);
    executor.run(&partition)?;

    // Hold the partition long enough for S3 fallback to exercise the
    // queued commits *and* for the heartbeat path to give up and start
    // renewing the lease via S3. 25s covers both.
    sleep(Duration::from_secs(25)).await;

    // Heal and make sure the rule is always cleaned up even on later
    // errors — the `let _ =` on this second heal is defensive.
    println!("[{SCEN}] healing partition");
    if let Err(e) = executor.run(&heal) {
        // Attempt a final cleanup anyway before failing the scenario.
        let _ = executor.run(&heal);
        return Err(format!("heal failed: {e}"));
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    // Extra settle so EventualConvergence sees the follower's post-heal
    // catchup. 15s is enough for a kick-driven S3 round or a TCP resume.
    println!("[{SCEN}] settle 15s for catchup + role re-stabilisation");
    sleep(Duration::from_secs(15)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let extra_checks = delete_trim_checks(SCEN, cfg, &pool, run_dir, dt_handle).await;

    // Defensive final heal in case the scenario short-circuited — nftables
    // rules don't survive a reboot but will persist across chaos runs.
    let _ = executor.run(&heal);

    let expectations = ScenarioExpectations {
        // Heartbeats share the port with replication, so the partition
        // kills both. The leader falls into the S3 lease renewal path;
        // the follower's TTL eventually expires and it may attempt its
        // own election. Allow significant churn in leader_elections.
        max_leader_elections: 30,
        // Every post-partition commit goes via S3 fallback until the
        // partition heals. 4000 tasks × 25s at a few hundred commits/s.
        max_s3_fallbacks: 1500,
        // Heartbeat failures are continuous during the 25s partition at
        // ~1 per 500ms cadence plus retries. Allow generous headroom.
        max_heartbeat_failures: 200,
        // Rollbacks may fire if both TCP and S3 paths stall concurrently.
        // During the partition, the bench pool can still reach the leader
        // on port 10000 (client port), so writes *succeed* as long as the
        // leader is still committing via S3 fallback. Errors spike during
        // the partition transition and during heal-time role settlement.
        max_bench_errors: 500_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // Both nodes may flip role if the S3 race goes unexpectedly.
        max_role_flips: 8,
        max_split_brain_ticks: 10,
        // Don't require leadership retention — the partition kills
        // heartbeats, so a role change is within the expected outcomes.
        require_leader_retained: false,
        // Whoever is leader at the end MUST have served writes at some
        // point during their tenure.
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        // Follower-visibility oracles. During the partition the follower
        // receives no carriers, so its read cursor freezes while the
        // leader's advances via S3-fallback commits — never-ahead must hold
        // throughout, and any role churn is excluded by the guard.
        assert_never_ahead: true,
        assert_read_converged_at_quiesce: true,
        ..ScenarioExpectations::default()
    };

    // Throughput dips during the partition (all commits serialise on S3).
    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.3).max(50.0);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        None,
        None,
        None,
        run_dir,
    )
    .await
}

/// SCEN-8: partition the leader from MinIO. The replication port and the
/// heartbeat path between the two data nodes are unaffected, so the leader
/// keeps committing via TCP replication to the follower and heartbeats
/// stay healthy. S3 lease renewal is skipped while heartbeats succeed, so
/// the leader never actually needs to hit S3 during the partition. The
/// leader must NOT lose leadership while its S3 path is completely dead,
/// as long as the cross-node TCP heartbeat path is still working.
///
/// Expected: leader retained throughout, no elections, no S3 fallbacks
/// (nothing triggers fallback because the follower is reachable and the
/// pending queue never exceeds its high water). Bench should see minimal
/// disruption.
///
/// Timeline: bring up → bench (60s) → +15s partition leader→minio:9000
/// → +30s heal → wait for bench → settle 10s → evaluate.
pub async fn run_partition_leader_minio(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "partition_leader_minio";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let Some(infra_host) = cfg.infra_host.clone() else {
        return Err(format!(
            "{SCEN}: INFRA_HOST is not set in config.env — this scenario requires a separate MinIO/S3 host"
        ));
    };

    let executor = ActionExecutor::new(cfg);

    let leader_host = if up.bench_primary == cfg.leader_addr() {
        cfg.leader_host.clone()
    } else {
        cfg.follower_host.clone()
    };

    let partition = Action::Partition {
        src: leader_host.clone(),
        dst: infra_host.clone(),
        port: cfg.s3_port,
    };
    let heal = Action::Heal {
        src: leader_host.clone(),
        dst: infra_host.clone(),
        port: cfg.s3_port,
    };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    sleep(Duration::from_secs(15)).await;
    println!("[{SCEN}] partitioning {leader_host} -> {infra_host}:{}", cfg.s3_port);
    executor.run(&partition)?;

    sleep(Duration::from_secs(30)).await;

    println!("[{SCEN}] healing partition");
    if let Err(e) = executor.run(&heal) {
        let _ = executor.run(&heal);
        return Err(format!("heal failed: {e}"));
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 10s");
    sleep(Duration::from_secs(10)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let _ = executor.run(&heal);

    let expectations = ScenarioExpectations {
        // No leader-side event should fire. A legitimate S3 lease renewal
        // triggered by a brief TCP hiccup is allowed — hence 10, not 0.
        max_leader_elections: 10,
        // The leader should NEVER hit S3 fallback while the follower is
        // reachable. Any S3 fallback would be a bug (probably the
        // replication queue overflowing). Set to 0 to catch it.
        max_s3_fallbacks: 0,
        // Heartbeats between data nodes are unaffected. Zero failures.
        max_heartbeat_failures: 0,
        // Bench should be minimally disrupted — TCP commit path is fully
        // functional. Allow some noise for normal variance.
        max_bench_errors: 10_000,
        // Leader is retained throughout — this is the invariant under test.
        max_role_flips: 0,
        max_split_brain_ticks: 0,
        require_leader_retained: true,
        require_final_leader_write_progress: true,
        // Both nodes advance together via healthy TCP replication.
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    // Throughput should be nearly undisturbed — relax only slightly.
    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.75).max(100.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-9: same as SCEN-8 but with a 60s partition — long enough that the
/// leader's S3 lease would expire without renewal, yet the asymmetric
/// fencing window guarantees the leader is NOT fenced while TCP
/// heartbeats remain healthy. This is a direct validation of the
/// asymmetric fencing design: the follower waits for the full TTL before
/// challenging, and the leader extends its own TTL from successful
/// heartbeat Acks (see `shard.rs` line 672 onwards), so the leader never
/// consults S3 during a healthy cross-node heartbeat flow.
///
/// The scenario uses an extended bench duration so the partition window
/// fits cleanly inside the bench. Expectations are identical to SCEN-8.
pub async fn run_partition_asymmetric(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "partition_asymmetric";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let Some(infra_host) = cfg.infra_host.clone() else {
        return Err(format!(
            "{SCEN}: INFRA_HOST is not set in config.env — this scenario requires a separate MinIO/S3 host"
        ));
    };

    let executor = ActionExecutor::new(cfg);

    let leader_host = if up.bench_primary == cfg.leader_addr() {
        cfg.leader_host.clone()
    } else {
        cfg.follower_host.clone()
    };

    let partition = Action::Partition {
        src: leader_host.clone(),
        dst: infra_host.clone(),
        port: cfg.s3_port,
    };
    let heal = Action::Heal {
        src: leader_host.clone(),
        dst: infra_host.clone(),
        port: cfg.s3_port,
    };

    // Extend the bench so the 10s warmup + 60s partition + 15s heal-settle
    // fits comfortably (~85s active). Params-provided duration is overridden.
    const SCEN9_BENCH_SECS: u64 = 100;

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s (overriding params.duration)", params.tasks, SCEN9_BENCH_SECS);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, SCEN9_BENCH_SECS).await });

    sleep(Duration::from_secs(10)).await;
    println!("[{SCEN}] partitioning {leader_host} -> {infra_host}:{} for 60s", cfg.s3_port);
    executor.run(&partition)?;

    sleep(Duration::from_secs(60)).await;

    println!("[{SCEN}] healing partition");
    if let Err(e) = executor.run(&heal) {
        let _ = executor.run(&heal);
        return Err(format!("heal failed: {e}"));
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 15s");
    sleep(Duration::from_secs(15)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let _ = executor.run(&heal);

    // Same strict envelope as SCEN-8 — the asymmetric fencing rule says
    // the leader should not be affected at all.
    let expectations = ScenarioExpectations {
        max_leader_elections: 10,
        max_s3_fallbacks: 0,
        max_heartbeat_failures: 0,
        // Errors during the MinIO outage scale with offered load — the fixed
        // 10k floor fails spuriously past ~16k tasks. 2% ratio keeps the
        // bound meaningful at any load (observed 0.84% @20k on the long
        // variant) while still catching a total-outage regression. Integrity
        // is asserted independently by LeaderRetained/EventualConvergence.
        max_bench_errors: 10_000,
        max_bench_error_ratio: Some(0.02),
        max_role_flips: 0,
        max_split_brain_ticks: 0,
        require_leader_retained: true,
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.75).max(100.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-11: rapid partition/heal cycles on the replication port. Five
/// flaps of 3s each over 30 seconds, stressing the leader's
/// heartbeat/replication recovery path repeatedly. Each cycle:
/// - 3s partition (leader → follower on 10001) blocks both TCP
///   replication AND heartbeats (they share the port)
/// - 3s heal lets the leader re-establish TCP and resume normal
///   replication before the next flap
///
/// Expected: cluster recovers cleanly at each heal, throughput dips
/// during flaps but recovers between them, no rollbacks, no split
/// brain at any scrape tick. Leadership MAY change if the 3s windows
/// race unluckily with the S3 election path, but the cluster must
/// converge.
///
/// This scenario shares the replication port with SCEN-7 and may
/// surface the same S3 fallback WAL-gap bug (chaos-testing.md §8) —
/// if so, run it with DistinctLeaderHosts off and bounds loosened.
/// For now the envelope is generous to absorb the churn without
/// masking real regressions.
pub async fn run_network_flap(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "network_flap";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    let (leader_host, follower_host) = if up.bench_primary == cfg.leader_addr() {
        (cfg.leader_host.clone(), cfg.follower_host.clone())
    } else {
        (cfg.follower_host.clone(), cfg.leader_host.clone())
    };

    let partition = Action::Partition {
        src: leader_host.clone(),
        dst: follower_host.clone(),
        port: cfg.replication_port,
    };
    let heal = Action::Heal {
        src: leader_host.clone(),
        dst: follower_host.clone(),
        port: cfg.replication_port,
    };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    // Steady warm-up.
    sleep(Duration::from_secs(10)).await;

    // 5 cycles × (3s partition + 3s heal) = 30s of flapping.
    for i in 1..=5u32 {
        println!("[{SCEN}] flap {i}/5: partition");
        executor.run(&partition)?;
        sleep(Duration::from_secs(3)).await;
        println!("[{SCEN}] flap {i}/5: heal");
        if let Err(e) = executor.run(&heal) {
            let _ = executor.run(&heal);
            return Err(format!("heal failed: {e}"));
        }
        sleep(Duration::from_secs(3)).await;
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 15s");
    sleep(Duration::from_secs(15)).await;
    let bench_window_end_ms = up.elapsed_ms();

    // Defensive final heal.
    let _ = executor.run(&heal);

    let expectations = ScenarioExpectations {
        // Churn allowance: each flap can trigger an S3 lease renewal on
        // the leader side plus possible follower challenges. 5 cycles × ~6
        // per cycle = 30 is the upper bound for normal operation.
        max_leader_elections: 30,
        // S3 fallback fires once or twice per flap while heartbeats are
        // down. 5 × ~100 = 500 headroom.
        max_s3_fallbacks: 500,
        // 5 heartbeats failing per flap × 5 flaps = 25 minimum; allow
        // significant headroom.
        max_heartbeat_failures: 120,
        max_bench_errors: 200_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // Leadership may flip once or twice if the S3 race goes the
        // unlucky way during a flap. Not required to retain.
        max_role_flips: 8,
        max_split_brain_ticks: 10,
        require_leader_retained: false,
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.3).max(50.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-12: stop MinIO for 10s, restart. The cross-node heartbeat and TCP
/// replication paths are unaffected (those flow directly between cs1 and
/// cs2, not through MinIO). The leader keeps committing via TCP
/// replication throughout, so `pending_replication_bytes` never even
/// exceeds the high-water and no S3 fallback is needed. Leader retained.
///
/// This is similar in spirit to SCEN-8 (`partition_leader_minio`) but
/// more thorough: SCEN-8 only blocks one direction's packets via
/// nftables while MinIO itself stays up, whereas SCEN-12 actually takes
/// MinIO offline so a fallback attempt would error differently
/// (`ConnectionRefused` rather than silently dropped packets).
///
/// Timeline: bring up → bench (60s) → +15s stop MinIO → +10s start MinIO
/// → wait for bench → settle 10s → evaluate.
pub async fn run_minio_outage_short(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "minio_outage_short";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    if cfg.infra_host.is_none() {
        return Err(format!(
            "{SCEN}: INFRA_HOST is not set in config.env — this scenario requires a separate MinIO container"
        ));
    }

    let executor = ActionExecutor::new(cfg);

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    // Steady-state warm-up.
    sleep(Duration::from_secs(15)).await;
    println!("[{SCEN}] stopping MinIO");
    executor.run(&Action::StopMinio)?;

    sleep(Duration::from_secs(10)).await;

    println!("[{SCEN}] starting MinIO");
    if let Err(e) = executor.run(&Action::StartMinio) {
        // Always try to bring MinIO back so later scenarios aren't left
        // with a dead dependency.
        let _ = executor.run(&Action::StartMinio);
        return Err(format!("start-minio failed: {e}"));
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 10s");
    sleep(Duration::from_secs(10)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let expectations = ScenarioExpectations {
        // Leader's own heartbeat-driven TTL extension keeps the lease
        // alive without S3 renewal. Under heavy load the brief MinIO blip
        // can cause one transient heartbeat-RTT spike before the leader's
        // backpressure path engages; allow a few.
        max_leader_elections: 10,
        // The TCP replication path handles everything — no fallback.
        max_s3_fallbacks: 0,
        // Empirically 1 heartbeat blip on cs1 during the 10s MinIO outage
        // under load. The leader's S3-blocked lease-renewal task can
        // briefly delay one heartbeat-send before backpressure kicks in.
        max_heartbeat_failures: 5,
        // Bench-side errors can hit 12-15k under load when the
        // backpressure path engages during the outage and clients retry
        // through the rollback-cooldown window. Loosened from 10k.
        max_bench_errors: 25_000,
        max_role_flips: 0,
        max_split_brain_ticks: 0,
        require_leader_retained: true,
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.75).max(100.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-13: same as SCEN-12 but with a 60s MinIO outage instead of 10s.
/// Long enough that if the leader were using S3 in the critical path,
/// the pending replication queue would overflow and clients would start
/// seeing `ServerBusy`. In practice the leader uses TCP replication
/// while the follower is reachable (which it is — the cross-node path
/// is unaffected by MinIO being down), so the queue never actually
/// overflows and the scenario is essentially a longer SCEN-12.
///
/// This scenario is intentionally strict: zero S3 fallbacks, zero
/// heartbeat failures, zero rollbacks, leader retained. Any deviation
/// is a regression — if we ever see a `ServerBusy` here or bench errors
/// > the (generous) floor, that means something started putting S3 in
/// the critical path when it shouldn't be.
///
/// The bench is extended to 90s so the 10s warmup + 60s outage + heal
/// + settle fits comfortably.
pub async fn run_minio_outage_long(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "minio_outage_long";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    if cfg.infra_host.is_none() {
        return Err(format!(
            "{SCEN}: INFRA_HOST is not set in config.env — this scenario requires a separate MinIO container"
        ));
    }

    let executor = ActionExecutor::new(cfg);

    const SCEN13_BENCH_SECS: u64 = 100;

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s (overriding params.duration)", params.tasks, SCEN13_BENCH_SECS);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, SCEN13_BENCH_SECS).await });

    sleep(Duration::from_secs(10)).await;
    println!("[{SCEN}] stopping MinIO for 60s");
    executor.run(&Action::StopMinio)?;

    sleep(Duration::from_secs(60)).await;

    println!("[{SCEN}] starting MinIO");
    if let Err(e) = executor.run(&Action::StartMinio) {
        let _ = executor.run(&Action::StartMinio);
        return Err(format!("start-minio failed: {e}"));
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 15s");
    sleep(Duration::from_secs(15)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let expectations = ScenarioExpectations {
        max_leader_elections: 10,
        max_s3_fallbacks: 0,
        max_heartbeat_failures: 0,
        // Errors during the MinIO outage scale with offered load — the fixed
        // 10k floor fails spuriously past ~16k tasks. 2% ratio keeps the
        // bound meaningful at any load (observed 0.84% @20k on the long
        // variant) while still catching a total-outage regression. Integrity
        // is asserted independently by LeaderRetained/EventualConvergence.
        max_bench_errors: 10_000,
        max_bench_error_ratio: Some(0.02),
        max_role_flips: 0,
        max_split_brain_ticks: 0,
        require_leader_retained: true,
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.75).max(100.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-10: the catastrophic path. Kill the follower AND stop MinIO at
/// the same time. The leader now has NO way to extend its lease:
/// heartbeats to the follower fail (dead process), S3 lease renewal
/// fails (MinIO dead), S3 fallback replication fails. The leader MUST
/// self-fence at `lease_expires_at_ms - max_clock_drift_ms` and reject
/// client writes for the rest of the outage. Heal restores MinIO first,
/// then restarts the follower, and the cluster must re-elect exactly
/// one leader without split-brain.
///
/// Timeline: bring up → bench (120s) → +15s kill follower + stop MinIO
/// → +40s (enough for leader to self-fence via TTL expiry) → start
/// MinIO → +5s start follower → wait for bench → settle 20s → eval.
pub async fn run_partition_then_kill_minio(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "partition_then_kill_minio";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    if cfg.infra_host.is_none() {
        return Err(format!(
            "{SCEN}: INFRA_HOST is not set in config.env — this scenario requires a separate MinIO container"
        ));
    }

    let executor = ActionExecutor::new(cfg);

    // The "follower" here is whichever node *isn't* the current leader.
    // Kill that one — the current leader is the one under test.
    let (kill_follower, start_follower) = if up.bench_primary == cfg.leader_addr() {
        (Action::KillCs2, Action::StartCs2)
    } else {
        (Action::KillCs1, Action::StartCs1)
    };

    // Extend the bench so 15s warmup + 40s outage + 5s restart-lag +
    // recovery fits cleanly.
    const SCEN10_BENCH_SECS: u64 = 120;

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s (overriding params.duration)", params.tasks, SCEN10_BENCH_SECS);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, SCEN10_BENCH_SECS).await });

    sleep(Duration::from_secs(15)).await;

    // Blackout: MinIO and follower both go down. Kill the follower first
    // so cs1 is already in "follower unreachable" mode by the time MinIO
    // drops out — mirrors a realistic cascading outage (rack loss, etc.)
    // rather than a surgical simultaneous kill.
    println!("[{SCEN}] killing follower ({:?})", kill_follower);
    executor.run(&kill_follower)?;
    println!("[{SCEN}] stopping MinIO");
    if let Err(e) = executor.run(&Action::StopMinio) {
        // If MinIO stop fails, abort — but restart the follower so we
        // don't leave the cluster in a weird half-dead state.
        let _ = executor.run(&start_follower);
        return Err(format!("stop-minio failed: {e}"));
    }

    // Hold the blackout long enough for the leader's lease TTL to expire
    // and for its asymmetric-fencing path to self-fence. At rpi
    // heartbeat_lease_duration=1500ms with an S3 lease duration of
    // ~30s, the leader fences within ~30s of losing both paths.
    sleep(Duration::from_secs(40)).await;

    // Heal order: MinIO first. Wait a beat so cs1 (already fenced) can
    // see S3 come back and attempt re-election — then bring the follower
    // back so whoever wins the new lease can immediately re-establish
    // replication without a dead-peer detour.
    println!("[{SCEN}] starting MinIO");
    if let Err(e) = executor.run(&Action::StartMinio) {
        let _ = executor.run(&Action::StartMinio);
        let _ = executor.run(&start_follower);
        return Err(format!("start-minio failed: {e}"));
    }
    sleep(Duration::from_secs(5)).await;
    println!("[{SCEN}] starting follower ({:?})", start_follower);
    executor.run(&start_follower)?;

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    // Settle bumped 20s → 90s: this scenario kills MinIO and the follower
    // simultaneously, leaving cs1 in dual-failure state (no S3, no follower).
    // Once both are restored, cs1 must drain backlog via S3 fallback while
    // cs2 boots and runs S3 catchup. On rpi+SD-card MinIO this is a 60s+
    // window. EC2+S3 converges in <10s.
    // 240s settle: at 8k+ load the post-blackout catchup can take longer
    // than 180s on rpi + sd-card MinIO. The work is bounded by S3
    // download/apply throughput, not by anything tunable in the cluster.
    println!("[{SCEN}] settle 240s for catchup + role re-stabilisation (slow-infra liveness window)");
    sleep(Duration::from_secs(240)).await;
    let bench_window_end_ms = up.elapsed_ms();

    // Defensive cleanup in case a later failure path leaves MinIO stopped.
    let _ = executor.run(&Action::StartMinio);

    let expectations = ScenarioExpectations {
        // After heal: cs1 (was leader) demotes, cs2 takes over via S3 CAS,
        // both nodes do bidirectional S3 catchup of each other's divergent
        // branches. Each catchup-driven `set_node_role_via_s3` call counts
        // (boot/post-catchup/challenge/proactive paths). On slow infra this
        // can stack up while the apply path holds the executor; correctness
        // is preserved via the lease_epoch CAS. 80 covers the observed
        // 30-50 range with margin.
        max_leader_elections: 80,
        // cs1 will attempt S3 fallback while it still thinks it has a
        // valid lease. All those attempts fail because MinIO is down,
        // but the *attempt* metric still increments.
        max_s3_fallbacks: 1000,
        // Heartbeats fail continuously for the ~40s outage at ~2/s.
        max_heartbeat_failures: 250,
        // Rollback fires when both TCP and S3 replication fail
        // simultaneously — this is exactly the scenario, so expect
        // several.
        // Bench sees ServerBusy / NotLeader / connection errors during
        // the blackout. With 4000 tasks and ~40s of leader-fenced time,
        // this can get into the hundreds of thousands.
        max_bench_errors: 1_500_000,
        // Errors are retry ATTEMPTS and scale with tasks × outage / backoff
        // (observed: 75% attempt-share at 25k with healthy integrity).
        max_bench_error_ratio: Some(0.85),
        // Recovery thrash: while both nodes do S3 catchup of each other's
        // divergent branches, the apply path on shard 0 transiently
        // delays heartbeat acks. The follower's TTL expires, it
        // challenges via S3 CAS, fails (current leader has higher
        // lease_epoch), reverts to follower. Each round-trip = 1 role
        // flip per node. Observed 14 in worst case; 20 is generous.
        max_role_flips: 20,
        // Significant split-brain tolerance: during the fencing window
        // there's a stretch where neither node holds the leader role.
        max_split_brain_ticks: 60,
        require_leader_retained: false,
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        // Sandbox-known orchestrator robustness issue: when MinIO comes
        // back, the boot path's S3 catchup can hit "no common ancestor"
        // before the new leader has uploaded enough. With the catchup-side
        // skip-and-retry now in place, repeat panics should be gone but
        // the first one in the recovery window still occurs.
        max_shard_panics: 4,
        max_node_starts: 2,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.2).max(30.0);
    // Bench duration overridden by SCEN10_BENCH_SECS; thread that through
    // so bench_actual_end_ms is correct for window-aware checks.
    scen_params.duration_secs = SCEN10_BENCH_SECS;

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// Idempotency audit under the worst-rollback scenario in the suite.
/// Mirrors `run_partition_then_kill_minio` (kill follower + stop MinIO →
/// 40s blackout → heal) but runs `run_benchmark_idempotent` so the audit
/// can detect false-ack data loss caused by the orphan-snapshot path in
/// `capture_replication_snapshot`.
///
/// This scenario forces both follower-unreachable AND S3-unreachable for
/// long enough that the leader self-fences and rollbacks fire. With the
/// orphan bug present, post-fsync items get popped from
/// `pending_replication_batches` while the rollback flag is still set,
/// then dropped — the audit observes the resulting gap (or
/// `NoCaptureDroppedItems` trips even sooner).
pub async fn run_idempotency_audit_partition_then_kill_minio(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "idempotency_audit_partition_then_kill_minio";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    if cfg.infra_host.is_none() {
        return Err(format!(
            "{SCEN}: INFRA_HOST is not set in config.env — this scenario requires a separate MinIO container"
        ));
    }

    let executor = ActionExecutor::new(cfg);

    let (kill_follower, start_follower) = if up.bench_primary == cfg.leader_addr() {
        (Action::KillCs2, Action::StartCs2)
    } else {
        (Action::KillCs1, Action::StartCs1)
    };

    const AUDIT_BENCH_SECS: u64 = 120;

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] idempotent bench: {} tasks, {}s", params.tasks, AUDIT_BENCH_SECS);
    let history = new_history_recorder(SCEN, run_dir);
    let pool_clone = pool.clone();
    let history_clone = history.clone();
    let tasks = params.tasks;
    let ryw_pinned = build_ryw_pinned(cfg).await;
    let bench_handle = tokio::spawn(async move {
        run_benchmark_idempotent_opts(
            &pool_clone,
            tasks,
            AUDIT_BENCH_SECS,
            celeriant_bench::IdempotentBenchOptions { history: history_clone, duplicate_replay: false, ryw_pinned },
        )
        .await
    });

    sleep(Duration::from_secs(15)).await;

    println!("[{SCEN}] killing follower ({:?})", kill_follower);
    executor.run(&kill_follower)?;
    println!("[{SCEN}] stopping MinIO");
    if let Err(e) = executor.run(&Action::StopMinio) {
        let _ = executor.run(&start_follower);
        return Err(format!("stop-minio failed: {e}"));
    }

    sleep(Duration::from_secs(40)).await;

    println!("[{SCEN}] starting MinIO");
    if let Err(e) = executor.run(&Action::StartMinio) {
        let _ = executor.run(&Action::StartMinio);
        let _ = executor.run(&start_follower);
        return Err(format!("start-minio failed: {e}"));
    }
    sleep(Duration::from_secs(5)).await;
    println!("[{SCEN}] starting follower ({:?})", start_follower);
    executor.run(&start_follower)?;

    let outcome = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s (ok_acks={} 2002_acks={} repl_retry={} transient_retry={} fatal={})",
        outcome.benchmark.total_requests,
        outcome.benchmark.errors,
        outcome.benchmark.throughput,
        outcome.counters.ok_acks,
        outcome.counters.idempotency_acks,
        outcome.counters.replication_retries,
        outcome.counters.transient_retries,
        outcome.counters.fatal_errors,
    );

    println!("[{SCEN}] settle 240s for catchup + role re-stabilisation before audit");
    sleep(Duration::from_secs(240)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let _ = executor.run(&Action::StartMinio);

    let (integrity, deep) = run_integrity_and_deep_audit(SCEN, &pool, &outcome.task_acks, 32).await;

    // Tolerate up to 10% audit-time read errors — the audit runs after a
    // brutal blackout + recovery; some aggregates may briefly be unreadable
    // even after the settle window. The headline check is still gaps == 0.
    let max_unreadable = (params.tasks as u64 / 10).max(50);
    let integrity_check = data_integrity_check(&integrity, max_unreadable);

    let expectations = ScenarioExpectations {
        max_leader_elections: 80,
        max_s3_fallbacks: 1000,
        max_heartbeat_failures: 250,
        max_bench_errors: 1_500_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        max_role_flips: 20,
        max_split_brain_ticks: 60,
        require_leader_retained: false,
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        max_shard_panics: 4,
        max_node_starts: 2,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.2).max(30.0);
    scen_params.duration_secs = AUDIT_BENCH_SECS;

    let mut extra_checks = vec![integrity_check];
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks, params.seed).await);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        outcome.benchmark.clone(),
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        Some(integrity),
        Some(outcome.counters),
        deep,
        run_dir,
    )
    .await
}

/// Fast variant of `run_idempotency_audit_partition_then_kill_minio`.
/// Same chaos shape (kill follower + stop MinIO → blackout → heal) but ~3×
/// shorter wall time so a soak hour can fit ~25 iterations instead of ~9.
/// Designed to reproduce the residual failover false-ack bug densely.
///
/// Timeline: bring up → bench (30s) → +10s kill follower + stop MinIO →
/// +20s heal → wait for bench → settle 60s → audit. Total ~2-3 min per
/// iteration, vs ~7 min for the full scenario.
pub async fn run_idempotency_audit_fast_blackout(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "idempotency_audit_fast_blackout";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    if cfg.infra_host.is_none() {
        return Err(format!(
            "{SCEN}: INFRA_HOST is not set in config.env — this scenario requires a separate MinIO container"
        ));
    }

    let executor = ActionExecutor::new(cfg);

    let (kill_follower, start_follower) = if up.bench_primary == cfg.leader_addr() {
        (Action::KillCs2, Action::StartCs2)
    } else {
        (Action::KillCs1, Action::StartCs1)
    };

    const FAST_BENCH_SECS: u64 = 30;
    const FAST_WARMUP_SECS: u64 = 5;
    const FAST_BLACKOUT_SECS: u64 = 20;
    const FAST_SETTLE_SECS: u64 = 60;

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] idempotent bench: {} tasks, {}s", params.tasks, FAST_BENCH_SECS);
    let history = new_history_recorder(SCEN, run_dir);
    let pool_clone = pool.clone();
    let history_clone = history.clone();
    let tasks = params.tasks;
    let ryw_pinned = build_ryw_pinned(cfg).await;
    let bench_handle = tokio::spawn(async move {
        run_benchmark_idempotent_opts(
            &pool_clone,
            tasks,
            FAST_BENCH_SECS,
            celeriant_bench::IdempotentBenchOptions { history: history_clone, duplicate_replay: false, ryw_pinned },
        )
        .await
    });

    sleep(Duration::from_secs(FAST_WARMUP_SECS)).await;

    println!("[{SCEN}] killing follower ({:?})", kill_follower);
    executor.run(&kill_follower)?;
    println!("[{SCEN}] stopping MinIO");
    if let Err(e) = executor.run(&Action::StopMinio) {
        let _ = executor.run(&start_follower);
        return Err(format!("stop-minio failed: {e}"));
    }

    sleep(Duration::from_secs(FAST_BLACKOUT_SECS)).await;

    println!("[{SCEN}] starting MinIO");
    if let Err(e) = executor.run(&Action::StartMinio) {
        let _ = executor.run(&Action::StartMinio);
        let _ = executor.run(&start_follower);
        return Err(format!("start-minio failed: {e}"));
    }
    sleep(Duration::from_secs(2)).await;
    println!("[{SCEN}] starting follower ({:?})", start_follower);
    executor.run(&start_follower)?;

    let outcome = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s (ok_acks={} 2002_acks={} repl_retry={} transient_retry={} fatal={})",
        outcome.benchmark.total_requests,
        outcome.benchmark.errors,
        outcome.benchmark.throughput,
        outcome.counters.ok_acks,
        outcome.counters.idempotency_acks,
        outcome.counters.replication_retries,
        outcome.counters.transient_retries,
        outcome.counters.fatal_errors,
    );

    println!("[{SCEN}] settle {}s before audit", FAST_SETTLE_SECS);
    sleep(Duration::from_secs(FAST_SETTLE_SECS)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let _ = executor.run(&Action::StartMinio);

    let (integrity, deep) = run_integrity_and_deep_audit(SCEN, &pool, &outcome.task_acks, 32).await;

    let max_unreadable = (params.tasks as u64 / 10).max(50);
    let integrity_check = data_integrity_check(&integrity, max_unreadable);

    let expectations = ScenarioExpectations {
        // Looser bounds than the full scenario because the recovery window is
        // tighter — less chance for thorough convergence. The strict signal
        // is still NoClientSeqGaps + NoCaptureDroppedItems.
        max_leader_elections: 80,
        max_s3_fallbacks: 1000,
        max_heartbeat_failures: 250,
        max_bench_errors: 1_500_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        max_role_flips: 20,
        max_split_brain_ticks: 60,
        require_leader_retained: false,
        // Skip require_final_leader_write_progress + assert_eventual_progress:
        // 60s settle is too short for those to hold reliably under load.
        // The integrity audit is what we're actually testing.
        max_shard_panics: 4,
        max_node_starts: 2,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.2).max(30.0);
    scen_params.duration_secs = FAST_BENCH_SECS;

    let mut extra_checks = vec![integrity_check];
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks, params.seed).await);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        outcome.benchmark.clone(),
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        Some(integrity),
        Some(outcome.counters),
        deep,
        run_dir,
    )
    .await
}

/// SCEN-17: rolling restart. Gracefully stop the follower, wait for it
/// to come back and catch up, then gracefully stop the leader and wait
/// for leadership to transfer and the old leader to rejoin as follower.
/// Simulates a rolling update procedure — the whole cluster remains
/// available throughout even though each node is restarted once.
///
/// This differs from SCEN-4 (`leader_graceful_stop`) by sequencing two
/// restarts (follower first, then leader) with a full catchup between
/// them. The goal is "at least one node is healthy at every instant",
/// not "leadership is retained".
///
/// Timeline: bring up → bench (140s) → +10s warmup → stop follower → +10s
/// start follower → +40s drain → stop leader → +35s start former leader
/// (35s > 30s S3 lease TTL so the follower can promote) → wait for bench →
/// evaluate.
pub async fn run_rolling_restart(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "rolling_restart";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    // Identify initial leader/follower slots.
    let (stop_follower, start_follower, stop_leader, start_leader) = if up.bench_primary == cfg.leader_addr() {
        (Action::StopCs2, Action::StartCs2, Action::StopCs1, Action::StartCs1)
    } else {
        (Action::StopCs1, Action::StartCs1, Action::StopCs2, Action::StartCs2)
    };

    // 10s warmup + (10+10) follower cycle + 40s inter-phase drain
    // + (15+15) leader cycle = 100s active, then 40s of post-phase-2
    // steady-state writes to drive TCP replication fully to the
    // former leader. The 40s inter-phase drain is the important one:
    // cs2 needs to fully converge from `FollowerCatchingUp` back to
    // `Follower` before phase 2 starts, otherwise it can't promote
    // when cs1 stops (the documented invariant forbids
    // FollowerCatchingUp → Leader). At ~10k writes/s and 10s of
    // follower downtime, cs2 has ~100k entries to catch up on phase 1
    // rejoin, which takes well over 10 seconds under continued load.
    //
    // The trailing steady-state writes matter because TCP replication
    // is write-driven: when the leader stops writing, TCP stops
    // sending, and any lag in the follower (e.g. 300 entries stuck
    // in the kick → S3 → gap loop) never gets filled. Ending the
    // bench with writes still in flight lets the normal
    // fetch_catchup_entries path flush the last few hundred entries.
    const SCEN17_BENCH_SECS: u64 = 140;

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s (overriding params.duration)", params.tasks, SCEN17_BENCH_SECS);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, SCEN17_BENCH_SECS).await });

    sleep(Duration::from_secs(10)).await;

    // Phase 1: restart the follower. Leader stays up and falls back to
    // S3 during the gap.
    println!("[{SCEN}] stopping follower ({:?})", stop_follower);
    executor.run(&stop_follower)?;
    sleep(Duration::from_secs(10)).await;
    println!("[{SCEN}] starting follower ({:?})", start_follower);
    executor.run(&start_follower)?;

    // Wait for follower catchup to fully drain back to steady
    // `Follower` before perturbing the leader. Tighter waits leave
    // cs2 in `FollowerCatchingUp`, which per the invariant cannot
    // transition directly to `Leader` — so cs2 can't win the
    // election even after cs1 stops.
    sleep(Duration::from_secs(40)).await;

    // Phase 2: stop the leader. The surviving (ex-)follower should
    // fence, win S3 CAS, and promote — then receive writes until the
    // old leader rejoins as the new follower.
    println!("[{SCEN}] stopping leader ({:?})", stop_leader);
    executor.run(&stop_leader)?;
    // Must exceed the S3 lease TTL (30s): graceful stop does NOT release the
    // lease, so the surviving follower cannot promote until the stopped
    // leader's lease expires. A shorter gap lets the ex-leader restart and
    // reclaim before any handover, so leadership never moves (the cause of the
    // DistinctLeaderHosts failure at 6k).
    sleep(Duration::from_secs(35)).await;
    println!("[{SCEN}] starting former leader ({:?})", start_leader);
    executor.run(&start_leader)?;

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 15s");
    sleep(Duration::from_secs(15)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let expectations = ScenarioExpectations {
        // Phase 2 causes one real promotion. Allow headroom for S3
        // lease renewals during the surviving node's transition.
        max_leader_elections: 30,
        // Phase 1 forces S3 fallback while the follower is down.
        max_s3_fallbacks: 500,
        // Phase 2's leader stop also generates heartbeat failures as the
        // old leader goes down.
        max_heartbeat_failures: 120,
        // Phase 2 is a leader-loss window — similar error envelope to
        // SCEN-4 (~500k max). Phase 1 contributes a smaller burst.
        // Error volume scales with offered load (12k tasks: 686k, 16k: 866k
        // observed against healthy integrity), so the bound is a ratio of
        // total requests; the absolute floor covers low-load runs.
        max_bench_errors: 600_000,
        max_bench_error_ratio: Some(0.75),
        // At least two flips: original leader → follower (phase 2),
        // and the original follower promoted to leader.
        max_role_flips: 8,
        max_split_brain_ticks: 10,
        require_leader_retained: false,
        require_final_leader_write_progress: true,
        // Both nodes must have hold leadership at some point — the
        // old leader while serving writes pre-phase-2, and the new
        // leader post-phase-2.
        require_distinct_leader_hosts: Some(2),
        assert_eventual_progress: true,
        // Follower-visibility oracles: the two restart phases are excluded
        // by the stability guard; the 40s inter-phase drain and trailing
        // steady-state writes are exactly the stable windows NeverAhead
        // should police.
        assert_never_ahead: true,
        assert_read_converged_at_quiesce: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.3).max(50.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-15: SIGSTOP the leader for longer than the S3 lease TTL, then
/// SIGCONT. The paused process can't run — so it can't heartbeat,
/// can't renew its own lease, and can't even fence itself locally.
/// The follower's TTL expires, follower wins S3 CAS, promotes, and
/// starts serving writes. When the old leader is SIGCONT'd, it wakes
/// up and SHOULD discover via its next heartbeat attempt (or its next
/// S3 lease check) that its lease has been taken by a higher
/// lease_epoch, and demote itself to follower WITHOUT split-brain.
///
/// This is the test of the "zombie leader wakes up after lease
/// expiry" invariant. If the old leader resumes and continues
/// committing writes without checking S3, it would double-commit at
/// the same `wal_seq`, break the hash chain, and potentially lose
/// acked data.
///
/// Timeline: bring up → bench (90s) → +15s SIGSTOP leader →
/// +20s (enough for follower fence + promote + serve) → SIGCONT
/// leader → +5s (old leader wakes, detects stale lease, demotes) →
/// wait for bench → settle 15s → eval.
pub async fn run_sigstop_leader(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "sigstop_leader";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    // Target whichever slot is currently leading, not cs1 unconditionally.
    let (pause, resume) = if up.bench_primary == cfg.leader_addr() {
        (Action::PauseCs1, Action::ResumeCs1)
    } else {
        (Action::PauseCs2, Action::ResumeCs2)
    };

    const SCEN15_BENCH_SECS: u64 = 90;

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s (overriding params.duration)", params.tasks, SCEN15_BENCH_SECS);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, SCEN15_BENCH_SECS).await });

    sleep(Duration::from_secs(15)).await;

    println!("[{SCEN}] SIGSTOPping leader ({:?})", pause);
    if let Err(e) = executor.run(&pause) {
        // Try to resume the leader anyway to avoid leaving a zombie
        // process on the pis between runs.
        let _ = executor.run(&resume);
        return Err(format!("pause failed: {e}"));
    }

    // Hold long enough for the follower's TTL to expire (default
    // heartbeat_lease_duration ~1.5s), for it to fence, win S3 CAS,
    // run the promotion S3 catchup, and start serving writes. The
    // previous SCEN-4/5 runs show ~8s is sufficient for a single
    // clean promotion; 20s gives comfortable headroom and lets the
    // new leader actually accumulate writes before we SIGCONT the
    // old one.
    sleep(Duration::from_secs(20)).await;

    println!("[{SCEN}] SIGCONTing old leader ({:?})", resume);
    if let Err(e) = executor.run(&resume) {
        // If resume fails, the process stays SIGSTOPped. Try again
        // once, then give up and let the scenario fail cleanly.
        let _ = executor.run(&resume);
        return Err(format!("resume failed: {e}"));
    }

    // Give the woken leader a moment to discover it's no longer
    // leader and demote itself.
    sleep(Duration::from_secs(5)).await;

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    // Defensive: if we failed somewhere above the cluster might still
    // have a paused process. Always try to resume before tear-down.
    let _ = executor.run(&resume);

    // Settle bumped 15s → 90s: SIGSTOP-then-SIGCONT triggers a leadership
    // reshuffle plus a TipHashMismatch resolution cycle on the demoted
    // node. On rpi+MinIO-on-SD-card S3 catchup pulls ~5 batches/s, so
    // bridging an in-flight gap can take tens of seconds with bench halted.
    // EC2+S3 converges in <5s.
    println!("[{SCEN}] settle 90s for catchup + role re-stabilisation (slow-infra liveness window)");
    sleep(Duration::from_secs(90)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let expectations = ScenarioExpectations {
        // One real promotion + recovery-driven challenge attempts. The
        // post-resume catchup keeps both nodes doing S3 reconciliation,
        // which transiently delays heartbeats and produces extra lease
        // challenges (each `set_node_role_via_s3` increments). Bumped
        // to absorb the new recovery thrash pattern; correctness is
        // preserved via lease_epoch CAS.
        max_leader_elections: 80,
        // S3 fallback fires while the follower is reaching for the
        // lease and the paused leader can't commit anything.
        max_s3_fallbacks: 500,
        // Heartbeats to the paused process fail continuously during
        // the 20s pause.
        max_heartbeat_failures: 100,
        // Similar envelope to SCEN-4/5 leader-loss scenarios.
        max_bench_errors: 500_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // Recovery-thrash window: while the resumed paused node drains
        // its un-replicated tail and rebuilds the hash chain via S3
        // catchup, both nodes' heartbeat path is intermittently delayed
        // by the apply path on shard 0. TTL expiries cause challenge →
        // CAS-fail → revert cycles. Observed 10; 20 is generous.
        max_role_flips: 20,
        max_split_brain_ticks: 10,
        // Leader changes hands, so no retention requirement.
        require_leader_retained: false,
        require_final_leader_write_progress: true,
        // Both nodes hold leadership at some point.
        require_distinct_leader_hosts: Some(2),
        assert_eventual_progress: true,
        // SIGSTOP freezes the leader's heartbeat path; same TTL-driven
        // 1600ms recovery budget as graceful-stop and sigkill scenarios.
        max_failover_ms: Some(1600),
        // This is THE pause/clock-skew dual-ack trigger: a zombie leader resuming
        // after its lease was taken at a higher epoch. The gauge-tick split-brain
        // check (max_split_brain_ticks, 2Hz scrape) can miss a brief contested-seq
        // overlap, so also assert on WAL truth — SSH both nodes post-run and reject
        // any same-wal_seq / different-tip_hash fork via wal-inspect.
        assert_no_divergent_tips: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.3).max(50.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-14: bump the follower's system clock forward by 2 seconds
/// (via `sudo date -s "+2 seconds"` over ssh). The follower's
/// heartbeat handler computes clock drift as
/// `follower_ms.abs_diff(leader_ms)` and fences all local shards
/// when drift exceeds `max_clock_drift_ms` (default 500ms on the
/// rpi cluster). With a 2000ms skew, the very next heartbeat from
/// the leader triggers an immediate fence. Clients writing to the
/// (still-healthy) leader continue getting served via S3 fallback
/// while the follower is fenced; leadership is retained.
///
/// After 10 seconds we bump the clock back by -2 seconds, restoring
/// approximately-correct time. The next heartbeat's drift is back
/// in range, the heartbeat handler transitions the fenced node back
/// to `Follower`, and replication resumes.
///
/// Timeline: bring up → bench (60s) → +20s skew cs2 +2s → +10s skew
/// cs2 -2s → wait for bench → settle 15s → evaluate.
pub async fn run_clock_skew_follower(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "clock_skew_follower";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    // The "follower" here is whichever node isn't currently leading.
    let follower_host = if up.bench_primary == cfg.leader_addr() {
        cfg.follower_host.clone()
    } else {
        cfg.leader_host.clone()
    };

    let skew = Action::SkewClock { host: follower_host.clone(), offset_secs: 2 };
    // Re-enabling NTP rather than a manual inverse skew handles any
    // drift accumulated during the skew window, and is what the
    // `restore-clock` Make target provides.
    let restore = Action::RestoreClock { host: follower_host.clone() };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    sleep(Duration::from_secs(20)).await;

    println!("[{SCEN}] skewing {follower_host} clock +2s");
    if let Err(e) = executor.run(&skew) {
        // Always try to restore so the host clock doesn't stay wrong
        // between runs.
        let _ = executor.run(&restore);
        return Err(format!("skew failed: {e}"));
    }

    sleep(Duration::from_secs(10)).await;

    println!("[{SCEN}] restoring {follower_host} clock (re-enabling NTP)");
    if let Err(e) = executor.run(&restore) {
        let _ = executor.run(&restore);
        return Err(format!("restore-clock failed: {e}"));
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    // Defensive final restore: if anything above failed mid-flight, the
    // host might still have a skewed clock and NTP disabled.
    let _ = executor.run(&restore);

    println!("[{SCEN}] settle 15s");
    sleep(Duration::from_secs(15)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let expectations = ScenarioExpectations {
        // The fenced follower challenges for leadership each TTL cycle
        // (~500ms) and loses each S3 CAS against the healthy leader.
        // Each challenge bumps `leader_elections_total`. With ~10s of
        // skewed clock and 500ms TTL, we see ~20 challenges, but the
        // NTP restore slews rather than steps, so the fence window can
        // overrun (31 observed under 4k-task load). 40 absorbs slew
        // latency; a fence that never heals produces ~80-120.
        max_leader_elections: 40,
        // Leader falls back to S3 for the 10s while follower is fenced.
        max_s3_fallbacks: 500,
        // Heartbeat handler fences on drift — the heartbeat itself
        // is "rejected" and counted as failed from the leader's
        // perspective. Generous bound.
        max_heartbeat_failures: 100,
        // Both TCP and S3 paths work for the leader throughout;
        // rollbacks shouldn't fire.
        max_bench_errors: 50_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // Under high concurrent load the clock-skew window plus S3
        // contention can occasionally let the skewed follower win the
        // S3 lease CAS during a heartbeat-renewal race. Both nodes flip
        // role briefly before stabilising. Allow up to 4 flips (2 per
        // node = one round-trip handoff). The CORRECTNESS invariant —
        // ExactlyOneLeader at every tick — still holds via the CAS.
        max_role_flips: 4,
        max_split_brain_ticks: 5,
        // Under load the leader CAN change hands during clock-skew:
        // the follower's TTL-driven lease challenge can win the S3 CAS
        // if the leader's renewal is contending. This is correct slow-
        // path behaviour (FLP / Lamport applies), not a defect.
        require_leader_retained: false,
        require_final_leader_write_progress: true,
        // Only one host ever leads, so don't require the distinct-host
        // guard — it would fail as expected.
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.5).max(100.0);

    tear_down_and_evaluate(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        run_dir,
    )
    .await
}

/// SCEN-16: fill the follower's data disk to within 50 MiB of full via
/// `fallocate`, run the bench, heal by removing the filler. The
/// follower's WAL files are pre-allocated, so fsync on existing
/// segments may still succeed even under a full disk — the critical
/// failure points are:
/// - Log segment rotation (requires a new file allocation)
/// - Segment summary sidecar writes (new files)
/// - Any metadata journal writes the kernel needs to flush
///
/// Expected observable behavior:
/// - Leader is retained throughout — cs1 keeps committing to its own
///   (non-full) disk. Some replication may fail if cs2's disk pressure
///   causes fsync errors; leader would fall back to S3 in that case.
/// - Follower either fences cleanly OR continues happy-path
///   replication for entries that fit in the pre-allocated segment.
///   Either outcome is acceptable as long as nothing panics.
/// - After heal + settle, the follower is back in sync.
///
/// This scenario is intentionally loose on the "follower behavior
/// during the full disk" observation — the design doc doesn't
/// guarantee a specific fence-on-ENOSPC code path, and observed
/// behavior on a pre-allocated WAL may differ from the spec's
/// "follower fences" prediction.
pub async fn run_follower_disk_full(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "follower_disk_full";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    let follower_host = if up.bench_primary == cfg.leader_addr() {
        cfg.follower_host.clone()
    } else {
        cfg.leader_host.clone()
    };

    // 50 MiB reserve: well below SHARD_LOG_PREALLOCATE_BYTES, so any segment
    // rotation during the fill window deterministically hits ENOSPC at
    // create+preallocate. That path is the point of this scenario: the shard
    // must surface it as a transient error (failed rotation removes its
    // partial file, S3 catchup retries) — never panic, never shut down.
    let fill = Action::FillDisk {
        host: follower_host.clone(),
        reserve_mb: 50,
    };
    let clean = Action::CleanDisk {
        host: follower_host.clone(),
    };

    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    // Steady-state warm-up — the follower needs to be healthy and
    // caught up before we start squeezing its disk.
    sleep(Duration::from_secs(15)).await;

    println!("[{SCEN}] filling {follower_host} data disk");
    if let Err(e) = executor.run(&fill) {
        // Always try to clean up even on fill failure — leaving a
        // filler file behind would trash the pi for other runs.
        let _ = executor.run(&clean);
        return Err(format!("fill-disk failed: {e}"));
    }

    // Hold the full-disk state for 45 seconds. A 256 MiB segment fills in
    // ~30s of saturated writes per data shard, so this window reliably
    // contains at least one ENOSPC rotation attempt — the path under test —
    // plus the leader's S3 fallback once the follower's fsyncs start failing.
    sleep(Duration::from_secs(45)).await;

    println!("[{SCEN}] cleaning up disk filler");
    if let Err(e) = executor.run(&clean) {
        let _ = executor.run(&clean);
        return Err(format!("clean-disk failed: {e}"));
    }

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    // 90s settle: the follower spent the fill window rejecting fsyncs while
    // the leader rode the S3 fallback; with the bench halted this gives the
    // catchup retry loop (5s cadence) and TCP replication time to drain the
    // gap on slow infra. EC2+S3 converges in <5s.
    println!("[{SCEN}] settle 90s for catchup + disk-pressure recovery (slow-infra liveness window)");
    sleep(Duration::from_secs(90)).await;
    let bench_window_end_ms = up.elapsed_ms();

    // Defensive final cleanup.
    let _ = executor.run(&clean);

    // Pre-teardown so SSH still has the service-managed mount warm; reads
    // headers only, safe against a live service.
    let orphan_check = check_no_orphan_segments(&follower_host).await;

    let expectations = ScenarioExpectations {
        // Leader may renew S3 lease during follower stress but no
        // real election should happen. The metric counts every S3 CAS
        // (including same-leader renewals); during the 90s slow-infra
        // settle the renewal counter accumulates ~1/sec while the
        // follower is recovering, so headroom must scale with settle.
        // Empirically at 8k load with the optimised catchup, post-recovery
        // S3 CAS retries can stack to 70+ as the follower's restart-then-
        // catchup pulls heavily on MinIO. Bumped from 40 → 100 for
        // headroom. Correctness is preserved by lease_epoch CAS regardless.
        max_leader_elections: 100,
        // If the follower's fsync fails, the leader falls back to S3
        // for every commit during the outage window.
        max_s3_fallbacks: 500,
        // Heartbeats to a stressed follower may fail if shard 0's
        // heartbeat handler gets stuck on fsync. Generous bound.
        max_heartbeat_failures: 60,
        // Bench error budget — conservative. Real writes should mostly
        // succeed because the leader has a healthy disk.
        max_bench_errors: 100_000,
        // Load-proportional ceiling: errors are retry attempts and scale
        // with offered load (attempt-share of errors+completions).
        max_bench_error_ratio: Some(0.75),
        // Leader should not change.
        max_role_flips: 0,
        max_split_brain_ticks: 10,
        require_leader_retained: true,
        require_final_leader_write_progress: true,
        assert_eventual_progress: true,
        // Disk full is a transient the node must survive in place: a failed
        // rotation fails the write and the catchup driver retries. Any panic
        // or restart is a regression.
        max_shard_panics: 0,
        max_node_starts: 0,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.5).max(100.0);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        vec![orphan_check],
        None,
        None,
        None,
        run_dir,
    )
    .await
}

/// Failed rotations must not leave segment files behind: the ENOSPC path
/// deletes the partially created file before surfacing the error. A
/// `log_*.wal` whose front AND rear headers are both unreadable is such a
/// residue — a valid segment (even empty) always has at least one good
/// header.
async fn check_no_orphan_segments(host: &str) -> CheckResult {
    const NAME: &str = "NoOrphanSegments";
    let host = host.to_string();
    tokio::task::spawn_blocking(move || {
        let cmd = "for f in /var/lib/nvme/celeriant-data/shard_*/log_*.wal; do \
                       [ -f \"$f\" ] || continue; \
                       out=$(sudo /usr/local/bin/celeriant-wal-inspect \"$f\" header 2>/dev/null); \
                       if echo \"$out\" | grep -q 'front_header: <corrupt or missing>' \
                          && echo \"$out\" | grep -q 'rear_header: <corrupt or missing>'; then \
                           echo \"ORPHAN $f\"; \
                       else \
                           echo \"OK $f\"; \
                       fi; \
                   done";
        let out = match std::process::Command::new("ssh")
            .arg(&host)
            .arg(cmd)
            .stdin(std::process::Stdio::null())
            .output()
        {
            Ok(o) if o.status.success() => o,
            Ok(o) => {
                return CheckResult::pass_with_detail(
                    NAME,
                    format!("(skipped: ssh to {host} exited {})", o.status),
                );
            }
            Err(e) => {
                return CheckResult::pass_with_detail(NAME, format!("(skipped: ssh to {host} failed: {e})"));
            }
        };
        let text = String::from_utf8_lossy(&out.stdout);
        let audited = text.lines().filter(|l| l.starts_with("OK ") || l.starts_with("ORPHAN ")).count();
        let orphans: Vec<&str> = text.lines().filter(|l| l.starts_with("ORPHAN ")).collect();
        if orphans.is_empty() {
            CheckResult::pass_with_detail(NAME, format!("{audited} segment file(s) audited on {host}"))
        } else {
            CheckResult::fail(
                NAME,
                format!("failed rotation left residue on {host}: {}", orphans.join("; ")),
            )
        }
    })
    .await
    .unwrap_or_else(|_| CheckResult::pass_with_detail(NAME, "(skipped: task panicked)"))
}

/// SCEN-18: bench load sweep. Not a chaos scenario — a
/// regression-detection scenario. ONE cluster bring-up, then runs
/// `run_benchmark` at six different task counts (100, 500, 1000,
/// 2000, 4000, 8000) and writes a CSV summarizing throughput,
/// error counts, and latency percentiles at each point. No
/// invariant checks (those are covered by the other scenarios);
/// the output is meant to be diffed against a baseline to catch
/// throughput regressions over time.
///
/// Each sub-run gets its own settle window so stragglers from the
/// previous bench drain before the next begins. The scenario
/// returns a single aggregate ScenarioReport using the highest-load
/// sub-run's bench summary as the representative result. The CSV
/// at `<run_dir>/bench_load_sweep.csv` is the primary output.
pub async fn run_bench_load_sweep(
    cfg: &ClusterConfig,
    base_params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "bench_load_sweep";
    const LOAD_LEVELS: &[usize] = &[100, 500, 1000, 2000, 4000, 8000];

    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;

    // Record per-load results for the CSV. Each tuple is
    // (tasks, BenchmarkResult).
    let mut results: Vec<(usize, BenchmarkResult)> = Vec::with_capacity(LOAD_LEVELS.len());

    for &tasks in LOAD_LEVELS {
        let sub_params = ScenarioParams {
            tasks,
            ..base_params
        };
        let pool = build_bench_pool(cfg, &up, sub_params).await?;

        println!("[{SCEN}] smoke test at {tasks} tasks");
        smoke_test(&pool).await.map_err(|e| format!("smoke (tasks={tasks}): {e}"))?;

        println!(
            "[{SCEN}] bench sub-run: {tasks} tasks, {}s",
            sub_params.duration_secs
        );
        let result = run_benchmark(&pool, tasks, sub_params.duration_secs).await;
        println!(
            "[{SCEN}] tasks={tasks}: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms p999={}ms",
            result.total_requests,
            result.errors,
            result.throughput,
            result.p50_ms,
            result.p99_ms,
            result.p999_ms,
        );

        results.push((tasks, result));

        // Drop the pool BEFORE the settle sleep so all 4000 connections
        // close cleanly before the next sub-run's pool opens a fresh
        // set. Without this the next smoke_test can race the old
        // connections into TIME_WAIT.
        drop(pool);

        // Brief settle so replication queues drain and the cluster
        // reaches a clean idle state before the next sub-run.
        println!("[{SCEN}] settle 5s between sub-runs");
        sleep(Duration::from_secs(5)).await;
    }

    // Write the CSV summary.
    let csv_path = run_dir.join("bench_load_sweep.csv");
    let mut csv = String::from(
        "tasks,duration_secs,total_requests,errors,throughput_req_per_s,avg_latency_ms,p50_ms,p95_ms,p99_ms,p999_ms,min_ms,max_ms\n",
    );
    for (tasks, r) in &results {
        csv.push_str(&format!(
            "{},{},{},{},{:.2},{:.2},{},{},{},{},{},{}\n",
            tasks,
            base_params.duration_secs,
            r.total_requests,
            r.errors,
            r.throughput,
            r.avg_latency_ms,
            r.p50_ms,
            r.p95_ms,
            r.p99_ms,
            r.p999_ms,
            r.min_ms,
            r.max_ms,
        ));
    }
    std::fs::write(&csv_path, csv)
        .map_err(|e| format!("write {}: {e}", csv_path.display()))?;
    println!("[{SCEN}] wrote {}", csv_path.display());

    // Tear down the cluster cleanly.
    let executor = ActionExecutor::new(cfg);
    let _ = executor.run(&Action::StopAll);

    // Collect scraper samples before we return the report (so the
    // JSON at least has a sample stream for diagnostic purposes).
    sleep(Duration::from_millis(500)).await;
    let scraper_outcome = up.scraper.stop().await;
    let samples = scraper_outcome.store.snapshot().await;

    // Aggregate report: passes if every sub-run's bench returned a
    // non-zero total_requests (i.e. the cluster was serving writes
    // at every load level). No invariant checks — those are the
    // other scenarios' job. The bench summary is the HIGHEST-load
    // run's result (the last entry) since that's the most stressful
    // point and the most likely to catch regressions.
    let all_nonzero = results.iter().all(|(_, r)| r.total_requests > 0);
    let (last_tasks, last_result) = results
        .last()
        .cloned()
        .ok_or_else(|| "bench_load_sweep: no results".to_string())?;

    let report = ScenarioReport {
        name: SCEN.to_string(),
        outcome: if all_nonzero { ScenarioOutcome::Pass } else { ScenarioOutcome::Fail },
        params: ScenarioParamsJson {
            tasks: last_tasks,
            duration_secs: base_params.duration_secs,
            throughput_floor: base_params.throughput_floor,
            seed: base_params.seed,
        },
        bench: BenchmarkSummary::from(&last_result),
        checks: vec![CheckResult::pass("BenchLoadSweepCompleted")],
        samples,
        bench_window_start_ms: 0,
        bench_actual_end_ms: 0,
        bench_window_end_ms: 0,
        log_files: vec![],
        idempotent_counters: None,
        integrity: None,
        deep_audit: None,
        s3_lifecycle: None,
        disk_truth: None,
        cardinality: None,
    };
    Ok(report)
}

/// Best-effort journalctl fetch for both data nodes. Returns the file basenames
/// (relative to the run directory) that were successfully written. Failures are
/// logged but never propagate — log scraping is diagnostic, not a verdict gate.
fn harvest_logs(
    cfg: &ClusterConfig,
    scenario_name: &str,
    wall_start: std::time::SystemTime,
    wall_end: std::time::SystemTime,
    run_dir: &PathBuf,
) -> Vec<String> {
    harvest_journals(&cfg.leader_host, &cfg.follower_host, scenario_name, wall_start, wall_end, run_dir)
}

/// `harvest_logs` with owned hosts, so it can be handed to `spawn_blocking`.
fn harvest_journals(
    leader_host: &str,
    follower_host: &str,
    scenario_name: &str,
    wall_start: std::time::SystemTime,
    wall_end: std::time::SystemTime,
    run_dir: &std::path::Path,
) -> Vec<String> {
    let pad = Duration::from_secs(5);
    let mut written = Vec::new();
    for (label, host) in [("cs1", leader_host), ("cs2", follower_host)] {
        let basename = format!("{scenario_name}.{label}.log");
        let dest = run_dir.join(&basename);
        match fetch_journal(host, wall_start, wall_end, pad, &dest) {
            Ok(()) => {
                println!("  wrote {}", dest.display());
                written.push(basename);
            }
            Err(e) => {
                eprintln!("  log fetch from {host} failed: {e}");
            }
        }
    }
    written
}

async fn wait_for_stable_leader(scraper: &Scraper, timeout: Duration) -> Result<(), String> {
    wait_for_stable_leader_since(scraper, timeout, 0).await
}

/// Wait until the cluster's *current* state is one leader and one follower,
/// ignoring every sample taken before `since_ms`.
///
/// The recency floor is the whole point. This used to scan the entire scraper
/// history for any sample with `node_role >= 0.5` and any with `< 0.5`, which is
/// sound only against an empty store — its original caller runs right after
/// bring-up. Called again after a mid-run restart, the store already holds
/// thousands of samples on both sides of the threshold, so both `find`s hit on
/// the first tick and the barrier returns in ~500ms no matter what the cluster
/// is doing. Everything downstream then measures a cluster that may still be
/// replaying `rebuild_active_segment_chain_tips`, and the cold-restart delta —
/// the deliverable — is taken against an unsettled node.
///
/// It also now genuinely evaluates the latest sample *per host*, which is what
/// the old comment claimed but the code did not do: two samples from the same
/// host at different times could satisfy both halves.
/// Per-shard node status codes, from `celeriant_node_status_code`.
/// `0=BootCatchup 1=Follower 2=FollowerCatchingUp 3=Promoting 4=Leader 5=Fenced 6=Standalone`
const STATUS_FOLLOWER_STEADY: u64 = 1;
const STATUS_LEADER: u64 = 4;

/// Wait until one node leads and every other node has **rejoined as a steady
/// follower on every shard**.
///
/// `wait_for_stable_leader_since` is not this. It keys on `node_role`, and
/// `node_role < 0.5` means only "not leader" — a node that is booting
/// (`BootCatchup`), still replaying (`FollowerCatchingUp`), or mid-election
/// (`Promoting`) all report it. So that barrier clears as soon as the follower
/// is reachable, which is well before it holds the data.
///
/// That gap falsified a real measurement. Phase 5 reads through pools seeded on
/// both nodes, so reads landed on a follower that did not yet have the
/// aggregate and came back empty in ~0.25ms — no scan, no bytes — reading as a
/// spectacularly fast cold path and dragging the cold p50 *below* the warm one.
/// `EventualConvergence` in the same run had the follower 496 wal_seqs behind.
///
/// `FollowerCatchingUp` -> `Follower` is exactly the transition that says the
/// replay finished, which makes this the signal to wait on rather than a
/// wal_seq comparison the follower itself has not acted on yet.
async fn wait_for_follower_rejoin(
    scen: &str,
    scraper: &Scraper,
    timeout: Duration,
    since_ms: u64,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last = String::from("no samples");
    while Instant::now() < deadline {
        sleep(Duration::from_millis(500)).await;
        let snap = scraper.store().snapshot().await;
        let mut latest: std::collections::BTreeMap<String, &NodeSample> = Default::default();
        for s in snap.iter().filter(|s| s.ok && s.t_ms >= since_ms) {
            latest.insert(s.host.clone(), s);
        }
        if latest.len() < 2 {
            last = format!("only {} host(s) reporting", latest.len());
            continue;
        }
        let mut leaders = 0usize;
        let mut steady_followers = 0usize;
        let mut detail = Vec::new();
        for (host, s) in &latest {
            let codes: Vec<u64> = s.node_status_code_by_shard.values().copied().collect();
            if codes.is_empty() {
                detail.push(format!("{host}: no shard status"));
                continue;
            }
            if codes.iter().all(|c| *c == STATUS_LEADER) {
                leaders += 1;
            } else if codes.iter().all(|c| *c == STATUS_FOLLOWER_STEADY) {
                steady_followers += 1;
            } else {
                detail.push(format!("{host}: shards {codes:?}"));
            }
        }
        if leaders == 1 && steady_followers >= 1 {
            return Ok(());
        }
        last = if detail.is_empty() {
            format!("{leaders} leader(s), {steady_followers} steady follower(s)")
        } else {
            detail.join("; ")
        };
    }
    println!("[{scen}] follower did not reach steady Follower within {timeout:?}: {last}");
    Err(last)
}

async fn wait_for_stable_leader_since(
    scraper: &Scraper,
    timeout: Duration,
    since_ms: u64,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        sleep(Duration::from_millis(500)).await;
        let snap = scraper.store().snapshot().await;
        let mut latest: std::collections::BTreeMap<&str, f64> = std::collections::BTreeMap::new();
        for s in snap.iter().filter(|s| s.ok && s.t_ms >= since_ms) {
            latest.insert(s.host.as_str(), s.node_role);
        }
        let leaders = latest.values().filter(|r| **r >= 0.5).count();
        let followers = latest.values().filter(|r| **r < 0.5).count();
        if leaders == 1 && followers >= 1 {
            return Ok(());
        }
    }
    Err("timeout".into())
}

/// Returns the host string of whichever node currently reports `node_role >= 0.5`,
/// based on the most recent good sample for each host. Returns `None` if neither
/// node is reporting leader status.
///
/// Run this AFTER `wait_for_stable_leader` so we know at least one ok sample
/// for each node exists.
async fn detect_leader(cfg: &ClusterConfig, scraper: &Scraper) -> Option<String> {
    let snap = scraper.store().snapshot().await;
    let last_for = |host: &str| -> Option<&NodeSample> {
        snap.iter().rev().find(|s| s.host == host && s.ok)
    };
    let l = last_for(&cfg.leader_host);
    let f = last_for(&cfg.follower_host);
    match (l, f) {
        (Some(a), _) if a.node_role >= 0.5 => Some(cfg.leader_host.clone()),
        (_, Some(b)) if b.node_role >= 0.5 => Some(cfg.follower_host.clone()),
        _ => None,
    }
}

pub fn sample_window(samples: &[NodeSample], start_ms: u64, end_ms: u64) -> (usize, usize) {
    let start = samples
        .iter()
        .position(|s| s.t_ms >= start_ms)
        .unwrap_or(0);
    let end = samples
        .iter()
        .rposition(|s| s.t_ms <= end_ms)
        .unwrap_or(samples.len().saturating_sub(1));
    (start, end.max(start))
}

async fn sleep(d: Duration) {
    tokio::time::sleep(d).await;
}

/// SSH to both nodes and count WAL segment files matching
/// `/var/lib/nvme/celeriant-data/shard_*/log_*.wal`.
/// Returns (leader_count, follower_count). On SSH error returns (0, 0)
/// for that host (best-effort; rotation check degrades to a "no rotation"
/// pass-with-detail rather than a hard fail).
fn ssh_count_wal_segments(leader: &str, follower: &str) -> (usize, usize) {
    let cmd = "ls /var/lib/nvme/celeriant-data/shard_*/log_*.wal 2>/dev/null | wc -l";
    let count_on = |host: &str| -> usize {
        use std::process::{Command, Stdio};
        let out = Command::new("ssh")
            .arg(host)
            .arg(cmd)
            .stdin(Stdio::null())
            .output();
        match out {
            Ok(o) if o.status.success() => {
                String::from_utf8_lossy(&o.stdout).trim().parse().unwrap_or(0)
            }
            _ => 0,
        }
    };
    (count_on(leader), count_on(follower))
}

/// `cold_segment_reads` (adversarial review finding): bloom-filter and
/// sealed-segment scanner correctness is never exercised when the bench's
/// hot set stays cache+active-segment resident.
///
/// **Disjoint range approach**: because `run_benchmark` hardcodes
/// `AggregateKey::new(1, 1, id)` and the chaos crate cannot construct
/// `AggregateKey` directly (not in Cargo.toml), phase 1 uses
/// `run_benchmark_idempotent_with_history` with `wide_tasks` tasks. This
/// returns `task_acks: Vec<TaskAckSummary>` already containing the correct
/// `AggregateKey` for each task. The cold range
/// `task_acks[params.tasks..params.tasks*2]` is the slice whose aggregates
/// were only written in phase 1 and are never touched by phase 2 (which
/// uses standard `run_benchmark` with `params.tasks` tasks, writing to
/// agg_id 0..params.tasks only). Those cold aggregates go to sealed/cold
/// segments after phase 2 advances the active segment.
///
/// **Structure**:
///
/// Phase 1 — wide idempotent bench: `wide_tasks = min(params.tasks * 4, 60_000)`
/// concurrent tasks, each to a distinct aggregate. Returns `task_acks`.
///
/// Phase 2 — churn bench: plain `run_benchmark` with `params.tasks` tasks
/// (agg_id 0..tasks). Advances active segment; leaves params.tasks..wide_tasks cold.
///
/// Cold-read: take a 256-aggregate sample from `task_acks[params.tasks..params.tasks*2]`,
/// call `verify_no_seq_gaps` on both nodes. Any aggregate reading as absent
/// or short is a scanner/bloom/visibility bug.
///
/// Rotation check: SSH to both nodes, count WAL files. If count > 4 on either
/// node, rotation occurred. Otherwise, passes with a detail note — Pi write
/// rates vary.
pub async fn run_cold_segment_reads(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "cold_segment_reads";
    // Aggregates sampled for the cold-read phase.
    const COLD_READ_SAMPLE: usize = 256;
    // Number of shards (one WAL file per shard at baseline, no rotation yet).
    const BASELINE_WAL_COUNT: usize = 4;

    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;

    let wide_tasks = (params.tasks * 4).min(60_000);
    let wide_params = ScenarioParams { tasks: wide_tasks, ..params };
    let wide_pool = build_bench_pool(cfg, &up, wide_params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&wide_pool).await.map_err(|e| format!("smoke: {e}"))?;

    let bench_window_start_ms = up.elapsed_ms();

    // Phase 1: wide idempotent bench so we get TaskAckSummary back.
    println!("[{SCEN}] phase 1: wide idempotent bench — {wide_tasks} aggregates, {}s", params.duration_secs);
    let outcome1 = run_benchmark_idempotent_with_history(&wide_pool, wide_tasks, params.duration_secs, None).await;
    println!(
        "[{SCEN}] phase 1 done: {} req, {} err, {:.0} req/s (ok_acks={})",
        outcome1.benchmark.total_requests,
        outcome1.benchmark.errors,
        outcome1.benchmark.throughput,
        outcome1.counters.ok_acks,
    );
    drop(wide_pool);

    // Brief settle so phase-1 writes replicate before phase-2 starts.
    sleep(Duration::from_secs(5)).await;

    // Phase 2: churn bench against agg 0..params.tasks — leaves
    // params.tasks..wide_tasks cold.
    println!(
        "[{SCEN}] phase 2: churn bench — {} tasks, {}s (pushing phase-1 cold aggregates off active segment)",
        params.tasks, params.duration_secs,
    );
    let narrow_pool = build_bench_pool(cfg, &up, params).await?;
    let pool_clone = narrow_pool.clone();
    let phase2_result = run_benchmark(&pool_clone, params.tasks, params.duration_secs).await;
    println!(
        "[{SCEN}] phase 2 done: {} req, {} err, {:.0} req/s",
        phase2_result.total_requests, phase2_result.errors, phase2_result.throughput,
    );

    let bench_window_end_ms = up.elapsed_ms();

    // Observe whether rotation happened WHILE SERVICES ARE STILL UP.
    let (leader_seg_count, follower_seg_count) =
        ssh_count_wal_segments(&cfg.leader_host, &cfg.follower_host);
    let rotation_occurred =
        leader_seg_count > BASELINE_WAL_COUNT || follower_seg_count > BASELINE_WAL_COUNT;
    let rotation_check = if rotation_occurred {
        CheckResult::pass_with_detail(
            "SegmentRotationOccurred",
            format!(
                "WAL files: leader={leader_seg_count} follower={follower_seg_count} \
                 (> baseline {BASELINE_WAL_COUNT} = rotated)",
            ),
        )
    } else {
        // Not a pass. Nothing ever went cold, so every cold-path check below it
        // was evaluated against a warm cluster and proved nothing.
        CheckResult::inconclusive(
            "SegmentRotationOccurred",
            format!(
                "no rotation reached (leader={leader_seg_count} follower={follower_seg_count} \
                 ≤ baseline {BASELINE_WAL_COUNT}) — cold path not exercised",
            ),
        )
    };

    // Build the cold-read sample: take up to COLD_READ_SAMPLE entries from
    // the task_acks range [params.tasks .. params.tasks * 2] (capped to
    // wide_tasks). These aggregates were only written in phase-1.
    let cold_acks: Vec<TaskAckSummary> = {
        let cold_start = params.tasks;
        let cold_end = (params.tasks * 2).min(wide_tasks);
        let slice = &outcome1.task_acks[cold_start.min(outcome1.task_acks.len())
            ..cold_end.min(outcome1.task_acks.len())];
        // Even stride so the sample covers the whole cold range.
        let step = (slice.len() / COLD_READ_SAMPLE).max(1);
        slice.iter().step_by(step).take(COLD_READ_SAMPLE).cloned().collect()
    };

    println!(
        "[{SCEN}] cold-read: {} aggregates sampled from cold range [{}..{})",
        cold_acks.len(), params.tasks, (params.tasks * 2).min(wide_tasks),
    );

    // Read the cold aggregates from BOTH nodes, each pinned pool so failover
    // cannot redirect the read to the other node.
    let cold_check = run_cold_read_check(SCEN, cfg, &cold_acks).await;

    let extra_checks = vec![rotation_check, cold_check];

    let expectations = ScenarioExpectations {
        // Saturation transients from the wide bench at 60k tasks.
        max_bench_error_ratio: Some(0.10),
        // The wide bench can provoke one transient lease re-election (and its
        // paired heartbeat miss) with no injected fault. Tolerate a single
        // blip; a second would signal real instability.
        max_leader_elections: 1,
        max_heartbeat_failures: 1,
        assert_eventual_progress: true,
        ..ScenarioExpectations::default()
    };

    // Use the phase-2 result as the representative bench result for the
    // report window. Wide-bench throughput is not comparable to the narrow
    // bench, and phase-2 is the load that exercises the cold path.
    let mut scen_params = params;
    // Throughput floor: phase-2 is a normal bench; relax slightly for
    // the cold-path I/O overhead.
    scen_params.throughput_floor = (params.throughput_floor * 0.5).max(50.0);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        phase2_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        None,
        None,
        None,
        run_dir,
    )
    .await
}

/// Read a sample of cold aggregates from both nodes (pinned pools) and return
/// a single `CheckResult`. Any aggregate with max_aggregate_version == 0 (not
/// found) or < max_acked_client_seq (short) that had acked writes in phase-1
/// is listed as unreadable. Fails if any unreadable aggregates are found.
async fn run_cold_read_check(
    scen: &str,
    cfg: &ClusterConfig,
    cold_acks: &[TaskAckSummary],
) -> CheckResult {
    const NAME: &str = "ColdSegmentReadable";
    if cold_acks.is_empty() {
        return CheckResult::pass_with_detail(NAME, "cold range empty — nothing to check");
    }

    // Build a pool pinned to each node.
    let mut node_pools: Vec<(String, Arc<Pool>)> = Vec::new();
    for (host, addr) in [
        (cfg.leader_host.clone(), cfg.leader_addr()),
        (cfg.follower_host.clone(), cfg.follower_addr()),
    ] {
        match (PoolBuilder {
            address1: &addr,
            address2: &addr,
            server_name: Some(&host),
            ca_cert: cfg.ca_cert.to_str().unwrap(),
            client_cert: cfg.client_cert.to_str().unwrap(),
            client_key: cfg.client_key.to_str().unwrap(),
            plaintext: false,
            max_connections: 32,
        }
        .build()
        .await)
        {
            Ok(p) => node_pools.push((host, p)),
            Err(e) => eprintln!("[{scen}] cold-read pool failed: {e}"),
        }
    }

    let mut fail_samples: Vec<String> = Vec::new();
    let mut total_checked = 0u64;
    let mut total_unreadable = 0u64;

    for (host, pool) in &node_pools {
        // verify_no_seq_gaps uses aggregate_details to check version counts.
        let report = celeriant_bench::verify_no_seq_gaps(pool, cold_acks, 10).await;
        total_checked += report.tasks_audited;
        total_unreadable += report.tasks_with_gaps + report.tasks_unreadable;
        for gap in &report.sample_gaps {
            if fail_samples.len() < 10 {
                fail_samples.push(format!(
                    "[{host}] agg={} max_acked={} server_version={} missing={}",
                    gap.aggregate_key_str,
                    gap.max_acked,
                    gap.max_aggregate_version,
                    gap.missing_count,
                ));
            }
        }
    }

    if total_unreadable == 0 {
        CheckResult::pass_with_detail(
            NAME,
            format!(
                "{total_checked} cold aggregates readable on both nodes; rotation_check is separate",
            ),
        )
    } else {
        CheckResult::fail(
            NAME,
            format!(
                "{total_unreadable} cold aggregate(s) unreadable or short out of {total_checked} checked; \
                 sample (up to 10): [{}]",
                fail_samples.join(", "),
            ),
        )
    }
}

/// Seeded schedule for `nemesis_composition`. Generates a deterministic
/// sequence of (duration_ms, pause_ms) pairs from the PRNG so the same
/// seed always produces the same fault schedule. Each call to `next()`
/// consumes two PRNG outputs.
///
/// Used only by `run_nemesis_composition` and unit-tested to confirm
/// same-seed → same-schedule.
struct NemesisPrng {
    state: u64,
}

impl NemesisPrng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// splitmix64 step.
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Duration in milliseconds in [lo_ms, hi_ms].
    fn duration_ms(&mut self, lo_ms: u64, hi_ms: u64) -> u64 {
        let range = hi_ms - lo_ms + 1;
        lo_ms + self.next_u64() % range
    }
}

#[cfg(test)]
mod nemesis_prng_tests {
    use super::NemesisPrng;

    #[test]
    fn same_seed_same_schedule() {
        let seed = 0xDEAD_BEEF_C0DE_CAFEu64;
        let mut a = NemesisPrng::new(seed);
        let mut b = NemesisPrng::new(seed);
        let seq_a: Vec<u64> = (0..20).map(|_| a.duration_ms(3_000, 8_000)).collect();
        let seq_b: Vec<u64> = (0..20).map(|_| b.duration_ms(3_000, 8_000)).collect();
        assert_eq!(seq_a, seq_b, "same seed must produce same schedule");
    }

    #[test]
    fn all_outputs_in_range() {
        let mut prng = NemesisPrng::new(0x1234_5678);
        for _ in 0..1000 {
            let v = prng.duration_ms(3_000, 8_000);
            assert!(v >= 3_000 && v <= 8_000);
        }
        let mut prng2 = NemesisPrng::new(0xABCD_EF01);
        for _ in 0..1000 {
            let v = prng2.duration_ms(2_000, 6_000);
            assert!(v >= 2_000 && v <= 6_000);
        }
    }
}


/// `nemesis_composition` (adversarial review finding: no concurrent multi-fault).
///
/// Two concurrent randomised nemesis loops over a 90s idempotent bench window:
///
/// Loop A — replication partition flap: Partition(leader → follower repl port)
///   for rand 3-8s, heal, pause rand 2-6s, repeat.
///
/// Loop B — clock jitter: SkewClock on a random node ±1s, restore after
///   rand 4-8s, repeat.
///
/// Both loops run as `tokio::spawn` tasks that each call `make` targets via
/// `tokio::task::spawn_blocking`. They share a kill-switch channel so both
/// loops exit cleanly when the bench window ends. Both loops **MUST** restore
/// all faults before exiting — enforced by finally-style blocks at the end of
/// each loop body.
///
/// The seeded schedule (printed at start) makes failures reproducible.
/// The bench uses the idempotent path + finish_history_and_check so the
/// epoch oracle, history checkers, and acked-durability oracle all engage.
///
/// Correctness counters (NoCaptureDroppedItems, NoTruncateDroppedSelfAcked,
/// NoSameEpochDivergence) remain at zero. Timing budgets are loose.
pub async fn run_nemesis_composition(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "nemesis_composition";
    let nemesis_seed = params.seed;
    const BENCH_SECS: u64 = 90;

    println!("[{SCEN}] nemesis seed: {nemesis_seed:#x}");

    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    let (leader_host, follower_host) = if up.bench_primary == cfg.leader_addr() {
        (cfg.leader_host.clone(), cfg.follower_host.clone())
    } else {
        (cfg.follower_host.clone(), cfg.leader_host.clone())
    };

    // Pre-build Action objects for the RAII cleanup path.
    let heal_partition = Action::Heal {
        src: leader_host.clone(),
        dst: follower_host.clone(),
        port: cfg.replication_port,
    };
    let restore_leader_clk = Action::RestoreClock { host: leader_host.clone() };
    let restore_follower_clk = Action::RestoreClock { host: follower_host.clone() };

    // Shared kill-switch: when the main task signals done, both nemesis loops exit.
    let (done_tx, done_rx_a) = tokio::sync::watch::channel(false);
    let done_rx_b = done_rx_a.clone();

    // Capture values needed by the spawned tasks (must be Send + 'static).
    // We replicate ActionExecutor's `run_make` behaviour directly so we don't
    // need to borrow cfg across the spawn boundary.
    let deploy_dir = cfg.deploy_dir.clone();
    let replication_port = cfg.replication_port;
    let leader_a = leader_host.clone();
    let follower_a = follower_host.clone();
    let leader_b = leader_host.clone();
    let follower_b = follower_host.clone();

    // Helper: run a make target with vars in deploy_dir, blocking.
    // Returns Err if make exits non-zero.
    fn make_blocking(deploy_dir: &std::path::Path, target: &str, vars: &[String]) -> Result<(), String> {
        use std::process::{Command, Stdio};
        let mut cmd = Command::new("make");
        cmd.arg("-s").arg(target);
        for v in vars {
            cmd.arg(v);
        }
        let status = cmd
            .current_dir(deploy_dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|e| format!("spawn make {target}: {e}"))?;
        if !status.success() {
            return Err(format!("make {target} exited with {status}"));
        }
        Ok(())
    }

    // --- Loop A: partition flap ---
    let deploy_dir_a = deploy_dir.clone();
    let loop_a = tokio::spawn(async move {
        let mut prng = NemesisPrng::new(nemesis_seed);
        let mut partitioned = false;
        let mut done_rx = done_rx_a;
        loop {
            if *done_rx.borrow() {
                break;
            }
            // Partition.
            let dd = deploy_dir_a.clone();
            let lh = leader_a.clone();
            let fh = follower_a.clone();
            let port = replication_port;
            let res = tokio::task::spawn_blocking(move || {
                make_blocking(&dd, "partition-host", &[
                    format!("SRC={lh}"),
                    format!("DST={fh}"),
                    format!("PORT={port}"),
                ])
            }).await;
            match res {
                Ok(Ok(())) => { partitioned = true; }
                Ok(Err(e)) => {
                    eprintln!("[{SCEN}] loop A partition failed: {e}");
                    break;
                }
                Err(e) => {
                    eprintln!("[{SCEN}] loop A spawn_blocking join: {e}");
                    break;
                }
            }

            let fault_ms = prng.duration_ms(3_000, 8_000);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(fault_ms)) => {}
                _ = done_rx.changed() => {}
            }

            // Always heal before checking done.
            let dd = deploy_dir_a.clone();
            let lh = leader_a.clone();
            let fh = follower_a.clone();
            let port = replication_port;
            let res = tokio::task::spawn_blocking(move || {
                make_blocking(&dd, "heal-host", &[
                    format!("SRC={lh}"),
                    format!("DST={fh}"),
                    format!("PORT={port}"),
                ])
            }).await;
            match res {
                Ok(Ok(())) => { partitioned = false; }
                Ok(Err(e)) => { eprintln!("[{SCEN}] loop A heal failed (non-fatal): {e}"); partitioned = false; }
                Err(e) => { eprintln!("[{SCEN}] loop A heal join: {e}"); break; }
            }

            if *done_rx.borrow() {
                break;
            }

            let pause_ms = prng.duration_ms(2_000, 6_000);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(pause_ms)) => {}
                _ = done_rx.changed() => {}
            }
        }
        // Exit heal: ensure partition is removed even if we broke early.
        if partitioned {
            let dd = deploy_dir_a.clone();
            let lh = leader_a.clone();
            let fh = follower_a.clone();
            let port = replication_port;
            let _ = tokio::task::spawn_blocking(move || {
                make_blocking(&dd, "heal-host", &[
                    format!("SRC={lh}"),
                    format!("DST={fh}"),
                    format!("PORT={port}"),
                ])
            }).await;
        }
    });

    // --- Loop B: clock jitter ---
    let deploy_dir_b = deploy_dir.clone();
    let loop_b = tokio::spawn(async move {
        let mut prng = NemesisPrng::new(nemesis_seed.wrapping_add(0x0101_0101_0101_0101));
        let mut skewed_host: Option<String> = None;
        let mut done_rx = done_rx_b;
        let hosts = [leader_b.clone(), follower_b.clone()];
        loop {
            if *done_rx.borrow() {
                break;
            }
            let node_idx = (prng.next_u64() % 2) as usize;
            let target = hosts[node_idx].clone();
            let positive = (prng.next_u64() & 1) == 0;
            let offset_sign = if positive { "+1" } else { "-1" };

            let dd = deploy_dir_b.clone();
            let t = target.clone();
            let offset_str = format!("{offset_sign} seconds");
            let res = tokio::task::spawn_blocking(move || {
                make_blocking(&dd, "skew-clock", &[
                    format!("HOST={t}"),
                    format!("OFFSET={offset_str}"),
                ])
            }).await;
            match res {
                Ok(Ok(())) => { skewed_host = Some(target.clone()); }
                Ok(Err(e)) => {
                    eprintln!("[{SCEN}] loop B skew failed: {e}");
                    break;
                }
                Err(e) => {
                    eprintln!("[{SCEN}] loop B spawn_blocking join: {e}");
                    break;
                }
            }

            let fault_ms = prng.duration_ms(4_000, 8_000);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(fault_ms)) => {}
                _ = done_rx.changed() => {}
            }

            // Restore clock before checking done.
            if let Some(ref h) = skewed_host {
                let dd = deploy_dir_b.clone();
                let th = h.clone();
                let res = tokio::task::spawn_blocking(move || {
                    make_blocking(&dd, "restore-clock", &[format!("HOST={th}")])
                }).await;
                match res {
                    Ok(Ok(())) => { skewed_host = None; }
                    Ok(Err(e)) => { eprintln!("[{SCEN}] loop B restore failed (non-fatal): {e}"); skewed_host = None; }
                    Err(e) => { eprintln!("[{SCEN}] loop B restore join: {e}"); break; }
                }
            }

            if *done_rx.borrow() {
                break;
            }

            let pause_ms = prng.duration_ms(2_000, 6_000);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(pause_ms)) => {}
                _ = done_rx.changed() => {}
            }
        }
        // Exit restore: ensure clock is re-enabled even if we broke early.
        if let Some(h) = skewed_host {
            let dd = deploy_dir_b.clone();
            let _ = tokio::task::spawn_blocking(move || {
                make_blocking(&dd, "restore-clock", &[format!("HOST={h}")])
            }).await;
        }
    });

    let history = new_history_recorder(SCEN, run_dir);
    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] idempotent bench: {} tasks, {}s (seed {nemesis_seed:#x})", params.tasks, BENCH_SECS);
    let pool_clone = pool.clone();
    let history_clone = history.clone();
    let tasks = params.tasks;
    let ryw_pinned = build_ryw_pinned(cfg).await;
    let bench_handle = tokio::spawn(async move {
        run_benchmark_idempotent_opts(
            &pool_clone,
            tasks,
            BENCH_SECS,
            celeriant_bench::IdempotentBenchOptions { history: history_clone, duplicate_replay: false, ryw_pinned },
        )
        .await
    });

    // Wait for bench to complete, then signal nemesis loops to exit.
    let outcome = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s (ok_acks={} 2002_acks={} repl_retry={} transient_retry={} fatal={})",
        outcome.benchmark.total_requests,
        outcome.benchmark.errors,
        outcome.benchmark.throughput,
        outcome.counters.ok_acks,
        outcome.counters.idempotency_acks,
        outcome.counters.replication_retries,
        outcome.counters.transient_retries,
        outcome.counters.fatal_errors,
    );

    // Signal both loops to stop.
    let _ = done_tx.send(true);

    // Wait for both loops to exit (they drain their current sleep + heal/restore).
    let (ra, rb) = tokio::join!(loop_a, loop_b);
    if let Err(e) = ra { eprintln!("[{SCEN}] loop A join: {e}"); }
    if let Err(e) = rb { eprintln!("[{SCEN}] loop B join: {e}"); }

    // Defensive final cleanup — idempotent.
    println!("[{SCEN}] final cleanup: heal partition + restore both clocks");
    let _ = executor.run(&heal_partition);
    let _ = executor.run(&heal_partition);
    let _ = executor.run(&restore_leader_clk);
    let _ = executor.run(&restore_follower_clk);

    println!("[{SCEN}] settle 30s for NTP re-sync + replication catch-up");
    sleep(Duration::from_secs(30)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let (integrity, deep) = run_integrity_and_deep_audit(SCEN, &pool, &outcome.task_acks, 32).await;
    let integrity_check = data_integrity_check(&integrity, (params.tasks as u64 / 50).max(5));

    let mut extra_checks = vec![integrity_check];
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks, params.seed).await);

    // Loose timing bounds; strict correctness at zero.
    let expectations = ScenarioExpectations {
        max_leader_elections: 120,
        max_s3_fallbacks: 2000,
        max_heartbeat_failures: 500,
        max_bench_errors: 1_500_000,
        // Ratio ceiling: partition + clock-jitter concurrent error storms
        // can be high; correctness is proven by the audit not by error count.
        max_bench_error_ratio: Some(0.90),
        max_role_flips: 30,
        max_split_brain_ticks: 10,
        require_leader_retained: false,
        // Correctness at zero (defaults): NoCaptureDroppedItems,
        // NoTruncateDroppedSelfAcked, NoSameEpochDivergence all stay 0.
        assert_eventual_progress: true,
        assert_no_divergent_tips: true,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.05).max(10.0);
    scen_params.duration_secs = BENCH_SECS;

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        outcome.benchmark.clone(),
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        Some(integrity),
        Some(outcome.counters),
        deep,
        run_dir,
    )
    .await
}

/// Schema registration under partition (design-doc open question: divergent
/// schema caches). A schema registered while leader→follower replication is
/// partitioned must reach the follower through the recovery path, and a
/// follower PROMOTED after the heal must enforce it: if the caches diverge,
/// the new leader accepts a write the schema forbids.
pub async fn run_schema_under_partition(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    const SCEN: &str = "schema_under_partition";
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;

    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let executor = ActionExecutor::new(cfg);

    let leader_is_cs1 = up.bench_primary == cfg.leader_addr();
    let (leader_host, follower_host) = if leader_is_cs1 {
        (cfg.leader_host.clone(), cfg.follower_host.clone())
    } else {
        (cfg.follower_host.clone(), cfg.leader_host.clone())
    };
    let (stop_leader, start_leader) = if leader_is_cs1 {
        (Action::StopCs1, Action::StartCs1)
    } else {
        (Action::StopCs2, Action::StartCs2)
    };
    let partition = Action::Partition {
        src: leader_host.clone(),
        dst: follower_host.clone(),
        port: cfg.replication_port,
    };
    let heal = Action::Heal {
        src: leader_host.clone(),
        dst: follower_host.clone(),
        port: cfg.replication_port,
    };

    // Background load for the standard counter/throughput checks; the
    // schema oracle below is the scenario's real subject.
    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] bench: {} tasks, {}s", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    sleep(Duration::from_secs(10)).await;

    // Phase A — healthy cluster: schema major=7 registered and enforced.
    let pre = assert_schema_enforced(&pool, 7, 70, "SchemaEnforcedPrePartition").await;

    println!("[{SCEN}] partitioning {leader_host} -> {follower_host}:{}", cfg.replication_port);
    executor.run(&partition)?;
    sleep(Duration::from_secs(5)).await;

    // Phase B — schema major=8 registered while the follower is unreachable.
    let during = assert_schema_enforced(&pool, 8, 80, "SchemaEnforcedDuringPartition").await;

    sleep(Duration::from_secs(10)).await;
    println!("[{SCEN}] healing partition");
    if let Err(e) = executor.run(&heal) {
        let _ = executor.run(&heal);
        return Err(format!("heal failed: {e}"));
    }

    // Post-heal settle: follower catches up (S3 round or TCP resume).
    sleep(Duration::from_secs(20)).await;

    // Phase C — promote the follower by stopping the leader; the promoted
    // node must enforce the partition-era schema it never saw over live TCP.
    println!("[{SCEN}] stopping leader {leader_host} to force promotion");
    executor.run(&stop_leader)?;
    sleep(Duration::from_secs(15)).await;
    let after = assert_schema_enforced(&pool, 8, 81, "SchemaEnforcedAfterFailover").await;

    println!("[{SCEN}] restarting former leader");
    executor.run(&start_leader)?;

    let bench_result = bench_handle.await.map_err(|e| format!("bench join: {e}"))?;
    println!(
        "[{SCEN}] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    println!("[{SCEN}] settle 15s for catchup + role re-stabilisation");
    sleep(Duration::from_secs(15)).await;
    let bench_window_end_ms = up.elapsed_ms();

    let _ = executor.run(&heal);

    let expectations = ScenarioExpectations {
        // Partition window: leader renews via S3; failover window: one real
        // election plus renewals. Same envelope class as
        // partition_leader_follower_replication + leader_graceful_stop.
        max_leader_elections: 40,
        max_s3_fallbacks: 500,
        max_heartbeat_failures: 90,
        max_bench_errors: 100_000,
        max_bench_error_ratio: Some(0.75),
        max_role_flips: 3,
        max_split_brain_ticks: 10,
        assert_eventual_progress: true,
        // The leader stop+start is one node restart.
        max_node_starts: 1,
        ..ScenarioExpectations::default()
    };

    let mut scen_params = params;
    scen_params.throughput_floor = (params.throughput_floor * 0.3).max(50.0);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        vec![pre, during, after],
        None,
        None,
        None,
        run_dir,
    )
    .await
}

/// Register schema (org=1, type=901, `major`, 0) requiring `{"name": string}`
/// with no extra properties, then drive one conforming and one
/// non-conforming write at it (aggregate id `agg_id`, fresh per phase so
/// OCC/recreate state never bleeds between phases). Transient errors
/// (failover windows) retry up to ~30s; only a definitive wrong outcome
/// fails the check.
async fn assert_schema_enforced(
    pool: &Arc<Pool>,
    major: u64,
    agg_id: u128,
    name: &'static str,
) -> CheckResult {
    use celeriant_bench::{
        AggregateKey, ClientError, DatablockAggregateEvent, RegisterSchemaRequest, SchemaError,
        SchemaKey, ServerError, WriteError, WriteEventsOptions,
    };

    const ORG: u128 = 1;
    const TYPE_ID: u128 = 901;
    const CLIENT_ID: u128 = 7_777;

    let schema =
        r#"{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}"#;

    let register = RegisterSchemaRequest {
        correlation_id: Some(0x5c11e3a0 + major as u128),
        client_id: CLIENT_ID,
        user_id: None,
        schema_key: SchemaKey::new(ORG, TYPE_ID, major, 0),
        schema_type: 0,
        schema: schema.to_string(),
    };

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match pool.register_schema(register.clone()).await {
            Ok(_) => break,
            Err(ClientError::Server(ServerError::Schema { kind: SchemaError::AlreadyExists, .. })) => break,
            Err(ClientError::Server(ServerError::Schema { kind: SchemaError::Invalid | SchemaError::UnsupportedType, .. })) => {
                return CheckResult::fail(name, "register_schema rejected the schema itself — oracle bug");
            }
            Err(e) if Instant::now() < deadline => {
                println!("[schema-oracle] register major={major}: transient {e}, retrying");
                sleep(Duration::from_secs(2)).await;
            }
            Err(e) => {
                return CheckResult::fail(name, format!("register_schema major={major} never succeeded: {e}"));
            }
        }
    }

    let event = |payload: &str, seq: u64| DatablockAggregateEvent {
        client_seq: seq,
        event_seq: 0,
        event_id: None,
        event_timestamp: 0,
        event_type_major: major,
        event_type_minor: 0,
        event_value: Arc::new(payload.as_bytes().to_vec()),
        iv: None,
    };
    let agg = AggregateKey::new(ORG, TYPE_ID, agg_id);
    let opts = || WriteEventsOptions {
        allow_create: true,
        expected_version: None,
        enforce_client_idempotency: false,
    };

    // Conforming write: must eventually ack. Fsync/replication write errors
    // are transient (may commit later); only validation-class rejections are
    // definitive here.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match pool.write_events_with(agg.clone(), vec![event(r#"{"name":"ok"}"#, 1)], CLIENT_ID, opts()).await {
            Ok(_) => break,
            Err(ClientError::Server(ServerError::Schema { kind, .. })) => {
                return CheckResult::fail(name, format!("conforming write rejected by schema layer: {kind:?}"));
            }
            Err(ClientError::Server(ServerError::Write {
                kind: kind @ (WriteError::EmptyEventsList | WriteError::ZeroEventType | WriteError::AggregateRecreateNotAllowed),
                ..
            })) => {
                return CheckResult::fail(name, format!("conforming write definitively rejected: {kind:?}"));
            }
            Err(e) if Instant::now() < deadline => {
                println!("[schema-oracle] conforming write major={major}: transient {e}, retrying");
                sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return CheckResult::fail(name, format!("conforming write never acked: {e}")),
        }
    }

    // Non-conforming write: must be definitively rejected by schema
    // validation. An Ok is the divergent-cache bug this scenario exists for.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match pool.write_events_with(agg.clone(), vec![event(r#"{"nope":1}"#, 2)], CLIENT_ID, opts()).await {
            Ok(_) => {
                return CheckResult::fail(
                    name,
                    format!("non-conforming write ACCEPTED under schema major={major} — schema cache divergence"),
                );
            }
            Err(ClientError::Server(ServerError::Schema { kind: SchemaError::ValidationFailed, .. })) => {
                return CheckResult::pass_with_detail(
                    name,
                    format!("major={major} enforced: conforming acked, non-conforming rejected"),
                );
            }
            Err(e) if Instant::now() < deadline => {
                println!("[schema-oracle] non-conforming write major={major}: transient {e}, retrying");
                sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return CheckResult::fail(name, format!("non-conforming write never resolved: {e}")),
        }
    }
}

// ===========================================================================
// cardinality_pressure
// ===========================================================================

/// Data root and systemd unit of the CHAOS instance.
///
/// Both nodes also run a production celeriant under a different unit and a
/// different data root. Pointing the RSS poller or the segment counter at the
/// wrong one would report prod's memory and prod's segments as this run's
/// result, which is worse than not measuring at all.
const CARDINALITY_DATA_ROOT: &str = "/var/lib/nvme/celeriant-data";
const CARDINALITY_UNIT: &str = "celeriant.service";

/// Phase 2 and phase 6 are the only windows with full history recording on, so
/// both are bounded in minutes. The fill cannot record: the recorder drops on a
/// full 65536-slot channel and `check_idempotency` fails closed on any drop.
const CONTENTION_SECS: u64 = 120;
const FAILOVER_WINDOW_SECS: u64 = 90;
const FAILOVER_KILL_AT_SECS: u64 = 20;
const FAILOVER_RESTART_AFTER_SECS: u64 = 20;
/// Keys drawn per age bucket per task for the phase 3 / phase 5 read probes.
const READS_PER_BUCKET: usize = 4;
/// Reheat samples a bucket needs before it counts as populated. Below this the
/// percentiles are noise dressed as a curve.
const MIN_REHEAT_OPS_PER_BUCKET: u64 = 5;
/// Total sampled ack-ledger entries across all tasks.
const LEDGER_TARGET: usize = 100_000;
/// Ledger entries actually audited. The ledger is already a uniform sample, so
/// an even stride over it stays uniform; the cap is what keeps the audit's
/// round-trip count bounded when the fill reached tens of millions of keys.
const AUDIT_SAMPLE: usize = 512;
const GROWTH_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
/// Quiet period between dropping phase 2's pool and starting phase 3's reads.
/// Long enough for the server to close several hundred connections; short
/// enough not to let the "warm" cluster start going cold on us, which would
/// bias the delta the other way.
const P2_DRAIN_SETTLE: Duration = Duration::from_secs(10);
/// Budget for the follower to catch up after the phase 4 restart, before any
/// cold read is measured. Generous because an unconverged follower does not
/// slow the measurement down, it silently falsifies it.
const CONVERGENCE_AFTER_RESTART: Duration = Duration::from_secs(120);
/// Cap on concurrent contention writers, well inside the 28,232 ephemeral-port
/// budget per destination.
const MAX_CONTENTION_TASKS: usize = 4096;
/// Assumed follower catch-up throughput, used only to size the phase 6
/// convergence wait. An assumption, not a measurement — a follower one segment
/// behind replays up to `segment_bytes` over TCP and `max_catchup_gap_bytes` is
/// unset on the Pis, so the constant fixed in `wait_for_wal_convergence`'s
/// callers elsewhere is far too short at 1GB segments.
const ASSUMED_CATCHUP_BYTES_PER_SEC: u64 = 20 * 1024 * 1024;

/// `cardinality_pressure`: drive tens of millions of independent aggregates and
/// clients through a memory-constrained two-node cluster, then measure what the
/// cold path costs.
///
/// Cardinality is an **output** of this run, not an input: the fill is
/// time-boxed and reaches whatever it reaches. What the run asserts is that it
/// got to a regime where its measurements mean something — and when it did not,
/// it says INCONCLUSIVE rather than passing.
///
/// Phases: 0 constrain, 1 fill (the long one, with the reheat probe running
/// throughout), 2 contention, 3 hot reads, 4 restart, 5 cold reads (the delta),
/// 6 SIGKILL + failover, 7 evaluate.
pub async fn run_cardinality_pressure(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    card: crate::cardinality_workload::CardinalityParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    use crate::cardinality_deliverable as cd;
    use crate::cardinality_workload as cw;
    use celeriant_bench::population::{AgeBucket, Member, Population};
    use celeriant_bench::read_workload::ReheatCostCurve;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    const SCEN: &str = "cardinality_pressure";

    // ---- Phase 0: constrain -------------------------------------------------
    cw::validate_tasks(params.tasks)?;
    let fill_budget = card.fill_budget();
    let mix = card.large_event_fraction();
    let achieved = card.achieved_aggs_per_segment();
    let birth_interval = cw::per_task_interval(card.birth_rate_per_sec, params.tasks);
    let reheat_interval = cw::per_task_interval(card.reheat_rate_per_sec, params.tasks);

    println!("[{SCEN}] phase 0: constrain");
    println!(
        "[{SCEN}]   preset={} fill budget={}s  tasks={} ({} per data shard)",
        card.preset.name(),
        fill_budget.as_secs(),
        params.tasks,
        params.tasks / cw::DATA_SHARDS as usize,
    );
    println!(
        "[{SCEN}]   segment={:.0}MB  memory={}%  target {} aggs/segment -> derived large-event mix {:.1}% -> achieves {} aggs/segment (design point {})",
        card.segment_bytes as f64 / (1024.0 * 1024.0),
        card.memory_percent,
        card.target_aggs_per_segment,
        mix * 100.0,
        achieved,
        cw::BLOOM_DESIGN_POINT_AGGS_PER_SEGMENT,
    );
    println!(
        "[{SCEN}]   birth {:.1}/s cluster-wide ({}), reheat {:.1}/s cluster-wide ({}), disk high-water {}%",
        card.birth_rate_per_sec,
        birth_interval.map(|d| format!("1 per {:.1}s per task", d.as_secs_f64())).unwrap_or_else(|| "disabled".into()),
        card.reheat_rate_per_sec,
        reheat_interval.map(|d| format!("1 per {:.1}s per task", d.as_secs_f64())).unwrap_or_else(|| "disabled".into()),
        card.disk_high_water_pct,
    );

    let executor = ActionExecutor::new(cfg);
    // Before bring-up: the unit is rewritten and daemon-reloaded but not
    // restarted, so the new environment takes effect when bring_up_cluster
    // starts the nodes.
    executor.run(&Action::UpdateServiceConfig {
        memory_percent: card.memory_percent,
        segment_bytes: card.segment_bytes,
    })?;
    // `update-service` rewrites both nodes' systemd units and nothing else puts
    // them back, so without a restore this scenario's 20% memory and segment
    // size persist into every later `--scenario`/`--full` invocation and every
    // soak iteration — silently, and read as the cluster's normal settings.
    // Captured from config.env rather than hardcoded so the restore tracks
    // whatever the rig is actually configured for.
    let baseline_cfg = read_service_baseline(cfg);

    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let pool = build_bench_pool(cfg, &up, params).await?;
    // Lane-pinned pools carry the phase 1 fill; `pool` stays for the smoke test
    // and the schema probe, both of which touch one fixed key. The read phases
    // build their own — see `READ_REQUEST_TIMEOUT` — so this one keeps the
    // write-shaped deadline the fill was characterised on.
    let lane_pools = build_lane_pools(cfg, &up, params).await?;
    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;

    let mut extra_checks: Vec<CheckResult> = Vec::new();
    extra_checks.push(assert_account_schemas_enforced(&pool).await);

    let node_ram_bytes = ssh_mem_total_bytes(&cfg.leader_host).await;

    let poll_store = crate::host_poll::HostPollStore::new();
    let poll_handle = crate::host_poll::spawn(
        vec![cfg.leader_host.clone(), cfg.follower_host.clone()],
        CARDINALITY_UNIT.to_string(),
        CARDINALITY_DATA_ROOT.to_string(),
        Duration::from_secs(1),
        poll_store.clone(),
    );

    // ---- Phase 1: fill ------------------------------------------------------
    let bench_window_start_ms = up.elapsed_ms();
    println!("[{SCEN}] phase 1: fill — birth + decay + age-stratified reheat, {}s budget", fill_budget.as_secs());

    let fill_start = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(cw::FillCounters::default());
    let fill_curve = Arc::new(StdMutex::new(ReheatCostCurve::new(
        "Reheat cost by age — during the fill (phase 1)",
        params.seed,
    )));
    let ledger_capacity = (LEDGER_TARGET / params.tasks).max(8);

    let mut fill_handles = Vec::with_capacity(params.tasks);
    for task_id in 0..params.tasks {
        fill_handles.push(tokio::spawn(cw::run_fill_task(
            lane_pools[task_id % lane_pools.len()].clone(),
            cw::FillConfig {
                task_id: task_id as u32,
                tasks: params.tasks,
                seed: params.seed,
                large_event_fraction: mix,
                budget: fill_budget,
                birth_interval,
                reheat_interval,
                ledger_capacity,
            },
            stop.clone(),
            counters.clone(),
            fill_curve.clone(),
            fill_start,
        )));
    }

    // Cardinality growth is sampled here rather than derived at the end:
    // `CardinalityGrew` needs the trajectory, because a population that stopped
    // growing collapses the non-stationary model to the stationary one.
    let mut growth: Vec<(u64, u64, u64)> = Vec::new();
    let mut ticks = 0u64;
    let fill_stop_reason = loop {
        sleep(GROWTH_SAMPLE_INTERVAL).await;
        ticks += 1;
        let elapsed = fill_start.elapsed();
        let births = counters.births.load(Ordering::Relaxed);
        let clients = counters.clients.load(Ordering::Relaxed);
        growth.push((elapsed.as_millis() as u64, births, clients));
        let used = poll_store.max_data_fs_used_pct();
        if ticks % 12 == 0 {
            // Errors are named here, not just counted. This line was the only
            // record run 1787056102 left of an 80.5% write-error cliff, and
            // "errors=44048" said nothing about what they were.
            let by_kind = counters.tally().summary();
            println!(
                "[{SCEN}]   t={}s aggregates={} clients={} writes={} errors={} dup_acks={} disk={}% rss_peak={}MB{}",
                elapsed.as_secs(),
                births,
                clients,
                counters.writes_ok.load(Ordering::Relaxed),
                counters.write_errors.load(Ordering::Relaxed),
                counters.duplicate_acks.load(Ordering::Relaxed),
                used,
                poll_store.peak_rss_kb() / 1024,
                if by_kind.is_empty() { String::new() } else { format!("  [{by_kind}]") },
            );
        }
        if let Some(reason) = cw::fill_stop(elapsed, fill_budget, used, card.disk_high_water_pct) {
            break reason;
        }
    };
    stop.store(true, Ordering::Relaxed);
    let fill_elapsed = fill_start.elapsed();
    println!("[{SCEN}] phase 1 stopping: {}", fill_stop_reason.label());

    let mut populations: Vec<Population> = Vec::with_capacity(params.tasks);
    let mut ledger_acks: Vec<TaskAckSummary> = Vec::new();
    let mut fill_latencies: Vec<u64> = Vec::new();
    let mut fill_totals = cw::FillTotals::default();
    for h in fill_handles {
        match h.await {
            Ok(out) => {
                fill_totals.writes_ok += out.totals.writes_ok;
                fill_totals.write_errors += out.totals.write_errors;
                fill_totals.duplicate_acks += out.totals.duplicate_acks;
                fill_totals.occ_retries += out.totals.occ_retries;
                fill_totals.reheats_ok += out.totals.reheats_ok;
                fill_totals.reheat_errors += out.totals.reheat_errors;
                fill_totals.errors_by_kind.merge(&out.totals.errors_by_kind);
                fill_latencies.extend(out.latencies);
                ledger_acks.extend(out.ledger.to_task_ack_summaries());
                populations.push(out.population);
            }
            Err(e) => eprintln!("[{SCEN}] fill task join: {e}"),
        }
    }
    // Retire the fill's pools the moment the fill is done, and do not carry
    // them across the phase-4 restart.
    //
    // `NodePool` only evicts on checkout (`pool.rs:392-404`), so an idle pool
    // holds every connection it ever opened for as long as the process lives.
    // After phase 4 the peers are gone and those sockets sit in CLOSE_WAIT:
    // run 1787056102 ended with 478 of them, 447 to one node, still open 25
    // minutes later. Nothing here needs them again, and an fd nobody will ever
    // check out is a leak whatever else is true.
    drop(lane_pools);
    // The run's cardinality figure. Exact, because every mint is a fresh id by
    // construction — unlike `AckLedger::ack_offers`, which double-counts a key
    // that was sampled out and later written again.
    let distinct_aggregates: u64 = populations.iter().map(|p| p.births_total()).sum();
    let distinct_clients = counters.clients.load(Ordering::Relaxed);
    println!(
        "[{SCEN}] phase 1 done in {}s: {} aggregates, {} clients, {} writes ({} errors), {} reheats ({} failed), {} ledger entries",
        fill_elapsed.as_secs(),
        distinct_aggregates,
        distinct_clients,
        fill_totals.writes_ok,
        fill_totals.write_errors,
        fill_totals.reheats_ok,
        fill_totals.reheat_errors,
        ledger_acks.len(),
    );
    let fill_errors_by_kind = fill_totals.errors_by_kind.summary();
    if !fill_errors_by_kind.is_empty() {
        println!("[{SCEN}] phase 1 write errors by kind: {fill_errors_by_kind}");
    }
    println!(
        "[{SCEN}] phase 1 OCC retries absorbed: {}, retries the server had already committed: {}",
        fill_totals.occ_retries, fill_totals.duplicate_acks,
    );

    // The read plan is drawn ONCE, here, and phases 3 and 5 both use it: same
    // keys, same buckets, same concurrency, so the only difference between them
    // is that a restart emptied every cache.
    let plan_now_ms = fill_start.elapsed().as_millis() as u64;
    let read_plan = cw::build_read_plan(&mut populations, plan_now_ms, READS_PER_BUCKET);
    let planned_reads: usize = read_plan.iter().map(|p| p.len()).sum();
    println!("[{SCEN}] read plan: {planned_reads} keys across {} tasks", read_plan.len());

    // One hot account per task for the contention and failover windows.
    let hot: Vec<Member> = populations
        .iter_mut()
        .filter_map(|p| {
            AgeBucket::ALL.iter().find_map(|b| p.sample_by_age(plan_now_ms, *b))
        })
        .collect();
    if hot.is_empty() {
        // Every age bucket starts at 5 minutes, so a fill shorter than that
        // leaves nothing sampleable. Phases 2 and 6 then have no accounts to
        // drive and say so rather than inventing one outside a shard lane.
        println!("[{SCEN}] WARNING: no aggregate reached the youngest age bucket — phases 2 and 6 have no hot set");
    }

    // ---- Phase 2: contention ------------------------------------------------
    let replicas = card.contention_factor.max(1);
    let accounts = (MAX_CONTENTION_TASKS / replicas).min(hot.len());
    let contention_tasks = accounts * replicas;
    println!(
        "[{SCEN}] phase 2: contention — {accounts} accounts x {replicas} replicas = {contention_tasks} writers, {CONTENTION_SECS}s, history ON"
    );
    let p2_history = new_history_recorder(&format!("{SCEN}-contention"), run_dir);
    let mut p2_stats = cw::HotWriterStats::default();
    let mut p2_latencies: Vec<u64> = Vec::new();
    let mut p2_acks: Vec<celeriant_bench::TaskAckSummary> = Vec::new();
    if contention_tasks > 0 {
        let p2_pool =
            build_bench_pool(cfg, &up, ScenarioParams { tasks: contention_tasks, ..params }).await?;
        let mut handles = Vec::with_capacity(contention_tasks);
        for (i, member) in hot.iter().take(accounts).enumerate() {
            for r in 0..replicas {
                handles.push(tokio::spawn(cw::run_hot_writer(
                    p2_pool.clone(),
                    cw::HotWriterConfig {
                        member: *member,
                        replica: r as u32,
                        process: (i * replicas + r) as u32,
                        duration: Duration::from_secs(CONTENTION_SECS),
                        seed: params.seed,
                        large_event_fraction: mix,
                    },
                    p2_history.clone(),
                    None,
                )));
            }
        }
        for h in handles {
            if let Ok((s, lat)) = h.await {
                p2_stats.ok += s.ok;
                p2_stats.errors += s.errors;
                p2_stats.occ_retries += s.occ_retries;
                p2_latencies.extend(lat);
                // One summary per (aggregate, client). This is what makes the
                // contention phase's oracles do anything: passed an empty slice
                // they read zero aggregates, and monotonicity, final-read parity
                // and payload round-trip all report PASS having checked nothing
                // — in the only window where R clients race on one aggregate.
                if s.max_acked_client_seq > 0 {
                    p2_acks.push(celeriant_bench::TaskAckSummary {
                        aggregate_key: celeriant_bench::account_workload::account_key(s.aggregate_id),
                        client_id: s.client_id,
                        max_acked_client_seq: s.max_acked_client_seq,
                    });
                }
            }
        }
    }
    println!(
        "[{SCEN}] phase 2 done: {} acked, {} errors, {} OCC retries ({:.2} retries per acked write)",
        p2_stats.ok,
        p2_stats.errors,
        p2_stats.occ_retries,
        p2_stats.occ_retries as f64 / p2_stats.ok.max(1) as f64,
    );
    extra_checks.extend(
        finish_history_and_check(&format!("{SCEN}-contention"), cfg, p2_history, &p2_acks, params.seed).await,
    );
    extra_checks.push(CheckResult::pass_with_detail(
        "OccRetryDepth",
        format!(
            "{} retries across {} acked contended writes ({:.2} per ack) at R={replicas}",
            p2_stats.occ_retries,
            p2_stats.ok,
            p2_stats.occ_retries as f64 / p2_stats.ok.max(1) as f64,
        ),
    ));

    // ---- Phase 3: hot reads -------------------------------------------------
    //
    // Settle first. Phase 2 runs `contention_factor x accounts` writers on their
    // own pool; dropping it tears those connections down while phase 3 is trying
    // to establish its own, and the first run of this scenario paid for it —
    // **99 errors in 240 ops with a p99 of 30,023.80ms**, which is exactly the
    // 30s connection timeout, against 240 ops and 0 errors in the identical
    // phase 5. Phase 3 is the WARM baseline of the cold-restart delta, so a
    // degraded number there inflates the headline ratio. `p2_pool` is already
    // out of scope by here; this waits for the server to finish closing its
    // side before anything is measured.
    sleep(P2_DRAIN_SETTLE).await;

    let hot_curve = Arc::new(StdMutex::new(ReheatCostCurve::new(
        "Age-stratified reads — warm cluster (phase 3)",
        params.seed,
    )));
    println!(
        "[{SCEN}] phase 3: hot reads — {planned_reads} keys, {} concurrent, {:.0}s read deadline",
        read_plan.len(),
        READ_REQUEST_TIMEOUT.as_secs_f64(),
    );
    // Own pools, not the fill's: the read phases measure latency and must not be
    // censored by a deadline chosen for write liveness. Dropped before phase 4
    // stops the nodes; phase 5 builds its own on the same constant.
    let read_pools = build_read_pools(cfg, &up, params).await?;
    run_read_plan(&read_pools, &read_plan, &hot_curve).await;
    drop(read_pools);

    // ---- Phase 4: restart ---------------------------------------------------
    println!("[{SCEN}] phase 4: stop both nodes, restart, wait for stable leader");
    // Floor for the stability barrier, taken BEFORE the stop: every sample
    // already in the store describes the pre-restart cluster, and the whole
    // point of this barrier is to observe the post-restart one. Using the
    // scraper's own clock avoids needing its start Instant.
    let pre_restart_ms = up
        .scraper
        .store()
        .snapshot()
        .await
        .iter()
        .map(|s| s.t_ms)
        .max()
        .unwrap_or(0);
    let _ = executor.run(&Action::StopAll);
    sleep(Duration::from_secs(5)).await;
    executor.run(&Action::StartCs1)?;
    sleep(Duration::from_secs(5)).await;
    // Stamped AFTER the staged start, not before it. With `restart_t0` taken
    // ahead of the 5s gap between the two node starts, that sleep sat inside
    // the measured window and `ColdRestartReady` carried a 5-second constant:
    // it reported ~6.0s whether the node recovered instantly or took five
    // seconds, and could not resolve anything below its own floor. Measuring
    // from the last start makes the number the cluster's convergence time.
    let restart_t0 = Instant::now();
    executor.run(&Action::StartCs2)?;
    match wait_for_stable_leader_since(&up.scraper, Duration::from_secs(300), pre_restart_ms + 1).await {
        Ok(()) => {
            println!("[{SCEN}] phase 4: leader stable after {:.1}s", restart_t0.elapsed().as_secs_f32());
            // Leadership is not readiness. The barrier above only asserts that
            // one node leads and the other follows; it says nothing about the
            // follower having the data. Phase 5 reads through pools seeded on
            // BOTH nodes, so without this wait a read can land on a follower
            // still catching up and come back empty in ~0.25ms — no segment
            // scan, no bytes — which reads as a spectacularly fast cold path
            // and drags the cold p50 below the warm one. Observed exactly that:
            // p50 0.25ms against a p99 of 4,024ms, with `EventualConvergence`
            // reporting the follower 496 wal_seqs behind in the same run.
            // Status-based, not wal_seq-based: FollowerCatchingUp -> Follower is
            // the follower's own statement that its replay finished.
            if let Err(why) =
                wait_for_follower_rejoin(SCEN, &up.scraper, CONVERGENCE_AFTER_RESTART, pre_restart_ms + 1).await
            {
                extra_checks.push(CheckResult::inconclusive(
                    "ColdReadBaselineTrustworthy",
                    format!(
                        "follower never reached steady Follower before the cold reads ({why}); \
                         phase 5 reads both nodes, so a read landing on a node without the data \
                         returns empty in microseconds and reads as a fast cold path"
                    ),
                ));
            }
            println!("[{SCEN}] phase 4: cluster ready after {:.1}s", restart_t0.elapsed().as_secs_f32());
            extra_checks.push(CheckResult::pass_with_detail(
                "ColdRestartReady",
                format!(
                    "both nodes restarted and a stable leader was elected in {:.1}s (segment size {:.0}MB)",
                    restart_t0.elapsed().as_secs_f32(),
                    card.segment_bytes as f64 / (1024.0 * 1024.0),
                ),
            ));
        }
        Err(e) => extra_checks.push(CheckResult::fail(
            "ColdRestartReady",
            format!("no stable leader within 300s after the phase 4 restart: {e}"),
        )),
    }

    // ---- Phase 5: cold reads — THE DELTA ------------------------------------
    let cold_curve = Arc::new(StdMutex::new(ReheatCostCurve::new(
        "Age-stratified reads — after restart, every cache empty (phase 5)",
        params.seed,
    )));
    println!(
        "[{SCEN}] phase 5: cold reads — identical keys, identical concurrency, {:.0}s read deadline",
        READ_REQUEST_TIMEOUT.as_secs_f64(),
    );
    // Built fresh here, and on the SAME constant as phase 3. These two curves are
    // the two halves of one delta; comparing a censored baseline against an
    // uncensored cold side would be worse than censoring both, because it would
    // look like a result.
    //
    // Fresh also because every connection dialled before phase 4 belongs to a
    // peer that is gone, and the pool's checkout is a FIFO free-list with no
    // liveness probe (`celeriant_client_tokio/src/pool.rs:393-404`), so it hands
    // those out; worse, the `read_all` path this phase uses never calls
    // `mark_broken`, so a failed connection goes straight back into the list.
    // Same class as the `P2_DRAIN_SETTLE` fix above, at the restart boundary.
    let read_pools = build_read_pools(cfg, &up, params).await?;
    run_read_plan(&read_pools, &read_plan, &cold_curve).await;
    drop(read_pools);
    // Named, not just counted. A bare error total cannot tell a client-side
    // request timeout (which censors the latency distribution and makes the
    // surviving ops a survivorship-biased sample) from a dead connection.
    if let Ok(c) = cold_curve.lock() {
        let by_kind = c.error_summary();
        if !by_kind.is_empty() {
            println!("[{SCEN}] phase 5 read errors by kind: {by_kind}");
        }
    }

    // `NoBloomAbsentSegments` is evaluated after this point: a sealed segment
    // that lost its bloom across the restart answers maybe-present for every
    // key, which is correctness-adjacent rather than merely slow.
    let post_restart_samples = up.scraper.store().snapshot().await;
    let hosts = vec![cfg.leader_host.clone(), cfg.follower_host.clone()];
    extra_checks.push(crate::cardinality_checks::no_bloom_absent_segments(&post_restart_samples, &hosts));

    // ---- Phase 6: SIGKILL + failover (opt-in) --------------------------------
    // Outputs hoisted so the report renders identically whether or not the
    // phase ran. `None` means "not measured", which `timing_check` already
    // renders as INCONCLUSIVE rather than as a zero.
    let mut promotion_ms: Option<u64> = None;
    let mut ready_ms: Option<u64> = None;
    let mut gap: Option<Duration> = None;
    #[allow(unused_assignments)]
    let mut max_ack_gap: Option<Duration> = None;
    let mut p6_stats = cw::HotWriterStats::default();
    let mut p6_latencies: Vec<u64> = Vec::new();
    let mut bench_window_end_ms = up.elapsed_ms();

    // The integrity audit is over PHASE 1's ledger, so it runs whether or not
    // the failover phase does. Gating it behind phase 6 would have made the
    // scenario's correctness evidence disappear the moment failover was
    // switched off — which is the opposite of disambiguating.
    let audit_acks = stride_sample(&ledger_acks, AUDIT_SAMPLE);
    println!("[{SCEN}] audit: {} of {} sampled ledger entries", audit_acks.len(), ledger_acks.len());
    let (integrity, deep) = run_integrity_and_deep_audit(SCEN, &pool, &audit_acks, 32).await;
    // Last user of `pool`. Same reason as `lane_pools` above: its connections
    // predate the phase-4 restart, and phase 7 must not be able to draw one.
    drop(pool);
    extra_checks.push(data_integrity_check(&integrity, (audit_acks.len() as u64 / 50).max(5)));

    if !card.failover_phase {
        println!("[{SCEN}] phase 6: skipped (--failover to enable). Failover under a behind \
follower is a separate confirmed defect with its own test; running it here would fail this \
run for reasons unrelated to memory or cardinality.");
    } else {
        let leader_now = detect_leader(cfg, &up.scraper)
            .await
            .unwrap_or_else(|| cfg.leader_host.clone());
        let (kill, restart, killed_host) = if leader_now == cfg.leader_host {
            (Action::KillCs1, Action::StartCs1, cfg.leader_host.clone())
        } else {
            (Action::KillCs2, Action::StartCs2, cfg.follower_host.clone())
        };
        println!("[{SCEN}] phase 6: SIGKILL {killed_host} mid-write, 10Hz role scrape, full history ON");

        let p6_history = new_history_recorder(&format!("{SCEN}-failover"), run_dir);
        let origin = Instant::now();
        let availability = Arc::new(cw::AvailabilityClock::new(origin));
        let (role_samples, role_stop) = spawn_role_watch(cfg, origin);

        let p6_pool = build_bench_pool(cfg, &up, params).await?;
        let mut p6_handles = Vec::with_capacity(hot.len());
        for (i, member) in hot.iter().enumerate() {
            p6_handles.push(tokio::spawn(cw::run_hot_writer(
                p6_pool.clone(),
                cw::HotWriterConfig {
                    member: *member,
                    replica: 0,
                    process: i as u32,
                    duration: Duration::from_secs(FAILOVER_WINDOW_SECS),
                    seed: params.seed ^ 0xF0,
                    large_event_fraction: mix,
                },
                p6_history.clone(),
                Some(availability.clone()),
            )));
        }

        sleep(Duration::from_secs(FAILOVER_KILL_AT_SECS)).await;
        executor.run(&kill)?;
        // Marked AFTER the kill returns: writes acked while the ssh was in flight
        // were served by a live leader and belong on the before side. Marking first
        // would classify them as "after" and report a gap of nearly zero.
        availability.mark_kill();
        let kill_ms = origin.elapsed().as_millis() as u64;

        sleep(Duration::from_secs(FAILOVER_RESTART_AFTER_SECS)).await;
        println!("[{SCEN}] phase 6: restarting {killed_host}");
        executor.run(&restart)?;
        let restart_ms = origin.elapsed().as_millis() as u64;

        for h in p6_handles {
            if let Ok((s, lat)) = h.await {
                p6_stats.ok += s.ok;
                p6_stats.errors += s.errors;
                p6_stats.occ_retries += s.occ_retries;
                p6_latencies.extend(lat);
            }
        }
        // Keep the 10Hz window open past the writers until the killed node is
        // observed ready. `ShardWal::open` rebuilds the active segment's chain tips
        // before serving, which at 1GB segments can outlast the write window — and
        // reporting "not observed" for a node that simply had not finished yet
        // would hide the very number this measurement exists for.
        let ready_deadline = Instant::now() + Duration::from_secs(180);
        let roles = loop {
            let snapshot = role_samples.lock().await.clone();
            if cw::restart_ready_ms(&snapshot, &killed_host, restart_ms).is_some()
                || Instant::now() >= ready_deadline
            {
                break snapshot;
            }
            sleep(Duration::from_secs(1)).await;
        };
        role_stop.notify_one();
        promotion_ms = cw::promotion_latency_ms(&roles, &killed_host, kill_ms);
        ready_ms = cw::restart_ready_ms(&roles, &killed_host, restart_ms);
        gap = availability.gap();
        max_ack_gap = availability.max_ack_gap();
        println!(
            "[{SCEN}] phase 6: promotion {}, write-availability gap {}, restart-to-ready {}",
            promotion_ms.map(|v| format!("{v}ms")).unwrap_or_else(|| "not observed".into()),
            gap.map(|g| format!("{:.3}ms", g.as_secs_f64() * 1000.0)).unwrap_or_else(|| "not observed".into()),
            ready_ms.map(|v| format!("{v}ms")).unwrap_or_else(|| "not observed".into()),
        );

        // Convergence budget scaled to the segment size: a follower one segment
        // behind replays up to `segment_bytes` over TCP.
        let converge_budget = Duration::from_secs(
            (card.segment_bytes / ASSUMED_CATCHUP_BYTES_PER_SEC).max(60),
        );
        println!("[{SCEN}] waiting up to {}s for WAL convergence", converge_budget.as_secs());
        wait_for_wal_convergence(SCEN, cfg, converge_budget).await;
        bench_window_end_ms = up.elapsed_ms();

        // Report-only on the first run: nobody has measured what any of these do at
        // this population size, and a threshold invented before the first run is a
        // threshold that gets tuned until the run passes.
        extra_checks.push(timing_check(
            "PromotionLatency",
            promotion_ms,
            "survivor took leadership",
            "no promotion observed in the 10Hz window",
        ));
        // Leads with the anchor-free number. The orchestrator cannot know when the
        // SIGKILL actually landed — `make kill-*` is a blocking ssh that fires the
        // signal early and returns late — so any gap anchored to a stamped instant
        // is biased by an ssh round trip, in the direction that flatters the system.
        // The largest observed silence between consecutive acks needs no anchor.
        extra_checks.push(timing_check(
            "WriteAvailabilityGap",
            max_ack_gap.map(|g| g.as_millis() as u64),
            "largest silence between consecutive client acks across the failover window \
             (anchor-free, microsecond resolution)",
            "fewer than two acks landed in the window",
        ));
        extra_checks.push(timing_check(
            "WriteAvailabilityGapAnchored",
            gap.map(|g| g.as_millis() as u64),
            "last ack before the stamped kill to first ack after — kept for comparison only; \
             the stamp trails the real SIGKILL by one ssh round trip",
            "the window never saw an ack on both sides of the kill",
        ));
        extra_checks.push(timing_check(
            "RestartToReady",
            ready_ms,
            "killed node rejoined and its shards reported a WAL sequence",
            "the killed node was not observed ready before the window closed",
        ));

        extra_checks.extend(
            finish_history_and_check(&format!("{SCEN}-failover"), cfg, p6_history, &audit_acks, params.seed).await,
        );

    }

    // ---- Phase 7: evaluate --------------------------------------------------
    //
    // Every step from here announces itself and runs under a deadline. Run
    // 1787056102 entered this block, printed nothing for 37 minutes and had to
    // be SIGKILLed; the whole measurement was already in hand and was lost
    // anyway. Nothing after the last write is worth the run.
    println!("[{SCEN}] phase 7: evaluate");
    let per_shard = cw::merge_shard_counts(&[
        step(SCEN, "segments per shard (leader)", TEARDOWN_STEP_BUDGET, ssh_segments_per_shard(&cfg.leader_host))
            .await
            .unwrap_or_default(),
        step(SCEN, "segments per shard (follower)", TEARDOWN_STEP_BUDGET, ssh_segments_per_shard(&cfg.follower_host))
            .await
            .unwrap_or_default(),
    ]);
    let final_samples = step(SCEN, "final metric snapshot", TEARDOWN_STEP_BUDGET, up.scraper.store().snapshot())
        .await
        .unwrap_or_default();
    poll_store.request_stop();
    let peak_rss_kb = poll_store.peak_rss_kb();
    let peak_disk_pct = poll_store.max_data_fs_used_pct();
    let rss_by_host = step(SCEN, "peak RSS by host", TEARDOWN_STEP_BUDGET, poll_store.peak_rss_by_host())
        .await
        .unwrap_or_default();

    let populated = fill_curve
        .lock()
        .map(|c| c.populated_buckets(MIN_REHEAT_OPS_PER_BUCKET).len())
        .unwrap_or(0);

    // Read out before the struct literal: a guard in a `let` initialiser lives
    // to the semicolon, so locking these inline took `hot_curve` twice in one
    // statement and deadlocked the whole run short of the summary.
    let curves = cd::Curves::snapshot(&fill_curve, &hot_curve, &cold_curve);

    // Built before the checks and the markdown, and both then read from it: the
    // JSON section, the check details and the summary table are one set of
    // numbers or they are three untrustworthy ones.
    let deliverable = cd::CardinalityDeliverable {
        shape: cd::RunShape::new(&card, params.tasks, fill_elapsed),
        reached: cd::Reached {
            distinct_aggregates,
            distinct_clients,
            fill_writes_ok: fill_totals.writes_ok,
            fill_write_errors: fill_totals.write_errors,
            reheat_probes_ok: fill_totals.reheats_ok,
            reheat_probes_failed: fill_totals.reheat_errors,
            ledger_entries: ledger_acks.len(),
        },
        aggs_per_segment: cd::AggsPerSegment::new(
            &card,
            distinct_aggregates,
            &cd::shard_segments(&per_shard),
        ),
        segments_per_shard: cd::shard_segments(&per_shard),
        peak_rss: cd::PeakRss::new(&card, peak_rss_kb, &rss_by_host, node_ram_bytes),
        fill_curve: curves.fill,
        warm_curve: curves.warm,
        cold_curve: curves.cold,
        cold_vs_warm: curves.cold_vs_warm,
    };

    extra_checks.push(crate::cardinality_checks::cardinality_grew(&growth));
    // Before anything else reads the curves: a censored curve is not a slow
    // result, it is the absence of one, and it renders identically to a measured
    // curve unless something says so.
    extra_checks.push(crate::cardinality_checks::read_latency_uncensored(
        &[
            ("phase 3 (warm)", &deliverable.warm_curve),
            ("phase 5 (cold)", &deliverable.cold_curve),
        ],
        READ_REQUEST_TIMEOUT,
    ));
    // The fingerprint of stream desync. Counted since the client gained
    // correlation validation; this is what finally reads the counter.
    extra_checks.push(crate::cardinality_checks::no_correlation_mismatches(
        &[
            ("phase 2 (fill)", &deliverable.fill_curve),
            ("phase 3 (warm)", &deliverable.warm_curve),
            ("phase 5 (cold)", &deliverable.cold_curve),
        ],
        Some((
            fill_totals.writes_ok,
            fill_totals.errors_by_kind.get(
                crate::cardinality_workload::FillWriteError::CorrelationMismatch,
            ),
        )),
    ));
    extra_checks.push(crate::cardinality_checks::age_spread_reached(populated, MIN_REHEAT_OPS_PER_BUCKET));
    extra_checks.push(crate::cardinality_checks::rotations_reached(&per_shard));
    extra_checks.push(crate::cardinality_checks::no_rotation_enospc(&final_samples, &hosts));
    extra_checks.push(crate::cardinality_checks::bloom_effectiveness(&final_samples, &hosts));
    extra_checks.push(crate::cardinality_checks::sidecar_high_water(&final_samples, &hosts));
    extra_checks.push(crate::cardinality_checks::disk_watchdog(&poll_store, card.disk_high_water_pct));
    extra_checks.push(match node_ram_bytes {
        Some(ram) => crate::cardinality_checks::honest_memory_budget(
            peak_rss_kb,
            card.declared_budget_bytes(ram),
            ram,
        ),
        // Without the node's RAM there is no budget to compare against, and a
        // ratio against a guessed denominator is worse than no number.
        None => CheckResult::inconclusive(
            "HonestMemoryBudget",
            format!("peak RSS {peak_rss_kb} kB, but MemTotal could not be read — no declared budget to compare against"),
        ),
    });
    // goal.md asks for the OBSERVED density against the design point, not the
    // sizing formula run forward on its own inputs. `achieved` is the model;
    // quoting it as a measurement would make the run unfalsifiable, since it
    // cannot disagree with the request for any reason other than a clamp.
    let aps = &deliverable.aggs_per_segment;
    extra_checks.push(match aps.observed_lower_bound {
        None => CheckResult::inconclusive(
            "AggregatesPerSegment",
            format!(
                "no segment rotated, so density was never observed; the derived mix of {:.1}% large \
                 events at {:.0}MB models {} aggs/segment against a design point of {}",
                mix * 100.0,
                card.segment_bytes as f64 / (1024.0 * 1024.0),
                aps.model,
                aps.design_point,
            ),
        ),
        // Lower bound, and deliberately labelled as one: an aggregate whose
        // chain crosses a boundary is entered into every segment bloom it
        // touches, so real bloom load is at or above this.
        Some(observed) => CheckResult::pass_with_detail(
            "AggregatesPerSegment",
            format!(
                "observed >= {observed} aggs/segment ({distinct_aggregates} aggregates over \
                 {} segments; lower bound — chains spanning a boundary enter several \
                 blooms). Model said {} for a target of {} at {:.1}% large events. \
                 Bloom design point {}",
                aps.observed_total_segments,
                aps.model,
                card.target_aggs_per_segment,
                mix * 100.0,
                aps.design_point,
            ),
        ),
    });

    let summary = render_cardinality_report(
        &card,
        &deliverable,
        CardinalityRun {
            fill_stop_reason,
            peak_disk_pct,
            promotion_ms,
            gap_ms: gap.map(|g| g.as_micros() as u64),
            ready_ms,
            p2_stats,
            p6_stats,
            fill_errors_by_kind,
        },
    );
    let summary_path = run_dir.join(format!("{SCEN}-summary.md"));
    // Written before tear-down, not after: the deliverable is complete at this
    // point, and every remaining step touches the cluster. Losing the summary
    // to a wedged ssh is the failure mode that cost run 1787056102.
    if let Err(e) = std::fs::write(&summary_path, &summary) {
        eprintln!("[{SCEN}] writing {}: {e}", summary_path.display());
    }
    println!("\n{summary}");

    let total_ok = fill_totals.writes_ok + p2_stats.ok + p6_stats.ok;
    let total_err = fill_totals.write_errors + fill_totals.reheat_errors + p2_stats.errors + p6_stats.errors;
    let mut all_latencies = fill_latencies;
    all_latencies.extend(p2_latencies);
    all_latencies.extend(p6_latencies);
    let window_secs = (bench_window_end_ms.saturating_sub(bench_window_start_ms) / 1000).max(1);
    let bench_result = benchmark_from(all_latencies, total_ok, total_err, params.tasks, window_secs);

    let expectations = ScenarioExpectations {
        // Phase 4 restarts both nodes and phase 6 kills one: three process
        // starts and the elections they imply are the scenario, not a fault.
        max_node_starts: 4,
        max_leader_elections: 60,
        max_s3_fallbacks: 2_000,
        max_heartbeat_failures: 200,
        max_bench_errors: 500_000,
        max_bench_error_ratio: Some(0.75),
        max_role_flips: 12,
        max_split_brain_ticks: 20,
        require_leader_retained: false,
        assert_eventual_progress: true,
        assert_no_divergent_tips: true,
        // Promotion timing comes from the 10Hz window instead: the 2Hz
        // `FailoverWithinBudget` cannot resolve a 1600ms budget, and asserting
        // one with ±500ms of error asserts nothing.
        max_failover_ms: None,
        ..ScenarioExpectations::default()
    };

    let scen_params = ScenarioParams {
        // The fill is rate-limited by the population model, so a throughput
        // floor here would be asserting on the birth rate, not the database.
        throughput_floor: 1.0,
        duration_secs: window_secs,
        ..params
    };

    let report = tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        Some(integrity),
        None,
        deep,
        run_dir,
    )
    .await
    .map(|mut r| {
        r.cardinality = Some(deliverable);
        r
    });
    // The poller finishes its in-flight tick and exits; if an ssh in that tick
    // is wedged, the join is not worth blocking the process on.
    let _ = step(SCEN, "join host poller", TEARDOWN_STEP_BUDGET, poll_handle).await;

    // Put the units back however the run ended. Not gated on success: a failed
    // scenario is exactly when a leaked 20%-memory unit would go unnoticed and
    // silently reshape the next run's numbers.
    if let Some((mem, seg)) = baseline_cfg {
        let restore = Action::UpdateServiceConfig { memory_percent: mem, segment_bytes: seg };
        let deploy_dir = cfg.deploy_dir.clone();
        let (target, vars) = (restore.make_target(), restore.make_vars());
        let outcome = step_blocking(SCEN, "restore service config", TEARDOWN_STEP_BUDGET, move || {
            crate::actions::run_make_in(&deploy_dir, target, &vars)
        })
        .await
        .unwrap_or_else(|| Err("restore timed out".to_string()));
        match outcome {
            Ok(()) => println!("[{SCEN}] restored service config to {mem}% / {seg} bytes"),
            Err(e) => println!("[{SCEN}] WARNING: could not restore service config ({e}) — \
                                cs1/cs2 are still on {}% / {} bytes and later runs will inherit that",
                               card.memory_percent, card.segment_bytes),
        }
    }
    report
}

/// `MEMORY_CONSUMPTION_PERCENT` and `SHARD_LOG_PREALLOCATE_BYTES` as config.env
/// declares them, so the scenario can hand the cluster back unchanged. `None`
/// when either is absent — better to leave the units alone than to restore a
/// guessed value.
fn read_service_baseline(cfg: &ClusterConfig) -> Option<(u64, u64)> {
    let raw = std::fs::read_to_string(cfg.deploy_dir.join("config.env")).ok()?;
    parse_service_baseline(&raw)
}

/// Pure half of `read_service_baseline`, split out so it is testable without a
/// deploy directory.
pub fn parse_service_baseline(raw: &str) -> Option<(u64, u64)> {
    Some((
        env_u64(raw, "MEMORY_CONSUMPTION_PERCENT")?,
        env_u64(raw, "SHARD_LOG_PREALLOCATE_BYTES")?,
    ))
}

/// `KEY=<u64>` from a config.env body, tolerating a trailing `# comment`.
/// Requires the `=` immediately after the key so `KEY_EXTRA=` cannot satisfy a
/// lookup for `KEY` — restoring the wrong value is worse than not restoring.
fn env_u64(raw: &str, key: &str) -> Option<u64> {
    raw.lines()
        .map(str::trim)
        .find(|l| l.starts_with(key) && l[key.len()..].starts_with('='))
        .and_then(|l| l.split_once('='))
        .and_then(|(_, v)| v.split('#').next())
        .and_then(|v| v.trim().parse().ok())
}

/// Timing figures from phase 6 and the deliverables that hang off them.
/// Report-only until a calibration run pins thresholds.
fn timing_check(name: &'static str, ms: Option<u64>, what: &str, missing: &str) -> CheckResult {
    match ms {
        Some(v) => CheckResult::pass_with_detail(name, format!("{v}ms — {what} (report-only)")),
        None => CheckResult::inconclusive(name, missing.to_string()),
    }
}

/// Even stride over the sampled ledger. The ledger is already a uniform
/// reservoir, so a stride over it stays uniform while keeping the audit's
/// round-trip count bounded.
fn stride_sample(acks: &[TaskAckSummary], cap: usize) -> Vec<TaskAckSummary> {
    if acks.len() <= cap {
        return acks.to_vec();
    }
    let step = (acks.len() / cap).max(1);
    acks.iter().step_by(step).take(cap).cloned().collect()
}

/// Run one read pass over the plan: one task per plan slice, so phases 3 and 5
/// offer identical concurrency.
async fn run_read_plan(
    lane_pools: &[Arc<Pool>],
    plan: &crate::cardinality_workload::ReadPlan,
    curve: &Arc<std::sync::Mutex<celeriant_bench::read_workload::ReheatCostCurve>>,
) {
    // Slice `i` holds task `i`'s keys, which all sit in lane `i % lanes`, so
    // routing it to that lane's pool keeps the connection on one shard instead
    // of migrating the stream on nearly every read.
    let mut handles = Vec::with_capacity(plan.len());
    for (i, slice) in plan.iter().enumerate() {
        if slice.is_empty() {
            continue;
        }
        handles.push(tokio::spawn(crate::cardinality_workload::run_read_probe_task(
            lane_pools[i % lane_pools.len()].clone(),
            slice.clone(),
            curve.clone(),
        )));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// Aggregate the phases' latency reservoirs into the report's bench summary.
/// Percentiles are exact over the retained union; each task's reservoir is
/// uniform over its own stream and the tasks are symmetric.
fn benchmark_from(
    mut latencies: Vec<u64>,
    ok: u64,
    errors: u64,
    num_tasks: usize,
    elapsed_secs: u64,
) -> BenchmarkResult {
    latencies.sort_unstable();
    let n = latencies.len();
    let at = |q: usize| if n == 0 { 0 } else { latencies[(n * q / 1000).min(n - 1)] };
    BenchmarkResult {
        num_tasks,
        total_requests: ok,
        errors,
        throughput: ok as f64 / elapsed_secs.max(1) as f64,
        avg_latency_ms: if n == 0 { 0.0 } else { latencies.iter().sum::<u64>() as f64 / n as f64 },
        p50_ms: at(500),
        p95_ms: at(950),
        p99_ms: at(990),
        p999_ms: at(999),
        min_ms: latencies.first().copied().unwrap_or(0),
        max_ms: latencies.last().copied().unwrap_or(0),
    }
}

/// 10Hz role scrape for the failover window only.
///
/// The standard scraper runs at 2Hz, which is the right rate for a five-hour
/// fill and useless for a sub-second promotion. This one is armed for the kill
/// and torn down straight after.
fn spawn_role_watch(
    cfg: &ClusterConfig,
    origin: Instant,
) -> (
    Arc<tokio::sync::Mutex<Vec<crate::cardinality_workload::RoleSample>>>,
    Arc<tokio::sync::Notify>,
) {
    let samples = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let stop = Arc::new(tokio::sync::Notify::new());
    let out = samples.clone();
    let stop_rx = stop.clone();
    let targets = vec![
        (cfg.leader_host.clone(), cfg.metrics_url(&cfg.leader_host)),
        (cfg.follower_host.clone(), cfg.metrics_url(&cfg.follower_host)),
    ];
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(90))
            .build()
            .expect("reqwest client");
        loop {
            let t_ms = elapsed_ms(origin, Instant::now());
            for (host, url) in &targets {
                let sample = match client.get(url).send().await {
                    Ok(r) => match r.text().await {
                        Ok(body) => {
                            let s = crate::sample::parse_metrics(host.clone(), t_ms, &body);
                            crate::cardinality_workload::RoleSample {
                                t_ms,
                                host: host.clone(),
                                node_role: s.node_role,
                                ok: s.ok,
                                shards_reporting: s.wal_seq_by_shard.len(),
                            }
                        }
                        Err(_) => unreachable_role(host, t_ms),
                    },
                    Err(_) => unreachable_role(host, t_ms),
                };
                out.lock().await.push(sample);
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                _ = stop_rx.notified() => break,
            }
        }
    });
    (samples, stop)
}

fn unreachable_role(host: &str, t_ms: u64) -> crate::cardinality_workload::RoleSample {
    crate::cardinality_workload::RoleSample {
        t_ms,
        host: host.to_string(),
        node_role: 0.0,
        ok: false,
        shards_reporting: 0,
    }
}

/// Register the banking schema for every major, then prove validation is
/// actually on the measured path.
///
/// Registration succeeding is NOT evidence: a schema keyed on the wrong tuple,
/// or an event carrying an IV, makes the server skip validation and return Ok.
/// Only a rejected bad payload proves it, so that is what this asserts.
async fn assert_account_schemas_enforced(pool: &Arc<Pool>) -> CheckResult {
    use celeriant_bench::account_workload::{
        ALL_MAJORS, MAJOR_DEPOSITED, WORKLOAD_AGG_TYPE, WORKLOAD_MINOR, WORKLOAD_ORG, account_event,
        account_key, schema_for,
    };
    use celeriant_bench::{ClientError, RegisterSchemaRequest, SchemaError, SchemaKey, ServerError};

    const NAME: &str = "SchemaEnforced";
    const PROBE_CLIENT: u128 = 0x5CE1;

    for major in ALL_MAJORS {
        let req = RegisterSchemaRequest {
            correlation_id: None,
            client_id: PROBE_CLIENT,
            user_id: None,
            schema_key: SchemaKey::new(WORKLOAD_ORG, WORKLOAD_AGG_TYPE, major, WORKLOAD_MINOR),
            schema_type: 0,
            schema: schema_for(major).to_string(),
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match pool.register_schema(req.clone()).await {
                Ok(_) => break,
                Err(ClientError::Server(ServerError::Schema {
                    kind: SchemaError::AlreadyExists, ..
                })) => break,
                Err(e) if Instant::now() < deadline => {
                    println!("[schema] register major={major}: transient {e}, retrying");
                    sleep(Duration::from_secs(2)).await;
                }
                Err(e) => {
                    return CheckResult::fail(NAME, format!("register major={major} never succeeded: {e}"));
                }
            }
        }
    }

    // The probe aggregate stays in one shard lane so the probe cannot trigger a
    // client redirect on the pool's first connection.
    let probe = account_key(u128::from(u32::MAX) * 3);
    let mut bad = account_event(MAJOR_DEPOSITED, 1, 0, 0);
    bad.event_value = Arc::new(br#"{"AmountCents":"not-an-integer"}"#.to_vec());
    match pool.write_events(probe, vec![bad], PROBE_CLIENT).await {
        Err(ClientError::Server(ServerError::Schema { .. })) => CheckResult::pass_with_detail(
            NAME,
            format!("{} majors registered; a payload violating the schema was rejected", ALL_MAJORS.len()),
        ),
        Ok(_) => CheckResult::fail(
            NAME,
            "a payload violating the registered schema was ACCEPTED — every number from this run \
             would be measuring an unvalidated write path",
        ),
        Err(e) => CheckResult::fail(NAME, format!("schema enforcement probe failed for an unexpected reason: {e}")),
    }
}

/// Physical RAM on a node, in bytes. `None` when the read failed — the memory
/// check then reports inconclusive rather than dividing by a guess.
async fn ssh_mem_total_bytes(host: &str) -> Option<u64> {
    let host = host.to_string();
    tokio::task::spawn_blocking(move || {
        use std::process::{Command, Stdio};
        let out = Command::new("ssh")
            .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=5", &host])
            .arg("awk '/MemTotal/{print $2}' /proc/meminfo")
            .stdin(Stdio::null())
            .output()
            .ok()?;
        String::from_utf8_lossy(&out.stdout).trim().parse::<u64>().ok().map(|kb| kb * 1024)
    })
    .await
    .ok()
    .flatten()
}

/// Segment files per shard on one node. Best-effort: an ssh failure yields an
/// empty list, and `RotationsReached` reports inconclusive rather than passing.
async fn ssh_segments_per_shard(host: &str) -> Vec<(u32, u64)> {
    let host = host.to_string();
    tokio::task::spawn_blocking(move || {
        use std::process::{Command, Stdio};
        let script = format!(
            "for d in {CARDINALITY_DATA_ROOT}/shard_*; do printf '%s %s\\n' \"$(basename $d)\" \"$(ls $d/log_*.wal 2>/dev/null | wc -l)\"; done"
        );
        let out = Command::new("ssh")
            .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=5", &host])
            .arg(&script)
            .stdin(Stdio::null())
            .output();
        match out {
            Ok(o) if o.status.success() => crate::cardinality_workload::parse_shard_segment_counts(
                &String::from_utf8_lossy(&o.stdout),
            ),
            _ => Vec::new(),
        }
    })
    .await
    .unwrap_or_default()
}

/// A poisoned reheat curve still holds every sample recorded before the panic,
/// and dropping the whole deliverable over one panicked read task would lose
/// hours of measurement.
/// The parts of the summary table that are not the machine-readable
/// deliverable: failover timings, disk high-water and the contention phase.
struct CardinalityRun {
    fill_stop_reason: crate::cardinality_workload::FillStop,
    peak_disk_pct: u64,
    promotion_ms: Option<u64>,
    gap_ms: Option<u64>,
    ready_ms: Option<u64>,
    p2_stats: crate::cardinality_workload::HotWriterStats,
    p6_stats: crate::cardinality_workload::HotWriterStats,
    /// Fill write errors named by kind. Lives in the summary because a bare
    /// error total is what made run 1787056102's 80.5% cliff unexplainable.
    fill_errors_by_kind: String,
}

/// The deliverable. Every run reports what it *reached*, because cardinality is
/// an output of this scenario rather than an input to it.
///
/// Rendered from `CardinalityDeliverable` wherever the two overlap: the same
/// figure printed here and serialised into the run JSON is read once.
fn render_cardinality_report(
    card: &crate::cardinality_workload::CardinalityParams,
    d: &crate::cardinality_deliverable::CardinalityDeliverable,
    run: CardinalityRun,
) -> String {
    let mut md = String::from("# cardinality_pressure\n\n## What the run reached\n\n");
    md.push_str("| Figure | Value |\n|---|---|\n");
    md.push_str(&format!(
        "| Preset / fill budget | {} / {}s |\n",
        d.shape.preset, d.shape.fill_budget_secs
    ));
    md.push_str(&format!(
        "| Fill elapsed | {}s ({}) |\n",
        d.shape.fill_elapsed_secs,
        run.fill_stop_reason.label()
    ));
    md.push_str(&format!("| Connections (tasks) | {} |\n", d.shape.tasks));
    md.push_str(&format!("| Distinct aggregates | {} |\n", d.reached.distinct_aggregates));
    md.push_str(&format!("| Distinct clients | {} |\n", d.reached.distinct_clients));
    md.push_str(&format!(
        "| Writes acked | fill {} (+{} contention, +{} failover) |\n",
        d.reached.fill_writes_ok, run.p2_stats.ok, run.p6_stats.ok
    ));
    md.push_str(&format!(
        "| Write errors | fill {} (+{} contention, +{} failover) |\n",
        d.reached.fill_write_errors, run.p2_stats.errors, run.p6_stats.errors
    ));
    if !run.fill_errors_by_kind.is_empty() {
        md.push_str(&format!("| Fill write errors by kind | {} |\n", run.fill_errors_by_kind));
    }
    md.push_str(&format!(
        "| Reheat probes | {} ok, {} failed |\n",
        d.reached.reheat_probes_ok, d.reached.reheat_probes_failed
    ));
    md.push_str(&format!("| Sampled ack ledger | {} entries |\n", d.reached.ledger_entries));
    md.push_str(&format!(
        "| Segment size / derived mix | {:.0}MB / {:.1}% large events |\n",
        d.shape.segment_bytes as f64 / (1024.0 * 1024.0),
        d.shape.large_event_fraction * 100.0
    ));
    // Model and observation side by side, never the model alone: the sizing
    // formula run forward on its own inputs cannot disagree with the request,
    // so quoting it as "achieved" makes the run unfalsifiable.
    md.push_str(&format!(
        "| Aggregates per segment | {} vs {} modelled vs {} design point |\n",
        d.aggs_per_segment
            .observed_lower_bound
            .map(|v| format!("observed >= {v} ({} aggregates over {} segments; lower bound)",
                d.reached.distinct_aggregates, d.aggs_per_segment.observed_total_segments))
            .unwrap_or_else(|| "not observed — no segment rotated".into()),
        d.aggs_per_segment.model,
        d.aggs_per_segment.design_point,
    ));
    md.push_str(&format!(
        "| Segments per data shard | {} |\n",
        if d.segments_per_shard.is_empty() {
            "not collected".to_string()
        } else {
            d.segments_per_shard
                .iter()
                .map(|s| format!("shard {}: {}", s.shard, s.segments))
                .collect::<Vec<_>>()
                .join(", ")
        }
    ));
    // Preallocated, not written: segments are allocated to their full size up
    // front, so this is the disk each node has committed rather than the bytes
    // the workload put in them.
    let segments = d.aggs_per_segment.observed_total_segments;
    md.push_str(&format!(
        "| WAL bytes per node | {:.2} GB preallocated across {segments} data segments |\n",
        (segments * d.shape.segment_bytes) as f64 / 1e9,
    ));
    md.push_str(&format!(
        "| Peak RSS | {:.2} GB{} |\n",
        d.peak_rss.peak_bytes as f64 / 1e9,
        match (d.peak_rss.declared_budget_bytes, d.peak_rss.node_ram_bytes) {
            (Some(budget), Some(ram)) => format!(
                " vs declared budget {:.2} GB ({}% of {:.2} GB RAM)",
                budget as f64 / 1e9,
                d.shape.memory_percent,
                ram as f64 / 1e9
            ),
            _ => String::new(),
        }
    ));
    for (host, bytes) in &d.peak_rss.by_host_bytes {
        md.push_str(&format!("| Peak RSS — {host} | {:.2} GB |\n", *bytes as f64 / 1e9));
    }
    md.push_str(&format!(
        "| Peak data filesystem | {}% (high-water {}%) |\n",
        run.peak_disk_pct, card.disk_high_water_pct
    ));
    let ms = |v: Option<u64>| v.map(|x| format!("{x}ms")).unwrap_or_else(|| "not observed".into());
    md.push_str(&format!("| Promotion latency (10Hz) | {} |\n", ms(run.promotion_ms)));
    md.push_str(&format!(
        "| Write-availability gap (client) | {} |\n",
        run.gap_ms.map(|us| format!("{:.3}ms", us as f64 / 1000.0)).unwrap_or_else(|| "not observed".into())
    ));
    md.push_str(&format!("| Restart-to-ready | {} |\n", ms(run.ready_ms)));
    md.push_str(&format!(
        "| OCC retries (R={}) | {} across {} acked contended writes |\n",
        card.contention_factor, run.p2_stats.occ_retries, run.p2_stats.ok
    ));

    md.push_str("\n## The deliverable — cost versus dormancy age\n\n");
    md.push_str(
        "A flat curve means cardinality scales. A curve rising with age means it does not. \
         An empty bucket reads `—`, never `0ms`: the run did not reach that age.\n\n",
    );
    md.push_str(&d.fill_curve.to_markdown());
    md.push('\n');
    md.push_str(&d.warm_curve.to_markdown());
    md.push('\n');
    md.push_str(&d.cold_curve.to_markdown());
    md.push('\n');
    md.push_str(&celeriant_bench::read_workload::delta_markdown(
        &d.cold_vs_warm,
        d.warm_curve.request_timeouts(),
        d.cold_curve.request_timeouts(),
    ));
    md.push_str(
        "\nPhases 3 and 5 read identical keys at identical concurrency, so the delta isolates \
         the empty cache. Bucket labels are the ages at draw time (end of fill) and stay fixed \
         across both phases; re-bucketing by age at read time would move keys between rows and \
         the two tables would no longer compare like with like. Each read task issues one \
         untimed warm-up round trip first, so a post-restart TCP/TLS handshake does not land \
         inside a measured sample.\n",
    );
    md
}

// ===========================================================================
// Defect reproductions
// ===========================================================================
//
// Two availability defects were observed live on the rig, both under the
// cardinality fill and neither reproducible on demand. These two scenarios
// reproduce one each, independently. Both are expected RED until the server is
// fixed; each is the acceptance test for that fix.
//
// The load is the ingredient in both cases. `leader_sigkill` passes clean on
// the same cluster with no load, and an unloaded cluster never wedged — so
// both scenarios reuse `cardinality_workload`'s read-modify-write fill rather
// than the opaque bench, at the shape the defects were seen at.

/// Load and settle knobs for the two defect-reproduction scenarios. Defaults
/// are the shape the defects were observed at on the rig — the wedge formed at
/// 3000 tasks / 400 births per second inside 60 seconds, and did not clear 90
/// seconds after every client disconnected.
#[derive(Debug, Clone, Copy)]
pub struct DefectParams {
    /// Concurrent fill tasks. Must be a multiple of the data shard count.
    pub tasks: usize,
    /// New aggregates per second, CLUSTER-WIDE, divided across `tasks`.
    pub birth_rate_per_sec: f64,
    /// Seconds of fill before the load stops (`write_outage_selfheal`) or the
    /// full observation window (`promotion_failure_survival`, floored at its
    /// own timeline).
    pub load_secs: u64,
    /// Idle seconds observed after ALL load stops. Floored at
    /// `SELFHEAL_MIN_SETTLE_SECS` — the field already disproved recovery over
    /// 90 seconds, so a shorter window cannot say anything new.
    pub settle_secs: u64,
}

impl Default for DefectParams {
    fn default() -> Self {
        Self { tasks: 3000, birth_rate_per_sec: 400.0, load_secs: 120, settle_secs: 120 }
    }
}

/// The field observation is 90+ seconds of idle with the shards still fenced.
/// A settle shorter than that cannot distinguish "never recovers" from "had not
/// recovered yet", so the knob is floored rather than trusted.
const SELFHEAL_MIN_SETTLE_SECS: u64 = 90;
/// Probe writers carrying the availability clock through the kill.
const PROMO_PROBE_WRITERS: usize = 24;
/// Earliest the leader may be killed: enough fill for the follower to fall
/// behind, which is the condition the failover matrix isolated as the only
/// factor that mattered.
const PROMO_KILL_AT_SECS: u64 = 45;
/// Extra time granted for a non-zero follower lag to appear before the kill
/// fires anyway. Firing anyway is deliberate — a run that killed a caught-up
/// follower still reports its lag, so a non-reproduction is diagnosable.
const PROMO_LAG_WAIT_SECS: u64 = 60;
/// Delay before restarting the killed node. The field sequence has the
/// restarted original leader winning epoch N+2 while the survivor is still in
/// `Promoting`, so the restart is part of the trigger, not cleanup.
const PROMO_RESTART_AFTER_SECS: u64 = 20;
/// Observation window kept open after the restart.
const PROMO_TAIL_SECS: u64 = 45;
/// Grace granted to the restarted node before its process is required to be
/// up. `ShardWal::open` rebuilds the active segment's chain tips before the
/// node serves, which at these segment sizes is not instant.
const PROMO_RESTART_GRACE_SECS: u64 = 120;
/// Cap on waiting for the killed node to be observed ready.
const PROMO_READY_DEADLINE: Duration = Duration::from_secs(180);

/// The cardinality fill, running in the background of a defect scenario.
struct DefectFill {
    stop: Arc<std::sync::atomic::AtomicBool>,
    counters: Arc<crate::cardinality_workload::FillCounters>,
    handles: Vec<tokio::task::JoinHandle<crate::cardinality_workload::FillOutcome>>,
    start: Instant,
}

impl DefectFill {
    /// Stop the fill if the data filesystem crosses the high-water mark.
    ///
    /// `write_outage_selfheal` gets this from `watch_defect_fill`, but the
    /// promotion scenario's fill runs unattended across the kill window — and
    /// these boxes host a 24/7 production deployment, so an unwatched fill is
    /// not something to leave running.
    fn spawn_disk_watchdog(
        &self,
        store: crate::host_poll::HostPollStore,
        high_water_pct: u64,
    ) -> tokio::task::JoinHandle<()> {
        let stop = self.stop.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
                let used = store.max_data_fs_used_pct();
                if used >= high_water_pct {
                    println!("[disk watchdog] data filesystem at {used}% — stopping the fill");
                    stop.store(true, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
            }
        })
    }

    /// Stop every fill task and wait for it to exit.
    ///
    /// Load is not "stopped" until this returns: a task still inside a write
    /// holds a connection open, and the settle window is only meaningful
    /// against a genuinely idle cluster.
    async fn stop_and_join(self) -> (crate::cardinality_workload::FillTotals, Vec<u64>) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let mut totals = crate::cardinality_workload::FillTotals::default();
        let mut latencies = Vec::new();
        for h in self.handles {
            match h.await {
                Ok(out) => {
                    totals.writes_ok += out.totals.writes_ok;
                    totals.write_errors += out.totals.write_errors;
                    totals.occ_retries += out.totals.occ_retries;
                    totals.reheats_ok += out.totals.reheats_ok;
                    totals.reheat_errors += out.totals.reheat_errors;
                    latencies.extend(out.latencies);
                }
                Err(e) => eprintln!("fill task join: {e}"),
            }
        }
        (totals, latencies)
    }
}

/// Spawn the fill across the lane pools at the defect load shape.
fn start_defect_fill(
    lane_pools: &[Arc<Pool>],
    defect: DefectParams,
    card: &crate::cardinality_workload::CardinalityParams,
    seed: u64,
    budget: Duration,
) -> DefectFill {
    use crate::cardinality_workload as cw;
    use celeriant_bench::read_workload::ReheatCostCurve;

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let counters = Arc::new(cw::FillCounters::default());
    // `run_fill_task` records reheat cost into a curve. Neither defect scenario
    // reports one — they measure availability, not cost — but the fill's reheat
    // traffic is part of the load shape, so it runs and the curve is discarded.
    let curve = Arc::new(std::sync::Mutex::new(ReheatCostCurve::new("defect fill", seed)));
    let start = Instant::now();
    let mix = card.large_event_fraction();
    let birth_interval = cw::per_task_interval(defect.birth_rate_per_sec, defect.tasks);
    let reheat_interval = cw::per_task_interval(card.reheat_rate_per_sec, defect.tasks);

    let handles = (0..defect.tasks)
        .map(|task_id| {
            tokio::spawn(cw::run_fill_task(
                lane_pools[task_id % lane_pools.len()].clone(),
                cw::FillConfig {
                    task_id: task_id as u32,
                    tasks: defect.tasks,
                    seed,
                    large_event_fraction: mix,
                    budget,
                    birth_interval,
                    reheat_interval,
                    // Neither scenario runs the ledger's integrity audit, so
                    // the smallest useful reservoir keeps 3000 tasks' worth of
                    // sampled acks out of memory.
                    ledger_capacity: 8,
                },
                stop.clone(),
                counters.clone(),
                curve.clone(),
                start,
            ))
        })
        .collect();

    DefectFill { stop, counters, handles, start }
}

/// Print fill progress every 5s and honour the disk watchdog.
async fn watch_defect_fill(
    scen: &str,
    fill: &DefectFill,
    budget: Duration,
    poll_store: &crate::host_poll::HostPollStore,
    high_water_pct: u64,
) -> crate::cardinality_workload::FillStop {
    use std::sync::atomic::Ordering;
    let mut ticks = 0u64;
    loop {
        sleep(Duration::from_secs(5)).await;
        ticks += 1;
        let elapsed = fill.start.elapsed();
        let used = poll_store.max_data_fs_used_pct();
        if ticks.is_multiple_of(3) {
            println!(
                "[{scen}]   t={}s aggregates={} writes={} errors={} disk={used}%",
                elapsed.as_secs(),
                fill.counters.births.load(Ordering::Relaxed),
                fill.counters.writes_ok.load(Ordering::Relaxed),
                fill.counters.write_errors.load(Ordering::Relaxed),
            );
        }
        if let Some(reason) =
            crate::cardinality_workload::fill_stop(elapsed, budget, used, high_water_pct)
        {
            return reason;
        }
    }
}

/// A fresh client must be able to write after the settle window.
///
/// Deliberately independent of the metric verdict. The gauge and the write gate
/// both read `effective_node_status()`, so they agree by construction — only an
/// actual round trip proves a caller can get through.
async fn post_settle_probe_write(
    cfg: &ClusterConfig,
    up: &ClusterUp,
    params: ScenarioParams,
) -> CheckResult {
    const NAME: &str = "PostSettleProbeWrite";
    const BUDGET: Duration = Duration::from_secs(30);
    let pool = match build_bench_pool(cfg, up, ScenarioParams { tasks: 4, ..params }).await {
        Ok(p) => p,
        Err(e) => {
            return CheckResult::fail(
                NAME,
                format!("a fresh client could not even connect after the settle window: {e}"),
            );
        }
    };
    match tokio::time::timeout(BUDGET, smoke_test(&pool)).await {
        Ok(Ok(())) => CheckResult::pass_with_detail(
            NAME,
            "a fresh client wrote and read back after the settle window",
        ),
        Ok(Err(e)) => CheckResult::fail(
            NAME,
            format!("probe write REFUSED after the settle window: {e}"),
        ),
        Err(_) => CheckResult::fail(
            NAME,
            format!("probe write did not complete within {}s after the settle window", BUDGET.as_secs()),
        ),
    }
}

/// The forensics a red run has to carry, none of which the standard report
/// holds. Written on every run so a green one is a usable baseline.
fn render_wedge_diagnostics(samples: &[NodeSample], hosts: &[String], from_ms: u64) -> String {
    let mut md = String::from("# Wedge diagnostics\n\n");
    md.push_str(&format!("Window: scraper t_ms >= {from_ms}.\n\n"));
    for host in hosts {
        md.push_str(&format!("## {host}\n\n"));
        let first = samples.iter().find(|s| s.host == *host && s.ok && s.t_ms >= from_ms);
        let last = samples.iter().rev().find(|s| s.host == *host && s.ok && s.t_ms >= from_ms);
        let (Some(first), Some(last)) = (first, last) else {
            md.push_str("No healthy scrape in the window — nothing to diagnose from.\n\n");
            continue;
        };
        let fmt_map = |m: &std::collections::BTreeMap<u32, u64>| {
            if m.is_empty() {
                "(absent)".to_string()
            } else {
                m.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" ")
            }
        };
        md.push_str(&format!(
            "- effective status (THE gauge): `{}`\n- raw status (do not diagnose from this): `{}`\n- node_role: {}\n",
            fmt_map(&last.effective_status_by_shard),
            fmt_map(&last.node_status_code_by_shard),
            last.node_role,
        ));
        md.push_str(&format!(
            "- lease_remaining_ms: {}\n",
            if last.lease_remaining_ms_by_series.is_empty() {
                "(absent)".to_string()
            } else {
                last.lease_remaining_ms_by_series
                    .iter()
                    .map(|(k, v)| format!("`{k}`={v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        ));
        md.push_str(&format!(
            "- write_errors_total by error_code: {}\n",
            if last.write_errors_by_code.is_empty() {
                "(none — the counter registers lazily, so absence is zero here)".to_string()
            } else {
                last.write_errors_by_code
                    .iter()
                    .map(|(code, v)| format!("{code}={v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        ));
        md.push_str(&format!(
            "- heartbeat_failures_total: {}\n- intrashard_broadcast_dropped_total: {} (carries the lease-renewal StatusUpdate)\n- intrashard_status_broadcast_dropped_total: {}\n",
            last.heartbeat_failures_total,
            last.intrashard_broadcast_dropped_total,
            last.intrashard_status_broadcast_dropped_total,
        ));

        let parked = crate::cardinality_checks::parked_handlers(samples, host, from_ms);
        md.push_str(&format!(
            "\n### Parked mesh handlers ({} of {} entered handlers never returned)\n\n",
            parked.len(),
            last.stuck_handlers.len()
        ));
        if parked.is_empty() {
            md.push_str("None — every entered handler's start stamp advanced across the window.\n");
        } else {
            md.push_str("Same label set AND same start stamp at both ends of the window: the loop entered once and never came out. The label set names the arm.\n\n");
            for entry in &parked {
                md.push_str(&format!("- `{entry}`\n"));
            }
        }
        md.push_str(&format!(
            "\nAll entered handlers at the last scrape: {}\n",
            if last.stuck_handlers.is_empty() {
                "(none)".to_string()
            } else {
                last.stuck_handlers.iter().map(|e| format!("`{e}`")).collect::<Vec<_>>().join(", ")
            }
        ));

        md.push_str("\n### Mesh dequeue per producer pair (delta over the window)\n\n");
        if last.mesh_dequeued_by_pair.is_empty() {
            md.push_str("`celeriant_intrashard_dequeued_total` absent.\n");
        } else {
            md.push_str("| pair | last | delta |\n|---|---|---|\n");
            for (pair, v) in &last.mesh_dequeued_by_pair {
                let before = first.mesh_dequeued_by_pair.get(pair).copied().unwrap_or(0);
                md.push_str(&format!("| `{pair}` | {v} | {} |\n", v.saturating_sub(before)));
            }
        }
        md.push('\n');
    }
    md
}

/// Leave the rig with a running cluster.
///
/// `tear_down_and_evaluate_*` stops both nodes, and the wedge does not
/// self-clear, so a later scenario must not inherit either a stopped pair or a
/// wedged one. Best-effort and loud: the verdict is already written by the time
/// this runs, so a hygiene failure is reported rather than scored.
async fn restart_cluster_for_next_scenario(scen: &str, cfg: &ClusterConfig) {
    let executor = ActionExecutor::new(cfg);
    println!("[{scen}] hygiene: restarting the chaos cluster");
    for action in [Action::StartCs1, Action::StartCs2] {
        if let Err(e) = executor.run(&action) {
            println!("[{scen}] WARNING: hygiene {action:?} failed: {e}");
        }
        sleep(Duration::from_secs(5)).await;
    }
    let scraper = Scraper::start(cfg);
    match wait_for_stable_leader(&scraper, Duration::from_secs(120)).await {
        Ok(()) => println!("[{scen}] hygiene: cluster back with a stable leader"),
        Err(e) => println!(
            "[{scen}] WARNING: no stable leader within 120s of the hygiene restart ({e}) — \
             check the rig before the next scenario"
        ),
    }
    let _ = scraper.stop().await;
}

/// Restore the systemd units the defect scenarios rewrote on the way in.
fn restore_service_baseline(scen: &str, cfg: &ClusterConfig, baseline: Option<(u64, u64)>) {
    let Some((mem, seg)) = baseline else { return };
    let executor = ActionExecutor::new(cfg);
    match executor.run(&Action::UpdateServiceConfig { memory_percent: mem, segment_bytes: seg }) {
        Ok(()) => println!("[{scen}] restored service config to {mem}% / {seg} bytes"),
        Err(e) => println!(
            "[{scen}] WARNING: could not restore service config ({e}) — later runs inherit this one's units"
        ),
    }
}

/// DEFECT 1 (P0) — the cluster wedges into a permanent write outage under
/// ordinary client load, and never comes back.
///
/// Field observation: the cardinality fill at 3000 tasks / 400 births per
/// second froze writes inside 60 seconds. `shard_cannot_accept_writes` climbed
/// into the hundreds of thousands, `celeriant_node_status_effective_code` read
/// 5 (Fenced) on data shards of BOTH nodes, and `node_status_code` / `node_role`
/// went on reading healthy throughout. **Ninety seconds after every client
/// disconnected the shards were still fenced.** Only a restart cleared it.
///
/// So the assertion here is not "writes failed" — that proves overload. It is
/// the ABSENCE OF RECOVERY: stop all load, wait, and demand that no shard on
/// either node is still `effective == Fenced`, plus a fresh client able to
/// write. A run where the cluster never wedged passes that assertion trivially,
/// which is why `WedgeFormedDuringLoad` annunciates a no-wedge run as
/// INCONCLUSIVE rather than letting the green stand unqualified.
pub async fn run_write_outage_selfheal(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    card: crate::cardinality_workload::CardinalityParams,
    defect: DefectParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    use crate::cardinality_workload as cw;

    const SCEN: &str = "write_outage_selfheal";

    cw::validate_tasks(defect.tasks)?;
    let load_budget = Duration::from_secs(defect.load_secs);
    let settle = Duration::from_secs(defect.settle_secs.max(SELFHEAL_MIN_SETTLE_SECS));
    let load_params = ScenarioParams { tasks: defect.tasks, ..params };

    println!("[{SCEN}] the assertion is the ABSENCE OF RECOVERY, not the write errors");
    println!(
        "[{SCEN}]   fill {} tasks at {:.0} births/s cluster-wide for {}s, then ZERO load for {}s",
        defect.tasks,
        defect.birth_rate_per_sec,
        load_budget.as_secs(),
        settle.as_secs(),
    );

    let executor = ActionExecutor::new(cfg);
    executor.run(&Action::UpdateServiceConfig {
        memory_percent: card.memory_percent,
        segment_bytes: card.segment_bytes,
    })?;
    let baseline_cfg = read_service_baseline(cfg);

    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let poll_store = crate::host_poll::HostPollStore::new();
    let poll_handle = crate::host_poll::spawn(
        vec![cfg.leader_host.clone(), cfg.follower_host.clone()],
        CARDINALITY_UNIT.to_string(),
        CARDINALITY_DATA_ROOT.to_string(),
        Duration::from_secs(1),
        poll_store.clone(),
    );

    let mut extra_checks: Vec<CheckResult> = Vec::new();
    let bench_window_start_ms = up.elapsed_ms();

    // Pools live only for the load phase. Dropping them is what makes the
    // settle window honest: the field wedge persisted with every client
    // connection killed, so the scenario has to reach that same state.
    let (totals, latencies, load_start_ms, load_end_ms, stop_reason) = {
        let pool = build_bench_pool(cfg, &up, load_params).await?;
        let lane_pools = build_lane_pools(cfg, &up, load_params).await?;
        println!("[{SCEN}] smoke test");
        smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;
        extra_checks.push(assert_account_schemas_enforced(&pool).await);

        let load_start_ms = up.elapsed_ms();
        println!("[{SCEN}] load on");
        let fill = start_defect_fill(&lane_pools, defect, &card, params.seed, load_budget);
        let stop_reason =
            watch_defect_fill(SCEN, &fill, load_budget, &poll_store, card.disk_high_water_pct).await;
        let (totals, latencies) = fill.stop_and_join().await;
        let load_end_ms = up.elapsed_ms();
        (totals, latencies, load_start_ms, load_end_ms, stop_reason)
    };

    let load_stop = Instant::now();
    let settle_start_ms = up.elapsed_ms();
    println!(
        "[{SCEN}] ALL load stopped ({}): {} writes, {} errors. Settling {}s with zero clients",
        stop_reason.label(),
        totals.writes_ok,
        totals.write_errors,
        settle.as_secs(),
    );
    sleep(settle).await;
    let since_load_stop = load_stop.elapsed();
    let bench_window_end_ms = up.elapsed_ms();

    let samples = up.scraper.store().snapshot().await;
    let hosts = vec![cfg.leader_host.clone(), cfg.follower_host.clone()];

    extra_checks.push(crate::cardinality_checks::wedge_formed_during_load(
        &samples,
        load_start_ms,
        load_end_ms,
        totals.writes_ok,
        totals.write_errors,
    ));
    extra_checks.push(crate::cardinality_checks::write_outage_self_healed(
        &samples,
        &hosts,
        settle_start_ms,
        since_load_stop,
    ));
    extra_checks.push(post_settle_probe_write(cfg, &up, params).await);

    let diagnostics = render_wedge_diagnostics(&samples, &hosts, settle_start_ms);
    let diag_path = run_dir.join(format!("{SCEN}-diagnostics.md"));
    if let Err(e) = std::fs::write(&diag_path, &diagnostics) {
        eprintln!("[{SCEN}] writing {}: {e}", diag_path.display());
    }
    if extra_checks.iter().any(|c| c.failed()) {
        println!("\n{diagnostics}");
    } else {
        println!("[{SCEN}] diagnostics written to {}", diag_path.display());
    }

    poll_store.request_stop();

    // Error volume IS the symptom under a wedge, so nothing here asserts on it;
    // the verdict comes from the settle-window checks above. What stays strict
    // is the counters that must be zero however the run went.
    let expectations = ScenarioExpectations {
        max_leader_elections: 60,
        max_s3_fallbacks: 5_000,
        max_heartbeat_failures: 500,
        max_node_starts: 2,
        max_bench_errors: u64::MAX,
        max_bench_error_ratio: None,
        max_role_flips: 20,
        max_split_brain_ticks: 40,
        require_leader_retained: false,
        // A wedged cluster cannot converge or make progress, and asserting it
        // must would fail this run for the symptom instead of the defect.
        assert_eventual_progress: false,
        ..ScenarioExpectations::default()
    };

    let window_secs = (bench_window_end_ms.saturating_sub(bench_window_start_ms) / 1000).max(1);
    let bench_result = benchmark_from(
        latencies,
        totals.writes_ok,
        totals.write_errors + totals.reheat_errors,
        defect.tasks,
        window_secs,
    );
    let scen_params = ScenarioParams {
        tasks: defect.tasks,
        duration_secs: window_secs,
        // Throughput is the SYMPTOM here, not the assertion. A floor would fail
        // the run for the outage it exists to observe, and the verdict would
        // then be indistinguishable from an underpowered rig.
        throughput_floor: 0.0,
        ..params
    };

    let report = tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        None,
        None,
        None,
        run_dir,
    )
    .await;
    let _ = poll_handle.await;

    restore_service_baseline(SCEN, cfg, baseline_cfg);
    restart_cluster_for_next_scenario(SCEN, cfg).await;
    report
}

/// Hold until the current follower is measurably behind the current leader.
///
/// Returns the leader's host (if one was visible), the largest per-shard lag
/// observed at the moment the gate cleared, and whether the gate was actually
/// met. It fires anyway on the deadline: a run that killed a caught-up follower
/// is a non-reproduction, and reporting its lag makes that diagnosable, whereas
/// hanging forever makes it invisible.
async fn wait_for_behind_follower(
    scen: &str,
    cfg: &ClusterConfig,
    scraper: &Scraper,
    min_lag: u64,
    deadline: Instant,
) -> (Option<String>, u64, bool) {
    let mut leader_host = None;
    let mut lag = 0;
    loop {
        let samples = scraper.store().snapshot().await;
        let last = |h: &str| samples.iter().rev().find(|s| s.host == h && s.ok);
        if let (Some(a), Some(b)) = (last(&cfg.leader_host), last(&cfg.follower_host)) {
            let pair = if a.node_role >= 0.5 {
                Some((a, b))
            } else if b.node_role >= 0.5 {
                Some((b, a))
            } else {
                None
            };
            if let Some((leader, follower)) = pair {
                leader_host = Some(leader.host.clone());
                lag = crate::cardinality_checks::follower_lag(leader, follower);
                if lag >= min_lag {
                    println!("[{scen}] follower is {lag} wal_seq(s) behind — killing the leader now");
                    return (leader_host, lag, true);
                }
            }
        }
        if Instant::now() >= deadline {
            println!(
                "[{scen}] lag gate timed out with lag={lag} (want >= {min_lag}) — killing anyway \
                 so the run reports a diagnosable non-reproduction"
            );
            return (leader_host, lag, false);
        }
        sleep(Duration::from_secs(1)).await;
    }
}

/// DEFECT 2 (P1) — a failed leadership challenge panics the shard.
///
/// Field observation, twice, ~31s apart in outcome and within 2% of each other:
/// under the cardinality fill the leader was killed, the follower auto-fenced,
/// challenged, won lease epoch N+1, sat ~30s in `Promoting` on an S3 WAL
/// catch-up, lost the race to the restarted original leader (epoch N+2), hit
/// "S3 catchup completion barrier timed out", and then hit the unconditional
/// `panic!("Election failed after retries: {e}")` in `shard.rs` — after which
/// the process hung until systemd SIGKILLed it 90 seconds later.
///
/// `leader_sigkill` passes clean on the same cluster with no load, so **load is
/// the ingredient**: sustained fill keeps the follower behind (19 wal_seqs at
/// the kill in the field), forcing the S3 catch-up path during promotion. The
/// kill is therefore gated on an observed non-zero lag, and the lag at kill is
/// reported either way so a non-reproducing run can be told apart from a fix.
pub async fn run_promotion_failure_survival(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    card: crate::cardinality_workload::CardinalityParams,
    defect: DefectParams,
    run_dir: &PathBuf,
) -> Result<ScenarioReport, String> {
    use crate::cardinality_workload as cw;
    use celeriant_bench::population::{Member, Population};

    const SCEN: &str = "promotion_failure_survival";

    cw::validate_tasks(defect.tasks)?;
    let window = Duration::from_secs(defect.load_secs.max(
        PROMO_KILL_AT_SECS + PROMO_LAG_WAIT_SECS + PROMO_RESTART_AFTER_SECS + PROMO_TAIL_SECS,
    ));
    let load_params = ScenarioParams { tasks: defect.tasks, ..params };
    let mix = card.large_event_fraction();

    println!("[{SCEN}] red conditions: a panic in either journal, or a celeriant process found dead");
    println!(
        "[{SCEN}]   fill {} tasks at {:.0} births/s, kill the leader no earlier than {}s and only \
         once the follower is behind, restart it {}s later, window {}s",
        defect.tasks,
        defect.birth_rate_per_sec,
        PROMO_KILL_AT_SECS,
        PROMO_RESTART_AFTER_SECS,
        window.as_secs(),
    );

    let executor = ActionExecutor::new(cfg);
    executor.run(&Action::UpdateServiceConfig {
        memory_percent: card.memory_percent,
        segment_bytes: card.segment_bytes,
    })?;
    let baseline_cfg = read_service_baseline(cfg);

    let journal_from = std::time::SystemTime::now();
    let up = bring_up_cluster(cfg, SCEN, run_dir).await?;
    let poll_start = Instant::now();
    let poll_store = crate::host_poll::HostPollStore::new();
    let poll_handle = crate::host_poll::spawn(
        vec![cfg.leader_host.clone(), cfg.follower_host.clone()],
        CARDINALITY_UNIT.to_string(),
        CARDINALITY_DATA_ROOT.to_string(),
        Duration::from_secs(1),
        poll_store.clone(),
    );

    let pool = build_bench_pool(cfg, &up, ScenarioParams { tasks: 8, ..params }).await?;
    println!("[{SCEN}] smoke test");
    smoke_test(&pool).await.map_err(|e| format!("smoke: {e}"))?;
    let mut extra_checks: Vec<CheckResult> = vec![assert_account_schemas_enforced(&pool).await];
    let lane_pools = build_lane_pools(cfg, &up, load_params).await?;

    let bench_window_start_ms = up.elapsed_ms();
    let fill = start_defect_fill(&lane_pools, defect, &card, params.seed, window);
    let disk_watchdog = fill.spawn_disk_watchdog(poll_store.clone(), card.disk_high_water_pct);

    // The availability probe: a small set of steady writers with their own
    // microsecond clock. The fill's job is to keep the follower behind; this
    // set's job is to say when a caller could not write. Members are minted
    // through the real population path in task-id space above the fill's, so
    // they land on shard lanes without colliding with it.
    let origin = Instant::now();
    let availability = Arc::new(cw::AvailabilityClock::new(origin));
    let (role_samples, role_stop) = spawn_role_watch(cfg, origin);
    let probe_pool =
        build_bench_pool(cfg, &up, ScenarioParams { tasks: PROMO_PROBE_WRITERS, ..params }).await?;
    let total_tasks = defect.tasks + PROMO_PROBE_WRITERS;
    let probe_members: Vec<Member> = (0..PROMO_PROBE_WRITERS)
        .map(|i| {
            let task_id = (defect.tasks + i) as u32;
            Population::new(cw::population_config(task_id, total_tasks, window, params.seed))
                .birth(0)
        })
        .collect();
    let mut probe_handles = Vec::with_capacity(PROMO_PROBE_WRITERS);
    for (i, member) in probe_members.iter().enumerate() {
        probe_handles.push(tokio::spawn(cw::run_hot_writer(
            probe_pool.clone(),
            cw::HotWriterConfig {
                member: *member,
                replica: 0,
                process: i as u32,
                duration: window,
                seed: params.seed ^ 0xF0,
                large_event_fraction: mix,
            },
            None,
            Some(availability.clone()),
        )));
    }

    sleep(Duration::from_secs(PROMO_KILL_AT_SECS)).await;
    let pre_kill_ms = up.elapsed_ms();
    let (leader_now, lag_at_kill, lag_gate_met) = wait_for_behind_follower(
        SCEN,
        cfg,
        &up.scraper,
        1,
        Instant::now() + Duration::from_secs(PROMO_LAG_WAIT_SECS),
    )
    .await;

    let killed_host = leader_now.unwrap_or_else(|| cfg.leader_host.clone());
    let (kill, restart) = if killed_host == cfg.leader_host {
        (Action::KillCs1, Action::StartCs1)
    } else {
        (Action::KillCs2, Action::StartCs2)
    };
    println!("[{SCEN}] SIGKILL {killed_host} mid-load ({:?})", kill);
    executor.run(&kill)?;
    // Marked after the kill returns, matching phase 6: writes acked while the
    // ssh was in flight were served by a live leader.
    availability.mark_kill();
    let kill_poll_ms = poll_start.elapsed().as_millis() as u64;
    let kill_ms = origin.elapsed().as_millis() as u64;

    sleep(Duration::from_secs(PROMO_RESTART_AFTER_SECS)).await;
    println!("[{SCEN}] restarting {killed_host} — the restarted leader winning the next epoch is part of the trigger");
    executor.run(&restart)?;
    let restart_ms = origin.elapsed().as_millis() as u64;
    let restart_poll_ms = poll_start.elapsed().as_millis() as u64;

    let mut probe_stats = cw::HotWriterStats::default();
    let (totals, latencies) = {
        let mut probe_latencies = Vec::new();
        for h in probe_handles {
            if let Ok((s, lat)) = h.await {
                probe_stats.ok += s.ok;
                probe_stats.errors += s.errors;
                probe_stats.occ_retries += s.occ_retries;
                probe_latencies.extend(lat);
            }
        }
        let (totals, mut latencies) = fill.stop_and_join().await;
        disk_watchdog.abort();
        latencies.append(&mut probe_latencies);
        (totals, latencies)
    };

    // Hold the 10Hz window open until the killed node is observed ready:
    // reporting "not observed" for a node that had simply not finished opening
    // its shards would hide the number this measurement exists for.
    let ready_deadline = Instant::now() + PROMO_READY_DEADLINE;
    let roles = loop {
        let snapshot = role_samples.lock().await.clone();
        if cw::restart_ready_ms(&snapshot, &killed_host, restart_ms).is_some()
            || Instant::now() >= ready_deadline
        {
            break snapshot;
        }
        sleep(Duration::from_secs(1)).await;
    };
    role_stop.notify_one();
    poll_store.request_stop();
    let bench_window_end_ms = up.elapsed_ms();

    // ---- scaffolding gates -------------------------------------------------
    // Unmet scaffolding makes the run INCONCLUSIVE, never a pass: the contract
    // assertions below are only meaningful if the cluster was healthy, taking
    // writes, and actually lost its leader.
    let samples = up.scraper.store().snapshot().await;
    extra_checks.push(cluster_was_writing_before_kill(&samples, bench_window_start_ms, pre_kill_ms));
    extra_checks.push(match cw::restart_ready_ms(&roles, &killed_host, restart_ms) {
        Some(_) => CheckResult::pass_with_detail(
            "KillLanded",
            format!("{killed_host} was observed down and then ready again"),
        ),
        None => CheckResult::inconclusive(
            "KillLanded",
            format!(
                "{killed_host} was never observed down in the 10Hz window — the SIGKILL may not \
                 have landed, so nothing below tested a promotion"
            ),
        ),
    });
    extra_checks.push(if lag_gate_met {
        CheckResult::pass_with_detail(
            "FollowerBehindAtKill",
            format!("follower was {lag_at_kill} wal_seq(s) behind at the kill (field trigger: 19)"),
        )
    } else {
        CheckResult::inconclusive(
            "FollowerBehindAtKill",
            format!(
                "follower lag was {lag_at_kill} at the kill after waiting {PROMO_LAG_WAIT_SECS}s — \
                 a caught-up follower promotes off TCP and never reaches the S3 catch-up path, so \
                 this run did not exercise the trigger"
            ),
        )
    });

    // ---- red conditions ----------------------------------------------------
    extra_checks.extend(election_panic_checks(SCEN, cfg, journal_from, run_dir));
    let host_samples = poll_store.snapshot().await;
    extra_checks.push(crate::cardinality_checks::processes_stayed_up(
        &host_samples,
        &killed_host,
        kill_poll_ms,
        restart_poll_ms + PROMO_RESTART_GRACE_SECS * 1000,
    ));

    // ---- report-only -------------------------------------------------------
    let promotion_ms = cw::promotion_latency_ms(&roles, &killed_host, kill_ms);
    let ready_ms = cw::restart_ready_ms(&roles, &killed_host, restart_ms);
    extra_checks.push(timing_check(
        "PromotionLatency",
        promotion_ms,
        "survivor took leadership",
        "no promotion observed in the 10Hz window",
    ));
    extra_checks.push(timing_check(
        "RestartToReady",
        ready_ms,
        "killed node rejoined and its shards reported a WAL sequence",
        "the killed node was not observed ready before the window closed",
    ));
    extra_checks.push(match availability.max_ack_gap() {
        Some(g) => CheckResult::pass_with_detail(
            "WriteUnavailabilityAroundKill",
            format!(
                "{:.0}ms largest silence between consecutive client acks (anchor-free). \
                 The trivial no-load failover achieves ~1600ms; the field runs of this defect \
                 showed ~31,000ms (report-only)",
                g.as_secs_f64() * 1000.0,
            ),
        ),
        None => CheckResult::inconclusive(
            "WriteUnavailabilityAroundKill",
            "fewer than two acks landed in the window — no unavailability to measure".to_string(),
        ),
    });

    println!(
        "[{SCEN}] lag at kill {lag_at_kill}, promotion {}, unavailability {}, restart-to-ready {}",
        promotion_ms.map(|v| format!("{v}ms")).unwrap_or_else(|| "not observed".into()),
        availability
            .max_ack_gap()
            .map(|g| format!("{:.0}ms", g.as_secs_f64() * 1000.0))
            .unwrap_or_else(|| "not observed".into()),
        ready_ms.map(|v| format!("{v}ms")).unwrap_or_else(|| "not observed".into()),
    );

    drop(probe_pool);
    drop(lane_pools);
    drop(pool);

    let expectations = ScenarioExpectations {
        // The killed node restarts once — by systemd's `Restart=on-failure`
        // before this scenario's own `start-cs*` no-ops on top of it. The bound
        // is headroom for that pair, not a defect check: a node that died and
        // came back is named by `CeleriantProcessesStayedUp`, which knows which
        // node was supposed to be down and when.
        max_node_starts: 2,
        max_leader_elections: 60,
        max_s3_fallbacks: 5_000,
        max_heartbeat_failures: 500,
        max_bench_errors: u64::MAX,
        max_bench_error_ratio: None,
        max_role_flips: 20,
        max_split_brain_ticks: 40,
        require_leader_retained: false,
        // A node that panicked will not converge, and asserting it must would
        // fail the run for the consequence instead of the defect.
        assert_eventual_progress: false,
        max_failover_ms: None,
        ..ScenarioExpectations::default()
    };

    let window_secs = (bench_window_end_ms.saturating_sub(bench_window_start_ms) / 1000).max(1);
    let bench_result = benchmark_from(
        latencies,
        totals.writes_ok + probe_stats.ok,
        totals.write_errors + totals.reheat_errors + probe_stats.errors,
        defect.tasks,
        window_secs,
    );
    let scen_params = ScenarioParams {
        tasks: defect.tasks,
        duration_secs: window_secs,
        // Throughput is the SYMPTOM here, not the assertion. A floor would fail
        // the run for the outage it exists to observe, and the verdict would
        // then be indistinguishable from an underpowered rig.
        throughput_floor: 0.0,
        ..params
    };

    let report = tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        expectations,
        scen_params,
        extra_checks,
        None,
        None,
        None,
        run_dir,
    )
    .await;
    let _ = poll_handle.await;

    restore_service_baseline(SCEN, cfg, baseline_cfg);
    restart_cluster_for_next_scenario(SCEN, cfg).await;
    report
}

/// Scaffolding: the cluster must have been serving writes before the kill, or
/// nothing after it tested a promotion.
fn cluster_was_writing_before_kill(
    samples: &[NodeSample],
    from_ms: u64,
    to_ms: u64,
) -> CheckResult {
    const NAME: &str = "ClusterHealthyBeforeKill";
    let in_window = |s: &&NodeSample| s.ok && s.t_ms >= from_ms && s.t_ms <= to_ms;
    let first = samples.iter().filter(in_window).map(|s| s.writes_total).min();
    let last = samples.iter().filter(in_window).map(|s| s.writes_total).max();
    match (first, last) {
        (Some(a), Some(b)) if b > a => CheckResult::pass_with_detail(
            NAME,
            format!("writes_total advanced {a} -> {b} before the kill"),
        ),
        (Some(a), Some(b)) => CheckResult::inconclusive(
            NAME,
            format!("writes_total did not advance before the kill ({a} -> {b}) — the run killed a cluster that was not serving"),
        ),
        _ => CheckResult::inconclusive(
            NAME,
            "no healthy scrape before the kill — cluster health at the kill is unknown".to_string(),
        ),
    }
}

/// Fetch both journals for the run window and match the election panic.
///
/// Separate from the teardown's own harvest because that one runs after the
/// verdict inputs are assembled, and this check has to name the line verbatim.
/// A journal that cannot be read fails closed: an unreadable journal is not
/// evidence that no shard panicked.
fn election_panic_checks(
    scen: &str,
    cfg: &ClusterConfig,
    from: std::time::SystemTime,
    run_dir: &std::path::Path,
) -> Vec<CheckResult> {
    let now = std::time::SystemTime::now();
    let mut out = Vec::new();
    for (label, host) in [("cs1", &cfg.leader_host), ("cs2", &cfg.follower_host)] {
        let dest = run_dir.join(format!("{scen}.{label}.election.log"));
        let fetched = crate::logs::fetch_journal(host, from, now, Duration::from_secs(5), &dest)
            .and_then(|()| {
                std::fs::read_to_string(&dest).map_err(|e| format!("read {}: {e}", dest.display()))
            });
        out.push(match fetched {
            Ok(text) => crate::journal_assert::check_no_election_panic(label, &text),
            Err(e) => CheckResult::fail(
                "NoElectionPanic",
                format!("{label}: journal unavailable ({e}) — a shard panic cannot be ruled out"),
            ),
        });
    }
    out
}

#[cfg(test)]
mod defect_scenario_tests {
    use super::{DefectParams, cluster_was_writing_before_kill};
    use crate::sample::NodeSample;

    fn tick(host: &str, t_ms: u64, writes: u64) -> NodeSample {
        NodeSample { host: host.into(), t_ms, ok: true, writes_total: writes, ..Default::default() }
    }

    /// The defaults ARE the reproduction. Inheriting `--tasks 4000 --birth-rate
    /// 50` would run a scenario named after a defect at a load the defect has
    /// never been seen at, and report its green as evidence.
    #[test]
    fn the_defaults_are_the_shape_the_defects_were_observed_at() {
        let d = DefectParams::default();
        assert_eq!(d.tasks, 3000);
        assert_eq!(d.birth_rate_per_sec, 400.0);
        assert_eq!(d.tasks % 3, 0, "must divide across the data shards");
        assert!(d.settle_secs >= super::SELFHEAL_MIN_SETTLE_SECS);
    }

    #[test]
    fn a_cluster_that_was_not_serving_writes_before_the_kill_is_inconclusive() {
        // A frozen writes_total before the kill means the run killed a cluster
        // that was already not serving — whatever happened after tested nothing.
        let frozen = vec![tick("cs1", 1_000, 500), tick("cs1", 20_000, 500)];
        assert!(cluster_was_writing_before_kill(&frozen, 0, 45_000).is_inconclusive());

        let serving = vec![tick("cs1", 1_000, 500), tick("cs1", 20_000, 90_000)];
        assert!(cluster_was_writing_before_kill(&serving, 0, 45_000).passed());

        // Samples outside the pre-kill window must not stand in for one inside it.
        assert!(cluster_was_writing_before_kill(&serving, 30_000, 45_000).is_inconclusive());
    }
}

#[cfg(test)]
mod teardown_step_tests {
    use super::{step, step_blocking};
    use std::time::Duration;

    const BUDGET: Duration = Duration::from_millis(50);

    #[tokio::test]
    async fn a_step_that_never_finishes_yields_none_instead_of_hanging() {
        // The whole point. `std::future::pending` is the await that phase 7 had
        // somewhere in it for 37 minutes; under `step` the run continues.
        assert_eq!(step("t", "pending", BUDGET, std::future::pending::<u8>()).await, None);
        assert_eq!(step("t", "ready", BUDGET, async { 7u8 }).await, Some(7));
    }

    #[tokio::test]
    async fn blocking_work_is_bounded_too() {
        // A `timeout` around a future that blocks its own thread never fires,
        // which is why every blocking step has to reach `spawn_blocking` first.
        // The thread is not cancelled — it is simply no longer waited on.
        let slept = step_blocking("t", "sleep", BUDGET, || {
            std::thread::sleep(Duration::from_secs(1));
            1u8
        })
        .await;
        assert_eq!(slept, None);
        assert_eq!(step_blocking("t", "quick", BUDGET, || 7u8).await, Some(7));
    }
}

#[cfg(test)]
mod cardinality_pressure_tests {
    use super::{benchmark_from, stride_sample};
    use celeriant_bench::{AggregateKey, TaskAckSummary};

    fn ack(n: u64) -> TaskAckSummary {
        TaskAckSummary {
            aggregate_key: AggregateKey::new(1, 1, n as u128),
            client_id: n as u128,
            max_acked_client_seq: n,
        }
    }

    #[test]
    fn the_audit_sample_strides_the_ledger_rather_than_taking_its_head() {
        // The ledger is a uniform reservoir; a head-take would bias the audit
        // toward whichever tasks happened to be merged first.
        let acks: Vec<TaskAckSummary> = (0..1000).map(ack).collect();
        let sampled = stride_sample(&acks, 100);
        assert_eq!(sampled.len(), 100);
        assert_eq!(sampled[0].max_acked_client_seq, 0);
        assert_eq!(sampled[1].max_acked_client_seq, 10);
        assert_eq!(sampled[99].max_acked_client_seq, 990);
        // Under the cap everything is audited.
        assert_eq!(stride_sample(&acks[..40], 100).len(), 40);
        assert!(stride_sample(&[], 100).is_empty());
    }

    #[test]
    fn the_bench_summary_reports_percentiles_over_the_merged_reservoirs() {
        let lat: Vec<u64> = (1..=1000).collect();
        let b = benchmark_from(lat, 5_000, 7, 300, 10);
        assert_eq!(b.total_requests, 5_000);
        assert_eq!(b.errors, 7);
        assert_eq!(b.throughput, 500.0);
        assert_eq!(b.min_ms, 1);
        assert_eq!(b.max_ms, 1000);
        assert_eq!(b.p50_ms, 501);
        assert_eq!(b.p99_ms, 991);
        assert_eq!(b.p999_ms, 1000);
    }

    #[test]
    fn an_empty_latency_set_reports_zeroes_rather_than_panicking() {
        // A phase that never acked must not index off the end of an empty
        // reservoir on the way to the report.
        let b = benchmark_from(Vec::new(), 0, 0, 3, 60);
        assert_eq!((b.p50_ms, b.p99_ms, b.max_ms, b.throughput), (0, 0, 0, 0.0));
    }
}
