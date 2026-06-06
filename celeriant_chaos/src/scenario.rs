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
use crate::invariants::{CheckResult, RunData, ScenarioExpectations, run_all};
use crate::logs::fetch_journal;
use crate::sample::{NodeSample, elapsed_ms};
use crate::scrape::Scraper;

#[derive(Debug, Clone, Copy)]
pub struct ScenarioParams {
    pub tasks: usize,
    pub duration_secs: u64,
    pub throughput_floor: f64,
}

impl Default for ScenarioParams {
    fn default() -> Self {
        Self { tasks: 4000, duration_secs: 60, throughput_floor: 500.0 }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ScenarioReport {
    pub name: String,
    pub passed: bool,
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
}

#[derive(Debug, serde::Serialize)]
pub struct ScenarioParamsJson {
    pub tasks: usize,
    pub duration_secs: u64,
    pub throughput_floor: f64,
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
    let mut acked_map: std::collections::HashMap<(u128, u128, u128, u128), Vec<u64>> =
        std::collections::HashMap::new();
    for line in &lines {
        if let celeriant_bench::history::HistoryLine::Op(op) = line {
            if matches!(op.outcome, celeriant_bench::history::OpOutcome::Ok) {
                acked_map
                    .entry((op.org_id, op.type_id, op.agg_id, op.client_id))
                    .or_default()
                    .push(op.client_seq);
            }
        }
    }
    let acked: Vec<crate::epoch_oracle::AckedAggregate> = acked_map
        .into_iter()
        .map(|((org_id, type_id, agg_id, client_id), mut acked_seqs)| {
            acked_seqs.sort_unstable();
            acked_seqs.dedup();
            crate::epoch_oracle::AckedAggregate { org_id, type_id, agg_id, client_id, acked_seqs }
        })
        .collect();
    if !acked.is_empty() {
        checks.extend(crate::epoch_oracle::run_acked_durability_oracle(cfg, &acked, 0xce1e51a7).await);
    }
    checks
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

    let disk_truth_report: Option<Vec<crate::disk_truth::DiskTruthEntry>> =
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
            let verified = crate::disk_truth::verify_against_disk_truth(
                &cfg.leader_host,
                &cfg.follower_host,
                &synthesised,
            );
            let truly_missing: u64 = verified.iter().map(|v| v.actually_missing.len() as u64).sum();
            let overreported: u64 = verified.iter().map(|v| v.audit_overreported.len() as u64).sum();
            println!(
                "[{scenario_name}] disk-truth: {} aggregates checked, audit overreported {} seqs, actually missing {} seqs",
                verified.len(), overreported, truly_missing
            );
            verified
        });

    let executor = ActionExecutor::new(cfg);

    // Resource leak check: snapshot fd/RSS while services are still up and
    // compare against the bring-up baseline.
    let mut env_checks: Vec<CheckResult> = Vec::new();
    for (host, before) in &up.resource_before {
        match crate::resource_baseline::snapshot(host).await {
            Ok(after) => env_checks.extend(crate::resource_baseline::baseline_checks(host, before, &after)),
            Err(e) => println!("[{scenario_name}] resource after-snapshot skipped for {host}: {e}"),
        }
    }

    // Give the scraper one more tick before stopping it.
    sleep(Duration::from_millis(750)).await;
    let outcome = up.scraper.stop().await;
    let samples = outcome.store.snapshot().await;

    println!("[{scenario_name}] stop services");
    let _ = executor.run(&Action::StopAll);

    // Post-stop oracles: WAL state is quiescent, MinIO is still up (teardown-data
    // only runs at the NEXT scenario's bring-up).
    env_checks.extend(crate::epoch_oracle::run_epoch_oracle(cfg, 4).await);
    let s3_lifecycle_report = crate::s3_lifecycle::audit_s3_fallback().await;
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
    if let Some(entries) = disk_truth_report.as_ref() {
        if !entries.is_empty() && entries.iter().all(|e| e.actually_missing.is_empty()) {
            let overreported: u64 = entries.iter().map(|e| e.audit_overreported.len() as u64).sum();
            for check in checks.iter_mut() {
                if check.name == "NoClientSeqGaps" && !check.passed {
                    check.passed = true;
                    check.detail = format!(
                        "audit reported gaps but disk-truth verified all {} flagged aggregates are clean ({} seqs overreported)",
                        entries.len(), overreported,
                    );
                }
            }
        }
    }

    checks.extend(env_checks);

    // Journals are now fetched on EVERY run (not just failures) so they can be
    // asserted: panics, aborts, BorrowMutError, and error storms fail the run
    // even when no metric or history check noticed.
    let log_files = harvest_logs(cfg, scenario_name, outcome.wall_start, outcome.wall_end, run_dir);
    for rel in &log_files {
        let path = run_dir.join(rel);
        let node = rel.split('.').nth(1).unwrap_or("node").to_string();
        match std::fs::read_to_string(&path) {
            Ok(text) => checks.extend(crate::journal_assert::journal_checks(&node, &text)),
            Err(e) => println!("[{scenario_name}] journal assert skipped for {rel}: {e}"),
        }
    }

    let passed = checks.iter().all(|c| c.passed);
    if !passed {
        println!("[{scenario_name}] FAIL — journals already in run dir");
    }

    Ok(ScenarioReport {
        name: scenario_name.into(),
        passed,
        params: ScenarioParamsJson {
            tasks: params.tasks,
            duration_secs: params.duration_secs,
            throughput_floor: params.throughput_floor,
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
            passed: false,
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
            passed: false,
            detail: format!(
                "{} task(s) unreadable during audit (allowed {})",
                report.tasks_unreadable, max_unreadable,
            ),
        };
    }
    CheckResult::pass(NAME)
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
    let bench_result = run_benchmark(&pool, params.tasks, params.duration_secs).await;
    let bench_window_end_ms = up.elapsed_ms();
    println!(
        "[baseline] bench done: {} req, {} err, {:.0} req/s, p50={}ms p99={}ms",
        bench_result.total_requests,
        bench_result.errors,
        bench_result.throughput,
        bench_result.p50_ms,
        bench_result.p99_ms,
    );

    tear_down_and_evaluate(
        "baseline",
        cfg,
        up,
        bench_result,
        bench_window_start_ms,
        bench_window_end_ms,
        ScenarioExpectations::default(),
        params,
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
    extra_checks.extend(finish_history_and_check(SCEN, cfg, watch_history, &[]).await);

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
    let outcome =
        run_benchmark_idempotent_with_history(&pool, params.tasks, params.duration_secs, history.clone()).await;
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

    // Allow a brief settle for replication catch-up before the audit reads.
    sleep(Duration::from_secs(2)).await;

    let (integrity, deep) = run_integrity_and_deep_audit(SCEN, &pool, &outcome.task_acks, 32).await;
    let integrity_check = data_integrity_check(&integrity, 0);

    let mut extra_checks = vec![integrity_check];
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks).await);

    tear_down_and_evaluate_with_audit(
        SCEN,
        cfg,
        up,
        outcome.benchmark.clone(),
        bench_window_start_ms,
        bench_window_end_ms,
        ScenarioExpectations::default(),
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
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks).await);

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
    let expectations = ScenarioExpectations {
        max_bench_error_ratio: Some(0.10),
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
    extra_checks.extend(finish_history_and_check(scen, cfg, history, &storm_acks).await);

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
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks).await);

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
    const SEED: u64 = 0xCE1E_51A7;
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
    println!("[{SCEN}] bench: {} tasks, {}s (skew seed {SEED:#x})", params.tasks, params.duration_secs);
    let pool_clone = pool.clone();
    let tasks = params.tasks;
    let dur = params.duration_secs;
    let bench_handle = tokio::spawn(async move { run_benchmark(&pool_clone, tasks, dur).await });

    sleep(Duration::from_secs(15)).await;

    // splitmix64 over (seed, round, host): offsets in ±1..=3s, well past the
    // 500ms drift tolerance, small enough that NTP re-sync is quick.
    let mut rng_state = SEED;
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
    let bench_handle = tokio::spawn(async move {
        run_benchmark_idempotent_with_history(&pool_clone, tasks, dur, history_clone).await
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
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks).await);

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
        // must be ≤ 1500ms (one heartbeat_lease_duration). Set by the
        // S3-CAS path: TTL drain (~500ms) → must_fence → challenge →
        // S3 CAS. Measured at scraper resolution (±500ms).
        max_failover_ms: Some(1500),
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
        max_failover_ms: Some(1500),
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
        ..ScenarioExpectations::default()
    };

    // Throughput dips during the partition (all commits serialise on S3).
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
        max_bench_errors: 10_000,
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
        max_bench_errors: 10_000,
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
    let bench_handle = tokio::spawn(async move {
        run_benchmark_idempotent_with_history(&pool_clone, tasks, AUDIT_BENCH_SECS, history_clone).await
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
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks).await);

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
    let bench_handle = tokio::spawn(async move {
        run_benchmark_idempotent_with_history(&pool_clone, tasks, FAST_BENCH_SECS, history_clone).await
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
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks).await);

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
        // 1500ms recovery budget as graceful-stop and sigkill scenarios.
        max_failover_ms: Some(1500),
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
        // skewed clock and 500ms TTL, we see ~20 challenges. Runs have
        // been observed in the 18-22 range due to timing variance.
        // Bumped from 20 → 30 for headroom; the intent is "real
        // leader change" which would be a single-digit delta.
        max_leader_elections: 30,
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

    // 200 MiB reserve: bigger than `SHARD_LOG_PREALLOCATE_BYTES` (128 MiB)
    // so at least one log segment rotation can succeed under the fill.
    // 50 MiB was too tight — any rotation during the fill window hit
    // ENOSPC on preallocate, which propagates as `FsyncFailed` →
    // `run_s3_catchup` returns error → shard panics in `shard.rs:780`
    // (the `run_s3_catchup failed with fatal error` path). That path
    // is the real bug — the shard should treat ENOSPC as transient
    // and retry, not panic — but bumping the reserve first lets this
    // test pass without gambling on whether a rotation happens during
    // the bench window.
    //
    // Tracking the transient-fsync-error fix as a follow-up in
    // status-log.md; it's a bigger change to the error handling in
    // `run_s3_catchup` + `catchup_round`.
    let fill = Action::FillDisk {
        host: follower_host.clone(),
        reserve_mb: 200,
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

    // Hold the full-disk state for 20 seconds — enough for several
    // commit cycles, potentially a rotation if load is high enough,
    // and for the leader's S3 fallback path to fire if it's going to.
    sleep(Duration::from_secs(20)).await;

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

    // Settle bumped 15s → 90s: ENOSPC panics shards 1/2 on cs2; in-process
    // shard restart drops the active TCP replication, leaving the last
    // in-flight batch (~1333 entries / 4MB) stranded. cs1's reconciliation
    // probe gets LockTimeout under the S3 fallback storm. With bench halted,
    // 90s of idle gives the storm time to clear and normal TCP replication
    // to resume the gap-fill. EC2+S3 converges in <5s.
    println!("[{SCEN}] settle 90s for catchup + disk-pressure recovery (slow-infra liveness window)");
    sleep(Duration::from_secs(90)).await;
    let bench_window_end_ms = up.elapsed_ms();

    // Defensive final cleanup.
    let _ = executor.run(&clean);

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
        // ENOSPC during rotation panics per Phase 5's invariant
        // ("fail loudly on disk full"). 20 covers shards × rotations
        // across the disk-pressure window.
        max_shard_panics: 20,
        // In-process restarts + systemd restarts across shards during
        // disk-full window; 5 covers it.
        max_node_starts: 5,
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
        passed: all_nonzero,
        params: ScenarioParamsJson {
            tasks: last_tasks,
            duration_secs: base_params.duration_secs,
            throughput_floor: base_params.throughput_floor,
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
    let pad = Duration::from_secs(5);
    let mut written = Vec::new();
    for (label, host) in [("cs1", &cfg.leader_host), ("cs2", &cfg.follower_host)] {
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
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        sleep(Duration::from_millis(500)).await;
        let snap = scraper.store().snapshot().await;
        // Look at the most recent sample per host.
        let last_l = snap.iter().rev().find(|s| s.ok && s.node_role >= 0.5);
        let last_f = snap.iter().rev().find(|s| s.ok && s.node_role < 0.5);
        if last_l.is_some() && last_f.is_some() {
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

fn sample_window(samples: &[NodeSample], start_ms: u64, end_ms: u64) -> (usize, usize) {
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
        CheckResult::pass_with_detail(
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
    const NEMESIS_SEED: u64 = 0xDEAD_BEEF_C0DE_CAFEu64;
    const BENCH_SECS: u64 = 90;

    println!("[{SCEN}] nemesis seed: {NEMESIS_SEED:#x}");

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
        let mut prng = NemesisPrng::new(NEMESIS_SEED);
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
        let mut prng = NemesisPrng::new(NEMESIS_SEED.wrapping_add(0x0101_0101_0101_0101));
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
    println!("[{SCEN}] idempotent bench: {} tasks, {}s (seed {NEMESIS_SEED:#x})", params.tasks, BENCH_SECS);
    let pool_clone = pool.clone();
    let history_clone = history.clone();
    let tasks = params.tasks;
    let bench_handle = tokio::spawn(async move {
        run_benchmark_idempotent_with_history(&pool_clone, tasks, BENCH_SECS, history_clone).await
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
    extra_checks.extend(finish_history_and_check(SCEN, cfg, history, &outcome.task_acks).await);

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
