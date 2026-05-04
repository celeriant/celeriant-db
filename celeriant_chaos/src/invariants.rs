use serde::Serialize;

use crate::sample::NodeSample;

/// Result of a single check against the captured run data.
#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

impl CheckResult {
    pub fn pass(name: &'static str) -> Self {
        Self { name, passed: true, detail: "ok".into() }
    }
    fn fail(name: &'static str, detail: impl Into<String>) -> Self {
        Self { name, passed: false, detail: detail.into() }
    }
}

/// Per-scenario tolerance for the counter-based and stability checks.
///
/// `baseline` uses `Default` (strict zero everywhere). Chaos scenarios bump
/// the relevant fields to the largest count they expect to see during the
/// chaos window. If a real run exceeds the bound it's a fail, same as if a
/// "should be zero" check tripped.
///
/// All counter fields are *maximum allowed deltas* across the bench window.
/// Each is checked per-host (leader and follower); the larger of the two
/// deltas is compared against the bound.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ScenarioExpectations {
    pub max_leader_elections: u64,
    pub max_s3_fallbacks: u64,
    pub max_heartbeat_failures: u64,
    pub max_shard_panics: u64,
    pub max_node_starts: u64,
    pub max_bench_errors: u64,

    /// Maximum number of scrape ticks where `node_role` summed across both
    /// nodes is not exactly 1. Brief split-brain windows are tolerated by
    /// chaos scenarios that exercise leadership change.
    pub max_split_brain_ticks: u64,
    /// Maximum number of role flips on either node during the bench window.
    pub max_role_flips: u64,

    /// If true, run `EventualConvergence`: at the end of the bench window
    /// the lagging node's `wal_index_max` must either equal the leading
    /// node's, or have strictly advanced over the final settle window.
    /// Catches genuine stuck-state divergence (lagging node frozen at a
    /// non-zero diff) while accepting "still catching up" as a pass.
    /// Replaces the old fixed-tolerance shape — tolerance was arbitrary
    /// and could not distinguish slow-but-progressing from stuck.
    /// `false` (default) skips the check.
    pub assert_eventual_progress: bool,

    /// If true, run `LeaderRetained`: the host that holds `node_role >= 0.5`
    /// at the first ok tick must be the same host that holds it at the last
    /// ok tick. Used by scenarios that perturb the *follower* and expect
    /// leadership to never change hands. False (default) skips the check.
    pub require_leader_retained: bool,

    /// If true, run `FinalLeaderWroteDuringBench`: the host that is leader
    /// at the last ok tick of the bench window must have strictly advanced
    /// its `writes_total` from the first tick it held leadership to that
    /// last tick. Catches "promoted leader that never actually served client
    /// writes" — a failure mode that `WalIndexAdvanced` and
    /// `EventualConvergence` both miss because they can be satisfied by
    /// frozen, matching-but-dead values. Required by SCEN-4/5.
    pub require_final_leader_write_progress: bool,

    /// If `Some(n)`, run `DistinctLeaderHosts`: at least `n` distinct hosts
    /// must have held `node_role >= 0.5` at some ok tick inside the bench
    /// window. Catches "the scenario tried to force a failover but the
    /// surviving node never actually promoted", which otherwise trivially
    /// passes the rest of the checks. SCEN-6 sets this to 2 so a run where
    /// cs1 was killed three times but bounced back each time before cs2
    /// could promote fails loudly. `None` (default) skips the check.
    pub require_distinct_leader_hosts: Option<usize>,
}

impl Default for ScenarioExpectations {
    fn default() -> Self {
        Self {
            max_leader_elections: 0,
            max_s3_fallbacks: 0,
            max_heartbeat_failures: 0,
            max_shard_panics: 0,
            max_node_starts: 0,
            max_bench_errors: 0,
            max_split_brain_ticks: 0,
            max_role_flips: 0,
            assert_eventual_progress: false,
            require_leader_retained: false,
            require_final_leader_write_progress: false,
            require_distinct_leader_hosts: None,
        }
    }
}

/// Aggregated context the checks run against. Built once after a scenario.
pub struct RunData<'a> {
    pub samples: &'a [NodeSample],
    pub leader_host: &'a str,
    pub follower_host: &'a str,
    /// The first scrape index inside the bench window (inclusive).
    pub bench_start_idx: usize,
    /// The last scrape index inside the bench window (inclusive).
    pub bench_end_idx: usize,
    pub bench_errors: u64,
    pub bench_throughput: f64,
    pub throughput_floor: f64,
}

impl<'a> RunData<'a> {
    /// Iterate (leader_sample, follower_sample) pairs at each scrape tick
    /// in the bench window. Pairs are matched by t_ms (the scraper pushes
    /// them at the same tick).
    fn pairs(&self) -> impl Iterator<Item = (&NodeSample, &NodeSample)> {
        let leader = self.leader_host;
        let follower = self.follower_host;
        let slice = &self.samples[self.bench_start_idx..=self.bench_end_idx];
        slice.iter().filter(move |s| s.host == leader).filter_map(move |l| {
            slice
                .iter()
                .find(|s| s.host == follower && s.t_ms == l.t_ms)
                .map(|f| (l, f))
        })
    }

    fn leader_first_last(&self) -> Option<(&NodeSample, &NodeSample)> {
        let host = self.leader_host;
        let first = self.samples[self.bench_start_idx..=self.bench_end_idx]
            .iter()
            .find(|s| s.host == host && s.ok)?;
        let last = self.samples[self.bench_start_idx..=self.bench_end_idx]
            .iter()
            .rev()
            .find(|s| s.host == host && s.ok)?;
        Some((first, last))
    }
}

pub fn run_all(data: &RunData, expect: &ScenarioExpectations) -> Vec<CheckResult> {
    let mut out = vec![
        check_exactly_one_leader(data, expect),
        check_leader_stable(data, expect),
        check_counter("NoUnexpectedElections", data, |s| s.leader_elections_total, expect.max_leader_elections),
        check_counter("NoS3Fallbacks", data, |s| s.s3_fallbacks_total, expect.max_s3_fallbacks),
        check_counter("NoHeartbeatFailures", data, |s| s.heartbeat_failures_total, expect.max_heartbeat_failures),
        check_counter("NoShardPanics", data, |s| s.shard_panics_total, expect.max_shard_panics),
        check_counter("NoNodeStarts", data, |s| s.node_starts_total, expect.max_node_starts),
        check_bench_errors(data, expect),
        check_bench_throughput_floor(data),
        check_wal_index_advanced(data),
    ];
    if expect.require_leader_retained {
        out.push(check_leader_retained(data));
    }
    if expect.assert_eventual_progress {
        out.push(check_eventual_convergence(data));
    }
    if expect.require_final_leader_write_progress {
        out.push(check_final_leader_write_progress(data));
    }
    if let Some(min) = expect.require_distinct_leader_hosts {
        out.push(check_distinct_leader_hosts(data, min));
    }
    out
}

fn check_exactly_one_leader(data: &RunData, expect: &ScenarioExpectations) -> CheckResult {
    const NAME: &str = "ExactlyOneLeader";
    let mut bad_ticks = 0u64;
    let mut first_bad: Option<String> = None;
    for (l, f) in data.pairs() {
        if !(l.ok && f.ok) {
            continue;
        }
        let role_sum = l.node_role + f.node_role;
        if (role_sum - 1.0).abs() > 0.5 {
            bad_ticks += 1;
            if first_bad.is_none() {
                first_bad = Some(format!(
                    "first at t_ms={}: leader.role={} follower.role={} (sum={})",
                    l.t_ms, l.node_role, f.node_role, role_sum
                ));
            }
        }
    }
    if bad_ticks > expect.max_split_brain_ticks {
        CheckResult::fail(
            NAME,
            format!(
                "{bad_ticks} ticks without exactly one leader (allowed {}); {}",
                expect.max_split_brain_ticks,
                first_bad.unwrap_or_else(|| "no detail".into())
            ),
        )
    } else {
        CheckResult::pass(NAME)
    }
}

fn check_leader_stable(data: &RunData, expect: &ScenarioExpectations) -> CheckResult {
    const NAME: &str = "LeaderStable";
    let mut role_changes_leader = 0u64;
    let mut role_changes_follower = 0u64;
    let mut prev_l: Option<f64> = None;
    let mut prev_f: Option<f64> = None;
    for (l, f) in data.pairs() {
        if !(l.ok && f.ok) {
            continue;
        }
        if let Some(p) = prev_l
            && (p - l.node_role).abs() > 0.5
        {
            role_changes_leader += 1;
        }
        if let Some(p) = prev_f
            && (p - f.node_role).abs() > 0.5
        {
            role_changes_follower += 1;
        }
        prev_l = Some(l.node_role);
        prev_f = Some(f.node_role);
    }
    let total = role_changes_leader + role_changes_follower;
    if total > expect.max_role_flips {
        CheckResult::fail(
            NAME,
            format!(
                "{total} role flips (allowed {}): leader={role_changes_leader} follower={role_changes_follower}",
                expect.max_role_flips
            ),
        )
    } else {
        CheckResult::pass(NAME)
    }
}

fn check_counter(
    name: &'static str,
    data: &RunData,
    field: fn(&NodeSample) -> u64,
    allowed: u64,
) -> CheckResult {
    let mut worst = (0u64, "".to_string());
    for host in [data.leader_host, data.follower_host] {
        let first = data.samples[data.bench_start_idx..=data.bench_end_idx]
            .iter()
            .find(|s| s.host == host && s.ok);
        let last = data.samples[data.bench_start_idx..=data.bench_end_idx]
            .iter()
            .rev()
            .find(|s| s.host == host && s.ok);
        if let (Some(a), Some(b)) = (first, last) {
            let delta = field(b).saturating_sub(field(a));
            if delta > worst.0 {
                worst = (delta, format!("{host}: {} → {}", field(a), field(b)));
            }
        }
    }
    if worst.0 > allowed {
        CheckResult::fail(name, format!("delta {} exceeds allowed {} ({})", worst.0, allowed, worst.1))
    } else {
        CheckResult::pass(name)
    }
}

fn check_bench_errors(data: &RunData, expect: &ScenarioExpectations) -> CheckResult {
    const NAME: &str = "BenchErrorsBounded";
    if data.bench_errors <= expect.max_bench_errors {
        CheckResult::pass(NAME)
    } else {
        CheckResult::fail(
            NAME,
            format!("bench reported {} errors (allowed {})", data.bench_errors, expect.max_bench_errors),
        )
    }
}

fn check_bench_throughput_floor(data: &RunData) -> CheckResult {
    const NAME: &str = "BenchThroughputFloor";
    if data.bench_throughput >= data.throughput_floor {
        CheckResult::pass(NAME)
    } else {
        CheckResult::fail(
            NAME,
            format!("throughput {:.0} req/s below floor {:.0}", data.bench_throughput, data.throughput_floor),
        )
    }
}

/// The host that holds `node_role >= 0.5` at the first ok pair-tick must be
/// the same host that holds it at the last ok pair-tick. Used to assert that
/// follower-only chaos never causes a leadership handover.
fn check_leader_retained(data: &RunData) -> CheckResult {
    const NAME: &str = "LeaderRetained";
    let mut first: Option<&str> = None;
    let mut last: Option<&str> = None;
    for (l, f) in data.pairs() {
        if !(l.ok && f.ok) {
            continue;
        }
        let leader_at_tick = if l.node_role >= 0.5 {
            Some(l.host.as_str())
        } else if f.node_role >= 0.5 {
            Some(f.host.as_str())
        } else {
            None
        };
        if let Some(h) = leader_at_tick {
            if first.is_none() {
                first = Some(h);
            }
            last = Some(h);
        }
    }
    match (first, last) {
        (Some(a), Some(b)) if a == b => CheckResult::pass(NAME),
        (Some(a), Some(b)) => CheckResult::fail(NAME, format!("leader changed: {a} → {b}")),
        _ => CheckResult::fail(NAME, "no ok ticks where either node was leader"),
    }
}

/// At the last ok sample of each host within the bench window, both nodes'
/// PROGRESS check: at the end of the bench+settle window, the lagging
/// node must either (a) have converged to the leading node's `wal_index_max`,
/// or (b) be strictly advancing across the final `PROGRESS_WINDOW_MS`.
/// A "lagging node frozen at a non-zero diff" is the stuck-state failure
/// this catches; "still catching up, just slower than settle" passes.
///
/// Replaces the prior fixed-tolerance shape, which could not distinguish
/// "slow but converging" from "permanently stuck" — both showed up as
/// `wal_index diverges by N (tolerance M)` with no signal about whether
/// the cluster was making forward progress.
fn check_eventual_convergence(data: &RunData) -> CheckResult {
    const NAME: &str = "EventualConvergence";
    /// Window over which we measure progress on the lagging node, in ms.
    /// 10s gives slow rpi MinIO time to deliver at least one S3 catchup
    /// round; tighter windows produced false "STUCK" verdicts when the
    /// follower had drained everything S3 had but the leader hadn't yet
    /// uploaded its trailing batch.
    const PROGRESS_WINDOW_MS: u64 = 10_000;

    let last_ok = |host: &str| -> Option<&NodeSample> {
        data.samples[data.bench_start_idx..=data.bench_end_idx]
            .iter()
            .rev()
            .find(|s| s.host == host && s.ok)
    };
    let l = match last_ok(data.leader_host) {
        Some(s) => s,
        None => return CheckResult::fail(NAME, "missing ok samples for leader host in bench window"),
    };
    let f = match last_ok(data.follower_host) {
        Some(s) => s,
        None => return CheckResult::fail(NAME, "missing ok samples for follower host in bench window"),
    };

    if l.wal_index_max == f.wal_index_max {
        return CheckResult::pass(NAME);
    }

    let (lagging_host, lagging_final) = if l.wal_index_max < f.wal_index_max {
        (data.leader_host, l)
    } else {
        (data.follower_host, f)
    };
    let window_start_ms = lagging_final.t_ms.saturating_sub(PROGRESS_WINDOW_MS);
    let lagging_window_first = data.samples[data.bench_start_idx..=data.bench_end_idx]
        .iter()
        .find(|s| s.host == lagging_host && s.ok && s.t_ms >= window_start_ms);

    match lagging_window_first {
        Some(first) if lagging_final.wal_index_max > first.wal_index_max => CheckResult::pass(NAME),
        Some(first) => {
            let diff = l.wal_index_max.max(f.wal_index_max) - l.wal_index_max.min(f.wal_index_max);
            CheckResult::fail(
                NAME,
                format!(
                    "STUCK: lagging host {} frozen at wal_index={} for {}ms (diff from peer: {}); leading host {} at wal_index={}",
                    lagging_host, lagging_final.wal_index_max,
                    lagging_final.t_ms.saturating_sub(first.t_ms),
                    diff,
                    if lagging_host == data.leader_host { data.follower_host } else { data.leader_host },
                    if lagging_host == data.leader_host { f.wal_index_max } else { l.wal_index_max },
                ),
            )
        }
        None => CheckResult::fail(
            NAME,
            format!("no ok samples for lagging host {} in final {}ms window", lagging_host, PROGRESS_WINDOW_MS),
        ),
    }
}

/// The host that is leader at the last ok sample of the bench window must
/// have strictly advanced its `writes_total` from the first tick in the
/// window where it first held leadership to that last tick. A frozen
/// promoted leader (writes_total == 0 throughout) fails this check.
fn check_final_leader_write_progress(data: &RunData) -> CheckResult {
    const NAME: &str = "FinalLeaderWroteDuringBench";
    let slice = &data.samples[data.bench_start_idx..=data.bench_end_idx];

    // Find the host holding leadership at the last ok tick.
    let Some(final_leader_host) = slice
        .iter()
        .rev()
        .find(|s| s.ok && s.node_role >= 0.5)
        .map(|s| s.host.as_str())
    else {
        return CheckResult::fail(NAME, "no ok tick with any node as leader in bench window");
    };

    // First tick within the window where this host was leader.
    let first_as_leader = slice
        .iter()
        .find(|s| s.ok && s.host == final_leader_host && s.node_role >= 0.5);
    // Last ok tick for this host (always exists if first_as_leader does).
    let last_for_host = slice
        .iter()
        .rev()
        .find(|s| s.ok && s.host == final_leader_host);

    match (first_as_leader, last_for_host) {
        (Some(first), Some(last)) => {
            if last.writes_total > first.writes_total {
                CheckResult::pass(NAME)
            } else {
                CheckResult::fail(
                    NAME,
                    format!(
                        "final leader {} did not advance writes_total: {} → {} (first-as-leader t_ms={}, last t_ms={})",
                        final_leader_host,
                        first.writes_total,
                        last.writes_total,
                        first.t_ms,
                        last.t_ms,
                    ),
                )
            }
        }
        _ => CheckResult::fail(
            NAME,
            format!("missing samples for final leader {final_leader_host}"),
        ),
    }
}

/// At least `min` distinct hosts must have held `node_role >= 0.5` at some
/// ok tick inside the bench window. Used by SCEN-6 to detect the case where
/// the scenario killed the leader N times but the surviving node never
/// actually promoted (e.g. because the kill-to-restart window was shorter
/// than the lease TTL), so leadership stayed on a single host the whole
/// time and `FinalLeaderWroteDuringBench` passed trivially.
fn check_distinct_leader_hosts(data: &RunData, min: usize) -> CheckResult {
    const NAME: &str = "DistinctLeaderHosts";
    let slice = &data.samples[data.bench_start_idx..=data.bench_end_idx];
    let mut seen: Vec<&str> = Vec::new();
    for s in slice.iter() {
        if !s.ok || s.node_role < 0.5 {
            continue;
        }
        let h = s.host.as_str();
        if !seen.iter().any(|x| *x == h) {
            seen.push(h);
        }
    }
    if seen.len() >= min {
        CheckResult::pass(NAME)
    } else {
        CheckResult::fail(
            NAME,
            format!(
                "only {} distinct host(s) held leadership in bench window (required {min}): {:?}",
                seen.len(),
                seen
            ),
        )
    }
}

fn check_wal_index_advanced(data: &RunData) -> CheckResult {
    const NAME: &str = "WalIndexAdvanced";
    let Some((first, last)) = data.leader_first_last() else {
        return CheckResult::fail(NAME, "no leader samples in bench window");
    };
    if last.wal_index_max > first.wal_index_max {
        CheckResult::pass(NAME)
    } else {
        CheckResult::fail(
            NAME,
            format!(
                "leader wal_index did not advance: {} → {}",
                first.wal_index_max, last.wal_index_max
            ),
        )
    }
}
