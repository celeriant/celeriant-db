//! Checks specific to `cardinality_pressure`.
//!
//! Split from `invariants.rs` because these answer a different question. The
//! standard suite asks "did the database stay correct". These ask "did this run
//! reach a regime where its measurements mean anything", and they answer
//! INCONCLUSIVE rather than PASS when it did not.
//!
//! ## Absence is not zero — but it is not always fatal either
//!
//! Almost every `celeriant_*` counter registers lazily: it first appears in
//! `/metrics` only after its first increment. Verified in the field on cs1 —
//! after a 34k-request bench, `celeriant_read_bloom_gate_total` was present at
//! 38 while `celeriant_read_segments_walked_total` was absent entirely, because
//! nothing ever survived the bloom to be walked.
//!
//! That splits into two rules, and conflating them is how a broken harness
//! reports a green run:
//!
//! - **Must-be-zero checks** (`NoRotationEnospc`, `NoBloomAbsentSegments`):
//!   absent means never incremented, which is exactly the zero being asserted.
//!   Absence is a legitimate PASS.
//! - **Denominator checks** (`BloomEffectiveness`): absent means the ratio has no
//!   denominator. Reporting that as "100% effective" is the worst failure mode
//!   available, because it is indistinguishable from a perfect result. Absence
//!   is INCONCLUSIVE.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::host_poll::{HostPollStore, HostSample};
use crate::invariants::CheckResult;
use crate::sample::NodeSample;

/// Minimum segments per data shard before anything was genuinely cold.
pub const MIN_SEGMENTS_PER_SHARD: u64 = 10;
/// Minimum populated age buckets before the reheat curve is a curve.
pub const MIN_POPULATED_AGE_BUCKETS: usize = 3;

/// `celeriant_node_status_effective_code` for `Fenced`. Encoding:
/// `0=BootCatchup 1=Follower 2=FollowerCatchingUp 3=Promoting 4=Leader
/// 5=Fenced 6=Standalone`.
pub const STATUS_FENCED: u64 = 5;

fn last_ok<'a>(samples: &'a [NodeSample], host: &str) -> Option<&'a NodeSample> {
    samples.iter().rev().find(|s| s.host == host && s.ok)
}

fn metric_present(samples: &[NodeSample], key: &str) -> bool {
    samples.iter().any(|s| s.ok && s.metric_keys_present.contains(key))
}

/// Distinct aggregates and clients must both strictly increase across the fill.
///
/// A population that stopped growing means the birth process stalled and the
/// whole non-stationary model collapsed to the stationary one — at which point
/// every age bucket is measuring the same thing and the curve is meaningless.
pub fn cardinality_grew(samples: &[(u64, u64, u64)]) -> CheckResult {
    const NAME: &str = "CardinalityGrew";
    if samples.len() < 2 {
        return CheckResult::inconclusive(
            NAME,
            format!("only {} cardinality sample(s) — need at least 2 to see growth", samples.len()),
        );
    }
    let mut stalls = Vec::new();
    for w in samples.windows(2) {
        let (t0, a0, c0) = w[0];
        let (t1, a1, c1) = w[1];
        if a1 <= a0 || c1 <= c0 {
            stalls.push(format!("{t0}ms→{t1}ms: aggs {a0}→{a1}, clients {c0}→{c1}"));
        }
    }
    let (_, first_a, first_c) = samples[0];
    let (_, last_a, last_c) = samples[samples.len() - 1];
    let detail = format!(
        "aggregates {first_a}→{last_a}, clients {first_c}→{last_c} over {} samples",
        samples.len()
    );
    if stalls.is_empty() {
        CheckResult::pass_with_detail(NAME, detail)
    } else {
        CheckResult::fail(NAME, format!("{detail}; {} stalled interval(s): {}", stalls.len(), stalls.join("; ")))
    }
}

/// At least `MIN_POPULATED_AGE_BUCKETS` reheat buckets carry a usable sample.
///
/// Without spread there is no cost-versus-age curve, only a blended number — and
/// a blended number cannot distinguish "flat" from "steep", which is the entire
/// question. Not reaching spread is a property of the run's duration, not a
/// defect, so this is INCONCLUSIVE rather than FAIL. `smoke` fails it by
/// construction and that is the correct result.
pub fn age_spread_reached(populated: usize, min_ops: u64) -> CheckResult {
    const NAME: &str = "AgeSpreadReached";
    let detail = format!(
        "{populated}/{} age buckets carry >= {min_ops} reheat sample(s) (need {MIN_POPULATED_AGE_BUCKETS})",
        crate::sample::AGE_BUCKET_COUNT
    );
    if populated >= MIN_POPULATED_AGE_BUCKETS {
        CheckResult::pass_with_detail(NAME, detail)
    } else {
        CheckResult::inconclusive(NAME, format!("{detail} — no curve, only a blended number"))
    }
}

/// At least `MIN_SEGMENTS_PER_SHARD` segments on every data shard.
///
/// Below that nothing was ever genuinely cold, so every cold-path number in the
/// report was measured against a warm cluster.
pub fn rotations_reached(per_shard: &[(u32, u64)]) -> CheckResult {
    const NAME: &str = "RotationsReached";
    if per_shard.is_empty() {
        return CheckResult::inconclusive(NAME, "no segment counts collected".to_string());
    }
    let short: Vec<String> = per_shard
        .iter()
        .filter(|(_, n)| *n < MIN_SEGMENTS_PER_SHARD)
        .map(|(s, n)| format!("shard {s}: {n}"))
        .collect();
    let detail = format!(
        "segments per data shard: [{}] (need >= {MIN_SEGMENTS_PER_SHARD})",
        per_shard.iter().map(|(s, n)| format!("{s}={n}")).collect::<Vec<_>>().join(", ")
    );
    if short.is_empty() {
        CheckResult::pass_with_detail(NAME, detail)
    } else {
        CheckResult::inconclusive(NAME, format!("{detail} — cold path not exercised on {}", short.join(", ")))
    }
}

/// Rotation ENOSPC must never fire.
///
/// It keeps the shard alive but fails every write needing rotation, which
/// otherwise reads as a mystery throughput collapse late in the fill. If this
/// trips, the disk watchdog was too slow and the tail of the run is garbage.
///
/// Absent metric is a legitimate pass: lazily registered, so absence means it
/// never fired.
pub fn no_rotation_enospc(samples: &[NodeSample], hosts: &[String]) -> CheckResult {
    const NAME: &str = "NoRotationEnospc";
    let mut worst = 0u64;
    let mut detail = Vec::new();
    for h in hosts {
        let v = last_ok(samples, h).map(|s| s.rotation_out_of_space_total).unwrap_or(0);
        worst = worst.max(v);
        detail.push(format!("{h}={v}"));
    }
    let detail = format!("rotation ENOSPC: {}", detail.join(", "));
    if worst == 0 {
        CheckResult::pass_with_detail(NAME, detail)
    } else {
        CheckResult::fail(NAME, format!("{detail} — disk watchdog too slow; the tail of this fill is not trustworthy"))
    }
}

/// No sealed segment may lose its bloom across a restart.
///
/// An absent bloom answers maybe-present for every key, so the segment silently
/// becomes unskippable — a correctness-adjacent bug, not a performance one, and
/// invisible before `read_bloom_absent_total` existed.
pub fn no_bloom_absent_segments(samples: &[NodeSample], hosts: &[String]) -> CheckResult {
    const NAME: &str = "NoBloomAbsentSegments";
    let mut worst = 0u64;
    let mut detail = Vec::new();
    for h in hosts {
        let v = last_ok(samples, h).map(|s| s.read_bloom_absent_total).unwrap_or(0);
        worst = worst.max(v);
        detail.push(format!("{h}={v}"));
    }
    let detail = format!("keyed visits with a no-information bloom: {}", detail.join(", "));
    if worst == 0 {
        CheckResult::pass_with_detail(NAME, detail)
    } else {
        CheckResult::fail(NAME, format!("{detail} — sealed segments lost their bloom; these scans escaped filtering entirely"))
    }
}

/// Bloom skips over gate consultations, report-only until a calibration run
/// pins a threshold.
///
/// **Absence is INCONCLUSIVE, never a pass.** A missing gate counter means a
/// renamed metric, a stale binary, or a read path that never ran — and a naive
/// ratio would render every one of those as flawless bloom effectiveness.
pub fn bloom_effectiveness(samples: &[NodeSample], hosts: &[String]) -> CheckResult {
    const NAME: &str = "BloomEffectiveness";
    if !metric_present(samples, "celeriant_read_bloom_gate_total") {
        return CheckResult::inconclusive(
            NAME,
            "celeriant_read_bloom_gate_total never appeared in /metrics — no keyed scan ran, \
             or the binary predates the counter. Cannot be read as 100% effective."
                .to_string(),
        );
    }
    let mut gate = 0u64;
    let mut skip = 0u64;
    let mut walked = 0u64;
    for h in hosts {
        if let Some(s) = last_ok(samples, h) {
            gate += s.read_bloom_gate_total;
            skip += s.read_bloom_short_circuit_total;
            walked += s.read_segments_walked_total;
        }
    }
    if gate == 0 {
        return CheckResult::inconclusive(NAME, "no keyed segment visits recorded — nothing to measure".to_string());
    }
    CheckResult::pass_with_detail(
        NAME,
        format!(
            "bloom skipped {skip}/{gate} keyed segment visits ({:.1}%); {walked} segments walked",
            100.0 * skip as f64 / gate as f64
        ),
    )
}

/// Neither read phase may have had its latency distribution censored by the
/// client's request deadline.
///
/// **The same class of hazard as `BloomEffectiveness`, and the same verdict.** A
/// read that blows the deadline records no latency at all, so the percentiles
/// are computed over whichever reads happened to finish in time. Run 1787054105
/// is the worked example: 11,729 of 12,000 cold reads timed out and the 271
/// survivors reported p50 4,811ms, p99 5,095ms and max 5,154ms — three numbers
/// all pinned against the 5s deadline, none of them the cost of a cold read.
/// That is not a slow result, it is the absence of a result, and the cost of a
/// cold read is this scenario's entire deliverable.
///
/// INCONCLUSIVE rather than FAIL: the database did nothing wrong, the harness
/// stopped looking too early. INCONCLUSIVE rather than PASS: a censored curve
/// reads exactly like a measured one, which is the worst available outcome.
///
/// A phase with no ops at all is INCONCLUSIVE for the same reason — "no reads
/// timed out" over zero reads is not evidence the curve is trustworthy.
pub fn read_latency_uncensored(
    curves: &[(&str, &celeriant_bench::read_workload::ReheatCurveJson)],
    deadline: Duration,
) -> CheckResult {
    const NAME: &str = "ReadLatencyUncensored";
    let ops = |c: &celeriant_bench::read_workload::ReheatCurveJson| -> u64 {
        c.buckets.iter().map(|b| b.ops).sum()
    };
    let mut censored = Vec::new();
    let mut empty = Vec::new();
    let mut detail = Vec::new();
    for (phase, curve) in curves {
        let (t, n) = (curve.request_timeouts(), ops(curve));
        detail.push(format!("{phase}: {n} timed reads, {t} request timeout(s)"));
        if t > 0 {
            censored.push(format!("{phase} lost {t} read(s) to the deadline"));
        } else if n == 0 {
            empty.push(*phase);
        }
    }
    let detail = format!(
        "client request deadline {:.0}s — {}",
        deadline.as_secs_f64(),
        detail.join("; ")
    );
    if !censored.is_empty() {
        return CheckResult::inconclusive(
            NAME,
            format!(
                "{detail}. CENSORED AT THE CLIENT DEADLINE: {}. A timed-out read contributes no \
                 latency sample, so every p50/p99/max in that curve is a LOWER BOUND over the \
                 reads that finished in time, and the cold/warm ratio is arithmetic over an \
                 unknown. Raise the read deadline and re-run before quoting any of it",
                censored.join(", ")
            ),
        );
    }
    if !empty.is_empty() {
        return CheckResult::inconclusive(
            NAME,
            format!(
                "{detail}. No read completed in {} — zero timeouts over zero reads is not \
                 evidence that the curve is trustworthy",
                empty.join(", ")
            ),
        );
    }
    CheckResult::pass_with_detail(
        NAME,
        format!("{detail} — no read was cut off, so the percentiles are the distribution"),
    )
}

/// Nothing — read or write — came back carrying another request's correlation id.
///
/// goal.md's complaint about the previous chaos harness was that the error
/// variants proving stream desync were "recorded and never read", naming both
/// the read and the write side. This is the assertion that reads them.
///
/// Unlike every other check here it has no threshold and no tolerance: load,
/// partitions, restarts and S3 outages are all modelled faults that cannot
/// produce a misbound response. One is a defect.
///
/// Zero mismatches over zero operations is not evidence, so a phase that did no
/// work at all is INCONCLUSIVE rather than a green tick — and that is judged per
/// phase, because a run whose cold phase did nothing must not inherit a pass
/// from the fill phase.
pub fn no_correlation_mismatches(
    curves: &[(&str, &celeriant_bench::read_workload::ReheatCurveJson)],
    fill_writes: Option<(u64, u64)>,
) -> CheckResult {
    const NAME: &str = "NoCorrelationMismatch";
    let mut offenders = Vec::new();
    let mut silent = Vec::new();
    let mut total = 0u64;

    for (phase, curve) in curves {
        let ops: u64 = curve.buckets.iter().map(|b| b.ops).sum();
        total += ops;
        let n = curve.correlation_mismatches();
        if n > 0 {
            offenders.push(format!("{phase} reads: {n}"));
        } else if ops == 0 {
            silent.push(*phase);
        }
    }
    if let Some((writes, mismatches)) = fill_writes {
        total += writes;
        if mismatches > 0 {
            offenders.push(format!("fill writes: {mismatches}"));
        } else if writes == 0 {
            silent.push("phase 1 (fill writes)");
        }
    }

    if !offenders.is_empty() {
        return CheckResult::fail(
            NAME,
            format!(
                "operation(s) returned a response bound to a different request ({}). This is \
                 stream desynchronisation, not load — no injected fault in this harness can \
                 cause it",
                offenders.join(", ")
            ),
        );
    }
    if !silent.is_empty() {
        return CheckResult::inconclusive(
            NAME,
            format!(
                "{} did no work — zero mismatches over zero operations tests nothing there, so \
                 the {total} operation(s) elsewhere do not vouch for it",
                silent.join(", ")
            ),
        );
    }
    CheckResult::pass_with_detail(NAME, format!("{total} operation(s), none misbound"))
}

/// Peak RSS against the declared budget. Report-only on the first run: the
/// question is honesty, not survival, and nobody has measured what this does at
/// this population size.
pub fn honest_memory_budget(store_peak_kb: u64, declared_budget_bytes: u64, node_ram_bytes: u64) -> CheckResult {
    const NAME: &str = "HonestMemoryBudget";
    if store_peak_kb == 0 {
        return CheckResult::inconclusive(NAME, "no RSS samples collected".to_string());
    }
    let peak = store_peak_kb * 1024;
    let pct_of_ram = 100.0 * peak as f64 / node_ram_bytes.max(1) as f64;
    CheckResult::pass_with_detail(
        NAME,
        format!(
            "peak RSS {:.2} GB vs declared budget {:.2} GB ({:.1}x); {:.1}% of node RAM",
            peak as f64 / 1e9,
            declared_budget_bytes as f64 / 1e9,
            peak as f64 / declared_budget_bytes.max(1) as f64,
            pct_of_ram,
        ),
    )
}

/// The segment-summary sidecar high-water against the soft 4 MiB cap.
///
/// `trim_out_client_sets` degrades client sets to `Unknown` but never drops
/// entries, so the cap does not bound the file. A value far above 4 MiB is that
/// cap failing where anyone can see it.
pub fn sidecar_high_water(samples: &[NodeSample], hosts: &[String]) -> CheckResult {
    const NAME: &str = "SidecarHighWater";
    const SOFT_CAP: u64 = 4 * 1024 * 1024;
    if !metric_present(samples, "celeriant_segment_summary_max_bytes") {
        return CheckResult::inconclusive(NAME, "no segment sealed — sidecar size never observed".to_string());
    }
    let mut worst = 0u64;
    let mut worst_aggs = 0u64;
    for h in hosts {
        if let Some(s) = last_ok(samples, h) {
            worst = worst.max(s.segment_summary_max_bytes);
            worst_aggs = worst_aggs.max(s.segment_summary_max_aggregates);
        }
    }
    let detail = format!(
        "largest sealed sidecar {:.2} MiB across {worst_aggs} aggregates (soft cap {:.0} MiB)",
        worst as f64 / (1024.0 * 1024.0),
        SOFT_CAP as f64 / (1024.0 * 1024.0)
    );
    if worst > SOFT_CAP {
        CheckResult::pass_with_detail(NAME, format!("{detail} — OVER the soft cap, as predicted"))
    } else {
        CheckResult::pass_with_detail(NAME, detail)
    }
}

// ===========================================================================
// Defect reproductions: write_outage_selfheal and promotion_failure_survival
// ===========================================================================
//
// Every verdict below reads `celeriant_node_status_effective_code` and nothing
// else. `celeriant_node_status_code` and `celeriant_node_role` both read
// healthy on a leader rejecting 100% of writes, so a check built on them
// reports a green run through a total outage.

/// Shards on this node whose EFFECTIVE status is `Fenced`.
fn fenced_shards(s: &NodeSample) -> Vec<u32> {
    s.effective_status_by_shard
        .iter()
        .filter(|(_, code)| **code == STATUS_FENCED)
        .map(|(shard, _)| *shard)
        .collect()
}

fn effective_codes(s: &NodeSample) -> String {
    s.effective_status_by_shard
        .iter()
        .map(|(shard, code)| format!("{shard}={code}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Last healthy scrape of `host` at or after `from_ms`.
fn last_ok_since<'a>(samples: &'a [NodeSample], host: &str, from_ms: u64) -> Option<&'a NodeSample> {
    samples.iter().rev().find(|s| s.host == host && s.ok && s.t_ms >= from_ms)
}

/// DEFECT 1, the assertion that matters: **the absence of recovery**.
///
/// All client load has stopped and `since_load_stop` has passed. Any shard on
/// either node still reading `effective == Fenced` means the lease/fencing
/// state machine never re-acquired a valid lease on its own — a liveness bug,
/// not overload. A run that merely shows write errors under load proves
/// overload; this is the part only a restart cleared in the field.
///
/// Absence is INCONCLUSIVE, never a pass: this is a named-value read, and a
/// gauge that never appeared cannot be evidence that no shard is fenced.
pub fn write_outage_self_healed(
    samples: &[NodeSample],
    hosts: &[String],
    settle_start_ms: u64,
    since_load_stop: Duration,
) -> CheckResult {
    const NAME: &str = "WriteOutageSelfHealed";
    let mut per_host = Vec::new();
    let mut fenced = Vec::new();
    let mut saw_gauge = false;

    for h in hosts {
        let Some(s) = last_ok_since(samples, h, settle_start_ms) else {
            return CheckResult::inconclusive(
                NAME,
                format!("no healthy scrape of {h} after load stopped — the settle window observed nothing"),
            );
        };
        if s.effective_status_by_shard.is_empty() {
            per_host.push(format!("{h}: effective gauge absent"));
            continue;
        }
        saw_gauge = true;
        per_host.push(format!("{h}: effective [{}]", effective_codes(s)));
        for shard in fenced_shards(s) {
            fenced.push(format!("{h} shard {shard}"));
        }
    }

    let detail = format!(
        "{:.0}s after all client load stopped — {}",
        since_load_stop.as_secs_f64(),
        per_host.join("; "),
    );
    if !saw_gauge {
        return CheckResult::inconclusive(
            NAME,
            format!(
                "{detail}. celeriant_node_status_effective_code never appeared, and the raw \
                 gauges read healthy through a total write outage — nothing here can be read \
                 as evidence of recovery"
            ),
        );
    }
    if fenced.is_empty() {
        CheckResult::pass_with_detail(NAME, format!("{detail} — no shard left fenced"))
    } else {
        CheckResult::fail(
            NAME,
            format!(
                "{detail}. STILL FENCED with zero load: {}. The wedge does not self-heal; \
                 only a restart cleared it in the field",
                fenced.join(", ")
            ),
        )
    }
}

/// Whether the wedge this scenario exists to observe actually formed.
///
/// Scaffolding, not a defect assertion. A run where the cluster never wedged
/// leaves `WriteOutageSelfHealed` trivially green, and reading that as "defect
/// fixed" is the one mistake this scenario must not enable — so a no-wedge run
/// is INCONCLUSIVE and says so with the numbers that make it diagnosable.
pub fn wedge_formed_during_load(
    samples: &[NodeSample],
    start_ms: u64,
    end_ms: u64,
    writes: u64,
    errors: u64,
) -> CheckResult {
    const NAME: &str = "WedgeFormedDuringLoad";
    let mut per_tick: BTreeMap<u64, usize> = BTreeMap::new();
    let mut saw_gauge = false;
    for s in samples.iter().filter(|s| s.ok && s.t_ms >= start_ms && s.t_ms <= end_ms) {
        if s.effective_status_by_shard.is_empty() {
            continue;
        }
        saw_gauge = true;
        *per_tick.entry(s.t_ms).or_insert(0) += fenced_shards(s).len();
    }
    if !saw_gauge {
        return CheckResult::inconclusive(
            NAME,
            format!(
                "celeriant_node_status_effective_code never appeared during the load window \
                 (writes {writes}, errors {errors}) — whether the cluster wedged is unknown, \
                 not disproved"
            ),
        );
    }
    let peak = per_tick.values().copied().max().unwrap_or(0);
    let detail = format!("writes {writes}, errors {errors}, peak fenced shards {peak}");
    if peak == 0 {
        CheckResult::inconclusive(
            NAME,
            format!(
                "WEDGE DID NOT FORM ({detail}). WriteOutageSelfHealed had no outage to recover \
                 from, so its PASS is not evidence the defect is fixed — check this line before \
                 reading this run as green"
            ),
        )
    } else {
        CheckResult::pass_with_detail(NAME, format!("wedge formed under load ({detail})"))
    }
}

/// Mesh handlers that entered at one instant and never returned.
///
/// `celeriant_intrashard_handler_started_at_ms` is zero while a loop is idle,
/// so `NodeSample::stuck_handlers` already carries only the entered ones. An
/// entry whose stamp is IDENTICAL in the first and last sample of the window
/// has been inside the same handler for the whole window, and its label set
/// names which arm — an open investigation question, so this is diagnostic
/// output rather than a verdict.
pub fn parked_handlers(samples: &[NodeSample], host: &str, from_ms: u64) -> Vec<String> {
    let first = samples.iter().find(|s| s.host == host && s.ok && s.t_ms >= from_ms);
    let last = last_ok_since(samples, host, from_ms);
    let (Some(first), Some(last)) = (first, last) else { return Vec::new() };
    if first.t_ms == last.t_ms {
        return Vec::new();
    }
    let mut parked: Vec<String> = last
        .stuck_handlers
        .iter()
        .filter(|entry| first.stuck_handlers.contains(entry))
        .cloned()
        .collect();
    parked.sort();
    parked
}

/// Largest per-shard WAL gap between the leader's view and the follower's.
///
/// Only shards present in BOTH scrapes count: a shard the follower has not
/// registered yet would otherwise read as a gap the size of the leader's whole
/// WAL, and the kill gate would fire on a bookkeeping artifact.
pub fn follower_lag(leader: &NodeSample, follower: &NodeSample) -> u64 {
    leader
        .wal_seq_by_shard
        .iter()
        .filter_map(|(shard, l)| follower.wal_seq_by_shard.get(shard).map(|f| l.saturating_sub(*f)))
        .max()
        .unwrap_or(0)
}

/// DEFECT 2, red condition B: a celeriant process found dead when it should be up.
///
/// `HostSample::vm_rss_kb` is 0 exactly when the unit has no main PID, so a
/// healthy poll tick reading zero RSS is the process being gone. The killed
/// node is exempt for its own kill→restart window; everything else must stay up.
pub fn processes_stayed_up(
    samples: &[HostSample],
    exempt_host: &str,
    exempt_from_ms: u64,
    exempt_to_ms: u64,
) -> CheckResult {
    const NAME: &str = "CeleriantProcessesStayedUp";
    if !samples.iter().any(|s| s.ok) {
        return CheckResult::inconclusive(
            NAME,
            "no healthy host poll ticks — process liveness was never observed".to_string(),
        );
    }
    let mut down: BTreeMap<&str, (u64, u64, usize)> = BTreeMap::new();
    for s in samples.iter().filter(|s| s.ok && s.vm_rss_kb == 0) {
        let exempt =
            s.host == exempt_host && s.t_ms >= exempt_from_ms && s.t_ms <= exempt_to_ms;
        if exempt {
            continue;
        }
        let e = down.entry(s.host.as_str()).or_insert((s.t_ms, s.t_ms, 0));
        e.0 = e.0.min(s.t_ms);
        e.1 = e.1.max(s.t_ms);
        e.2 += 1;
    }
    if down.is_empty() {
        return CheckResult::pass_with_detail(
            NAME,
            format!(
                "every node had a live celeriant process across {} poll ticks \
                 (exempt: {exempt_host} {exempt_from_ms}..{exempt_to_ms}ms)",
                samples.iter().filter(|s| s.ok).count()
            ),
        );
    }
    let detail = down
        .iter()
        .map(|(h, (from, to, n))| format!("{h}: no process for {n} tick(s), {from}..{to}ms"))
        .collect::<Vec<_>>()
        .join("; ");
    CheckResult::fail(
        NAME,
        format!("{detail} — a celeriant unit was dead outside its own kill window"),
    )
}

/// Peak data-filesystem occupancy against the watchdog high-water.
pub fn disk_watchdog(store: &HostPollStore, high_water_pct: u64) -> CheckResult {
    const NAME: &str = "DiskWatchdog";
    let peak = store.max_data_fs_used_pct();
    let detail = format!("peak data filesystem {peak}% used (high-water {high_water_pct}%)");
    if peak == 0 {
        CheckResult::inconclusive(NAME, "no disk samples collected".to_string())
    } else if peak >= high_water_pct {
        // Stopping on the space budget is a clean, reported outcome, not a fault.
        CheckResult::pass_with_detail(NAME, format!("{detail} — fill stopped on the space budget"))
    } else {
        CheckResult::pass_with_detail(NAME, detail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(host: &str, t_ms: u64) -> NodeSample {
        NodeSample { host: host.into(), t_ms, ok: true, ..Default::default() }
    }

    #[test]
    fn a_stalled_birth_process_fails_rather_than_passing_quietly() {
        // A population that stopped growing collapses the non-stationary model
        // to the stationary one; every age bucket then measures the same thing.
        let growing = [(0, 10, 10), (1000, 50, 50), (2000, 90, 90)];
        assert!(cardinality_grew(&growing).passed());
        let stalled = [(0, 10, 10), (1000, 50, 50), (2000, 50, 60)];
        assert!(cardinality_grew(&stalled).failed());
        // Too few samples is not evidence of a stall.
        assert!(cardinality_grew(&[(0, 1, 1)]).is_inconclusive());
    }

    #[test]
    fn no_correlation_mismatches_has_no_tolerance_and_no_free_pass() {
        use celeriant_bench::population::AgeBucket;
        use celeriant_bench::read_workload::{ReadErrorKind, ReheatCostCurve};
        use crate::invariants::CheckOutcome;

        let curve = |errs: &[ReadErrorKind], ops: u64| {
            let mut c = ReheatCostCurve::new("c", 1);
            for _ in 0..ops {
                c.record(AgeBucket::FiveMin, 1_000, 64);
            }
            for e in errs {
                c.record_error(AgeBucket::FiveMin, *e);
            }
            c.to_json()
        };
        let writes = Some((10u64, 0u64));

        let clean = curve(&[], 10);
        assert_eq!(
            no_correlation_mismatches(&[("p", &clean)], writes).outcome,
            CheckOutcome::Pass
        );

        // One is enough. No injected fault in this harness can produce it.
        let dirty = curve(&[ReadErrorKind::CorrelationMismatch], 10);
        assert_eq!(
            no_correlation_mismatches(&[("p", &dirty)], writes).outcome,
            CheckOutcome::Fail,
            "one misbound read must fail the run"
        );

        // The write side counts too — goal.md names both.
        assert_eq!(
            no_correlation_mismatches(&[("p", &clean)], Some((10, 1))).outcome,
            CheckOutcome::Fail,
            "one misbound write must fail the run"
        );

        // Other error kinds are load artefacts and must not trip it.
        let busy = curve(&[ReadErrorKind::ServerBusy, ReadErrorKind::Wire], 10);
        assert_eq!(
            no_correlation_mismatches(&[("p", &busy)], writes).outcome,
            CheckOutcome::Pass
        );

        // Zero over zero is not evidence — judged per phase, so a silent phase
        // cannot inherit a pass from a busy one.
        let empty = curve(&[], 0);
        assert_eq!(
            no_correlation_mismatches(&[("busy", &clean), ("silent", &empty)], writes).outcome,
            CheckOutcome::Inconclusive,
            "a phase that did no work must not be vouched for by another phase"
        );
        assert_eq!(
            no_correlation_mismatches(&[("p", &clean)], Some((0, 0))).outcome,
            CheckOutcome::Inconclusive,
            "zero writes is equally unevidenced"
        );
    }

    #[test]
    fn an_unreached_regime_is_inconclusive_not_failed() {
        // These are properties of how long the run went, not defects in the
        // database. `smoke` trips all of them by construction.
        assert!(age_spread_reached(2, 5).is_inconclusive());
        assert!(age_spread_reached(3, 5).passed());
        assert!(rotations_reached(&[(1, 12), (2, 3)]).is_inconclusive());
        assert!(rotations_reached(&[(1, 12), (2, 10)]).passed());
        assert!(rotations_reached(&[]).is_inconclusive());
    }

    #[test]
    fn a_missing_denominator_never_reads_as_perfect_effectiveness() {
        // The worst available failure mode: a renamed metric or a stale binary
        // rendering as a flawless bloom. Proven reachable in the field —
        // read_segments_walked_total was genuinely absent from cs1's /metrics.
        let hosts = vec!["a".to_string()];
        let mut s = sample("a", 0);
        s.read_bloom_short_circuit_total = 100;
        let r = bloom_effectiveness(&[s.clone()], &hosts);
        assert!(r.is_inconclusive(), "{}", r.detail);
        assert!(!r.passed());

        // Present but zero is equally undefined.
        let mut z = sample("a", 0);
        z.metric_keys_present = std::sync::Arc::new(["celeriant_read_bloom_gate_total".to_string()].into_iter().collect());
        assert!(bloom_effectiveness(&[z], &hosts).is_inconclusive());

        // Present and non-zero reports the real ratio.
        let mut ok = sample("a", 0);
        ok.metric_keys_present = std::sync::Arc::new(["celeriant_read_bloom_gate_total".to_string()].into_iter().collect());
        ok.read_bloom_gate_total = 40;
        ok.read_bloom_short_circuit_total = 30;
        let r = bloom_effectiveness(&[ok], &hosts);
        assert!(r.passed() && r.detail.contains("75.0%"), "{}", r.detail);
    }

    #[test]
    fn a_censored_read_curve_is_inconclusive_in_either_phase() {
        use celeriant_bench::population::AgeBucket;
        use celeriant_bench::read_workload::{ReadErrorKind, ReheatCostCurve};

        // `ops` reads that finished plus `timeouts` reads the client cut off —
        // the exact shape run 1787054105 produced on the cold side.
        let curve = |ops: u64, timeouts: u64| {
            let mut c = ReheatCostCurve::new("x", 1);
            for _ in 0..ops {
                c.record(AgeBucket::FiveMin, 4_811_000, 4_096);
            }
            for _ in 0..timeouts {
                c.record_error(AgeBucket::FiveMin, ReadErrorKind::RequestTimeout);
            }
            c.to_json()
        };
        let verdict = |warm: (u64, u64), cold: (u64, u64)| {
            let (w, c) = (curve(warm.0, warm.1), curve(cold.0, cold.1));
            read_latency_uncensored(
                &[("phase 3 (warm)", &w), ("phase 5 (cold)", &c)],
                Duration::from_secs(130),
            )
        };

        // Nothing cut off: the percentiles are the distribution.
        assert!(verdict((12_000, 0), (12_000, 0)).passed());

        // Either side censored is INCONCLUSIVE — not FAIL, the database did
        // nothing wrong; not PASS, a censored curve renders like a measured one.
        for (warm, cold, phase, count) in [
            ((12_000, 0), (271, 11_729), "phase 5 (cold)", "11729"),
            ((240, 60), (12_000, 0), "phase 3 (warm)", "60"),
        ] {
            let r = verdict(warm, cold);
            assert!(r.is_inconclusive() && !r.passed() && !r.failed(), "{}", r.detail);
            assert!(r.detail.contains(phase) && r.detail.contains(count), "{}", r.detail);
            assert!(r.detail.contains("LOWER BOUND"), "{}", r.detail);
        }

        // A wire error is not censoring: the read got an answer it did not like,
        // and every latency it did record is real.
        let mut wire = ReheatCostCurve::new("x", 1);
        wire.record(AgeBucket::FiveMin, 1_000, 8);
        wire.record_error(AgeBucket::FiveMin, ReadErrorKind::Wire);
        let clean = curve(10, 0);
        let r = read_latency_uncensored(
            &[("phase 3 (warm)", &wire.to_json()), ("phase 5 (cold)", &clean)],
            Duration::from_secs(130),
        );
        assert!(r.passed(), "{}", r.detail);

        // Zero timeouts over zero reads is not evidence the curve is sound.
        assert!(verdict((0, 0), (12_000, 0)).is_inconclusive());
    }

    #[test]
    fn must_be_zero_checks_treat_absence_as_the_zero_they_assert() {
        // Opposite rule to the denominator case: these counters register
        // lazily, so absent means never fired, which is the assertion holding.
        let hosts = vec!["a".to_string()];
        assert!(no_rotation_enospc(&[sample("a", 0)], &hosts).passed());
        assert!(no_bloom_absent_segments(&[sample("a", 0)], &hosts).passed());

        let mut bad = sample("a", 0);
        bad.rotation_out_of_space_total = 3;
        assert!(no_rotation_enospc(&[bad], &hosts).failed());

        let mut blind = sample("a", 0);
        blind.read_bloom_absent_total = 1;
        assert!(no_bloom_absent_segments(&[blind], &hosts).failed());
    }

    #[test]
    fn checks_read_the_last_healthy_sample_not_a_failed_scrape() {
        // Nodes are killed and restarted mid-scenario; a trailing unreachable
        // scrape must not zero out a counter that did fire.
        let hosts = vec!["a".to_string()];
        let mut hit = sample("a", 0);
        hit.rotation_out_of_space_total = 7;
        let dead = NodeSample::unreachable("a".into(), 1, "connection refused".into());
        assert!(no_rotation_enospc(&[hit, dead], &hosts).failed());
    }

    // ---- defect reproductions -------------------------------------------

    fn hosts() -> Vec<String> {
        vec!["cs1".to_string(), "cs2".to_string()]
    }

    /// A node scraped with the field's exact reading: every raw gauge healthy,
    /// `effective` fenced on the data shards.
    fn wedged(host: &str, t_ms: u64) -> NodeSample {
        let mut s = sample(host, t_ms);
        s.node_role = 1.0;
        s.node_status_code_by_shard = [(0, 4), (1, 4), (2, 4), (3, 4)].into_iter().collect();
        s.effective_status_by_shard = [(0, 4), (1, 5), (2, 5), (3, 5)].into_iter().collect();
        s
    }

    fn healthy(host: &str, t_ms: u64, code: u64) -> NodeSample {
        let mut s = sample(host, t_ms);
        s.effective_status_by_shard = (0..4).map(|i| (i, code)).collect();
        s
    }

    #[test]
    fn a_shard_still_fenced_after_the_settle_window_is_the_defect() {
        // The assertion is the ABSENCE OF RECOVERY: zero load, 120s elapsed,
        // shards still Fenced. Only a restart cleared this in the field.
        let samples = vec![wedged("cs1", 5_000), wedged("cs2", 5_000)];
        let r = write_outage_self_healed(&samples, &hosts(), 1_000, Duration::from_secs(120));
        assert!(r.failed(), "{}", r.detail);
        assert!(r.detail.contains("cs1 shard 1"), "{}", r.detail);
        assert!(r.detail.contains("cs2 shard 3"), "{}", r.detail);
        assert!(r.detail.contains("120s"), "{}", r.detail);

        let recovered = vec![healthy("cs1", 5_000, 4), healthy("cs2", 5_000, 1)];
        assert!(write_outage_self_healed(&recovered, &hosts(), 1_000, Duration::from_secs(120)).passed());
    }

    #[test]
    fn the_verdict_reads_the_effective_gauge_and_nothing_else() {
        // node_status_code=4 and node_role=1 on every shard while the effective
        // gauge is missing. Trusting the raw pair here is exactly the trap that
        // produced a confident wrong diagnosis in the field, so absence must be
        // INCONCLUSIVE — a named-value read, not a must-be-zero counter.
        let mut s = sample("cs1", 5_000);
        s.node_role = 1.0;
        s.node_status_code_by_shard = [(1, 4), (2, 4), (3, 4)].into_iter().collect();
        let samples = vec![s, sample("cs2", 5_000)];
        let r = write_outage_self_healed(&samples, &hosts(), 1_000, Duration::from_secs(120));
        assert!(r.is_inconclusive(), "{}", r.detail);
        assert!(!r.passed());
    }

    #[test]
    fn a_settle_window_that_observed_nothing_is_inconclusive() {
        // Every scrape predates the load stop: nothing describes the settled
        // cluster, and the last pre-stop sample must not stand in for one.
        let samples = vec![wedged("cs1", 500), wedged("cs2", 500)];
        assert!(
            write_outage_self_healed(&samples, &hosts(), 1_000, Duration::from_secs(120))
                .is_inconclusive()
        );
    }

    #[test]
    fn a_run_that_never_wedged_is_annunciated_rather_than_passed() {
        // The green nobody may read as "defect fixed" without checking.
        let clean = vec![healthy("cs1", 2_000, 4), healthy("cs2", 2_000, 1)];
        let r = wedge_formed_during_load(&clean, 0, 10_000, 215_757, 0);
        assert!(r.is_inconclusive(), "{}", r.detail);
        assert!(r.detail.contains("WEDGE DID NOT FORM"), "{}", r.detail);
        assert!(r.detail.contains("writes 215757, errors 0, peak fenced shards 0"), "{}", r.detail);

        // Peak is per tick across both nodes: 3 fenced on cs1 + 2 on cs2.
        let mut cs2 = sample("cs2", 2_000);
        cs2.effective_status_by_shard = [(0, 1), (1, 5), (2, 1), (3, 5)].into_iter().collect();
        let wedge = vec![wedged("cs1", 2_000), cs2];
        let r = wedge_formed_during_load(&wedge, 0, 10_000, 53_392, 428_636);
        assert!(r.passed(), "{}", r.detail);
        assert!(r.detail.contains("peak fenced shards 5"), "{}", r.detail);
    }

    #[test]
    fn parked_handlers_names_only_the_arm_that_never_returned() {
        // Same label set AND same start stamp at both ends of the window means
        // the loop entered once and never came out. A handler that re-entered
        // carries a newer stamp and is not parked.
        let mut first = sample("cs1", 1_000);
        first.stuck_handlers = vec![
            "{src_shard=\"0\",shard_id=\"2\",kind=\"status_update\"}@1785377615123".to_string(),
            "{src_shard=\"1\",shard_id=\"2\",kind=\"probe\"}@1785377615000".to_string(),
        ];
        let mut last = sample("cs1", 90_000);
        last.stuck_handlers = vec![
            "{src_shard=\"0\",shard_id=\"2\",kind=\"status_update\"}@1785377615123".to_string(),
            "{src_shard=\"1\",shard_id=\"2\",kind=\"probe\"}@1785377690000".to_string(),
        ];
        let parked = parked_handlers(&[first, last], "cs1", 500);
        assert_eq!(parked, vec!["{src_shard=\"0\",shard_id=\"2\",kind=\"status_update\"}@1785377615123"]);
    }

    #[test]
    fn a_single_sample_cannot_prove_a_handler_is_parked() {
        let mut only = sample("cs1", 1_000);
        only.stuck_handlers = vec!["{kind=\"status_update\"}@1".to_string()];
        assert!(parked_handlers(&[only], "cs1", 500).is_empty());
    }

    #[test]
    fn follower_lag_ignores_a_shard_the_follower_has_not_registered() {
        let mut leader = sample("cs1", 0);
        leader.wal_seq_by_shard = [(1, 100), (2, 200), (3, 300)].into_iter().collect();
        let mut follower = sample("cs2", 0);
        // Shard 3 absent: counting it as zero would report a lag of 300.
        follower.wal_seq_by_shard = [(1, 81), (2, 200)].into_iter().collect();
        assert_eq!(follower_lag(&leader, &follower), 19);

        // A follower that is ahead on one shard does not produce a negative.
        let mut ahead = sample("cs2", 0);
        ahead.wal_seq_by_shard = [(1, 120), (2, 200), (3, 300)].into_iter().collect();
        assert_eq!(follower_lag(&leader, &ahead), 0);
    }

    #[test]
    fn a_dead_process_outside_its_kill_window_fails() {
        use crate::host_poll::HostSample;
        let tick = |host: &str, t_ms: u64, rss: u64| HostSample {
            host: host.into(),
            t_ms,
            ok: true,
            vm_rss_kb: rss,
            ..Default::default()
        };
        // cs1 was the killed node: down between 20s and 45s is the scenario.
        let killed = vec![
            tick("cs1", 10_000, 500_000),
            tick("cs1", 25_000, 0),
            tick("cs2", 25_000, 400_000),
            tick("cs1", 50_000, 500_000),
        ];
        assert!(processes_stayed_up(&killed, "cs1", 20_000, 45_000).passed());

        // The survivor going away is the defect: a panicked shard that hung
        // until systemd SIGKILLed it 90s later.
        let mut panicked = killed.clone();
        panicked.push(tick("cs2", 60_000, 0));
        let r = processes_stayed_up(&panicked, "cs1", 20_000, 45_000);
        assert!(r.failed(), "{}", r.detail);
        assert!(r.detail.contains("cs2"), "{}", r.detail);

        // A failed poll tick is not evidence of a dead process.
        let blind = vec![HostSample { host: "cs1".into(), t_ms: 1, ok: false, ..Default::default() }];
        assert!(processes_stayed_up(&blind, "cs1", 0, 0).is_inconclusive());
    }

    #[test]
    fn an_oversized_sidecar_is_reported_as_over_the_cap() {
        let hosts = vec!["a".to_string()];
        assert!(sidecar_high_water(&[sample("a", 0)], &hosts).is_inconclusive());

        let mut big = sample("a", 0);
        big.metric_keys_present = std::sync::Arc::new(["celeriant_segment_summary_max_bytes".to_string()].into_iter().collect());
        big.segment_summary_max_bytes = 110 * 1024 * 1024;
        big.segment_summary_max_aggregates = 1_000_000;
        let r = sidecar_high_water(&[big], &hosts);
        assert!(r.detail.contains("OVER the soft cap"), "{}", r.detail);
    }
}

/// Parsing of the service-config baseline that `run_cardinality_pressure`
/// restores on exit. Wrong values here silently reshape every later run, so the
/// parser refuses rather than guesses. Exercises the real function, not a copy.
#[cfg(test)]
mod service_baseline_tests {
    use crate::scenario::parse_service_baseline;

    const SAMPLE: &str = "\
# Celeriant tuning
MEMORY_CONSUMPTION_PERCENT=60
SHARD_LOG_PREALLOCATE_BYTES=268435456   # 256MB
RESERVE_COORDINATOR_SHARD=true
";

    #[test]
    fn reads_the_declared_baseline_including_a_trailing_comment() {
        assert_eq!(parse_service_baseline(SAMPLE), Some((60, 268_435_456)));
    }

    #[test]
    fn a_prefix_match_is_not_a_key_match() {
        // MEMORY_CONSUMPTION_PERCENT_EXTRA must not satisfy a lookup for
        // MEMORY_CONSUMPTION_PERCENT.
        let raw = "MEMORY_CONSUMPTION_PERCENT_EXTRA=99\nSHARD_LOG_PREALLOCATE_BYTES=1\n";
        assert_eq!(parse_service_baseline(raw), None);
    }

    #[test]
    fn a_missing_or_unparseable_value_yields_none_rather_than_a_default() {
        // None means "leave the units alone", which is the safe direction.
        assert_eq!(parse_service_baseline(""), None);
        assert_eq!(parse_service_baseline("MEMORY_CONSUMPTION_PERCENT=abc\nSHARD_LOG_PREALLOCATE_BYTES=1\n"), None);
        assert_eq!(parse_service_baseline("MEMORY_CONSUMPTION_PERCENT=60\n"), None);
    }
}

/// The `metric_keys_present` interning that keeps a long run's sample trace
/// affordable. Type alone proves nothing — an `Arc` per sample costs the same as
/// an owned set. What matters is that unchanged ticks SHARE one allocation.
#[cfg(test)]
mod interning_tests {
    use crate::sample::parse_metrics;
    use std::sync::Arc;

    fn body(extra: &str) -> String {
        format!("celeriant_writes_total 5\nceleriant_read_bloom_gate_total 7\n{extra}")
    }

    #[test]
    fn identical_key_sets_can_share_one_allocation() {
        // The scraper's rule: same set as last tick, reuse the previous Arc.
        let a = parse_metrics("h".into(), 0, &body(""));
        let mut b = parse_metrics("h".into(), 500, &body(""));
        assert_eq!(a.metric_keys_present, b.metric_keys_present, "premise: same names seen");

        b.metric_keys_present = Arc::clone(&a.metric_keys_present);
        assert!(
            Arc::ptr_eq(&a.metric_keys_present, &b.metric_keys_present),
            "unchanged ticks must share, not copy"
        );
        assert_eq!(Arc::strong_count(&a.metric_keys_present), 2);
    }

    #[test]
    fn a_newly_registered_counter_changes_the_set_and_must_not_be_shared() {
        // Counters register lazily, so the set genuinely grows mid-run. Sharing
        // through that change would make a metric look present before it was.
        let a = parse_metrics("h".into(), 0, &body(""));
        let b = parse_metrics("h".into(), 500, &body("celeriant_read_segments_walked_total 1\n"));
        assert_ne!(a.metric_keys_present, b.metric_keys_present);
        assert!(!a.metric_keys_present.contains("celeriant_read_segments_walked_total"));
        assert!(b.metric_keys_present.contains("celeriant_read_segments_walked_total"));
    }
}

/// The payload-round-trip escape hatch for workloads that do not write the
/// opaque bench's reconstructible bodies.
#[cfg(test)]
mod payload_verdict_tests {
    use crate::final_read::payload_verdict_for_test as verdict;

    #[test]
    fn an_all_json_miss_is_inconclusive_not_a_corruption_report() {
        // Every sample missed and every one is a JSON object: this check simply
        // does not apply to the workload. `total_verified` counts MATCHES, so a
        // total miss leaves it at 0 — the condition must key on that.
        let got: Vec<Vec<u8>> = (0..5).map(|i| format!(r#"{{"AmountCents":{i}}}"#).into_bytes()).collect();
        let r = verdict(0, 5, 0, &got);
        assert!(r.is_inconclusive(), "{}", r.detail);
    }

    #[test]
    fn a_partial_miss_still_fails() {
        // The shape real corruption takes. It must not hide behind the hatch.
        let got: Vec<Vec<u8>> = vec![br#"{"AmountCents":1}"#.to_vec()];
        assert!(verdict(9, 10, 0, &got).failed());
    }

    #[test]
    fn a_total_miss_of_non_json_still_fails() {
        let got: Vec<Vec<u8>> = vec![b"[t-1-s-99]".to_vec(), b"garbage".to_vec()];
        assert!(verdict(0, 2, 0, &got).failed());
    }
}
