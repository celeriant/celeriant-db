use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;

use celeriant_chaos::actions::find_project_root;
use celeriant_chaos::config::ClusterConfig;
use celeriant_chaos::report::{RunDir, write_run_report, write_scenario};
use celeriant_chaos::scenario::{
    DefectParams, run_promotion_failure_survival, run_write_outage_selfheal,
    ScenarioParams, ScenarioReport, run_baseline, run_bridge, run_cas_storm_scenario,
    run_clock_scrambler, run_duplicate_replay,
    run_follower_graceful_stop, run_follower_sigkill, run_single_node_isolation,
    run_idempotency_audit_baseline, run_idempotency_audit_minio_outage,
    run_idempotency_audit_partition_then_kill_minio, run_idempotency_audit_fast_blackout,
    run_leader_graceful_stop, run_leader_restart_loop, run_leader_sigkill,
    run_bench_load_sweep, run_clock_skew_follower, run_follower_disk_full,
    run_minio_outage_long, run_minio_outage_short, run_network_flap,
    run_partition_asymmetric, run_partition_then_kill_minio,
    run_rolling_restart, run_sigstop_leader,
    run_partition_leader_follower_replication, run_partition_leader_minio,
    run_watch_storm, run_watch_storm_failover,
    run_cold_segment_reads, run_nemesis_composition, run_schema_under_partition,
    run_cardinality_pressure,
};
use celeriant_chaos::cardinality_workload::{CardinalityParams, Preset};

#[derive(Parser)]
#[command(name = "celeriant-chaos", about = "Chaos test orchestrator for the RPi cluster")]
struct Args {
    /// Run the full suite in order. Without it, only baseline runs.
    #[arg(long)]
    full: bool,

    /// Run one scenario by name. See README for the list; a few sit outside
    /// --full, including the defect reproductions `write_outage_selfheal` and
    /// `promotion_failure_survival`.
    #[arg(long)]
    scenario: Option<String>,

    /// Concurrent bench tasks.
    #[arg(long, default_value = "4000")]
    tasks: usize,

    /// Spread bench task starts over this many seconds (baseline scenario
    /// only). Default: off — the cold-connect herd is part of the test.
    #[arg(long)]
    connect_ramp: Option<u64>,

    /// Bench duration in seconds.
    #[arg(long, default_value = "60")]
    duration: u64,

    /// Minimum sustained throughput (req/s) for the BenchThroughputFloor check.
    #[arg(long, default_value = "500")]
    throughput_floor: f64,

    /// Deploy directory: must contain config.env, Makefile, and certs/. Resolved
    /// relative to the workspace root unless absolute. Defaults to deploy/rpi-cluster.
    #[arg(long, default_value = "deploy/rpi-cluster")]
    deploy_dir: PathBuf,

    /// SCEN-19 soak mode: repeat the scenario set in a loop for at least this
    /// many seconds. Each iteration is its own run directory. Exits after the
    /// first iteration whose elapsed time pushes the total past `--soak`, or
    /// immediately on any iteration failure. 0 (default) disables soak mode
    /// and runs a single pass. Example: `--soak 86400` for 24 hours.
    #[arg(long, default_value = "0")]
    soak: u64,

    /// In soak mode, continue even if an iteration fails (record the failure
    /// and proceed to the next iteration). Without this flag, the first
    /// failing iteration aborts the soak. Has no effect outside soak mode.
    #[arg(long)]
    soak_continue_on_failure: bool,

    /// Seed driving nemesis fault schedules, clock-skew jitter, and oracle
    /// sample selection. Default: derived from wall-clock entropy so every
    /// run explores a different fault plan (N3). Fix it to replay a run
    /// exactly, e.g. after triaging a Heisenbug from a printed seed.
    #[arg(long)]
    seed: Option<u64>,

    // --- cardinality_pressure only ---
    /// Fill budget for `cardinality_pressure`: smoke (10min), short (1h),
    /// deep (5h). The oldest dormancy age the run can measure is bounded by
    /// how long the fill ran, so this is not just a duration knob — `deep`
    /// reaches a part of the reheat curve `short` cannot.
    #[arg(long, default_value = "smoke")]
    preset: String,

    /// Override the preset's fill budget, in seconds.
    #[arg(long)]
    fill_duration: Option<u64>,

    /// Stop the fill when the data filesystem crosses this percentage. A clock
    /// without a disk watchdog is how a fast day produces ENOSPC instead of a
    /// result.
    #[arg(long, default_value = "70")]
    disk_high_water: u64,

    /// `shard_log_preallocate_bytes` applied to both nodes. Everything in the
    /// segment-summary pipeline scales linearly with it.
    #[arg(long, default_value = "268435456")]
    segment_bytes: u64,

    /// `MEMORY_CONSUMPTION_PERCENT` applied to both nodes.
    #[arg(long, default_value = "20")]
    memory_percent: u64,

    /// Target aggregates per segment. The payload mix is DERIVED from this and
    /// `--segment-bytes`, never passed raw: bloom load is set by the mix
    /// relative to the segment size, so holding the mix constant across two
    /// segment sizes makes the two stages incomparable.
    #[arg(long, default_value = "200000")]
    target_aggs_per_segment: u64,

    /// R distinct replicas per account in the contention phase.
    #[arg(long, default_value = "8")]
    contention_factor: usize,

    /// New aggregates per second, CLUSTER-WIDE, divided across `--tasks`.
    /// Never per-task: otherwise a 16k-connection run mints sixteen times
    /// faster than a 1k-connection run and their reheat curves are not
    /// comparable.
    #[arg(long, default_value = "50")]
    birth_rate: f64,

    /// Run the SIGKILL failover phase in `cardinality_pressure`.
    ///
    /// Off by default. Failover under a behind-follower is a separate confirmed
    /// defect with its own red test (`failover_pressure_matrix`), and it panics
    /// a shard — so leaving it on makes every run FAIL for reasons unrelated to
    /// memory or cardinality. Phase 4's graceful stop/start of both nodes still
    /// runs, so the cold-restart delta is unaffected.
    #[arg(long)]
    failover: bool,

    /// Reheat probes per second, cluster-wide, divided across `--tasks`.
    #[arg(long, default_value = "5")]
    reheat_rate: f64,

    // --- write_outage_selfheal / promotion_failure_survival only ---
    //
    // Separate from `--tasks` and `--birth-rate` on purpose. Both defects were
    // observed at ONE load shape, and these scenarios are reproductions of that
    // shape — inheriting the suite-wide defaults would silently run them at a
    // load neither defect has ever been seen at.
    /// Fill tasks for the defect scenarios. Multiple of 3. Default 3000 — the
    /// connection count the wedge formed at.
    #[arg(long)]
    defect_tasks: Option<usize>,

    /// New aggregates per second, cluster-wide, for the defect scenarios.
    /// Default 400 — the birth rate the wedge formed at.
    #[arg(long)]
    defect_birth_rate: Option<f64>,

    /// Seconds of load before `write_outage_selfheal` stops it, and the floor
    /// for `promotion_failure_survival`'s observation window. Default 120 —
    /// the field wedge formed inside 60.
    #[arg(long)]
    defect_load_secs: Option<u64>,

    /// Idle seconds `write_outage_selfheal` watches after ALL load stops.
    /// Default 120, floored at 90 — the field already disproved recovery over
    /// 90 seconds, so a shorter window cannot say anything new.
    #[arg(long)]
    defect_settle_secs: Option<u64>,
}

/// Entropy source for the default seed: no `rand` crate dependency here, so
/// mix wall-clock nanos with `RandomState`'s per-process random keys (both
/// vary run-to-run without needing a PRNG library).
fn entropy_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(nanos);
    hasher.finish()
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = Args::parse();
    let project_root = find_project_root();
    let deploy_dir = if args.deploy_dir.is_absolute() {
        args.deploy_dir.clone()
    } else {
        project_root.join(&args.deploy_dir)
    };
    let cfg = ClusterConfig::load(deploy_dir)?;

    let seed = args.seed.unwrap_or_else(entropy_seed);

    println!("=== celeriant-chaos ===");
    println!("  project root: {}", project_root.display());
    println!("  leader:       {}", cfg.leader_host);
    println!("  follower:     {}", cfg.follower_host);
    println!("  infra:        {}", cfg.infra_host.as_deref().unwrap_or("(none — managed by deploy)"));
    println!("  tasks:        {}", args.tasks);
    println!("  duration:     {}s", args.duration);
    println!("  seed:         {seed:#x}  (reproduce with --seed {seed})");
    println!();

    let params = ScenarioParams {
        tasks: args.tasks,
        duration_secs: args.duration,
        throughput_floor: args.throughput_floor,
        connect_ramp_secs: args.connect_ramp,
        seed,
    };

    let card = CardinalityParams {
        failover_phase: args.failover,
        preset: Preset::parse(&args.preset)?,
        fill_duration: args.fill_duration.map(std::time::Duration::from_secs),
        disk_high_water_pct: args.disk_high_water,
        segment_bytes: args.segment_bytes,
        memory_percent: args.memory_percent,
        target_aggs_per_segment: args.target_aggs_per_segment,
        contention_factor: args.contention_factor,
        birth_rate_per_sec: args.birth_rate,
        reheat_rate_per_sec: args.reheat_rate,
    };

    let defect_defaults = DefectParams::default();
    let defect = DefectParams {
        tasks: args.defect_tasks.unwrap_or(defect_defaults.tasks),
        birth_rate_per_sec: args.defect_birth_rate.unwrap_or(defect_defaults.birth_rate_per_sec),
        load_secs: args.defect_load_secs.unwrap_or(defect_defaults.load_secs),
        settle_secs: args.defect_settle_secs.unwrap_or(defect_defaults.settle_secs),
    };

    let scenarios_to_run: Vec<&str> = match args.scenario.as_deref() {
        Some("baseline") => vec!["baseline"],
        Some("follower_graceful_stop") => vec!["follower_graceful_stop"],
        Some("follower_sigkill") => vec!["follower_sigkill"],
        Some("leader_graceful_stop") => vec!["leader_graceful_stop"],
        Some("leader_sigkill") => vec!["leader_sigkill"],
        Some("leader_restart_loop") => vec!["leader_restart_loop"],
        Some("partition_leader_follower_replication") => vec!["partition_leader_follower_replication"],
        Some("partition_leader_minio") => vec!["partition_leader_minio"],
        Some("partition_asymmetric") => vec!["partition_asymmetric"],
        Some("network_flap") => vec!["network_flap"],
        Some("minio_outage_short") => vec!["minio_outage_short"],
        Some("minio_outage_long") => vec!["minio_outage_long"],
        Some("partition_then_kill_minio") => vec!["partition_then_kill_minio"],
        Some("rolling_restart") => vec!["rolling_restart"],
        Some("sigstop_leader") => vec!["sigstop_leader"],
        Some("clock_skew_follower") => vec!["clock_skew_follower"],
        Some("follower_disk_full") => vec!["follower_disk_full"],
        Some("bench_load_sweep") => vec!["bench_load_sweep"],
        Some("bridge") => vec!["bridge"],
        Some("single_node_isolation") => vec!["single_node_isolation"],
        Some("clock_scrambler") => vec!["clock_scrambler"],
        Some("duplicate_replay") => vec!["duplicate_replay"],
        Some("cas_storm") => vec!["cas_storm"],
        Some("cas_storm_partition") => vec!["cas_storm_partition"],
        Some("idempotency_audit_baseline") => vec!["idempotency_audit_baseline"],
        Some("idempotency_audit_minio_outage") => vec!["idempotency_audit_minio_outage"],
        Some("idempotency_audit_partition_then_kill_minio") => vec!["idempotency_audit_partition_then_kill_minio"],
        Some("idempotency_audit_fast_blackout") => vec!["idempotency_audit_fast_blackout"],
        Some("watch_storm") => vec!["watch_storm"],
        Some("watch_storm_failover") => vec!["watch_storm_failover"],
        Some("cold_segment_reads") => vec!["cold_segment_reads"],
        Some("nemesis_composition") => vec!["nemesis_composition"],
        Some("schema_under_partition") => vec!["schema_under_partition"],
        // Deliberately NOT in --full: the fill alone runs for up to five hours.
        // Lives alongside bench_load_sweep as a --scenario-only entry.
        Some("cardinality_pressure") => vec!["cardinality_pressure"],
        // Defect reproductions. Deliberately NOT in --full: both are expected
        // RED until the server is fixed, and both leave the rig busy for
        // several minutes restarting a cluster they deliberately broke.
        Some("write_outage_selfheal") => vec!["write_outage_selfheal"],
        Some("promotion_failure_survival") => vec!["promotion_failure_survival"],
        Some(other) => return Err(format!("unknown scenario: {other}")),
        None if args.full => vec![
            "baseline",
            "watch_storm",
            "watch_storm_failover",
            "follower_graceful_stop",
            "follower_sigkill",
            "leader_graceful_stop",
            "leader_sigkill",
            "leader_restart_loop",
            "partition_leader_minio",
            "partition_asymmetric",
            "partition_leader_follower_replication",
            "bridge",
            "single_node_isolation",
            "network_flap",
            "minio_outage_short",
            "minio_outage_long",
            "partition_then_kill_minio",
            "rolling_restart",
            "clock_skew_follower",
            "clock_scrambler",
            "sigstop_leader",
            "follower_disk_full",
            "idempotency_audit_baseline",
            "idempotency_audit_minio_outage",
            "idempotency_audit_partition_then_kill_minio",
            "duplicate_replay",
            "cas_storm",
            "cas_storm_partition",
            // Previously excluded: partition_then_kill_minio exposed a
            // follower-orphan-entries-after-leader-rollback bug that the
            // `rollback_active_to_read_cursor` fallback from SCEN-15
            // couldn't address. The wal_walk fix (Mode B divergence
            // recovery via reverse WAL walk, commit b2ffaea) resolved
            // that path. Re-enabled after standalone retest
            // `1775886847` passed all 13 checks (25567 req/s, 0 errors).
            "cold_segment_reads",
            "nemesis_composition",
            "schema_under_partition",
        ],
        None => vec!["baseline"],
    };

    if args.soak > 0 {
        run_soak(&cfg, params, card, defect, &scenarios_to_run, args.soak, args.soak_continue_on_failure).await
    } else {
        run_single_pass(&cfg, params, card, defect, &scenarios_to_run).await
    }
}

/// One iteration: create a new RunDir, run each scenario, write reports,
/// return the aggregate (number of scenarios passed, total, root path).
/// Does NOT propagate scenario execution errors — those turn into failed
/// reports inside the loop above — but WILL propagate setup errors like
/// "couldn't create run dir".
async fn run_one_iteration(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    card: CardinalityParams,
    defect: DefectParams,
    scenarios_to_run: &[&str],
) -> Result<(Vec<ScenarioReport>, RunDir), String> {
    let dir = RunDir::create(&cfg.deploy_dir)?;
    println!("Run directory: {}", dir.root.display());

    let mut reports = Vec::new();
    for name in scenarios_to_run {
        let report = match *name {
            "baseline" => run_baseline(cfg, params, &dir.root).await?,
            "watch_storm" => run_watch_storm(cfg, params, &dir.root).await?,
            "watch_storm_failover" => run_watch_storm_failover(cfg, params, &dir.root).await?,
            "cold_segment_reads" => run_cold_segment_reads(cfg, params, &dir.root).await?,
            "nemesis_composition" => run_nemesis_composition(cfg, params, &dir.root).await?,
            "schema_under_partition" => run_schema_under_partition(cfg, params, &dir.root).await?,
            "follower_graceful_stop" => run_follower_graceful_stop(cfg, params, &dir.root).await?,
            "follower_sigkill" => run_follower_sigkill(cfg, params, &dir.root).await?,
            "leader_graceful_stop" => run_leader_graceful_stop(cfg, params, &dir.root).await?,
            "leader_sigkill" => run_leader_sigkill(cfg, params, &dir.root).await?,
            "leader_restart_loop" => run_leader_restart_loop(cfg, params, &dir.root).await?,
            "partition_leader_follower_replication" => {
                run_partition_leader_follower_replication(cfg, params, &dir.root).await?
            }
            "partition_leader_minio" => run_partition_leader_minio(cfg, params, &dir.root).await?,
            "partition_asymmetric" => run_partition_asymmetric(cfg, params, &dir.root).await?,
            "network_flap" => run_network_flap(cfg, params, &dir.root).await?,
            "minio_outage_short" => run_minio_outage_short(cfg, params, &dir.root).await?,
            "minio_outage_long" => run_minio_outage_long(cfg, params, &dir.root).await?,
            "partition_then_kill_minio" => {
                run_partition_then_kill_minio(cfg, params, &dir.root).await?
            }
            "rolling_restart" => run_rolling_restart(cfg, params, &dir.root).await?,
            "sigstop_leader" => run_sigstop_leader(cfg, params, &dir.root).await?,
            "clock_skew_follower" => run_clock_skew_follower(cfg, params, &dir.root).await?,
            "follower_disk_full" => run_follower_disk_full(cfg, params, &dir.root).await?,
            "bench_load_sweep" => run_bench_load_sweep(cfg, params, &dir.root).await?,
            "bridge" => run_bridge(cfg, params, &dir.root).await?,
            "single_node_isolation" => run_single_node_isolation(cfg, params, &dir.root).await?,
            "clock_scrambler" => run_clock_scrambler(cfg, params, &dir.root).await?,
            "duplicate_replay" => run_duplicate_replay(cfg, params, &dir.root).await?,
            "cas_storm" => run_cas_storm_scenario(cfg, params, &dir.root, false).await?,
            "cas_storm_partition" => run_cas_storm_scenario(cfg, params, &dir.root, true).await?,
            "idempotency_audit_baseline" => run_idempotency_audit_baseline(cfg, params, &dir.root).await?,
            "idempotency_audit_minio_outage" => run_idempotency_audit_minio_outage(cfg, params, &dir.root).await?,
            "idempotency_audit_partition_then_kill_minio" => run_idempotency_audit_partition_then_kill_minio(cfg, params, &dir.root).await?,
            "idempotency_audit_fast_blackout" => run_idempotency_audit_fast_blackout(cfg, params, &dir.root).await?,
            "cardinality_pressure" => run_cardinality_pressure(cfg, params, card, &dir.root).await?,
            "write_outage_selfheal" => run_write_outage_selfheal(cfg, params, card, defect, &dir.root).await?,
            "promotion_failure_survival" => run_promotion_failure_survival(cfg, params, card, defect, &dir.root).await?,
            _ => unreachable!(),
        };
        write_scenario(&dir, &report)?;
        reports.push(report);
    }

    write_run_report(&dir, &reports)?;
    Ok((reports, dir))
}

/// Standard single-pass mode: run all scenarios once and exit.
async fn run_single_pass(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    card: CardinalityParams,
    defect: DefectParams,
    scenarios_to_run: &[&str],
) -> Result<(), String> {
    let (reports, dir) = run_one_iteration(cfg, params, card, defect, scenarios_to_run).await?;
    let pass = reports.iter().filter(|s| s.outcome.is_pass()).count();
    println!();
    println!("=== {} / {} scenarios passed ===", pass, reports.len());
    println!("Report: {}/report.md", dir.root.display());
    if pass == reports.len() {
        Ok(())
    } else {
        Err("one or more scenarios failed".into())
    }
}

/// SCEN-19 soak mode: repeat the scenario set in a loop until `soak_secs`
/// have elapsed. Each iteration is its own run directory. Without
/// `continue_on_failure`, the first failing iteration aborts the loop.
async fn run_soak(
    cfg: &ClusterConfig,
    params: ScenarioParams,
    card: CardinalityParams,
    defect: DefectParams,
    scenarios_to_run: &[&str],
    soak_secs: u64,
    continue_on_failure: bool,
) -> Result<(), String> {
    let start = Instant::now();
    let mut iteration = 0u32;
    let mut total_pass = 0u32;
    let mut total_fail = 0u32;
    let mut failed_iterations: Vec<(u32, PathBuf)> = Vec::new();

    println!(
        "=== SOAK MODE: running {} scenario(s) for at least {}s ({:.1}h) — continue_on_failure={} ===",
        scenarios_to_run.len(),
        soak_secs,
        soak_secs as f64 / 3600.0,
        continue_on_failure,
    );

    loop {
        iteration += 1;
        let elapsed = start.elapsed().as_secs();
        let remaining = soak_secs.saturating_sub(elapsed);
        // Per-iteration seed (N3): otherwise a 24h soak replays the identical
        // fault schedule hundreds of times instead of exploring a distribution.
        let iter_seed = params.seed ^ (iteration as u64);
        let iter_params = ScenarioParams { seed: iter_seed, ..params };
        println!();
        println!(
            "=== SOAK iteration {iteration}: elapsed {}s / {}s ({}s remaining), seed {iter_seed:#x} ===",
            elapsed, soak_secs, remaining,
        );

        let (reports, dir) = match run_one_iteration(cfg, iter_params, card, defect, scenarios_to_run).await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("soak iteration {iteration} bring-up or report write failed: {e}");
                if !continue_on_failure {
                    return Err(format!(
                        "soak aborted at iteration {iteration} after {}s: {e}",
                        start.elapsed().as_secs()
                    ));
                }
                total_fail += 1;
                continue;
            }
        };

        let pass = reports.iter().filter(|s| s.outcome.is_pass()).count();
        let iter_passed = pass == reports.len();
        if iter_passed {
            total_pass += 1;
            println!("=== SOAK iteration {iteration}: PASS {}/{} ===", pass, reports.len());
        } else {
            total_fail += 1;
            failed_iterations.push((iteration, dir.root.clone()));
            let failed_names: Vec<&str> = reports
                .iter()
                .filter(|s| !s.outcome.is_pass())
                .map(|s| s.name.as_str())
                .collect();
            eprintln!(
                "=== SOAK iteration {iteration}: FAIL {}/{} — failing: {} ===",
                pass,
                reports.len(),
                failed_names.join(", ")
            );
            if !continue_on_failure {
                return Err(format!(
                    "soak aborted at iteration {iteration} after {}s: scenarios failed — {}",
                    start.elapsed().as_secs(),
                    failed_names.join(", ")
                ));
            }
        }

        if start.elapsed().as_secs() >= soak_secs {
            break;
        }
    }

    println!();
    println!(
        "=== SOAK COMPLETE: {} iterations, {} pass, {} fail, elapsed {}s ({:.1}h) ===",
        iteration,
        total_pass,
        total_fail,
        start.elapsed().as_secs(),
        start.elapsed().as_secs_f64() / 3600.0,
    );
    if !failed_iterations.is_empty() {
        println!("Failed iterations:");
        for (i, path) in &failed_iterations {
            println!("  #{i}: {}", path.display());
        }
    }
    if total_fail == 0 {
        Ok(())
    } else {
        Err(format!(
            "{} / {} soak iterations had failing scenarios",
            total_fail,
            iteration,
        ))
    }
}
