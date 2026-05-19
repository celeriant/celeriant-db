mod actions;
mod config;
mod disk_truth;
mod invariants;
mod logs;
mod report;
mod sample;
mod scenario;
mod scrape;

use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;

use crate::actions::find_project_root;
use crate::config::ClusterConfig;
use crate::report::{RunDir, write_run_report, write_scenario};
use crate::scenario::{
    ScenarioParams, ScenarioReport, run_baseline, run_follower_graceful_stop, run_follower_sigkill,
    run_idempotency_audit_baseline, run_idempotency_audit_minio_outage,
    run_idempotency_audit_partition_then_kill_minio, run_idempotency_audit_fast_blackout,
    run_leader_graceful_stop, run_leader_restart_loop, run_leader_sigkill,
    run_bench_load_sweep, run_clock_skew_follower, run_follower_disk_full,
    run_minio_outage_long, run_minio_outage_short, run_network_flap,
    run_partition_asymmetric, run_partition_then_kill_minio,
    run_rolling_restart, run_sigstop_leader,
    run_partition_leader_follower_replication, run_partition_leader_minio,
};

#[derive(Parser)]
#[command(name = "celeriant-chaos", about = "Chaos test orchestrator for the RPi cluster")]
struct Args {
    /// Run all scenarios in the suite. Today: same as default (only baseline exists).
    #[arg(long)]
    full: bool,

    /// Run a specific scenario by name. Today: only "baseline".
    #[arg(long)]
    scenario: Option<String>,

    /// Concurrent bench tasks.
    #[arg(long, default_value = "4000")]
    tasks: usize,

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

    println!("=== celeriant-chaos ===");
    println!("  project root: {}", project_root.display());
    println!("  leader:       {}", cfg.leader_host);
    println!("  follower:     {}", cfg.follower_host);
    println!("  infra:        {}", cfg.infra_host.as_deref().unwrap_or("(none — managed by deploy)"));
    println!("  tasks:        {}", args.tasks);
    println!("  duration:     {}s", args.duration);
    println!();

    let params = ScenarioParams {
        tasks: args.tasks,
        duration_secs: args.duration,
        throughput_floor: args.throughput_floor,
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
        Some("idempotency_audit_baseline") => vec!["idempotency_audit_baseline"],
        Some("idempotency_audit_minio_outage") => vec!["idempotency_audit_minio_outage"],
        Some("idempotency_audit_partition_then_kill_minio") => vec!["idempotency_audit_partition_then_kill_minio"],
        Some("idempotency_audit_fast_blackout") => vec!["idempotency_audit_fast_blackout"],
        Some(other) => return Err(format!("unknown scenario: {other}")),
        None if args.full => vec![
            "baseline",
            "follower_graceful_stop",
            "follower_sigkill",
            "leader_graceful_stop",
            "leader_sigkill",
            "leader_restart_loop",
            "partition_leader_minio",
            "partition_asymmetric",
            "partition_leader_follower_replication",
            "network_flap",
            "minio_outage_short",
            "minio_outage_long",
            "partition_then_kill_minio",
            "rolling_restart",
            "clock_skew_follower",
            "sigstop_leader",
            "follower_disk_full",
            "idempotency_audit_baseline",
            "idempotency_audit_minio_outage",
            "idempotency_audit_partition_then_kill_minio",
            // Previously excluded: partition_then_kill_minio exposed a
            // follower-orphan-entries-after-leader-rollback bug that the
            // `rollback_active_to_read_cursor` fallback from SCEN-15
            // couldn't address. The wal_walk fix (Mode B divergence
            // recovery via reverse WAL walk, commit b2ffaea) resolved
            // that path. Re-enabled after standalone retest
            // `1775886847` passed all 13 checks (25567 req/s, 0 errors).
        ],
        None => vec!["baseline"],
    };

    if args.soak > 0 {
        run_soak(&cfg, params, &scenarios_to_run, args.soak, args.soak_continue_on_failure).await
    } else {
        run_single_pass(&cfg, params, &scenarios_to_run).await
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
    scenarios_to_run: &[&str],
) -> Result<(Vec<ScenarioReport>, RunDir), String> {
    let dir = RunDir::create(&cfg.deploy_dir)?;
    println!("Run directory: {}", dir.root.display());

    let mut reports = Vec::new();
    for name in scenarios_to_run {
        let report = match *name {
            "baseline" => run_baseline(cfg, params, &dir.root).await?,
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
            "idempotency_audit_baseline" => run_idempotency_audit_baseline(cfg, params, &dir.root).await?,
            "idempotency_audit_minio_outage" => run_idempotency_audit_minio_outage(cfg, params, &dir.root).await?,
            "idempotency_audit_partition_then_kill_minio" => run_idempotency_audit_partition_then_kill_minio(cfg, params, &dir.root).await?,
            "idempotency_audit_fast_blackout" => run_idempotency_audit_fast_blackout(cfg, params, &dir.root).await?,
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
    scenarios_to_run: &[&str],
) -> Result<(), String> {
    let (reports, dir) = run_one_iteration(cfg, params, scenarios_to_run).await?;
    let pass = reports.iter().filter(|s| s.passed).count();
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
        println!();
        println!(
            "=== SOAK iteration {iteration}: elapsed {}s / {}s ({}s remaining) ===",
            elapsed, soak_secs, remaining,
        );

        let (reports, dir) = match run_one_iteration(cfg, params, scenarios_to_run).await {
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

        let pass = reports.iter().filter(|s| s.passed).count();
        let iter_passed = pass == reports.len();
        if iter_passed {
            total_pass += 1;
            println!("=== SOAK iteration {iteration}: PASS {}/{} ===", pass, reports.len());
        } else {
            total_fail += 1;
            failed_iterations.push((iteration, dir.root.clone()));
            let failed_names: Vec<&str> = reports
                .iter()
                .filter(|s| !s.passed)
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
