//! Replay stored chaos run JSONs through the comparator checks.
//!
//! Manual dev tool, not CI-gated (run artifacts are not repo-tracked):
//! comparator changes re-prove their red/green verdicts mechanically against
//! recorded runs instead of by argument.
//!
//! Usage: replay <run.json> [<run.json> ...]

use celeriant_chaos::invariants::{RunData, check_never_ahead, check_read_converged_at_quiesce};
use celeriant_chaos::sample::NodeSample;
use celeriant_chaos::scenario::sample_window;

#[derive(serde::Deserialize)]
struct StoredRun {
    name: String,
    samples: Vec<NodeSample>,
    bench_window_start_ms: u64,
    bench_actual_end_ms: u64,
    bench_window_end_ms: u64,
}

fn main() {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    if paths.is_empty() {
        eprintln!("usage: replay <run.json> [<run.json> ...]");
        std::process::exit(2);
    }
    let mut all_meaningful_passes = true;
    for path in &paths {
        match replay(path) {
            Ok(clean) => all_meaningful_passes &= clean,
            Err(e) => {
                eprintln!("{path}: {e}");
                std::process::exit(2);
            }
        }
    }
    // The verdict IS the product of this tool: a FAIL or a vacuous pass
    // (zero audited ticks — the shape a gutted/renamed field produces under
    // serde defaults) must not exit 0.
    if !all_meaningful_passes {
        std::process::exit(1);
    }
}

fn replay(path: &str) -> Result<bool, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let run: StoredRun = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    if run.samples.is_empty() {
        return Err("no samples".into());
    }
    // The scraper pushes the config-leader-slot sample first at every tick.
    let leader_host = run.samples[0].host.clone();
    let follower_host = run
        .samples
        .iter()
        .map(|s| &s.host)
        .find(|h| **h != leader_host)
        .ok_or("only one host in samples")?
        .clone();
    let (start_idx, end_idx) =
        sample_window(&run.samples, run.bench_window_start_ms, run.bench_window_end_ms);
    let data = RunData {
        samples: &run.samples,
        leader_host: &leader_host,
        follower_host: &follower_host,
        bench_start_idx: start_idx,
        bench_end_idx: end_idx,
        bench_actual_end_ms: run.bench_actual_end_ms,
        bench_errors: 0,
        bench_total_requests: 0,
        bench_throughput: 0.0,
        throughput_floor: 0.0,
    };
    println!(
        "{path} [{}] leader={leader_host} follower={follower_host} samples={} window={}..{}ms",
        run.name,
        run.samples.len(),
        run.bench_window_start_ms,
        run.bench_window_end_ms,
    );
    let mut clean = true;
    for check in [
        check_never_ahead(&data),
        check_read_converged_at_quiesce(&run.samples, &leader_host, &follower_host),
    ] {
        let vacuous = check.passed()
            && check.name == "NeverAhead"
            && check.detail.starts_with("0 stable ticks");
        let verdict = if vacuous {
            clean = false;
            "VACUOUS (0 audited ticks — not evidence)"
        } else if check.passed() {
            "PASS"
        } else {
            clean = false;
            "FAIL"
        };
        println!("  {}: {verdict} — {}", check.name, check.detail);
    }
    Ok(clean)
}
