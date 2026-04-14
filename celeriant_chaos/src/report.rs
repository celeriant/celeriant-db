use std::fs;
use std::path::{Path, PathBuf};

use crate::scenario::ScenarioReport;

pub struct RunDir {
    pub root: PathBuf,
}

impl RunDir {
    pub fn create(deploy_dir: &Path) -> Result<Self, String> {
        let ts = current_timestamp();
        let root = deploy_dir.join("runs").join(&ts);
        fs::create_dir_all(&root).map_err(|e| format!("create {}: {e}", root.display()))?;
        Ok(Self { root })
    }
}

pub fn write_scenario(dir: &RunDir, report: &ScenarioReport) -> Result<(), String> {
    let path = dir.root.join(format!("{}.json", report.name));
    let body = serde_json::to_string_pretty(report)
        .map_err(|e| format!("serialize: {e}"))?;
    fs::write(&path, body).map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(())
}

pub fn write_run_report(dir: &RunDir, scenarios: &[ScenarioReport]) -> Result<(), String> {
    let mut md = String::new();
    md.push_str("# Chaos Run Report\n\n");
    md.push_str(&format!("Run directory: `{}`\n\n", dir.root.display()));

    let pass = scenarios.iter().filter(|s| s.passed).count();
    let total = scenarios.len();
    md.push_str(&format!("**{} / {} scenarios passed**\n\n", pass, total));

    md.push_str("## Summary\n\n");
    md.push_str("| Scenario | Verdict | Throughput | Errors | P50 | P99 | Failed checks |\n");
    md.push_str("|---|---|---|---|---|---|---|\n");
    for s in scenarios {
        let verdict = if s.passed { "PASS" } else { "FAIL" };
        let failed: Vec<&str> = s.checks.iter().filter(|c| !c.passed).map(|c| c.name).collect();
        md.push_str(&format!(
            "| {} | {} | {:.0} req/s | {} | {}ms | {}ms | {} |\n",
            s.name,
            verdict,
            s.bench.throughput,
            s.bench.errors,
            s.bench.p50_ms,
            s.bench.p99_ms,
            if failed.is_empty() { "—".to_string() } else { failed.join(", ") },
        ));
    }
    md.push('\n');

    for s in scenarios {
        md.push_str(&format!("## {}\n\n", s.name));
        md.push_str(&format!(
            "Params: {} tasks, {}s, throughput floor {:.0} req/s\n\n",
            s.params.tasks, s.params.duration_secs, s.params.throughput_floor
        ));
        md.push_str(&format!(
            "Bench: {} req, {} errors, {:.0} req/s, avg {:.1}ms, P50 {}ms, P95 {}ms, P99 {}ms, P99.9 {}ms\n\n",
            s.bench.total_requests,
            s.bench.errors,
            s.bench.throughput,
            s.bench.avg_latency_ms,
            s.bench.p50_ms,
            s.bench.p95_ms,
            s.bench.p99_ms,
            s.bench.p999_ms,
        ));
        md.push_str("### Checks\n\n");
        for c in &s.checks {
            let mark = if c.passed { "PASS" } else { "FAIL" };
            md.push_str(&format!("- **{}** [{}] — {}\n", c.name, mark, c.detail));
        }
        md.push('\n');
        md.push_str(&format!("Full sample stream: `{}.json`\n\n", s.name));
        if !s.log_files.is_empty() {
            md.push_str("### Logs (failure window, ±5s pad)\n\n");
            for f in &s.log_files {
                md.push_str(&format!("- `{}`\n", f));
            }
            md.push('\n');
        }
    }

    let path = dir.root.join("report.md");
    fs::write(&path, md).map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(())
}

fn current_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    // Plain unix seconds — no chrono dep, sortable lexicographically.
    format!("{secs}")
}
