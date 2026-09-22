use std::fs;
use std::path::{Path, PathBuf};

use crate::scenario::{ScenarioOutcome, ScenarioReport};

pub struct RunDir {
    pub root: PathBuf,
}

impl RunDir {
    pub fn create(deploy_dir: &Path) -> Result<Self, String> {
        let ts = current_timestamp();
        let root = deploy_dir.join("runs").join(&ts);
        fs::create_dir_all(&root).map_err(|e| format!("create {}: {e}", root.display()))?;
        let dir = Self { root };
        dir.write_build_stamp(deploy_dir, &ts)?;
        Ok(dir)
    }

    /// Which build produced this run. Without it a run directory is a pile of
    /// numbers with no way back to the code that made them.
    fn write_build_stamp(&self, repo_dir: &Path, ts: &str) -> Result<(), String> {
        let head = git(repo_dir, &["rev-parse", "HEAD"]).unwrap_or_else(|e| format!("(unavailable: {e})"));
        let status = git(repo_dir, &["status", "--porcelain"]).unwrap_or_else(|e| format!("(unavailable: {e})"));
        let body = format!(
            "timestamp_unix: {ts}\nceleriant_chaos: {}\ngit_head: {head}\ngit_status_porcelain:\n{}\n",
            env!("CARGO_PKG_VERSION"),
            if status.is_empty() { "(clean)" } else { &status },
        );
        let path = self.root.join("build.txt");
        fs::write(&path, body).map_err(|e| format!("write {}: {e}", path.display()))
    }
}

/// Run `git` in `repo_dir` and return trimmed stdout.
pub fn git(repo_dir: &Path, args: &[&str]) -> Result<String, String> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_dir)
        .args(args)
        .output()
        .map_err(|e| format!("git {}: {e}", args.join(" ")))?;
    if !out.status.success() {
        return Err(format!("git {} exited {}", args.join(" "), out.status));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim_end().to_string())
}

/// Porcelain lines for TRACKED changes only. Untracked (`??`) entries are
/// ignored: a working session keeps scratch directories around, and treating
/// those as a dirty tree would refuse every run.
pub fn tracked_changes(porcelain: &str) -> Vec<&str> {
    porcelain.lines().filter(|l| !l.starts_with("??") && !l.trim().is_empty()).collect()
}

pub fn write_scenario(dir: &RunDir, report: &ScenarioReport) -> Result<(), String> {
    print_accept_summary(report);
    let path = dir.root.join(format!("{}.json", report.name));
    let body = serde_json::to_string_pretty(report)
        .map_err(|e| format!("serialize: {e}"))?;
    fs::write(&path, body).map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(())
}

/// Server accept path, from the last good sample of each host. One line per
/// scenario: connections the listener actually took, and what the inline
/// handshake cost the shard executor that took them.
fn print_accept_summary(report: &ScenarioReport) {
    for host in report.samples.iter().map(|s| s.host.as_str()).collect::<std::collections::BTreeSet<_>>() {
        let Some(last) = report.samples.iter().rev().find(|s| s.ok && s.host == host) else { continue };
        let mean_ms = if last.tls_handshake_count == 0 {
            0.0
        } else {
            last.tls_handshake_seconds_sum / last.tls_handshake_count as f64 * 1000.0
        };
        println!(
            "[{}] {host} accept: {} accepted, {} conns active, handshakes {} (mean {mean_ms:.1}ms, {} failed, {} in flight)",
            report.name,
            last.client_accepts_total,
            last.client_connections_active,
            last.tls_handshake_count,
            last.tls_handshake_failures_total,
            last.tls_handshakes_in_flight,
        );
    }
}

pub fn write_run_report(dir: &RunDir, scenarios: &[ScenarioReport]) -> Result<(), String> {
    let mut md = String::new();
    md.push_str("# Chaos Run Report\n\n");
    md.push_str(&format!("Run directory: `{}`\n\n", dir.root.display()));

    let pass = scenarios.iter().filter(|s| s.outcome.is_pass()).count();
    let inconclusive = scenarios.iter().filter(|s| s.outcome == ScenarioOutcome::Inconclusive).count();
    let total = scenarios.len();
    md.push_str(&format!("**{} / {} scenarios passed**", pass, total));
    // Called out on the headline rather than buried in the table: an inconclusive
    // run measured nothing, and reading it as a near-pass is the whole mistake
    // this verdict exists to prevent.
    if inconclusive > 0 {
        md.push_str(&format!(" — {inconclusive} INCONCLUSIVE (reached no regime where the measurement means anything)"));
    }
    md.push_str("\n\n");

    md.push_str("## Summary\n\n");
    // "Unmet" rather than "Failed": the column lists every check that did not
    // pass, and an inconclusive one has not failed.
    md.push_str("| Scenario | Verdict | Throughput | Errors | P50 | P99 | Unmet checks |\n");
    md.push_str("|---|---|---|---|---|---|---|\n");
    for s in scenarios {
        let verdict = s.outcome.label();
        let failed: Vec<&str> = s.checks.iter().filter(|c| !c.passed()).map(|c| c.name).collect();
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
            "Params: {} tasks, {}s, throughput floor {:.0} req/s, seed {:#x}\n\n",
            s.params.tasks, s.params.duration_secs, s.params.throughput_floor, s.params.seed
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
            // Three states, not two. A check that could not be evaluated is not
            // a failure, and rendering it as one hides the difference between
            // "this is broken" and "this run never reached the regime".
            let mark = if c.passed() {
                "PASS"
            } else if c.is_inconclusive() {
                "INCONCLUSIVE"
            } else {
                "FAIL"
            };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untracked_entries_do_not_make_the_tree_dirty() {
        let porcelain = "?? session/\n M celeriant_chaos/src/main.rs\nA  new.rs\n";
        assert_eq!(
            tracked_changes(porcelain),
            vec![" M celeriant_chaos/src/main.rs", "A  new.rs"]
        );
        assert!(tracked_changes("?? session/\n?? debate-workspace/\n").is_empty());
    }

    #[test]
    fn a_run_directory_stamps_the_build_it_came_from() {
        let tmp = std::env::temp_dir().join(format!("celeriant-chaos-rundir-{}", std::process::id()));
        let dir = RunDir::create(&tmp).unwrap();
        let stamp = fs::read_to_string(dir.root.join("build.txt")).unwrap();
        assert!(stamp.contains("git_head: "), "{stamp}");
        assert!(stamp.contains("git_status_porcelain:"), "{stamp}");
        assert!(stamp.contains(concat!("celeriant_chaos: ", env!("CARGO_PKG_VERSION"))), "{stamp}");
        let _ = fs::remove_dir_all(&tmp);
    }
}
