use std::process::{Command, Stdio};

use crate::invariants::CheckResult;

#[derive(Debug, Clone)]
pub struct ResourceSnapshot {
    pub fd_count: u64,
    pub vm_rss_kb: u64,
}

/// SSH to `host`, find the celeriant process's PID via systemctl, then read
/// fd count and VmRSS in one remote shell invocation.
pub async fn snapshot(host: &str) -> Result<ResourceSnapshot, String> {
    let host = host.to_string();
    // On a blocking thread, and with `BatchMode`/`ConnectTimeout` like every
    // other ssh in the harness. Driven directly on a tokio worker this call
    // could not be cancelled by a `timeout` at the call site — a future that
    // never yields never observes the deadline — and an ssh that connects and
    // then stalls had nothing bounding it at all.
    tokio::task::spawn_blocking(move || {
        // One ssh: get PID, count fds, read VmRSS.
        let cmd = "pid=$(systemctl show -p MainPID --value celeriant); \
                   ls /proc/$pid/fd | wc -l; \
                   awk '/VmRSS/{print $2}' /proc/$pid/status";

        let out = Command::new("ssh")
            .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=5", &host])
            .arg(cmd)
            .stdin(Stdio::null())
            .output()
            .map_err(|e| format!("spawn ssh {host}: {e}"))?;

        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(format!("ssh {host} exited {}: {}", out.status, stderr.trim()));
        }

        let stdout = String::from_utf8_lossy(&out.stdout);
        parse_snapshot_output(&stdout)
            .ok_or_else(|| format!("failed to parse snapshot output from {host}: {:?}", stdout.trim()))
    })
    .await
    .map_err(|e| format!("resource snapshot join: {e}"))?
}

/// Parse two-line output: first line is fd count, second is VmRSS kB.
fn parse_snapshot_output(text: &str) -> Option<ResourceSnapshot> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let fd_count: u64 = lines.next()?.trim().parse().ok()?;
    let vm_rss_kb: u64 = lines.next()?.trim().parse().ok()?;
    Some(ResourceSnapshot { fd_count, vm_rss_kb })
}

/// Compare a before/after snapshot pair for a node. Always records both
/// values in the detail string so soak runs can extract trends even on pass.
pub fn baseline_checks(
    node: &str,
    before: &ResourceSnapshot,
    after: &ResourceSnapshot,
) -> Vec<CheckResult> {
    vec![
        check_fd_return(node, before, after),
        check_rss_bounded(node, before, after),
    ]
}

fn check_fd_return(node: &str, before: &ResourceSnapshot, after: &ResourceSnapshot) -> CheckResult {
    const NAME: &str = "FdReturnToBaseline";
    const TOLERANCE: u64 = 64;
    let allowed = before.fd_count + TOLERANCE;
    let detail = format!(
        "{node}: fd before={} after={} (tolerance +{TOLERANCE})",
        before.fd_count, after.fd_count
    );
    if after.fd_count <= allowed {
        CheckResult::pass_with_detail(NAME, detail)
    } else {
        CheckResult::fail(NAME, format!("{detail} — exceeded by {}", after.fd_count - allowed))
    }
}

fn check_rss_bounded(node: &str, before: &ResourceSnapshot, after: &ResourceSnapshot) -> CheckResult {
    const NAME: &str = "RssBounded";
    // The server legitimately grows from a cold-boot baseline (~140MB) to its
    // configured memory budget (60% of 8GB ≈ 4.8GiB on the Pis) as LRUs fill —
    // a before-relative multiplier false-positives on every loaded run. The
    // ceiling is therefore absolute: budget + slack. Growth ABOVE the configured
    // budget is the runaway signal; per-iteration trends come from the detail
    // string either way.
    const RSS_CEILING_KB: u64 = 6 * 1024 * 1024; // 6 GiB
    let allowed = RSS_CEILING_KB.max(before.vm_rss_kb);
    let detail = format!(
        "{node}: rss_kb before={} after={} (allowed ≤{})",
        before.vm_rss_kb, after.vm_rss_kb, allowed
    );
    if after.vm_rss_kb <= allowed {
        CheckResult::pass_with_detail(NAME, detail)
    } else {
        CheckResult::fail(NAME, format!("{detail} — exceeded by {}", after.vm_rss_kb - allowed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(fd: u64, rss: u64) -> ResourceSnapshot {
        ResourceSnapshot { fd_count: fd, vm_rss_kb: rss }
    }

    #[test]
    fn parse_valid_output() {
        let text = "342\n512000\n";
        let s = parse_snapshot_output(text).unwrap();
        assert_eq!(s.fd_count, 342);
        assert_eq!(s.vm_rss_kb, 512000);
    }

    #[test]
    fn parse_with_whitespace() {
        let text = "  12  \n  98304  \n";
        let s = parse_snapshot_output(text).unwrap();
        assert_eq!(s.fd_count, 12);
        assert_eq!(s.vm_rss_kb, 98304);
    }

    #[test]
    fn parse_missing_second_line_returns_none() {
        let text = "100\n";
        assert!(parse_snapshot_output(text).is_none());
    }

    #[test]
    fn fd_within_tolerance_passes() {
        let before = snap(100, 512000);
        let after = snap(164, 512000); // exactly at tolerance limit
        let results = baseline_checks("node1", &before, &after);
        let fd = results.iter().find(|r| r.name == "FdReturnToBaseline").unwrap();
        assert!(fd.passed(), "{}", fd.detail);
        assert!(fd.detail.contains("node1"));
    }

    #[test]
    fn fd_exceeds_tolerance_fails() {
        let before = snap(100, 512000);
        let after = snap(165, 512000); // 100 + 64 + 1 = over tolerance
        let results = baseline_checks("node1", &before, &after);
        let fd = results.iter().find(|r| r.name == "FdReturnToBaseline").unwrap();
        assert!(!fd.passed(), "{}", fd.detail);
        assert!(fd.detail.contains("exceeded by 1"));
    }

    #[test]
    fn rss_within_2x_passes() {
        // before=2 GiB, after=4 GiB (exactly 2x, above the 1 GiB floor).
        let before = snap(100, 2_097_152);
        let after = snap(100, 4_194_304);
        let results = baseline_checks("node1", &before, &after);
        let rss = results.iter().find(|r| r.name == "RssBounded").unwrap();
        assert!(rss.passed(), "{}", rss.detail);
    }

    #[test]
    fn rss_over_ceiling_fails() {
        // after just above the 6 GiB absolute ceiling (runaway growth past the
        // ~4.8 GiB configured budget).
        let before = snap(100, 141_312);
        let after = snap(100, 6 * 1024 * 1024 + 1);
        let results = baseline_checks("node1", &before, &after);
        let rss = results.iter().find(|r| r.name == "RssBounded").unwrap();
        assert!(!rss.passed(), "{}", rss.detail);
    }

    #[test]
    fn rss_budget_fill_from_cold_boot_passes() {
        // Cold-boot 141MB growing to 3.2 GiB (LRUs filling to the configured
        // budget) is legitimate, not a leak — regression guard for the original
        // 2×-before formula that false-positived on every loaded run.
        let before = snap(100, 141_312);
        let after = snap(100, 3_228_320);
        let results = baseline_checks("node1", &before, &after);
        let rss = results.iter().find(|r| r.name == "RssBounded").unwrap();
        assert!(rss.passed(), "{}", rss.detail);
    }

    #[test]
    fn detail_always_includes_both_values() {
        let before = snap(50, 256000);
        let after = snap(60, 300000);
        let results = baseline_checks("node2", &before, &after);
        for r in &results {
            assert!(r.detail.contains("before="), "missing before= in: {}", r.detail);
            assert!(r.detail.contains("after="), "missing after= in: {}", r.detail);
            assert!(r.detail.contains("node2"), "missing node in: {}", r.detail);
        }
    }

    #[test]
    fn rss_zero_before_uses_floor() {
        // before=0 would make allowed=0, which is nonsensical. Floor is 1 GiB.
        let before = snap(0, 0);
        let after = snap(0, 500_000); // 500 MiB — under the 1 GiB floor
        let results = baseline_checks("node1", &before, &after);
        let rss = results.iter().find(|r| r.name == "RssBounded").unwrap();
        assert!(rss.passed(), "{}", rss.detail);
    }
}
