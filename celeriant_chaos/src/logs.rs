use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Pulls `journalctl -u celeriant` over SSH, bracketed by the supplied window
/// (with a small pad on each side), and writes it to `dest`.
///
/// Returns the on-disk path on success. Errors are non-fatal at the call site —
/// log fetch is best-effort diagnostic, never blocks the verdict.
pub fn fetch_journal(
    host: &str,
    window_start: SystemTime,
    window_end: SystemTime,
    pad: Duration,
    dest: &Path,
) -> Result<(), String> {
    let start_unix = unix_secs(window_start).saturating_sub(pad.as_secs());
    let end_unix = unix_secs(window_end).saturating_add(pad.as_secs());

    // `--since=@<unix>` / `--until=@<unix>` is the most portable, locale-free
    // form. Works on systemd 230+ which is everything since 2016.
    let remote = format!(
        "journalctl -u celeriant --no-pager --since=@{start_unix} --until=@{end_unix}"
    );

    let file = std::fs::File::create(dest)
        .map_err(|e| format!("create {}: {e}", dest.display()))?;

    let status = Command::new("ssh")
        .arg(host)
        .arg(&remote)
        .stdin(Stdio::null())
        .stdout(Stdio::from(file))
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("spawn ssh journalctl {host}: {e}"))?;

    if !status.success() {
        return Err(format!("journalctl on {host} exited with {status}"));
    }
    Ok(())
}

fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
