//! 1 Hz per-node RSS and data-filesystem occupancy.
//!
//! `resource_baseline` samples before and after a run, which is enough for an
//! fd leak and useless for this one: the segment-summary pipeline is expected to
//! sawtooth, climbing as each active accumulator fills and dropping at seal. A
//! before/after pair sees neither the peak nor the shape. So this polls
//! throughout and keeps the whole trace — the trace is the deliverable.
//!
//! The free-space reading does double duty. A five-hour fill on a fast day can
//! outrun the disk, and hitting real ENOSPC turns the tail of the run into
//! garbage (rotation ENOSPC keeps the shard alive but fails every write needing
//! rotation, which reads as an unexplained throughput collapse). So the fill
//! phase stops on whichever comes first: its time budget, or this poller
//! crossing a high-water mark.
//!
//! One ssh per node per tick, on `spawn_blocking`. `ActionExecutor::run` already
//! blocks a tokio worker with `std::process::Command`; adding a 1 Hz blocking
//! poller to the same pool would have it contend with the metric scraper.

use std::collections::BTreeMap;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct HostSample {
    pub host: String,
    /// Milliseconds since the poller started. Monotonic, matches `NodeSample::t_ms`.
    pub t_ms: u64,
    pub ok: bool,
    pub error: Option<String>,
    /// Resident set of the celeriant process. 0 when the service is down, which
    /// is a legitimate state during the restart and SIGKILL phases.
    pub vm_rss_kb: u64,
    /// Percent used on the filesystem holding the data root.
    pub data_fs_used_pct: u64,
}

/// Shared, append-only trace plus the two hot readings the fill loop polls.
///
/// The atomics exist so the fill loop can check the stop condition every
/// iteration without taking the trace lock at write rate.
#[derive(Clone)]
pub struct HostPollStore {
    samples: Arc<tokio::sync::Mutex<Vec<HostSample>>>,
    max_used_pct: Arc<AtomicU64>,
    peak_rss_kb: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
}

impl Default for HostPollStore {
    fn default() -> Self {
        Self::new()
    }
}

impl HostPollStore {
    pub fn new() -> Self {
        Self {
            samples: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            max_used_pct: Arc::new(AtomicU64::new(0)),
            peak_rss_kb: Arc::new(AtomicU64::new(0)),
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Highest data-filesystem occupancy seen on any node. Drives the fill stop.
    pub fn max_data_fs_used_pct(&self) -> u64 {
        self.max_used_pct.load(Ordering::Relaxed)
    }

    /// Peak RSS across all nodes and all ticks. The number `HonestMemoryBudget`
    /// compares against the declared budget.
    pub fn peak_rss_kb(&self) -> u64 {
        self.peak_rss_kb.load(Ordering::Relaxed)
    }

    pub async fn snapshot(&self) -> Vec<HostSample> {
        self.samples.lock().await.clone()
    }

    /// Peak RSS per host. Reported per node because the two nodes carry
    /// different roles and only the leader's accumulators are filling.
    pub async fn peak_rss_by_host(&self) -> BTreeMap<String, u64> {
        let mut peaks: BTreeMap<String, u64> = BTreeMap::new();
        for s in self.samples.lock().await.iter().filter(|s| s.ok) {
            let e = peaks.entry(s.host.clone()).or_insert(0);
            *e = (*e).max(s.vm_rss_kb);
        }
        peaks
    }

    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    async fn push(&self, sample: HostSample) {
        if sample.ok {
            self.max_used_pct.fetch_max(sample.data_fs_used_pct, Ordering::Relaxed);
            self.peak_rss_kb.fetch_max(sample.vm_rss_kb, Ordering::Relaxed);
        }
        self.samples.lock().await.push(sample);
    }
}

/// Poll `hosts` every `interval` until `store.request_stop()`.
///
/// `unit` is the systemd unit to read the PID from, and `data_root` the path
/// whose filesystem is measured. Both are parameters rather than constants
/// because these boxes also run a production celeriant under a different unit
/// and a different data root, and pointing this at the wrong one would report
/// prod's memory as the scenario's result.
pub fn spawn(
    hosts: Vec<String>,
    unit: String,
    data_root: String,
    interval: Duration,
    store: HostPollStore,
) -> tokio::task::JoinHandle<()> {
    let start = Instant::now();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while !store.stop.load(Ordering::Relaxed) {
            ticker.tick().await;
            let t_ms = start.elapsed().as_millis() as u64;
            for host in &hosts {
                let (host, unit, data_root) = (host.clone(), unit.clone(), data_root.clone());
                let sample = tokio::task::spawn_blocking(move || poll_once(&host, &unit, &data_root, t_ms))
                    .await
                    .unwrap_or_else(|e| HostSample {
                        t_ms,
                        error: Some(format!("join error: {e}")),
                        ..Default::default()
                    });
                store.push(sample).await;
            }
        }
    })
}

fn poll_once(host: &str, unit: &str, data_root: &str, t_ms: u64) -> HostSample {
    let fail = |e: String| HostSample {
        host: host.to_string(),
        t_ms,
        ok: false,
        error: Some(e),
        ..Default::default()
    };

    // A stopped service yields MainPID=0 and no /proc entry, which is expected
    // across the restart and SIGKILL phases — RSS reads 0 and df still answers,
    // so a down node still contributes a disk reading.
    let script = format!(
        "pid=$(systemctl show -p MainPID --value {unit}); \
         if [ \"$pid\" != 0 ] && [ -r /proc/$pid/status ]; then \
           awk '/VmRSS/{{print $2}}' /proc/$pid/status; \
         else echo 0; fi; \
         df --output=pcent {data_root} 2>/dev/null | tail -1 | tr -dc '0-9'; echo"
    );

    let out = match Command::new("ssh")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("ConnectTimeout=5")
        .arg(host)
        .arg(&script)
        .stdin(Stdio::null())
        .output()
    {
        Ok(o) => o,
        Err(e) => return fail(format!("spawn ssh: {e}")),
    };
    if !out.status.success() {
        return fail(format!("ssh exited {}: {}", out.status, String::from_utf8_lossy(&out.stderr).trim()));
    }

    let stdout = String::from_utf8_lossy(&out.stdout);
    match parse_poll_output(&stdout) {
        Some((vm_rss_kb, data_fs_used_pct)) => HostSample {
            host: host.to_string(),
            t_ms,
            ok: true,
            error: None,
            vm_rss_kb,
            data_fs_used_pct,
        },
        None => fail(format!("unparseable output: {:?}", stdout.trim())),
    }
}

/// Two lines: VmRSS in kB, then data-filesystem percent used.
fn parse_poll_output(text: &str) -> Option<(u64, u64)> {
    let mut lines = text.lines().map(str::trim).filter(|l| !l.is_empty());
    let rss = lines.next()?.parse().ok()?;
    let pct: u64 = lines.next()?.parse().ok()?;
    // A percentage above 100 means the field was misread, not a full disk.
    // Better to report the tick unparseable than to trip the fill stop on noise.
    (pct <= 100).then_some((rss, pct))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rss_and_percent() {
        assert_eq!(parse_poll_output("123456\n71\n"), Some((123456, 71)));
        // Service down: RSS 0 is a real reading, not a parse failure — the disk
        // number from a stopped node still counts toward the watchdog.
        assert_eq!(parse_poll_output("0\n12\n"), Some((0, 12)));
    }

    #[test]
    fn rejects_garbage_rather_than_guessing() {
        for bad in ["", "123456\n", "\n71\n", "abc\n71\n", "123\nxyz\n", "123\n101\n"] {
            assert_eq!(parse_poll_output(bad), None, "should reject {bad:?}");
        }
    }

    #[tokio::test]
    async fn store_tracks_peaks_and_ignores_failed_ticks() {
        let store = HostPollStore::new();
        store.push(HostSample { host: "a".into(), t_ms: 0, ok: true, vm_rss_kb: 100, data_fs_used_pct: 10, ..Default::default() }).await;
        store.push(HostSample { host: "a".into(), t_ms: 1, ok: true, vm_rss_kb: 900, data_fs_used_pct: 65, ..Default::default() }).await;
        store.push(HostSample { host: "b".into(), t_ms: 1, ok: true, vm_rss_kb: 300, data_fs_used_pct: 20, ..Default::default() }).await;
        // A failed tick must not reset the watermarks — an ssh blip mid-fill
        // would otherwise disarm the disk watchdog.
        store.push(HostSample { host: "a".into(), t_ms: 2, ok: false, ..Default::default() }).await;

        assert_eq!(store.peak_rss_kb(), 900);
        assert_eq!(store.max_data_fs_used_pct(), 65);
        let by_host = store.peak_rss_by_host().await;
        assert_eq!(by_host.get("a"), Some(&900));
        assert_eq!(by_host.get("b"), Some(&300));
        assert_eq!(store.snapshot().await.len(), 4);
    }
}
