use std::path::Path;
use std::process::{Command, Stdio};

use crate::config::ClusterConfig;

/// A typed cluster action. Each action is implemented by shelling out to the
/// existing Makefile in deploy/rpi-cluster, so we don't reinvent the SSH layer.
#[allow(dead_code)] // Stop/Kill/Partition variants are foundation for future chaos scenarios.
#[derive(Debug, Clone)]
pub enum Action {
    /// Stop both nodes, infra, and wipe all data on cs1, cs2, and MinIO. Destructive.
    TeardownData,
    /// `docker compose up -d` on the infra node.
    StartInfra,
    /// `systemctl start celeriant` on cs1.
    StartCs1,
    /// `systemctl start celeriant` on cs2.
    StartCs2,
    /// `systemctl stop celeriant` on cs1 (graceful).
    StopCs1,
    /// `systemctl stop celeriant` on cs2 (graceful).
    StopCs2,
    /// SIGKILL celeriant on cs1.
    KillCs1,
    /// SIGKILL celeriant on cs2.
    KillCs2,
    /// Stop both Celeriant nodes (no data wipe).
    StopAll,
    /// Graceful stop MinIO (`docker compose stop minio`) on the infra node.
    /// Only meaningful on deploys with a dedicated MinIO container (rpi).
    StopMinio,
    /// Start MinIO (`docker compose start minio`) on the infra node.
    StartMinio,
    /// SIGSTOP cs1 — freezes the process without terminating. Pairs with `ResumeCs1`.
    PauseCs1,
    /// SIGSTOP cs2.
    PauseCs2,
    /// SIGCONT cs1 — unfreezes a previously-paused cs1.
    ResumeCs1,
    /// SIGCONT cs2.
    ResumeCs2,
    /// Shift `host`'s system clock by `offset_secs` seconds
    /// (positive = forward, negative = backward). Implemented via
    /// `timedatectl set-ntp false && date -s "+N seconds"` over ssh
    /// (NTP must be disabled first or systemd-timesyncd immediately
    /// reverts the jump). Affects the whole host, not just celeriant
    /// — used by SCEN-14 to test clock drift detection.
    SkewClock { host: String, offset_secs: i64 },
    /// Re-enable NTP on `host` to restore correct system time after
    /// a SkewClock. Pairs with `SkewClock`.
    RestoreClock { host: String },
    /// Fill `host`'s data disk to within `reserve_mb` of full via
    /// `fallocate`. Leaves a small reserve to avoid bricking the
    /// box — too low a reserve and systemd/journald can't even log.
    /// Pairs with `CleanDisk`.
    FillDisk { host: String, reserve_mb: u64 },
    /// Remove the chaos filler file from `host`. Idempotent.
    CleanDisk { host: String },
    /// Install an iptables DROP rule on `src` blocking TCP traffic to
    /// `dst:port`. `src` and `dst` are host IPs (or hostnames) — the rule
    /// runs on `src`'s OUTPUT chain, so only outbound from `src` is affected.
    /// Symmetric partitions require two Partition actions.
    Partition { src: String, dst: String, port: u16 },
    /// Remove the matching iptables DROP rule from `src`. Safe to call on an
    /// already-healed partition (the Makefile target loops on `-C` and exits
    /// cleanly once no rules match).
    Heal { src: String, dst: String, port: u16 },
}

impl Action {
    fn make_target(&self) -> &'static str {
        match self {
            Self::TeardownData => "teardown-data",
            Self::StartInfra => "start-infra",
            Self::StartCs1 => "start-cs1",
            Self::StartCs2 => "start-cs2",
            Self::StopCs1 => "stop-cs1",
            Self::StopCs2 => "stop-cs2",
            Self::KillCs1 => "kill-cs1",
            Self::KillCs2 => "kill-cs2",
            Self::StopAll => "stop",
            Self::StopMinio => "stop-minio",
            Self::StartMinio => "start-minio",
            Self::PauseCs1 => "pause-cs1",
            Self::PauseCs2 => "pause-cs2",
            Self::ResumeCs1 => "resume-cs1",
            Self::ResumeCs2 => "resume-cs2",
            Self::Partition { .. } => "partition-host",
            Self::Heal { .. } => "heal-host",
            Self::SkewClock { .. } => "skew-clock",
            Self::RestoreClock { .. } => "restore-clock",
            Self::FillDisk { .. } => "fill-disk",
            Self::CleanDisk { .. } => "clean-disk",
        }
    }

    /// Extra `KEY=VALUE` variables to set when invoking the target. Used to
    /// skip interactive confirmation prompts in destructive targets like
    /// `teardown-data`, and to pass dynamic partition targets (SRC/DST/PORT).
    fn make_vars(&self) -> Vec<String> {
        match self {
            Self::TeardownData => vec!["FORCE=yes".to_string()],
            Self::Partition { src, dst, port } | Self::Heal { src, dst, port } => {
                vec![
                    format!("SRC={src}"),
                    format!("DST={dst}"),
                    format!("PORT={port}"),
                ]
            }
            Self::SkewClock { host, offset_secs } => {
                // `date -s` accepts a relative string like "+2 seconds"
                // or "-2 seconds". Construct it from the signed i64.
                vec![
                    format!("HOST={host}"),
                    format!("OFFSET={:+} seconds", offset_secs),
                ]
            }
            Self::RestoreClock { host } => {
                vec![format!("HOST={host}")]
            }
            Self::FillDisk { host, reserve_mb } => {
                vec![
                    format!("HOST={host}"),
                    format!("RESERVE_MB={reserve_mb}"),
                ]
            }
            Self::CleanDisk { host } => {
                vec![format!("HOST={host}")]
            }
            _ => Vec::new(),
        }
    }
}

pub struct ActionExecutor<'a> {
    cfg: &'a ClusterConfig,
}

impl<'a> ActionExecutor<'a> {
    pub fn new(cfg: &'a ClusterConfig) -> Self {
        Self { cfg }
    }

    pub fn run(&self, action: &Action) -> Result<(), String> {
        self.run_make(action.make_target(), &action.make_vars())
    }

    fn run_make(&self, target: &str, vars: &[String]) -> Result<(), String> {
        let mut cmd = Command::new("make");
        cmd.arg("-s").arg(target);
        cmd.arg("DANGEROUS=1");
        for v in vars {
            cmd.arg(v);
        }
        let status = cmd
            .current_dir(&self.cfg.deploy_dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|e| format!("spawn make {target}: {e}"))?;
        if !status.success() {
            return Err(format!("make {target} exited with {status}"));
        }
        Ok(())
    }
}

/// Find the project root by walking up from CWD until we hit a Cargo.toml
/// containing `[workspace]`. Falls back to CWD.
pub fn find_project_root() -> std::path::PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let mut p: &Path = &cwd;
    loop {
        let candidate = p.join("Cargo.toml");
        if candidate.exists()
            && let Ok(s) = std::fs::read_to_string(&candidate)
            && s.contains("[workspace]")
        {
            return p.to_path_buf();
        }
        match p.parent() {
            Some(parent) => p = parent,
            None => return cwd,
        }
    }
}
