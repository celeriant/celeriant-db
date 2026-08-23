//! Startup pre-flight for the process NOFILE limit.
//!
//! Segments cost two fds each (writer + dup'd reader) in a per-shard LRU capped
//! at `max_open_files` (plus the active segment, which lives outside the LRU),
//! and each live glommio executor costs exactly 4 fds (3 io_uring rings +
//! 1 eventfd, measured on the pinned rev). Under fd exhaustion glommio panics
//! mid-flight with a misleading message, so the headroom is verified here,
//! before the executor pool is built. The soft NOFILE limit is raised to the
//! hard limit in-process when needed.

use rlimit::Resource;

/// Estimate of fds the process uses outside segments and executor rings:
/// listeners, accepted sockets, mesh, sidecar runtime, metrics, stdio.
/// An estimate until counted on a running cluster — and wrong in a known
/// direction at high shard counts: the client and replication listeners bind
/// per shard (2 fds/shard) and accepted client sockets are per-shard too, so
/// at 64 shards the listeners alone consume half of this constant.
pub const PROCESS_BASELINE_FDS: u64 = 256;

/// Slack on top of the computed requirement.
pub const FD_HEADROOM_MARGIN: u64 = 128;

const FDS_PER_SEGMENT: u64 = 2;
const FDS_PER_EXECUTOR: u64 = 4;

/// `num_shards * (2 * (max_open_files + 1)) + 4 * num_shards + baseline + margin`
/// — the `+ 1` is the active segment, held outside the LRU. Every step checked;
/// `None` on u64 overflow.
pub fn required_nofile(num_shards: u64, max_open_files: u64, baseline: u64, margin: u64) -> Option<u64> {
    let segment_fds = max_open_files
        .checked_add(1)?
        .checked_mul(FDS_PER_SEGMENT)?
        .checked_mul(num_shards)?;
    let executor_fds = num_shards.checked_mul(FDS_PER_EXECUTOR)?;
    segment_fds.checked_add(executor_fds)?.checked_add(baseline)?.checked_add(margin)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NofilePlan {
    Sufficient,
    /// Raise soft to `to` (the hard limit, for maximum headroom).
    Raise { to: u64 },
    ExceedsHard,
}

pub fn plan_nofile(soft: u64, hard: u64, required: u64) -> NofilePlan {
    if required <= soft {
        NofilePlan::Sufficient
    } else if required <= hard {
        NofilePlan::Raise { to: hard }
    } else {
        NofilePlan::ExceedsHard
    }
}

#[derive(Debug)]
pub enum FdLimitError {
    NofileOverflow {
        num_shards: u64,
        max_open_files: u64,
    },
    NofileInsufficient {
        required: u64,
        soft: u64,
        hard: u64,
        num_shards: u64,
        max_open_files: u64,
        baseline: u64,
        margin: u64,
    },
    NofileRaiseFailed {
        target: u64,
        source: String,
    },
    LimitQuery {
        resource: &'static str,
        source: String,
    },
}

impl std::fmt::Display for FdLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FdLimitError::NofileOverflow { num_shards, max_open_files } => write!(
                f,
                "NOFILE requirement overflows u64: num_shards={num_shards}, max_open_files={max_open_files}"
            ),
            FdLimitError::NofileInsufficient { required, soft, hard, num_shards, max_open_files, baseline, margin } => write!(
                f,
                "RLIMIT_NOFILE hard limit {hard} is below the required {required} fds \
                 (= {num_shards} shards x 2 x ({max_open_files} max_open_files + 1 active) \
                 + 4 x {num_shards} executor fds + {baseline} baseline + {margin} margin; \
                 soft limit was {soft}). Raise the hard limit or lower max_open_files/shards."
            ),
            FdLimitError::NofileRaiseFailed { target, source } => write!(
                f,
                "failed to raise soft RLIMIT_NOFILE to {target}: {source}"
            ),
            FdLimitError::LimitQuery { resource, source } => write!(
                f,
                "failed to query RLIMIT_{resource}: {source}"
            ),
        }
    }
}

impl std::error::Error for FdLimitError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FdHeadroom {
    pub required: u64,
    pub soft_before: u64,
    pub soft: u64,
    pub hard: u64,
}

/// Verify NOFILE headroom for `num_shards` executors, raising soft to hard
/// in-process when needed. NOFILE only.
pub fn ensure_fd_headroom(num_shards: u64, max_open_files: u64) -> Result<FdHeadroom, FdLimitError> {
    let (soft_before, hard) = Resource::NOFILE
        .get()
        .map_err(|e| FdLimitError::LimitQuery { resource: "NOFILE", source: e.to_string() })?;

    let required = required_nofile(num_shards, max_open_files, PROCESS_BASELINE_FDS, FD_HEADROOM_MARGIN)
        .ok_or(FdLimitError::NofileOverflow { num_shards, max_open_files })?;

    let soft = match plan_nofile(soft_before, hard, required) {
        NofilePlan::Sufficient => soft_before,
        NofilePlan::Raise { to } => {
            Resource::NOFILE
                .set(to, hard)
                .map_err(|e| FdLimitError::NofileRaiseFailed { target: to, source: e.to_string() })?;
            to
        }
        NofilePlan::ExceedsHard => {
            return Err(FdLimitError::NofileInsufficient {
                required,
                soft: soft_before,
                hard,
                num_shards,
                max_open_files,
                baseline: PROCESS_BASELINE_FDS,
                margin: FD_HEADROOM_MARGIN,
            });
        }
    };

    Ok(FdHeadroom { required, soft_before, soft, hard })
}
