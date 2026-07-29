//! Attempt-level decision core for the S3 catchup orchestrator. Pure state +
//! classification so attempt sequences are drivable in tests; the side effects
//! (kicks, renewal, sleeps, shutdown broadcast) stay in `run_s3_catchup`.

use celeriant_distributed::node_status::NodeStatus;
use celeriant_shard::shard_wal_s3_catchup::{CatchupCompletion, CatchupRole};
use tracing::{error, info, warn};

use super::connection_handler::CatchupCompletionMsg;

/// Catchup role for the orchestrator's CURRENT status. Recomputed by shard 0
/// at the top of every attempt (its own status is the elections authority, so
/// this is not the per-data-shard ambient race the message-carried role
/// guards against): a promotion superseded mid-run demotes the next attempt
/// to Following instead of running promotion semantics on a stale role.
pub(crate) fn role_for_status(status: NodeStatus) -> CatchupRole {
    match status {
        s if s.is_promoting() => CatchupRole::Promoting,
        NodeStatus::BootCatchup => CatchupRole::Boot,
        _ => CatchupRole::Following,
    }
}

// Consecutive zero-progress catchup attempts tolerated before handing
// recovery back to the TCP/kick path. Real settled-stall patience is ~39s:
// 4 x ~6s drain barriers inside the invocations plus 3 x 5s inter-attempt
// sleeps (the bailing 4th attempt returns before sleeping). Zero-progress
// attempts always pay the sleep — the immediate re-attempt path requires
// every shard progressing — so even barrier-less gap-escape churn paces at
// >=5s/attempt. Kept at 4 rather than collapsed to Boot's 1: a stall bail
// from a challenge-path promotion surfaces as an election failure and panics
// (see scraps: challenge-caller StallBail escalation), so a smaller budget
// turns a transiently-fed promotion (deposed leader uploading on its stale
// in-window TTL) into a faster crash, and the flap risk of 1 under S3 outage
// recovery is unproven.
const MAX_STALLED_ATTEMPTS: u32 = 4;

// 4 rounds x 5s ~= 20s of confirmed S3 outage before bailing so a follower can resume via TCP.
const MAX_S3_UNREACHABLE_ROUNDS: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttemptDecision {
    /// Every shard caught up: catchup run succeeded.
    Caught,
    /// A shard hit a fatal error: the node must shut down.
    Shutdown,
    /// Consecutive stalled attempts exhausted the role's budget: hand
    /// recovery back to the TCP/kick path.
    StallBail,
    /// S3 unreachable across consecutive attempts: bail so a heartbeated
    /// follower can resume via TCP.
    UnreachableBail,
    /// Neither caught nor bailing: renew (if promoting), sleep, re-attempt.
    Continue,
}

/// True only when EVERY shard is riding a live feed: applied batches, or
/// already Caught. The caller skips the inter-attempt sleep for exactly this
/// case (racing an uploader). Any zero-progress Retry or any Err means some
/// shard is waiting on the view, not racing it — that shard's bail bound is
/// paced in attempts-with-sleeps, and an unpaced re-attempt would compress it
/// below its commented guarantee.
pub(crate) fn attempt_racing_live_feed(results: &[CatchupCompletionMsg]) -> bool {
    results.iter().all(|msg| match &msg.result {
        Ok(r) => r.batches_applied > 0 || r.completion == CatchupCompletion::Caught,
        Err(_) => false,
    })
}

/// Cross-attempt counters for one `run_s3_catchup` invocation.
///
/// Stall accounting is PER SHARD: one shard riding a live feed (a batch
/// applied every attempt) must not mask another shard that is permanently
/// stalled — under node-level accounting the live shard reset the counter
/// every attempt and the node looped in catching-up forever, rejecting the
/// TCP replication whose absence sustained the feed. A shard's consecutive
/// zero-progress attempts cannot converge by retrying catchup alone
/// (resolution needs the leader's TCP/kick path or a fresh upload), so each
/// shard's run is bounded on its own evidence; later kicks re-enter.
pub(crate) struct AttemptTracker {
    per_shard_stalled: Vec<u32>,
    consecutive_s3_unreachable: u32,
}

impl AttemptTracker {
    pub(crate) fn new() -> Self {
        AttemptTracker {
            per_shard_stalled: Vec::new(),
            consecutive_s3_unreachable: 0,
        }
    }

    fn stall_count_mut(&mut self, shard_id: usize) -> &mut u32 {
        if shard_id >= self.per_shard_stalled.len() {
            self.per_shard_stalled.resize(shard_id + 1, 0);
        }
        &mut self.per_shard_stalled[shard_id]
    }

    /// Classify one attempt's per-shard results and advance the counters.
    pub(crate) fn assess(&mut self, attempt: u32, role: CatchupRole, results: &[CatchupCompletionMsg]) -> AttemptDecision {
        // Split so a pure-S3-error round (no contact) counts toward the unreachable bound, while
        // any S3 contact (undrained) resets it. Disk-full is its own class:
        // retried indefinitely (space gets recovered; rotation already alarms
        // via celeriant_rotation_out_of_space_total) and never counted toward
        // the unreachable bound; S3 is fine, the local disk isn't. The
        // indefinite-retry promise is per-shard: a sibling shard's own stall
        // evidence still bails the run.
        let mut has_undrained = false;
        let mut has_s3_error = false;
        let mut has_disk_full = false;
        let mut has_fatal = false;

        for msg in results {
            match &msg.result {
                Ok(r) => match r.completion {
                    CatchupCompletion::Caught => {
                        info!(
                            shard_id = msg.shard_id,
                            batches_applied = r.batches_applied,
                            bytes_downloaded = r.bytes_downloaded,
                            rounds = r.rounds,
                            "S3 catchup caught up for shard"
                        );
                    }
                    CatchupCompletion::Retry => {
                        warn!(
                            shard_id = msg.shard_id,
                            batches_applied = r.batches_applied,
                            bytes_downloaded = r.bytes_downloaded,
                            rounds = r.rounds,
                            "S3 catchup did not drain for shard, will retry"
                        );
                        has_undrained = true;
                    }
                },
                Err(e) if e.is_disk_full() => {
                    warn!(shard_id = msg.shard_id, error = ?e, "S3 catchup blocked on full disk, will retry");
                    has_disk_full = true;
                }
                Err(e) if e.is_retriable() => {
                    warn!(shard_id = msg.shard_id, error = ?e, "S3 catchup retriable error, will retry");
                    has_s3_error = true;
                }
                Err(e) => {
                    error!(shard_id = msg.shard_id, error = ?e, "S3 catchup fatal error, shutting down");
                    has_fatal = true;
                }
            }
        }

        if has_fatal {
            return AttemptDecision::Shutdown;
        }

        if !has_undrained && !has_s3_error && !has_disk_full {
            return AttemptDecision::Caught;
        }

        // Boot's budget of 1 counts only settle-proven stalls: the stalled
        // flag means the backlog drained and the view held stable through a
        // full settle window — done waiting, proceed to the election. A
        // round-cap exit under churn proves nothing about settling, and Boot
        // has no TCP handoff a bail could reach (BootCatchup rejects
        // heartbeats and replication; a bailed boot election re-enters as
        // Boot), so churn freezes Boot's count and the node keeps waiting.
        let max_stalled = if role == CatchupRole::Boot { 1 } else { MAX_STALLED_ATTEMPTS };
        let mut bailing_shard: Option<(usize, u32)> = None;
        for msg in results {
            // For Following/Promoting a zero-progress Retry accrues regardless
            // of stalled_awaiting_s3: churn proves the hole is not closing
            // just as a settle-proven stall does, and both roles can hand
            // recovery to TCP. Progress or Caught resets only this shard's
            // count. An Err freezes it — an error attempt proves nothing about
            // this shard's wedge either way, and resetting on it would let a
            // flapping S3 pin the node in catchup indefinitely.
            if let Ok(r) = &msg.result {
                if r.completion == CatchupCompletion::Retry && r.batches_applied == 0 {
                    if role != CatchupRole::Boot || r.stalled_awaiting_s3 {
                        let count = self.stall_count_mut(msg.shard_id);
                        *count += 1;
                        if *count >= max_stalled {
                            bailing_shard.get_or_insert((msg.shard_id, *count));
                        }
                    }
                } else {
                    *self.stall_count_mut(msg.shard_id) = 0;
                }
            }
        }
        if let Some((shard_id, consecutive_stalled)) = bailing_shard {
            warn!(
                attempt,
                shard_id,
                consecutive_stalled,
                "S3 catchup shard made zero progress across its stall budget; bailing so TCP/kick recovery can proceed"
            );
            metrics::counter!("celeriant_s3_catchup_stall_bail_total").increment(1);
            return AttemptDecision::StallBail;
        }

        if has_s3_error && !has_undrained {
            self.consecutive_s3_unreachable += 1;
            if self.consecutive_s3_unreachable >= MAX_S3_UNREACHABLE_ROUNDS {
                warn!(attempt, consecutive_s3_unreachable = self.consecutive_s3_unreachable, "S3 unreachable across consecutive catchup rounds; bailing so a heartbeated follower can resume via TCP");
                return AttemptDecision::UnreachableBail;
            }
        } else {
            self.consecutive_s3_unreachable = 0;
        }

        AttemptDecision::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The immediate re-attempt is reserved for a whole node racing a live
    /// feed: any shard waiting (zero-progress Retry) or erroring keeps the
    /// paced path, so no bail bound is compressed below its guarantee.
    #[test]
    fn racing_live_feed_requires_every_shard_progressing() {
        use celeriant_shard::error::s3_catchup_error::S3CatchupError;
        use celeriant_shard::shard_wal_s3_catchup::S3CatchupResult;
        use CatchupCompletion::{Caught, Retry};

        let res = |completion: CatchupCompletion, applied: u64, stalled: bool| -> Result<S3CatchupResult, S3CatchupError> {
            Ok(S3CatchupResult { batches_applied: applied, bytes_downloaded: 0, rounds: 1, completion, stalled_awaiting_s3: stalled })
        };
        let msg = |shard_id: usize, result: Result<S3CatchupResult, S3CatchupError>| CatchupCompletionMsg { shard_id, attempt: 1, result };

        // all shards applying → racing
        assert!(attempt_racing_live_feed(&[msg(0, res(Retry, 2, false)), msg(1, res(Retry, 1, false))]));
        // progress + an already-caught shard (applied 0) → racing
        assert!(attempt_racing_live_feed(&[msg(0, res(Retry, 2, false)), msg(1, res(Caught, 0, false))]));
        // progress + a zero-progress Retry (stalled or churn) → not racing
        assert!(!attempt_racing_live_feed(&[msg(0, res(Retry, 2, false)), msg(1, res(Retry, 0, true))]));
        assert!(!attempt_racing_live_feed(&[msg(0, res(Retry, 2, false)), msg(1, res(Retry, 0, false))]));
        // progress + any Err → not racing
        assert!(!attempt_racing_live_feed(&[
            msg(0, res(Retry, 2, false)),
            msg(1, Err(S3CatchupError::SidecarUnavailable)),
        ]));
        // all zero-progress → not racing
        assert!(!attempt_racing_live_feed(&[msg(0, res(Retry, 0, true))]));
    }

    /// Only a live Promoting status runs promotion semantics; BootCatchup is
    /// Boot; every other status — including Fenced and a mid-catchup
    /// FollowerCatchingUp — must catch up as Following (fail-closed: Following
    /// keeps full patience and can yield to TCP, never drains as a leader).
    #[test]
    fn role_for_status_mapping() {
        use NodeStatus::*;
        let cases: &[(NodeStatus, CatchupRole)] = &[
            (Promoting { lease_epoch: 3 }, CatchupRole::Promoting),
            (BootCatchup, CatchupRole::Boot),
            (Leader { lease_epoch: 3 }, CatchupRole::Following),
            (Follower { leader_lease_epoch: 3 }, CatchupRole::Following),
            (FollowerCatchingUp { leader_lease_epoch: 3 }, CatchupRole::Following),
            (Fenced, CatchupRole::Following),
            (Standalone, CatchupRole::Following),
        ];
        for &(status, expected) in cases {
            assert_eq!(role_for_status(status), expected, "status {status:?}");
        }
    }

}
