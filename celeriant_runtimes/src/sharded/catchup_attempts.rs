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
// recovery back to the TCP/kick path. Real settled-stall patience is ~21s:
// 4 x 1.5s drain barriers inside the invocations plus 3 x 5s inter-attempt
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
    pub(crate) fn assess(&mut self, attempt: u32, role: CatchupRole, results: &[CatchupCompletionMsg], timed_out: &[usize]) -> AttemptDecision {
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

        if !timed_out.is_empty() {
            metrics::counter!("celeriant_s3_catchup_barrier_timeout_total").increment(timed_out.len() as u64);
            if role != CatchupRole::Boot {
                error!(attempt, ?timed_out, ?role, "S3 catchup completion barrier timed out; bailing the catchup run");
                return AttemptDecision::StallBail;
            }
            error!(attempt, ?timed_out, "S3 catchup completion barrier timed out; Boot has no TCP handoff, waiting");
            return AttemptDecision::Continue;
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
        
    use crate::sharded::catchup_attempts::{AttemptDecision as D, AttemptTracker};
    use crate::sharded::connection_handler::CatchupCompletionMsg;
    use celeriant_rotating_log::errors::open_or_create_error::OpenOrCreateError;
    use celeriant_shard::error::s3_catchup_error::S3CatchupError;
    use celeriant_shard::error::shard_fsync_error::ShardFsyncError;
    use celeriant_shard::shard_wal_s3_catchup::{CatchupCompletion, CatchupRole, S3CatchupResult};

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

    /// A shard that misses the completion barrier is inconclusive, not clean.
    /// The reporting shards here are all Caught, so a classifier that ignored
    /// `timed_out` would return Caught and send the node on to its post-catchup
    /// election with one shard's data state unknown.
    #[test]
    fn barrier_timeout_never_reads_as_caught() {
        use celeriant_shard::shard_wal_s3_catchup::S3CatchupResult;
        let caught = |shard_id: usize| CatchupCompletionMsg {
            shard_id,
            attempt: 1,
            result: Ok(S3CatchupResult { batches_applied: 0, bytes_downloaded: 0, rounds: 1, completion: CatchupCompletion::Caught, stalled_awaiting_s3: false }),
        };

        // Following and Promoting bail to the TCP/kick path.
        for role in [CatchupRole::Following, CatchupRole::Promoting] {
            let mut tracker = AttemptTracker::new();
            assert_eq!(tracker.assess(1, role, &[caught(0), caught(1)], &[2]), AttemptDecision::StallBail, "role {role:?}");
        }

        // Boot has no TCP handoff, so it waits instead — but still never Caught.
        let mut tracker = AttemptTracker::new();
        assert_eq!(tracker.assess(1, CatchupRole::Boot, &[caught(0), caught(1)], &[2]), AttemptDecision::Continue);
        assert_eq!(tracker.assess(2, CatchupRole::Boot, &[caught(0), caught(1)], &[2]), AttemptDecision::Continue);
    }

    /// A fatal result outranks a barrier timeout. FAIL means a slow sibling
    /// masks a WAL-integrity fault on a shard that DID report: Boot retries the
    /// fatal indefinitely, and Following/Promoting resume on it — the latter
    /// panicking with the wrong reason.
    #[test]
    fn fatal_result_outranks_a_peer_barrier_timeout() {
        use celeriant_shard::error::s3_catchup_error::S3CatchupError;
        use celeriant_shard::error::shard_fsync_error::ShardFsyncError;

        let fatal = || {
            let e = S3CatchupError::TruncationFailed(ShardFsyncError::ActiveWriteFileUnavailable);
            assert!(!e.is_retriable() && !e.is_disk_full(), "fixture must be the fatal class");
            CatchupCompletionMsg { shard_id: 0, attempt: 1, result: Err(e) }
        };

        // Baseline: no timeout, the fatal shuts the node down as designed.
        let mut tracker = AttemptTracker::new();
        assert_eq!(tracker.assess(1, CatchupRole::Boot, &[fatal()], &[]), AttemptDecision::Shutdown);

        // Same fatal, but a sibling missed the barrier.
        for role in [CatchupRole::Boot, CatchupRole::Following, CatchupRole::Promoting] {
            let mut tracker = AttemptTracker::new();
            assert_eq!(tracker.assess(1, role, &[fatal()], &[1]), AttemptDecision::Shutdown, "role {role:?}");
        }
    }

    /// A timed-out shard must not burn the reporting shards' stall budgets: the
    /// attempt says nothing about them either. FAIL means a run that recovers
    /// from a transient mesh stall arrives at the next real stall pre-charged.
    #[test]
    fn barrier_timeout_leaves_peer_stall_counts_alone() {
        use celeriant_shard::shard_wal_s3_catchup::S3CatchupResult;
        let stalled = |shard_id: usize| CatchupCompletionMsg {
            shard_id,
            attempt: 1,
            result: Ok(S3CatchupResult { batches_applied: 0, bytes_downloaded: 0, rounds: 1, completion: CatchupCompletion::Retry, stalled_awaiting_s3: true }),
        };

        let mut tracker = AttemptTracker::new();
        for attempt in 1..=3 {
            assert_eq!(tracker.assess(attempt, CatchupRole::Boot, &[stalled(0)], &[1]), AttemptDecision::Continue);
        }
        // Boot's budget is 1, so shard 0 bails on its FIRST counted stall. If the
        // three timed-out attempts had charged it, this would already have bailed.
        assert_eq!(tracker.assess(4, CatchupRole::Boot, &[stalled(0), stalled(1)], &[]), AttemptDecision::StallBail);
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

    fn ok_msg(shard_id: usize, batches_applied: u64, completion: CatchupCompletion, stalled_awaiting_s3: bool) -> CatchupCompletionMsg {
        CatchupCompletionMsg {
            shard_id,
            attempt: 0,
            result: Ok(S3CatchupResult {
                batches_applied,
                bytes_downloaded: batches_applied * 4096,
                rounds: 1,
                completion,
                stalled_awaiting_s3,
            }),
        }
    }

    /// Zero-progress Retry, drained S3 view: the classic stall.
    fn stalled(shard_id: usize) -> CatchupCompletionMsg {
        ok_msg(shard_id, 0, CatchupCompletion::Retry, true)
    }

    /// Zero-progress Retry, round-cap exit under churn (stalled_awaiting_s3=false).
    fn churn_retry(shard_id: usize) -> CatchupCompletionMsg {
        ok_msg(shard_id, 0, CatchupCompletion::Retry, false)
    }

    /// Applied batches but not caught yet: real progress.
    fn progress(shard_id: usize) -> CatchupCompletionMsg {
        ok_msg(shard_id, 3, CatchupCompletion::Retry, false)
    }

    fn caught(shard_id: usize, batches_applied: u64) -> CatchupCompletionMsg {
        ok_msg(shard_id, batches_applied, CatchupCompletion::Caught, false)
    }

    fn err_msg(shard_id: usize, e: S3CatchupError) -> CatchupCompletionMsg {
        CatchupCompletionMsg { shard_id, attempt: 0, result: Err(e) }
    }

    fn retriable_err(shard_id: usize) -> CatchupCompletionMsg {
        let e = S3CatchupError::S3ListFailed { prefix: "shard".to_string(), message: "connect timeout".to_string() };
        assert!(e.is_retriable() && !e.is_disk_full(), "fixture must be retriable");
        err_msg(shard_id, e)
    }

    fn disk_full_err(shard_id: usize) -> CatchupCompletionMsg {
        let e = S3CatchupError::FsyncFailed(ShardFsyncError::UnableToRotateToNewLogSegmentFile(
            OpenOrCreateError::OutOfSpace { log_id: 2, path: "log_2.wal".into(), preallocate_bytes: 1 << 27 },
        ));
        assert!(e.is_disk_full(), "fixture must be disk-full");
        err_msg(shard_id, e)
    }

    fn fatal_err(shard_id: usize) -> CatchupCompletionMsg {
        let e = S3CatchupError::SidecarUnavailable;
        assert!(!e.is_retriable() && !e.is_disk_full(), "fixture must be fatal");
        err_msg(shard_id, e)
    }

    /// Drive one tracker through a sequence of attempts, asserting each decision.
    fn assert_seq(role: CatchupRole, steps: Vec<(Vec<CatchupCompletionMsg>, D)>) {
        let mut tracker = AttemptTracker::new();
        for (i, (msgs, expected)) in steps.into_iter().enumerate() {
            let attempt = i as u32 + 1;
            let got = tracker.assess(attempt, role, &msgs, &[]);
            assert_eq!(got, expected, "unexpected decision at attempt {attempt}");
        }
    }

    // ── Per-shard stall accounting (the confirmed bug class) ──

    /// PASS: shard 1 stalling every attempt trips StallBail at its 4th consecutive
    /// zero-progress attempt (Following budget 4) even though shard 0 applies
    /// batches every attempt. FAIL: shard 0's live feed masks shard 1's stall and
    /// the loop never bails — the node spins forever behind a dead shard.
    #[test]
    fn stalled_shard_bails_despite_live_peer_progress() {
        let step = || vec![progress(0), stalled(1)];
        assert_seq(CatchupRole::Following, vec![
            (step(), D::Continue),
            (step(), D::Continue),
            (step(), D::Continue),
            (step(), D::StallBail),
        ]);
    }

    /// PASS: Promoting shares the 4-attempt stall budget: three stalls Continue,
    /// the 4th bails. FAIL: promotion either bails early or hangs past its budget.
    #[test]
    fn promoting_stall_budget_is_four() {
        assert_seq(CatchupRole::Promoting, vec![
            (vec![stalled(0)], D::Continue),
            (vec![stalled(0)], D::Continue),
            (vec![stalled(0)], D::Continue),
            (vec![stalled(0)], D::StallBail),
        ]);
    }

    /// PASS: under Boot's budget of 1, a single settle-proven stall bails
    /// immediately. FAIL: boot grants a stalled shard extra attempts it has no
    /// budget for.
    #[test]
    fn boot_single_zero_progress_retry_bails_immediately() {
        assert_seq(CatchupRole::Boot, vec![(vec![stalled(0)], D::StallBail)]);
    }

    /// PASS: unflagged churn never burns Boot's budget (a churning view proves
    /// nothing about settling and Boot has nowhere to bail to), yet a
    /// settle-proven stall still bails immediately even after arbitrary churn.
    /// FAIL: churn either bails Boot into the election crash-loop or poisons the
    /// count so the real stall signal is late.
    #[test]
    fn boot_churn_waits_then_settled_stall_bails() {
        assert_seq(CatchupRole::Boot, vec![
            (vec![churn_retry(0)], D::Continue),
            (vec![churn_retry(0)], D::Continue),
            (vec![churn_retry(0)], D::Continue),
            (vec![churn_retry(0)], D::Continue),
            (vec![churn_retry(0)], D::Continue),
            (vec![churn_retry(0)], D::Continue),
            (vec![stalled(0)], D::StallBail),
        ]);
    }

    /// PASS: zero-progress Retries with stalled_awaiting_s3=false (round-cap exit
    /// under churn) burn the stall budget exactly like flagged stalls. FAIL: the
    /// flag gates accounting and unflagged spinning never trips the bail.
    #[test]
    fn churn_retry_accrues_stalls_like_flagged_following() {
        assert_seq(CatchupRole::Following, vec![
            (vec![churn_retry(0)], D::Continue),
            (vec![churn_retry(0)], D::Continue),
            (vec![churn_retry(0)], D::Continue),
            (vec![churn_retry(0)], D::StallBail),
        ]);
    }

    /// PASS: shard 1's own progress at attempt 4 resets its count, so the bail
    /// needs 4 FRESH consecutive stalls (attempt 8) — and shard 0's constant
    /// progress never touches shard 1's count. FAIL: either the reset leaks across
    /// shards (bail far too late or never) or the count survives shard 1's own
    /// progress (bail at attempt 5).
    #[test]
    fn own_progress_resets_stall_count_requiring_fresh_budget() {
        let stall_step = || vec![progress(0), stalled(1)];
        assert_seq(CatchupRole::Following, vec![
            (stall_step(), D::Continue),
            (stall_step(), D::Continue),
            (stall_step(), D::Continue),
            (vec![progress(0), progress(1)], D::Continue),
            (stall_step(), D::Continue),
            (stall_step(), D::Continue),
            (stall_step(), D::Continue),
            (stall_step(), D::StallBail),
        ]);
    }

    /// PASS: flagged stalls and unflagged churn draw on ONE shared per-shard
    /// budget — alternating them still bails at the 4th consecutive zero-progress
    /// attempt (Following). FAIL: the two kinds are tracked separately and reset
    /// each other, reintroducing intermittent-lull starvation.
    #[test]
    fn interleaved_churn_and_stalled_share_one_budget_following() {
        assert_seq(CatchupRole::Following, vec![
            (vec![stalled(0)], D::Continue),
            (vec![churn_retry(0)], D::Continue),
            (vec![stalled(0)], D::Continue),
            (vec![churn_retry(0)], D::StallBail),
        ]);
    }

    /// PASS: a peer shard blocked on a full disk does not shield a sibling's own
    /// stall evidence — the sibling still bails the run at its budget (Following).
    /// FAIL: disk-full anywhere freezes ALL stall accounting and the wedged
    /// sibling spins forever.
    #[test]
    fn sibling_stall_bails_despite_disk_full_peer() {
        let step = || vec![disk_full_err(0), stalled(1)];
        assert_seq(CatchupRole::Following, vec![
            (step(), D::Continue),
            (step(), D::Continue),
            (step(), D::Continue),
            (step(), D::StallBail),
        ]);
    }

    /// PASS: an Err attempt freezes a shard's stall count — 2 stalls, an error,
    /// 2 more stalls totals 4 and bails at the 5th call (Following). FAIL: the
    /// error resets the count (bail late/never) or increments it (bail early).
    #[test]
    fn err_freezes_stall_count() {
        for err in [retriable_err(1), disk_full_err(1)] {
            assert_seq(CatchupRole::Following, vec![
                (vec![stalled(1)], D::Continue),
                (vec![stalled(1)], D::Continue),
                (vec![err], D::Continue),
                (vec![stalled(1)], D::Continue),
                (vec![stalled(1)], D::StallBail),
            ]);
        }
    }

    // ── Pinned unchanged semantics ──

    /// PASS: every shard reporting Caught yields Caught, whether or not batches
    /// were applied on the final attempt. FAIL: the rewrite lost the success path.
    #[test]
    fn all_caught_yields_caught() {
        assert_seq(CatchupRole::Following, vec![(vec![caught(0, 0), caught(1, 7)], D::Caught)]);
        assert_seq(CatchupRole::Boot, vec![(vec![caught(0, 0)], D::Caught)]);
    }

    /// PASS: a fatal (non-retriable, non-disk-full) error yields Shutdown at once,
    /// even beside a healthy progressing shard. FAIL: fatal errors are retried or
    /// masked by peer progress.
    #[test]
    fn fatal_error_yields_shutdown() {
        assert_seq(CatchupRole::Following, vec![(vec![progress(0), fatal_err(1)], D::Shutdown)]);
        assert_seq(CatchupRole::Promoting, vec![(vec![fatal_err(0)], D::Shutdown)]);
    }

    /// PASS: attempts whose only problem is retriable S3 errors Continue three
    /// times, then UnreachableBail on the 4th consecutive. FAIL: an unreachable
    /// S3 is retried forever or given up on early.
    #[test]
    fn retriable_only_unreachable_bails_on_fourth() {
        assert_seq(CatchupRole::Following, vec![
            (vec![retriable_err(0)], D::Continue),
            (vec![retriable_err(0)], D::Continue),
            (vec![retriable_err(0)], D::Continue),
            (vec![retriable_err(0)], D::UnreachableBail),
        ]);
    }

    /// PASS: an attempt that reaches S3 (a Retry result present) resets the
    /// unreachable counter, so the bail again needs 4 fresh consecutive
    /// error-only attempts. FAIL: stale unreachable counts trip the bail after
    /// contact was re-established.
    #[test]
    fn s3_contact_resets_unreachable_counter() {
        assert_seq(CatchupRole::Following, vec![
            (vec![retriable_err(0)], D::Continue),
            (vec![retriable_err(0)], D::Continue),
            (vec![retriable_err(0)], D::Continue),
            (vec![progress(0)], D::Continue),
            (vec![retriable_err(0)], D::Continue),
            (vec![retriable_err(0)], D::Continue),
            (vec![retriable_err(0)], D::Continue),
            (vec![retriable_err(0)], D::UnreachableBail),
        ]);
    }

    /// PASS: disk-full errors alone Continue indefinitely — well past the
    /// unreachable budget — never UnreachableBail, never Shutdown. FAIL: a full
    /// disk is misclassified as unreachable S3 or as fatal.
    #[test]
    fn disk_full_alone_continues_indefinitely() {
        let steps = (0..8).map(|_| (vec![disk_full_err(0)], D::Continue)).collect();
        assert_seq(CatchupRole::Following, steps);
    }

    /// PASS: a shard applying batches every attempt never trips StallBail, no
    /// matter how many attempts it takes. FAIL: attempt count alone is treated as
    /// a stall.
    #[test]
    fn progressing_shard_never_stall_bails() {
        let steps = (0..6).map(|_| (vec![progress(0)], D::Continue)).collect();
        assert_seq(CatchupRole::Following, steps);
    }


}
