//! Blind-oracle contract tests for `AttemptTracker::assess`. Authored against
//! the Phase 2 attempt-accounting contract only, never against the
//! implementation.
//!
//! Tested promise: stall accounting is PER SHARD. For Following/Promoting
//! (budget 4) a shard's zero-progress Retry results burn its own
//! consecutive-stall budget regardless of the stalled_awaiting_s3 flag, and
//! regardless of any other shard making progress. Boot (budget 1) counts only
//! settle-proven stalls (flag set): unflagged churn freezes Boot's count —
//! Boot has no TCP handoff to bail to, so it waits. Progress resets only that
//! shard's count; an Err freezes it. Fatal errors shut down, retriable-only
//! attempts bail as unreachable after 4, disk-full waits forever (per-shard:
//! a sibling's own stall evidence still bails the run).

use crate::sharded::catchup_attempts::{AttemptDecision as D, AttemptTracker};
use crate::sharded::connection_handler::CatchupCompletionMsg;
use celeriant_rotating_log::errors::open_or_create_error::OpenOrCreateError;
use celeriant_shard::error::s3_catchup_error::S3CatchupError;
use celeriant_shard::error::shard_fsync_error::ShardFsyncError;
use celeriant_shard::shard_wal_s3_catchup::{CatchupCompletion, CatchupRole, S3CatchupResult};

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
        let got = tracker.assess(attempt, role, &msgs);
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
