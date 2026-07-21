//! Blind-oracle contract tests for the S3 catchup surface. Authored against
//! the catchup contract and `catchup_test_support` fixtures only, never
//! against the implementation.
//!
//! Tested promise: a catchup round that made no progress must be fail-closed.
//! It may only declare Caught after proving no more data is coming (empty S3,
//! or a settle/re-list barrier over a strictly-behind view). Data visible
//! ahead of the local position, or files landing late while the leader's
//! upload queue drains, must never be silently skipped.

use std::rc::Rc;

use bytes::Bytes;
use celeriant_distributed::paths::fallback_batch_path;
use celeriant_wal::constants::GENESIS_HASH;
use glommio::{LocalExecutorBuilder, Placement};

use crate::catchup_test_support::*;
use crate::shard_wal_s3_catchup::{no_progress_verdict, CatchupCompletion, CatchupRole, NoProgressVerdict};

macro_rules! glommio_test {
    ($body:expr) => {
        LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move { $body })
            .unwrap()
            .join()
            .unwrap()
    };
}

/// Name a completion without requiring Debug/PartialEq on the enum.
fn completion_name(c: &CatchupCompletion) -> &'static str {
    match c {
        CatchupCompletion::Caught => "Caught",
        CatchupCompletion::Retry => "Retry",
    }
}

fn verdict_name(v: &NoProgressVerdict) -> &'static str {
    match v {
        NoProgressVerdict::CaughtNow => "CaughtNow",
        NoProgressVerdict::DrainThenCaught => "DrainThenCaught",
        NoProgressVerdict::AwaitMore => "AwaitMore",
    }
}

// ── C1: empty S3 is the fast path ──

/// PASS: catchup against an empty S3 returns Caught immediately with nothing
/// applied or downloaded. FAIL: the fail-closed fix over-corrected and an
/// empty bucket no longer short-circuits.
#[test]
fn empty_s3_declares_caught_immediately() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        let wal_before = tc.wal_seq();

        let dl = Rc::new(MockDownloader::new());
        let res = tc.catchup_with_gap(&dl, 0, None, None).await.expect("catchup must not error on empty S3");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "empty S3 must be Caught, got {}",
            completion_name(&res.completion)
        );
        assert_eq!(res.batches_applied, 0);
        assert_eq!(res.bytes_downloaded, 0);
        assert_eq!(tc.wal_seq(), wal_before, "empty S3 must not move the local wal");
        tc.close().await;
    });
}

// ── C2: late-landing covering file ──

/// PASS: a kicked follower that KNOWS the leader holds up to wal 15 (recorded
/// from its last rejected replication batch) does not declare Caught on a
/// behind-only view; it awaits the leader's draining upload queue, applies the
/// late-landing 11..=15 file, and only then exits Caught. FAIL: catchup
/// declares Caught while still behind the observed leader tip and the acked
/// writes in the late file never reach the rejoiner.
/// (Contract amended to carry the observed-leader target; see the C6 note.)
#[test]
fn late_landing_covering_file_is_consumed_before_caught() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        tc.seed_chain(10).await;
        assert_eq!(tc.wal_seq(), 10, "scaffolding: seed must land local wal at 10");
        let tip = tc.tip_hash();

        let dl = Rc::new(MockDownloader::new());
        // Initial view: only a file strictly behind the local position.
        let (behind_path, behind_bytes) = make_fallback_batch(0, 1, 9, GENESIS_HASH);
        dl.insert(behind_path, behind_bytes);
        // The covering file (11..15, chained on the local tip) lands late.
        // Registered on several consecutive list calls to be robust to the
        // exact number of lists the settle barrier performs.
        let (late_path, late_bytes) = make_fallback_batch(0, 11, 15, tip);
        for call in 1..=6 {
            let (p, b) = (late_path.clone(), late_bytes.clone());
            dl.on_list(call, move |dl| dl.insert(p.clone(), b.clone()));
        }

        let res = tc.catchup_with_target(&dl, 0, 15).await.expect("catchup must not error");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "late file consumed means Caught, got {}",
            completion_name(&res.completion)
        );
        assert!(res.batches_applied >= 1, "the late-landing covering file must be applied, applied={}", res.batches_applied);
        assert_eq!(tc.wal_seq(), 15, "local wal must advance to the late file's end");
        tc.close().await;
    });
}

// ── C3: data visible ahead, hole at position ──

/// PASS: with a file visible AHEAD of the local position and nothing covering
/// it, catchup returns Retry (loud) without applying or moving the wal, under
/// both the deployed gap config (None) and a large explicit cap.
/// FAIL: catchup silently declares Caught and the hole is never re-driven.
#[test]
fn visible_ahead_with_hole_at_position_is_never_silent_caught() {
    glommio_test!({
        for gap in [None, Some(1_000_000u64)] {
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            tc.seed_chain(10).await;

            let dl = Rc::new(MockDownloader::new());
            // 20..25 proves more data exists, but 11..19 is a hole.
            let (path, bytes) = make_fallback_batch(0, 20, 25, GENESIS_HASH);
            dl.insert(path, bytes);

            let res = tc.catchup_with_gap(&dl, 0, None, gap).await.expect("catchup must not error");

            assert!(
                matches!(res.completion, CatchupCompletion::Retry),
                "gap={gap:?}: data visible ahead of a hole must be Retry, got {}",
                completion_name(&res.completion)
            );
            assert_eq!(res.batches_applied, 0, "gap={gap:?}: nothing covers the position, nothing may apply");
            assert_eq!(tc.wal_seq(), 10, "gap={gap:?}: local wal must not move over a hole");
            tc.close().await;
        }
    });
}

// ── C4: stable behind-only view stays bounded ──

/// PASS: a fresh catchup over a view containing only files strictly behind the
/// local position (and nothing ever landing) returns Caught with nothing
/// applied, in bounded time. FAIL: the fail-closed fix wedges catchup open on
/// already-consumed history.
#[test]
fn stable_behind_only_view_declares_caught_bounded() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        tc.seed_chain(10).await;

        let dl = Rc::new(MockDownloader::new());
        let (path, bytes) = make_fallback_batch(0, 1, 9, GENESIS_HASH);
        dl.insert(path, bytes);

        let res = tc.catchup_with_gap(&dl, 0, None, None).await.expect("catchup must not error");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "a stable strictly-behind view must settle to Caught, got {}",
            completion_name(&res.completion)
        );
        assert_eq!(res.batches_applied, 0);
        assert_eq!(tc.wal_seq(), 10, "behind-only files must not move the local wal");
        tc.close().await;
    });
}

// ── C5: poison file cannot wedge ──

/// PASS: a corrupt object at a path claiming to cover the next position is
/// skipped and catchup completes Caught, bounded, with nothing applied.
/// FAIL: the fail-closed logic counts the poison file as forever-unprocessed
/// evidence and holds catchup open (or errors out).
#[test]
fn poison_file_claiming_coverage_cannot_wedge_catchup() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        tc.seed_chain(10).await;

        let dl = Rc::new(MockDownloader::new());
        dl.insert(fallback_batch_path(0, 11, 15, 0), Bytes::from_static(b"corrupt"));

        let res = tc.catchup_with_gap(&dl, 0, None, None).await.expect("poison file must not error catchup out");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "poison file must be skipped and catchup Caught, got {}",
            completion_name(&res.completion)
        );
        assert_eq!(res.batches_applied, 0, "garbage bytes must not apply");
        assert_eq!(tc.wal_seq(), 10, "garbage bytes must not move the local wal");
        tc.close().await;
    });
}

// ── C6: no-progress verdict truth table ──
//
// Contract amendment: the original two-input contract ("late-landing files
// must always be awaited") was falsified by the chaos oracle. A follower that
// never exits catchup under live load rejects TCP and forces the entire
// workload onto S3 fallback (a NoS3Fallbacks baseline red). The refined
// contract keys patience on the node's role and its position vs the last
// OBSERVED leader tip: a follower at/past everything the leader was known to
// hold exits immediately (TCP owns the residue); behind it, or with data
// visible ahead, it never claims Caught. A promoting leader-elect has no TCP
// fallback and always settles the view first.

/// PASS: the pure no-progress decision matches the fail-closed contract on
/// every input row. FAIL: some row decides fail-open (Caught while behind the
/// observed leader tip or over data ahead) or fail-stuck (a follower at the
/// known tip refusing to hand back to TCP).
#[test]
fn no_progress_verdict_truth_table() {
    use CatchupRole::{Boot, Following, Promoting};
    // (any_peer_candidates_visible, unprocessed_at_or_ahead, role, next_beyond_observed_leader, expected)
    let rows = [
        // Boot maps like Following in the no-progress verdict (it differs only
        // at the live-tail gate, which is upstream of this decision). Its
        // observed target is always 0, so the behind-target row is unreachable
        // in practice; pinned anyway, fail-closed dominates.
        (false, false, Boot, true, "CaughtNow"),
        (true, false, Boot, true, "CaughtNow"),
        (true, true, Boot, true, "AwaitMore"),
        (true, false, Boot, false, "AwaitMore"),
        // Empty view: caught unless a follower knows the leader holds more.
        (false, false, Following, true, "CaughtNow"),
        (false, false, Promoting, true, "CaughtNow"),
        (false, false, Following, false, "AwaitMore"), // kicked before the first file landed
        // Behind-only view: promoting settles; a follower keys on the target.
        (true, false, Promoting, true, "DrainThenCaught"),
        (true, false, Following, true, "CaughtNow"),   // at/past target: TCP owns the residue
        (true, false, Following, false, "AwaitMore"),  // behind target: stay for the queue drain
        // Data at-or-ahead: never a silent Caught, any role, any target.
        (true, true, Following, true, "AwaitMore"),
        (true, true, Following, false, "AwaitMore"),
        (true, true, Promoting, true, "AwaitMore"),
        (false, true, Following, true, "AwaitMore"),   // unreachable combination: fail-closed dominates
    ];
    for (visible, ahead, role, beyond, expected) in rows {
        let got = verdict_name(&no_progress_verdict(visible, ahead, role, beyond));
        assert_eq!(
            got, expected,
            "no_progress_verdict(visible={visible}, ahead={ahead}, role={role:?}, next_beyond_observed_leader={beyond}) must be {expected}, got {got}"
        );
    }
}

// ── C7: follower at the observed tip hands back to TCP (anti-storm) ──

/// Implementer-authored pin (not blind): encodes a NoS3Fallbacks baseline
/// red from chaos. PASS: a follower that has consumed up to the leader tip
/// it observed exits Caught immediately. It does NOT sit in a settle window
/// scooping up whatever the (still-uploading) leader lands next: while a
/// follower is in catchup it rejects TCP and every leader commit is forced
/// onto S3 fallback, so lingering here is a self-sustaining storm.
/// FAIL: catchup keeps consuming past its target and the exit never comes
/// under live load.
#[test]
fn following_at_observed_tip_exits_despite_live_uploads() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;

        let dl = Rc::new(MockDownloader::new());
        let (p, d) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
        dl.insert(p, d);
        // Live leader keeps uploading: a chained 4..=6 lands on every list
        // call after the first. A follower at its target must not consume it.
        let lsc = tc.log_segments_cache.clone();
        for call in 1..=8 {
            let lsc = lsc.clone();
            dl.on_list(call, move |dl| {
                let tip = lsc.active().metadata.borrow().write.tip_hash;
                let (p, d) = make_fallback_batch(0, 4, 6, tip);
                dl.insert(p, d);
            });
        }

        let res = tc.catchup_with_target(&dl, 0, 3).await.expect("catchup must not error");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "at the observed leader tip the follower must be Caught, got {}",
            completion_name(&res.completion)
        );
        assert_eq!(tc.wal_seq(), 3, "must stop at the observed tip and leave live traffic to TCP");
        tc.close().await;
    });
}

// ── C8: untaught follower with a large visible backlog consumes it ──

/// Implementer-authored pin (red-first vs the live-tail gate as shipped):
/// encodes the cas_storm_partition chaos wedge. A follower whose observed
/// leader tip was never taught (the leader was in permanent fallback mode for
/// this shard, so no TCP batch ever arrived to reject) is kicked with
/// thousands of entries sitting consumable in S3. PASS: catchup consumes the
/// visible backlog and converges (the gate may only yield to TCP for a SMALL
/// visible tail). FAIL: the gate reads "past observed target + covering file =
/// live tail" and exits instantly with nothing applied, freezing the shard
/// behind a leader that will never TCP it.
#[test]
fn untaught_follower_consumes_large_visible_backlog() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;

        // Chained 1500-entry files spanning 1..=6000: a visible backlog far
        // beyond any live-tail epsilon. observed target stays 0 (no reject
        // ever taught it). Each file's anchor tip comes from a scratch
        // component set consuming the chain incrementally (the batch builder
        // can't compute chain tips), and files stay under the per-batch cap.
        let files = {
            let (_tmp3, dir3) = test_dir();
            let tc3 = TestComponents::new(&dir3).await;
            let dl3 = Rc::new(MockDownloader::new());
            let mut files = Vec::new();
            let mut prev = GENESIS_HASH;
            for start in (1..6000u64).step_by(1500) {
                let end = start + 1499;
                let (p, d) = make_fallback_batch(0, start, end, prev);
                files.push((p.clone(), d.clone()));
                dl3.insert(p, d);
                tc3.catchup_with_target(&dl3, 0, end).await.expect("scratch chain build");
                assert_eq!(tc3.wal_seq(), end, "scaffolding: scratch build must consume through {end}");
                prev = tc3.tip_hash();
            }
            tc3.close().await;
            files
        };

        let dl = Rc::new(MockDownloader::new());
        for (p, d) in files {
            dl.insert(p, d);
        }

        let res = tc.catchup_with_gap(&dl, 0, None, None).await.expect("catchup must not error");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "backlog consumption must end Caught, got {}",
            completion_name(&res.completion)
        );
        assert_eq!(tc.wal_seq(), 6000, "the full visible backlog must be consumed, wal={}", tc.wal_seq());
        tc.close().await;
    });
}

// ── B1: fresh-boot fork recovery converges ──

/// PASS: a node holding an unacked divergent tail at wal=6, with the read
/// cursor uninitialized (fresh boot, `metadata.read = None`), converges when
/// S3 presents the authoritative 6..=8 chain: the divergent tail is truncated,
/// the batch applies, local wal ends at 8, completion Caught. FAIL: fork
/// recovery cannot operate without a read cursor and the shard wedges behind
/// the authoritative chain (production: frozen 130k entries back, retrying
/// "no common ancestor" forever).
#[test]
fn fresh_boot_fork_recovery_converges() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        let dl = divergence_at_6(&tc).await;
        assert_eq!(tc.wal_seq(), 6, "scaffolding: divergence fixture must land local wal at 6");
        tc.clear_read_cursor();

        let res = tc.catchup(&dl, 0, 10).await.expect("fresh-boot fork recovery must not error");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "fresh-boot fork recovery must converge to Caught, got {}",
            completion_name(&res.completion)
        );
        assert!(res.batches_applied >= 1, "the authoritative 6..=8 batch must be applied, applied={}", res.batches_applied);
        assert_eq!(tc.wal_seq(), 8, "local wal must reach the authoritative batch's end");
        tc.close().await;
    });
}

/// PASS: the production shape of the rolling_restart wedge. A KICKED FOLLOWER
/// (role Following, leader tip observed from its rejected batches) holding an
/// unacked divergent tail on a fresh boot (read cursor None) truncates the tail,
/// applies the authoritative chain, and converges. FAIL: the follower fast-exits
/// over the divergent covering file (or wedges) and never reaches the leader tip.
#[test]
fn fresh_boot_fork_recovery_converges_as_kicked_follower() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        let dl = divergence_at_6(&tc).await;
        assert_eq!(tc.wal_seq(), 6, "scaffolding: divergence fixture must land local wal at 6");
        tc.clear_read_cursor();

        // The kicked follower knows the leader holds through wal 8.
        let res = tc.catchup_with_target(&dl, 0, 8).await.expect("kicked-follower fork recovery must not error");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "kicked-follower fork recovery must converge to Caught, got {}",
            completion_name(&res.completion)
        );
        assert!(res.batches_applied >= 1, "the authoritative 6..=8 batch must be applied, applied={}", res.batches_applied);
        assert_eq!(tc.wal_seq(), 8, "local wal must reach the observed leader tip on the authoritative chain");
        tc.close().await;
    });
}

// ── B2: ack barrier holds on fresh boot ──

/// PASS: same fresh-boot divergence as B1, but the divergent wal=6 entry was
/// ACKED to a client (self-ack floor at 6): catchup must refuse to cull the
/// tail. Local wal stays 6, nothing applies, the tip hash is byte-identical,
/// and the outcome is loud (Err or Retry). FAIL: the acked tail is truncated,
/// or catchup declares a clean Caught over an acked fork.
#[test]
fn ack_barrier_preserves_acked_divergent_tail_on_fresh_boot() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        let dl = divergence_at_6(&tc).await;
        assert_eq!(tc.wal_seq(), 6, "scaffolding: divergence fixture must land local wal at 6");
        tc.set_last_self_acked(6);
        tc.clear_read_cursor();
        let tip_before = tc.tip_hash();

        let res = tc.catchup(&dl, 0, 10).await;

        match &res {
            Err(_) => {} // loud failure over an acked fork is acceptable
            Ok(r) => {
                assert!(
                    matches!(r.completion, CatchupCompletion::Retry),
                    "an acked fork must never be a clean Caught, got {}",
                    completion_name(&r.completion)
                );
                assert_eq!(r.batches_applied, 0, "nothing may apply over an acked divergent tail");
            }
        }
        assert_eq!(tc.wal_seq(), 6, "the acked tail must not be culled");
        assert_eq!(tc.tip_hash(), tip_before, "the acked tail must be preserved byte-for-byte");
        tc.close().await;
    });
}

// ── C9: boot-and-latch live-tail contract ──
//
// The live-tail yield gate carries a caller-owned latch (`&Cell<u64>`, one per
// shard for the process lifetime, starts at 0). Contract: a Following node
// past its observed leader target with a SMALL consumable backlog yields to
// TCP at most ONCE per local position; the same latch at an unchanged
// position must consume. Boot role never yields at all. A moved position
// re-arms the yield (the anti-storm pin): under healthy TCP every kick sees
// a fresh position and never camps in catchup.

/// PASS: a Boot-role catchup over a small covering backlog (11..=15 on a
/// wal=10 local) consumes it: Caught with the files applied and the local
/// wal at 15, no yield regardless of backlog size or target. FAIL: the
/// live-tail gate fires for Boot and the node comes up behind data it could
/// see, trusting a TCP session that does not exist yet.
#[test]
fn boot_role_consumes_small_covering_backlog_without_yield() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        tc.seed_chain(10).await;
        let tip = tc.tip_hash();

        let dl = Rc::new(MockDownloader::new());
        let (p, d) = make_fallback_batch(0, 11, 15, tip);
        dl.insert(p, d);

        let res = tc.catchup_as_boot(&dl, 0).await.expect("boot catchup must not error");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "boot over a covering backlog must end Caught, got {}",
            completion_name(&res.completion)
        );
        assert!(res.batches_applied >= 1, "boot must apply the covering file, applied={}", res.batches_applied);
        assert_eq!(tc.wal_seq(), 15, "boot must consume to the newest visible file's end, wal={}", tc.wal_seq());
        tc.close().await;
    });
}

/// PASS: Boot against an EMPTY S3 view returns Caught immediately with
/// nothing applied or downloaded; the fast boot path stays fast. FAIL: the
/// never-yield rule for Boot over-corrected into waiting on an empty bucket.
#[test]
fn boot_role_empty_s3_still_caught_immediately() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        let wal_before = tc.wal_seq();

        let dl = Rc::new(MockDownloader::new());
        let res = tc.catchup_as_boot(&dl, 0).await.expect("boot catchup must not error on empty S3");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "boot over empty S3 must be Caught, got {}",
            completion_name(&res.completion)
        );
        assert_eq!(res.batches_applied, 0);
        assert_eq!(res.bytes_downloaded, 0);
        assert_eq!(tc.wal_seq(), wal_before, "empty S3 must not move the local wal");
        tc.close().await;
    });
}

/// PASS: a Following node past its observed target (wal=10, target 10) with a
/// small covering 11..=15 visible yields on the FIRST kick with a fresh latch
/// (Caught, nothing applied, live TCP presumed to bridge), then on the
/// SECOND kick with the SAME latch at the unchanged position consumes the
/// covering file and lands the wal at 15. FAIL: either the first kick
/// consumes (no yield, every live tail dragged through S3), or the second
/// kick yields again forever and the acked 11..=15 never reaches a follower
/// whose TCP is not actually delivering.
#[test]
fn following_yields_once_then_consumes_at_unchanged_position() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        tc.seed_chain(10).await;
        let tip = tc.tip_hash();

        let dl = Rc::new(MockDownloader::new());
        let (p, d) = make_fallback_batch(0, 11, 15, tip);
        dl.insert(p, d);

        let latch = std::cell::Cell::new(0u64);

        let first = tc
            .catchup_full_with_latch(&dl, 0, None, None, CatchupRole::Following, 10, &latch)
            .await
            .expect("first kick must not error");
        assert!(
            matches!(first.completion, CatchupCompletion::Caught),
            "first kick over a small live tail must yield Caught, got {}",
            completion_name(&first.completion)
        );
        assert_eq!(first.batches_applied, 0, "the yield must apply nothing: TCP owns the tail");
        assert_eq!(tc.wal_seq(), 10, "the yield must not move the local wal");

        let second = tc
            .catchup_full_with_latch(&dl, 0, None, None, CatchupRole::Following, 10, &latch)
            .await
            .expect("second kick must not error");
        assert!(
            matches!(second.completion, CatchupCompletion::Caught),
            "the consuming second kick must end Caught, got {}",
            completion_name(&second.completion)
        );
        assert!(
            second.batches_applied >= 1,
            "same latch, unchanged position: TCP did not bridge, the covering file must apply, applied={}",
            second.batches_applied
        );
        assert_eq!(tc.wal_seq(), 15, "second kick must advance to the newest visible file's end, wal={}", tc.wal_seq());
        tc.close().await;
    });
}

/// PASS: after the latch has fired and a later kick consumed to a NEW
/// position (wal 10 → 15), the next kick at the new position with the SAME
/// latch yields again over a fresh small tail (16..=20): Caught, nothing
/// applied, wal stays 15. FAIL: the latch never re-arms and a healthy
/// follower under live load camps in catchup, dragging every tail through S3
/// (the anti-storm pin).
#[test]
fn moved_position_rearms_live_tail_yield() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        tc.seed_chain(10).await;
        let tip = tc.tip_hash();

        let dl = Rc::new(MockDownloader::new());
        let (p, d) = make_fallback_batch(0, 11, 15, tip);
        dl.insert(p, d);

        let latch = std::cell::Cell::new(0u64);

        // Kick 1: yields at wal 10. Kick 2: same position, consumes to 15.
        tc.catchup_full_with_latch(&dl, 0, None, None, CatchupRole::Following, 10, &latch)
            .await
            .expect("first kick must not error");
        tc.catchup_full_with_latch(&dl, 0, None, None, CatchupRole::Following, 10, &latch)
            .await
            .expect("second kick must not error");
        assert_eq!(tc.wal_seq(), 15, "scaffolding: the second kick must have consumed to 15, wal={}", tc.wal_seq());

        // A fresh small tail lands, chained on the new tip.
        let (p, d) = make_fallback_batch(0, 16, 20, tc.tip_hash());
        dl.insert(p, d);

        let third = tc
            .catchup_full_with_latch(&dl, 0, None, None, CatchupRole::Following, 10, &latch)
            .await
            .expect("third kick must not error");
        assert!(
            matches!(third.completion, CatchupCompletion::Caught),
            "a moved position must yield again, got {}",
            completion_name(&third.completion)
        );
        assert_eq!(third.batches_applied, 0, "the re-armed yield must apply nothing");
        assert_eq!(tc.wal_seq(), 15, "the re-armed yield must not move the local wal, wal={}", tc.wal_seq());
        tc.close().await;
    });
}

// ── B3: read-cursor-present recovery regression pin ──

/// PASS: the same unacked divergence at wal=6 with the read cursor still
/// present (seeded, no restart) converges to wal 8 / Caught: pins that the
/// fresh-boot fix does not regress the established recovery path. FAIL: fork
/// recovery with a live read cursor no longer truncates and converges.
#[test]
fn read_cursor_present_fork_recovery_still_converges() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let tc = TestComponents::new(&dir).await;
        let dl = divergence_at_6(&tc).await;
        assert_eq!(tc.wal_seq(), 6, "scaffolding: divergence fixture must land local wal at 6");

        let res = tc.catchup(&dl, 0, 10).await.expect("read-cursor-present fork recovery must not error");

        assert!(
            matches!(res.completion, CatchupCompletion::Caught),
            "read-cursor-present fork recovery must converge to Caught, got {}",
            completion_name(&res.completion)
        );
        assert!(res.batches_applied >= 1, "the authoritative 6..=8 batch must be applied, applied={}", res.batches_applied);
        assert_eq!(tc.wal_seq(), 8, "local wal must reach the authoritative batch's end");
        tc.close().await;
    });
}
