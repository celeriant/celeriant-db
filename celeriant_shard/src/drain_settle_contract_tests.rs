//! Blind contract tests for the catchup drain-settle barrier.
//! ## What is under test
//!
//! O2 shrinks the production barrier from 8 rounds to 2, leaving the interval at 750ms
//! and the test-cfg round count at 3. Two separate things need pinning:
//!
//!   * the SIZING, which is a deliberate operational choice (6s -> 1.5s of required
//!     stability on the cold recovery path) and must not be edited without breaking a
//!     test on the way past;
//!   * the BEHAVIOUR, which the change must not alter: the barrier still burns exactly
//!     the round count under a stable view, still short-circuits on the first new peer
//!     file, and still ignores files that are ours or already accounted for.
//!
//! ## Already covered in `shard_wal_s3_catchup.rs`'s own `mod tests` — NOT duplicated here
//!
//!   * `drain_barrier_counts_only_files_not_yet_seen_or_processed` — a never-before-seen
//!     peer file holds the barrier open; the same path in `seen_paths` does not; the same
//!     path in `processed_paths` does not. All three by direct call.
//!   * `drain_barrier_catches_late_landing_predecessor_file` — a late file injected at a
//!     specific list index is caught and applied, end-to-end through `catchup_from_s3`.
//!   * `drain_barrier_stable_window_declares_caught` — a stable view still declares
//!     `Caught` (no round accounting).
//!
//! What none of them pin, and what is below: the number of list calls the barrier spends
//! (the thing O2 changes), the short-circuit on a mid-window arrival, and a SELF-node
//! file — the case that would silently hold the barrier open for the full window on every
//! leader that ever fell back to S3.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use celeriant_distributed::paths::{fallback_batch_path, fallback_shard_prefix};
use glommio::{LocalExecutorBuilder, Placement};

use crate::error::s3_catchup_error::S3CatchupError;
use crate::s3_downloader::{S3Downloader, S3ObjectRef};
use crate::shard_wal_s3_catchup::{
    drain_settle_barrier, DRAIN_MAX_ROUNDS, DRAIN_MAX_ROUNDS_PROD, DRAIN_SETTLE_INTERVAL_PROD,
};

const SHARD_ID: u32 = 0;
const SELF_NODE: u128 = 99;
const PEER_NODE: u128 = 7;
const NEXT_WAL_SEQ: u64 = 3;

macro_rules! glommio_test {
    ($body:expr) => {
        LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async move { $body })
            .unwrap()
            .join()
            .unwrap()
    };
}

/// Lists whatever has been scheduled to be visible by the current list index, and counts
/// the calls. The barrier only ever lists, so `download`/`delete` are unreachable.
#[derive(Default)]
struct CountingDownloader {
    list_calls: Cell<u32>,
    /// `(visible_from_list_index, path)`.
    schedule: RefCell<Vec<(u32, String)>>,
}

impl CountingDownloader {
    fn visible_from(&self, index: u32, path: String) {
        self.schedule.borrow_mut().push((index, path));
    }
}

impl S3Downloader for CountingDownloader {
    async fn list_objects(&self, _prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError> {
        let index = self.list_calls.get();
        self.list_calls.set(index + 1);
        Ok(self
            .schedule
            .borrow()
            .iter()
            .filter(|(from, _)| *from <= index)
            .map(|(_, path)| S3ObjectRef { path: path.clone(), size: 1 })
            .collect())
    }

    async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError> {
        Err(S3CatchupError::S3GetFailed { path: path.to_string(), message: "barrier never downloads".to_string() })
    }

    async fn delete(&self, _path: &str) -> Result<(), S3CatchupError> {
        Ok(())
    }
}

async fn run_barrier(dl: &Rc<CountingDownloader>) -> bool {
    drain_settle_barrier(
        dl,
        &fallback_shard_prefix(SHARD_ID),
        SHARD_ID,
        SELF_NODE,
        None,
        NEXT_WAL_SEQ,
        &HashSet::new(),
        &HashSet::new(),
    )
    .await
    .expect("barrier must not error on a listing that succeeds")
}

// ── Sizing ────────────────────────────────────────────────────────────────────

/// INVARIANT: the production settle window is 2 x 750ms. This is an operational sizing
/// decision, not an implementation detail: the barrier sits on the cold recovery path,
/// and every round is dead time a promoting node spends not serving writes. Changing
/// either number must break this test on the way past.
#[test]
fn contract_production_drain_window_is_two_rounds_of_750ms() {
    assert_eq!(
        DRAIN_MAX_ROUNDS_PROD, 2,
        "production drain rounds changed; the approved sizing is 2 rounds"
    );
    assert_eq!(
        DRAIN_SETTLE_INTERVAL_PROD,
        Duration::from_millis(750),
        "production drain settle interval changed; the approved sizing is 750ms"
    );
    assert_eq!(
        DRAIN_SETTLE_INTERVAL_PROD * DRAIN_MAX_ROUNDS_PROD,
        Duration::from_millis(1_500),
        "the required-stability window a promoting node pays before declaring Caught"
    );
    assert_eq!(
        DRAIN_MAX_ROUNDS, 3,
        "the test-cfg round count must stay at 3: the round-accounting tests below need \
         headroom between an early short-circuit and window exhaustion"
    );
}

// ── Behaviour, under the active (test-cfg) round count ───────────────────────

/// INVARIANT: a stable view costs exactly `DRAIN_MAX_ROUNDS` listings and no more.
/// The round count IS the cost, so it has to be observable; `drain_barrier_stable_window_
/// declares_caught` proves the verdict but never counts the listings.
#[test]
fn contract_stable_view_spends_exactly_the_round_budget() {
    let (late, list_calls) = glommio_test!({
        let dl = Rc::new(CountingDownloader::default());
        let late = run_barrier(&dl).await;
        (late, dl.list_calls.get())
    });

    assert!(!late, "an empty, stable S3 view must not hold the barrier open");
    assert_eq!(
        list_calls, DRAIN_MAX_ROUNDS,
        "a stable view must cost exactly DRAIN_MAX_ROUNDS listings"
    );
}

/// INVARIANT: the barrier short-circuits. A new peer file landing in round k returns
/// immediately; it does not sit out the remaining rounds. With the production budget cut
/// to 2, the difference between short-circuiting and exhausting is the difference between
/// 750ms and 1.5s on every drain that actually catches something.
#[test]
fn contract_new_peer_file_returns_without_exhausting_the_remaining_rounds() {
    let (late, list_calls) = glommio_test!({
        let dl = Rc::new(CountingDownloader::default());
        // Lands on the SECOND listing, leaving at least one round unspent.
        dl.visible_from(1, fallback_batch_path(SHARD_ID, 5, 5, PEER_NODE));
        let late = run_barrier(&dl).await;
        (late, dl.list_calls.get())
    });

    assert!(late, "a peer file that appears mid-window must hold the barrier open");
    assert_eq!(
        list_calls, 2,
        "the barrier must return on the listing that found the late file, not spend the \
         remaining {} round(s)",
        DRAIN_MAX_ROUNDS - 2
    );
}

/// INVARIANT: our OWN fallback uploads are not evidence that a peer's queue is draining.
/// Every leader that has ever fallen back to S3 leaves its own files under the shard
/// prefix, so counting them would hold the barrier open for the full window on every
/// single drain — and, once the barrier returns true, send the caller round the main loop
/// again on a file it can never consume.
#[test]
fn contract_self_node_file_does_not_hold_the_barrier_open() {
    let (late, list_calls) = glommio_test!({
        let dl = Rc::new(CountingDownloader::default());
        // Never seen, never processed, ahead of next_wal_seq — but ours.
        dl.visible_from(0, fallback_batch_path(SHARD_ID, 5, 5, SELF_NODE));
        let late = run_barrier(&dl).await;
        (late, dl.list_calls.get())
    });

    assert!(!late, "a file uploaded by THIS node must not hold the drain barrier open");
    assert_eq!(
        list_calls, DRAIN_MAX_ROUNDS,
        "scaffolding: the barrier must have run the full window for the verdict to mean \
         'stable', not 'exited early'"
    );
}
