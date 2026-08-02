use std::time::{Duration, Instant};
use glommio::channels::local_channel::LocalReceiver;
use super::connection_handler::CatchupCompletionMsg;

pub(crate) enum BarrierOutcome {
    Complete(Vec<CatchupCompletionMsg>),
    TimedOut { results: Vec<CatchupCompletionMsg>, unreported: Vec<usize> },
    Closed(Vec<CatchupCompletionMsg>),
}

/// Await all other shards' s3 cacthup. Apply timeout on channel
/// so that we don't get stuck forever.
pub(crate) async fn await_completions(
    rx: &LocalReceiver<CatchupCompletionMsg>,
    generation: u64,
    shard_count: usize,
    timeout: Duration,
) -> BarrierOutcome {
    let mut results: Vec<CatchupCompletionMsg> = Vec::new();
    let mut reported = vec![false; shard_count];

    let unreported = |reported: &[bool]| -> Vec<usize> {
        (1..shard_count).filter(|&id| !reported[id]).collect()
    };

    let deadline = Instant::now() + timeout;
    while results.len() < shard_count.saturating_sub(1) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return BarrierOutcome::TimedOut { results, unreported: unreported(&reported) };
        }

        let recv = glommio::timer::timeout(remaining, async {
            Ok::<_, glommio::GlommioError<()>>(rx.recv().await)
        })
        .await;

        match recv {
            Ok(Some(msg)) => {
                if msg.attempt != generation {
                    tracing::debug!(stale_attempt = msg.attempt, generation, shard_id = msg.shard_id, "discarding stale catchup completion");
                    continue;
                }
                if msg.shard_id == 0 || msg.shard_id >= shard_count || reported[msg.shard_id] {
                    tracing::warn!(shard_id = msg.shard_id, generation, "discarding out-of-range or duplicate catchup completion");
                    continue;
                }
                reported[msg.shard_id] = true;
                results.push(msg);
            }
            Ok(None) => return BarrierOutcome::Closed(results),
            Err(_) => return BarrierOutcome::TimedOut { results, unreported: unreported(&reported) },
        }
    }

    BarrierOutcome::Complete(results)
}

#[cfg(test)]
mod tests {
    use crate::sharded::catchup_barrier::{await_completions, BarrierOutcome};
    use crate::sharded::connection_handler::CatchupCompletionMsg;
    use celeriant_shard::shard_wal_s3_catchup::{CatchupCompletion, S3CatchupResult};
    use glommio::channels::local_channel::{new_unbounded, LocalSender};
    use glommio::timer::sleep;
    use glommio::{LocalExecutorBuilder, Placement};
    use std::time::{Duration, Instant};

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    const GEN: u64 = 9;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    /// `batches_applied` carries the shard id so a returned message can be traced
    /// back to the send that produced it.
    fn msg(shard_id: usize, attempt: u64) -> CatchupCompletionMsg {
        CatchupCompletionMsg {
            shard_id,
            attempt,
            result: Ok(S3CatchupResult {
                batches_applied: shard_id as u64,
                bytes_downloaded: 0,
                rounds: 1,
                completion: CatchupCompletion::Caught,
                stalled_awaiting_s3: false,
            }),
        }
    }

    fn send(tx: &LocalSender<CatchupCompletionMsg>, m: CatchupCompletionMsg) {
        assert!(tx.try_send(m).is_ok(), "unbounded send must succeed");
    }

    fn reported(results: &[CatchupCompletionMsg]) -> Vec<usize> {
        let mut ids: Vec<usize> = results.iter().map(|m| m.shard_id).collect();
        ids.sort_unstable();
        ids
    }

    // Destructuring helpers rather than `matches!` on a Debug print: the outcome
    // carries messages whose Debug is not part of the contract.

    fn expect_complete(outcome: BarrierOutcome) -> Vec<CatchupCompletionMsg> {
        match outcome {
            BarrierOutcome::Complete(results) => results,
            BarrierOutcome::TimedOut { .. } => panic!("expected Complete, got TimedOut"),
            BarrierOutcome::Closed(_) => panic!("expected Complete, got Closed"),
        }
    }

    fn expect_timed_out(outcome: BarrierOutcome) -> (Vec<CatchupCompletionMsg>, Vec<usize>) {
        match outcome {
            BarrierOutcome::TimedOut { results, unreported } => (results, unreported),
            BarrierOutcome::Complete(_) => panic!("expected TimedOut, got Complete"),
            BarrierOutcome::Closed(_) => panic!("expected TimedOut, got Closed"),
        }
    }

    fn expect_closed(outcome: BarrierOutcome) -> Vec<CatchupCompletionMsg> {
        match outcome {
            BarrierOutcome::Closed(results) => results,
            BarrierOutcome::Complete(_) => panic!("expected Closed, got Complete"),
            BarrierOutcome::TimedOut { .. } => panic!("expected Closed, got TimedOut"),
        }
    }

    /// PASS: all three peers reporting this generation before the deadline yields
    /// Complete holding exactly those three messages. FAIL: the barrier cannot
    /// recognise a healthy round — catchup either never releases or releases with
    /// the wrong result set feeding the attempt tracker.
    #[test]
    fn all_peers_reporting_current_generation_completes() {
        glommio_test!({
            let (tx, rx) = new_unbounded();
            for shard_id in 1..4 {
                send(&tx, msg(shard_id, GEN));
            }
            let results = expect_complete(await_completions(&rx, GEN, 4, ms(2000)).await);
            assert_eq!(reported(&results), vec![1, 2, 3]);
            assert!(results.iter().all(|m| m.attempt == GEN), "results must carry this generation");
        });
    }

    /// PASS: completions from a previous catchup cycle never satisfy this one —
    /// two stale messages leave both peers unreported and out of the results.
    /// FAIL: a late arrival from a dead cycle fast-satisfies the barrier and shard
    /// 0 declares peers caught up when they have not even started.
    #[test]
    fn stale_generation_messages_satisfy_nothing() {
        glommio_test!({
            let (tx, rx) = new_unbounded();
            send(&tx, msg(1, GEN - 1));
            send(&tx, msg(2, GEN - 1));
            let (results, unreported) = expect_timed_out(await_completions(&rx, GEN, 3, ms(150)).await);
            assert!(results.is_empty(), "stale messages must not appear in results");
            assert_eq!(unreported, vec![1, 2]);
        });
    }

    /// PASS: a stale message from shard 1 is discarded, and shard 1's later
    /// current-generation message still completes the barrier — the results hold
    /// one message per shard, all of this generation. FAIL: the stale message is
    /// counted (shard 1 satisfied twice, shard 2 never waited for) or the discard
    /// wedges shard 1 out of the barrier entirely.
    #[test]
    fn stale_message_discarded_then_current_completes() {
        glommio_test!({
            let (tx, rx) = new_unbounded();
            send(&tx, msg(1, GEN - 1));
            send(&tx, msg(1, GEN));
            send(&tx, msg(2, GEN));
            let results = expect_complete(await_completions(&rx, GEN, 3, ms(2000)).await);
            assert_eq!(reported(&results), vec![1, 2]);
            assert!(results.iter().all(|m| m.attempt == GEN), "stale message must not survive into results");
        });
    }

    /// PASS: a peer that never reports makes the barrier return TimedOut naming
    /// exactly that peer, with the reporting peer's message kept. FAIL: the wait is
    /// unbounded — this is the chaos-run freeze, three nodes pinned in
    /// FollowerCatchingUp rejecting replication until SIGTERM.
    #[test]
    fn silent_peer_times_out_naming_the_missing_shard() {
        glommio_test!({
            let (tx, rx) = new_unbounded();
            send(&tx, msg(1, GEN));
            let (results, unreported) = expect_timed_out(await_completions(&rx, GEN, 3, ms(150)).await);
            assert_eq!(reported(&results), vec![1]);
            assert_eq!(unreported, vec![2]);
        });
    }

    /// Pins the TOTAL BUDGET reading of the deadline: the clock starts on entry and
    /// arriving messages never extend it. The alternative (per-message idle timer)
    /// is what the production freeze looks like when peers dribble in — an
    /// unbounded wait dressed up as a bounded one.
    ///
    /// Three peers, a 250ms budget, completions 150ms apart (150ms / 300ms /
    /// 450ms) — every gap is comfortably inside the budget but the run is not.
    /// PASS: the barrier returns TimedOut holding shard 1 only, shards 2 and 3
    /// unreported. FAIL (Complete with all three): each message reset the deadline,
    /// so a peer trickling completions holds the node in catchup indefinitely.
    #[test]
    fn deadline_is_a_total_budget_not_a_per_message_idle_timer() {
        glommio_test!({
            let (tx, rx) = new_unbounded();
            glommio::spawn_local(async move {
                for shard_id in 1..4 {
                    sleep(ms(150)).await;
                    send(&tx, msg(shard_id, GEN));
                }
            })
            .detach();
            let (results, unreported) = expect_timed_out(await_completions(&rx, GEN, 4, ms(250)).await);
            assert_eq!(reported(&results), vec![1]);
            assert_eq!(unreported, vec![2, 3]);
        });
    }

    /// PASS: two current-generation messages from shard 1 count once — shard 2 is
    /// still unreported and the barrier times out. FAIL: the barrier counts
    /// messages instead of distinct shards, so one chatty shard releases catchup
    /// while another is still mid-download, and shard 0 acts on a set of results
    /// that never covered the cluster.
    #[test]
    fn duplicate_from_one_shard_does_not_cover_a_silent_shard() {
        glommio_test!({
            let (tx, rx) = new_unbounded();
            send(&tx, msg(1, GEN));
            send(&tx, msg(1, GEN));
            let (results, unreported) = expect_timed_out(await_completions(&rx, GEN, 3, ms(150)).await);
            assert_eq!(unreported, vec![2]);
            assert!(results.iter().all(|m| m.shard_id == 1), "only shard 1 ever reported");
        });
    }

    /// PASS: the sender dropping with shards outstanding returns Closed promptly,
    /// carrying what did arrive — the node is shutting down and the barrier must
    /// not sit out its full budget or hang. FAIL: shutdown either blocks on a dead
    /// channel or is mistaken for a satisfied barrier.
    #[test]
    fn dropped_sender_with_shards_outstanding_yields_closed() {
        glommio_test!({
            let (tx, rx) = new_unbounded();
            send(&tx, msg(1, GEN));
            drop(tx);
            let started = Instant::now();
            let results = expect_closed(await_completions(&rx, GEN, 3, ms(10_000)).await);
            assert_eq!(reported(&results), vec![1]);
            assert!(started.elapsed() < ms(5_000), "close must not wait out the deadline");
        });
    }

    /// PASS: a single-shard node has no peers to wait for, so the barrier returns
    /// Complete and empty without touching the clock. FAIL: single-shard deploys
    /// eat the full timeout on every catchup attempt, or worse report TimedOut and
    /// take the bail path on a node that is perfectly healthy.
    #[test]
    fn single_shard_node_completes_immediately() {
        glommio_test!({
            let (_tx, rx) = new_unbounded::<CatchupCompletionMsg>();
            let started = Instant::now();
            let results = expect_complete(await_completions(&rx, GEN, 1, ms(10_000)).await);
            assert!(results.is_empty());
            assert!(started.elapsed() < ms(1_000), "zero peers must not wait on the deadline");
        });
    }

}