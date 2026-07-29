//! What a follower learns from a replication batch it rejects: the leader's
//! wal tip. That tip is the floor S3 catchup must reach before the follower
//! yields back to live TCP replication.

use std::cell::Cell;

/// Last observed leader tip, tagged with the lease epoch that taught it.
pub struct ObservedLeaderTarget {
    lease_epoch: Cell<u64>,
    wal_seq: Cell<u64>,
}

impl ObservedLeaderTarget {
    pub fn new() -> Self {
        ObservedLeaderTarget {
            lease_epoch: Cell::new(0),
            wal_seq: Cell::new(0),
        }
    }

    /// Record the leader tip carried by a rejected batch at `batch_lease_epoch`.
    /// A newer epoch replaces the target even when its tip is lower: a
    /// leadership change can cull the old tail, shrinking the chain. Within one
    /// epoch the tip only grows, and older-epoch stragglers are stale.
    pub fn teach(&self, batch_lease_epoch: u64, observed_wal_seq: u64) {
        let taught_epoch = self.lease_epoch.get();
        if batch_lease_epoch > taught_epoch {
            self.lease_epoch.set(batch_lease_epoch);
            self.wal_seq.set(observed_wal_seq);
        } else if batch_lease_epoch == taught_epoch && observed_wal_seq > self.wal_seq.get() {
            self.wal_seq.set(observed_wal_seq);
        }
    }

    /// Raw taught `(lease_epoch, wal_seq)`, for the phantom-discard log line.
    pub fn taught(&self) -> (u64, u64) {
        (self.lease_epoch.get(), self.wal_seq.get())
    }

    /// The catchup target, validated against the leader epoch the follower
    /// currently knows (`None` when it has no leader, e.g. at boot). A target
    /// taught by an older epoch is a phantom: its tail may be culled
    /// cluster-wide and the seq unreachable forever, so it reads 0 (no target)
    /// rather than wedging catchup on AwaitMore. A taught epoch newer than the
    /// known one is valid, the batch just outran the heartbeat. With no known
    /// epoch to judge against, the taught seq stands. Pure read; the state
    /// survives for later teachings.
    pub fn target_for(&self, current_leader_epoch: Option<u64>) -> u64 {
        match current_leader_epoch {
            Some(known) if self.lease_epoch.get() < known => 0,
            _ => self.wal_seq.get(),
        }
    }
}

/// Blind-authored contract tests, written from the doc comments alone, not the
/// implementation. The struct exists to stop the phantom-target wedge: a tip
/// taught by a dead leader epoch may be culled cluster-wide, and catchup
/// chasing it would wait on AwaitMore forever.
#[cfg(test)]
mod tests {
    use super::ObservedLeaderTarget;

    /// Build a target and replay a sequence of teachings against it.
    fn taught_with(teachings: &[(u64, u64)]) -> ObservedLeaderTarget {
        let target = ObservedLeaderTarget::new();
        for &(epoch, seq) in teachings {
            target.teach(epoch, seq);
        }
        target
    }

    /// An untaught target reads 0 regardless of leader knowledge.
    /// PASS: a freshly booted follower has no catchup floor and yields to live
    /// replication immediately. FAIL: catchup chases a target nobody set and
    /// stalls a healthy follower at boot.
    #[test]
    fn untaught_reads_no_target() {
        let target = ObservedLeaderTarget::new();
        for known in [None, Some(1), Some(u64::MAX)] {
            assert_eq!(target.target_for(known), 0, "untaught read with known epoch {known:?}");
        }
        assert_eq!(target.taught().1, 0, "untaught raw wal_seq must mean no target");
    }

    /// teach() keeps the newest epoch's tip: within an epoch the tip only grows,
    /// a newer epoch replaces even a lower tip, older stragglers are dropped.
    /// PASS: the stored target always tracks the live leader's real chain, shrink
    /// included. FAIL: catchup aims at a culled tail or regresses on stragglers.
    #[test]
    fn teach_follows_epoch_ordering() {
        let cases: &[(&str, &[(u64, u64)], (u64, u64))] = &[
            ("single teach sticks", &[(5, 100)], (5, 100)),
            ("same epoch grows", &[(5, 100), (5, 150)], (5, 150)),
            ("same epoch never shrinks", &[(5, 100), (5, 90)], (5, 100)),
            ("same epoch equal is a no-op", &[(5, 100), (5, 100)], (5, 100)),
            ("newer epoch, higher tip", &[(5, 100), (6, 200)], (6, 200)),
            ("newer epoch replaces even a lower tip", &[(5, 100), (6, 40)], (6, 40)),
            ("older straggler dropped", &[(5, 100), (4, 500)], (5, 100)),
            ("growth continues after straggler", &[(5, 100), (4, 500), (5, 120)], (5, 120)),
            ("generations chain", &[(5, 100), (6, 40), (6, 60), (7, 10)], (7, 10)),
        ];
        for (name, teachings, expected) in cases {
            assert_eq!(taught_with(teachings).taught(), *expected, "{name}");
        }
    }

    /// target_for() validates the taught epoch against the known leader epoch:
    /// older taught epoch is a phantom and reads 0, same or newer reads the seq.
    /// PASS: catchup never waits on a seq a leadership change culled forever.
    /// FAIL: a stale tip wedges S3 catchup on AwaitMore with no way out.
    #[test]
    fn target_for_validates_taught_epoch() {
        let cases: &[(&str, &[(u64, u64)], Option<u64>, u64)] = &[
            ("taught epoch matches known", &[(5, 100)], Some(5), 100),
            ("phantom from older epoch reads 0", &[(5, 100)], Some(6), 0),
            ("taught epoch outran the heartbeat", &[(5, 100)], Some(4), 100),
            ("no known leader cannot invalidate", &[(5, 100)], None, 100),
            ("shrunk target from new epoch reads through", &[(5, 100), (6, 40)], Some(6), 40),
            ("straggler never becomes the target", &[(5, 100), (4, 500)], Some(5), 100),
        ];
        for (name, teachings, known, expected) in cases {
            assert_eq!(taught_with(teachings).target_for(*known), *expected, "{name}");
        }
    }

    /// Reads are pure: a phantom verdict discards nothing, and later teachings
    /// still land on the surviving state.
    /// PASS: one stale heartbeat read cannot erase a valid target. FAIL: a
    /// transient epoch mismatch wipes the floor and catchup undershoots the tip.
    #[test]
    fn reads_never_mutate_state() {
        let target = taught_with(&[(5, 100)]);
        for known in [Some(6), Some(4), None, Some(5)] {
            target.target_for(known);
        }
        assert_eq!(target.taught(), (5, 100), "reads must leave the taught pair intact");
        assert_eq!(target.target_for(Some(5)), 100, "target still readable after a phantom verdict");
        target.teach(6, 40);
        assert_eq!(target.taught(), (6, 40), "teaching must still work after reads");
    }
}
