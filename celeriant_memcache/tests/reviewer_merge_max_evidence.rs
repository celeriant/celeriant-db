//! `merge_aggregate_client_seq_max` shipped with no direct test: the acceptance
//! tests only observe it end-to-end, and they accept EITHER idempotency
//! rejection, so they cannot distinguish the stored wal_seq. These pin the
//! properties the fix's soundness argument leans on.

use celeriant_memcache::cached_schema::Validate;
use celeriant_memcache::shard_mem_cache::{ClientSeqStatus, ShardMemCache};
use celeriant_wal::aggregate_client_key::AggregateClientKey;
use celeriant_wal::aggregate_key::AggregateKey;

struct NoValidation;
impl Validate for NoValidation {
    fn validate(&self, _event_value: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

fn cache() -> ShardMemCache<NoValidation> {
    ShardMemCache::new(1 << 20, 1 << 20, 1 << 20, 1 << 20, 1 << 20, 1 << 20)
}

fn agg() -> AggregateKey {
    AggregateKey::new(1, 2, 3)
}

const CLIENT: u128 = 7;

fn key() -> AggregateClientKey {
    AggregateClientKey::new(agg(), CLIENT)
}

fn entry(mc: &mut ShardMemCache<NoValidation>) -> Option<(u64, u64)> {
    mc.get_client_seq_entry(&agg(), CLIENT).map(|s| match s {
        ClientSeqStatus::Fsynced { client_seq, wal_seq } => (client_seq, wal_seq),
        ClientSeqStatus::InflightInQueue { client_seq } => (client_seq, u64::MAX),
    })
}

/// Insert-when-absent, raise-on-greater, no-op on equal-or-lower — including
/// the wal_seq, which must NOT be touched when the client_seq does not move.
/// The no-op-on-equal case is the one the apply paths hit constantly (a
/// re-delivered batch, a mid-batch resume): it must not rewrite wal_seq
/// downward or the durability gate would flip an applied entry back to
/// "in flight".
#[test]
fn merge_is_a_strict_max_and_leaves_wal_seq_alone_below_the_high_water_mark() {
    let mut mc = cache();
    assert_eq!(entry(&mut mc), None, "nothing cached yet");

    mc.merge_aggregate_client_seq_max(key(), 5, 100);
    assert_eq!(entry(&mut mc), Some((5, 100)), "absent -> insert");

    mc.merge_aggregate_client_seq_max(key(), 3, 999);
    assert_eq!(entry(&mut mc), Some((5, 100)), "lower client_seq must not lower or retag");

    mc.merge_aggregate_client_seq_max(key(), 5, 999);
    assert_eq!(entry(&mut mc), Some((5, 100)), "equal client_seq must not retag wal_seq");

    mc.merge_aggregate_client_seq_max(key(), 6, 101);
    assert_eq!(entry(&mut mc), Some((6, 101)), "greater client_seq raises both fields");
}

/// The apply-path merge must be able to raise the disk-scan sentinel. A cold
/// miss-load that found nothing stores (0, 0), which `aggregate_client_load_status`
/// reads as "checked, this client never wrote here" and which suppresses any
/// further disk scan. If the merge could not overwrite it, replicated history
/// arriving after a negative scan would be invisible to the dedup gate forever.
#[test]
fn merge_raises_the_never_wrote_sentinel_installed_by_a_negative_disk_scan() {
    let mut mc = cache();
    mc.put_aggregate_client_into_cache(key(), 0, false);
    assert_eq!(entry(&mut mc), None, "sentinel 0 is filtered out of the gate");
    assert_eq!(
        mc.aggregate_client_load_status(&agg(), &key()),
        (true, None),
        "sentinel suppresses the disk scan"
    );

    mc.merge_aggregate_client_seq_max(key(), 4, 42);
    assert_eq!(entry(&mut mc), Some((4, 42)), "apply path must beat the sentinel");
}

/// Order-insensitivity: the apply paths can deliver the same (client, seq)
/// twice, or out of order relative to a warmup/miss-load insert, and must
/// converge on the same value. This is the property that lets ONE merge site
/// in commit_sync cover both apply paths without ordering coordination.
#[test]
fn merge_converges_regardless_of_arrival_order() {
    let seqs = [(3u64, 30u64), (1, 10), (7, 70), (2, 20), (7, 71)];

    let mut forward = cache();
    for (s, w) in seqs {
        forward.merge_aggregate_client_seq_max(key(), s, w);
    }

    let mut reverse = cache();
    for (s, w) in seqs.iter().rev() {
        reverse.merge_aggregate_client_seq_max(key(), *s, *w);
    }

    assert_eq!(entry(&mut forward), Some((7, 70)));
    assert_eq!(
        entry(&mut reverse),
        Some((7, 71)),
        "converges on the same client_seq; wal_seq is whichever delivery first \
         carried the max, and both are real wal_seqs for that seq"
    );
}
