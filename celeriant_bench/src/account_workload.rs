//! Banking workload for the cardinality-pressure scenario: event shapes, the
//! derived payload mix, and the bounded ack ledger.
//!
//! Event majors are lifted from `celeriant_reference::events` so the shapes stay
//! recognisable, plus a fifth that is deliberately large enough to force the
//! datablock path.

use std::collections::HashMap;

use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, MINIBATCH_SIZE_BYTES};
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::sync::Arc;

/// One `SchemaKey` per `(org, aggregate_type, major, minor)`; the server looks
/// validation up by exactly that tuple and silently skips it on a miss.
///
/// **Deliberately NOT `(1, 1)`.** That is the opaque bench's tuple
/// (`celeriant_bench/src/lib.rs`), and schemas are durable: registering JSON
/// schemas there leaves every later bench and scenario on the same cluster
/// failing validation with
/// `"Event value is not valid JSON: expected ident at line 1 column 3"`,
/// because the opaque payload `[t-1-s-1]` is not JSON. Only `teardown-data`
/// clears it. On a rig shared with a 24/7 prod deployment and the rest of the
/// chaos suite, silently invalidating the default tuple is not acceptable.
/// Observed in the field before this namespace existed.
pub const WORKLOAD_ORG: u128 = 0xCA5D;
pub const WORKLOAD_AGG_TYPE: u128 = 0xACC7;
pub const WORKLOAD_MINOR: u64 = 0;

pub const MAJOR_DEPOSITED: u64 = 1;
pub const MAJOR_WITHDRAWN: u64 = 2;
pub const MAJOR_TRANSFERRED_OUT: u64 = 3;
pub const MAJOR_TRANSFERRED_IN: u64 = 4;
/// Settlement statement carrying a line-item array. Sized past
/// `MINIBATCH_SIZE_BYTES` so it must take the datablock path.
pub const MAJOR_STATEMENT_ATTACHED: u64 = 5;

pub const ALL_MAJORS: [u64; 5] = [
    MAJOR_DEPOSITED,
    MAJOR_WITHDRAWN,
    MAJOR_TRANSFERRED_OUT,
    MAJOR_TRANSFERRED_IN,
    MAJOR_STATEMENT_ATTACHED,
];

/// Nominal datablock cost of one large event. The statement size is drawn from a
/// distribution rather than fixed, so datablock packing and the unaligned
/// carry-over buffers see varied input; this is the mean used for sizing.
pub const NOMINAL_DATABLOCK_BYTES: u64 = 8192;
pub const STATEMENT_MIN_BYTES: usize = 4 * 1024;
pub const STATEMENT_MAX_BYTES: usize = 16 * 1024;

// ---------------------------------------------------------------------------
// Payload mix
// ---------------------------------------------------------------------------

/// Aggregates a segment holds at a given large-event fraction.
///
/// Cost per aggregate is `FIXED_BLOCK_SIZE_BYTES + f * D`: every write takes a
/// metablock, and the fraction `f` that serialises past `MINIBATCH_SIZE_BYTES`
/// additionally takes a datablock.
pub fn aggregates_per_segment(segment_bytes: u64, large_event_fraction: f64, datablock_bytes: u64) -> u64 {
    let per = FIXED_BLOCK_SIZE_BYTES as f64 + large_event_fraction.clamp(0.0, 1.0) * datablock_bytes as f64;
    if per <= 0.0 {
        return 0;
    }
    (segment_bytes as f64 / per) as u64
}

/// Large-event fraction needed to reach `target_aggs_per_segment`.
///
/// **The mix is derived, never passed in.** Bloom load is set by the payload mix
/// *relative to* the segment size, not by either alone: hold `f` constant while
/// walking 256MB then 1GB and the two stages are not comparable — a 50% mix at
/// 256MB leaves the bloom four times under-loaded, so a flat curve there would
/// say nothing about 1GB. Holding aggregates-per-segment constant and letting
/// `f` move is what makes the stages a controlled comparison.
///
/// Clamped to `[0, 1]`. A clamp means the target is unreachable at this segment
/// size — below the metablock floor, or beyond an all-large payload — and the
/// achieved density will differ from the target. Callers should report the
/// achieved figure rather than the requested one.
pub fn derive_large_event_fraction(segment_bytes: u64, target_aggs_per_segment: u64, datablock_bytes: u64) -> f64 {
    if target_aggs_per_segment == 0 || datablock_bytes == 0 || segment_bytes == 0 {
        return 0.0;
    }
    let bytes_per_agg = segment_bytes as f64 / target_aggs_per_segment as f64;
    let f = (bytes_per_agg - FIXED_BLOCK_SIZE_BYTES as f64) / datablock_bytes as f64;
    if f.is_finite() { f.clamp(0.0, 1.0) } else { 0.0 }
}

/// True when an event of this size must take a datablock rather than living
/// inline in the metablock.
pub fn takes_datablock(serialized_len: usize) -> bool {
    serialized_len > MINIBATCH_SIZE_BYTES
}

// ---------------------------------------------------------------------------
// Event construction
// ---------------------------------------------------------------------------

/// Build one banking event.
///
/// `iv` **must** stay `None`. The server skips schema validation entirely for any
/// event carrying an IV ("validate OR encrypt, not both"), so an IV here would
/// silently measure an unvalidated path while the report claims validation is on.
pub fn account_event(major: u64, client_seq: u64, nonce: u64, statement_bytes: usize) -> DatablockAggregateEvent {
    let body = match major {
        MAJOR_DEPOSITED => format!(r#"{{"AmountCents":{}}}"#, 1 + nonce % 10_000),
        MAJOR_WITHDRAWN => format!(r#"{{"AmountCents":{}}}"#, 1 + nonce % 5_000),
        MAJOR_TRANSFERRED_OUT => format!(
            r#"{{"AmountCents":{},"ToAccountId":"{}"}}"#,
            1 + nonce % 5_000,
            uuid_like(nonce)
        ),
        MAJOR_TRANSFERRED_IN => format!(
            r#"{{"AmountCents":{},"FromAccountId":"{}"}}"#,
            1 + nonce % 5_000,
            uuid_like(nonce)
        ),
        _ => statement_json(nonce, statement_bytes),
    };
    DatablockAggregateEvent {
        client_seq,
        event_seq: 0,
        event_id: None,
        event_timestamp: 0,
        event_type_major: major,
        event_type_minor: WORKLOAD_MINOR,
        event_value: Arc::new(body.into_bytes()),
        iv: None,
    }
}

/// A settlement statement padded with line items to approximately `target_bytes`.
/// Built to a byte target rather than a fixed item count so the datablock packer
/// sees varied, unaligned input.
fn statement_json(nonce: u64, target_bytes: usize) -> String {
    let target = target_bytes.clamp(STATEMENT_MIN_BYTES, STATEMENT_MAX_BYTES);
    let mut s = String::with_capacity(target + 64);
    s.push_str(r#"{"StatementId":""#);
    s.push_str(&uuid_like(nonce));
    s.push_str(r#"","LineItems":["#);
    let mut i = 0u64;
    while s.len() < target.saturating_sub(32) {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            r#"{{"Seq":{},"AmountCents":{},"Memo":"txn-{:016x}"}}"#,
            i,
            1 + (nonce.wrapping_add(i)) % 100_000,
            nonce.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(i)
        ));
        i += 1;
    }
    s.push_str("]}");
    s
}

fn uuid_like(n: u64) -> String {
    let a = n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let b = n.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        a as u32,
        (a >> 32) as u16,
        (a >> 48) as u16,
        b as u16,
        b >> 16 & 0xFFFF_FFFF_FFFF
    )
}

/// JSON schema per major. Registered against `(WORKLOAD_ORG, WORKLOAD_AGG_TYPE,
/// major, WORKLOAD_MINOR)`.
pub fn schema_for(major: u64) -> &'static str {
    match major {
        MAJOR_DEPOSITED | MAJOR_WITHDRAWN => {
            r#"{"type":"object","properties":{"AmountCents":{"type":"integer"}},"required":["AmountCents"]}"#
        }
        MAJOR_TRANSFERRED_OUT => {
            r#"{"type":"object","properties":{"AmountCents":{"type":"integer"},"ToAccountId":{"type":"string"}},"required":["AmountCents","ToAccountId"]}"#
        }
        MAJOR_TRANSFERRED_IN => {
            r#"{"type":"object","properties":{"AmountCents":{"type":"integer"},"FromAccountId":{"type":"string"}},"required":["AmountCents","FromAccountId"]}"#
        }
        _ => {
            r#"{"type":"object","properties":{"StatementId":{"type":"string"},"LineItems":{"type":"array"}},"required":["StatementId","LineItems"]}"#
        }
    }
}

pub fn account_key(aggregate_id: u128) -> AggregateKey {
    AggregateKey::new(WORKLOAD_ORG, WORKLOAD_AGG_TYPE, aggregate_id)
}

// ---------------------------------------------------------------------------
// Ack ledger
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AckKey {
    pub aggregate_id: u128,
    pub client_id: u128,
}

/// Bounded reservoir over acked `(aggregate, client)` pairs.
///
/// `TaskAckSummary` carries one `max_acked_client_seq` per *task*, which assumes
/// one aggregate per task for the run's lifetime. This fill writes many
/// aggregates per task, and full history recording is impossible over a
/// multi-hour run — the recorder drops on a full channel and `check_idempotency`
/// fails closed on any drop. So the ledger keeps a uniform sample instead, and
/// because cardinality is unknown until the clock runs out it must be a
/// reservoir rather than a preplanned key range: bounded memory, uniform sample,
/// and it behaves identically whether the fill reached 2M aggregates or 200M.
pub struct AckLedger {
    cap: usize,
    entries: HashMap<AckKey, u64>,
    /// Parallel index so eviction can pick a victim in O(1).
    keys: Vec<AckKey>,
    observed: u64,
    rng: u64,
}

impl AckLedger {
    pub fn new(capacity: usize, seed: u64) -> Self {
        Self {
            cap: capacity,
            entries: HashMap::with_capacity(capacity.min(1 << 16)),
            keys: Vec::with_capacity(capacity.min(1 << 16)),
            observed: 0,
            rng: seed ^ 0x9E37_79B9_7F4A_7C15,
        }
    }

    /// Record an ack. Keeps the MAXIMUM seq, not the latest: a client retries
    /// with a held-constant `client_seq` after an ambiguous result, so acks
    /// legitimately arrive out of order. Last-write-wins would lower the ack
    /// high-water and make `verify_no_seq_gaps` report data loss that never
    /// happened — a false alarm on a correctness check costs as much as a miss.
    pub fn record(&mut self, key: AckKey, client_seq: u64) {
        if let Some(v) = self.entries.get_mut(&key) {
            *v = (*v).max(client_seq);
            return;
        }
        self.observed += 1;
        if self.cap == 0 {
            return;
        }
        if self.keys.len() < self.cap {
            self.keys.push(key);
            self.entries.insert(key, client_seq);
            return;
        }
        // Algorithm R over distinct keys: the nth new key displaces a uniformly
        // chosen resident with probability cap/n. Already-resident keys are
        // never re-offered, so a key that made it in stays in.
        let j = self.next_rand() % self.observed;
        if j < self.cap as u64 {
            let victim = self.keys[j as usize];
            self.entries.remove(&victim);
            self.keys[j as usize] = key;
            self.entries.insert(key, client_seq);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// Ack offers for a key that was not resident at the time.
    ///
    /// **Not a distinct-key count.** A key that is sampled out and later written
    /// again is counted twice, because recognising it would need the unbounded
    /// index this ledger exists to avoid. So this is an upper bound on distinct
    /// keys and a lower bound on total acks. Divergence from `len()` is what
    /// proves sampling happened; it is not the run's cardinality figure.
    ///
    /// True distinct cardinality comes from `population::Population::births_total`,
    /// which is exact because every mint is a fresh id by construction.
    pub fn ack_offers(&self) -> u64 {
        self.observed
    }
    pub fn max_acked(&self, key: AckKey) -> Option<u64> {
        self.entries.get(&key).copied()
    }
    pub fn entries(&self) -> Vec<(AckKey, u64)> {
        self.entries.iter().map(|(k, v)| (*k, *v)).collect()
    }

    /// Emit the shape `verify_no_seq_gaps` consumes. It takes
    /// `&[TaskAckSummary]` and works unchanged once the ledger produces this.
    pub fn to_task_ack_summaries(&self) -> Vec<crate::TaskAckSummary> {
        self.entries
            .iter()
            .map(|(k, seq)| crate::TaskAckSummary {
                aggregate_key: account_key(k.aggregate_id),
                client_id: k.client_id,
                max_acked_client_seq: *seq,
            })
            .collect()
    }

    fn next_rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_events_take_the_datablock_path() {
        // Major 5 exists to exercise datablock packing. If it fitted inline the
        // scenario would measure the wrong storage path while reporting a mix.
        for n in [0u64, 7, 12345] {
            for target in [STATEMENT_MIN_BYTES, 8192, STATEMENT_MAX_BYTES] {
                let e = account_event(MAJOR_STATEMENT_ATTACHED, 1, n, target);
                assert!(takes_datablock(e.event_value.len()), "len {}", e.event_value.len());
                assert!(e.event_value.len() >= STATEMENT_MIN_BYTES - 64);
                assert!(e.event_value.len() <= STATEMENT_MAX_BYTES + 64);
            }
        }
    }

    #[test]
    fn small_majors_stay_inline() {
        // The inline/datablock split is the whole basis of the sizing model.
        for major in [MAJOR_DEPOSITED, MAJOR_WITHDRAWN, MAJOR_TRANSFERRED_OUT, MAJOR_TRANSFERRED_IN] {
            let e = account_event(major, 1, 42, 0);
            assert!(!takes_datablock(e.event_value.len()), "major {major} len {}", e.event_value.len());
        }
    }

    #[test]
    fn every_major_emits_json_matching_its_registered_schema_shape() {
        // A payload that does not parse would be rejected by the validator and
        // the run would measure error handling instead of the write path.
        for major in ALL_MAJORS {
            let e = account_event(major, 1, 99, 6000);
            let v: serde_json::Value = serde_json::from_slice(&e.event_value)
                .unwrap_or_else(|err| panic!("major {major} emitted invalid JSON: {err}"));
            assert!(v.is_object());
            assert!(e.iv.is_none(), "an IV would make the server skip validation entirely");
        }
    }
}
