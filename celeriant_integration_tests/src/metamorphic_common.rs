//! Shared helpers for metamorphic parity tests.
//!
//! `response_digest` is a client-side `DefaultHasher` fingerprint over the read
//! response fields; it is NOT the server's Blake3 WAL tip hash (which is not
//! exposed through the client API). Redundant with per-field equality — kept
//! as a compact log signal.

use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_wal::aggregate_key::AggregateKey;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// How to compare two read results.
///
/// Time/UUID artefacts differ across independent runs of the same workload,
/// so cross-run comparisons must skip them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffMode {
    /// Both sides read from the same cluster state (leader vs follower).
    /// Every field must match byte-for-byte.
    SameRun,
    /// Each side is a separate run of the same workload (standalone vs
    /// distributed). Skip `event_id`, `server_timestamp`, `event_timestamp`.
    CrossRun,
}

pub fn format_key(key: &AggregateKey) -> String {
    format!("({},{},{})", key.org_id, key.aggregate_type_id, key.aggregate_id)
}

pub fn response_digest(batches: &[AggregateEventBatch]) -> u64 {
    let mut h = DefaultHasher::new();
    for b in batches {
        b.aggregate_version.hash(&mut h);
        b.client_id.hash(&mut h);
        b.user_id.hash(&mut h);
        b.server_timestamp.hash(&mut h);
        for e in &b.events {
            e.client_seq.hash(&mut h);
            e.event_seq.hash(&mut h);
            e.event_id.hash(&mut h);
            e.event_timestamp.hash(&mut h);
            e.event_type_major.hash(&mut h);
            e.event_type_minor.hash(&mut h);
            e.event_value.as_slice().hash(&mut h);
            e.iv.hash(&mut h);
        }
    }
    h.finish()
}

/// Diff two reads for one aggregate. Returns `Err` with a precise description
/// of the first differing field; no "just throw an assertion". Sides are named
/// `a` and `b` in error messages so callers can label either as leader/follower,
/// standalone/cluster, old/new leader, etc.
pub fn diff_aggregate(
    key: &AggregateKey,
    a: &[AggregateEventBatch],
    b: &[AggregateEventBatch],
    mode: DiffMode,
) -> Result<(), String> {
    let k = format_key(key);

    if a.len() != b.len() {
        return Err(format!(
            "aggregate {}: batch count mismatch (a={}, b={})",
            k, a.len(), b.len()
        ));
    }

    for (i, (ab, bb)) in a.iter().zip(b.iter()).enumerate() {
        if ab.aggregate_version != bb.aggregate_version {
            return Err(format!(
                "aggregate {} batch[{}]: aggregate_version mismatch (a={}, b={})",
                k, i, ab.aggregate_version, bb.aggregate_version
            ));
        }
        if ab.client_id != bb.client_id {
            return Err(format!(
                "aggregate {} batch[{}] (idx={}): client_id mismatch (a={}, b={})",
                k, i, ab.aggregate_version, ab.client_id, bb.client_id
            ));
        }
        if ab.user_id != bb.user_id {
            return Err(format!(
                "aggregate {} batch[{}] (idx={}): user_id mismatch (a={:?}, b={:?})",
                k, i, ab.aggregate_version, ab.user_id, bb.user_id
            ));
        }
        if mode == DiffMode::SameRun && ab.server_timestamp != bb.server_timestamp {
            return Err(format!(
                "aggregate {} batch[{}] (idx={}): server_timestamp mismatch (a={}, b={}) — leader-assigned, should propagate unchanged",
                k, i, ab.aggregate_version, ab.server_timestamp, bb.server_timestamp
            ));
        }
        if ab.events.len() != bb.events.len() {
            return Err(format!(
                "aggregate {} batch[{}] (idx={}): event count mismatch (a={}, b={})",
                k, i, ab.aggregate_version, ab.events.len(), bb.events.len()
            ));
        }
        for (j, (ae, be)) in ab.events.iter().zip(bb.events.iter()).enumerate() {
            let bidx = ab.aggregate_version;
            if ae.client_seq != be.client_seq {
                return Err(format!(
                    "aggregate {} batch[{}] (idx={}) event[{}]: client_seq mismatch (a={}, b={})",
                    k, i, bidx, j, ae.client_seq, be.client_seq
                ));
            }
            if ae.event_seq != be.event_seq {
                return Err(format!(
                    "aggregate {} batch[{}] (idx={}) event[{}] (client_idx={}): event_seq mismatch (a={}, b={})",
                    k, i, bidx, j, ae.client_seq, ae.event_seq, be.event_seq
                ));
            }
            if mode == DiffMode::SameRun && ae.event_id != be.event_id {
                return Err(format!(
                    "aggregate {} batch[{}] (idx={}) event[{}] (client_idx={}): event_id mismatch (a={:?}, b={:?})",
                    k, i, bidx, j, ae.client_seq, ae.event_id, be.event_id
                ));
            }
            if mode == DiffMode::SameRun && ae.event_timestamp != be.event_timestamp {
                return Err(format!(
                    "aggregate {} batch[{}] (idx={}) event[{}] (client_idx={}): event_timestamp mismatch (a={}, b={})",
                    k, i, bidx, j, ae.client_seq, ae.event_timestamp, be.event_timestamp
                ));
            }
            if ae.event_type_major != be.event_type_major
                || ae.event_type_minor != be.event_type_minor
            {
                return Err(format!(
                    "aggregate {} batch[{}] (idx={}) event[{}] (client_idx={}): event_type mismatch (a={}.{}, b={}.{})",
                    k, i, bidx, j, ae.client_seq,
                    ae.event_type_major, ae.event_type_minor,
                    be.event_type_major, be.event_type_minor
                ));
            }
            if ae.iv != be.iv {
                return Err(format!(
                    "aggregate {} batch[{}] (idx={}) event[{}] (client_idx={}): iv mismatch",
                    k, i, bidx, j, ae.client_seq
                ));
            }
            if ae.event_value.as_slice() != be.event_value.as_slice() {
                return Err(format!(
                    "aggregate {} batch[{}] (idx={}) event[{}] (client_idx={}): event_value bytes mismatch (a_len={}, b_len={})",
                    k, i, bidx, j, ae.client_seq,
                    ae.event_value.len(), be.event_value.len()
                ));
            }
        }
    }

    if mode == DiffMode::SameRun {
        let ah = response_digest(a);
        let bh = response_digest(b);
        if ah != bh {
            return Err(format!(
                "aggregate {}: response digest mismatch despite per-field equality — indicates a field this diff doesn't cover (a={:016x}, b={:016x})",
                k, ah, bh
            ));
        }
    }

    Ok(())
}
