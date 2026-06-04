use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const TTL: Duration = Duration::from_secs(90);

#[derive(Clone, Copy)]
pub struct IdempotencyEntry {
    pub balance_cents: i64,
    pub aggregate_version: u64,
}

struct CacheEntry {
    value: IdempotencyEntry,
    expires_at: Instant,
}

/// Per-instance idempotency cache keyed by (event_id, aggregate_id).
///
/// Populated from two sources:
/// - HTTP handler after a successful write (in-instance retries hit immediately)
/// - `AccountService::catch_up`, which reconstructs the response for any replayed
///   event that carries an `event_id`. This warms cold instances after a BFF crash
///   so a retried `Idempotency-Key` can be resolved without re-writing.
///
/// `entries` is not a correctness layer: server-side CEI is the dedup, this just
/// restores the lost response. `seq_owners` IS load-bearing. Requests share the
/// service's client_id, so two can derive the same client_seq; if the loser's OCC
/// rejection is lost to a timeout, its retry gets a CEI violation that refers to
/// the sibling's event. The `(aggregate_id, client_seq) -> event_id` map is how
/// the violation arm tells "mine" from "theirs" before claiming success.
pub struct IdempotencyCache {
    entries: Mutex<HashMap<(u128, u128), CacheEntry>>,
    seq_owners: Mutex<HashMap<(u128, u64), SeqOwnerEntry>>,
}

struct SeqOwnerEntry {
    event_id: u128,
    expires_at: Instant,
}

impl IdempotencyCache {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            seq_owners: Mutex::new(HashMap::new()),
        }
    }

    pub fn try_get(&self, event_id: u128, aggregate_id: u128) -> Option<IdempotencyEntry> {
        let mut map = self.entries.lock().unwrap();
        evict(&mut map);
        let entry = map.get(&(event_id, aggregate_id))?;
        if entry.expires_at > Instant::now() {
            Some(entry.value)
        } else {
            None
        }
    }

    pub fn set(&self, event_id: u128, aggregate_id: u128, value: IdempotencyEntry) {
        self.entries.lock().unwrap().insert((event_id, aggregate_id), CacheEntry {
            value,
            expires_at: Instant::now() + TTL,
        });
    }

    /// Which event_id landed on this (aggregate, client_seq)? None = unknown
    /// (outside the warm window, or the event carried no event_id).
    pub fn seq_owner(&self, aggregate_id: u128, client_seq: u64) -> Option<u128> {
        let mut map = self.seq_owners.lock().unwrap();
        let now = Instant::now();
        map.retain(|_, v| v.expires_at > now);
        map.get(&(aggregate_id, client_seq)).map(|e| e.event_id)
    }

    pub fn set_seq_owner(&self, aggregate_id: u128, client_seq: u64, event_id: u128) {
        self.seq_owners.lock().unwrap().insert((aggregate_id, client_seq), SeqOwnerEntry {
            event_id,
            expires_at: Instant::now() + TTL,
        });
    }
}

fn evict(map: &mut HashMap<(u128, u128), CacheEntry>) {
    let now = Instant::now();
    map.retain(|_, v| v.expires_at > now);
}
