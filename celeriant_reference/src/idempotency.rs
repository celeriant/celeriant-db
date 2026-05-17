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
/// Not a correctness layer. Server-side `enforce_client_idempotency` (CEI) is the
/// underlying dedup for retries that hold `client_seq` constant. This cache shortens
/// the cross-instance recovery path for the BFF-crash-after-fsync case.
pub struct IdempotencyCache {
    entries: Mutex<HashMap<(u128, u128), CacheEntry>>,
}

impl IdempotencyCache {
    pub fn new() -> Self {
        Self { entries: Mutex::new(HashMap::new()) }
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
}

fn evict(map: &mut HashMap<(u128, u128), CacheEntry>) {
    let now = Instant::now();
    map.retain(|_, v| v.expires_at > now);
}
