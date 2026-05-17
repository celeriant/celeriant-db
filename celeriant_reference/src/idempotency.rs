use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;

const TTL: Duration = Duration::from_secs(90);

struct Entry {
    result: Value,
    expires_at: Instant,
}

/// In-memory HTTP-level idempotency cache with 90-second TTL and lazy eviction.
/// Per-instance, non-durable — a convenience layer on top of Celeriant's infrastructure-level
/// ClientSeq deduplication.
pub struct IdempotencyCache {
    entries: Mutex<HashMap<u128, Entry>>,
}

impl IdempotencyCache {
    pub fn new() -> Self {
        Self { entries: Mutex::new(HashMap::new()) }
    }

    pub fn try_get(&self, key: u128) -> Option<Value> {
        let mut map = self.entries.lock().unwrap();
        evict(&mut map);
        let entry = map.get(&key)?;
        if entry.expires_at > Instant::now() { Some(entry.result.clone()) } else { None }
    }

    pub fn set(&self, key: u128, result: Value) {
        self.entries.lock().unwrap().insert(key, Entry {
            result,
            expires_at: Instant::now() + TTL,
        });
    }
}

fn evict(map: &mut HashMap<u128, Entry>) {
    let now = Instant::now();
    map.retain(|_, v| v.expires_at > now);
}
