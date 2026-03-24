use std::sync::LazyLock;

use celeriant_wal::aggregate_key::AggregateKey;
use uuid::Uuid;

const NAMESPACE: Uuid = uuid::uuid!("a1b2c3d4-e5f6-7890-abcd-ef1234567890");

pub fn deterministic_id(name: &str) -> u128 {
    Uuid::new_v5(&NAMESPACE, name.as_bytes()).as_u128()
}

pub fn u128_to_uuid(id: u128) -> Uuid {
    Uuid::from_u128(id)
}

pub static ORG_ID: LazyLock<u128> = LazyLock::new(|| deterministic_id("DemoOrg"));
pub static ACCOUNT_TYPE_ID: LazyLock<u128> = LazyLock::new(|| deterministic_id("Account"));

/// Single service-owned ClientId. All API instances share this identity.
/// ClientEventIndex is per (AggregateKey, ClientId) — OCC serialises concurrent writes.
pub static SERVICE_CLIENT_ID: LazyLock<u128> =
    LazyLock::new(|| deterministic_id("ReferenceApiService"));

pub struct AccountSeed {
    pub name: &'static str,
    pub id: u128,
    pub seed_cents: i32,
}

pub static ACCOUNTS: LazyLock<Vec<AccountSeed>> = LazyLock::new(|| {
    vec![
        AccountSeed { name: "Alice", id: deterministic_id("Alice"), seed_cents: 50_000 },
        AccountSeed { name: "Bob", id: deterministic_id("Bob"), seed_cents: 25_000 },
        AccountSeed { name: "Charlie", id: deterministic_id("Charlie"), seed_cents: 10_000 },
    ]
});

pub fn account_key(account_id: u128) -> AggregateKey {
    AggregateKey::new(*ORG_ID, *ACCOUNT_TYPE_ID, account_id)
}
