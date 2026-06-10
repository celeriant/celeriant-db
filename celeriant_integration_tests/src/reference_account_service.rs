//! The reference BFF account service, driven as a fleet of replicas.
//!
//! Pins the request-dedup guarantee that the docs promise for HPA deployments:
//! a retried request (same Idempotency-Key) returns the original response and
//! appends nothing, no matter which replica it lands on, for both projection
//! shapes: in-memory (each replica folds the stream, the fold maintains the
//! dedup index) and Postgres (cursor and index share the table, move
//! atomically). Concurrent sibling requests across replicas must each land
//! exactly once.
//!
//! The in-memory variant always runs. The Postgres variant runs when
//! POSTGRES_URL is set and is skipped (with a notice) otherwise.

use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::{CeleriantPool, PoolOptions};
use celeriant_reference::account_service_mem::MemAccountService;
use celeriant_reference::account_service_pg::PgAccountService;
use celeriant_reference::constants::{account_key, deterministic_id, u128_to_uuid};
use celeriant_reference::types::{AccountError, TransferResult, WriteResult};
use celeriant_wal::aggregate_key::AggregateKey;

use crate::common::{flatten, read_all};
use crate::{ServerConfig, TestServer};

/// The surface both projection variants share; lets one scenario suite drive
/// either fleet.
trait AccountApi {
    async fn deposit(&self, account_id: u128, amount_cents: i32, event_id: u128)
        -> Result<WriteResult, AccountError>;
    async fn withdraw(&self, account_id: u128, amount_cents: i32, event_id: u128)
        -> Result<WriteResult, AccountError>;
    async fn transfer(&self, from: u128, to: u128, amount_cents: i32, event_id: u128)
        -> Result<TransferResult, AccountError>;
}

impl AccountApi for MemAccountService {
    async fn deposit(&self, a: u128, c: i32, e: u128) -> Result<WriteResult, AccountError> {
        MemAccountService::deposit(self, a, c, e).await
    }
    async fn withdraw(&self, a: u128, c: i32, e: u128) -> Result<WriteResult, AccountError> {
        MemAccountService::withdraw(self, a, c, e).await
    }
    async fn transfer(&self, f: u128, t: u128, c: i32, e: u128) -> Result<TransferResult, AccountError> {
        MemAccountService::transfer(self, f, t, c, e).await
    }
}

impl AccountApi for PgAccountService {
    async fn deposit(&self, a: u128, c: i32, e: u128) -> Result<WriteResult, AccountError> {
        PgAccountService::deposit(self, a, c, e).await
    }
    async fn withdraw(&self, a: u128, c: i32, e: u128) -> Result<WriteResult, AccountError> {
        PgAccountService::withdraw(self, a, c, e).await
    }
    async fn transfer(&self, f: u128, t: u128, c: i32, e: u128) -> Result<TransferResult, AccountError> {
        PgAccountService::transfer(self, f, t, c, e).await
    }
}

async fn count_events(
    client: &mut CeleriantClient,
    key: &AggregateKey,
) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(flatten(&read_all(client, key).await?).len())
}

/// Drives a two-replica fleet through the dedup scenarios. `acct`/`acct2` must
/// be fresh aggregates so event counts are exact.
async fn run_suite<A: AccountApi>(
    label: &str,
    pod_a: &A,
    pod_b: &A,
    client: &mut CeleriantClient,
    acct: u128,
    acct2: u128,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = account_key(acct);
    let key2 = account_key(acct2);
    let eid = |n: u128| deterministic_id(&format!("{label}-req-{n}"));

    // 1. Plain deposit.
    let r = pod_a.deposit(acct, 100, eid(1)).await.map_err(|e| e.to_string())?;
    assert_eq!(r.balance_cents, 100);
    assert_eq!(count_events(client, &key).await?, 1);
    println!("  [{label}] deposit lands: balance=100");

    // 2. Same-replica retry: the response was lost; the client sends the same
    // Idempotency-Key again. Original response back, nothing appended.
    let r = pod_a.deposit(acct, 100, eid(1)).await.map_err(|e| e.to_string())?;
    assert_eq!(r.balance_cents, 100, "retry must return the ORIGINAL response");
    assert_eq!(count_events(client, &key).await?, 1, "retry must append nothing");
    println!("  [{label}] same-replica retry: original response, no new event");

    // 3. The HPA case: retry lands on a DIFFERENT replica than the original.
    let r = pod_a.deposit(acct, 50, eid(2)).await.map_err(|e| e.to_string())?;
    assert_eq!(r.balance_cents, 150);
    let r = pod_b.deposit(acct, 50, eid(2)).await.map_err(|e| e.to_string())?;
    assert_eq!(r.balance_cents, 150, "cross-replica retry must return the original response");
    assert_eq!(count_events(client, &key).await?, 2, "cross-replica retry must append nothing");
    println!("  [{label}] cross-replica retry: original response, no new event");

    // 4. Concurrent duplicate: both replicas process the SAME request at once.
    // Exactly one event; both callers get the same outcome.
    let (ra, rb) = tokio::join!(
        pod_a.deposit(acct, 25, eid(3)),
        pod_b.deposit(acct, 25, eid(3)),
    );
    let (ra, rb) = (ra.map_err(|e| e.to_string())?, rb.map_err(|e| e.to_string())?);
    assert_eq!(ra.balance_cents, 175);
    assert_eq!(rb.balance_cents, 175);
    assert_eq!(count_events(client, &key).await?, 3, "concurrent duplicate must land once");
    println!("  [{label}] concurrent duplicate across replicas: exactly one event");

    // 5. Concurrent siblings: different requests racing on one aggregate.
    // Both must land, exactly once each.
    let (ra, rb) = tokio::join!(
        pod_a.deposit(acct, 10, eid(4)),
        pod_b.deposit(acct, 20, eid(5)),
    );
    ra.map_err(|e| e.to_string())?;
    rb.map_err(|e| e.to_string())?;
    assert_eq!(count_events(client, &key).await?, 5, "both siblings must land exactly once");
    let events = flatten(&read_all(client, &key).await?);
    for n in [4u128, 5] {
        let hits = events.iter().filter(|e| e.event_id == Some(eid(n))).count();
        assert_eq!(hits, 1, "request {n} must appear exactly once");
    }
    println!("  [{label}] concurrent siblings: each landed exactly once, balance=205");

    // 6. Withdraw over balance fails cleanly.
    match pod_a.withdraw(acct, 100_000, eid(6)).await {
        Err(AccountError::InsufficientFunds { balance_cents, .. }) => {
            assert_eq!(balance_cents, 205);
        }
        other => return Err(format!("expected InsufficientFunds, got {:?}", other.map(|r| r.balance_cents)).into()),
    }
    println!("  [{label}] over-balance withdraw rejected");

    // 7. Transfer, retried cross-replica: the two-aggregate write is
    // all-or-nothing and the retry reconstructs both legs.
    let r = pod_a.transfer(acct, acct2, 105, eid(7)).await.map_err(|e| e.to_string())?;
    assert_eq!(r.from.balance_cents, 100);
    assert_eq!(r.to.balance_cents, 105);
    let r = pod_b.transfer(acct, acct2, 105, eid(7)).await.map_err(|e| e.to_string())?;
    assert_eq!(r.from.balance_cents, 100, "transfer retry must return the original response");
    assert_eq!(r.to.balance_cents, 105);
    assert_eq!(count_events(client, &key).await?, 6, "transfer retry must append nothing (from leg)");
    assert_eq!(count_events(client, &key2).await?, 1, "transfer retry must append nothing (to leg)");
    println!("  [{label}] cross-replica transfer retry: original response, no new events");

    // 8. Index-lifetime regression: a dedup entry must survive its replica
    // folding other replicas' newer events. Force pod_a to fold a pod_b write,
    // then retry pod_a's very first request. If the index aged entries against
    // the fold tip instead of real elapsed time, eid(1) was just evicted and
    // this retry double-deposits.
    pod_b.deposit(acct, 7, eid(8)).await.map_err(|e| e.to_string())?;
    pod_a.deposit(acct, 9, eid(9)).await.map_err(|e| e.to_string())?; // pod_a folds pod_b's event here
    let r = pod_a.deposit(acct, 100, eid(1)).await.map_err(|e| e.to_string())?;
    assert_eq!(r.balance_cents, 100, "late retry must return the ORIGINAL response");
    assert_eq!(count_events(client, &key).await?, 8, "late retry after cross-replica folds must append nothing");
    println!("  [{label}] retry after cross-replica folds: index entry survived, no new event");

    Ok(())
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Reference account service: request dedup across replicas ===\n");

    let port = 15910 + (std::process::id() % 100) as u16;
    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let _server = TestServer::start_with_config(port, config).await?;
    let addr = format!("127.0.0.1:{}", port);
    let mut client = CeleriantClient::connect(&addr).await?;

    // ── In-memory projection fleet ──
    println!("In-memory projection, two replicas:");
    let pod_a = MemAccountService::new(Arc::new(CeleriantPool::new(PoolOptions::new(&addr))));
    let pod_b = MemAccountService::new(Arc::new(CeleriantPool::new(PoolOptions::new(&addr))));
    run_suite(
        "mem", &pod_a, &pod_b, &mut client,
        deterministic_id("mem-suite-acct-1"),
        deterministic_id("mem-suite-acct-2"),
    ).await?;

    // ── Postgres projection fleet (needs a database) ──
    match std::env::var("POSTGRES_URL") {
        Err(_) => {
            println!("\nPostgres projection: SKIPPED (set POSTGRES_URL to enable)");
        }
        Ok(url) => {
            println!("\nPostgres projection, two replicas:");
            let acct = deterministic_id("pg-suite-acct-1");
            let acct2 = deterministic_id("pg-suite-acct-2");

            let mut dbs = Vec::new();
            for _ in 0..2 {
                let (pg, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await?;
                tokio::spawn(async move { let _ = conn.await; });
                dbs.push(Arc::new(pg));
            }
            PgAccountService::init_schema(&dbs[0]).await?;
            // The Celeriant server is fresh but Postgres persists across runs;
            // stale rows would point the cursor past the new stream.
            for id in [acct, acct2] {
                dbs[0].execute("DELETE FROM account_balances WHERE account_id = $1",
                    &[&u128_to_uuid(id)]).await?;
                dbs[0].execute("DELETE FROM request_responses WHERE aggregate_id = $1",
                    &[&u128_to_uuid(id)]).await?;
            }

            let pod_a = PgAccountService::new(
                Arc::new(CeleriantPool::new(PoolOptions::new(&addr))), dbs[0].clone());
            let pod_b = PgAccountService::new(
                Arc::new(CeleriantPool::new(PoolOptions::new(&addr))), dbs[1].clone());
            run_suite("pg", &pod_a, &pod_b, &mut client, acct, acct2).await?;
        }
    }

    println!("\n=== All Tests Passed ===");
    Ok(())
}
