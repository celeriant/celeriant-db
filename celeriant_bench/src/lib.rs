use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use celeriant_client_tokio::ClientTlsConfig;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_client_tokio::{ClientError, ServerError, WriteError, WriteEventsOptions};

/// Re-exported so consumers (the chaos runner) don't need a direct dependency
/// on `celeriant_client_tokio` just to name the pool type returned by
/// `PoolBuilder::build`.
pub use celeriant_client_tokio::pool::CeleriantPool as Pool;
use celeriant_crypto::pki::PkiManager;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use rustls_pki_types::ServerName;
use tokio::sync::Barrier;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub num_tasks: usize,
    pub total_requests: u64,
    pub errors: u64,
    pub throughput: f64,
    pub avg_latency_ms: f64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub p999_ms: u64,
    pub min_ms: u64,
    pub max_ms: u64,
}

pub fn expand_home(path: &str) -> PathBuf {
    if path.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(&path[2..]);
        }
    }
    PathBuf::from(path)
}

pub async fn resolve_to_ip(host_port: &str) -> Result<String, Box<dyn std::error::Error>> {
    let addrs: Vec<_> = tokio::net::lookup_host(host_port).await?.collect();
    let addr = addrs.first().ok_or_else(|| format!("DNS lookup failed for {host_port}"))?;
    Ok(addr.to_string())
}

pub fn build_tls_config(
    ca_cert: &str,
    client_cert: &str,
    client_key: &str,
    server_name: &str,
) -> Result<ClientTlsConfig, Box<dyn std::error::Error>> {
    let ca_bundle = PkiManager::load_ca_bundle(&expand_home(ca_cert))?;
    let (cert_chain, key) = PkiManager::load_identity(&expand_home(client_cert), &expand_home(client_key))?;
    let client_config = PkiManager::build_client_config(&ca_bundle, cert_chain, key)?;
    let sni = ServerName::try_from(server_name.to_string())
        .map_err(|e| format!("Invalid server name '{server_name}': {e}"))?;
    Ok(ClientTlsConfig::new(client_config, sni))
}

pub struct PoolBuilder<'a> {
    pub address1: &'a str,
    pub address2: &'a str,
    pub server_name: Option<&'a str>,
    pub ca_cert: &'a str,
    pub client_cert: &'a str,
    pub client_key: &'a str,
    pub plaintext: bool,
    pub max_connections: usize,
}

impl<'a> PoolBuilder<'a> {
    pub async fn build(self) -> Result<Arc<CeleriantPool>, Box<dyn std::error::Error>> {
        let resolved1 = resolve_to_ip(self.address1).await?;
        let resolved2 = resolve_to_ip(self.address2).await?;

        let mut opts = PoolOptions::new(&resolved1)
            .with_seed_addresses(vec![resolved2])
            .with_max_connections(self.max_connections)
            .with_connection_timeout(Duration::from_secs(30))
            .with_request_timeout(Duration::from_secs(5));

        if !self.plaintext {
            let server_name = self
                .server_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| self.address1.split(':').next().unwrap_or(self.address1).to_string());
            opts = opts.with_tls(build_tls_config(self.ca_cert, self.client_cert, self.client_key, &server_name)?);
        }

        Ok(Arc::new(CeleriantPool::new(opts)))
    }
}

pub async fn smoke_test(pool: &Arc<CeleriantPool>) -> Result<(), Box<dyn std::error::Error>> {
    let smoke_key = AggregateKey::new(99, 99, 99);
    let smoke_event = DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: None,
        event_timestamp: 0,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(b"smoke-test".to_vec()),
        iv: None,
    };
    pool.write_events(smoke_key, vec![smoke_event]).await?;
    Ok(())
}

pub async fn run_benchmark(
    pool: &Arc<CeleriantPool>,
    num_tasks: usize,
    duration_secs: u64,
) -> BenchmarkResult {
    let barrier = Arc::new(Barrier::new(num_tasks));
    let total_ok = Arc::new(AtomicU64::new(0));
    let total_err = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::with_capacity(num_tasks);
    let start = Instant::now();

    for id in 0..num_tasks {
        let pool = Arc::clone(pool);
        let barrier = barrier.clone();
        let ok_counter = total_ok.clone();
        let err_counter = total_err.clone();

        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            barrier.wait().await;
            let deadline = Instant::now() + Duration::from_secs(duration_secs);
            let mut seq = 0u64;
            // Jittered exponential backoff on repeated errors. Without this,
            // 4000 concurrent tasks hammer a broken leader during the failover
            // window and generate millions of cheap errors per second, which
            // swamps the BenchErrorsBounded invariant even when the system is
            // actually behaving correctly. Reset to zero on every success so a
            // brief blip doesn't leak into steady-state throughput.
            let mut backoff_ms: u64 = 0;
            const BACKOFF_INITIAL_MS: u64 = 10;
            const BACKOFF_MAX_MS: u64 = 500;

            while Instant::now() < deadline {
                let event = DatablockAggregateEvent {
                    client_seq: 0,
                    event_seq: 0,
                    event_id: None,
                    event_timestamp: 0,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(format!("[t-{id}-r-{seq}] hello").into_bytes()),
                    iv: None,
                };

                let key = AggregateKey::new(1, 1, id as u128);
                let req_start = Instant::now();
                match pool.write_events(key, vec![event]).await {
                    Ok(_) => {
                        latencies.push(req_start.elapsed().as_millis() as u64);
                        ok_counter.fetch_add(1, Ordering::Relaxed);
                        backoff_ms = 0;
                    }
                    Err(e) => {
                        err_counter.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Task {id} error: {e}");
                        let next = if backoff_ms == 0 {
                            BACKOFF_INITIAL_MS
                        } else {
                            (backoff_ms * 2).min(BACKOFF_MAX_MS)
                        };
                        // 50-150% jitter on top of the base delay so tasks
                        // don't resync into lock-step retry waves.
                        let jitter_num = ((id as u64).wrapping_mul(2654435761).wrapping_add(seq)) % 1000;
                        let sleep_ms = next / 2 + (next * jitter_num) / 1000;
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                        backoff_ms = next;
                    }
                }
                seq += 1;
            }
            latencies
        }));
    }

    let mut all_latencies = Vec::new();
    for task in tasks {
        if let Ok(lats) = task.await {
            all_latencies.extend(lats);
        }
    }

    let elapsed = start.elapsed();
    let ok = total_ok.load(Ordering::Relaxed);
    let errors = total_err.load(Ordering::Relaxed);
    all_latencies.sort_unstable();

    let throughput = ok as f64 / elapsed.as_secs_f64();
    let (avg, p50, p95, p99, p999, min, max) = if !all_latencies.is_empty() {
        let len = all_latencies.len();
        let avg = all_latencies.iter().sum::<u64>() as f64 / len as f64;
        (
            avg,
            all_latencies[len * 50 / 100],
            all_latencies[len * 95 / 100],
            all_latencies[len * 99 / 100],
            all_latencies[len * 999 / 1000],
            all_latencies[0],
            all_latencies[len - 1],
        )
    } else {
        (0.0, 0, 0, 0, 0, 0, 0)
    };

    BenchmarkResult {
        num_tasks,
        total_requests: ok,
        errors,
        throughput,
        avg_latency_ms: avg,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        p999_ms: p999,
        min_ms: min,
        max_ms: max,
    }
}

/// Per-task summary from the idempotent bench: what the client *believes*
/// is durable. `max_acked_client_seq` is the largest `client_seq` whose write
/// either returned `Ok` or `ClientIdempotencyViolation` (server says "already
/// applied" — client treats that the same as a fresh ACK). Used by
/// `verify_no_seq_gaps` to detect false-ack data loss.
#[derive(Debug, Clone)]
pub struct TaskAckSummary {
    pub aggregate_key: AggregateKey,
    pub client_id: u128,
    pub max_acked_client_seq: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct IdempotentBenchCounters {
    /// Writes that returned `Ok`.
    pub ok_acks: u64,
    /// Writes that returned `ClientIdempotencyViolation` (2002). Treated as
    /// an ACK by the client; counted separately because a non-zero value
    /// outside of forced retries is itself worth surfacing.
    pub idempotency_acks: u64,
    /// Writes that returned `ReplicationError` — explicit "I didn't commit"
    /// signal. Retried with the same `client_seq`.
    pub replication_retries: u64,
    /// Network / connection / timeout failures. Retried with the same
    /// `client_seq` (idempotency on the server makes that safe).
    pub transient_retries: u64,
    /// Non-retriable server errors that abort a task (schema, OCC, etc.).
    pub fatal_errors: u64,
}

#[derive(Debug, Clone)]
pub struct IdempotentBenchOutcome {
    pub benchmark: BenchmarkResult,
    pub counters: IdempotentBenchCounters,
    pub task_acks: Vec<TaskAckSummary>,
}

/// Variant of `run_benchmark` that exercises the idempotent-write path so we
/// can detect false-ack data loss. Each task:
///
/// 1. Owns a unique aggregate and a unique `client_id`.
/// 2. Maintains a monotonically increasing `client_seq` starting at 1.
/// 3. Submits writes with `enforce_client_idempotency: true`.
/// 4. On `Ok` *or* `ClientIdempotencyViolation`, treats the current `client_seq`
///    as durable and advances. (`ClientIdempotencyViolation` is what a server
///    returns when it thinks the client has already written this seq — a real
///    client must accept that signal as an ACK.)
/// 5. On `ReplicationError` or transport errors, retries with the same
///    `client_seq` (server-side idempotency makes that safe).
///
/// The returned `task_acks` are what the client *believes* is durable. Feed
/// them to `verify_no_seq_gaps` after the cluster has had a chance to settle.
pub async fn run_benchmark_idempotent(
    pool: &Arc<CeleriantPool>,
    num_tasks: usize,
    duration_secs: u64,
) -> IdempotentBenchOutcome {
    let barrier = Arc::new(Barrier::new(num_tasks));
    let total_ok = Arc::new(AtomicU64::new(0));
    let total_err = Arc::new(AtomicU64::new(0));
    let ok_acks = Arc::new(AtomicU64::new(0));
    let idempotency_acks = Arc::new(AtomicU64::new(0));
    let replication_retries = Arc::new(AtomicU64::new(0));
    let transient_retries = Arc::new(AtomicU64::new(0));
    let fatal_errors = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::with_capacity(num_tasks);
    let start = Instant::now();

    for id in 0..num_tasks {
        let pool = Arc::clone(pool);
        let barrier = barrier.clone();
        let total_ok = total_ok.clone();
        let total_err = total_err.clone();
        let ok_acks = ok_acks.clone();
        let idempotency_acks = idempotency_acks.clone();
        let replication_retries = replication_retries.clone();
        let transient_retries = transient_retries.clone();
        let fatal_errors = fatal_errors.clone();

        // client_id 0 is the protocol-level default; give every task a
        // distinct non-zero id so the server-side idempotency cache doesn't
        // collapse across tasks.
        let client_id: u128 = (id as u128) + 1;
        let aggregate_key = AggregateKey::new(1, 1, id as u128);

        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            barrier.wait().await;
            let deadline = Instant::now() + Duration::from_secs(duration_secs);

            let mut current_seq: u64 = 1;
            let mut max_acked: u64 = 0;
            let mut backoff_ms: u64 = 0;
            const BACKOFF_INITIAL_MS: u64 = 10;
            const BACKOFF_MAX_MS: u64 = 500;

            while Instant::now() < deadline {
                let event = DatablockAggregateEvent {
                    client_seq: current_seq,
                    event_seq: 0,
                    event_id: None,
                    event_timestamp: 0,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(format!("[t-{id}-s-{current_seq}]").into_bytes()),
                    iv: None,
                };

                let req_start = Instant::now();
                let res = pool.write_events_with(
                    aggregate_key.clone(),
                    vec![event],
                    WriteEventsOptions {
                        client_id,
                        allow_create: true,
                        expected_version: None,
                        enforce_client_idempotency: true,
                    },
                ).await;

                match res {
                    Ok(_) => {
                        latencies.push(req_start.elapsed().as_millis() as u64);
                        total_ok.fetch_add(1, Ordering::Relaxed);
                        ok_acks.fetch_add(1, Ordering::Relaxed);
                        max_acked = current_seq;
                        current_seq += 1;
                        backoff_ms = 0;
                    }
                    Err(ClientError::Server(ServerError::Write {
                        kind: WriteError::ClientIdempotencyViolation { .. },
                        ..
                    })) => {
                        // Server says "already applied". Real client treats as ACK.
                        // If the bug exists, the underlying write may have been
                        // rolled back and this is a false ACK — the verifier
                        // catches that by reading the aggregate back.
                        idempotency_acks.fetch_add(1, Ordering::Relaxed);
                        max_acked = current_seq;
                        current_seq += 1;
                        backoff_ms = 0;
                    }
                    Err(ClientError::Server(ServerError::Write {
                        kind: WriteError::ReplicationError, ..
                    })) => {
                        total_err.fetch_add(1, Ordering::Relaxed);
                        replication_retries.fetch_add(1, Ordering::Relaxed);
                        sleep_with_backoff(&mut backoff_ms, BACKOFF_INITIAL_MS, BACKOFF_MAX_MS, id as u64, current_seq).await;
                    }
                    Err(ClientError::Server(ServerError::Write {
                        kind: WriteError::FsyncError | WriteError::CacheAggregateClientError | WriteError::AggregateExistsCacheError, ..
                    })) => {
                        total_err.fetch_add(1, Ordering::Relaxed);
                        replication_retries.fetch_add(1, Ordering::Relaxed);
                        sleep_with_backoff(&mut backoff_ms, BACKOFF_INITIAL_MS, BACKOFF_MAX_MS, id as u64, current_seq).await;
                    }
                    Err(ClientError::Server(ServerError::Write {
                        kind: WriteError::InflightDuplicateWrite { .. }, ..
                    })) => {
                        // Not an ack. Retry with the same client_seq until replication
                        // completes or the write rolls back.
                        total_err.fetch_add(1, Ordering::Relaxed);
                        replication_retries.fetch_add(1, Ordering::Relaxed);
                        sleep_with_backoff(&mut backoff_ms, BACKOFF_INITIAL_MS, BACKOFF_MAX_MS, id as u64, current_seq).await;
                    }
                    Err(e) => {
                        total_err.fetch_add(1, Ordering::Relaxed);
                        // Transport/transient failures retry with the same
                        // client_seq. The server's idempotency check makes
                        // this safe — the worst case is a 2002 next time.
                        if is_transient(&e) {
                            transient_retries.fetch_add(1, Ordering::Relaxed);
                            sleep_with_backoff(&mut backoff_ms, BACKOFF_INITIAL_MS, BACKOFF_MAX_MS, id as u64, current_seq).await;
                        } else {
                            fatal_errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("idempotent bench task {id} fatal at seq {current_seq}: {e}");
                            break;
                        }
                    }
                }
            }

            (latencies, TaskAckSummary {
                aggregate_key,
                client_id,
                max_acked_client_seq: max_acked,
            })
        }));
    }

    let mut all_latencies = Vec::new();
    let mut task_acks = Vec::with_capacity(num_tasks);
    for task in tasks {
        if let Ok((lats, ack)) = task.await {
            all_latencies.extend(lats);
            task_acks.push(ack);
        }
    }

    let elapsed = start.elapsed();
    let ok = total_ok.load(Ordering::Relaxed);
    let errors = total_err.load(Ordering::Relaxed);
    all_latencies.sort_unstable();

    let throughput = ok as f64 / elapsed.as_secs_f64();
    let (avg, p50, p95, p99, p999, min, max) = if !all_latencies.is_empty() {
        let len = all_latencies.len();
        let avg = all_latencies.iter().sum::<u64>() as f64 / len as f64;
        (
            avg,
            all_latencies[len * 50 / 100],
            all_latencies[len * 95 / 100],
            all_latencies[len * 99 / 100],
            all_latencies[len * 999 / 1000],
            all_latencies[0],
            all_latencies[len - 1],
        )
    } else {
        (0.0, 0, 0, 0, 0, 0, 0)
    };

    IdempotentBenchOutcome {
        benchmark: BenchmarkResult {
            num_tasks,
            total_requests: ok,
            errors,
            throughput,
            avg_latency_ms: avg,
            p50_ms: p50,
            p95_ms: p95,
            p99_ms: p99,
            p999_ms: p999,
            min_ms: min,
            max_ms: max,
        },
        counters: IdempotentBenchCounters {
            ok_acks: ok_acks.load(Ordering::Relaxed),
            idempotency_acks: idempotency_acks.load(Ordering::Relaxed),
            replication_retries: replication_retries.load(Ordering::Relaxed),
            transient_retries: transient_retries.load(Ordering::Relaxed),
            fatal_errors: fatal_errors.load(Ordering::Relaxed),
        },
        task_acks,
    }
}

fn is_transient(e: &ClientError) -> bool {
    // The bench treats almost any client-side error short of an explicit
    // application-level rejection (OCC, schema, ZeroEventType, etc.) as
    // transient and retriable. Server-rejection paths above already
    // intercepted ClientIdempotencyViolation / ReplicationError. Anything
    // landing here that's NOT a deterministic application rejection is
    // treated as retriable.
    match e {
        ClientError::Server(ServerError::Write { kind, .. }) => matches!(
            kind,
            WriteError::FsyncError
                | WriteError::CacheAggregateClientError
                | WriteError::AggregateExistsCacheError
        ),
        // Network / connection / timeout / protocol — treat as transient.
        _ => true,
    }
}

async fn sleep_with_backoff(backoff_ms: &mut u64, initial: u64, max: u64, id_seed: u64, seq_seed: u64) {
    let next = if *backoff_ms == 0 { initial } else { (*backoff_ms * 2).min(max) };
    // 50-150% jitter so retries don't synchronise.
    let jitter_num = id_seed.wrapping_mul(2654435761).wrapping_add(seq_seed) % 1000;
    let sleep_ms = next / 2 + (next * jitter_num) / 1000;
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    *backoff_ms = next;
}

/// Per-aggregate failure record from the audit.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SeqGap {
    pub aggregate_key_str: String,
    pub client_id: u128,
    pub max_acked: u64,
    /// Highest `aggregate_version` (= count of committed batches) present
    /// on the server. Each task writes 1-event batches with monotonically
    /// increasing `client_seq`, so without gaps this equals `max_acked`.
    pub max_aggregate_version: u64,
    /// `max_acked - max_aggregate_version` — the number of client_seq values
    /// the client believed were durable but that the server lost.
    pub missing_count: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DataIntegrityReport {
    pub tasks_audited: u64,
    pub tasks_with_gaps: u64,
    /// Total number of acknowledged `client_seq` values that turned out to be
    /// missing from the WAL across all tasks. The headline number for "how
    /// much was the client lied to".
    pub total_missing_acks: u64,
    /// Tasks whose audit read itself errored — counted separately so a network
    /// blip during the read pass doesn't get conflated with data loss.
    pub tasks_unreadable: u64,
    /// Up to `gap_sample_cap` representative gap records for the report —
    /// full list is dropped to keep the report file small.
    pub sample_gaps: Vec<SeqGap>,
    /// Full list of `TaskAckSummary`s that had gaps. Used by deep_audit
    /// callers to focus the follow-up inspection. Skipped in JSON output
    /// to keep the report size manageable.
    #[serde(skip)]
    pub failing_task_acks: Vec<TaskAckSummary>,
}

/// Audit each acknowledged task's aggregate via a single `aggregate_details`
/// round trip per aggregate, run with bounded parallelism. Each task writes
/// 1-event batches with monotonically increasing `client_seq` to its own
/// aggregate, so `max_aggregate_version` (committed batch count) must equal
/// `max_acked_client_seq`. Any task where the server's count is lower has
/// lost client-acknowledged data — the false-ack data-loss signature the
/// audit exists to detect.
///
/// `gap_sample_cap` bounds how many failing tasks are quoted in the
/// returned report (totals always cover everything). Concurrency is
/// bounded by `max_in_flight` so a 4000-aggregate audit doesn't melt the
/// cluster's read path.
pub async fn verify_no_seq_gaps(
    pool: &Arc<CeleriantPool>,
    acks: &[TaskAckSummary],
    gap_sample_cap: usize,
) -> DataIntegrityReport {
    verify_no_seq_gaps_with_concurrency(pool, acks, gap_sample_cap, 64).await
}

pub async fn verify_no_seq_gaps_with_concurrency(
    pool: &Arc<CeleriantPool>,
    acks: &[TaskAckSummary],
    gap_sample_cap: usize,
    max_in_flight: usize,
) -> DataIntegrityReport {
    use celeriant_msg::request::requests::AggregateDetailsRequest;
    use tokio::sync::Semaphore;

    // Retry transient read failures during the audit. Post-blackout the
    // cluster may still be settling leadership when the audit fires;
    // without retry we'd misclassify a recoverable read error as
    // `tasks_unreadable`. Backoff is jittered to avoid synchronising 8k
    // concurrent readers against the same recovering shard.
    const READ_MAX_ATTEMPTS: u32 = 6;
    const READ_BACKOFF_INITIAL_MS: u64 = 100;
    const READ_BACKOFF_MAX_MS: u64 = 2_000;

    let semaphore = Arc::new(Semaphore::new(max_in_flight.max(1)));
    let mut handles = Vec::with_capacity(acks.len());

    for ack in acks {
        if ack.max_acked_client_seq == 0 {
            continue;
        }
        let pool = Arc::clone(pool);
        let ack = ack.clone();
        let permit = Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            let _p = permit.acquire_owned().await.expect("semaphore closed");
            let mut backoff_ms: u64 = READ_BACKOFF_INITIAL_MS;
            let mut res = Err(ClientError::ProtocolError);
            for attempt in 1..=READ_MAX_ATTEMPTS {
                let attempt_res = pool
                    .aggregate_details(AggregateDetailsRequest {
                        correlation_id: None,
                        aggregate_key: ack.aggregate_key.clone(),
                    })
                    .await;
                match attempt_res {
                    Ok(d) => { res = Ok(d); break; }
                    Err(e) => {
                        res = Err(e);
                        if attempt == READ_MAX_ATTEMPTS { break; }
                        // 50-150% jitter so 8k concurrent readers don't sync up
                        let jitter = (ack.client_id as u64).wrapping_mul(2654435761) % 1000;
                        let sleep_ms = backoff_ms / 2 + (backoff_ms * jitter) / 1000;
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(READ_BACKOFF_MAX_MS);
                    }
                }
            }
            (ack, res)
        }));
    }

    let mut report = DataIntegrityReport::default();
    report.tasks_audited = handles.len() as u64;

    for handle in handles {
        let (ack, res) = match handle.await {
            Ok(t) => t,
            Err(_) => {
                report.tasks_unreadable += 1;
                continue;
            }
        };
        match res {
            Ok(details) => {
                if details.max_aggregate_version < ack.max_acked_client_seq {
                    let missing = ack.max_acked_client_seq - details.max_aggregate_version;
                    report.tasks_with_gaps += 1;
                    report.total_missing_acks = report.total_missing_acks.saturating_add(missing);
                    report.failing_task_acks.push(ack.clone());
                    if report.sample_gaps.len() < gap_sample_cap {
                        report.sample_gaps.push(SeqGap {
                            aggregate_key_str: format!("{}", ack.aggregate_key),
                            client_id: ack.client_id,
                            max_acked: ack.max_acked_client_seq,
                            max_aggregate_version: details.max_aggregate_version,
                            missing_count: missing,
                        });
                    }
                }
            }
            Err(_) => {
                report.tasks_unreadable += 1;
            }
        }
    }

    report
}

/// Per-aggregate forensic record from `deep_audit_failing_aggregates`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DeepAuditEntry {
    pub aggregate_key_str: String,
    pub client_id: u128,
    pub max_acked: u64,
    /// Distinct `client_seq` values present in the WAL for `client_id`.
    pub present_count: u64,
    /// Specific seqs in `1..=max_acked` that the client believed were durable
    /// but are absent from the WAL.
    pub missing_seqs: Vec<u64>,
    /// `(client_seq, occurrence_count)` for any seq that appears more than
    /// once. Non-empty here confirms the duplicate-acceptance theory: the
    /// same client_seq was accepted on multiple leaders during/after a
    /// lease handover, producing distinct batches.
    pub duplicate_seqs: Vec<(u64, u64)>,
    /// The set of distinct `aggregate_version`s the duplicate seqs landed on.
    /// Multiple distinct versions = different physical batches = different
    /// accepting leaders. Empty if no duplicates were observed.
    pub duplicate_aggregate_versions: Vec<u64>,
    /// Total number of batches (events) read for this aggregate's client_id.
    /// Should equal `max_aggregate_version` minus any batches that belonged
    /// to a different client_id (which shouldn't happen in the bench where
    /// one client owns one aggregate).
    pub total_batches: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DeepAuditReport {
    pub aggregates_inspected: u64,
    pub aggregates_with_duplicates: u64,
    pub total_duplicate_occurrences: u64,
    pub aggregates_unreadable: u64,
    pub entries: Vec<DeepAuditEntry>,
    /// Error strings from the first few unreadable cases.
    pub unreadable_errors_sample: Vec<String>,
}

/// Re-read each acknowledged task's aggregate event-by-event to surface the
/// specific `client_seq` values that are missing and whether any `client_seq`
/// was accepted MORE THAN ONCE (the smoking gun for the duplicate-acceptance
/// failover bug — same seq accepted on two different leaders produces two
/// physical batches with different `aggregate_version`s).
///
/// Bounded by `max_inspect` to keep run time manageable on large soaks. Pass
/// only the failing tasks (those flagged by the headline audit) for a focused
/// post-mortem.
pub async fn deep_audit_failing_aggregates(
    pool: &Arc<CeleriantPool>,
    failing: &[TaskAckSummary],
    max_inspect: usize,
    max_in_flight: usize,
) -> DeepAuditReport {
    use celeriant_msg::request::read_filters::ReadFilters;
    use tokio::sync::Semaphore;

    // Retry transient read timeouts; the cluster may still be settling post-chaos.
    const READ_MAX_ATTEMPTS: u32 = 6;
    const READ_BACKOFF_INITIAL_MS: u64 = 100;
    const READ_BACKOFF_MAX_MS: u64 = 2_000;

    let semaphore = Arc::new(Semaphore::new(max_in_flight.max(1)));
    let mut handles = Vec::with_capacity(failing.len().min(max_inspect));

    for ack in failing.iter().take(max_inspect) {
        if ack.max_acked_client_seq == 0 {
            continue;
        }
        let pool = Arc::clone(pool);
        let ack = ack.clone();
        let permit = Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            let _p = permit.acquire_owned().await.expect("semaphore closed");
            let mut backoff_ms: u64 = READ_BACKOFF_INITIAL_MS;
            let mut last_err: String = String::new();
            for attempt in 1..=READ_MAX_ATTEMPTS {
                let iter_res = pool.read_all(ack.aggregate_key.clone(), Some(ReadFilters::new(1))).await;
                let iter = match iter_res {
                    Ok(it) => it,
                    Err(e) => {
                        last_err = format!("read_all open: {e:?}");
                        if attempt == READ_MAX_ATTEMPTS { break; }
                        let jitter = (ack.client_id as u64).wrapping_mul(2654435761) % 1000;
                        let sleep_ms = backoff_ms / 2 + (backoff_ms * jitter) / 1000;
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(READ_BACKOFF_MAX_MS);
                        continue;
                    }
                };
                match iter.collect().await {
                    Ok(b) => return (ack, Ok(b)),
                    Err(e) => {
                        last_err = format!("read_all collect: {e:?}");
                        if attempt == READ_MAX_ATTEMPTS { break; }
                        let jitter = (ack.client_id as u64).wrapping_mul(2654435761) % 1000;
                        let sleep_ms = backoff_ms / 2 + (backoff_ms * jitter) / 1000;
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(READ_BACKOFF_MAX_MS);
                    }
                }
            }
            (ack, Err(last_err))
        }));
    }

    let mut report = DeepAuditReport::default();
    const MAX_ERROR_SAMPLES: usize = 8;
    for handle in handles {
        let (ack, batches) = match handle.await {
            Ok(t) => t,
            Err(e) => {
                report.aggregates_unreadable += 1;
                if report.unreadable_errors_sample.len() < MAX_ERROR_SAMPLES {
                    report.unreadable_errors_sample.push(format!("join: {e:?}"));
                }
                continue;
            }
        };
        report.aggregates_inspected += 1;
        let batches = match batches {
            Ok(b) => b,
            Err(err_string) => {
                report.aggregates_unreadable += 1;
                if report.unreadable_errors_sample.len() < MAX_ERROR_SAMPLES {
                    report.unreadable_errors_sample.push(format!("{} {}", ack.aggregate_key, err_string));
                }
                continue;
            }
        };

        // Track per-client_seq occurrence count and which aggregate_versions
        // hold it. Multiple aggregate_versions for the same client_seq is
        // the duplicate-acceptance signature.
        use std::collections::BTreeMap;
        let mut seq_occurrences: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        let mut total_batches: u64 = 0;
        for batch in &batches {
            if batch.client_id != ack.client_id {
                continue;
            }
            total_batches += 1;
            for ev in &batch.events {
                if ev.client_seq > 0 {
                    seq_occurrences.entry(ev.client_seq).or_default().push(batch.aggregate_version);
                }
            }
        }

        let present_seqs: std::collections::BTreeSet<u64> = seq_occurrences.keys().copied().collect();
        let missing_seqs: Vec<u64> = (1..=ack.max_acked_client_seq)
            .filter(|s| !present_seqs.contains(s))
            .take(64) // cap per-entry to keep the report small
            .collect();

        let duplicate_seqs: Vec<(u64, u64)> = seq_occurrences
            .iter()
            .filter(|(_, vs)| vs.len() > 1)
            .map(|(seq, vs)| (*seq, vs.len() as u64))
            .collect();
        let duplicate_aggregate_versions: Vec<u64> = if !duplicate_seqs.is_empty() {
            let mut set: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
            for (_, vs) in seq_occurrences.iter().filter(|(_, vs)| vs.len() > 1) {
                for v in vs {
                    set.insert(*v);
                }
            }
            set.into_iter().collect()
        } else {
            Vec::new()
        };

        if !duplicate_seqs.is_empty() {
            report.aggregates_with_duplicates += 1;
            report.total_duplicate_occurrences = report.total_duplicate_occurrences.saturating_add(
                duplicate_seqs.iter().map(|(_, c)| c.saturating_sub(1)).sum(),
            );
        }

        report.entries.push(DeepAuditEntry {
            aggregate_key_str: format!("{}", ack.aggregate_key),
            client_id: ack.client_id,
            max_acked: ack.max_acked_client_seq,
            present_count: present_seqs.len() as u64,
            missing_seqs,
            duplicate_seqs,
            duplicate_aggregate_versions,
            total_batches,
        });
    }

    report
}
