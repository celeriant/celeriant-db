//! Chaos Delete Testing
//!
//! Concurrent write/delete/read testing with verification.
//! Creates a temporary data directory and spawns the server automatically.
//!
//! Run with: cargo run --bin chaos_delete_main

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::TestServer;
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::list_operations::{
    ListAggregateTypesIterator, ListAggregatesIterator, ListOptions, ListOrgsIterator,
};
use celeriant_msg::request::requests::SingleAggregateDelete;
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::{
        read_filters::ReadFilters,
        requests::{DeleteRequest, ReadRequest, SingleAggregateWrite, WriteRequest},
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Barrier, Mutex};
use tokio::time::Instant;

// Distribution configuration
const NUM_ORGS: usize = 5;
const NUM_AGGREGATE_TYPES_PER_ORG: usize = 10;
const NUM_AGGREGATES_PER_TYPE: usize = 100;
const TOTAL_AGGREGATES: usize = NUM_ORGS * NUM_AGGREGATE_TYPES_PER_ORG * NUM_AGGREGATES_PER_TYPE;

// Test configuration
const NUM_WRITERS: usize = 10;
const NUM_READERS: usize = 5;
const TEST_DURATION_SECS: u64 = 30;
const CLIENTSIDE_TIMEOUT_S: u64 = 30;

// Payload configuration
const MIN_PAYLOAD_SIZE: usize = 1;
const MAX_PAYLOAD_SIZE: usize = 1024 * 1024; // 1MB

// Delete probability (1 in N writes triggers a delete)
const DELETE_PROBABILITY: u32 = 20;

/// Converts a flat aggregate index to (org_id, aggregate_type_id, aggregate_id)
fn index_to_aggregate_key(index: usize) -> AggregateKey {
    let org_id = (index / (NUM_AGGREGATE_TYPES_PER_ORG * NUM_AGGREGATES_PER_TYPE)) + 1;
    let type_index = (index / NUM_AGGREGATES_PER_TYPE) % NUM_AGGREGATE_TYPES_PER_ORG;
    let aggregate_type_id = type_index + 1;
    let aggregate_id = (index % NUM_AGGREGATES_PER_TYPE) + 1;

    AggregateKey::new(
        org_id as u128,
        aggregate_type_id as u128,
        aggregate_id as u128,
    )
}

/// Tracking state for each aggregate
#[derive(Debug, Default)]
struct AggregateTrackingState {
    /// Number of successful writes
    write_count: AtomicU64,
    /// Number of successful deletes
    delete_count: AtomicU64,
    /// Whether this aggregate currently exists (not deleted)
    exists: AtomicBool,
    /// Last aggregate version written
    #[allow(dead_code)]
    last_aggregate_version: AtomicU64,
}

/// Global tracking state shared across all workers
struct GlobalTrackingState {
    /// Per-aggregate state indexed by flat index
    aggregates: Vec<AggregateTrackingState>,
    /// Set of org_ids that have been written to
    written_orgs: Mutex<HashSet<u128>>,
    /// Set of (org_id, aggregate_type_id) that have been written to
    written_types: Mutex<HashSet<(u128, u128)>>,
    /// Global counters
    total_writes: AtomicU64,
    total_deletes: AtomicU64,
    total_reads: AtomicU64,
    write_errors: AtomicU64,
    delete_errors: AtomicU64,
    read_errors: AtomicU64,
    total_bytes_written: AtomicU64,
}

impl GlobalTrackingState {
    fn new() -> Self {
        let aggregates = (0..TOTAL_AGGREGATES)
            .map(|_| AggregateTrackingState::default())
            .collect();

        Self {
            aggregates,
            written_orgs: Mutex::new(HashSet::new()),
            written_types: Mutex::new(HashSet::new()),
            total_writes: AtomicU64::new(0),
            total_deletes: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            delete_errors: AtomicU64::new(0),
            read_errors: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
        }
    }

    async fn record_write(&self, index: usize, key: &AggregateKey, bytes: u64) {
        if index < self.aggregates.len() {
            self.aggregates[index]
                .write_count
                .fetch_add(1, Ordering::Relaxed);
            self.aggregates[index].exists.store(true, Ordering::Release);
        }
        self.total_writes.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_written.fetch_add(bytes, Ordering::Relaxed);

        // Track orgs and types
        {
            let mut orgs = self.written_orgs.lock().await;
            orgs.insert(key.org_id);
        }
        {
            let mut types = self.written_types.lock().await;
            types.insert((key.org_id, key.aggregate_type_id));
        }
    }

    fn record_delete(&self, index: usize) {
        if index < self.aggregates.len() {
            self.aggregates[index]
                .delete_count
                .fetch_add(1, Ordering::Relaxed);
            self.aggregates[index].exists.store(false, Ordering::Release);
        }
        self.total_deletes.fetch_add(1, Ordering::Relaxed);
    }

    fn get_expected_aggregates(&self) -> (HashSet<AggregateKey>, HashSet<AggregateKey>) {
        let mut existing = HashSet::new();
        let mut deleted = HashSet::new();

        for i in 0..TOTAL_AGGREGATES {
            let write_count = self.aggregates[i].write_count.load(Ordering::Relaxed);
            if write_count > 0 {
                let key = index_to_aggregate_key(i);
                if self.aggregates[i].exists.load(Ordering::Acquire) {
                    existing.insert(key);
                } else {
                    deleted.insert(key);
                }
            }
        }

        (existing, deleted)
    }
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Chaos Delete Testing Mode ===\n");

    println!("Starting test server...");
    let server = TestServer::start().await?;
    let server_addr = server.address();
    println!("Server started at {}\n", server_addr);

    println!(
        "Orgs: {}, Types/Org: {}, Aggregates/Type: {} (Total: {})",
        NUM_ORGS, NUM_AGGREGATE_TYPES_PER_ORG, NUM_AGGREGATES_PER_TYPE, TOTAL_AGGREGATES
    );
    let num_writers = crate::load_scale(NUM_WRITERS);
    let num_readers = crate::load_scale(NUM_READERS);
    println!(
        "Writers: {}, Readers: {}, Duration: {}s",
        num_writers, num_readers, TEST_DURATION_SECS
    );
    println!("Delete probability: 1 in {} writes\n", DELETE_PROBABILITY);

    let connect_start = Instant::now();
    let state = Arc::new(GlobalTrackingState::new());

    // Establish connections
    let total_connections = num_writers + num_readers;
    println!("Establishing {} connections...", total_connections);

    let mut writer_clients = Vec::with_capacity(num_writers);
    let mut reader_clients = Vec::with_capacity(num_readers);

    for i in 0..num_writers {
        let client = CeleriantClient::connect_with_timeout(
            server_addr,
            Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
            None,
        )
        .await
        .map_err(|e| format!("Writer {} connection error: {}", i, e))?
        .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));
        writer_clients.push((i, client));
    }

    for i in 0..num_readers {
        let client = CeleriantClient::connect_with_timeout(
            server_addr,
            Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
            None,
        )
        .await
        .map_err(|e| format!("Reader {} connection error: {}", i, e))?
        .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S));
        reader_clients.push((i, client));
    }

    let connect_duration = connect_start.elapsed();
    println!(
        "Established {} connections in {:.2}s\n",
        total_connections,
        connect_duration.as_secs_f64()
    );

    // Synchronization barrier
    let barrier = Arc::new(Barrier::new(total_connections));

    println!("Starting chaos test...\n");
    let start_time = Instant::now();

    // Spawn writer tasks
    let mut tasks = Vec::with_capacity(total_connections);

    for (worker_id, client) in writer_clients {
        let barrier = Arc::clone(&barrier);
        let state = Arc::clone(&state);
        let task =
            tokio::spawn(async move { run_writer_task(worker_id, client, barrier, state).await });
        tasks.push(("writer", worker_id, task));
    }

    // Spawn reader tasks
    for (worker_id, client) in reader_clients {
        let barrier = Arc::clone(&barrier);
        let state = Arc::clone(&state);
        let task =
            tokio::spawn(async move { run_reader_task(worker_id, client, barrier, state).await });
        tasks.push(("reader", worker_id, task));
    }

    // Wait for all tasks
    for (task_type, worker_id, task) in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("{} {} error: {}", task_type, worker_id, e),
            Err(e) => eprintln!("{} {} join error: {}", task_type, worker_id, e),
        }
    }

    let test_duration = start_time.elapsed();

    // Print test results
    println!("\n=== Chaos Test Results ===");
    println!("Test Duration: {:.2}s\n", test_duration.as_secs_f64());

    let total_writes = state.total_writes.load(Ordering::Relaxed);
    let total_deletes = state.total_deletes.load(Ordering::Relaxed);
    let total_reads = state.total_reads.load(Ordering::Relaxed);
    let write_errors = state.write_errors.load(Ordering::Relaxed);
    let delete_errors = state.delete_errors.load(Ordering::Relaxed);
    let read_errors = state.read_errors.load(Ordering::Relaxed);
    let total_bytes = state.total_bytes_written.load(Ordering::Relaxed);

    println!("{:<15} {:>12} {:>12}", "Operation", "Success", "Errors");
    println!("{}", "-".repeat(41));
    println!("{:<15} {:>12} {:>12}", "Writes", total_writes, write_errors);
    println!(
        "{:<15} {:>12} {:>12}",
        "Deletes", total_deletes, delete_errors
    );
    println!("{:<15} {:>12} {:>12}", "Reads", total_reads, read_errors);
    println!("{}", "-".repeat(41));
    println!("Total bytes written: {}\n", format_bytes(total_bytes));

    println!("=== Throughput ===");
    println!(
        "Write throughput: {:.2} req/s",
        total_writes as f64 / test_duration.as_secs_f64()
    );
    println!(
        "Delete throughput: {:.2} req/s",
        total_deletes as f64 / test_duration.as_secs_f64()
    );
    println!(
        "Read throughput: {:.2} req/s",
        total_reads as f64 / test_duration.as_secs_f64()
    );

    // Verification phase
    println!("\n=== Verification Phase ===");
    let verify_start = Instant::now();

    let mut verify_client = CeleriantClient::connect_with_timeout(
        server_addr,
        Some(Duration::from_secs(CLIENTSIDE_TIMEOUT_S)),
        None,
    )
    .await
    .map_err(|e| format!("Verification connection error: {}", e))?
    .with_timeout(Duration::from_secs(CLIENTSIDE_TIMEOUT_S * 2));

    // Get expected state
    let expected_orgs: HashSet<u128> = state.written_orgs.lock().await.clone();
    let expected_types: HashSet<(u128, u128)> = state.written_types.lock().await.clone();
    let (expected_existing, expected_deleted) = state.get_expected_aggregates();

    println!(
        "Expected: {} orgs, {} types, {} existing aggregates, {} deleted aggregates",
        expected_orgs.len(),
        expected_types.len(),
        expected_existing.len(),
        expected_deleted.len()
    );

    // Verify orgs
    println!("\nVerifying organizations...");
    let list_options = ListOptions::default();
    let org_iter = ListOrgsIterator::new(&mut verify_client, list_options);
    let listed_orgs: Vec<_> = org_iter.collect().await?;
    let listed_org_ids: HashSet<u128> = listed_orgs.iter().map(|o| o.org_id).collect();

    let missing_orgs: Vec<_> = expected_orgs.difference(&listed_org_ids).collect();
    let extra_orgs: Vec<_> = listed_org_ids.difference(&expected_orgs).collect();

    if missing_orgs.is_empty() && extra_orgs.is_empty() {
        println!("  Organizations verified: {} found", listed_org_ids.len());
    } else {
        println!("  Organization mismatch!");
        if !missing_orgs.is_empty() {
            println!("    Missing: {:?}", missing_orgs);
        }
        if !extra_orgs.is_empty() {
            println!(
                "    Extra (may be from other tests): {} found",
                extra_orgs.len()
            );
        }
    }

    // Verify aggregate types
    println!("\nVerifying aggregate types...");
    let list_options = ListOptions::default();
    let type_iter = ListAggregateTypesIterator::new(&mut verify_client, None, list_options);
    let listed_types: Vec<_> = type_iter.collect().await?;
    let listed_type_keys: HashSet<(u128, u128)> = listed_types
        .iter()
        .map(|t| (t.org_id, t.aggregate_type_id))
        .collect();

    let missing_types: Vec<_> = expected_types.difference(&listed_type_keys).collect();
    let extra_types: Vec<_> = listed_type_keys.difference(&expected_types).collect();

    if missing_types.is_empty() && extra_types.is_empty() {
        println!(
            "  Aggregate types verified: {} found",
            listed_type_keys.len()
        );
    } else {
        println!("  Aggregate type mismatch!");
        if !missing_types.is_empty() {
            println!("    Missing: {:?}", missing_types);
        }
        if !extra_types.is_empty() {
            println!(
                "    Extra (may be from other tests): {} found",
                extra_types.len()
            );
        }
    }

    // Verify aggregates (including deleted)
    println!("\nVerifying aggregates (including deleted)...");
    let list_options = ListOptions {
        include_deleted: true,
        ..Default::default()
    };
    let agg_iter = ListAggregatesIterator::new(&mut verify_client, None, None, list_options);
    let listed_aggregates: Vec<_> = agg_iter.collect().await?;

    // Control listing. `max_shard_hint` suppresses the iterator's shard-discovery probe,
    // which is the only path that can end the walk early. If this returns more rows than
    // the default listing, the shortfall is the client iterator truncating rather than the
    // database losing aggregates.
    let max_shard = server.config().num_shards.unwrap_or(1).saturating_sub(1) as u64;
    let hinted_iter = ListAggregatesIterator::new(
        &mut verify_client,
        None,
        None,
        ListOptions {
            include_deleted: true,
            max_shard_hint: Some(max_shard),
            ..Default::default()
        },
    );
    let hinted_aggregates: Vec<_> = hinted_iter.collect().await?;
    println!(
        "  Listed {} aggregates via shard discovery, {} with max_shard_hint={}",
        listed_aggregates.len(),
        hinted_aggregates.len(),
        max_shard
    );

    let (listed_existing, listed_deleted) = split_by_deleted(&listed_aggregates);
    let (hinted_existing, hinted_deleted) = split_by_deleted(&hinted_aggregates);
    println!(
        "  Against expectations — discovery: {} existing / {} deleted missing; hinted: {} existing / {} deleted missing",
        expected_existing.difference(&listed_existing).count(),
        expected_deleted.difference(&listed_deleted).count(),
        expected_existing.difference(&hinted_existing).count(),
        expected_deleted.difference(&hinted_deleted).count()
    );

    // Check existing aggregates
    let missing_existing: Vec<_> = expected_existing.difference(&listed_existing).collect();
    let wrongly_deleted: Vec<_> = expected_existing.intersection(&listed_deleted).collect();

    // Check deleted aggregates
    let missing_deleted: Vec<_> = expected_deleted.difference(&listed_deleted).collect();
    let wrongly_existing: Vec<_> = expected_deleted.intersection(&listed_existing).collect();

    let mut aggregate_errors = false;

    if missing_existing.is_empty() && wrongly_deleted.is_empty() {
        println!(
            "  Existing aggregates verified: {} found",
            expected_existing.len()
        );
    } else {
        aggregate_errors = true;
        println!("  Existing aggregate mismatch!");
        if !missing_existing.is_empty() {
            println!("    Missing existing: {} aggregates", missing_existing.len());
            dump_missing_keys("missing_existing", &missing_existing);
        }
        if !wrongly_deleted.is_empty() {
            println!(
                "    Wrongly marked deleted: {} aggregates",
                wrongly_deleted.len()
            );
        }
    }

    if missing_deleted.is_empty() && wrongly_existing.is_empty() {
        println!(
            "  Deleted aggregates verified: {} found",
            expected_deleted.len()
        );
    } else {
        aggregate_errors = true;
        println!("  Deleted aggregate mismatch!");
        if !missing_deleted.is_empty() {
            println!("    Missing deleted: {} aggregates", missing_deleted.len());
        }
        if !wrongly_existing.is_empty() {
            println!(
                "    Wrongly marked existing: {} aggregates",
                wrongly_existing.len()
            );
        }
    }

    let verify_duration = verify_start.elapsed();
    println!(
        "\nVerification completed in {:.2}s",
        verify_duration.as_secs_f64()
    );

    // Final summary
    println!("\n=== Final Summary ===");
    let has_errors = write_errors > 0
        || delete_errors > 0
        || read_errors > 0
        || !missing_orgs.is_empty()
        || !missing_types.is_empty()
        || aggregate_errors;

    if has_errors {
        println!("  ISSUES DETECTED - Review output above");
        return Err(format!(
            "chaos_delete verification failed: write_errors={} delete_errors={} read_errors={} \
             missing_orgs={} missing_types={} aggregate_errors={}",
            write_errors,
            delete_errors,
            read_errors,
            missing_orgs.len(),
            missing_types.len(),
            aggregate_errors
        )
        .into());
    }
    println!("  All verifications passed!");

    Ok(())
}

/// Split a listing into (existing, deleted) key sets.
fn split_by_deleted(
    listed: &[celeriant_client_tokio::list_operations::AggregateStats],
) -> (HashSet<AggregateKey>, HashSet<AggregateKey>) {
    let mut existing = HashSet::new();
    let mut deleted = HashSet::new();
    for agg in listed {
        let key = AggregateKey::new(agg.org_id, agg.aggregate_type_id, agg.aggregate_id);
        if agg.is_deleted {
            deleted.insert(key);
        } else {
            existing.insert(key);
        }
    }
    (existing, deleted)
}

/// Write the failing keys to `CELERIANT_TEST_MISSING_KEYS_DIR` so they can be cross-checked
/// against disk with celeriant-wal-inspect. Silent unless the directory is set.
fn dump_missing_keys(label: &str, keys: &[&AggregateKey]) {
    let Some(dir) = std::env::var_os("CELERIANT_TEST_MISSING_KEYS_DIR") else {
        return;
    };
    let path = std::path::Path::new(&dir).join(format!("{}.txt", label));
    let body: String = keys
        .iter()
        .map(|k| format!("{} {} {}\n", k.org_id, k.aggregate_type_id, k.aggregate_id))
        .collect();
    match std::fs::write(&path, body) {
        Ok(()) => println!("    Wrote {} keys to {:?}", keys.len(), path),
        Err(e) => println!("    Could not write {:?}: {}", path, e),
    }
}

async fn run_writer_task(
    worker_id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
    state: Arc<GlobalTrackingState>,
) -> Result<(), String> {
    let mut rng = StdRng::from_entropy();
    let mut event_seq = 0u64;

    barrier.wait().await;

    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        // Pick a random aggregate
        let aggregate_version = rng.gen_range(0..TOTAL_AGGREGATES);
        let aggregate_key = index_to_aggregate_key(aggregate_version);

        // Decide whether to delete or write
        let should_delete = rng.gen_ratio(1, DELETE_PROBABILITY);

        if should_delete {
            let mut deletes = HashMap::new();
            deletes.insert(
                aggregate_key.clone(),
                SingleAggregateDelete {
                    allow_recreate: true,
                    allow_sequence_continuation: false,
                    expected_version: None,
                },
            );
            // Delete request (allow_recreate = true so future writes work)
            let request = ClientRequest::Delete(DeleteRequest {
                correlation_id: None,
                deletes,
                client_id: worker_id as u128,
                user_id: None,
            });

            match client
                .send_request(&request)
                .await
            {
                Ok(_) => {
                    state.record_delete(aggregate_version);
                }
                Err(ClientError::Server(ref err)) => {
                    use celeriant_client_tokio::server_error::{DeleteError, ReadError, ServerError};
                    // AggregateNotExists is expected for aggregates never written or already deleted.
                    let is_expected = matches!(err,
                        ServerError::Read { kind: ReadError::AggregateNotExists, .. } |
                        ServerError::Delete { kind: DeleteError::AggregateNotExists, .. }
                    );
                    if !is_expected {
                        eprintln!("[Writer {}] Delete error: {}", worker_id, err);
                        state.delete_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    eprintln!("[Writer {}] Delete error: {}", worker_id, e);
                    state.delete_errors.fetch_add(1, Ordering::Relaxed);
                    if !matches!(e, ClientError::RequestTimeout) {
                        break;
                    }
                }
            }
        } else {
            // Write request
            let payload_size = generate_random_payload_size(&mut rng);
            let payload = generate_random_payload(&mut rng, payload_size);

            let event = DatablockAggregateEvent {
                client_seq: event_seq,
                event_seq: 0,
                event_id: Some(rng.r#gen()),
                event_timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                event_type_major: rng.gen_range(1..=10),
                event_type_minor: rng.gen_range(1..=100),
                event_value: Arc::new(payload),
                iv: None,
            };

            let mut writes = HashMap::new();
            writes.insert(
                aggregate_key.clone(),
                SingleAggregateWrite {
                    events: vec![event],
                    allow_create: true,
                    expected_version: None,
                    enforce_client_idempotency: false,
                },
            );

            let request = ClientRequest::Write(WriteRequest {
                correlation_id: None,
                client_id: worker_id as u128,
                user_id: None,
                writes,
            });

            match client
                .send_request(&request)
                .await
            {
                Ok(_) => {
                    state
                        .record_write(aggregate_version, &aggregate_key, payload_size as u64)
                        .await;
                    event_seq += 1;
                }
                Err(ClientError::Server(err)) => {
                    eprintln!("[Writer {}] Write error: {}", worker_id, err);
                    state.write_errors.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("[Writer {}] Write error: {}", worker_id, e);
                    state.write_errors.fetch_add(1, Ordering::Relaxed);
                    if !matches!(e, ClientError::RequestTimeout) {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn run_reader_task(
    worker_id: usize,
    mut client: CeleriantClient,
    barrier: Arc<Barrier>,
    state: Arc<GlobalTrackingState>,
) -> Result<(), String> {
    let mut rng = StdRng::from_entropy();

    barrier.wait().await;

    // Small delay to let writers get ahead
    tokio::time::sleep(Duration::from_millis(100)).await;

    let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);

    while Instant::now() < deadline {
        // Pick a random aggregate to read
        let aggregate_version = rng.gen_range(0..TOTAL_AGGREGATES);
        let aggregate_key = index_to_aggregate_key(aggregate_version);

        let request = ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key,
            filters: ReadFilters::new(1),
        });

        match client
            .send_request(&request)
            .await
        {
            Ok(_) => {
                state.total_reads.fetch_add(1, Ordering::Relaxed);
            }
            Err(ClientError::Server(ref err)) => {
                use celeriant_client_tokio::server_error::{DeleteError, ReadError, ServerError};
                // AggregateNotExists is expected for aggregates not yet written or already deleted.
                let is_expected = matches!(err,
                    ServerError::Read { kind: ReadError::AggregateNotExists, .. } |
                    ServerError::Delete { kind: DeleteError::AggregateNotExists, .. }
                );
                if !is_expected {
                    state.read_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                eprintln!("[Reader {}] Read error: {}", worker_id, e);
                state.read_errors.fetch_add(1, Ordering::Relaxed);
                if !matches!(e, ClientError::RequestTimeout) {
                    break;
                }
            }
        }

        // Small delay between reads
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    Ok(())
}

fn generate_random_payload_size(rng: &mut impl Rng) -> usize {
    let log_min = (MIN_PAYLOAD_SIZE as f64).ln();
    let log_max = (MAX_PAYLOAD_SIZE as f64).ln();
    let log_size = rng.gen_range(log_min..=log_max);
    log_size.exp() as usize
}

fn generate_random_payload(rng: &mut impl Rng, size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; size];
    rng.fill(&mut payload[..]);
    payload
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
