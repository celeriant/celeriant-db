//! SIGKILL under sustained concurrent load: the crash-recovery contract, pinned.
//!
//! Many clients fan out over many aggregates writing incompressible payloads with
//! `enforce_client_idempotency: true`, pushing enough volume through the single
//! 2 MiB-preallocate shard log that it rotates several times (constant payloads
//! compress to nothing and never rotate — see `idempotency_across_seal`). At a
//! random acked-volume point past at least one rotation the server is SIGKILLed
//! mid-load — no warning, writes in flight — then restarted on the SAME data root.
//!
//! The contract verified after every restart, for every aggregate:
//!   - every ACKED (client, seq) is present exactly once with byte-exact content,
//!   - per client the present seqs are a contiguous prefix 1..=k with
//!     acked_high <= k <= attempted_high: the single in-flight write at the kill
//!     may land or vanish (crash ambiguity) but is never duplicated or torn,
//!   - aggregate versions are strictly monotonic across the full read,
//!   - retrying the last acked seq is rejected as a duplicate; the next fresh seq
//!     is accepted; a brand-new client is accepted.
//!
//! The kill/restart cycle runs three times in one process lifetime: whatever a
//! recovery makes readable is promoted to must-survive for the next crash, so
//! recovery-of-recovered-state is pinned too. Payloads are recomputed from
//! (aggregate, client, seq) at verify time, never stored.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

use crate::common::{port_for, read_all};
use crate::{ServerConfig, TestServer};

const N_AGGS: usize = 6;
const CLIENTS_PER_AGG: usize = 4;
const PAYLOAD_BYTES: usize = 16 * 1024;
const PREALLOCATE: u64 = 2 * 1024 * 1024;
/// Acked payload volume that makes the kill eligible. Random bytes don't
/// compress, so 3x the preallocate guarantees the log rotated at least once
/// before the trigger; a random extra up to one more preallocate varies the
/// kill point run to run.
const KILL_FLOOR_BYTES: u64 = 3 * PREALLOCATE;
const ROUNDS: usize = 3;
const TYPE: u64 = 7;

fn agg_key(a: usize) -> AggregateKey {
    AggregateKey::new(9_000 + a as u128, 1, 1)
}

fn creator_id(a: usize) -> u128 {
    500 + a as u128
}

fn load_client_id(a: usize, c: usize) -> u128 {
    1_000 + (a * 100 + c) as u128
}

fn round_new_client_id(round: usize) -> u128 {
    900_000 + round as u128
}

/// Incompressible payload derived only from identity: splitmix64 keyed by
/// (aggregate, client, seq). Verification recomputes it, so a torn or
/// cross-wired block can't match.
fn payload(a: usize, client: u128, seq: u64) -> Vec<u8> {
    let mut x = (a as u64).wrapping_mul(0x9E3779B97F4A7C15)
        ^ (client as u64).wrapping_mul(0xBF58476D1CE4E5B9)
        ^ ((client >> 64) as u64)
        ^ seq.wrapping_mul(0x94D049BB133111EB);
    let mut out = Vec::with_capacity(PAYLOAD_BYTES);
    while out.len() < PAYLOAD_BYTES {
        x = x.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        out.extend_from_slice((z ^ (z >> 31)).to_le_bytes().as_slice());
    }
    out
}

fn load_event(a: usize, client: u128, seq: u64) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: seq,
        event_seq: 0,
        event_id: None,
        event_timestamp: seq,
        event_type_major: TYPE,
        event_type_minor: 0,
        event_value: Arc::new(payload(a, client, seq)),
        iv: None,
    }
}

/// What the driver knows about one client of one aggregate. `acked` seqs MUST
/// be readable after recovery; the window (acked, attempted] — at most one seq
/// wide — may land or vanish but never tear or duplicate.
#[derive(Clone, Copy)]
struct ClientState {
    acked: u64,
    attempted: u64,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Crash: kill -9 under sustained load, durability across {ROUNDS} recovery cycles ===\n");

    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        shard_log_preallocate_bytes: PREALLOCATE,
        ..Default::default()
    };
    let mut server =
        TestServer::start_with_config(port_for("crash_kill9_under_load_durability"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    // Track every client the test ever writes as: (agg, client_id) -> state.
    let mut state: HashMap<(usize, u128), ClientState> = HashMap::new();

    println!("Creating {N_AGGS} aggregates");
    for a in 0..N_AGGS {
        let opts = WriteEventsOptions {
            allow_create: true,
            enforce_client_idempotency: true,
            ..Default::default()
        };
        c.write_events_with(agg_key(a), vec![load_event(a, creator_id(a), 1)], creator_id(a), opts)
            .await?;
        state.insert((a, creator_id(a)), ClientState { acked: 1, attempted: 1 });
    }
    drop(c);

    for round in 1..=ROUNDS {
        println!("\n--- Round {round}: load, kill -9, restart, verify ---");

        let stop = Arc::new(AtomicBool::new(false));
        let acked_bytes = Arc::new(AtomicU64::new(0));
        let live_writers = Arc::new(AtomicUsize::new(N_AGGS * CLIENTS_PER_AGG));
        let addr = server.address().to_string();

        let mut tasks = Vec::new();
        for a in 0..N_AGGS {
            for cl in 0..CLIENTS_PER_AGG {
                let id = load_client_id(a, cl);
                let start_seq = state.get(&(a, id)).map_or(0, |s| s.acked) + 1;
                let addr = addr.clone();
                let stop = stop.clone();
                let acked_bytes = acked_bytes.clone();
                let live = live_writers.clone();
                tasks.push(tokio::spawn(async move {
                    let res = write_until_killed(&addr, a, id, start_seq, &stop, &acked_bytes).await;
                    live.fetch_sub(1, Ordering::Relaxed);
                    res
                }));
            }
        }

        // Kill at a random acked-volume point, always past at least one rotation.
        let kill_at = KILL_FLOOR_BYTES + rand::random::<u64>() % PREALLOCATE;
        println!("Load running; kill trigger at {} acked bytes", kill_at);
        let t0 = Instant::now();
        loop {
            let acked = acked_bytes.load(Ordering::Relaxed);
            if acked >= kill_at {
                break;
            }
            if live_writers.load(Ordering::Relaxed) == 0 {
                return Err(format!(
                    "all writers died at {acked} acked bytes, before the {kill_at}-byte kill trigger — server unhealthy under load"
                )
                .into());
            }
            if t0.elapsed() > Duration::from_secs(120) {
                return Err(format!(
                    "load never reached the kill trigger ({acked}/{kill_at} acked bytes in 120s)"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        println!("SIGKILL at {} acked bytes ({:?} of load)", acked_bytes.load(Ordering::Relaxed), t0.elapsed());
        server.stop(); // Child::kill == SIGKILL: no shutdown path runs
        stop.store(true, Ordering::Relaxed);

        for t in tasks {
            let (a, id, acked, attempted) = t.await?.map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            state.insert((a, id), ClientState { acked, attempted });
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("Restarting on the same data root");
        server.restart().await?;
        let mut c = CeleriantClient::connect(server.address()).await?;

        // Full readback: acked writes present exactly once, correct bytes, ordered.
        for a in 0..N_AGGS {
            let seen = verify_aggregate(&mut c, a, &state).await?;
            // Whatever recovery made readable is durable state now: it must
            // survive the NEXT crash too.
            for (id, high) in seen {
                state.insert((a, id), ClientState { acked: high, attempted: high });
            }
        }
        println!("Readback clean: acked prefix intact, no duplicates, no tears");

        // Post-restart idempotency floors: retry of last acked seq rejected,
        // fresh seq accepted, brand-new client accepted.
        let opts = WriteEventsOptions {
            allow_create: false,
            enforce_client_idempotency: true,
            ..Default::default()
        };
        for a in 0..N_AGGS {
            for cl in 0..CLIENTS_PER_AGG {
                let id = load_client_id(a, cl);
                let st = state[&(a, id)];
                if st.acked >= 1 {
                    expect_duplicate(&mut c, a, id, st.acked, round).await?;
                }
                let fresh = st.acked + 1;
                c.write_events_with(agg_key(a), vec![load_event(a, id, fresh)], id, opts.clone())
                    .await
                    .map_err(|e| format!("round {round}: fresh seq {fresh} for client {id} rejected after restart: {e:?}"))?;
                state.insert((a, id), ClientState { acked: fresh, attempted: fresh });
            }
        }
        let new_id = round_new_client_id(round);
        let a = (round - 1) % N_AGGS;
        c.write_events_with(agg_key(a), vec![load_event(a, new_id, 1)], new_id, opts)
            .await
            .map_err(|e| format!("round {round}: brand-new client {new_id} rejected after restart: {e:?}"))?;
        state.insert((a, new_id), ClientState { acked: 1, attempted: 1 });
        println!("Idempotency floors correct: retries rejected, fresh and new-client writes accepted");
    }

    // Closing audit with everything quiesced: the model and the store agree.
    let mut c = CeleriantClient::connect(server.address()).await?;
    for a in 0..N_AGGS {
        let seen = verify_aggregate(&mut c, a, &state).await?;
        for ((agg, id), st) in state.iter().filter(|((agg, _), _)| *agg == a) {
            let got = seen.get(id).copied().unwrap_or(0);
            if got != st.acked {
                return Err(format!(
                    "final audit: aggregate {agg} client {id} has seqs to {got}, expected exactly {}",
                    st.acked
                )
                .into());
            }
        }
    }

    println!("\n=== PASS: every acked write survived {ROUNDS} kill -9 cycles; ambiguity window never duplicated or torn ===");
    Ok(())
}

/// One load client: sequential seqs, own connection, every ack recorded. On any
/// error the in-flight seq's outcome is unknown (the crash ambiguity window) and
/// the writer stops. Returns (agg, client, acked_high, attempted_high).
async fn write_until_killed(
    addr: &str,
    a: usize,
    id: u128,
    start_seq: u64,
    stop: &AtomicBool,
    acked_bytes: &AtomicU64,
) -> Result<(usize, u128, u64, u64), String> {
    let mut c = CeleriantClient::connect(addr)
        .await
        .map_err(|e| format!("writer ({a},{id}) connect: {e}"))?;
    let opts = WriteEventsOptions {
        allow_create: false,
        enforce_client_idempotency: true,
        ..Default::default()
    };
    let mut acked = start_seq - 1;
    let mut seq = start_seq;
    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok((a, id, acked, acked));
        }
        match c.write_events_with(agg_key(a), vec![load_event(a, id, seq)], id, opts.clone()).await {
            Ok(_) => {
                acked = seq;
                acked_bytes.fetch_add(PAYLOAD_BYTES as u64, Ordering::Relaxed);
                seq += 1;
            }
            // The kill races the in-flight request: outcome unknown, never retried
            // by the driver — verification bounds it instead.
            Err(_) => return Ok((a, id, acked, seq)),
        }
    }
}

/// Read one aggregate end to end and check every crash-contract clause that a
/// read can check. Returns each client's highest present seq.
async fn verify_aggregate(
    c: &mut CeleriantClient,
    a: usize,
    state: &HashMap<(usize, u128), ClientState>,
) -> Result<HashMap<u128, u64>, Box<dyn std::error::Error>> {
    let batches = read_all(c, &agg_key(a)).await?;
    let mut last_version = 0u64;
    let mut seen: HashMap<u128, u64> = HashMap::new();
    for b in &batches {
        if b.aggregate_version <= last_version {
            return Err(format!(
                "aggregate {a}: version {} after {} — ordering broken",
                b.aggregate_version, last_version
            )
            .into());
        }
        last_version = b.aggregate_version;
        if b.events.len() != 1 {
            return Err(format!("aggregate {a}: batch v{} has {} events, wrote 1", b.aggregate_version, b.events.len()).into());
        }
        let e = &b.events[0];
        let prev = seen.entry(b.client_id).or_insert(0);
        if e.client_seq != *prev + 1 {
            return Err(format!(
                "aggregate {a} client {}: seq {} follows {} — duplicate, gap, or reorder",
                b.client_id, e.client_seq, prev
            )
            .into());
        }
        *prev = e.client_seq;
        if *e.event_value != payload(a, b.client_id, e.client_seq) {
            return Err(format!(
                "aggregate {a} client {} seq {}: payload bytes wrong — torn or cross-wired write",
                b.client_id, e.client_seq
            )
            .into());
        }
    }
    for ((_, id), st) in state.iter().filter(|((agg, _), _)| *agg == a) {
        let got = seen.get(id).copied().unwrap_or(0);
        if got < st.acked || got > st.attempted {
            return Err(format!(
                "aggregate {a} client {id}: present through seq {got}, but acked {} / attempted {} — {}",
                st.acked,
                st.attempted,
                if got < st.acked { "ACKED WRITE LOST" } else { "phantom write beyond anything attempted" }
            )
            .into());
        }
    }
    for id in seen.keys() {
        if !state.contains_key(&(a, *id)) {
            return Err(format!("aggregate {a}: events from unknown client {id}").into());
        }
    }
    Ok(seen)
}

async fn expect_duplicate(
    c: &mut CeleriantClient,
    a: usize,
    id: u128,
    seq: u64,
    round: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let opts = WriteEventsOptions {
        allow_create: false,
        enforce_client_idempotency: true,
        ..Default::default()
    };
    let res = c.write_events_with(agg_key(a), vec![load_event(a, id, seq)], id, opts).await;
    match res {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::ClientIdempotencyViolation { last_client_seq, attempted_client_seq },
            ..
        }))
        | Err(ClientError::Server(ServerError::Write {
            kind: WriteError::InflightDuplicateWrite { last_client_seq, attempted_client_seq },
            ..
        })) => {
            if attempted_client_seq != Some(seq) || last_client_seq < Some(seq) {
                return Err(format!(
                    "round {round}: client {id} retry of seq {seq} rejected with wrong floor: last={last_client_seq:?} attempted={attempted_client_seq:?}"
                )
                .into());
            }
            Ok(())
        }
        Ok(_) => Err(format!(
            "round {round}: client {id} retry of acked seq {seq} was ACCEPTED after crash recovery — exactly-once violation"
        )
        .into()),
        Err(other) => Err(format!(
            "round {round}: client {id} retry of seq {seq}: expected duplicate rejection, got {other:?}"
        )
        .into()),
    }
}
