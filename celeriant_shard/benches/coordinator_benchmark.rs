//! Coordinator fsync-amortisation benchmark.
//!
//! The amortisation coordinator's job is to coalesce concurrent writers into
//! shared fsyncs. Its risk is the opposite: under load it can de-amortise —
//! fire many small syncs instead of batching — which raises per-writer latency.
//!
//! The existing write benches model bursty arrival (waves with idle gaps) and
//! pure-sequential idle, both regimes where a fast path helps. They are blind to
//! SUSTAINED saturation, where an opportunistic fast path fragments batches. This
//! bench fills that gap: K writers each issue R requests back-to-back (no gap), so
//! the coordinator is never idle. We drive the coordinator directly with a mock
//! sync_fn that simulates fsync cost and counts invocations, so the result is the
//! coordinator's batching behaviour in isolation, not shard or disk noise.
//!
//! Two signals per config:
//! - criterion wall-time per iteration (lower = better amortisation).
//! - amortisation ratio = requests / syncs, printed to stderr (higher = better).
//!
//! A de-amortisation regression shows as more syncs (lower ratio) and higher
//! wall-time at moderate-to-high K.

use std::cell::Cell;
use std::hint::black_box;
use std::rc::Rc;
use std::time::{Duration, Instant};

use celeriant_shard::amortisation::coordinator::{CaptureResult, Coordinator};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use glommio::timer::sleep;
use glommio::{LocalExecutorBuilder, Placement};

criterion_group!(benches, bench_two_phase_sustained, bench_two_phase_delay_sweep);
criterion_main!(benches);

/// Amortisation delay the leader sleeps to let writers accumulate. Mirrors a
/// realistic `fsync_delay_us`.
const DELAY: Duration = Duration::from_micros(1000);

/// Simulated fsync cost. The leader holds the gate for this long; writers
/// arriving during it should batch into the next cycle, not start their own.
const FSYNC_SIM: Duration = Duration::from_micros(300);

/// Requests each writer issues back-to-back. Re-arrival with no gap is what
/// creates sustained pressure.
const REQUESTS_PER_WRITER: usize = 20;

/// Max per-request arrival jitter. Real clients are independent: their requests
/// land at staggered, uncorrelated times, not in lockstep. Without jitter, glommio
/// tasks resume in a fixed order and batch artificially cleanly, hiding the
/// fast-path fragmentation. A jitter on the order of the gate-hold desyncs them so
/// a writer can arrive in a momentarily-free-gate window — exactly when an
/// opportunistic fast path fires a tiny batch instead of coalescing.
const JITTER_MAX_US: u64 = 800;

/// Deterministic per-writer LCG (Knuth MMIX constants) so the jitter pattern is
/// reproducible across runs and across coordinator versions — the comparison must
/// not move because the randomness moved.
fn next_jitter_us(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (*state >> 33) % JITTER_MAX_US
}

/// Run `writers` tasks concurrently, each issuing `REQUESTS_PER_WRITER` two-phase
/// syncs through one coordinator, with independent per-request arrival jitter so
/// the load is sustained AND staggered. Returns the elapsed time for all requests
/// and the number of sync_fn invocations (the amortisation denominator).
fn run_sustained(writers: usize, delay: Duration) -> (Duration, u64) {
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let syncs = Rc::new(Cell::new(0u64));

            let start = Instant::now();
            let mut handles = Vec::with_capacity(writers);
            for w in 0..writers {
                let coord = coordinator.clone();
                let syncs = syncs.clone();
                handles.push(glommio::spawn_local(async move {
                    let mut rng = (w as u64).wrapping_add(1);
                    for _ in 0..REQUESTS_PER_WRITER {
                        let jitter = next_jitter_us(&mut rng);
                        if jitter > 0 {
                            sleep(Duration::from_micros(jitter)).await;
                        }
                        let syncs = syncs.clone();
                        let r = coord
                            .request_sync_two_phase(
                                Some(delay),
                                "gate-timeout".to_string(),
                                || CaptureResult::Captured(()),
                                move |_| async move {
                                    syncs.set(syncs.get() + 1);
                                    sleep(FSYNC_SIM).await;
                                    Ok(())
                                },
                            )
                            .await;
                        debug_assert!(r.is_ok());
                    }
                }));
            }
            for h in handles {
                h.await;
            }
            (start.elapsed(), syncs.get())
        })
        .unwrap()
        .join()
        .unwrap()
}

fn bench_two_phase_sustained(c: &mut Criterion) {
    let mut group = c.benchmark_group("coordinator_sustained_two_phase");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    for writers in [32usize, 128, 512] {
        let total_requests = writers * REQUESTS_PER_WRITER;
        group.bench_with_input(
            BenchmarkId::new("writers", writers),
            &writers,
            |b, &writers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    let mut last_syncs = 0u64;
                    for _ in 0..iters {
                        let (elapsed, syncs) = run_sustained(writers, DELAY);
                        total += elapsed;
                        last_syncs = syncs;
                        black_box(syncs);
                    }
                    let ratio = total_requests as f64 / last_syncs.max(1) as f64;
                    eprintln!(
                        "  [writers={writers}] requests={total_requests} syncs={last_syncs} \
                         amortisation_ratio={ratio:.1} reqs/sync (higher is better)"
                    );
                    total
                });
            },
        );
    }
    group.finish();
}

/// Hold writers fixed at a saturating level and sweep the amortisation delay.
/// A larger delay should batch more (higher ratio) — verifies the delay still
/// drives amortisation and that the fast path is not bypassing it under load.
fn bench_two_phase_delay_sweep(c: &mut Criterion) {
    const WRITERS: usize = 256;
    let mut group = c.benchmark_group("coordinator_delay_sweep");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    for delay_us in [0u64, 250, 1000, 2000] {
        let delay = Duration::from_micros(delay_us);
        let total_requests = WRITERS * REQUESTS_PER_WRITER;
        group.bench_with_input(
            BenchmarkId::new("delay_us", delay_us),
            &delay,
            |b, &delay| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    let mut last_syncs = 0u64;
                    for _ in 0..iters {
                        let (elapsed, syncs) = run_sustained(WRITERS, delay);
                        total += elapsed;
                        last_syncs = syncs;
                        black_box(syncs);
                    }
                    let ratio = total_requests as f64 / last_syncs.max(1) as f64;
                    eprintln!(
                        "  [delay={delay_us}us writers={WRITERS}] syncs={last_syncs} \
                         amortisation_ratio={ratio:.1} reqs/sync"
                    );
                    total
                });
            },
        );
    }
    group.finish();
}
