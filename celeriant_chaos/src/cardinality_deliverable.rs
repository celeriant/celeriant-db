//! The machine-readable `cardinality_pressure` deliverable.
//!
//! The scenario's answer used to exist only as prose in the summary markdown and
//! inside free-text check details, so nothing could re-derive a headline figure
//! from the run JSON, and two runs at different segment sizes could only be
//! compared by reading. This is the same deliverable as typed numbers, hung off
//! `ScenarioReport`.
//!
//! Every latency stays in microseconds and every unmeasured figure stays
//! `Option`. A bucket the run never reached must never be readable as
//! "measured 0ms" — that turns missing data into a flat curve, which is the
//! exact false negative the scenario exists to avoid.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use celeriant_bench::read_workload::{ReheatCostCurve, ReheatCurveJson, ReheatDeltaRow, reheat_delta_rows};

use crate::cardinality_workload::{BLOOM_DESIGN_POINT_AGGS_PER_SEGMENT, CardinalityParams};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CardinalityDeliverable {
    pub shape: RunShape,
    pub reached: Reached,
    pub segments_per_shard: Vec<ShardSegments>,
    pub aggs_per_segment: AggsPerSegment,
    pub peak_rss: PeakRss,
    pub fill_curve: ReheatCurveJson,
    /// Phase 3: warm, every cache populated.
    pub warm_curve: ReheatCurveJson,
    /// Phase 5: cold, after the restart. Its `error_kinds` is the phase-5
    /// tally — empty when the phase ran clean.
    pub cold_curve: ReheatCurveJson,
    pub cold_vs_warm: Vec<ReheatDeltaRow>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunShape {
    pub preset: String,
    pub fill_budget_secs: u64,
    pub fill_elapsed_secs: u64,
    pub tasks: usize,
    pub segment_bytes: u64,
    pub memory_percent: u64,
    pub target_aggs_per_segment: u64,
    /// Derived from segment size and the target density, never taken raw.
    pub large_event_fraction: f64,
}

impl RunShape {
    pub fn new(card: &CardinalityParams, tasks: usize, fill_elapsed: Duration) -> Self {
        Self {
            preset: card.preset.name().into(),
            fill_budget_secs: card.fill_budget().as_secs(),
            fill_elapsed_secs: fill_elapsed.as_secs(),
            tasks,
            segment_bytes: card.segment_bytes,
            memory_percent: card.memory_percent,
            target_aggs_per_segment: card.target_aggs_per_segment,
            large_event_fraction: card.large_event_fraction(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Reached {
    pub distinct_aggregates: u64,
    pub distinct_clients: u64,
    pub fill_writes_ok: u64,
    pub fill_write_errors: u64,
    pub reheat_probes_ok: u64,
    pub reheat_probes_failed: u64,
    pub ledger_entries: usize,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct ShardSegments {
    pub shard: u32,
    pub segments: u64,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct AggsPerSegment {
    /// The sizing formula run forward on its own inputs. A model: it cannot
    /// disagree with the request for any reason other than a clamp, so it is
    /// never a measurement and must never stand alone.
    pub model: u64,
    /// Distinct aggregates over the segments actually on disk, and a LOWER
    /// BOUND rather than a steady-state density: a fill that never rotated
    /// holds its whole population in one open segment, and a chain crossing a
    /// boundary enters every segment bloom it touches. `None` when no segment
    /// rotated or no count was collected — there is nothing to divide by.
    pub observed_lower_bound: Option<u64>,
    pub observed_total_segments: u64,
    pub design_point: u64,
}

impl AggsPerSegment {
    pub fn new(card: &CardinalityParams, distinct_aggregates: u64, per_shard: &[ShardSegments]) -> Self {
        let observed_total_segments: u64 = per_shard.iter().map(|s| s.segments).sum();
        Self {
            model: card.achieved_aggs_per_segment(),
            observed_lower_bound: (observed_total_segments > 0)
                .then(|| distinct_aggregates / observed_total_segments),
            observed_total_segments,
            design_point: BLOOM_DESIGN_POINT_AGGS_PER_SEGMENT,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeakRss {
    pub by_host_bytes: BTreeMap<String, u64>,
    pub peak_bytes: u64,
    /// `memory_percent` of the node's physical RAM. `None` when MemTotal could
    /// not be read: a ratio against a guessed denominator is worse than none.
    pub declared_budget_bytes: Option<u64>,
    pub node_ram_bytes: Option<u64>,
}

impl PeakRss {
    pub fn new(
        card: &CardinalityParams,
        peak_rss_kb: u64,
        by_host_kb: &BTreeMap<String, u64>,
        node_ram_bytes: Option<u64>,
    ) -> Self {
        Self {
            by_host_bytes: by_host_kb.iter().map(|(h, kb)| (h.clone(), kb * 1024)).collect(),
            peak_bytes: peak_rss_kb * 1024,
            declared_budget_bytes: node_ram_bytes.map(|ram| card.declared_budget_bytes(ram)),
            node_ram_bytes,
        }
    }
}

/// The four curve fields of the deliverable, read out of the live curves.
pub struct Curves {
    pub fill: ReheatCurveJson,
    pub warm: ReheatCurveJson,
    pub cold: ReheatCurveJson,
    pub cold_vs_warm: Vec<ReheatDeltaRow>,
}

impl Curves {
    /// One statement per lock. Building these four fields inside a single
    /// struct literal keeps every guard alive to the semicolon, so the delta
    /// row's `warm` lock met the field above it still held and the thread
    /// wedged forever: `std::sync::Mutex` is not reentrant.
    pub fn snapshot(fill: &Mutex<ReheatCostCurve>, warm: &Mutex<ReheatCostCurve>, cold: &Mutex<ReheatCostCurve>) -> Self {
        let cold_vs_warm = reheat_delta_rows(&locked(warm), &locked(cold));
        let fill = locked(fill).to_json();
        let warm = locked(warm).to_json();
        let cold = locked(cold).to_json();
        Self { fill, warm, cold, cold_vs_warm }
    }
}

fn locked<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn shard_segments(per_shard: &[(u32, u64)]) -> Vec<ShardSegments> {
    per_shard.iter().map(|&(shard, segments)| ShardSegments { shard, segments }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_bench::population::AgeBucket;
    use celeriant_bench::read_workload::{ReadErrorKind, ReheatCostCurve};

    fn params() -> CardinalityParams {
        CardinalityParams::default()
    }

    #[test]
    fn an_unreached_bucket_serialises_as_null_and_never_as_zero() {
        // The single most important property of the whole section: a bucket the
        // run never reached must not be readable as "measured 0us".
        let curve = ReheatCostCurve::new("fill", 1).to_json();
        let v: serde_json::Value = serde_json::to_value(&curve).unwrap();
        let rows = v["buckets"].as_array().unwrap();
        assert_eq!(rows.len(), AgeBucket::ALL.len());
        for row in rows {
            for key in ["p50_us", "p99_us", "max_us", "bytes_per_read"] {
                assert!(row[key].is_null(), "{key} is {} on an unreached bucket", row[key]);
                assert_ne!(row[key].as_u64(), Some(0));
            }
            assert_eq!(row["ops"].as_u64(), Some(0));
        }
        // And back the other way: null must not deserialise into a zero.
        let back: ReheatCurveJson = serde_json::from_value(v).unwrap();
        assert!(back.buckets.iter().all(|r| r.p50_us.is_none()));
        assert_eq!(back, curve);
    }

    #[test]
    fn a_measured_bucket_round_trips_with_microsecond_resolution() {
        // The markdown formats to 2dp of a millisecond; the JSON must not.
        let mut c = ReheatCostCurve::new("cold", 3);
        c.record(AgeBucket::TwoHour, 1_234_567, 4_096);
        c.record_error(AgeBucket::TwoHour, ReadErrorKind::RequestTimeout);
        let json = c.to_json();
        let back: ReheatCurveJson = serde_json::from_str(&serde_json::to_string(&json).unwrap()).unwrap();
        assert_eq!(back, json);

        let row = back.buckets.iter().find(|r| r.bucket == AgeBucket::TwoHour).unwrap();
        assert_eq!(row.p50_us, Some(1_234_567));
        assert_eq!(row.bytes_per_read, Some(4_096));
        assert_eq!(row.errors, 1);
        assert_eq!(back.error_kinds[0].kind, ReadErrorKind::RequestTimeout);
        assert_eq!(back.error_kinds[0].count, 1);
    }

    /// Phase 7 built all four curve fields inside one struct literal. Every
    /// guard in a `let` initialiser lives to the semicolon, so the delta row's
    /// second `warm` lock met the first one still held and the thread wedged in
    /// the futex forever — no panic, no progress, nothing written after it.
    /// Bounded on a side thread on purpose: a test that hangs the suite is
    /// worse than no test.
    #[test]
    fn snapshotting_the_curves_never_locks_a_curve_twice() {
        let curve = |ops| {
            let mut c = ReheatCostCurve::new("x", 1);
            c.record(AgeBucket::FiveMin, ops, 512);
            std::sync::Arc::new(Mutex::new(c))
        };
        let (fill, warm, cold) = (curve(100), curve(200), curve(900));

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || tx.send(Curves::snapshot(&fill, &warm, &cold)));
        let out = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Curves::snapshot deadlocked: a curve is locked twice in one statement");

        assert_eq!(out.fill.buckets.len(), AgeBucket::ALL.len());
        let row = out.cold_vs_warm.iter().find(|r| r.bucket == AgeBucket::FiveMin).unwrap();
        assert_eq!((row.p50_before_us, row.p50_after_us), (Some(200), Some(900)));
        assert_eq!(row.ratio, Some(4.5));
    }

    #[test]
    fn observed_density_divides_the_population_by_the_segments_on_disk() {
        // Run 1787052083: ~30,007 aggregates across 3 segments, reported as
        // "199999 achieved" — the model, which never saw the run.
        let d = AggsPerSegment::new(
            &params(),
            30_007,
            &shard_segments(&[(1, 1), (2, 1), (3, 1)]),
        );
        assert_eq!(d.observed_total_segments, 3);
        assert_eq!(d.observed_lower_bound, Some(10_002));
        assert_eq!(d.model, params().achieved_aggs_per_segment());
        assert_ne!(d.observed_lower_bound, Some(d.model));

        // Nothing rotated: no denominator, so no observation. Not zero.
        let none = AggsPerSegment::new(&params(), 30_007, &[]);
        assert_eq!(none.observed_total_segments, 0);
        assert_eq!(none.observed_lower_bound, None);
        assert_eq!(none.model, d.model);
    }

    #[test]
    fn the_markdown_curve_is_rendered_from_the_json_curve() {
        // Guards the two-sources-of-truth risk: the report table and the run
        // JSON are the same numbers or the test fails.
        let mut c = ReheatCostCurve::new("warm", 9);
        c.record(AgeBucket::FiveMin, 2_500, 1_024);
        let json = c.to_json();
        assert_eq!(c.to_markdown(), json.to_markdown());
        let row = json.buckets.iter().find(|r| r.bucket == AgeBucket::FiveMin).unwrap();
        assert_eq!(row.p50_us, Some(2_500));
        assert!(json.to_markdown().contains("| 5m | 1 | 0 | 0 | 2.50ms |"));
        // The empty buckets stay `—` in the table and null in the JSON.
        assert!(json.to_markdown().contains("| 4h | 0 | 0 | 0 | — | — | — | — |"));
    }
}
