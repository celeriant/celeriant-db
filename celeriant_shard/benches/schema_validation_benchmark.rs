//! Per-event schema validation cost benchmark.
//!
//! Isolates the pure validation cost on the write hot path. On every write,
//! `ShardWal::write` calls `validator.validate(&event.event_value)` once per
//! event (see shard_wal.rs ~line 2131). This bench measures that call directly
//! against a pre-compiled validator — no WAL, no fsync, no disk — so the number
//! is the marginal cost validation adds per event.
//!
//! Approach (a): the validator exposes a pure callable function. We sweep
//! events-per-write, payload size, and schema complexity (field count).

use std::hint::black_box;
use std::time::Duration;

use celeriant_memcache::cached_schema::CachedValidator;
use celeriant_shard::schema_validator::CompiledValidator;
use celeriant_wal::SchemaType;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

criterion_group!(benches, bench_schema_validation);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

/// Events validated per simulated write (mirrors the per-event loop in write()).
const EVENTS_PER_WRITE: &[usize] = &[1, 10, 100];

/// Schema complexity: number of properties in the JSON schema (and the payload).
const SCHEMA_FIELDS: &[usize] = &[4, 32];

// =============================================================================
// HELPERS
// =============================================================================

/// Build a JSON-schema string with `n_fields` required string properties.
fn build_json_schema(n_fields: usize) -> String {
    let mut props = String::from(r#"{"type":"object","properties":{"#);
    for i in 0..n_fields {
        if i > 0 {
            props.push(',');
        }
        props.push_str(&format!(r#""field_{i}":{{"type":"string"}}"#));
    }
    props.push_str(r#"},"required":["#);
    for i in 0..n_fields {
        if i > 0 {
            props.push(',');
        }
        props.push_str(&format!(r#""field_{i}""#));
    }
    props.push_str("]}");
    props
}

/// Build a JSON payload matching a schema with `n_fields` string fields, each
/// value padded so the encoded event is roughly `target_bytes` long.
fn build_json_payload(n_fields: usize, target_bytes: usize) -> Vec<u8> {
    // Distribute the target size across the field values.
    let per_field = target_bytes / n_fields.max(1);
    let mut out = String::from("{");
    for i in 0..n_fields {
        if i > 0 {
            out.push(',');
        }
        let val = "x".repeat(per_field.max(1));
        out.push_str(&format!(r#""field_{i}":"{val}""#));
    }
    out.push('}');
    out.into_bytes()
}

fn compile(n_fields: usize) -> CachedValidator<CompiledValidator> {
    CompiledValidator::compile(SchemaType::Json, &build_json_schema(n_fields))
        .expect("schema compiles")
}

// =============================================================================
// BENCHMARK
// =============================================================================

/// Measure pure per-event validation cost, sweeping events-per-write, schema
/// complexity (field count), and event payload size.
fn bench_schema_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_validation");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    // Payload sizes (bytes per event value) to sweep.
    let payload_sizes: &[usize] = &[64, 1024];

    for &n_fields in SCHEMA_FIELDS {
        // Compile once per schema complexity — compilation is NOT part of the
        // hot path (schemas are cached), so it stays outside the measured loop.
        let validator = compile(n_fields);

        for &payload_size in payload_sizes {
            // Pre-build the event payload once; on the real write path the event
            // value is already in hand, so payload construction is excluded.
            let payload = build_json_payload(n_fields, payload_size);

            // Sanity: ensure the payload actually validates (so we measure the
            // success path, which is what production writes hit).
            assert!(
                validator.validate(&payload).is_ok(),
                "payload must validate against schema (fields={n_fields})"
            );

            for &events in EVENTS_PER_WRITE {
                let id = BenchmarkId::new(
                    format!("fields{n_fields}_payload{payload_size}b"),
                    format!("{events}ev"),
                );

                // Throughput in events lets Criterion report per-event time.
                group.throughput(Throughput::Elements(events as u64));

                group.bench_with_input(id, &events, |b, &events| {
                    b.iter(|| {
                        // Validate a batch of `events` event values, exactly as
                        // the per-event write loop does.
                        for _ in 0..events {
                            let r = validator.validate(black_box(&payload));
                            black_box(r).expect("valid");
                        }
                    });
                });
            }
        }
    }

    group.finish();
}
