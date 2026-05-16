use std::{hint::black_box, u64};
use std::time::Duration;

use bincode::{Decode, Encode};
use celeriant_wire::network::wire_header::{
    WireHeader, wire_header_write_fixed_size, wire_header_write_variable_size_uncompressed,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures_lite::{future::block_on, io::Cursor};
use serde::{Deserialize, Serialize};

const PROTOCOL_VERSION_V2: u32 = 2;
const PROTOCOL_VERSION_V3: u32 = 3;

criterion_group!(benches, bench_fixed_vs_variable);
criterion_main!(benches);

/// Small message that fits in WIRE_FIXED_BODY_SIZE - used for both fixed and variable benchmarks
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
struct SmallMessage {
    request_id: u64,
    timestamp: u64,
    flags: u32,
    sequence: u32,
}

fn create_small_message() -> SmallMessage {
    SmallMessage {
        request_id: 0x123456789ABCDEF0,
        timestamp: 1700000000000,
        flags: 0xDEADBEEF,
        sequence: 42,
    }
}

fn protocol_versions() -> Vec<(&'static str, u32)> {
    vec![
        ("v2_bincode", PROTOCOL_VERSION_V2),
        ("v3_msgpack", PROTOCOL_VERSION_V3),
    ]
}

fn bench_fixed_vs_variable(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_header/fixed_vs_variable");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_millis(500));

    let message = create_small_message();
    let request_type = 1u32;

    for (version_name, version) in protocol_versions() {
        // === WRITE BENCHMARKS ===

        // Fixed write (stack buffer)
        group.bench_with_input(
            BenchmarkId::new("write/fixed", version_name),
            &(&message, version),
            |b, (msg, ver)| {
                b.iter(|| {
                    block_on(async {
                        let mut buffer = Vec::with_capacity(128);
                        wire_header_write_fixed_size(
                            &mut buffer,
                            black_box(*msg),
                            request_type,
                            *ver,
                        )
                        .await
                        .unwrap();
                        buffer
                    })
                });
            },
        );

        // Variable write (heap allocated)
        group.bench_with_input(
            BenchmarkId::new("write/variable", version_name),
            &(&message, version),
            |b, (msg, ver)| {
                b.iter(|| {
                    block_on(async {
                        let mut buffer = Vec::with_capacity(128);
                        wire_header_write_variable_size_uncompressed(
                            &mut buffer,
                            black_box(*msg),
                            request_type,
                            u64::MAX,
                            *ver,
                        )
                        .await
                        .unwrap();
                        buffer
                    })
                });
            },
        );

        // === READ BENCHMARKS ===

        // Pre-serialize for read benchmarks
        let mut fixed_buffer = Vec::new();
        block_on(async {
            wire_header_write_fixed_size(&mut fixed_buffer, &message, request_type, version)
                .await
                .unwrap();
        });

        let mut variable_buffer = Vec::new();
        block_on(async {
            wire_header_write_variable_size_uncompressed(
                &mut variable_buffer,
                &message,
                request_type,
                u64::MAX,
                version,
            )
            .await
            .unwrap();
        });

        // Fixed read (stack buffer)
        group.bench_with_input(
            BenchmarkId::new("read/fixed", version_name),
            &fixed_buffer,
            |b, data| {
                b.iter(|| {
                    block_on(async {
                        let mut reader = Cursor::new(black_box(data.as_slice()));
                        let header = WireHeader::from_reader(&mut reader, u64::MAX).await.unwrap();
                        let decoded: SmallMessage = header
                            .read_fixed_size(&mut reader)
                            .await
                            .unwrap();
                        decoded
                    })
                });
            },
        );

        // Variable read (heap allocated)
        group.bench_with_input(
            BenchmarkId::new("read/variable", version_name),
            &variable_buffer,
            |b, data| {
                b.iter(|| {
                    block_on(async {
                        let mut reader = Cursor::new(black_box(data.as_slice()));
                        let header = WireHeader::from_reader(&mut reader, u64::MAX).await.unwrap();
                        let decoded: SmallMessage =
                            header.read_variable_size_uncompressed(&mut reader).await.unwrap();
                        decoded
                    })
                });
            },
        );

        // === ROUNDTRIP BENCHMARKS ===

        // Fixed roundtrip
        group.bench_with_input(
            BenchmarkId::new("roundtrip/fixed", version_name),
            &(&message, version),
            |b, (msg, ver)| {
                b.iter(|| {
                    block_on(async {
                        let mut buffer = Vec::with_capacity(128);
                        wire_header_write_fixed_size(
                            &mut buffer,
                            black_box(*msg),
                            request_type,
                            *ver,
                        )
                        .await
                        .unwrap();

                        let mut reader = Cursor::new(buffer.as_slice());
                        let header = WireHeader::from_reader(&mut reader, u64::MAX).await.unwrap();
                        let decoded: SmallMessage = header
                            .read_fixed_size(&mut reader)
                            .await
                            .unwrap();
                        decoded
                    })
                });
            },
        );

        // Variable roundtrip
        group.bench_with_input(
            BenchmarkId::new("roundtrip/variable", version_name),
            &(&message, version),
            |b, (msg, ver)| {
                b.iter(|| {
                    block_on(async {
                        let mut buffer = Vec::with_capacity(128);
                        wire_header_write_variable_size_uncompressed(
                            &mut buffer,
                            black_box(*msg),
                            request_type,
                            u64::MAX,
                            *ver,
                        )
                        .await
                        .unwrap();

                        let mut reader = Cursor::new(buffer.as_slice());
                        let header = WireHeader::from_reader(&mut reader, u64::MAX).await.unwrap();
                        let decoded: SmallMessage =
                            header.read_variable_size_uncompressed(&mut reader).await.unwrap();
                        decoded
                    })
                });
            },
        );
    }

    group.finish();
}