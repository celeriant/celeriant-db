//! Sweeps the Auto-path stack scratch size against representative datablock sizes.
//!
//! The serialiser tries bincode-stack-ser into a `[u8; STACK_SCRATCH]` buffer first; if the
//! data fits, no heap allocation happens for the uncompressed form. Bigger scratch ⇒ more
//! batches stay off the heap. This bench measures the actual cost of that swap across
//! `(stack_scratch, datablock_size, compression_allowed)`.
//!
//! Scratch sizes start at 512 (the current production behaviour up to Phase 19) and go
//! through 8192. Datablock payload sizes are picked to land at:
//!   - 128 B  — comfortably inside MINIBATCH (512 B); all scratch sizes inline uncompressed
//!   - 700 B  — straddles the 513-1024 B "compression-to-fit" band
//!   - 1500 B — fits in 2K+ scratch, overflows 512/1K
//!   - 3000 B — fits in 4K+ scratch
//!   - 6000 B — only fits in 8K scratch
//!   - 12000 B — exceeds all tested scratch sizes; heap fallback regardless
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use celeriant_wal::datablocks::datablock_kind::DatablockKind;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::disk::serialised_datablock::{CompressionPolicy, SerialisedDatablock};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn make_datablock(payload_size: usize) -> Datablock {
    Datablock {
        datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
            aggregate_version: 1,
            events: vec![DatablockAggregateEvent {
                client_seq: 1,
                event_seq: 1,
                event_id: Some(0xAA),
                event_timestamp: 1_700_000_000,
                event_type_major: 1,
                event_type_minor: 0,
                event_value: Arc::new(vec![0x2Bu8; payload_size]),
                iv: None,
            }],
        }),
    }
}

fn payload_sizes() -> &'static [(usize, &'static str)] {
    &[
        (128, "0128B"),
        (700, "0700B"),
        (1500, "1500B"),
        (3000, "3000B"),
        (6000, "6000B"),
        (12000, "12000B"),
        (30000, "30000B"),
    ]
}

fn bench_stack_sweep(c: &mut Criterion) {
    let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict");

    for (payload_size, payload_label) in payload_sizes() {
        let datablock = make_datablock(*payload_size);

        for &allowed in &[true, false] {
            let allowed_label = if allowed { "comp_ok" } else { "encrypted" };
            let mut group = c.benchmark_group(format!("serialise/{}/{}", allowed_label, payload_label));
            group.sample_size(100);
            group.measurement_time(Duration::from_secs(2));
            group.warm_up_time(Duration::from_millis(300));

            let policy = CompressionPolicy::Auto { compression_allowed: allowed };

            // Each scratch size is its own monomorphization (const generic); use a macro
            // to keep the call sites tight.
            macro_rules! bench_scratch {
                ($n:literal) => {
                    group.bench_with_input(
                        BenchmarkId::from_parameter(format!("stack_{:>4}B", $n)),
                        &(&datablock, &codec),
                        |b, (datablock, codec)| {
                            b.iter(|| {
                                let serialised =
                                    SerialisedDatablock::new_with_stack_scratch::<$n>(
                                        black_box(*datablock),
                                        match policy {
                                            CompressionPolicy::Auto { compression_allowed } => {
                                                CompressionPolicy::Auto { compression_allowed }
                                            }
                                            CompressionPolicy::Fixed(t) => CompressionPolicy::Fixed(t),
                                        },
                                        black_box(*codec),
                                    )
                                    .expect("serialise");
                                black_box(serialised);
                            });
                        },
                    );
                };
            }

            bench_scratch!(512);
            bench_scratch!(1024);
            bench_scratch!(2048);
            bench_scratch!(4096);
            bench_scratch!(8192);
            bench_scratch!(16384);
            bench_scratch!(32768);

            group.finish();
        }
    }
}

criterion_group!(benches, bench_stack_sweep);
criterion_main!(benches);
