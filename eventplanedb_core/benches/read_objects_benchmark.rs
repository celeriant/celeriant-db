
use std::hint::black_box;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::{criterion_group, criterion_main};

criterion_group!(benches, read_objects);
criterion_main!(benches);

fn read_objects(c: &mut Criterion) {

    let mut group = c.benchmark_group("read_objects");

    for &size in &[1usize, 2, 3] {

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("1D", size), 
            &size, 
            |b, &s| {
                b.iter(|| print_1d(black_box(s)));
            });
        
    }

    group.finish();
}

fn print_1d(size: usize) {
    for i in 0..size {
        println!("Number: {i}");
    }
}