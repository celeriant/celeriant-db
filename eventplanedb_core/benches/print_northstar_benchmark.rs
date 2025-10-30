use std::hint::black_box;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::{criterion_group, criterion_main};

criterion_group!(benches, printlns);
criterion_main!(benches);

fn printlns(c: &mut Criterion) {

    let mut group = c.benchmark_group("printlns");

    for &size in &[1usize, 2, 3] {

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("1D", size), 
            &size, 
            |b, &s| {
                b.iter(|| print_1d(black_box(s)));
            });

        group.throughput(Throughput::Elements((size*size) as u64));
        group.bench_with_input(
            BenchmarkId::new("2D", size), 
            &size, 
            |b, &s| {
                b.iter(|| print_2d(black_box(s)));
            });
        
    }

    group.finish();
}

fn print_1d(size: usize) {
    for i in 0..size {
        println!("Number: {i}");
    }
}

fn print_2d(size: usize) {
    for i in 0..size {
        for j in 0..size {
            println!("Number: {i}x{j}");
        }
    }
}