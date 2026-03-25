//! Kafka producer benchmark — mirrors Celeriant's rpi_cluster_pool_bench.
//!
//! N concurrent tokio tasks, each doing synchronous produce-wait-for-ack in a
//! tight loop. Uses rdkafka FutureProducer with batch.num.messages=1,
//! queue.buffering.max.ms=0, acks=all for true per-record latency measurement.
//!
//! Environment variables:
//!   KAFKA_BOOTSTRAP     — broker addresses (default: localhost:9093)
//!   KAFKA_TOPIC         — topic name (default: benchmark-test)
//!   KAFKA_TASKS         — concurrent producer tasks (default: 2000)
//!   KAFKA_DURATION      — test duration in seconds (default: 15)
//!   KAFKA_RECORD_SIZE   — payload size in bytes (default: 256)
//!   KAFKA_TLS           — enable TLS: "true" or "false" (default: true)
//!   KAFKA_CA_CERT       — CA cert PEM file (default: /etc/kafka/certs/ca.crt)
//!   KAFKA_PARTITIONS    — number of partitions to create topic with (default: 16)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::Barrier;
use tokio::time::Instant;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn build_config(bootstrap: &str, tls: bool, ca_cert: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap)
        .set("request.required.acks", "all")
        .set("queue.buffering.max.ms", "0")
        .set("batch.num.messages", "1")
        .set("message.timeout.ms", "30000")
        .set("socket.nagle.disable", "true");

    if tls {
        config
            .set("security.protocol", "SSL")
            .set("ssl.ca.location", ca_cert)
            .set("ssl.endpoint.identification.algorithm", "none");
    }

    config
}

async fn ensure_topic(
    config: &ClientConfig,
    topic: &str,
    partitions: i32,
) {
    let admin: AdminClient<DefaultClientContext> = config
        .clone()
        .create()
        .expect("failed to create admin client");

    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(2));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    // Ignore errors (topic may already exist)
    let _ = admin.create_topics(&[new_topic], &opts).await;
}

struct BenchmarkResult {
    num_tasks: usize,
    total_requests: u64,
    errors: u64,
    throughput: f64,
    avg_latency_ms: f64,
    p50_ms: u64,
    p95_ms: u64,
    p99_ms: u64,
    p999_ms: u64,
    min_ms: u64,
    max_ms: u64,
}

fn print_result(r: &BenchmarkResult) {
    println!(
        "  Tasks: {} | Requests: {} | Errors: {} | Throughput: {:.0} req/s",
        r.num_tasks, r.total_requests, r.errors, r.throughput
    );
    println!(
        "  Latency — Avg: {:.1}ms | P50: {}ms | P95: {}ms | P99: {}ms | P99.9: {}ms | Min: {}ms | Max: {}ms",
        r.avg_latency_ms, r.p50_ms, r.p95_ms, r.p99_ms, r.p999_ms, r.min_ms, r.max_ms
    );
}

async fn run_benchmark(
    producer: &FutureProducer,
    topic: &str,
    num_tasks: usize,
    duration_secs: u64,
    payload: &[u8],
) -> BenchmarkResult {
    let barrier = Arc::new(Barrier::new(num_tasks));
    let total_ok = Arc::new(AtomicU64::new(0));
    let total_err = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::with_capacity(num_tasks);
    let start = Instant::now();

    for id in 0..num_tasks {
        let producer = producer.clone();
        let barrier = barrier.clone();
        let ok_counter = total_ok.clone();
        let err_counter = total_err.clone();
        let topic = topic.to_string();
        let payload = payload.to_vec();

        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            barrier.wait().await;
            let deadline = Instant::now() + Duration::from_secs(duration_secs);
            let key = format!("task-{id}");

            while Instant::now() < deadline {
                let record = FutureRecord::to(&topic)
                    .key(&key)
                    .payload(&payload);

                let req_start = Instant::now();
                match producer.send(record, Duration::from_secs(10)).await {
                    Ok(_) => {
                        latencies.push(req_start.elapsed().as_millis() as u64);
                        ok_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err((e, _)) => {
                        err_counter.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Task {id} error: {e}");
                    }
                }
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = env_or("KAFKA_BOOTSTRAP", "localhost:9093");
    let topic = env_or("KAFKA_TOPIC", "benchmark-test");
    let num_tasks: usize = env_or("KAFKA_TASKS", "2000").parse()?;
    let duration_secs: u64 = env_or("KAFKA_DURATION", "15").parse()?;
    let record_size: usize = env_or("KAFKA_RECORD_SIZE", "256").parse()?;
    let tls = env_or("KAFKA_TLS", "true") == "true";
    let ca_cert = env_or("KAFKA_CA_CERT", "/etc/kafka/certs/ca.crt");
    let partitions: i32 = env_or("KAFKA_PARTITIONS", "16").parse()?;

    println!("=== Kafka Producer Benchmark ===\n");
    println!("  Bootstrap:   {bootstrap}");
    println!("  Topic:       {topic} ({partitions} partitions, RF=2, min.isr=2)");
    println!("  TLS:         {}", if tls { "enabled" } else { "disabled" });
    println!("  Tasks:       {num_tasks}");
    println!("  Duration:    {duration_secs}s");
    println!("  Record size: {record_size} bytes");
    println!("  acks=all, batch.num.messages=1, linger.ms=0");
    println!();

    let config = build_config(&bootstrap, tls, &ca_cert);
    let producer: FutureProducer = config.create()?;

    // Create topic if it doesn't exist
    ensure_topic(&config, &topic, partitions).await;

    // Smoke test
    println!("--- Smoke test ---");
    let record = FutureRecord::to(&topic)
        .key("smoke")
        .payload("smoke-test");
    producer.send(record, Duration::from_secs(10)).await
        .map_err(|(e, _)| format!("Smoke test failed: {e}"))?;
    println!("  Write OK\n");

    // Generate payload
    let payload = vec![0x42u8; record_size];

    // Throughput benchmark
    println!("--- Throughput ({num_tasks} tasks, {duration_secs}s) ---");
    let result = run_benchmark(&producer, &topic, num_tasks, duration_secs, &payload).await;
    print_result(&result);

    println!("\n=== Done ===");
    Ok(())
}
