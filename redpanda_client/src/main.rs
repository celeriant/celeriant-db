use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{ClientConfig, FromClientConfig};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task;

fn parse_arg<T: std::str::FromStr>(args: &Vec<String>, idx: usize, default: T) -> T {
    if args.len() > idx {
        args[idx].parse::<T>().unwrap_or(default)
    } else {
        default
    }
}

async fn ensure_topics(
    brokers: &str,
    topic_prefix: &str,
    num_aggregates: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers);

    let admin: AdminClient<DefaultClientContext> = AdminClient::from_config(&cfg)?;

    // Collect names first so they outlive NewTopic borrows.
    let mut names = Vec::with_capacity(num_aggregates);
    for i in 0..num_aggregates {
        names.push(format!("{topic_prefix}-{i}"));
    }

    // Build NewTopic values that borrow from `names`.
    let new_topics: Vec<_> = names
        .iter()
        .map(|n| NewTopic::new(n, 1, TopicReplication::Fixed(1)))
        .collect();

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
    match admin.create_topics(&new_topics, &opts).await {
        Ok(results) => {
            for r in results {
                if let Err(e) = r {
                    // Ignore "topic already exists"
                    // if e.code() != RDKafkaErrorCode::TopicAlreadyExists {
                    //     return Err(format!("Topic creation failed: {e}").into());
                    // }
                }
            }
        }
        Err(e) => return Err(format!("Admin create_topics failed: {e}").into()),
    }

    Ok(names)
}

async fn run_task(
    producer: Arc<FutureProducer>,
    duration: Duration,
    topic: Arc<str>,
    key: Arc<[u8]>,
    payload: &'static [u8],
) -> u64 {
    let deadline = Instant::now() + duration;
    let mut count: u64 = 0;

    while Instant::now() < deadline {
        let rec = FutureRecord::to(&topic).key(&*key).payload(payload);
        match producer.send(rec, Timeout::Never).await {
            Ok(_delivery) => {
                count = count.wrapping_add(1);
            }
            Err((_e, _msg)) => break, // keep overhead low; stop this task on error
        }
    }
    count
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Args: [brokers] [topic_prefix] [num_connections] [num_aggregates] [duration_secs]
    // Example: 127.0.0.1:9092 bench-agg 512 16 30
    let args: Vec<String> = env::args().collect();
    let brokers = parse_arg(&args, 1, String::from("127.0.0.1:9092"));
    let topic_prefix = parse_arg(&args, 2, String::from("bench-aggregate"));
    let num_connections = parse_arg(&args, 3, 200usize);
    let num_aggregates = parse_arg(&args, 4, 4usize);
    let duration_secs = parse_arg(&args, 5, 30u64);

    println!("Redpanda Client (one topic per aggregate, durable-ish ack)");
    println!("Brokers: {}", brokers);
    println!("Topic prefix: {}", topic_prefix);
    println!("Connections (tasks): {}", num_connections);
    println!("Aggregates (topics): {}", num_aggregates);
    println!("Duration (s): {}", duration_secs);

    // Create one topic per aggregate (RF=1, 1 partition).
    let topic_names = ensure_topics(&brokers, &topic_prefix, num_aggregates).await?;

    // Build producer with acks=all and minimal client batching.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("retries", "1000000")
        .set("message.timeout.ms", "300000")
        // Minimize client-side batching (closer to per-request semantics)
        .set("linger.ms", "0")
        .set("batch.num.messages", "1")
        .set("compression.type", "none")
        // Large queues to avoid local backpressure under high concurrency
        .set("queue.buffering.max.messages", "1000000")
        .create()?;

    let producer = Arc::new(producer);
    let duration = Duration::from_secs(duration_secs);
    let payload: &'static [u8] = b"hello world";

    // Precompute per-aggregate keys and Arc<str> topics.
    let topics: Vec<Arc<str>> = topic_names.into_iter().map(|t| Arc::from(t)).collect();
    let keys: Vec<Arc<[u8]>> = (0..num_aggregates)
        .map(|i| Arc::from(format!("aggregate-{i}").into_bytes().into_boxed_slice()))
        .collect();

    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_connections);
    for conn_id in 0..num_connections {
        let p = producer.clone();
        let topic = topics[conn_id % num_aggregates].clone();
        let key = keys[conn_id % num_aggregates].clone();
        handles.push(task::spawn(run_task(p, duration, topic, key, payload)));
    }

    let mut total: u64 = 0;
    for h in handles {
        if let Ok(c) = h.await {
            total = total.wrapping_add(c);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rps = if elapsed > 0.0 { total as f64 / elapsed } else { 0.0 };
    println!("Completed: {} appends in {:.2}s -> {:.1} RPS", total, elapsed, rps);

    Ok(())
}