use bytes::Bytes;
use eventstore::{AppendToStreamOptions, Client, ExpectedRevision};
use std::env;
use std::time::{Duration, Instant};
use tokio::task;

fn parse_arg<T: std::str::FromStr>(args: &Vec<String>, idx: usize, default: T) -> T {
    if args.len() > idx {
        args[idx].parse::<T>().unwrap_or(default)
    } else {
        default
    }
}

async fn run_client_connection(
    client: Client,
    duration: Duration,
    stream_name: String,
    event_type: &'static str,
    payload: Bytes,
) -> u64 {
    let deadline = Instant::now() + duration;
    let options = AppendToStreamOptions::default().expected_revision(ExpectedRevision::Any);
    let mut count: u64 = 0;

    // Tight loop, minimal allocations. Event id is generated per call by the client.
    while Instant::now() < deadline {
        // Create a tiny binary event with a stable event type and shared payload.
        let evt = eventstore::EventData::binary(event_type, payload.clone());

        // Append and count; break on error to keep overhead low.
        match client.append_to_stream(stream_name.as_str(), &options, evt).await {
            Ok(_) => {
                count = count.wrapping_add(1);
            }
            Err(_) => break,
        }
    }

    count
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Args: [esdb_connection_string] [num_connections] [num_aggregates] [duration_secs]
    // Example: esdb://admin:changeit@localhost:2113?tls=false
    let args: Vec<String> = env::args().collect();

    let esdb_conn_str = parse_arg(
        &args,
        1,
        String::from("esdb://admin:changeit@localhost:2113?tls=false"),
    );
    let num_connections = parse_arg(&args, 2, 200usize);
    let num_aggregates = parse_arg(&args, 3, 4usize);
    let duration_secs = parse_arg(&args, 4, 30u64);

    println!("EventStoreDB Client (append benchmark)");
    println!("ESDB: {}", esdb_conn_str);
    println!("Connections (tasks): {}", num_connections);
    println!("Aggregates (streams): {}", num_aggregates);
    println!("Duration (s): {}", duration_secs);

    let duration = Duration::from_secs(duration_secs);

    // Build a single client; cloning Client is cheap (Arc under the hood).
    let settings = esdb_conn_str.parse()?;
    let client = Client::new(settings)?;

    // Pre-build stream names and a shared payload buffer.
    let stream_names: Vec<String> = (0..num_aggregates)
        .map(|i| format!("bench-aggregate-{}", i))
        .collect();

    // Small, static payload cloned via Bytes (shared ref-counted buffer).
    let payload = Bytes::from_static(b"hello world");
    let event_type: &'static str = "hello-event";

    let start = Instant::now();

    // Spawn tasks, each targeting one aggregate (round-robin).
    let mut handles = Vec::with_capacity(num_connections);
    for conn_id in 0..num_connections {
        let client_clone = client.clone();
        let stream_name = stream_names[conn_id % num_aggregates].clone();
        let payload_clone = payload.clone();

        handles.push(task::spawn(run_client_connection(
            client_clone,
            duration,
            stream_name,
            event_type,
            payload_clone,
        )));
    }

    // Gather results.
    let mut total: u64 = 0;
    for h in handles {
        if let Ok(c) = h.await {
            total = total.wrapping_add(c);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rps = if elapsed > 0.0 {
        total as f64 / elapsed
    } else {
        0.0
    };

    println!("Completed: {} appends in {:.2}s -> {:.1} RPS", total, elapsed, rps);

    Ok(())
}