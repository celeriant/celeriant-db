use bincode::{config, Encode};
use eventplanedb_structures::event_item::EventItem;
use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// Wire protocol request (only the variant we use)
#[derive(Debug, Clone, Encode)]
pub enum Request {
    AppendEvents {
        sync_delay_us: u64,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        allow_create: bool,
        expected_event_batch_index: Option<u64>,
        filter_duplicate_client_events: bool,
        durable_write: bool,
    },
}

fn build_combined_request_bytes(
    aggregate_id: u128,
    sync_delay_us: u64,
    org_id: u128,
    aggregate_type_id: u128,
    client_id: u128,
    events: &Vec<EventItem>,
) -> Vec<u8> {
    let request = Request::AppendEvents {
        sync_delay_us,
        org_id,
        aggregate_type_id,
        aggregate_id,
        client_id,
        user_id: None,
        events: events.clone(), // cloned once at startup per aggregate
        expected_event_batch_index: None,
        filter_duplicate_client_events: false,
        allow_create: true,
        durable_write: true,
    };

    let encoded = bincode::encode_to_vec(&request, config::standard()).unwrap();
    let mut combined = Vec::with_capacity(8 + encoded.len()); // 4 bytes version + 4 bytes length + payload
    combined.extend_from_slice(&(2u32).to_be_bytes()); // version
    combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes()); // length
    combined.extend_from_slice(&encoded);
    combined
}

fn run_client_connection(
    server_addr: String,
    duration: Duration,
    cached_requests: Arc<HashMap<u128, Vec<u8>>>,
    aggregate_id: u128,
) -> u64 {
    let mut stream = match TcpStream::connect(&server_addr) {
        Ok(s) => s,
        Err(_) => return 0,
    };

    // Socket options (set once per thread)
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));

    let deadline = Instant::now() + duration;
    let mut count: u64 = 0;

    let mut header = [0u8; 8];
    let mut scratch: Vec<u8> = vec![0u8; 4096]; // reusable buffer for response payload
    // let mut agg_idx: u128 = 0;

    while Instant::now() < deadline {
        // Pick aggregate id (simple round-robin to avoid RNG overhead)
        // agg_idx += 1;
        // if agg_idx >= num_aggregates {
        //     agg_idx = 0;
        // }

        let bytes = match cached_requests.get(&aggregate_id) {
            Some(b) => b,
            None => break,
        };

        if stream.write_all(bytes).is_err() {
            break;
        }

        // Read and discard response (version + length + payload)
        if stream.read_exact(&mut header).is_err() {
            break;
        }

        // length is in bytes 4..8
        let len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;

        if len > scratch.len() {
            scratch.resize(len, 0);
        }

        if stream.read_exact(&mut scratch[..len]).is_err() {
            break;
        }

        count = count.wrapping_add(1);
    }

    count
}

fn parse_arg<T: std::str::FromStr>(args: &Vec<String>, idx: usize, default: T) -> T {
    if args.len() > idx {
        args[idx].parse::<T>().unwrap_or(default)
    } else {
        default
    }
}

fn main() {
    // Args: [server_addr] [num_connections] [num_aggregates] [sync_delay_us] [duration_secs]
    let args: Vec<String> = env::args().collect();

    let server_addr = parse_arg(&args, 1, String::from("127.0.0.1:10000"));
    let num_connections = parse_arg(&args, 2, 200usize);
    let num_aggregates = parse_arg(&args, 3, 4u128);
    let sync_delay_us = parse_arg(&args, 4, 100u64);
    let duration_secs = parse_arg(&args, 5, 30u64);

    println!("TCP Client (minimal work)");
    println!("Server: {}", server_addr);
    println!("Connections: {}", num_connections);
    println!("Aggregates: {}", num_aggregates);
    println!("Sync delay (us): {}", sync_delay_us);
    println!("Duration (s): {}", duration_secs);

    let duration = Duration::from_secs(duration_secs);

    // Static fields (can be made args if needed)
    let org_id: u128 = 1;
    let aggregate_type_id: u128 = 1;
    let client_id: u128 = 1;

    // One tiny event payload reused in all requests (only cloned during cache build)
    let base_events = vec![
        EventItem::new(
            0,
            0,
            None,
            0,
            1,
            0,
            b"Hello world".to_vec(),
        ),
    ];
    // let base_events = vec![
    //     EventItem::new(
    //         0,
    //         0,
    //         None,
    //         0,
    //         1,
    //         0,
    //         b"In the quiet hum of servers running through the night, we craft elegant algorithms and pristine code. Each function a verse, each variable a word. We debug with patience, refactor with purpose, and ship with hope. Through countless iterations we learn and grow, transforming coffee into software, ideas into reality. We embrace the edge cases, handle the errors gracefully, and celebrate when the tests finally pass. In this digital forge we are architects and artists, building systems that scale, creating experiences that matter, writing the future one commit at a time with determination and endless curiosity.".to_vec(),
    //     ),
    //     EventItem::new(
    //         1,
    //         0,
    //         None,
    //         0,
    //         1,
    //         0,
    //         b"Through fiber and copper the data streams flow, carrying dreams across the digital expanse. Packets traverse networks, dancing through routers, finding their path through the chaos of wires. We build bridges of bandwidth, tunnels of trust, establishing connections that span continents and cultures. Each byte tells a story, each frame holds meaning. We monitor latencies, optimize throughput, ensuring the message arrives intact and timely. In this web of communication we are guardians and guides, maintaining the infrastructure that binds our world together, enabling conversations that change lives and forge futures.".to_vec(),
    //     ),
    //     EventItem::new(
    //         2,
    //         0,
    //         None,
    //         0,
    //         1,
    //         0,
    //         b"Layer upon layer we construct the architecture, foundations of databases supporting towers of services. Microservices communicate, APIs expose interfaces, containers orchestrate in harmony. We design for resilience, plan for failure, build redundancy into every component. Load balancers distribute the weight, caches accelerate the response, queues buffer the storms. Through careful planning and thoughtful design we create systems that endure. We document thoroughly, test rigorously, deploy cautiously. In this realm of distributed systems we are engineers and visionaries, solving puzzles of scale and complexity, turning requirements into elegant solutions.".to_vec(),
    //     ),
    // ];

    // Up-front cache of full request bytes for each aggregate
    let mut map = HashMap::with_capacity(num_aggregates as usize);
    for aggregate_id in 0..num_aggregates {
        let bytes = build_combined_request_bytes(
            aggregate_id,
            sync_delay_us,
            org_id,
            aggregate_type_id,
            client_id,
            &base_events,
        );
        map.insert(aggregate_id, bytes);
    }
    let cached_requests = Arc::new(map);

    // Start benchmark
    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_connections);
    for conn_id in 0..num_connections {
        let addr = server_addr.clone();
        let cache = Arc::clone(&cached_requests);
        let d = duration;
        let aggregate_id = conn_id as u128 % num_aggregates;

        handles.push(thread::spawn(move || run_client_connection(addr, d, cache, aggregate_id)));
    }

    let mut total: u64 = 0;
    for h in handles {
        if let Ok(c) = h.join() {
            total = total.wrapping_add(c);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rps = if elapsed > 0.0 {
        total as f64 / elapsed
    } else {
        0.0
    };

    println!("Completed: {} requests in {:.2}s -> {:.1} RPS", total, elapsed, rps);
}