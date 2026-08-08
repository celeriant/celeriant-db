use celeriant_bench::{
    BenchmarkResult, PoolBuilder, WorkloadOptions, assert_schema_enforced, register_workload_schema,
    run_benchmark_ramped_fanout, run_benchmark_workload_fanout, smoke_test,
};
use clap::Parser;

#[derive(Parser)]
#[command(name = "celeriant-bench", about = "Write throughput and latency benchmark against a remote Celeriant cluster")]
struct Args {
    #[arg(long, env = "CLUSTER_ADDRESS_1")]
    address1: String,

    #[arg(long, env = "CLUSTER_ADDRESS_2")]
    address2: String,

    #[arg(long, env = "CLUSTER_SERVER_NAME")]
    server_name: Option<String>,

    #[arg(long, env = "CLUSTER_CA_CERT", default_value = "deploy/rpi-cluster/certs/client-ca.crt")]
    ca_cert: String,

    #[arg(long, env = "CLUSTER_CLIENT_CERT", default_value = "deploy/rpi-cluster/certs/client.crt")]
    client_cert: String,

    #[arg(long, env = "CLUSTER_CLIENT_KEY", default_value = "deploy/rpi-cluster/certs/client.key")]
    client_key: String,

    #[arg(long, env = "CLUSTER_PLAINTEXT")]
    plaintext: bool,

    #[arg(long, env = "CLUSTER_TASKS", default_value = "8000")]
    tasks: usize,

    #[arg(long, env = "CLUSTER_CONNECTIONS")]
    connections: Option<usize>,

    #[arg(long, env = "CLUSTER_DURATION", default_value = "15")]
    duration: u64,

    /// Drive the P99-conf talk workload: one small JSON event ({"a":N,"b":M}) per request
    /// instead of the opaque hello payload. Required by --occ and --schema.
    #[arg(long, env = "CLUSTER_WORKLOAD")]
    workload: bool,

    /// Register the workload's JSON schema before load starts, and refuse to run unless a
    /// schema-violating write is actually rejected.
    #[arg(long, env = "CLUSTER_SCHEMA")]
    schema: bool,

    /// Send expected_version on every write.
    #[arg(long, env = "CLUSTER_OCC")]
    occ: bool,

    /// Offset every aggregate id by this much. Task ids restart at 0 on each client
    /// machine, so without distinct offsets both machines drive the same aggregates —
    /// harmless for plain appends, a permanent conflict for OCC.
    #[arg(long, env = "CLUSTER_AGGREGATE_OFFSET", default_value = "0")]
    aggregate_offset: usize,

    /// Spread the tasks over N pools, each dialling a different destination address, to get
    /// past the client's ephemeral-port ceiling.
    ///
    /// A connection is keyed on (src_ip, src_port, dst_ip, dst_port), so every connection to
    /// one destination burns a distinct source port and Linux offers only 28,232 — which is
    /// why 24,000 connections work here and 32,000 do not. Each extra destination is a fresh
    /// budget.
    ///
    /// Destinations are derived by incrementing the last octet of --address1, so this is
    /// meant for a loopback target (all of 127.0.0.0/8 reaches a server bound to 0.0.0.0,
    /// with no server-side change). 1 means the old single-pool behaviour exactly.
    ///
    /// Each pool is seeded on its own destination for BOTH addresses. Seeding them all with a
    /// shared --address2 made the pools see different topologies and route differently: it
    /// halved connection redirects per write and moved throughput 0.57%, well over the 0.32%
    /// bar this rig measures at (F-33). Requires --address2 == --address1 for the same reason.
    #[arg(long, env = "CLUSTER_DEST_FANOUT", default_value = "1")]
    dest_fanout: usize,

    /// Spread task starts linearly over this many seconds instead of releasing them as one
    /// herd. `run_benchmark_ramped` has always taken this and nothing exposed it, so a
    /// multi-client run had every client cold-connect in the same instant — which is how a
    /// four-client run silently became a three-client run on 2026-08-05.
    #[arg(long, env = "CLUSTER_CONNECT_RAMP")]
    connect_ramp: Option<u64>,
}

/// Build `n` destination addresses from a base `ip:port` by incrementing the final octet.
///
/// Only sensible for loopback, and deliberately not clever: a non-IPv4 base, or a fanout
/// that would run the last octet past 254, is an error rather than a silent single pool.
/// Silently degrading here would mean measuring a port ceiling and reporting it as a server
/// limit, which is the exact mistake this flag exists to undo.
fn fanout_addresses(base: &str, n: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if n == 0 {
        return Err("--dest-fanout must be at least 1".into());
    }
    if n == 1 {
        return Ok(vec![base.to_string()]);
    }
    let (host, port) = base.rsplit_once(':').ok_or("--address1 must be ip:port")?;
    let octets: Vec<&str> = host.split('.').collect();
    if octets.len() != 4 {
        return Err(format!("--dest-fanout needs an IPv4 --address1, got '{host}'").into());
    }
    let last: usize = octets[3].parse()?;
    if last + n - 1 > 254 {
        return Err(format!("--dest-fanout {n} from {host} runs past .254").into());
    }
    Ok((0..n)
        .map(|i| format!("{}.{}.{}.{}:{}", octets[0], octets[1], octets[2], last + i, port))
        .collect())
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let max_conns = args.connections.unwrap_or(args.tasks);

    println!("=== RPi Cluster Pool Benchmark ===\n");
    println!("  Primary:    {}", args.address1);
    println!("  Seed:       {}", args.address2);
    println!("  TLS:        {}", if args.plaintext { "disabled" } else { "mTLS" });
    println!("  Pool conns: {max_conns}/node");
    println!("  Tasks:      {}", args.tasks);
    println!("  Duration:   {}s", args.duration);
    println!("  Workload:   {}", if args.workload { "json-events" } else { "opaque" });
    println!("  Schema:     {}", if args.schema { "on" } else { "off" });
    println!("  OCC:        {}", if args.occ { "on" } else { "off" });
    println!("  Agg offset: {}", args.aggregate_offset);
    println!();

    // Each pool carries its own share of the connection budget, so a fanout run offers the
    // same total concurrency as a single-pool run of the same --tasks. Without the division
    // the fanout would raise offered load and connection count together and the two effects
    // could not be separated.
    let destinations = fanout_addresses(&args.address1, args.dest_fanout)?;
    let conns_per_pool = max_conns.div_ceil(destinations.len());
    if destinations.len() > 1 {
        // A second seed pointing somewhere else would make pool 0 symmetric and every other
        // pool asymmetric, which is the F-33 defect. Refuse rather than measure it again.
        if args.address2 != args.address1 {
            return Err("--dest-fanout > 1 needs --address2 == --address1: each pool is seeded \
                        on its own destination, and a shared second seed changes how the pools \
                        route (F-33)"
                .into());
        }
        println!("  Fanout:     {} destinations, {conns_per_pool} conns each", destinations.len());
        println!("              {}", destinations.join(" "));
    }

    let mut pools = Vec::with_capacity(destinations.len());
    for dest in &destinations {
        pools.push(
            PoolBuilder {
                address1: dest,
                address2: if destinations.len() > 1 { dest } else { &args.address2 },
                server_name: args.server_name.as_deref(),
                ca_cert: &args.ca_cert,
                client_cert: &args.client_cert,
                client_key: &args.client_key,
                plaintext: args.plaintext,
                max_connections: conns_per_pool,
            }
            .build()
            .await?,
        );
    }
    let pool = pools[0].clone();

    println!("--- Smoke test ---");
    smoke_test(&pool).await?;
    println!("  Write OK\n");

    if args.schema {
        if !args.workload {
            return Err("--schema requires --workload: the opaque hello payload is not JSON \
                        and every write would be rejected"
                .into());
        }
        println!("--- Schema ---");
        register_workload_schema(&pool).await?;
        // Registration returning Ok does not mean writes are validated — a mismatched
        // schema key or an IV on the event makes the server skip validation silently. Prove
        // enforcement before measuring, or the run prices an unvalidated path.
        assert_schema_enforced(&pool).await?;
        println!("  Registered and enforcement confirmed\n");
    }

    println!("--- Throughput ({} tasks, {}s) ---", args.tasks, args.duration);
    let result = if args.workload {
        let opts = WorkloadOptions { occ: args.occ, aggregate_offset: args.aggregate_offset };
        run_benchmark_workload_fanout(&pools, args.tasks, args.duration, args.connect_ramp, opts).await
    } else {
        run_benchmark_ramped_fanout(&pools, args.tasks, args.duration, args.connect_ramp).await
    };
    print_result(&result);

    println!("\n=== Done ===");
    Ok(())
}
