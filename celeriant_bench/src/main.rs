use celeriant_bench::{BenchmarkResult, PoolBuilder, run_benchmark, smoke_test};
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
    println!();

    let pool = PoolBuilder {
        address1: &args.address1,
        address2: &args.address2,
        server_name: args.server_name.as_deref(),
        ca_cert: &args.ca_cert,
        client_cert: &args.client_cert,
        client_key: &args.client_key,
        plaintext: args.plaintext,
        max_connections: max_conns,
    }
    .build()
    .await?;

    println!("--- Smoke test ---");
    smoke_test(&pool).await?;
    println!("  Write OK\n");

    println!("--- Throughput ({} tasks, {}s) ---", args.tasks, args.duration);
    let result = run_benchmark(&pool, args.tasks, args.duration).await;
    print_result(&result);

    println!("\n=== Done ===");
    Ok(())
}
