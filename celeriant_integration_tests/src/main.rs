use celeriant_integration_tests::registry::{all_tests, parse_categories, Category, TestFilter};
use clap::Parser;
use std::fs::File;
use std::process::Command;
use std::time::Instant;
use wait_timeout::ChildExt;

#[derive(Parser)]
#[command(name = "celeriant-integration-tests", about = "Celeriant integration test runner")]
struct Cli {
    /// Run a specific test by name
    #[arg(long)]
    test: Option<String>,

    /// Include tests matching ALL of these categories (AND). Comma-separated.
    #[arg(long, value_delimiter = ',')]
    include: Vec<String>,

    /// Include tests matching ANY of these categories (OR). Comma-separated.
    #[arg(long, value_delimiter = ',')]
    include_or: Vec<String>,

    /// Exclude tests matching ALL of these categories (AND). Comma-separated.
    #[arg(long, value_delimiter = ',')]
    exclude: Vec<String>,

    /// Exclude tests matching ANY of these categories (OR). Comma-separated.
    #[arg(long, value_delimiter = ',')]
    exclude_or: Vec<String>,

    /// Only run distributed tests (require MinIO + multi-node)
    #[arg(long)]
    distributed: bool,

    /// Only run standalone tests (single node, no MinIO)
    #[arg(long)]
    standalone: bool,

    /// List matching tests without running them
    #[arg(long)]
    list: bool,

    /// List all available categories
    #[arg(long)]
    list_categories: bool,

    /// Per-test timeout in seconds (default: auto based on 2x estimated time, min 60s)
    #[arg(long)]
    timeout: Option<u32>,

    /// Internal: run a single test directly (used for subprocess dispatch)
    #[arg(long, hide = true)]
    run_test: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Subprocess dispatch: run a single test directly
    if let Some(ref name) = cli.run_test {
        if let Err(e) = dispatch_test(name).await {
            eprintln!("{e}");
            std::process::exit(1);
        }
        return;
    }

    if cli.list_categories {
        println!("Available categories:");
        for cat in Category::ALL {
            let count = all_tests().iter().filter(|t| t.has_category(*cat)).count();
            println!("  {:<16} ({} tests)", cat.as_str(), count);
        }
        return;
    }

    if cli.distributed && cli.standalone {
        eprintln!("error: --distributed and --standalone are mutually exclusive");
        std::process::exit(1);
    }

    let filter = match build_filter(&cli) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    let tests = all_tests();
    let selected = filter.apply(tests);

    if selected.is_empty() {
        println!("No tests match the given filters.");
        return;
    }

    if cli.list {
        print_test_list(&selected);
        return;
    }

    // Single test: run as subprocess, tee output to both terminal and log file
    if selected.len() == 1 {
        let name = selected[0].name;
        let self_exe = std::env::current_exe().expect("cannot resolve own executable path");
        let log_path = format!("/tmp/celeriant_test_{name}.log");

        println!("Running test: {name}");
        println!("Log: {log_path}\n");

        let log_file = File::create(&log_path).expect("cannot create log file");
        let log_file2 = log_file.try_clone().expect("cannot clone log handle");

        let mut child = Command::new(&self_exe)
            .args(["--run-test", name])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("failed to spawn test subprocess");

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        let t1 = std::thread::spawn(move || tee(stdout, std::io::stdout(), log_file));
        let t2 = std::thread::spawn(move || tee(stderr, std::io::stderr(), log_file2));

        let status = child.wait().expect("failed to wait on test subprocess");
        let _ = t1.join();
        let _ = t2.join();

        if status.success() {
            println!("\nPASS");
        } else {
            eprintln!("\nFAILED (exit {})", status.code().unwrap_or(-1));
            std::process::exit(1);
        }
        return;
    }

    run_tests(&selected, cli.timeout);
}

async fn dispatch_test(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    use celeriant_integration_tests::*;
    match name {
        "api_key_test" => api_key_test::run().await,
        "batch" => batch::run().await,
        "batch_standalone_cleartext" => batch_standalone_cleartext::run().await,
        "chaos" => chaos::run().await,
        "chaos_delete" => chaos_delete::run().await,
        "compaction_replicated" => compaction_replicated::run().await,
        "compaction_restart" => compaction_restart::run().await,
        "compaction_standalone" => compaction_standalone::run().await,
        "connection_test" => connection_test::run().await,
        "debug_follower_pressure" => debug_follower_pressure::run().await,
        "edge_concurrent_heartbeat_replication_s3" => edge_concurrent_heartbeat_replication_s3::run().await,
        "edge_corrupted_s3_batch" => edge_corrupted_s3_batch::run().await,
        "edge_empty_replication_batch" => edge_empty_replication_batch::run().await,
        "edge_heartbeat_lock_contention" => edge_heartbeat_lock_contention::run().await,
        "edge_leader_crash_divergent_wal" => edge_leader_crash_divergent_wal::run().await,
        "edge_list_pagination_cache_eviction" => edge_list_pagination_cache_eviction::run().await,
        "edge_log_eviction_before_s3" => edge_log_eviction_before_s3::run().await,
        "edge_log_rotation_mid_replication" => edge_log_rotation_mid_replication::run().await,
        "edge_s3_batch_ordering" => edge_s3_batch_ordering::run().await,
        "edge_s3_missing_batches" => edge_s3_missing_batches::run().await,
        "edge_split_brain_s3_unavailable" => edge_split_brain_s3_unavailable::run().await,
        "edge_stale_cache_rotation" => edge_stale_cache_rotation::run().await,
        "edge_wal_divergence_recovery" => edge_wal_divergence_recovery::run().await,
        "edge_wal_tip_hash_divergence" => edge_wal_tip_hash_divergence::run().await,
        "follower_read_snapshot" => follower_read_snapshot::run().await,
        "identity_test" => identity_test::run().await,
        "invariant_concurrent_write" => invariant_concurrent_write::run().await,
        "invariant_read_count" => invariant_read_count::run().await,
        "invariant_replication_convergence" => invariant_replication_convergence::run().await,
        "invariant_replication_queue_pressure" => invariant_replication_queue_pressure::run().await,
        "invariant_s3_fallback_dedup" => invariant_s3_fallback_dedup::run().await,
        "leader_read_visibility" => leader_read_visibility::run().await,
        "mtls_test" => mtls_test::run().await,
        "multi_shard_watch_test" => multi_shard_watch_test::run().await,
        "not_leader_error" => not_leader_error::run().await,
        "p1_1_dcb_rollback" => p1_1_dcb_rollback::run().await,
        "p1_2_concurrent_dcb" => p1_2_concurrent_dcb::run().await,
        "p1_3_cross_shard_rejection" => p1_3_cross_shard_rejection::run().await,
        "p1_4_exactly_once" => p1_4_exactly_once::run().await,
        "p1_6_ordering_verification" => p1_6_ordering_verification::run().await,
        "p1_7_multitenancy_isolation" => p1_7_multitenancy_isolation::run().await,
        "p2_1_write_survival" => p2_1_write_survival::run().await,
        "p2_2_dual_restart" => p2_2_dual_restart::run().await,
        "p2_3_wal_corruption" => p2_3_wal_corruption::run().await,
        "p2_4_s3_capacity" => p2_4_s3_capacity::run().await,
        "p3_1_cold_read_latency" => p3_1_cold_read_latency::run().await,
        "p3_2_bloom_filter" => p3_2_bloom_filter::run().await,
        "p3_3_sequential_cold_reads" => p3_3_sequential_cold_reads::run().await,
        "p4_1_rolling_upgrade" => p4_1_rolling_upgrade::run().await,
        "pool_test" => pool_test::run().await,
        "read_list_benchmark" => read_list_benchmark::run().await,
        "s3_concurrent_cas" => s3_concurrent_cas::run().await,
        "s3_election" => s3_election::run().await,
        "s3_failover" => s3_failover::run().await,
        "s3_failover_latency" => s3_failover_latency::run().await,
        "s3_fallback" => s3_fallback::run().await,
        "s3_fallback_catchup" => s3_fallback_catchup::run().await,
        "s3_fallback_createonly" => s3_fallback_createonly::run().await,
        "s3_fallback_s3_down" => s3_fallback_s3_down::run().await,
        "s3_fencing_writes" => s3_fencing_writes::run().await,
        "s3_follower_crash" => s3_follower_crash::run().await,
        "s3_follower_kick" => s3_follower_kick::run().await,
        "s3_leader_solo" => s3_leader_solo::run().await,
        "s3_lease_monotonicity" => s3_lease_monotonicity::run().await,
        "s3_network_partition" => s3_network_partition::run().await,
        "s3_old_leader_recovery" => s3_old_leader_recovery::run().await,
        "s3_reconvergence" => s3_reconvergence::run().await,
        "s3_stale_lease" => s3_stale_lease::run().await,
        "s3_unreachable_failover" => s3_unreachable_failover::run().await,
        "s3_writes_during_fencing" => s3_writes_during_fencing::run().await,
        "schema_bank_bench" => schema_bank_bench::run().await,
        "schema_failover" => schema_failover::run().await,
        "schema_follower_crash" => schema_follower_crash::run().await,
        "schema_old_leader_recovery" => schema_old_leader_recovery::run().await,
        "schema_validation" => schema_validation::run().await,
        "schema_zero_cache" => schema_zero_cache::run().await,
        "single" => single::run().await,
        "standalone_to_distributed" => standalone_to_distributed::run().await,
        "typed_operations" => typed_operations::run().await,
        "watch_test" => watch_test::run().await,
        _ => {
            eprintln!("unknown test: {name}");
            std::process::exit(1);
        }
    }
}

fn build_filter(cli: &Cli) -> Result<TestFilter, String> {
    let mut filter = TestFilter {
        distributed: cli.distributed,
        standalone: cli.standalone,
        test: cli.test.clone(),
        ..Default::default()
    };

    if !cli.include.is_empty() {
        filter.include = parse_categories(&cli.include.join(","))?;
    }
    if !cli.include_or.is_empty() {
        filter.include_or = parse_categories(&cli.include_or.join(","))?;
    }
    if !cli.exclude.is_empty() {
        filter.exclude = parse_categories(&cli.exclude.join(","))?;
    }
    if !cli.exclude_or.is_empty() {
        filter.exclude_or = parse_categories(&cli.exclude_or.join(","))?;
    }

    Ok(filter)
}

fn print_test_list(selected: &[&celeriant_integration_tests::registry::TestEntry]) {
    let est_total: u32 = selected.iter().map(|t| t.estimated_secs).sum();
    println!(
        "{} tests selected (est. {}m {}s)\n",
        selected.len(),
        est_total / 60,
        est_total % 60,
    );
    for t in selected {
        let mode = if t.distributed { "distributed" } else { "standalone" };
        let cats: Vec<&str> = t.categories.iter().map(|c| c.as_str()).collect();
        println!(
            "  {:<45} {:>4}s  {:<12} [{}]",
            t.name, t.estimated_secs, mode, cats.join(", "),
        );
    }
}

fn run_tests(
    tests: &[&celeriant_integration_tests::registry::TestEntry],
    timeout_override: Option<u32>,
) {
    let est_total: u32 = tests.iter().map(|t| t.estimated_secs).sum();
    println!(
        "Running {} tests (est. {}m {}s)\n",
        tests.len(),
        est_total / 60,
        est_total % 60,
    );

    let self_exe = std::env::current_exe().expect("cannot resolve own executable path");

    let mut passed = 0u32;
    let mut failed = 0u32;
    let mut timed_out = 0u32;
    let mut is_first = true;

    let suite_start = Instant::now();

    for test in tests {
        // Allow previous test's servers to fully tear down
        if !is_first {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        is_first = false;

        let timeout = timeout_override.unwrap_or_else(|| test.timeout_secs());
        let log_path = format!("/tmp/celeriant_test_{}.log", test.name);

        print!("{:<50} ", test.name);

        // Write output to temp file to avoid pipe buffer deadlocks
        let log_file = match File::create(&log_path) {
            Ok(f) => f,
            Err(e) => {
                println!("ERROR  (cannot create log: {e})");
                failed += 1;
                continue;
            }
        };
        let stderr_file = match log_file.try_clone() {
            Ok(f) => f,
            Err(e) => {
                println!("ERROR  (cannot clone log handle: {e})");
                failed += 1;
                continue;
            }
        };

        let start = Instant::now();
        let result = Command::new(&self_exe)
            .args(["--run-test", test.name])
            .stdout(log_file)
            .stderr(stderr_file)
            .spawn();

        match result {
            Ok(mut child) => {
                let timeout_dur = std::time::Duration::from_secs(timeout as u64);
                match child.wait_timeout(timeout_dur) {
                    Ok(Some(status)) => {
                        let elapsed = start.elapsed();
                        if status.success() {
                            println!("PASS  ({:.1}s)", elapsed.as_secs_f64());
                            passed += 1;
                        } else {
                            println!(
                                "FAIL  (exit {}, {:.1}s)",
                                status.code().unwrap_or(-1),
                                elapsed.as_secs_f64(),
                            );
                            print_log_tail(&log_path, 5);
                            failed += 1;
                        }
                    }
                    Ok(None) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        println!("TIMEOUT  ({}s)", timeout);
                        timed_out += 1;
                    }
                    Err(e) => {
                        println!("ERROR  ({e})");
                        failed += 1;
                    }
                }
            }
            Err(e) => {
                println!("ERROR  (spawn failed: {e})");
                failed += 1;
            }
        }
    }

    let suite_elapsed = suite_start.elapsed();
    println!();
    println!("=== Summary ===");
    println!(
        "Passed: {}  Failed: {}  Timed out: {}  Total: {}  ({:.0}s)",
        passed, failed, timed_out, tests.len(), suite_elapsed.as_secs_f64(),
    );
    println!("Logs: /tmp/celeriant_test_*.log");

    if failed + timed_out > 0 {
        std::process::exit(1);
    }
}

fn print_log_tail(path: &str, n: usize) {
    use std::io::BufRead;
    let Ok(file) = File::open(path) else { return };
    let lines: Vec<String> = std::io::BufReader::new(file)
        .lines()
        .filter_map(|l| l.ok())
        .collect();
    let skip = lines.len().saturating_sub(n);
    for line in lines.into_iter().skip(skip) {
        println!("  {line}");
    }
}

/// Copy from `reader` to both `terminal` and `log_file` simultaneously.
fn tee(reader: impl std::io::Read, mut terminal: impl std::io::Write, mut log_file: File) {
    use std::io::Write;
    let mut buf = [0u8; 8192];
    let mut reader = reader;
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let _ = terminal.write_all(&buf[..n]);
                let _ = log_file.write_all(&buf[..n]);
            }
            Err(_) => break,
        }
    }
}
