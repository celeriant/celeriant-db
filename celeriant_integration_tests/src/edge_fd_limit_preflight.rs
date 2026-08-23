//! The startup fd pre-flight (celeriant_runtimes::fd_limits) under real rlimits.
//!
//! 1. Hard NOFILE lowered below the requirement in a wrapper shell: the server
//!    must exit non-zero before serving, naming the formula's numbers.
//! 2. Only soft lowered (hard left alone): the server must raise soft to hard
//!    in-process and keep booting.
//!
//! Limits are only ever lowered in a child shell, never in this process.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::{ServerConfig, ServerConfigExt, TestServer};

const WRAP_HARD: &str = "ulimit -S -n 512; ulimit -H -n 512; exec \"$0\" \"$@\"";
const WRAP_SOFT: &str = "ulimit -S -n 512; exec \"$0\" \"$@\"";

fn spawn_wrapped(wrap: &str, port: u16, data_root: &std::path::Path) -> std::process::Child {
    let config = ServerConfig {
        num_shards: Some(2),
        max_open_files: 1000,
        standalone: true,
        log_level: "info".to_string(),
        data_root: data_root.to_path_buf(),
        client_port: port,
        replication_port: port + 1,
        metrics_port: port + 2,
        ..Default::default()
    };
    let bin = TestServer::server_binary_path();
    Command::new("sh")
        .arg("-c")
        .arg(wrap)
        .arg(&bin)
        .args(config.to_cli_args())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn wrapped server")
}

/// Wait for `needle` on the child's stdout (where tracing writes), up to `timeout`.
/// Returns the lines seen.
fn wait_for_line(child: &mut std::process::Child, needle: &str, timeout: Duration) -> (bool, Vec<String>) {
    let stdout = child.stdout.take().expect("stdout piped");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + timeout;
    let mut seen = Vec::new();
    loop {
        let Some(left) = deadline.checked_duration_since(Instant::now()) else {
            return (false, seen);
        };
        match rx.recv_timeout(left) {
            Ok(line) => {
                let hit = line.contains(needle);
                seen.push(line);
                if hit {
                    return (true, seen);
                }
            }
            Err(_) => return (false, seen),
        }
    }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let port = 10200 + (std::process::id() % 100) as u16;

    println!("Case 1: hard NOFILE 512 < required -> startup fails with the formula");
    let dir1 = tempfile::TempDir::new()?;
    let mut child = spawn_wrapped(WRAP_HARD, port, dir1.path());
    let (found, lines) = wait_for_line(&mut child, "RLIMIT_NOFILE hard limit", Duration::from_secs(30));
    if !found {
        child.kill().ok();
        child.wait().ok();
        return Err(format!("expected NofileInsufficient message on stdout; got:\n{}", lines.join("\n")).into());
    }
    let status = child.wait()?;
    if status.success() {
        return Err("server exited 0 despite insufficient hard NOFILE".into());
    }
    if lines.iter().any(|l| l.contains("Metrics server listening")) {
        return Err("server started binding services before the fd pre-flight vetoed it".into());
    }
    let msg = lines.iter().find(|l| l.contains("RLIMIT_NOFILE hard limit")).unwrap();
    for number in ["512", "2 shards", "1000 max_open_files"] {
        if !msg.contains(number) {
            return Err(format!("error message missing '{number}': {msg}").into());
        }
    }
    println!("  failed fast before pool build, non-zero exit, message names the numbers");

    println!("Case 2: soft 512, hard untouched -> server raises soft and keeps booting");
    let dir2 = tempfile::TempDir::new()?;
    let mut child = spawn_wrapped(WRAP_SOFT, port + 3, dir2.path());
    let (found, lines) = wait_for_line(&mut child, "Raised soft RLIMIT_NOFILE", Duration::from_secs(30));
    if !found {
        child.kill().ok();
        child.wait().ok();
        return Err(format!("expected soft-raise log line; got:\n{}", lines.join("\n")).into());
    }
    // The log line alone proves nothing: read the kernel's view back. `sh` exec'd
    // the server, so the child pid IS the server pid.
    let limits = std::fs::read_to_string(format!("/proc/{}/limits", child.id()))?;
    let nofile_line = limits
        .lines()
        .find(|l| l.starts_with("Max open files"))
        .ok_or("no Max open files row in /proc/<pid>/limits")?
        .to_string();
    let fields: Vec<&str> = nofile_line.split_whitespace().collect();
    let (Some(&soft), Some(&hard)) = (fields.get(3), fields.get(4)) else {
        return Err(format!("unparseable limits row: {nofile_line}").into());
    };
    if soft == "512" || soft != hard {
        child.kill().ok();
        child.wait().ok();
        return Err(format!("kernel soft limit not raised to hard: {nofile_line}").into());
    }
    if child.try_wait()?.is_some() {
        return Err("server exited after raising the soft limit instead of booting".into());
    }
    child.kill().ok();
    child.wait().ok();
    println!("  kernel confirms soft {soft} == hard {hard}, server still booting");

    Ok(())
}
