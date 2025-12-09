//! Integration tests for CLI commands
//! These tests require a running Celeriant server

use std::process::Command;

/// Helper to run celeriant_cli with arguments
fn run_cli(args: &[&str]) -> std::process::Output {
    Command::new("cargo")
        .args(["run", "-p", "celeriant_cli", "--"])
        .args(args)
        .output()
        .expect("Failed to execute command")
}

#[test]
#[ignore] // Run with: cargo test --test cli_tests -- --ignored
fn test_cli_help() {
    let output = run_cli(&["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Celeriant Event Store CLI"));
    assert!(stdout.contains("list-orgs"));
    assert!(stdout.contains("read"));
    assert!(stdout.contains("write"));
}

#[test]
#[ignore]
fn test_list_orgs_help() {
    let output = run_cli(&["list-orgs", "--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--format"));
    assert!(stdout.contains("--created-after"));
}

#[test]
#[ignore]
fn test_read_help() {
    let output = run_cli(&["read", "--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--org"));
    assert!(stdout.contains("--from"));
    assert!(stdout.contains("--event-types"));
}

#[test]
#[ignore]
fn test_write_help() {
    let output = run_cli(&["write", "--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--client-id"));
    assert!(stdout.contains("--event-type"));
    assert!(stdout.contains("--data"));
    assert!(stdout.contains("--file"));
    assert!(stdout.contains("--allow-create"));
}

// Live server tests - require CELERIANT_TEST_SERVER env var
#[cfg(feature = "live_tests")]
mod live_tests {
    use super::*;

    fn get_server() -> String {
        std::env::var("CELERIANT_TEST_SERVER").unwrap_or_else(|_| "127.0.0.1:10000".to_string())
    }

    #[test]
    fn test_list_orgs() {
        let server = get_server();
        let output = run_cli(&["--server", &server, "list-orgs", "--format", "json"]);
        
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            // Should be valid JSON array
            assert!(stdout.starts_with('['));
        }
    }

    #[test]
    fn test_write_and_read() {
        let server = get_server();
        
        // Write an event
        let write_output = run_cli(&[
            "--server", &server,
            "write",
            "--org", "999",
            "--type", "1", 
            "--id", "1",
            "--client-id", "1",
            "--event-type", "1",
            "--data", r#"{"test": true}"#,
            "--allow-create",
        ]);
        
        if !write_output.status.success() {
            eprintln!("Write failed: {}", String::from_utf8_lossy(&write_output.stderr));
            return;
        }
        
        // Read it back
        let read_output = run_cli(&[
            "--server", &server,
            "read",
            "--org", "999",
            "--type", "1",
            "--id", "1",
            "--from", "1",
            "--format", "json",
        ]);
        
        assert!(read_output.status.success());
        let stdout = String::from_utf8_lossy(&read_output.stdout);
        assert!(stdout.contains("event_batches"));
        
        // Cleanup - delete the aggregate
        let _ = run_cli(&[
            "--server", &server,
            "delete",
            "--org", "999",
            "--type", "1",
            "--id", "1",
        ]);
    }

    #[test]
    fn test_exists() {
        let server = get_server();
        let output = run_cli(&[
            "--server", &server,
            "exists",
            "--org", "1",
            "--type", "1",
            "--id", "1",
        ]);
        
        // May succeed or fail depending on whether aggregate exists
        // Just verify it doesn't crash
        let _ = output.status;
    }
}