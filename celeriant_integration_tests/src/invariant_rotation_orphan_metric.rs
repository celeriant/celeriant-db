//! Metrics invariant: orphan deletion during runtime log rotation reports on
//! its own counter, `celeriant_rotation_orphan_deleted_total`, not on the
//! boot-recovery series `celeriant_orphan_segment_recovered_total`
//! (metrics-orphan-counter-label-inconsistency). Before the fix both paths
//! shared the boot counter, so an operator watching "orphans recovered at
//! boot" saw increments from a server that never rebooted.
//!
//! The runtime path only runs when rotation finds its target file already on
//! disk with zeroed dual headers — the leftover of a prior aborted rotation.
//! The test plants that leftover while the server runs, then fills the 2MB
//! segment with 64KB incompressible events until rotation trips over it.

use celeriant_client_tokio::celeriant_client::CeleriantClient;

use crate::common::{R, port_for};
use crate::{ServerConfig, TestServer, scrape_counter, write_event, write_large_event};

const SEGMENT_BYTES: u64 = 2 * 1024 * 1024;
/// 40 x 64KB = 2.5MB comfortably overfills the 2MB segment.
const ROTATION_WRITES: u64 = 40;

const ROTATION_COUNTER: &str = "celeriant_rotation_orphan_deleted_total";
const BOOT_COUNTER: &str = "celeriant_orphan_segment_recovered_total";

pub async fn rotation_orphan_reports_on_its_own_counter() -> R {
    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        shard_log_preallocate_bytes: SEGMENT_BYTES,
        ..Default::default()
    };
    let server =
        TestServer::start_with_config(port_for("invariant_rotation_orphan_metric"), config).await?;

    // Open the shard's WAL first (first write), so the planted orphan is seen
    // by the runtime rotation path, not by the boot/cache-warmup scan.
    let mut client = CeleriantClient::connect(server.address()).await?;
    let key = crate::common::unique_key("invariant_rotation_orphan_metric");
    write_event(&mut client, &key, 1, true).await?;

    // Plant the aborted-rotation leftover: the next segment (log_2.wal) as a
    // zeroed file, in the single shard's directory.
    let mut shard_dirs: Vec<std::path::PathBuf> = std::fs::read_dir(&server.config().data_root)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .map(|n| n.to_string_lossy().starts_with("shard_"))
                    .unwrap_or(false)
        })
        .collect();
    if shard_dirs.len() != 1 {
        return Err(format!("expected exactly one shard_* dir, found {shard_dirs:?}").into());
    }
    let orphan_path = shard_dirs.remove(0).join("log_2.wal");
    std::fs::write(&orphan_path, vec![0u8; SEGMENT_BYTES as usize])?;

    // Fill log_1 past its 2MB preallocation so rotation targets log_2.
    for i in 2..=ROTATION_WRITES {
        write_large_event(&mut client, &key, i, 64 * 1024).await?;
    }

    let metrics_port = server.config().metrics_port;
    let rotation = scrape_counter("127.0.0.1", metrics_port, ROTATION_COUNTER).await?;
    let boot = scrape_counter("127.0.0.1", metrics_port, BOOT_COUNTER).await?;

    // Premise: the orphan-deletion path ran exactly once, on SOME counter.
    // Zero on both means rotation never happened and the test proves nothing.
    if rotation + boot != 1 {
        return Err(format!(
            "premise unmet: expected exactly one orphan deletion across both series, got \
             {ROTATION_COUNTER}={rotation} {BOOT_COUNTER}={boot} — did rotation run?"
        )
        .into());
    }
    if boot != 0 {
        return Err(format!(
            "runtime rotation deleted the planted orphan but reported it on the \
             boot-recovery series ({BOOT_COUNTER}={boot}); it must use {ROTATION_COUNTER}"
        )
        .into());
    }
    Ok(())
}
