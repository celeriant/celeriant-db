use std::fs;
use std::path::Path;
use std::time::Instant;

/// Warms XFS inode and directory metadata by walking the shard data directories.
///
/// With Direct I/O (O_DIRECT), data never touches the page cache — but filesystem
/// metadata (XFS inodes, directory entries, extent tree root nodes) always lives
/// there. After a restart, this metadata is cold and every `open()` / `stat()` /
/// `fallocate()` must fault it in from disk.
///
/// This function walks every shard directory and stats each WAL file. This is
/// purely metadata — no file data is read, no page cache is consumed for data,
/// and the cost is O(number of files) regardless of file sizes.
pub fn warm_fs_metadata(data_root: &Path) -> Result<(usize, u64), String> {
    let start = Instant::now();
    let mut file_count = 0usize;
    let mut total_bytes = 0u64;

    let entries = fs::read_dir(data_root)
        .map_err(|e| format!("Failed to read data_root {:?}: {}", data_root, e))?;

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if !path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("shard_"))
        {
            continue;
        }

        let shard_entries = match fs::read_dir(&path) {
            Ok(e) => e,
            Err(_) => continue,
        };

        for file_entry in shard_entries.flatten() {
            let file_path = file_entry.path();
            if file_path.extension().and_then(|e| e.to_str()) != Some("wal") {
                continue;
            }

            // stat() loads the inode into the VFS cache. open() loads the XFS
            // in-core extent list. Together these cover the metadata that every
            // subsequent O_DIRECT I/O operation needs.
            if let Ok(metadata) = fs::metadata(&file_path) {
                if let Ok(_file) = fs::File::open(&file_path) {
                    total_bytes += metadata.len();
                    file_count += 1;
                }
            }
        }
    }

    let elapsed = start.elapsed();
    tracing::info!(
        "Filesystem metadata warmup: {} WAL files across {:.1} GB in {:.1}ms",
        file_count,
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        elapsed.as_secs_f64() * 1000.0
    );

    Ok((file_count, total_bytes))
}
