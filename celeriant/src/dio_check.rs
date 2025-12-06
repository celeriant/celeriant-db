use std::fs::{self, OpenOptions};
use std::io::{Error, ErrorKind};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

/// Verifies that O_DIRECT is actually enforced on the given directory.
/// Returns Ok(()) if Direct I/O works, Err if it silently falls back to buffered I/O.
pub fn verify_direct_io(data_root: &Path) -> Result<(), String> {
    let test_file = data_root.join(".dio_check");

    // Ensure data_root exists
    fs::create_dir_all(data_root)
        .map_err(|e| format!("Failed to create data directory: {}", e))?;

    let result = perform_dio_check(&test_file);

    // Clean up test file
    let _ = fs::remove_file(&test_file);

    result
}

fn perform_dio_check(test_file: &Path) -> Result<(), String> {
    // Open file with O_DIRECT
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(test_file)
        .map_err(|e| format!("Failed to open test file with O_DIRECT: {}", e))?;

    let fd = file.as_raw_fd();

    // Intentionally unaligned: 41 bytes at offset 77
    // True O_DIRECT requires alignment (typically 512 or 4096 bytes)
    let buf = vec![0xDDu8; 41];
    let offset: libc::off_t = 77;

    let ret = unsafe {
        libc::pwrite(
            fd,
            buf.as_ptr() as *const libc::c_void,
            buf.len(),
            offset,
        )
    };

    if ret < 0 {
        let err = Error::last_os_error();
        if err.kind() == ErrorKind::InvalidInput {
            // EINVAL means O_DIRECT is properly enforced
            return Ok(());
        }
        return Err(format!("Unexpected error during Direct I/O check: {}", err));
    }

    // Write succeeded with unaligned data - O_DIRECT is being ignored
    Err(
        "Direct I/O (O_DIRECT) is not enforced on this filesystem. \
         The system silently falls back to buffered I/O. \
         This may occur with encrypted filesystems or certain configurations. \
         Celeriant requires true Direct I/O for correctness."
            .to_string(),
    )
}