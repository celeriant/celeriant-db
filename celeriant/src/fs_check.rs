use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

/// Verifies that `compaction_temp_dir` is on the same filesystem as `data_root`.
/// `rename(2)` is atomic only within a single filesystem; cross-device rename returns EXDEV.
pub fn verify_same_filesystem(data_root: &Path, compaction_temp_dir: &Path) -> Result<(), String> {
    fs::create_dir_all(compaction_temp_dir)
        .map_err(|e| format!("Failed to create compaction temp directory {:?}: {}", compaction_temp_dir, e))?;

    let data_dev = fs::metadata(data_root)
        .map_err(|e| format!("Failed to stat data_root {:?}: {}", data_root, e))?
        .dev();
    let tmp_dev = fs::metadata(compaction_temp_dir)
        .map_err(|e| format!("Failed to stat compaction_temp_dir {:?}: {}", compaction_temp_dir, e))?
        .dev();

    if data_dev != tmp_dev {
        return Err(format!(
            "compaction_temp_dir ({:?}, device {}) is on a different filesystem than data_root ({:?}, device {}). \
             Compaction uses atomic rename to swap log segments, which requires both paths on the same filesystem.",
            compaction_temp_dir, tmp_dev, data_root, data_dev
        ));
    }
    Ok(())
}
