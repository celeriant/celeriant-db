use std::path::Path;

use hex::ToHex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const FILE_NAME: &str = "server_meta.toml";
const DICT_FILE_NAME: &str = "dictionary.zstd_dict";

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompressionMeta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionary_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionary_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerMeta {
    pub num_shards: u32,
    pub timestamp_precision: String,
    pub timestamp_epoch_offset_secs: i64,
    pub routing_rule: String,
    #[serde(default)]
    pub reserve_coordinator_shard: bool,
    #[serde(default)]
    pub compression: CompressionMeta,
}

fn sha256_of_file(path: &Path) -> Result<String, String> {
    let bytes =
        std::fs::read(path).map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
    let digest = Sha256::digest(&bytes);
    Ok(digest.encode_hex::<String>())
}

fn sha256_of_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.encode_hex::<String>()
}

/// Validate that immutable config hasn't changed since initial setup.
///
/// On first startup (no `server_meta.toml`), persists the current config and handles the
/// first-boot decision table for the dict file.
/// On subsequent startups, compares and returns an error describing any mismatches.
///
/// Every cluster always uses ZstdDict. `built_in_resolver` maps a dict name to built-in
/// bytes; `None` means no built-in is available.
///
/// First-boot decision table:
/// - M absent + D absent → materialize built-in (or fail if not a built-in), write M.
/// - M absent + D present → adopt the pre-staged dict, write M with its sha.
/// - M present + D absent → rehydrate from built-in if sha matches, else fail.
/// - M present + D present → verify sha matches M, OK.
/// All other states (sha mismatch) are hard errors.
pub fn validate_or_create(
    data_root: &Path,
    current: &ServerMeta,
    desired_compression: &CompressionMeta,
    built_in_resolver: Option<fn(&str) -> Option<&'static [u8]>>,
) -> Result<(), String> {
    let meta_path = data_root.join(FILE_NAME);
    let dict_path = data_root.join(DICT_FILE_NAME);

    let meta_exists = meta_path.exists();
    let dict_exists = dict_path.exists();

    let dict_name = desired_compression
        .dictionary_name
        .as_deref()
        .ok_or("dictionary_name is required")?;

    match (meta_exists, dict_exists) {
        (false, false) => {
            // M absent, D absent: materialize from built-in or fail.
            match built_in_resolver.and_then(|r| r(dict_name)) {
                Some(builtin_bytes) => {
                    std::fs::write(&dict_path, builtin_bytes).map_err(|e| {
                        format!("Failed to write {}: {e}", dict_path.display())
                    })?;
                }
                None => {
                    return Err(format!(
                        "dict '{dict_name}' is not a built-in and no file present at '{}'",
                        dict_path.display()
                    ));
                }
            }
            // Fall through to write M.
        }
        (false, true) => {
            // M absent, D present: adopt the pre-staged dict; sha will be stored in M below.
        }
        (true, false) => {
            // M present, D absent: rehydrate from built-in if sha matches, else fail.
            let saved_content = std::fs::read_to_string(&meta_path)
                .map_err(|e| format!("Failed to read {FILE_NAME}: {e}"))?;
            let saved: ServerMeta = toml::from_str(&saved_content)
                .map_err(|e| format!("Failed to parse {FILE_NAME}: {e}"))?;

            let saved_name = saved.compression.dictionary_name.as_deref().unwrap_or("");
            let saved_sha = saved.compression.dictionary_sha256.as_deref().unwrap_or("");

            let builtin_bytes = built_in_resolver
                .and_then(|r| r(saved_name))
                .ok_or_else(|| format!(
                    "dict '{saved_name}' is missing from '{}' and is not a built-in; \
                     cannot rehydrate — restore the dict file from backup",
                    dict_path.display()
                ))?;

            let builtin_sha = sha256_of_bytes(builtin_bytes);
            if builtin_sha != saved_sha {
                return Err(format!(
                    "dict '{saved_name}' is missing from '{}' and the sha of the built-in \
                     ({builtin_sha}) does not match the persisted sha ({saved_sha}); \
                     refusing to start because proceeding would corrupt data — \
                     restore the dict file from backup",
                    dict_path.display()
                ));
            }

            std::fs::write(&dict_path, builtin_bytes)
                .map_err(|e| format!("Failed to write {}: {e}", dict_path.display()))?;

            // M and D are now consistent; run the normal validation path.
            return validate_existing_meta(data_root, &meta_path, current, desired_compression);
        }
        (true, true) => {
            // M present, D present: full validation.
            return validate_existing_meta(data_root, &meta_path, current, desired_compression);
        }
    }

    // M was absent (both absent or absent+present). Write M now.
    let sha = sha256_of_file(&dict_path)?;
    let meta_to_write = ServerMeta {
        compression: CompressionMeta {
            level: desired_compression.level,
            dictionary_name: Some(dict_name.to_string()),
            dictionary_sha256: Some(sha),
        },
        ..current.clone()
    };
    let content = toml::to_string_pretty(&meta_to_write)
        .map_err(|e| format!("Failed to serialize {FILE_NAME}: {e}"))?;
    std::fs::write(&meta_path, content)
        .map_err(|e| format!("Failed to write {FILE_NAME}: {e}"))?;
    Ok(())
}

/// Compare current config against the persisted `server_meta.toml` and return any mismatches.
fn validate_existing_meta(
    _data_root: &Path,
    meta_path: &Path,
    current: &ServerMeta,
    desired_compression: &CompressionMeta,
) -> Result<(), String> {
    let content = std::fs::read_to_string(meta_path)
        .map_err(|e| format!("Failed to read {FILE_NAME}: {e}"))?;
    let saved: ServerMeta =
        toml::from_str(&content).map_err(|e| format!("Failed to parse {FILE_NAME}: {e}"))?;

    let mut mismatches = Vec::new();

    if saved.num_shards != current.num_shards {
        mismatches.push(format!(
            "num_shards: saved={}, configured={}",
            saved.num_shards, current.num_shards
        ));
    }
    if saved.timestamp_precision != current.timestamp_precision {
        mismatches.push(format!(
            "timestamp_precision: saved={}, configured={}",
            saved.timestamp_precision, current.timestamp_precision
        ));
    }
    if saved.timestamp_epoch_offset_secs != current.timestamp_epoch_offset_secs {
        mismatches.push(format!(
            "timestamp_epoch_offset_secs: saved={}, configured={}",
            saved.timestamp_epoch_offset_secs, current.timestamp_epoch_offset_secs
        ));
    }
    if saved.routing_rule != current.routing_rule {
        mismatches.push(format!(
            "routing_rule: saved={}, configured={}",
            saved.routing_rule, current.routing_rule
        ));
    }
    if saved.reserve_coordinator_shard != current.reserve_coordinator_shard {
        mismatches.push(format!(
            "reserve_coordinator_shard: saved={}, configured={}",
            saved.reserve_coordinator_shard, current.reserve_coordinator_shard
        ));
    }

    // Compression fields
    if saved.compression.level != desired_compression.level {
        mismatches.push(format!(
            "compression.level: saved={:?}, configured={:?}",
            saved.compression.level, desired_compression.level
        ));
    }

    let configured_name = desired_compression.dictionary_name.as_deref().unwrap_or("");
    let saved_name = saved.compression.dictionary_name.as_deref().unwrap_or("");
    if configured_name != saved_name {
        mismatches.push(format!(
            "compression.dictionary_name: saved={saved_name:?}, configured={configured_name:?}"
        ));
    }

    // sha mismatch check: compare persisted sha against on-disk dict bytes
    if let Some(ref saved_sha) = saved.compression.dictionary_sha256 {
        let dict_path = meta_path.parent().unwrap().join(DICT_FILE_NAME);
        match sha256_of_file(&dict_path) {
            Ok(actual_sha) => {
                if &actual_sha != saved_sha {
                    return Err(format!(
                        "compression.dictionary_sha256 mismatch for dict '{saved_name}': \
                         persisted sha={saved_sha}, actual on-disk sha={actual_sha}; \
                         refusing to start because proceeding would corrupt data"
                    ));
                }
            }
            Err(e) => {
                return Err(format!(
                    "Cannot verify dict sha for '{saved_name}': {e}"
                ));
            }
        }
    } else {
        mismatches.push(
            "compression.dictionary_sha256: saved=None (required)".to_string(),
        );
    }

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Immutable configuration in {FILE_NAME} does not match current settings. \
             These cannot be changed after initial setup.\n  {}",
            mismatches.join("\n  ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_meta() -> ServerMeta {
        ServerMeta {
            num_shards: 4,
            timestamp_precision: "milliseconds".to_string(),
            timestamp_epoch_offset_secs: 0,
            routing_rule: "aggregate_id".to_string(),
            reserve_coordinator_shard: false,
            compression: CompressionMeta::default(),
        }
    }

    fn zstd_dict_compression(name: &str) -> CompressionMeta {
        CompressionMeta {
            level: Some(3),
            dictionary_name: Some(name.to_string()),
            dictionary_sha256: None,
        }
    }

    fn builtin_resolver(name: &str) -> Option<&'static [u8]> {
        match name {
            "test-dict-v1" => Some(b"fake dict bytes for testing"),
            _ => None,
        }
    }

    // -------------------------------------------------------------------------
    // Existing tests
    // -------------------------------------------------------------------------

    #[test]
    fn first_startup_creates_file() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        let path = dir.path().join(FILE_NAME);
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        let saved: ServerMeta = toml::from_str(&content).unwrap();
        assert_eq!(saved.num_shards, test_meta().num_shards);
        assert!(saved.compression.dictionary_sha256.is_some());
    }

    #[test]
    fn matching_config_passes() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();
    }

    #[test]
    fn changed_shard_count_fails() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        let mut changed = test_meta();
        changed.num_shards = 8;
        let err = validate_or_create(dir.path(), &changed, &desired, Some(builtin_resolver)).unwrap_err();
        assert!(err.contains("num_shards: saved=4, configured=8"));
    }

    #[test]
    fn changed_timestamp_precision_fails() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        let mut changed = test_meta();
        changed.timestamp_precision = "nanoseconds".to_string();
        let err = validate_or_create(dir.path(), &changed, &desired, Some(builtin_resolver)).unwrap_err();
        assert!(err.contains("timestamp_precision"));
    }

    #[test]
    fn changed_epoch_offset_fails() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        let mut changed = test_meta();
        changed.timestamp_epoch_offset_secs = 1_000_000;
        let err = validate_or_create(dir.path(), &changed, &desired, Some(builtin_resolver)).unwrap_err();
        assert!(err.contains("timestamp_epoch_offset_secs"));
    }

    #[test]
    fn changed_routing_rule_fails() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        let mut changed = test_meta();
        changed.routing_rule = "org_id".to_string();
        let err = validate_or_create(dir.path(), &changed, &desired, Some(builtin_resolver)).unwrap_err();
        assert!(err.contains("routing_rule"));
    }

    #[test]
    fn multiple_changes_reported() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        let changed = ServerMeta {
            num_shards: 8,
            timestamp_precision: "nanoseconds".to_string(),
            timestamp_epoch_offset_secs: 100,
            routing_rule: "org_id".to_string(),
            reserve_coordinator_shard: false,
            compression: CompressionMeta::default(),
        };
        let err = validate_or_create(dir.path(), &changed, &desired, Some(builtin_resolver)).unwrap_err();
        assert!(err.contains("num_shards"));
        assert!(err.contains("timestamp_precision"));
        assert!(err.contains("timestamp_epoch_offset_secs"));
        assert!(err.contains("routing_rule"));
    }

    // -------------------------------------------------------------------------
    // CompressionMeta round-trip
    // -------------------------------------------------------------------------

    #[test]
    fn compression_meta_round_trips_through_toml() {
        let meta = CompressionMeta {
            level: Some(3),
            dictionary_name: Some("json-web-events-v1".to_string()),
            dictionary_sha256: Some("abc123def456".to_string()),
        };
        let serialized = toml::to_string_pretty(&meta).unwrap();
        let deserialized: CompressionMeta = toml::from_str(&serialized).unwrap();
        assert_eq!(meta, deserialized);
    }

    #[test]
    fn compression_meta_default_round_trips_without_optional_fields() {
        let meta = CompressionMeta::default();
        let serialized = toml::to_string_pretty(&meta).unwrap();
        assert!(!serialized.contains("level"));
        assert!(!serialized.contains("dictionary_name"));
        assert!(!serialized.contains("dictionary_sha256"));
        let deserialized: CompressionMeta = toml::from_str(&serialized).unwrap();
        assert_eq!(meta, deserialized);
    }

    // -------------------------------------------------------------------------
    // First-boot decision table
    // -------------------------------------------------------------------------

    /// Row 1: M absent, D absent, name resolves to built-in → materialize D, write M.
    #[test]
    fn validate_or_create_no_meta_no_dict_with_builtin() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");

        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        // Dict file must have been written.
        let dict_path = dir.path().join(DICT_FILE_NAME);
        assert!(dict_path.exists());
        assert_eq!(
            std::fs::read(&dict_path).unwrap(),
            b"fake dict bytes for testing"
        );

        // Meta must record sha and name.
        let meta_path = dir.path().join(FILE_NAME);
        let content = std::fs::read_to_string(&meta_path).unwrap();
        let saved: ServerMeta = toml::from_str(&content).unwrap();
        assert_eq!(
            saved.compression.dictionary_name.as_deref(),
            Some("test-dict-v1")
        );
        assert!(saved.compression.dictionary_sha256.is_some());
    }

    /// Row 1: M absent, D absent, name not a built-in, no resolver → hard exit error.
    #[test]
    fn validate_or_create_no_meta_no_dict_unknown_name_fails() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("unknown-dict");

        let err =
            validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver))
                .unwrap_err();
        assert!(
            err.contains("unknown-dict"),
            "error should name the dict: {err}"
        );
        assert!(
            err.contains("not a built-in"),
            "error should say not a built-in: {err}"
        );
    }

    /// Row 2: M absent, D present → adopt the pre-staged dict (write M with sha).
    #[test]
    fn validate_or_create_no_meta_with_pre_staged_dict() {
        let dir = TempDir::new().unwrap();
        let dict_bytes = b"operator-supplied custom dict";
        let dict_path = dir.path().join(DICT_FILE_NAME);
        std::fs::write(&dict_path, dict_bytes).unwrap();

        let desired = zstd_dict_compression("my-custom-v1");
        validate_or_create(dir.path(), &test_meta(), &desired, None).unwrap();

        let meta_path = dir.path().join(FILE_NAME);
        let content = std::fs::read_to_string(&meta_path).unwrap();
        let saved: ServerMeta = toml::from_str(&content).unwrap();
        assert_eq!(
            saved.compression.dictionary_name.as_deref(),
            Some("my-custom-v1")
        );

        let expected_sha = sha256_of_bytes(dict_bytes);
        assert_eq!(saved.compression.dictionary_sha256.as_deref(), Some(expected_sha.as_str()));
    }

    /// Row 3: M present, D absent, name is a built-in and sha matches → rehydrate D.
    #[test]
    fn validate_or_create_with_meta_missing_dict_builtin_rehydrate() {
        let dir = TempDir::new().unwrap();

        // First boot: write M + D normally.
        let desired = zstd_dict_compression("test-dict-v1");
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        // Delete D to simulate partial backup restore.
        let dict_path = dir.path().join(DICT_FILE_NAME);
        std::fs::remove_file(&dict_path).unwrap();

        // Second boot: M present, D absent → rehydrate.
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();
        assert!(dict_path.exists());
        assert_eq!(
            std::fs::read(&dict_path).unwrap(),
            b"fake dict bytes for testing"
        );
    }

    /// Row 3: M present, D absent, name is not a built-in → hard exit.
    #[test]
    fn validate_or_create_with_meta_missing_dict_custom_fails() {
        let dir = TempDir::new().unwrap();

        // Write M manually claiming a custom dict name.
        let dict_bytes = b"some custom dict bytes";
        let dict_path = dir.path().join(DICT_FILE_NAME);
        std::fs::write(&dict_path, dict_bytes).unwrap();

        let desired = zstd_dict_compression("my-custom-dict");
        validate_or_create(dir.path(), &test_meta(), &desired, None).unwrap();

        // Now delete D.
        std::fs::remove_file(&dict_path).unwrap();

        // Boot again: M present, D absent, custom name not in built-in → fail.
        let err =
            validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver))
                .unwrap_err();
        assert!(
            err.contains("my-custom-dict"),
            "error should name the dict: {err}"
        );
    }

    /// Row 4: M present, D present, sha matches → OK.
    #[test]
    fn validate_or_create_with_meta_and_dict_sha_match() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");

        // First boot: creates both files.
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();
        // Second boot: M present, D present, sha should match.
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();
    }

    /// Row 4: M present, D present, sha mismatch → hard exit with meaningful message.
    #[test]
    fn validate_or_create_with_meta_and_dict_sha_mismatch_fails() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");

        // First boot.
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        // Tamper with the dict file.
        let dict_path = dir.path().join(DICT_FILE_NAME);
        std::fs::write(&dict_path, b"tampered bytes").unwrap();

        let err =
            validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver))
                .unwrap_err();
        assert!(
            err.contains("sha256"),
            "error should mention sha256: {err}"
        );
        assert!(
            err.contains("corrupt"),
            "error should refuse to start due to corruption risk: {err}"
        );
    }

    /// M present, D present, name in config differs from name in M → hard exit.
    #[test]
    fn validate_or_create_with_meta_and_dict_name_mismatch_fails() {
        let dir = TempDir::new().unwrap();
        let desired = zstd_dict_compression("test-dict-v1");

        // First boot under name "test-dict-v1".
        validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver)).unwrap();

        // Now configure a different name.
        let different_name = zstd_dict_compression("different-name");
        let err =
            validate_or_create(dir.path(), &test_meta(), &different_name, Some(builtin_resolver))
                .unwrap_err();
        assert!(
            err.contains("dictionary_name"),
            "error should mention dictionary_name: {err}"
        );
    }

    /// Verify missing dictionary_name fails clearly.
    #[test]
    fn validate_or_create_missing_name_fails() {
        let dir = TempDir::new().unwrap();
        let desired = CompressionMeta {
            level: Some(3),
            dictionary_name: None,
            dictionary_sha256: None,
        };
        let err = validate_or_create(dir.path(), &test_meta(), &desired, None).unwrap_err();
        assert!(err.contains("dictionary_name"));
    }

    /// Verify that a persisted server_meta.toml with no sha is rejected.
    #[test]
    fn validate_or_create_persisted_missing_sha_fails() {
        let dir = TempDir::new().unwrap();

        // Write a server_meta with no sha.
        let corrupt_meta = r#"
num_shards = 4
timestamp_precision = "milliseconds"
timestamp_epoch_offset_secs = 0
routing_rule = "aggregate_id"
reserve_coordinator_shard = false

[compression]
level = 3
dictionary_name = "test-dict-v1"
"#;
        std::fs::write(dir.path().join(FILE_NAME), corrupt_meta).unwrap();

        // Also write the dict file so we don't hit the "D absent" branch.
        std::fs::write(dir.path().join(DICT_FILE_NAME), b"some bytes").unwrap();

        let desired = zstd_dict_compression("test-dict-v1");
        let err =
            validate_or_create(dir.path(), &test_meta(), &desired, Some(builtin_resolver))
                .unwrap_err();
        assert!(
            err.contains("sha256") || err.contains("dictionary_sha256"),
            "error should mention sha256: {err}"
        );
    }
}
