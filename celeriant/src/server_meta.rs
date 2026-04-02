use std::path::Path;

use serde::{Deserialize, Serialize};

const FILE_NAME: &str = "server_meta.toml";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerMeta {
    pub num_shards: u32,
    pub timestamp_precision: String,
    pub timestamp_epoch_offset_secs: i64,
    pub routing_rule: String,
    #[serde(default)]
    pub reserve_coordinator_shard: bool,
}

/// Validate that immutable config hasn't changed since initial setup.
///
/// On first startup (no `server_meta.toml`), persists the current config.
/// On subsequent startups, compares and returns an error describing any mismatches.
pub fn validate_or_create(data_root: &Path, current: &ServerMeta) -> Result<(), String> {
    let path = data_root.join(FILE_NAME);

    if !path.exists() {
        let content = toml::to_string_pretty(current)
            .map_err(|e| format!("Failed to serialize {FILE_NAME}: {e}"))?;
        std::fs::write(&path, content)
            .map_err(|e| format!("Failed to write {FILE_NAME}: {e}"))?;
        return Ok(());
    }

    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read {FILE_NAME}: {e}"))?;
    let saved: ServerMeta = toml::from_str(&content)
        .map_err(|e| format!("Failed to parse {FILE_NAME}: {e}"))?;

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
        }
    }

    #[test]
    fn first_startup_creates_file() {
        let dir = TempDir::new().unwrap();
        let meta = test_meta();
        validate_or_create(dir.path(), &meta).unwrap();

        let path = dir.path().join(FILE_NAME);
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        let saved: ServerMeta = toml::from_str(&content).unwrap();
        assert_eq!(saved, meta);
    }

    #[test]
    fn matching_config_passes() {
        let dir = TempDir::new().unwrap();
        let meta = test_meta();
        validate_or_create(dir.path(), &meta).unwrap();
        validate_or_create(dir.path(), &meta).unwrap();
    }

    #[test]
    fn changed_shard_count_fails() {
        let dir = TempDir::new().unwrap();
        validate_or_create(dir.path(), &test_meta()).unwrap();

        let mut changed = test_meta();
        changed.num_shards = 8;
        let err = validate_or_create(dir.path(), &changed).unwrap_err();
        assert!(err.contains("num_shards: saved=4, configured=8"));
    }

    #[test]
    fn changed_timestamp_precision_fails() {
        let dir = TempDir::new().unwrap();
        validate_or_create(dir.path(), &test_meta()).unwrap();

        let mut changed = test_meta();
        changed.timestamp_precision = "nanoseconds".to_string();
        let err = validate_or_create(dir.path(), &changed).unwrap_err();
        assert!(err.contains("timestamp_precision"));
    }

    #[test]
    fn changed_epoch_offset_fails() {
        let dir = TempDir::new().unwrap();
        validate_or_create(dir.path(), &test_meta()).unwrap();

        let mut changed = test_meta();
        changed.timestamp_epoch_offset_secs = 1_000_000;
        let err = validate_or_create(dir.path(), &changed).unwrap_err();
        assert!(err.contains("timestamp_epoch_offset_secs"));
    }

    #[test]
    fn changed_routing_rule_fails() {
        let dir = TempDir::new().unwrap();
        validate_or_create(dir.path(), &test_meta()).unwrap();

        let mut changed = test_meta();
        changed.routing_rule = "org_id".to_string();
        let err = validate_or_create(dir.path(), &changed).unwrap_err();
        assert!(err.contains("routing_rule"));
    }

    #[test]
    fn multiple_changes_reported() {
        let dir = TempDir::new().unwrap();
        validate_or_create(dir.path(), &test_meta()).unwrap();

        let changed = ServerMeta {
            num_shards: 8,
            timestamp_precision: "nanoseconds".to_string(),
            timestamp_epoch_offset_secs: 100,
            routing_rule: "org_id".to_string(),
            reserve_coordinator_shard: false,
        };
        let err = validate_or_create(dir.path(), &changed).unwrap_err();
        assert!(err.contains("num_shards"));
        assert!(err.contains("timestamp_precision"));
        assert!(err.contains("timestamp_epoch_offset_secs"));
        assert!(err.contains("routing_rule"));
    }
}
