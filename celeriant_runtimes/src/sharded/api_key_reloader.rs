use std::cell::Cell;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use serde::Deserialize;
use tracing::{info, warn};

use crate::sharded::shard_config::ApiKeyHashes;

/// TOML structure matching api_keys.toml
#[derive(Deserialize)]
struct ApiKeysToml {
    keys: KeysSection,
}

#[derive(Deserialize)]
struct KeysSection {
    primary_rw: String,
    secondary_rw: String,
    primary_ro: String,
    secondary_ro: String,
}

/// Watches api_keys.toml for mtime changes and rebuilds hashes on demand.
pub struct ApiKeyReloader {
    path: PathBuf,
    last_mtime: Cell<SystemTime>,
}

impl ApiKeyReloader {
    pub fn new(data_root: &PathBuf) -> Self {
        let path = data_root.join("api_keys.toml");
        let mtime = read_mtime(&path);
        Self {
            path,
            last_mtime: Cell::new(mtime),
        }
    }

    /// Check whether api_keys.toml has changed. If so, reload and return the new hashes.
    ///
    /// Returns `None` if no change was detected or if reload fails (a warning is logged).
    pub fn check_and_reload(&self) -> Option<Arc<ApiKeyHashes>> {
        let current = read_mtime(&self.path);
        if current == self.last_mtime.get() {
            return None;
        }

        info!(path = %self.path.display(), "api_keys.toml changed, reloading");

        match self.rebuild() {
            Ok(hashes) => {
                self.last_mtime.set(current);
                Some(Arc::new(hashes))
            }
            Err(e) => {
                warn!(error = %e, "API key reload failed, keeping existing config");
                None
            }
        }
    }

    fn rebuild(&self) -> Result<ApiKeyHashes, String> {
        let content = std::fs::read_to_string(&self.path)
            .map_err(|e| format!("read api_keys.toml: {e}"))?;

        let toml_config: ApiKeysToml = toml::from_str(&content)
            .map_err(|e| format!("parse api_keys.toml: {e}"))?;

        let primary_rw = parse_hex_hash(&toml_config.keys.primary_rw, "primary_rw")?;
        let secondary_rw = parse_hex_hash(&toml_config.keys.secondary_rw, "secondary_rw")?;
        let primary_ro = parse_hex_hash(&toml_config.keys.primary_ro, "primary_ro")?;
        let secondary_ro = parse_hex_hash(&toml_config.keys.secondary_ro, "secondary_ro")?;

        Ok(ApiKeyHashes {
            read_write: [primary_rw, secondary_rw],
            read_only: [primary_ro, secondary_ro],
        })
    }
}

/// Parse a 64-character hex string into a 32-byte array (SHA-256 hash)
fn parse_hex_hash(hex: &str, field_name: &str) -> Result<[u8; 32], String> {
    if hex.len() != 64 {
        return Err(format!(
            "{}: expected 64 hex characters, got {}",
            field_name,
            hex.len()
        ));
    }

    let mut hash = [0u8; 32];
    for i in 0..32 {
        let byte_hex = &hex[i * 2..i * 2 + 2];
        hash[i] = u8::from_str_radix(byte_hex, 16).map_err(|_| {
            format!("{}: invalid hex character in '{}'", field_name, byte_hex)
        })?;
    }

    Ok(hash)
}

/// Read the mtime of api_keys.toml. Uses `UNIX_EPOCH` as a fallback so that
/// an unreadable file does not mask future changes.
fn read_mtime(path: &PathBuf) -> SystemTime {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH)
}
