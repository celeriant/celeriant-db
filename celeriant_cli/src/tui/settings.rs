use std::path::PathBuf;

use directories::BaseDirs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectionSettings {
    pub server: String,
    pub seed_addresses: Vec<String>,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self {
            server: "127.0.0.1:10000".to_string(),
            seed_addresses: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsSettings {
    pub enabled: bool,
    pub ca_cert: String,
    pub client_cert: String,
    pub client_key: String,
    pub server_name: String,
}

impl Default for TlsSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_cert: String::new(),
            client_cert: String::new(),
            client_key: String::new(),
            server_name: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AuthSettings {
    pub api_key: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IdentityMode {
    #[default]
    Auto,
    Custom,
    None,
}

impl IdentityMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Custom => "custom",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IdentitySettings {
    pub mode: IdentityMode,
    pub public_key: String,
    pub private_key: String,
}

impl Default for IdentitySettings {
    fn default() -> Self {
        Self {
            mode: IdentityMode::Auto,
            public_key: String::new(),
            private_key: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PoolSettings {
    pub max_connections_per_node: u32,
    pub connection_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

impl Default for PoolSettings {
    fn default() -> Self {
        Self {
            max_connections_per_node: 4,
            connection_timeout_ms: 5000,
            request_timeout_ms: 30000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingSettings {
    pub route_reads_to_followers: bool,
    pub max_leader_retries: u32,
}

impl Default for RoutingSettings {
    fn default() -> Self {
        Self {
            route_reads_to_followers: false,
            max_leader_retries: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionSettings {
    pub enabled: bool,
    pub auto_threshold_bytes: u64,
}

impl Default for CompressionSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            auto_threshold_bytes: 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Settings {
    pub connection: ConnectionSettings,
    pub tls: TlsSettings,
    pub auth: AuthSettings,
    pub identity: IdentitySettings,
    pub pool: PoolSettings,
    pub routing: RoutingSettings,
    pub compression: CompressionSettings,
}

impl Settings {
    pub fn settings_path() -> PathBuf {
        BaseDirs::new()
            .map(|d| d.home_dir().join(".celeriant").join("settings.toml"))
            .unwrap_or_else(|| PathBuf::from(".celeriant/settings.toml"))
    }

    pub fn load() -> Self {
        let path = Self::settings_path();
        match std::fs::read_to_string(&path) {
            Ok(text) => toml::from_str(&text).unwrap_or_else(|e| {
                eprintln!("Warning: failed to parse {}: {e}", path.display());
                Self::default()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => {
                eprintln!("Warning: failed to read {}: {e}", path.display());
                Self::default()
            }
        }
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let path = Self::settings_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let text = toml::to_string_pretty(self)?;
        std::fs::write(&path, text)?;
        Ok(())
    }
}
