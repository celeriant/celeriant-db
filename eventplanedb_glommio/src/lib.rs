use std::{io, path::PathBuf};

use ahash::AHasher;
use std::hash::{Hash, Hasher};
use thiserror::Error;

use eventplanedb_storage_stateful::stateful_engine::StatefulEngineConfig;
use eventplanedb_storage_structures::{
    event_batch_metadata::EventBatchMetadata, event_item::EventItem, read_filters::ReadFilters,
    read_result::ReadResult,
};

pub mod protocol;
pub mod server;

pub use protocol::{ProtocolError, Request, Response};
pub use server::{GlommioServer, GlommioServerConfig};

#[derive(Error, Debug)]
pub enum GlommioError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Server error: {0}")]
    Server(String),
}

type GlommioResult<T> = Result<T, GlommioError>;

/// Hash function for aggregate_id to determine shard assignment
pub fn hash_aggregate_id(aggregate_id: &str) -> u64 {
    let mut hasher = AHasher::default();
    aggregate_id.hash(&mut hasher);
    hasher.finish()
}

/// Configuration for the Glommio server
#[derive(Debug, Clone)]
pub struct GlommioServerConfig {
    /// Base path for storage
    pub base_path: PathBuf,
    /// Number of shards to use (None = all available CPUs)
    pub shard_count: Option<usize>,
    /// Bind address
    pub bind_addr: std::net::SocketAddr,
    /// StatefulEngine configuration
    pub stateful_config: StatefulEngineConfig,
}

impl Default for GlommioServerConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("./data"),
            shard_count: None,
            bind_addr: "127.0.0.1:8080".parse().unwrap(),
            stateful_config: StatefulEngineConfig::default(),
        }
    }
}

impl GlommioServerConfig {
    pub fn with_base_path(mut self, base_path: PathBuf) -> Self {
        self.base_path = base_path.clone();
        self.stateful_config.base_path = base_path;
        self
    }

    pub fn with_shard_count(mut self, count: usize) -> Self {
        self.shard_count = Some(count);
        self
    }

    pub fn with_bind_addr(mut self, addr: std::net::SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }
}
