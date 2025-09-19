use std::{collections::HashMap, io, path::PathBuf, sync::Arc, time::Duration};

use ahash::AHasher;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    channels::{channel_mesh, shared_channel},
    net::{TcpListener, TcpStream},
    LocalExecutor, LocalExecutorBuilder, Placement,
};
use std::hash::{Hash, Hasher};
use thiserror::Error;

use eventplanedb_storage_stateful::stateful_engine::{StatefulEngine, StatefulEngineConfig};
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
    #[error("Server error: {0}")]
    Server(String),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

type GlommioResult<T> = Result<T, GlommioError>;

/// Hash function for aggregate_id to determine core assignment
fn hash_aggregate_id(aggregate_id: u128) -> u64 {
    let mut hasher = AHasher::default();
    aggregate_id.hash(&mut hasher);
    hasher.finish()
}
