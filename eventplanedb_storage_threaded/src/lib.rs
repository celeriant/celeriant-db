use std::{collections::HashMap, io, path::PathBuf, sync::Arc, thread, time::Duration};

use ahash::AHasher;
use core_affinity::CoreId;
use crossbeam::channel::{Receiver, Sender, unbounded};

use eventplanedb_storage_stateful::stateful_engine::{
    StatefulDestructive, StatefulEngine, StatefulEngineConfig, StatefulReader, StatefulWriter,
};
use eventplanedb_storage_structures::{
    event_batch_metadata::EventBatchMetadata, event_item::EventItem, read_filters::ReadFilters,
    read_result::ReadResult,
};
use futures::future::BoxFuture;
use std::hash::{Hash, Hasher};
use thiserror::Error;
use tokio::sync::oneshot;

pub mod threaded_engine;

pub use threaded_engine::ThreadedEngine;

#[derive(Error, Debug)]
pub enum ThreadedError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Thread communication error: {0}")]
    ThreadComm(String),
    #[error("Thread panic: {0}")]
    ThreadPanic(String),
}

type ThreadedResult<T> = Result<T, ThreadedError>;

/// Commands that can be sent to worker threads
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum WorkerCommand {
    AppendEvents {
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
        response_tx: oneshot::Sender<ThreadedResult<EventBatchMetadata>>,
    },
    ReadFiltered {
        aggregate_id: u128,
        filters: ReadFilters,
        response_tx: oneshot::Sender<ThreadedResult<ReadResult>>,
    },
    Exists {
        aggregate_id: u128,
        response_tx: oneshot::Sender<ThreadedResult<bool>>,
    },
    TrimStart {
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
        response_tx: oneshot::Sender<ThreadedResult<()>>,
    },
    Delete {
        aggregate_id: u128,
        response_tx: oneshot::Sender<ThreadedResult<()>>,
    },
    Shutdown,
}

/// Configuration for the threaded engine
#[derive(Debug, Clone)]
pub struct ThreadedEngineConfig {
    /// Number of worker threads to spawn
    /// If None, uses the number of CPU cores
    pub thread_count: Option<usize>,

    /// Pin threads to specific CPU cores
    pub pin_threads: bool,

    /// Base configuration for each StatefulEngine
    pub stateful_config: StatefulEngineConfig,

    /// Timeout for operations
    pub operation_timeout: Duration,
}

impl Default for ThreadedEngineConfig {
    fn default() -> Self {
        Self {
            thread_count: None,
            pin_threads: true,
            stateful_config: StatefulEngineConfig::default(),
            operation_timeout: Duration::from_secs(30),
        }
    }
}

impl ThreadedEngineConfig {
    pub fn with_base_path(base_path: PathBuf) -> Self {
        let mut config = Self::default();
        config.stateful_config.base_path = base_path;
        config
    }

    pub fn with_thread_count(mut self, count: usize) -> Self {
        self.thread_count = Some(count);
        self
    }

    pub fn with_pin_threads(mut self, pin: bool) -> Self {
        self.pin_threads = pin;
        self
    }

    pub fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }
}

/// Hash function for aggregate_id to determine thread assignment
fn hash_aggregate_id(aggregate_id: u128) -> u64 {
    let mut hasher = AHasher::default();
    aggregate_id.hash(&mut hasher);
    hasher.finish()
}

/// Worker thread that processes commands for a specific set of aggregates
struct Worker {
    thread_id: usize,
    engine: StatefulEngine,
    command_rx: Receiver<WorkerCommand>,
}

impl Worker {
    fn new(
        thread_id: usize,
        config: StatefulEngineConfig,
        command_rx: Receiver<WorkerCommand>,
    ) -> Self {
        let engine = StatefulEngine::new(config);
        Self {
            thread_id,
            engine,
            command_rx,
        }
    }

    fn run(self) {
        let Worker {
            thread_id: _, // We can ignore thread_id if not needed
            mut engine,   // We need mutable access to engine
            command_rx,   // Take ownership of the receiver
        } = self;

        for command in command_rx.iter() {
            if !Self::handle_command(&mut engine, command) {
                break; // Shutdown command received
            }
        }
    }

    fn handle_command(engine: &mut StatefulEngine, command: WorkerCommand) -> bool {
        match command {
            WorkerCommand::AppendEvents {
                aggregate_id,
                client_id,
                user_id,
                events,
                expected_event_batch_index,
                response_tx,
            } => {
                let result = engine.append_events(
                    aggregate_id,
                    client_id,
                    user_id,
                    events,
                    expected_event_batch_index,
                );
                let threaded_result = result.map_err(ThreadedError::from);
                let _ = response_tx.send(threaded_result);
                true
            }
            WorkerCommand::ReadFiltered {
                aggregate_id,
                filters,
                response_tx,
            } => {
                let result = engine.read_filtered(aggregate_id, &filters);
                let threaded_result = result.map_err(ThreadedError::from);
                let _ = response_tx.send(threaded_result);
                true
            }
            WorkerCommand::Exists {
                aggregate_id,
                response_tx,
            } => {
                let result = engine.exists(aggregate_id);
                let threaded_result = result.map_err(ThreadedError::from);
                let _ = response_tx.send(threaded_result);
                true
            }
            WorkerCommand::TrimStart {
                aggregate_id,
                keep_from_event_batch_index,
                response_tx,
            } => {
                let result = engine.trim_start(aggregate_id, keep_from_event_batch_index);
                let threaded_result = result.map_err(ThreadedError::from);
                let _ = response_tx.send(threaded_result);
                true
            }
            WorkerCommand::Delete {
                aggregate_id,
                response_tx,
            } => {
                let result = engine.delete(aggregate_id);
                let threaded_result = result.map_err(ThreadedError::from);
                let _ = response_tx.send(threaded_result);
                true
            }
            WorkerCommand::Shutdown => false, // Signal to stop the worker loop
        }
    }
}
