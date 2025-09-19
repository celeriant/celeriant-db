use crate::{
    ThreadedEngineConfig, ThreadedError, ThreadedResult, Worker, WorkerCommand, hash_aggregate_id,
};
use core_affinity::{CoreId, get_core_ids};
use crossbeam::channel::{Sender, unbounded};
use eventplanedb_storage_structures::{
    event_batch_metadata::EventBatchMetadata, event_item::EventItem, read_filters::ReadFilters,
    read_result::ReadResult,
};
use std::{sync::Arc, thread, time::Duration, usize};
use tokio::sync::oneshot;

/// Thread-safe, async wrapper around StatefulEngine
///
/// Routes operations to specific worker threads based on aggregate_id hash,
/// ensuring that operations on the same aggregate are serialized while
/// allowing concurrent operations on different aggregates.
///
/// # Important
/// You must call `shutdown()` before dropping this struct to ensure graceful cleanup.
pub struct ThreadedEngine {
    /// Senders for each worker thread
    workers: Vec<Sender<WorkerCommand>>,
    /// Number of worker threads
    thread_count: usize,
    /// Operation timeout
    operation_timeout: Duration,
    /// Thread handles for cleanup
    thread_handles: Vec<thread::JoinHandle<()>>,
}

impl ThreadedEngine {
    async fn execute_command<T, F>(
        &self,
        aggregate_id: u128,
        command_builder: F,
    ) -> ThreadedResult<T>
    where
        T: Send + 'static,
        F: FnOnce(oneshot::Sender<ThreadedResult<T>>) -> WorkerCommand,
    {
        let worker_index = self.get_worker_index(aggregate_id);
        let worker = &self.workers[worker_index];

        let (response_tx, response_rx) = oneshot::channel();
        let command = command_builder(response_tx);

        worker.send(command).map_err(|_| {
            ThreadedError::ThreadComm("Failed to send command to worker".to_string())
        })?;

        tokio::time::timeout(self.operation_timeout, response_rx)
            .await
            .map_err(|_| ThreadedError::ThreadComm("Operation timed out".to_string()))?
            .map_err(|_| ThreadedError::ThreadComm("Response channel closed".to_string()))?
    }

    /// Create a new ThreadedEngine with the given configuration
    pub fn new(config: ThreadedEngineConfig) -> Result<Self, ThreadedError> {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        let num_available_cores = core_ids.len().max(1); // At least 1 core

        let default_thread_count = num_available_cores;
        let thread_count = config
            .thread_count
            .unwrap_or(default_thread_count)
            .min(num_available_cores)
            .max(1); // At least 1 thread

        let mut workers = Vec::with_capacity(thread_count);
        let mut thread_handles = Vec::with_capacity(thread_count);

        for thread_id in 0..thread_count {
            let (command_tx, command_rx) = unbounded();

            let core_id = if config.pin_threads && !core_ids.is_empty() {
                Some(core_ids[thread_id % core_ids.len()])
            } else {
                None
            };

            let stateful_config = config.stateful_config.clone();

            let handle = thread::Builder::new()
                .name(format!("eventplane-worker-{thread_id}"))
                .spawn(move || {
                    // Set core affinity first if specified
                    if let Some(core_id) = core_id {
                        core_affinity::set_for_current(core_id);
                    }

                    let worker = Worker::new(thread_id, stateful_config, command_rx);

                    worker.run();
                })
                .map_err(|e| {
                    ThreadedError::ThreadComm(format!("Failed to spawn worker thread: {}", e))
                })?;

            workers.push(command_tx);
            thread_handles.push(handle);
        }

        Ok(Self {
            workers,
            thread_count,
            operation_timeout: config.operation_timeout,
            thread_handles,
        })
    }

    /// Create a ThreadedEngine with default configuration for the given base path
    pub fn with_default_config(base_path: std::path::PathBuf) -> Result<Self, ThreadedError> {
        let config = ThreadedEngineConfig::with_base_path(base_path);
        Self::new(config)
    }

    /// Get the worker thread index for a given aggregate_id
    fn get_worker_index(&self, aggregate_id: u128) -> usize {
        let hash = hash_aggregate_id(aggregate_id);
        (hash as usize) % self.thread_count
    }

    /// Append events to an aggregate
    pub async fn append_events(
        &self,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
    ) -> ThreadedResult<EventBatchMetadata> {
        self.execute_command(aggregate_id, |response_tx| WorkerCommand::AppendEvents {
            aggregate_id,
            client_id,
            user_id,
            events,
            expected_event_batch_index,
            response_tx,
        })
        .await
    }

    /// Read filtered events from an aggregate
    pub async fn read_filtered(
        &self,
        aggregate_id: u128,
        filters: ReadFilters,
    ) -> ThreadedResult<ReadResult> {
        self.execute_command(aggregate_id, |response_tx| WorkerCommand::ReadFiltered {
            aggregate_id,
            filters,
            response_tx,
        })
        .await
    }

    /// Check if an aggregate exists
    pub async fn exists(&self, aggregate_id: u128) -> ThreadedResult<bool> {
        self.execute_command(aggregate_id, |response_tx| WorkerCommand::Exists {
            aggregate_id,
            response_tx,
        })
        .await
    }

    /// Trim events from the start of an aggregate
    pub async fn trim_start(
        &self,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
    ) -> ThreadedResult<()> {
        self.execute_command(aggregate_id, |response_tx| WorkerCommand::TrimStart {
            aggregate_id,
            keep_from_event_batch_index,
            response_tx,
        })
        .await
    }

    /// Delete an aggregate
    pub async fn delete(&self, aggregate_id: u128) -> ThreadedResult<()> {
        self.execute_command(aggregate_id, |response_tx| WorkerCommand::Delete {
            aggregate_id,
            response_tx,
        })
        .await
    }

    /// Get the number of worker threads
    pub fn thread_count(&self) -> usize {
        self.thread_count
    }

    async fn join_worker_thread(handle: thread::JoinHandle<()>) -> ThreadedResult<()> {
        tokio::task::spawn_blocking(move || {
            handle.join().map_err(|_| {
                ThreadedError::ThreadPanic("Worker thread panicked during shutdown".to_string())
            })
        })
        .await
        .map_err(|e| ThreadedError::ThreadPanic(format!("Failed to join task: {}", e)))?
    }

    /// Shutdown the engine gracefully with default timeout
    ///
    /// This method should be called before dropping the engine to ensure
    /// all worker threads terminate cleanly.
    pub async fn shutdown(&mut self) -> ThreadedResult<()> {
        self.shutdown_with_timeout(Duration::from_secs(30)).await
    }

    pub async fn shutdown_with_timeout(&mut self, timeout: Duration) -> ThreadedResult<()> {
        // Send shutdown commands to all workers
        for worker in &self.workers {
            let _ = worker.send(WorkerCommand::Shutdown);
        }

        // Wait for all threads with timeout
        let handles = std::mem::take(&mut self.thread_handles);
        let shutdown_future = async move {
            for handle in handles {
                Self::join_worker_thread(handle).await?;
            }
            Ok::<(), ThreadedError>(())
        };

        tokio::time::timeout(timeout, shutdown_future)
            .await
            .map_err(|_| ThreadedError::ThreadComm("Shutdown timed out".to_string()))??;

        Ok(())
    }
}

impl Drop for ThreadedEngine {
    fn drop(&mut self) {
        // Send shutdown commands to all workers
        for worker in &self.workers {
            let _ = worker.send(WorkerCommand::Shutdown);
        }
        // Note: We can't wait for threads to join in Drop since it's not async
        // The threads will terminate when their command channels are closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventplanedb_storage_structures::event_item::EventItem;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::time::{Duration, timeout};

    fn create_test_events(start_index: u64, count: usize) -> Vec<EventItem> {
        (0..count)
            .map(|i| {
                EventItem::new(
                    start_index + i as u64,
                    start_index + i as u64,
                    1000 + i as u64,
                    42,
                    1,
                    format!("test event {}", i).into_bytes(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn test_high_concurrency_load() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let engine = Arc::new(ThreadedEngine::with_default_config(
            temp_dir.path().to_path_buf(),
        )?);

        let mut handles = Vec::new();

        // Test with high number of concurrent operations
        for i in 0..1000 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                let aggregate_id = (i % 100) as u128; // 100 different aggregates
                let events = create_test_events(i, 1);
                engine_clone
                    .append_events(aggregate_id, 100, None, events, None)
                    .await
            });
            handles.push(handle);
        }

        // Wait for all operations
        for handle in handles {
            handle.await??;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_basic_operations() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let mut engine = ThreadedEngine::with_default_config(temp_dir.path().to_path_buf())?;

        let aggregate_id = 123;
        let client_id = 100u128;
        let events = create_test_events(1, 3);

        // Test append
        let metadata = engine
            .append_events(aggregate_id, client_id, None, events, None)
            .await?;
        assert_eq!(metadata.event_batch_index, 0);

        // Test exists
        let exists = engine.exists(aggregate_id).await?;
        assert!(exists);

        // Test read
        let filters = ReadFilters::new(0);
        let result = engine.read_filtered(aggregate_id, filters).await?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 3);

        engine.shutdown_with_timeout(Duration::from_secs(3)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_aggregates_concurrent() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let engine = std::sync::Arc::new(ThreadedEngine::with_default_config(
            temp_dir.path().to_path_buf(),
        )?);

        // Create multiple concurrent operations on different aggregates
        let mut handles = Vec::new();

        for i in 0..10 {
            let engine_clone = engine.clone();
            let aggregate_id = i as u128;
            let handle = tokio::spawn(async move {
                let events = create_test_events(1, 2);
                engine_clone
                    .append_events(aggregate_id, 100, None, events, None)
                    .await
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            let result = handle.await??;
            assert_eq!(result.event_batch_index, 0);
        }

        // Verify all aggregates exist
        for i in 0..10 {
            let aggregate_id = i as u128;
            let exists = engine.exists(aggregate_id).await?;
            assert!(exists);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_same_aggregate_serialized() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let engine = std::sync::Arc::new(ThreadedEngine::with_default_config(
            temp_dir.path().to_path_buf(),
        )?);

        let aggregate_id = 123;

        // Create multiple concurrent operations on the same aggregate
        let mut handles = Vec::new();

        for i in 0..5 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                let events = create_test_events(i * 10 + 1, 2);
                engine_clone
                    .append_events(aggregate_id, 100, None, events, None)
                    .await
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        let mut batch_indices = Vec::new();
        for handle in handles {
            let result = handle.await??;
            batch_indices.push(result.event_batch_index);
        }

        // Batch indices should be sequential (0, 1, 2, 3, 4) even though operations were concurrent
        batch_indices.sort();
        assert_eq!(batch_indices, vec![0, 1, 2, 3, 4]);

        // Verify we can read all batches
        let filters = ReadFilters::new(0);
        let result = engine.read_filtered(aggregate_id, filters).await?;
        assert_eq!(result.event_batches.len(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_thread_assignment_consistency() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let config = ThreadedEngineConfig::with_base_path(temp_dir.path().to_path_buf())
            .with_thread_count(4);
        let mut engine = ThreadedEngine::new(config)?;

        // Same aggregate should always get assigned to the same thread
        let aggregate_id = 54321;
        let worker_index1 = engine.get_worker_index(aggregate_id);
        let worker_index2 = engine.get_worker_index(aggregate_id);
        let worker_index3 = engine.get_worker_index(aggregate_id);

        assert_eq!(worker_index1, worker_index2);
        assert_eq!(worker_index2, worker_index3);

        engine.shutdown_with_timeout(Duration::from_secs(3)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_operation_timeout() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let config = ThreadedEngineConfig::with_base_path(temp_dir.path().to_path_buf())
            .with_operation_timeout(Duration::from_millis(1)); // Very short timeout
        let mut engine = ThreadedEngine::new(config)?;

        let events = create_test_events(1, 1000); // Large number of events
        let result = engine
            .append_events(123, 100, None, events, None)
            .await;

        // Should either succeed (if fast enough) or timeout
        match result {
            Ok(_) => {} // Operation completed in time
            Err(ThreadedError::ThreadComm(msg)) if msg.contains("timed out") => {} // Expected timeout
            Err(e) => return Err(e.into()), // Unexpected error
        }

        engine.shutdown_with_timeout(Duration::from_secs(3)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_destructive_operations() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let mut engine = ThreadedEngine::with_default_config(temp_dir.path().to_path_buf())?;

        let aggregate_id = 99999;

        // Write multiple batches
        for i in 0..5 {
            let events = create_test_events(i * 10 + 1, 2);
            engine
                .append_events(aggregate_id, 100, None, events, None)
                .await?;
        }

        // Test trim_start
        engine.trim_start(aggregate_id, 2).await?;

        // Should only be able to read from batch index 2 onwards
        let filters = ReadFilters::new(2);
        let result = engine.read_filtered(aggregate_id, filters).await?;
        assert_eq!(result.event_batches.len(), 3); // Batches 2, 3, 4

        // Test delete
        engine.delete(aggregate_id).await?;
        let exists = engine.exists(aggregate_id).await?;
        assert!(!exists);

        engine.shutdown_with_timeout(Duration::from_secs(3)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_custom_thread_count() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let config = ThreadedEngineConfig::with_base_path(temp_dir.path().to_path_buf())
            .with_thread_count(2)
            .with_pin_threads(false); // Don't pin threads in test
        let mut engine = ThreadedEngine::new(config)?;

        assert_eq!(engine.thread_count(), 2);

        // Test basic operation still works
        let events = create_test_events(1, 1);
        let result = engine
            .append_events(9876, 100, None, events, None)
            .await?;
        assert_eq!(result.event_batch_index, 0);

        engine.shutdown_with_timeout(Duration::from_secs(3)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_filtering_operations() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let mut engine = ThreadedEngine::with_default_config(temp_dir.path().to_path_buf())?;

        let aggregate_id = 777777;

        // Write events with different types and clients
        let events1 = vec![EventItem::new(1, 1, 1000, 42, 1, b"type42".to_vec())];
        let events2 = vec![EventItem::new(1, 2, 1001, 43, 1, b"type43".to_vec())];

        engine
            .append_events(aggregate_id, 100, None, events1, None)
            .await?;
        engine
            .append_events(aggregate_id, 200, None, events2, None)
            .await?;

        // Test client filtering
        let filters = ReadFilters::new(0).include_client_id(100);
        let result = engine.read_filtered(aggregate_id, filters).await?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].client_id, 100);

        // Test event type filtering
        let event_types = vec![42u64];
        let filters = ReadFilters::new(0).include_event_types(event_types);
        let result = engine.read_filtered(aggregate_id, filters).await?;
        assert_eq!(result.event_batches.len(), 1);
        assert_eq!(result.event_batches[0].events.len(), 1);
        assert_eq!(result.event_batches[0].events[0].event_type_major, 42);

        engine.shutdown_with_timeout(Duration::from_secs(3)).await?;
        Ok(())
    }
}
