//! Tokio sidecar runtime for object store operations.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Semaphore;

use super::config::{ObjectStoreRetryConfig, ObjectStoreRuntimeConfig};
use super::error::ObjectStoreError;
use super::gateway::{GatewayReceivers, ObjectStoreRequest, SharedState};
use super::ops::{ObjectMetadata, ObjectStoreOp, ObjectStoreResult, PutCondition, QoSClass};

/// S3 configuration for building the object store client.
#[derive(Clone, Debug)]
pub struct S3Config {
    pub bucket: String,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub subfolder: Option<String>,
}

/// The Tokio sidecar runtime that processes object store requests.
pub struct ObjectStoreRuntime {
    runtime: Runtime,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ObjectStoreRuntime {
    /// Spawn the sidecar runtime with the given configuration.
    pub fn spawn(
        runtime_config: ObjectStoreRuntimeConfig,
        retry_config: ObjectStoreRetryConfig,
        s3_config: S3Config,
        receivers: GatewayReceivers,
    ) -> Result<Self, ObjectStoreError> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(runtime_config.worker_threads)
            .thread_name("object-store-sidecar")
            .enable_all()
            .build()
            .map_err(|e| ObjectStoreError::permanent(format!("Failed to build Tokio runtime: {}", e)))?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Build the object store client
        let store = runtime.block_on(async {
            build_s3_client(&s3_config)
        })?;

        let store = Arc::new(store);
        let retry_config = Arc::new(retry_config);
        let shared_state = receivers.shared_state.clone();
        let inflight_semaphore = Arc::new(Semaphore::new(runtime_config.max_inflight_ops));
        let heartbeat_interval = Duration::from_millis(runtime_config.heartbeat_interval_ms);

        // Spawn the main processing loop
        runtime.spawn(run_sidecar(
            store,
            retry_config,
            receivers,
            shutdown_rx,
            inflight_semaphore,
            heartbeat_interval,
        ));

        Ok(Self {
            runtime,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Gracefully shutdown the sidecar runtime.
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // TODO: cannot move out of type `ObjectStoreRuntime`, which implements the `Drop` trait
        // cannot move out of hererustcClick for full compiler diagnostic
        // self.runtime.shutdown_timeout(Duration::from_secs(10));
    }
}

impl Drop for ObjectStoreRuntime {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

fn build_s3_client(config: &S3Config) -> Result<impl ObjectStore, ObjectStoreError> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_conditional_put(S3ConditionalPut::ETagMatch);

    if let Some(ref region) = config.region {
        builder = builder.with_region(region);
    }

    if let Some(ref access_key) = config.access_key_id {
        builder = builder.with_access_key_id(access_key);
    }

    if let Some(ref secret_key) = config.secret_access_key {
        builder = builder.with_secret_access_key(secret_key);
    }

    builder
        .build()
        .map_err(|e| ObjectStoreError::permanent(format!("Failed to build S3 client: {}", e)))
}

async fn run_sidecar(
    store: Arc<impl ObjectStore + 'static>,
    retry_config: Arc<ObjectStoreRetryConfig>,
    receivers: GatewayReceivers,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    inflight_semaphore: Arc<Semaphore>,
    heartbeat_interval: Duration,
) {
    let shared_state = receivers.shared_state.clone();
    
    // Mark as healthy
    shared_state.healthy.store(true, Ordering::Release);
    update_heartbeat(&shared_state);

    // Spawn heartbeat task
    let heartbeat_state = shared_state.clone();
    let heartbeat_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(heartbeat_interval);
        loop {
            interval.tick().await;
            update_heartbeat(&heartbeat_state);
        }
    });

    // Spawn lane processors
    let control_handle = spawn_lane_processor(
        QoSClass::Control,
        receivers.control_rx,
        store.clone(),
        retry_config.clone(),
        inflight_semaphore.clone(),
        shared_state.clone(),
    );

    let data_handle = spawn_lane_processor(
        QoSClass::DegradedData,
        receivers.data_rx,
        store.clone(),
        retry_config.clone(),
        inflight_semaphore.clone(),
        shared_state.clone(),
    );

    let tiering_handle = spawn_lane_processor(
        QoSClass::Tiering,
        receivers.tiering_rx,
        store.clone(),
        retry_config.clone(),
        inflight_semaphore.clone(),
        shared_state.clone(),
    );

    // Wait for shutdown signal
    let _ = shutdown_rx.await;

    // Cleanup
    shared_state.healthy.store(false, Ordering::Release);
    heartbeat_handle.abort();
    control_handle.abort();
    data_handle.abort();
    tiering_handle.abort();
}

fn update_heartbeat(state: &SharedState) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    state.last_heartbeat_ms.store(now, Ordering::Release);
}

fn spawn_lane_processor(
    qos_class: QoSClass,
    rx: flume::Receiver<ObjectStoreRequest>,
    store: Arc<impl ObjectStore + 'static>,
    retry_config: Arc<ObjectStoreRetryConfig>,
    semaphore: Arc<Semaphore>,
    shared_state: Arc<SharedState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(request) = rx.recv_async().await {
            let store = store.clone();
            let retry_config = retry_config.clone();
            let semaphore = semaphore.clone();
            let shared_state = shared_state.clone();

            // Spawn each operation as a separate task for concurrency
            tokio::spawn(async move {
                // Acquire semaphore permit for inflight limiting
                let _permit = match semaphore.acquire().await {
                    Ok(p) => p,
                    Err(_) => {
                        let _ = request.response_tx.send(Err(ObjectStoreError::sidecar_unavailable(
                            "Semaphore closed",
                        )));
                        return;
                    }
                };

                // Check deadline before processing
                if let Some(deadline) = request.deadline {
                    if Instant::now() > deadline {
                        let _ = request.response_tx.send(Err(ObjectStoreError::timeout(
                            "Request deadline exceeded before processing",
                        )));
                        shared_state
                            .metrics_for_class(qos_class)
                            .total_timeouts
                            .fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }

                let result = execute_with_retry(
                    store.as_ref(),
                    &request.payload,
                    &retry_config,
                    qos_class,
                    request.deadline,
                )
                .await;

                let _ = request.response_tx.send(result);
            });
        }
    })
}

async fn execute_with_retry(
    store: &impl ObjectStore,
    op: &ObjectStoreOp,
    config: &ObjectStoreRetryConfig,
    qos_class: QoSClass,
    deadline: Option<Instant>,
) -> Result<ObjectStoreResult, ObjectStoreError> {
    let (timeout_ms, max_retries) = match qos_class {
        QoSClass::Control => (config.lease_timeout_ms, config.lease_retry_attempts),
        QoSClass::DegradedData => (config.batch_put_timeout_ms, config.batch_put_retries),
        QoSClass::Tiering => (config.batch_put_timeout_ms, config.batch_put_retries),
    };

    let mut last_error = None;
    let mut attempt = 0;

    while attempt < max_retries {
        // Check deadline
        if let Some(dl) = deadline {
            if Instant::now() > dl {
                return Err(ObjectStoreError::timeout("Deadline exceeded during retry loop"));
            }
        }

        let timeout = Duration::from_millis(timeout_ms);
        let result = tokio::time::timeout(timeout, execute_operation(store, op)).await;

        match result {
            Ok(Ok(res)) => return Ok(res),
            Ok(Err(e)) => {
                if !e.is_retryable() {
                    return Err(e);
                }
                last_error = Some(e);
            }
            Err(_) => {
                last_error = Some(ObjectStoreError::timeout("Operation timed out"));
            }
        }

        attempt += 1;
        if attempt < max_retries {
            let backoff = calculate_backoff(attempt, config);
            tokio::time::sleep(backoff).await;
        }
    }

    Err(last_error.unwrap_or_else(|| ObjectStoreError::permanent("Max retries exceeded")))
}

fn calculate_backoff(attempt: u32, config: &ObjectStoreRetryConfig) -> Duration {
    let base = config.base_backoff_ms as f64;
    let max = config.max_backoff_ms as f64;
    let jitter_factor = config.jitter_factor;

    // Exponential backoff: base * 2^attempt
    let exponential = base * (2_f64.powi(attempt as i32 - 1));
    let capped = exponential.min(max);

    // Add jitter
    let jitter_range = capped * jitter_factor;
    let jitter = rand_jitter(jitter_range);
    let final_ms = capped + jitter;

    Duration::from_millis(final_ms as u64)
}

fn rand_jitter(range: f64) -> f64 {
    // Simple pseudo-random jitter using system time
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as f64;
    let normalized = (nanos % 1000.0) / 1000.0; // 0.0 to 1.0
    (normalized - 0.5) * 2.0 * range // -range to +range
}

async fn execute_operation(
    store: &impl ObjectStore,
    op: &ObjectStoreOp,
) -> Result<ObjectStoreResult, ObjectStoreError> {
    match op {
        ObjectStoreOp::Put {
            path,
            data,
            condition,
        } => {
            let object_path = ObjectPath::from(path.as_str());
            let put_opts = match condition {
                PutCondition::CreateOnly => PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
                PutCondition::IfMatchETag(etag) => PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: Some(etag.clone()),
                        version: None,
                    }),
                    ..Default::default()
                },
                PutCondition::None => PutOptions::default(),
            };

            let result = store
                .put_opts(&object_path, data.clone().into(), put_opts)
                .await
                .map_err(map_object_store_error)?;

            Ok(ObjectStoreResult::Put {
                e_tag: result.e_tag,
            })
        }

        ObjectStoreOp::Get { path } => {
            let object_path = ObjectPath::from(path.as_str());
            let result = store
                .get(&object_path)
                .await
                .map_err(map_object_store_error)?;

            let meta = result.meta.clone();
            let data = result.bytes().await.map_err(|e| {
                ObjectStoreError::retryable(format!("Failed to read bytes: {}", e))
            })?;

            Ok(ObjectStoreResult::Get {
                data,
                e_tag: meta.e_tag,
                size: meta.size as u64,
            })
        }

        ObjectStoreOp::Head { path } => {
            let object_path = ObjectPath::from(path.as_str());
            let meta = store
                .head(&object_path)
                .await
                .map_err(map_object_store_error)?;

            Ok(ObjectStoreResult::Head(ObjectMetadata {
                path: path.clone(),
                size: meta.size as u64,
                e_tag: meta.e_tag,
                last_modified: Some(
                    meta.last_modified
                        .signed_duration_since(chrono::DateTime::UNIX_EPOCH)
                        .num_milliseconds() as u64,
                ),
            }))
        }

        ObjectStoreOp::Delete { path } => {
            let object_path = ObjectPath::from(path.as_str());
            store
                .delete(&object_path)
                .await
                .map_err(map_object_store_error)?;

            Ok(ObjectStoreResult::Delete)
        }

        ObjectStoreOp::DeleteBatch { paths } => {
            let mut failed_paths = Vec::new();

            // Process deletes with some concurrency but not unbounded
            let chunks: Vec<_> = paths.chunks(10).collect();
            for chunk in chunks {
                let futures: Vec<_> = chunk
                    .iter()
                    .map(|p| {
                        let path = ObjectPath::from(p.as_str());
                        let p_clone = p.clone();
                        async move { (p_clone, store.delete(&path).await) }
                    })
                    .collect();

                let results = futures::future::join_all(futures).await;
                for (path, result) in results {
                    if result.is_err() {
                        failed_paths.push(path);
                    }
                }
            }

            Ok(ObjectStoreResult::DeleteBatch { failed_paths })
        }

        ObjectStoreOp::List { prefix } => {
            use futures::StreamExt;

            let prefix_path = ObjectPath::from(prefix.as_str());
            let mut stream = store.list(Some(&prefix_path));
            let mut objects = Vec::new();

            while let Some(result) = stream.next().await {
                match result {
                    Ok(meta) => {
                        objects.push(ObjectMetadata {
                            path: meta.location.to_string(),
                            size: meta.size as u64,
                            e_tag: meta.e_tag,
                            last_modified: Some(
                                meta.last_modified
                                    .signed_duration_since(chrono::DateTime::UNIX_EPOCH)
                                    .num_milliseconds() as u64,
                            ),
                        });
                    }
                    Err(e) => {
                        return Err(map_object_store_error(e));
                    }
                }
            }

            Ok(ObjectStoreResult::List { objects })
        }
    }
}

fn map_object_store_error(e: object_store::Error) -> ObjectStoreError {
    use object_store::Error;

    match e {
        Error::NotFound { path, .. } => {
            ObjectStoreError::not_found(format!("Object not found: {}", path))
        }
        Error::Precondition { path, .. } => {
            ObjectStoreError::precondition_failed(format!("Precondition failed for: {}", path))
        }
        Error::AlreadyExists { path, .. } => {
            ObjectStoreError::precondition_failed(format!("Object already exists: {}", path))
        }
        Error::NotSupported { .. } => ObjectStoreError::permanent("Operation not supported"),
        Error::NotImplemented => ObjectStoreError::permanent("Operation not implemented"),
        Error::Generic { source, .. } => {
            // Check for retryable patterns in the error message
            let msg = source.to_string();
            if msg.contains("503")
                || msg.contains("SlowDown")
                || msg.contains("timeout")
                || msg.contains("connection")
            {
                ObjectStoreError::retryable(msg)
            } else if msg.contains("403") || msg.contains("401") || msg.contains("AccessDenied") {
                ObjectStoreError::auth(msg)
            } else {
                ObjectStoreError::permanent(msg)
            }
        }
        _ => ObjectStoreError::retryable(format!("Object store error: {}", e)),
    }
}
