//! Gateway for sending object store requests from Glommio shards to the Tokio sidecar.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flume::{Receiver, Sender, TrySendError};

use super::config::ObjectStoreRuntimeConfig;
use super::error::ObjectStoreError;
use super::ops::{ObjectStoreOp, ObjectStoreResult, ObjectStoreTarget, QoSClass};

/// Internal request envelope sent through the channel.
#[derive(Debug)]
pub struct ObjectStoreRequest {
    pub op_id: u64,
    pub target: ObjectStoreTarget,
    pub payload: ObjectStoreOp,
    pub response_tx: Sender<Result<ObjectStoreResult, ObjectStoreError>>,
    pub deadline: Option<Instant>,
    pub qos_class: QoSClass,
}

/// Metrics for a single lane.
#[derive(Debug, Default)]
pub struct LaneMetrics {
    pub queue_depth: AtomicU64,
    pub total_sent: AtomicU64,
    pub total_received: AtomicU64,
    pub total_errors: AtomicU64,
    pub total_timeouts: AtomicU64,
}

/// Shared health and metrics state between gateway and runtime.
#[derive(Debug)]
pub struct SharedState {
    pub healthy: AtomicBool,
    pub last_heartbeat_ms: AtomicU64,
    pub control_lane_metrics: LaneMetrics,
    pub data_lane_metrics: LaneMetrics,
    pub tiering_lane_metrics: LaneMetrics,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            healthy: AtomicBool::new(true),
            last_heartbeat_ms: AtomicU64::new(0),
            control_lane_metrics: LaneMetrics::default(),
            data_lane_metrics: LaneMetrics::default(),
            tiering_lane_metrics: LaneMetrics::default(),
        }
    }
}

impl SharedState {
    pub fn metrics_for_class(&self, class: QoSClass) -> &LaneMetrics {
        match class {
            QoSClass::Control => &self.control_lane_metrics,
            QoSClass::DegradedData => &self.data_lane_metrics,
            QoSClass::Tiering => &self.tiering_lane_metrics,
        }
    }
}

/// Handle for sending requests to the object store sidecar.
///
/// This is cloneable and can be shared across Glommio tasks within a shard.
#[derive(Clone)]
pub struct ObjectStoreGateway {
    control_tx: Sender<ObjectStoreRequest>,
    data_tx: Sender<ObjectStoreRequest>,
    tiering_tx: Sender<ObjectStoreRequest>,
    shared_state: Arc<SharedState>,
    op_counter: Arc<AtomicU64>,
}

impl ObjectStoreGateway {
    /// Create the gateway channels. Returns (gateway, receivers for the runtime).
    pub fn new(config: &ObjectStoreRuntimeConfig) -> (Self, GatewayReceivers) {
        let (control_tx, control_rx) = flume::bounded(config.control_lane_capacity);
        let (data_tx, data_rx) = flume::bounded(config.data_lane_capacity);
        let (tiering_tx, tiering_rx) = flume::bounded(config.tiering_lane_capacity);

        let shared_state = Arc::new(SharedState::default());

        let gateway = Self {
            control_tx,
            data_tx,
            tiering_tx,
            shared_state: shared_state.clone(),
            op_counter: Arc::new(AtomicU64::new(0)),
        };

        let receivers = GatewayReceivers {
            control_rx,
            data_rx,
            tiering_rx,
            shared_state,
        };

        (gateway, receivers)
    }

    /// Check if the sidecar is healthy.
    pub fn is_healthy(&self) -> bool {
        self.shared_state.healthy.load(Ordering::Acquire)
    }

    /// Get the current queue depth for a QoS class.
    pub fn queue_depth(&self, class: QoSClass) -> u64 {
        self.shared_state
            .metrics_for_class(class)
            .queue_depth
            .load(Ordering::Relaxed)
    }

    /// Send an operation and wait for the result.
    ///
    /// This is the primary interface for Glommio shards to interact with S3.
    pub async fn execute(
        &self,
        target: ObjectStoreTarget,
        op: ObjectStoreOp,
        deadline: Option<Instant>,
    ) -> Result<ObjectStoreResult, ObjectStoreError> {
        if !self.is_healthy() {
            return Err(ObjectStoreError::sidecar_unavailable(
                "Object store sidecar is not healthy",
            ));
        }

        let qos_class = target.qos_class();
        let op_id = self.op_counter.fetch_add(1, Ordering::Relaxed);

        // Create a oneshot-like channel for the response
        let (response_tx, response_rx) = flume::bounded(1);

        let request = ObjectStoreRequest {
            op_id,
            target,
            payload: op,
            response_tx,
            deadline,
            qos_class,
        };

        // Select the appropriate lane
        let (tx, metrics) = match qos_class {
            QoSClass::Control => (&self.control_tx, &self.shared_state.control_lane_metrics),
            QoSClass::DegradedData => (&self.data_tx, &self.shared_state.data_lane_metrics),
            QoSClass::Tiering => (&self.tiering_tx, &self.shared_state.tiering_lane_metrics),
        };

        // Try to send (non-blocking to check capacity)
        match tx.try_send(request) {
            Ok(()) => {
                metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
                metrics.total_sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(req)) => {
                // Channel is full - apply backpressure
                // For control plane, we block; for others, we may reject
                if qos_class == QoSClass::Control {
                    // Block for control plane operations
                    if tx.send_async(req).await.is_err() {
                        return Err(ObjectStoreError::sidecar_unavailable(
                            "Control lane channel closed",
                        ));
                    }
                    metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
                    metrics.total_sent.fetch_add(1, Ordering::Relaxed);
                } else {
                    return Err(ObjectStoreError::channel_full(format!(
                        "{:?} lane is full, apply backpressure",
                        qos_class
                    ))
                    .with_retry_after(100));
                }
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(ObjectStoreError::sidecar_unavailable(
                    "Object store channel disconnected",
                ));
            }
        }

        // Wait for response
        // Using recv_async which works with any async runtime through flume
        match response_rx.recv_async().await {
            Ok(result) => {
                metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
                metrics.total_received.fetch_add(1, Ordering::Relaxed);
                if result.is_err() {
                    metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                }
                result
            }
            Err(_) => {
                metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
                metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                Err(ObjectStoreError::sidecar_unavailable(
                    "Response channel closed unexpectedly",
                ))
            }
        }
    }

    /// Blocking variant for use in synchronous contexts.
    pub fn execute_blocking(
        &self,
        target: ObjectStoreTarget,
        op: ObjectStoreOp,
        timeout: Option<Duration>,
    ) -> Result<ObjectStoreResult, ObjectStoreError> {
        if !self.is_healthy() {
            return Err(ObjectStoreError::sidecar_unavailable(
                "Object store sidecar is not healthy",
            ));
        }

        let qos_class = target.qos_class();
        let op_id = self.op_counter.fetch_add(1, Ordering::Relaxed);
        let deadline = timeout.map(|t| Instant::now() + t);

        let (response_tx, response_rx) = flume::bounded(1);

        let request = ObjectStoreRequest {
            op_id,
            target,
            payload: op,
            response_tx,
            deadline,
            qos_class,
        };

        let tx = match qos_class {
            QoSClass::Control => &self.control_tx,
            QoSClass::DegradedData => &self.data_tx,
            QoSClass::Tiering => &self.tiering_tx,
        };

        tx.send(request).map_err(|_| {
            ObjectStoreError::sidecar_unavailable("Object store channel disconnected")
        })?;

        match timeout {
            Some(t) => response_rx.recv_timeout(t).map_err(|e| match e {
                flume::RecvTimeoutError::Timeout => {
                    ObjectStoreError::timeout("Operation timed out waiting for response")
                }
                flume::RecvTimeoutError::Disconnected => {
                    ObjectStoreError::sidecar_unavailable("Response channel closed")
                }
            })?,
            None => response_rx.recv().map_err(|_| {
                ObjectStoreError::sidecar_unavailable("Response channel closed")
            })?,
        }
    }
}

/// Receivers for the sidecar runtime to consume requests.
pub struct GatewayReceivers {
    pub control_rx: Receiver<ObjectStoreRequest>,
    pub data_rx: Receiver<ObjectStoreRequest>,
    pub tiering_rx: Receiver<ObjectStoreRequest>,
    pub shared_state: Arc<SharedState>,
}