use std::sync::Arc;

use celeriant_sidecar::store::SidecarStoreTrait;
use tokio::runtime::Builder;

use crate::{SidecarConfig, sidecar::{error::SidecarError, sidecar_messages::{SidecarRequest}, sidecar_channels::SidecarReceivers}};

pub struct SidecarRuntime {
    _runtime: tokio::runtime::Runtime,
}

impl SidecarRuntime {
    pub fn with_store<S: SidecarStoreTrait>(
        sidecar_config: SidecarConfig,
        sidecar_receivers: SidecarReceivers,
        store: S,
    ) -> Result<Self, SidecarError> {
        let store = Arc::new(store);
        let runtime = Builder::new_multi_thread()
            .worker_threads(sidecar_config.worker_threads)
            .thread_name("object-store-sidecar")
            .enable_all()
            .build()
            .map_err(|e| SidecarError::tokio_runtime_failure(format!("Failed to build Tokio runtime: {}", e)))?;
        runtime.spawn(run_sidecar(sidecar_receivers, store));
        Ok(Self { _runtime: runtime })
    }
}

async fn run_sidecar<S: SidecarStoreTrait>(
    sidecar_receivers: SidecarReceivers,
    sidecar_store: Arc<S>,
) {
    let control_handle = spawn_lane_processor(
        sidecar_receivers.control_rx,
        sidecar_store.clone(),
    );

    let data_handle = spawn_lane_processor(
        sidecar_receivers.data_rx,
        sidecar_store.clone(),
    );

    let (control_result, data_result) = tokio::join!(control_handle, data_handle);

    if let Err(e) = control_result {
        tracing::error!("Control lane processor panicked: {}", e);
    }
    if let Err(e) = data_result {
        tracing::error!("Data lane processor panicked: {}", e);
    }
}

fn spawn_lane_processor<S: SidecarStoreTrait>(
    rx: flume::Receiver<SidecarRequest>,
    sidecar_store: Arc<S>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(request) = rx.recv_async().await {
            let sidecar_store = sidecar_store.clone();
            tokio::spawn(async move {
                let response = sidecar_store.process_request(request.store_request).await;
                let _ = request.response_tx.send(response);
            });
        }
    })
}

impl Drop for SidecarRuntime {
    fn drop(&mut self) {
        tracing::info!("Sidecar runtime shutting down");
    }
}
