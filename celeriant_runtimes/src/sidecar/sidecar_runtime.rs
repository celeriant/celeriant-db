use std::sync::Arc;

use tokio::runtime::Builder;

use crate::{SidecarConfig, sidecar::{error::SidecarError, sidecar_messages::{QoSClass, SidecarRequest}, sidecar_senders::SidecarReceivers, sidecar_store::SidecarStore}};

pub struct SidecarRuntime {
    runtime: tokio::runtime::Runtime,
}

impl SidecarRuntime {
    pub fn new(sidecar_config: SidecarConfig, sidecar_receivers: SidecarReceivers) -> Result<Self, SidecarError> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(sidecar_config.worker_threads)
            .thread_name("object-store-sidecar")
            .enable_all()
            .build()
            .map_err(|e| SidecarError::permanent(format!("Failed to build Tokio runtime: {}", e)))?;

        // Build the object store client
        let sidecar_store = runtime.block_on(async {
            SidecarStore::new(sidecar_config.clone()).await
        })?;

        let sidecar_store = Arc::new(sidecar_store);

        runtime.spawn(run_sidecar(sidecar_receivers, sidecar_store));

        Ok(Self {
            runtime
        })
    }
}

async fn run_sidecar(
    sidecar_receivers: SidecarReceivers,
    sidecar_store: Arc<SidecarStore>,
) {
    // Spawn lane processors
    let control_handle = spawn_lane_processor(
        QoSClass::Control,
        sidecar_receivers.control_rx,
        sidecar_store.clone(),
    );

    let data_handle = spawn_lane_processor(
        QoSClass::Data,
        sidecar_receivers.data_rx,
        sidecar_store.clone(),
    );

    // Wait for both lane processors to exit
    let (control_result, data_result) = tokio::join!(control_handle, data_handle);

    // Optionally handle any join errors
    if let Err(e) = control_result {
        tracing::error!("Control lane processor panicked: {}", e);
    }
    if let Err(e) = data_result {
        tracing::error!("Data lane processor panicked: {}", e);
    }
}

fn spawn_lane_processor(
    qos_class: QoSClass,
    rx: flume::Receiver<SidecarRequest>,
    sidecar_store: Arc<SidecarStore>
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(request) = rx.recv_async().await {
            let sidecar_store = sidecar_store.clone();
            // Spawn each operation as a separate task for concurrency
            tokio::spawn(async move {
                let response = sidecar_store.as_ref().process_request(request.payload, qos_class, request.deadline).await;
                let _ = request.response_tx.send(response);
            });
        }
    })
}