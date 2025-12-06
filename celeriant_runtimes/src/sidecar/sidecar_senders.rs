use std::time::Instant;

use flume::{Receiver, Sender};

use crate::{SidecarConfig, sidecar::{error::SidecarError, sidecar_messages::{QoSClass, SidecarOperation, SidecarRequest, SidecarResponse, SidecarTarget}}};

/// Handle for sending requests to the sidecar.
/// This is cloneable and can be shared across Glommio tasks within a shard.
#[derive(Clone)]
pub struct SidecarSenders {
    control_tx: Sender<SidecarRequest>,
    data_tx: Sender<SidecarRequest>,
}

impl SidecarSenders {
    pub fn new(sidecar_config: &SidecarConfig) -> (Self, SidecarReceivers) {
        let (control_tx, control_rx) = flume::bounded(sidecar_config.control_lane_capacity);
        let (data_tx, data_rx) = flume::bounded(sidecar_config.data_lane_capacity);

        (Self { control_tx, data_tx }, SidecarReceivers { control_rx, data_rx })
    }

    pub async fn send_request(&self, 
        target: SidecarTarget,
        operation: SidecarOperation,
        deadline: Instant,
    ) -> Result<SidecarResponse, SidecarError> {
        
        // Select the appropriate lane
        let qos_class = target.qos_class();
        let tx = match qos_class {
            QoSClass::Control => &self.control_tx,
            QoSClass::Data => &self.data_tx,
        };

        // Create a oneshot-like channel for the response
        let (response_tx, response_rx) = flume::bounded(1);

        let request = SidecarRequest {
            target,
            payload: operation,
            response_tx,
            deadline,
            qos_class,
        };

        if tx.send_async(request).await.is_err() {
            return Err(SidecarError::sidecar_unavailable(
                "Control lane channel closed",
            ));
        }

        // Wait for response
        match response_rx.recv_async().await {
            Ok(result) => {
                result
            }
            Err(_) => {
                Err(SidecarError::sidecar_unavailable(
                    "Response channel closed unexpectedly",
                ))
            }
        }
    }
}

/// Receivers for the sidecar runtime to consume requests.
pub struct SidecarReceivers {
    pub control_rx: Receiver<SidecarRequest>,
    pub data_rx: Receiver<SidecarRequest>,
}