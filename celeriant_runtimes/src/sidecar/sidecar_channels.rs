use flume::{Receiver, Sender};
use crate::{SidecarConfig, sidecar::{error::SidecarError, sidecar_messages::{QoSClass, SidecarRequest, SidecarTarget}}};

/// Receivers are used on the tokio / sidecar side of the channel
pub struct SidecarReceivers {
    pub control_rx: Receiver<SidecarRequest>,
    pub data_rx: Receiver<SidecarRequest>,
}

/// Handle for sending requests to the sidecar.
/// This is cloneable and can be shared across Glommio tasks within a shard.
#[derive(Clone)]
pub struct SidecarSenders {
    control_tx: Sender<SidecarRequest>,
    data_tx: Sender<SidecarRequest>,
}

pub fn create_sidecar_channel(sidecar_config: &SidecarConfig) -> (SidecarSenders, SidecarReceivers) {
    let (control_tx, control_rx) = flume::bounded(sidecar_config.control_lane_capacity);
    let (data_tx, data_rx) = flume::bounded(sidecar_config.data_lane_capacity);

    (SidecarSenders { control_tx, data_tx }, SidecarReceivers { control_rx, data_rx })
}

impl SidecarSenders {
    pub async fn send_async(&self, 
        target: SidecarTarget,
        store_request: celeriant_sidecar::request::Request,
    ) -> Result<celeriant_sidecar::response::Response, SidecarError> {
        
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
            response_tx,
            qos_class,
            store_request,
        };

        if tx.send_async(request).await.is_err() {
            return Err(SidecarError::channel_closed(
                "Control lane channel closed".to_string(),
            ));
        }

        // Wait for response
        match response_rx.recv_async().await {
            Ok(result) => {
                Ok(result?)
            }
            Err(_) => {
                Err(SidecarError::channel_closed(
                    "Response channel closed unexpectedly".to_string(),
                ))
            }
        }
    }
}