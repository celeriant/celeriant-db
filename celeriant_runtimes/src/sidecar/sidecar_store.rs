use std::time::Instant;

use bytes::Bytes;
use tracing::info;

use crate::{SidecarConfig, sidecar::{error::SidecarError, sidecar_messages::{QoSClass, SidecarOperation, SidecarResponse}}};

#[derive(Clone)]
pub struct SidecarStore {

}

impl SidecarStore {    
    pub async fn process_request(&self, request_payload: SidecarOperation, qos_class: QoSClass, deadline: Instant) -> Result<SidecarResponse, SidecarError> {
        info!("Processing request: {:?}", request_payload);
        Ok(SidecarResponse::ObjectGet { data: Bytes::copy_from_slice(&[4u8,4]), e_tag: None, size: 33 })
    }
    
    pub async fn new(sidecar_config: SidecarConfig) -> Result<Self, SidecarError> {
        Ok(Self {  })
    }  
}