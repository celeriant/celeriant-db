use async_trait::async_trait;
use bytes::Bytes;
use tracing::info;

use crate::{error::StoreError, request::Request, response::Response, store_config::StoreConfig};

/// Trait defining the sidecar store interface for dependency injection.
#[async_trait]
pub trait SidecarStoreTrait: Send + Sync + 'static {
    async fn process_request(
        &self,
        request: Request,
    ) -> Result<Response, StoreError>;
}

#[derive(Clone)]
pub struct SidecarStore {
    _sidecar_store_config: StoreConfig
}

#[async_trait]
impl SidecarStoreTrait for SidecarStore {
    async fn process_request(&self, request: Request) -> Result<Response, StoreError> {
        info!("Processing request: {:?}", request);
        Ok(Response::ObjectGet { data: Bytes::copy_from_slice(&[4u8,4]), e_tag: None, size: 33 })
    }
}

impl SidecarStore {
    pub fn new(sidecar_store_config: StoreConfig) -> Result<Self, StoreError> {
        Ok(Self { _sidecar_store_config: sidecar_store_config })
    }
}