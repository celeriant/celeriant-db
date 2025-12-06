use crate::{SidecarConfig, sidecar::{sidecar_runtime::SidecarRuntime, sidecar_senders::SidecarSenders}};

pub struct SidecarHandle {
    pub sidecar_senders: SidecarSenders,
    sidecar_runtime: Option<SidecarRuntime>,
}

impl SidecarHandle {
    pub fn new(sidecar_config: SidecarConfig) -> Result<Option<Self>, String> {
        // if sidecar_config.s3.is_none() {
        //     return Ok(None);
        // }

        let (sidecar_senders, sidecar_receivers) = SidecarSenders::new(&sidecar_config);

        let sidecar_runtime = SidecarRuntime::new(sidecar_config, sidecar_receivers)
            .map_err(|e| format!("Failed to sidecar runtime: {}", e))?;

        Ok(Some(Self { 
            sidecar_senders, 
            sidecar_runtime: Some(sidecar_runtime) 
        }))
    }
}