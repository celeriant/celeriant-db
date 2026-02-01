use celeriant_sidecar::error::StoreError;
use flume::{Sender};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum QoSClass {
    /// Control plane operations such as leases, membership
    Control,
    /// Data storage operations
    Data,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SidecarTarget {
    /// Lease acquisition, renewal, and release.
    ControlPlaneLease,
    /// Cluster membership file operations.
    ControlPlaneMembership,
    /// Data plane S3 replication fallback operations.
    DataPlaneReplication,
}

impl SidecarTarget {
    pub fn qos_class(&self) -> QoSClass {
        match self {
            SidecarTarget::ControlPlaneLease
            | SidecarTarget::ControlPlaneMembership => QoSClass::Control,
            SidecarTarget::DataPlaneReplication => QoSClass::Data,
        }
    }
}

/// Internal request envelope sent through the channel.
#[derive(Debug)]
pub struct SidecarRequest {
    pub target: SidecarTarget,
    pub store_request: celeriant_sidecar::request::Request,
    pub response_tx: Sender<Result<celeriant_sidecar::response::Response, StoreError>>,
    pub qos_class: QoSClass,
}