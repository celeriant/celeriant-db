use std::time::Instant;

use bytes::Bytes;
use flume::{Sender};

use crate::sidecar::error::{SidecarError};

/// Quality of Service class for prioritizing operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum QoSClass {
    /// Control plane operations such as leases, membership
    Control,
    /// Data storage operations
    Data,
}

/// Target category for routing operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SidecarTarget {
    /// Lease acquisition, renewal, and release.
    ControlPlaneLease,
    /// Cluster membership file operations.
    ControlPlaneMembership,
}

impl SidecarTarget {
    pub fn qos_class(&self) -> QoSClass {
        match self {
            SidecarTarget::ControlPlaneLease
            | SidecarTarget::ControlPlaneMembership => QoSClass::Control,
        }
    }
}

/// Condition for conditional PUT operations.
#[derive(Clone, Debug)]
pub enum PutCondition {
    /// Object must not exist (create-only).
    CreateOnly,
    /// Object must have this ETag (optimistic concurrency).
    IfMatchETag(String),
    /// No condition, always overwrite.
    None,
}

/// Opeations that execute within the sidecar
#[derive(Clone, Debug)]
pub enum SidecarOperation {
    /// PUT an object with optional conditional write.
    ObjectPut {
        path: String,
        data: Bytes,
        condition: PutCondition,
    },
    /// GET an object.
    ObjectGet {
        path: String,
    },
    /// HEAD an object (metadata only).
    ObjectHead {
        path: String,
    },
    /// DELETE an object.
    ObjectDelete {
        path: String,
    },
    /// DELETE multiple objects.
    ObjectDeleteBatch {
        paths: Vec<String>,
    },
    /// LIST objects with a prefix.
    ObjectList {
        prefix: String,
    },
}

/// Metadata about a stored object.
#[derive(Clone, Debug)]
pub struct ObjectMetadata {
    pub path: String,
    pub size: u64,
    pub e_tag: Option<String>,
    pub last_modified: Option<u64>,
}

/// Result of a sidecar operation.
#[derive(Clone, Debug)]
pub enum SidecarResponse {
    /// PUT succeeded, returns the new ETag.
    ObjectPut { e_tag: Option<String> },
    /// GET succeeded, returns the data and metadata.
    ObjectGet {
        data: Bytes,
        e_tag: Option<String>,
        size: u64,
    },
    /// HEAD succeeded, returns metadata.
    ObjectHead(ObjectMetadata),
    /// DELETE succeeded.
    ObjectDelete,
    /// DELETE batch completed, returns paths that failed.
    ObjectDeleteBatch { failed_paths: Vec<String> },
    /// LIST succeeded, returns object metadata.
    ObjectList { objects: Vec<ObjectMetadata> },
}

/// Internal request envelope sent through the channel.
#[derive(Debug)]
pub struct SidecarRequest {
    pub target: SidecarTarget,
    pub payload: SidecarOperation,
    pub response_tx: Sender<Result<SidecarResponse, SidecarError>>,
    pub deadline: Instant,
    pub qos_class: QoSClass,
}