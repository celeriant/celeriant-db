//! Object store operation types and results.

use bytes::Bytes;

/// Quality of Service class for prioritizing operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum QoSClass {
    /// Control plane operations: leases, membership (highest priority).
    Control,
    /// Degraded mode data operations: batch uploads during replication failure.
    DegradedData,
    /// Tiered storage operations: cold-tier moves (lowest priority).
    Tiering,
}

/// Target category for routing operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectStoreTarget {
    /// Lease acquisition, renewal, and release.
    ControlPlaneLease,
    /// Cluster membership file operations.
    ControlPlaneMembership,
    /// Pending write markers (future).
    MetadataIntents,
    /// Batch data in degraded mode.
    DegradedBatch,
    /// Cold-tier storage moves (future).
    TieredStorage,
}

impl ObjectStoreTarget {
    pub fn qos_class(&self) -> QoSClass {
        match self {
            ObjectStoreTarget::ControlPlaneLease
            | ObjectStoreTarget::ControlPlaneMembership
            | ObjectStoreTarget::MetadataIntents => QoSClass::Control,
            ObjectStoreTarget::DegradedBatch => QoSClass::DegradedData,
            ObjectStoreTarget::TieredStorage => QoSClass::Tiering,
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

/// Individual object store operation.
#[derive(Clone, Debug)]
pub enum ObjectStoreOp {
    /// PUT an object with optional conditional write.
    Put {
        path: String,
        data: Bytes,
        condition: PutCondition,
    },
    /// GET an object.
    Get {
        path: String,
    },
    /// HEAD an object (metadata only).
    Head {
        path: String,
    },
    /// DELETE an object.
    Delete {
        path: String,
    },
    /// DELETE multiple objects.
    DeleteBatch {
        paths: Vec<String>,
    },
    /// LIST objects with a prefix.
    List {
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

/// Result of an object store operation.
#[derive(Clone, Debug)]
pub enum ObjectStoreResult {
    /// PUT succeeded, returns the new ETag.
    Put { e_tag: Option<String> },
    /// GET succeeded, returns the data and metadata.
    Get {
        data: Bytes,
        e_tag: Option<String>,
        size: u64,
    },
    /// HEAD succeeded, returns metadata.
    Head(ObjectMetadata),
    /// DELETE succeeded.
    Delete,
    /// DELETE batch completed, returns paths that failed.
    DeleteBatch { failed_paths: Vec<String> },
    /// LIST succeeded, returns object metadata.
    List { objects: Vec<ObjectMetadata> },
}