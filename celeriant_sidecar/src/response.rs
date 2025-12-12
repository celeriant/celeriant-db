use bytes::Bytes;

/// Result of a sidecar operation.
#[derive(Clone, Debug)]
pub enum Response {
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

/// Metadata about a stored object.
#[derive(Clone, Debug)]
pub struct ObjectMetadata {
    pub path: String,
    pub size: u64,
    pub e_tag: Option<String>,
    pub last_modified: Option<u64>,
}