use bytes::Bytes;

/// Opeations that execute within the sidecar
#[derive(Clone)]
pub enum Request {
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

impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Request::ObjectPut { path, data, condition } => f
                .debug_struct("ObjectPut")
                .field("path", path)
                .field("data_len", &data.len())
                .field("condition", condition)
                .finish(),
            Request::ObjectGet { path } => f.debug_struct("ObjectGet").field("path", path).finish(),
            Request::ObjectHead { path } => f.debug_struct("ObjectHead").field("path", path).finish(),
            Request::ObjectDelete { path } => f.debug_struct("ObjectDelete").field("path", path).finish(),
            Request::ObjectDeleteBatch { paths } => f.debug_struct("ObjectDeleteBatch").field("paths", paths).finish(),
            Request::ObjectList { prefix } => f.debug_struct("ObjectList").field("prefix", prefix).finish(),
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