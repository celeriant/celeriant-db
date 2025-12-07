use bytes::Bytes;

/// Opeations that execute within the sidecar
#[derive(Clone, Debug)]
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