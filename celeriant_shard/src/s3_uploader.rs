use bytes::Bytes;
use crate::error::replication_to_s3_error::ReplicateToS3Error;

/// Abstracts S3 upload so celeriant_shard doesn't depend on celeriant_runtimes.
/// Implemented in celeriant_runtimes using SidecarSenders.
#[allow(async_fn_in_trait)]
pub trait S3Uploader {
    /// Upload data to the given S3 path with CreateOnly semantics.
    /// Returns Ok(()) on success or if the object already exists (AlreadyExists).
    async fn upload(&self, path: String, data: Bytes) -> Result<(), ReplicateToS3Error>;
}
