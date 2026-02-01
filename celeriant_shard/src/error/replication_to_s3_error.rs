#[derive(Debug, Clone)]
pub enum ReplicateToS3Error {
    S3NotConfigured,
    S3Unavailable,
    S3PutFailed {
        path: String,
        message: String,
    },
    SidecarUnavailable,
    SerializationFailed(String),
}