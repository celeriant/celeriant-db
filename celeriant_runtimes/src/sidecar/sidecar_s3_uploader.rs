use bytes::Bytes;
use celeriant_shard::s3_uploader::S3Uploader;
use celeriant_shard::error::replication_to_s3_error::ReplicateToS3Error;
use crate::sidecar::sidecar_channels::SidecarSenders;
use crate::sidecar::sidecar_messages::SidecarTarget;
use crate::sidecar::error::ErrorKind;

pub struct SidecarS3Uploader {
    senders: SidecarSenders,
}

impl SidecarS3Uploader {
    pub fn new(senders: SidecarSenders) -> Self {
        Self { senders }
    }
}

impl S3Uploader for SidecarS3Uploader {
    async fn upload(&self, path: String, data: Bytes) -> Result<(), ReplicateToS3Error> {
        let request = celeriant_sidecar::request::Request::ObjectPut {
            path: path.clone(),
            data,
            condition: celeriant_sidecar::request::PutCondition::CreateOnly,
        };

        match self.senders.send_async(SidecarTarget::DataPlaneReplication, request).await {
            Ok(_response) => Ok(()),
            Err(err) => match err.kind {
                ErrorKind::ChannelClosed | ErrorKind::TokioRuntimeFailure => {
                    Err(ReplicateToS3Error::SidecarUnavailable)
                }
                ErrorKind::StoreError(store_kind) => {
                    use celeriant_sidecar::error::ErrorKind as StoreErrorKind;
                    match store_kind {
                        StoreErrorKind::AlreadyExists => {
                            // Invariant #8: AlreadyExists is not an error
                            // Same WAL index = same data (crash-restart scenario)
                            tracing::warn!("S3 batch already exists (likely crash-restart): {}", path);
                            Ok(())
                        }
                        StoreErrorKind::Configuration => Err(ReplicateToS3Error::S3NotConfigured),
                        _ => Err(ReplicateToS3Error::S3PutFailed {
                            path,
                            message: err.message,
                        }),
                    }
                }
            }
        }
    }
}
