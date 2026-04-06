use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use celeriant_shard::s3_uploader::S3Uploader;
use celeriant_shard::error::replication_to_s3_error::ReplicateToS3Error;
use crate::sidecar::sidecar_channels::SidecarSenders;
use crate::sidecar::sidecar_messages::SidecarTarget;
use crate::sidecar::error::ErrorKind;

pub struct SidecarS3Uploader {
    senders: SidecarSenders,
    /// Shared across all shards. Limits concurrent S3 fallback uploads to prevent
    /// MinIO saturation that can starve lease renewal on shard 0.
    inflight: Arc<AtomicU32>,
    max_concurrent_uploads: u32,
}

impl SidecarS3Uploader {
    pub fn new(senders: SidecarSenders, inflight: Arc<AtomicU32>, max_concurrent_uploads: u32) -> Self {
        Self { senders, inflight, max_concurrent_uploads }
    }
}

impl S3Uploader for SidecarS3Uploader {
    async fn upload(&self, path: String, data: Bytes) -> Result<(), ReplicateToS3Error> {
        // Wait for a permit — yield to glommio reactor if at capacity.
        loop {
            let current = self.inflight.load(Ordering::Acquire);
            if current < self.max_concurrent_uploads {
                if self.inflight.compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    break;
                }
            }
            glommio::timer::sleep(std::time::Duration::from_millis(10)).await;
        }

        let result = self.do_upload(path, data).await;

        self.inflight.fetch_sub(1, Ordering::Release);
        result
    }
}

impl SidecarS3Uploader {
    async fn do_upload(&self, path: String, data: Bytes) -> Result<(), ReplicateToS3Error> {
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
