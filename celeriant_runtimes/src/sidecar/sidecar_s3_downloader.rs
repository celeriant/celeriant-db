use celeriant_shard::error::s3_catchup_error::S3CatchupError;
use celeriant_shard::s3_downloader::{S3Downloader, S3ObjectRef};

use crate::sidecar::error::ErrorKind;
use crate::sidecar::sidecar_channels::SidecarSenders;
use crate::sidecar::sidecar_messages::SidecarTarget;

pub struct SidecarS3Downloader {
    senders: SidecarSenders,
}

impl SidecarS3Downloader {
    pub fn new(senders: SidecarSenders) -> Self {
        Self { senders }
    }
}

impl S3Downloader for SidecarS3Downloader {
    async fn list_objects(&self, prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError> {
        let request = celeriant_sidecar::request::Request::ObjectList {
            prefix: prefix.to_string(),
        };

        match self.senders.send_async(SidecarTarget::DataPlaneReplication, request).await {
            Ok(celeriant_sidecar::response::Response::ObjectList { objects }) => {
                Ok(objects.into_iter().map(|o| S3ObjectRef { path: o.path, size: o.size }).collect())
            }
            Ok(_) => unreachable!("ObjectList returned non-ObjectList response"),
            Err(err) => Err(map_error(err, |msg| S3CatchupError::S3ListFailed {
                prefix: prefix.to_string(),
                message: msg,
            })),
        }
    }

    async fn download(&self, path: &str) -> Result<bytes::Bytes, S3CatchupError> {
        let request = celeriant_sidecar::request::Request::ObjectGet {
            path: path.to_string(),
        };

        match self.senders.send_async(SidecarTarget::DataPlaneReplication, request).await {
            Ok(celeriant_sidecar::response::Response::ObjectGet { data, .. }) => Ok(data),
            Ok(_) => unreachable!("ObjectGet returned non-ObjectGet response"),
            Err(err) => Err(map_error(err, |msg| S3CatchupError::S3GetFailed {
                path: path.to_string(),
                message: msg,
            })),
        }
    }

    async fn delete(&self, path: &str) -> Result<(), S3CatchupError> {
        let request = celeriant_sidecar::request::Request::ObjectDelete {
            path: path.to_string(),
        };

        match self.senders.send_async(SidecarTarget::DataPlaneReplication, request).await {
            Ok(_) => Ok(()),
            Err(err) => Err(map_error(err, |msg| S3CatchupError::S3DeleteFailed {
                path: path.to_string(),
                message: msg,
            })),
        }
    }
}

fn map_error(
    err: crate::sidecar::error::SidecarError,
    store_error: impl FnOnce(String) -> S3CatchupError,
) -> S3CatchupError {
    match err.kind {
        ErrorKind::ChannelClosed | ErrorKind::TokioRuntimeFailure => S3CatchupError::SidecarUnavailable,
        ErrorKind::StoreError(_) => store_error(err.message),
    }
}
