use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use tracing::{debug, warn};

use crate::error::StoreError;
use crate::request::{PutCondition, Request};
use crate::response::{ObjectMetadata, Response};
use crate::s3_config::S3Config;
use crate::store_config::StoreConfig;

/// Trait defining the sidecar store interface for dependency injection.
#[async_trait]
pub trait SidecarStoreTrait: Send + Sync + 'static {
    async fn process_request(&self, request: Request) -> Result<Response, StoreError>;
}

pub struct SidecarStore {
    s3_client: Option<S3Client>,
}

struct S3Client {
    store: object_store::aws::AmazonS3,
    subfolder: Option<String>,
}

impl S3Client {
    fn resolve_path(&self, path: &str) -> Result<Path, StoreError> {
        let full_path = match &self.subfolder {
            Some(prefix) => format!("{}/{}", prefix.trim_matches('/'), path.trim_start_matches('/')),
            None => path.to_string(),
        };
        Path::parse(&full_path).map_err(Into::into)
    }
}

impl std::fmt::Debug for SidecarStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SidecarStore")
            .field("s3_configured", &self.s3_client.is_some())
            .finish()
    }
}

#[async_trait]
impl SidecarStoreTrait for SidecarStore {
    async fn process_request(&self, request: Request) -> Result<Response, StoreError> {
        debug!("Processing request: {:?}", request);

        match request {
            Request::ObjectPut {
                path,
                data,
                condition,
            } => self.put_object(&path, data, condition).await,

            Request::ObjectGet { path } => self.get_object(&path).await,

            Request::ObjectHead { path } => self.head_object(&path).await,

            Request::ObjectDelete { path } => self.delete_object(&path).await,

            Request::ObjectDeleteBatch { paths } => self.delete_objects(paths).await,

            Request::ObjectList { prefix } => self.list_objects(&prefix).await,
        }
    }
}

impl SidecarStore {
    pub fn new(config: StoreConfig) -> Result<Self, StoreError> {
        let s3_client = if let Some(s3_config) = config.s3 {
            Some(Self::build_s3_client(s3_config)?)
        } else {
            None
        };

        Ok(Self { s3_client })
    }

    fn build_s3_client(config: S3Config) -> Result<S3Client, StoreError> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region);

        if let Some(access_key) = &config.access_key_id {
            builder = builder.with_access_key_id(access_key);
        }

        if let Some(secret_key) = &config.secret_access_key {
            builder = builder.with_secret_access_key(secret_key);
        }

        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        if config.skip_signature {
            builder = builder.with_skip_signature(true);
        }

        if config.allow_http {
            builder = builder.with_allow_http(true);
        }

        let store = builder.build().map_err(|e| StoreError::S3Error {
            message: format!("Failed to build S3 client: {}", e),
        })?;

        Ok(S3Client {
            store,
            subfolder: config.subfolder,
        })
    }

    fn s3(&self) -> Result<&S3Client, StoreError> {
        self.s3_client.as_ref().ok_or(StoreError::S3NotConfigured)
    }

    async fn put_object(
        &self,
        path: &str,
        data: Bytes,
        condition: PutCondition,
    ) -> Result<Response, StoreError> {
        let s3 = self.s3()?;
        let location = s3.resolve_path(path)?;

        let mode = match condition {
            PutCondition::None => PutMode::Overwrite,
            PutCondition::CreateOnly => PutMode::Create,
            PutCondition::IfMatchETag(etag) => PutMode::Update(UpdateVersion {
                e_tag: Some(etag),
                version: None,
            }),
        };

        let opts = PutOptions {
            mode,
            ..Default::default()
        };

        let result = s3.store.put_opts(&location, data.into(), opts).await?;

        Ok(Response::ObjectPut {
            e_tag: result.e_tag,
        })
    }

    async fn get_object(&self, path: &str) -> Result<Response, StoreError> {
        let s3 = self.s3()?;
        let location = s3.resolve_path(path)?;

        let result = s3.store.get(&location).await?;
        let meta = result.meta.clone();
        let data = result.bytes().await?;

        Ok(Response::ObjectGet {
            data,
            e_tag: meta.e_tag,
            size: meta.size,
        })
    }

    async fn head_object(&self, path: &str) -> Result<Response, StoreError> {
        let s3 = self.s3()?;
        let location = s3.resolve_path(path)?;

        let meta = s3.store.head(&location).await?;

        Ok(Response::ObjectHead(ObjectMetadata {
            path: meta.location.to_string(),
            size: meta.size,
            e_tag: meta.e_tag,
            last_modified: Some(meta.last_modified.timestamp() as u64),
        }))
    }

    async fn delete_object(&self, path: &str) -> Result<Response, StoreError> {
        let s3 = self.s3()?;
        let location = s3.resolve_path(path)?;

        s3.store.delete(&location).await?;

        Ok(Response::ObjectDelete)
    }

    async fn delete_objects(&self, paths: Vec<String>) -> Result<Response, StoreError> {
        let s3 = self.s3()?;

        let locations: Vec<Path> = paths
            .iter()
            .map(|p| s3.resolve_path(p))
            .collect::<Result<Vec<_>, _>>()?;

        let stream = futures::stream::iter(locations.into_iter().map(Ok)).boxed();

        let results: Vec<Result<Path, object_store::Error>> =
            s3.store.delete_stream(stream).collect().await;

        let failed_paths: Vec<String> = results
            .into_iter()
            .filter_map(|r| match r {
                Ok(_) => None,
                Err(e) => {
                    warn!("Failed to delete object: {}", e);
                    match e {
                        object_store::Error::NotFound { path, .. } => Some(path),
                        _ => None,
                    }
                }
            })
            .collect();

        Ok(Response::ObjectDeleteBatch { failed_paths })
    }

    async fn list_objects(&self, prefix: &str) -> Result<Response, StoreError> {
        let s3 = self.s3()?;
        let location = s3.resolve_path(prefix)?;

        let stream = s3.store.list(Some(&location));

        let objects: Vec<ObjectMetadata> = stream
            .map_ok(|meta| ObjectMetadata {
                path: meta.location.to_string(),
                size: meta.size,
                e_tag: meta.e_tag,
                last_modified: Some(meta.last_modified.timestamp() as u64),
            })
            .try_collect()
            .await?;

        Ok(Response::ObjectList { objects })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_creation_without_s3() {
        let config = StoreConfig { s3: None };
        let store = SidecarStore::new(config).unwrap();
        assert!(store.s3_client.is_none());
    }

    #[test]
    fn test_s3_not_configured_error() {
        let config = StoreConfig { s3: None };
        let store = SidecarStore::new(config).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(store.process_request(Request::ObjectGet {
            path: "test".to_string(),
        }));

        assert!(matches!(result, Err(StoreError::S3NotConfigured)));
    }
}