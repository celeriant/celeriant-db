use bytes::Bytes;
use crate::error::s3_catchup_error::S3CatchupError;

pub struct S3ObjectRef {
    pub path: String,
    pub size: u64,
}

#[allow(async_fn_in_trait)]
pub trait S3Downloader {
    async fn list_objects(&self, prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError>;
    async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError>;
    async fn delete(&self, path: &str) -> Result<(), S3CatchupError>;
}

pub struct StubS3Downloader;

impl S3Downloader for StubS3Downloader {
    async fn list_objects(&self, _prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError> {
        glommio::timer::sleep(std::time::Duration::from_millis(30)).await;
        Ok(vec![])
    }
    
    async fn download(&self, _path: &str) -> Result<Bytes, S3CatchupError> {
        glommio::timer::sleep(std::time::Duration::from_millis(30)).await;
        Ok(Bytes::new())
    }
    
    async fn delete(&self, _path: &str) -> Result<(), S3CatchupError> {
        glommio::timer::sleep(std::time::Duration::from_millis(30)).await;
        Ok(())
    }
}