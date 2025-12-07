use crate::s3_config::S3Config;

#[derive(Clone)]
pub struct StoreConfig {
    /// Configuration if S3 control plane is enabled
    pub s3: Option<S3Config>,
}