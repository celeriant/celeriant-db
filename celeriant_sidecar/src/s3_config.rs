#[derive(Clone, Debug)]
pub struct S3Config {
    pub region: String,
    pub bucket: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub endpoint: Option<String>,
    pub subfolder: Option<String>,
    /// Skip signing requests (for public buckets)
    pub skip_signature: bool,
    /// Allow HTTP connections (for local testing)
    pub allow_http: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            bucket: String::new(),
            access_key_id: None,
            secret_access_key: None,
            endpoint: None,
            subfolder: None,
            skip_signature: false,
            allow_http: false,
        }
    }
}