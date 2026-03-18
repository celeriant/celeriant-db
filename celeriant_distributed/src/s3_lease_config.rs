use std::time::Duration;

#[derive(Debug, Clone)]
pub struct S3LeaseConfig {
    pub node_id: u128,
    pub advertised_client_address: String,
    pub advertised_replication_address: String,
    pub s3_lease_duration: Duration,
    pub max_clock_drift: Duration,
}