#[derive(Clone)]
pub struct NodeConfig {
    pub data_root_folder: String,
    pub node_id: u128,
    pub margin_ms: u64,
    pub lease_expiry_ms: u64,
    pub async_flush_ms: u64,
    pub max_open_aggregates: usize,
    pub max_request_size: Option<u32>,
    pub listen_address: String,
    pub max_event_batches_response_size: Option<usize>,
    pub s3_enabled: bool,
}

#[cfg(test)]
pub mod test_node_config {
    use crate::node_config::NodeConfig;

    pub fn test_config(data_root_folder: &str) -> NodeConfig {
        NodeConfig {
            data_root_folder: data_root_folder.to_string(),
            node_id: 0,
            margin_ms: 10,
            lease_expiry_ms: 100,
            async_flush_ms: 50,
            max_open_aggregates: 1000,
            max_request_size: None,
            listen_address: "127.0.0.1:8080".to_string(),
            max_event_batches_response_size: None,
            s3_enabled: false
        }
    }
}