/// Configuration for a single EventPlaneDB node.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Root folder for data storage.
    pub data_root_folder: String,
    /// Unique identifier for this node.
    pub node_id: u128,
    /// Delay before async flush in milliseconds.
    pub async_flush_ms: u64,
    /// Maximum number of open aggregates in cache.
    pub max_open_aggregates: usize,
    /// Maximum request size in bytes.
    pub max_request_size: Option<u32>,
    /// Server listen address.
    pub listen_address: String,
    /// Maximum event batches response size.
    pub max_event_batches_response_size: Option<usize>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            data_root_folder: "data".to_string(),
            node_id: 0,
            async_flush_ms: 100,
            max_open_aggregates: 10000,
            max_request_size: Some(16 * 1024 * 1024),
            listen_address: "0.0.0.0:10000".to_string(),
            max_event_batches_response_size: Some(64 * 1024 * 1024),
        }
    }
}

#[cfg(test)]
pub mod test_node_config {
    use crate::node_config::NodeConfig;

    pub fn test_config(data_root_folder: &str) -> NodeConfig {
        NodeConfig {
            data_root_folder: data_root_folder.to_string(),
            node_id: 0,
            async_flush_ms: 50,
            max_open_aggregates: 1000,
            max_request_size: None,
            listen_address: "127.0.0.1:8080".to_string(),
            max_event_batches_response_size: None,
        }
    }
}