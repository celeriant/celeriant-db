use std::time::Duration;

/// Configuration for a node participating in distributed replication.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub node_id: u128,
    pub client_address: String,
    pub replication_address: String,
    pub peer_client_address: Option<String>,
    pub peer_replication_address: Option<String>,
    pub num_shards: u32,
    pub initial_lease_duration: Duration,
    pub min_lease_duration: Duration,
    pub max_lease_duration: Duration,
    pub max_clock_drift: Duration,
    pub replication_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub max_missed_heartbeats: u32,
    pub bootstrap_as_leader: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            client_address: "0.0.0.0:10000".into(),
            replication_address: "0.0.0.0:10001".into(),
            peer_replication_address: None,
            peer_client_address: None,
            num_shards: 1,
            initial_lease_duration: Duration::from_secs(5),
            min_lease_duration: Duration::from_secs(1),
            max_lease_duration: Duration::from_secs(30),
            max_clock_drift: Duration::from_millis(500),
            replication_timeout: Duration::from_secs(2),
            heartbeat_interval: Duration::from_millis(500),
            max_missed_heartbeats: 3,
            bootstrap_as_leader: false,
        }
    }
}

impl ReplicationConfig {
    /// Builder-style method to set node_id.
    pub fn with_node_id(mut self, node_id: u128) -> Self {
        self.node_id = node_id;
        self
    }

    /// Builder-style method to set addresses.
    pub fn with_addresses(
        mut self,
        client_address: String,
        replication_address: String,
    ) -> Self {
        self.client_address = client_address;
        self.replication_address = replication_address;
        self
    }

    /// Builder-style method to set peer address.
    pub fn with_peer(mut self, peer_replication_address: String, peer_client_address: String) -> Self {
        self.peer_replication_address = Some(peer_replication_address);
        self.peer_client_address = Some(peer_client_address);
        self
    }

    /// Builder-style method to set bootstrap mode.
    pub fn bootstrap_leader(mut self) -> Self {
        self.bootstrap_as_leader = true;
        self
    }

    /// Calculate heartbeat timeout based on config.
    pub fn heartbeat_timeout(&self) -> Duration {
        self.heartbeat_interval * self.max_missed_heartbeats
    }
}
