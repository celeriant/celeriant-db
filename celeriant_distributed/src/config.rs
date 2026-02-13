use std::time::Duration;

/// Configuration for a node participating in distributed replication.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub node_id: u128,
    pub client_address: String,
    pub replication_address: String,
    pub num_shards: u32,
    pub initial_lease_duration: Duration,
    pub min_lease_duration: Duration,
    pub max_lease_duration: Duration,
    pub max_clock_drift: Duration,
    pub replication_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub max_missed_heartbeats: u32,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            client_address: "0.0.0.0:10000".into(),
            replication_address: "0.0.0.0:10001".into(),
            num_shards: 1,
            initial_lease_duration: Duration::from_secs(5),
            min_lease_duration: Duration::from_secs(1),
            max_lease_duration: Duration::from_secs(30),
            max_clock_drift: Duration::from_millis(500),
            replication_timeout: Duration::from_secs(2),
            heartbeat_interval: Duration::from_millis(500),
            max_missed_heartbeats: 3,
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

    /// Calculate heartbeat timeout based on config.
    pub fn heartbeat_timeout(&self) -> Duration {
        self.heartbeat_interval * self.max_missed_heartbeats
    }

    /// How long shards trust their status without a heartbeat refresh.
    /// Gives enough headroom for max_missed_heartbeats + clock drift before self-fencing.
    pub fn status_ttl_ms(&self) -> u64 {
        (self.heartbeat_timeout() + self.max_clock_drift).as_millis() as u64
    }

    /// Validates the configuration for consistency.
    pub fn validate(&self) -> Result<(), String> {
        let timeout = self.heartbeat_timeout();

        if timeout <= self.heartbeat_interval * 2 {
            return Err(format!(
                "heartbeat_timeout ({:?}) must be > 2x heartbeat_interval ({:?})",
                timeout, self.heartbeat_interval
            ));
        }

        if self.max_clock_drift >= timeout {
            return Err(format!(
                "max_clock_drift ({:?}) must be < heartbeat_timeout ({:?})",
                self.max_clock_drift, timeout
            ));
        }

        if self.min_lease_duration >= self.max_lease_duration {
            return Err(format!(
                "min_lease_duration ({:?}) must be < max_lease_duration ({:?})",
                self.min_lease_duration, self.max_lease_duration
            ));
        }

        if self.initial_lease_duration < self.min_lease_duration
            || self.initial_lease_duration > self.max_lease_duration
        {
            return Err(format!(
                "initial_lease_duration ({:?}) must be between min ({:?}) and max ({:?})",
                self.initial_lease_duration, self.min_lease_duration, self.max_lease_duration
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = ReplicationConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn heartbeat_timeout_too_small() {
        let config = ReplicationConfig {
            heartbeat_interval: Duration::from_millis(500),
            max_missed_heartbeats: 2,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn max_clock_drift_too_large() {
        let config = ReplicationConfig {
            max_clock_drift: Duration::from_secs(10),
            heartbeat_interval: Duration::from_millis(500),
            max_missed_heartbeats: 3,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn min_lease_greater_than_max() {
        let config = ReplicationConfig {
            min_lease_duration: Duration::from_secs(10),
            max_lease_duration: Duration::from_secs(5),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn initial_lease_out_of_bounds() {
        let config = ReplicationConfig {
            initial_lease_duration: Duration::from_secs(50),
            min_lease_duration: Duration::from_secs(1),
            max_lease_duration: Duration::from_secs(30),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
