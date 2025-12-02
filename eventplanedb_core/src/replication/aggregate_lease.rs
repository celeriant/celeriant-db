use std::{collections::HashSet, time::{SystemTime, UNIX_EPOCH}, u64};

use eventplanedb_structures::{aggregate_key::AggregateKey, lease_info::LeaseInfo};
use glommio::sync::RwLock;

use crate::{cache::lease_error::LeaseError, node_config::NodeConfig};

pub struct AggregateLease {
    aggregate_key: AggregateKey,
    node_config: NodeConfig,
    lease_info: RwLock<Option<LeaseInfo>>,
}

impl AggregateLease {
    pub fn new(
        aggregate_key: AggregateKey,
        node_config: NodeConfig,
    ) -> Self {
        Self {
            aggregate_key,
            lease_info: RwLock::new(None),
            node_config,
        }
    }

    /// Check if the curernt node is the active leader for the aggregate
    /// Will error if this node is not the current leader or it is ineligable to be a leader
    fn check_is_leader(&self, active_lease: &LeaseInfo, can_be_leader_check: bool) -> Result<Option<u64>, LeaseError> {
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if active_lease.is_leader(self.node_config.node_id) && !active_lease.is_expiring_soon(current_time_ms, self.node_config.margin_ms) {
            return Ok(Some(active_lease.lease_index));
        } 

        if !active_lease.is_leader(self.node_config.node_id) && !active_lease.is_expired(current_time_ms) {
            return Err(LeaseError::NotLeader { leader_node_id: active_lease.node_id});
        }

        if can_be_leader_check && active_lease.is_expired(current_time_ms) && !active_lease.can_be_leader(self.node_config.node_id) {
            return Err(LeaseError::CannotBeLeader);
        }

        Ok(None)
    }

    async fn get_active_lease(&self) -> Result<Option<LeaseInfo>, LeaseError> {
        //TODO: Reach out over channel to get current lease file from control plane
        Ok(Some(LeaseInfo {
            lease_index: 1,
            node_id: 0,
            lease_expiry_ms: u64::MAX,
            available_leaders: HashSet::new(),
        }))
    }
    
    async fn try_take_lease(&self, proposed_lease: LeaseInfo) -> Result<u64, LeaseError> {
        //TODO: Try to claim the lease for the node to become leader, may fail under OCC
        Ok(1)
    }

    /// Used for the write path, ensure this node is the leader for this aggregate
    /// if taking_lease_from_other_node is true, the node needs to check for any batches on s3 (degraded mode replication)
    pub async fn must_be_leader(&self) -> Result<(u64, bool), LeaseError> {

        if !self.node_config.s3_enabled {
            return Ok((0, false));
        }

        {
            let reader = self.lease_info.read().await?;
            if let Some(active_lease) = reader.as_ref() {
                if let Some(lease_index) = self.check_is_leader(&active_lease, false)? {
                    return Ok((lease_index, false));
                }
            }
        }

        // No cached lease or it's expired. Grab the latest from the control plane to see if there is a lease
        let mut taking_lease_from_other_node = false;
        let mut fetched_lease = self.get_active_lease().await?;
        if let Some(ref lease) = fetched_lease {
            let mut writer = self.lease_info.write().await?;
            *writer = Some(lease.clone());

            if let Some(lease_index) = self.check_is_leader(lease, true)? {
                return Ok((lease_index, false));
            }

            taking_lease_from_other_node = lease.node_id != self.node_config.node_id;
        }

        // Either no lease or its expired, and this node can become the leader. Try to become the leader.
        let proposed_lease = if let Some(mut lease) = fetched_lease.take() {
            lease.node_id = self.node_config.node_id;
            lease.lease_index += 1;
            lease
        } else {
            let mut available_leaders = HashSet::new();
            available_leaders.insert(self.node_config.node_id);
            LeaseInfo::new(1, self.node_config.node_id, self.node_config.lease_expiry_ms, available_leaders)
        };

        let lease_index = self.try_take_lease(proposed_lease).await?;

        Ok((lease_index, taking_lease_from_other_node))

    }
}
