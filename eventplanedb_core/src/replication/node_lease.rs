use std::{collections::HashSet, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use eventplanedb_structures::{lease_info::{CachedLease, LeaseInfo}};
use glommio::sync::RwLock;

use crate::{cache::{lease_error::LeaseError}, node_config::NodeConfig, object_store::{LeaseOps, ObjectStoreGateway}};

pub struct NodeLease {
    node_config: NodeConfig,
    lease_info: RwLock<Option<CachedLease>>,
    lease_ops: LeaseOps,
}

impl NodeLease {
    pub fn new(
        node_config: NodeConfig,
        gateway: ObjectStoreGateway,
        subfolder: Option<String>,
    ) -> Self {
        let lease_ops = LeaseOps::new(gateway, subfolder, node_config.lease_expiry_ms);
        Self {
            lease_info: RwLock::new(None),
            node_config,
            lease_ops: lease_ops,
        }
    }

    /// Check if the current node is the active leader for the aggregate.
    /// Will error if this node is not the current leader or it is ineligible to be a leader.
    fn check_is_leader(&self, active_lease: &LeaseInfo) -> Result<Option<u64>, LeaseError> {
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if active_lease.is_leader(self.node_config.node_id) && !active_lease.is_expiring_soon(current_time_ms, self.node_config.margin_ms) {
            return Ok(Some(active_lease.lease_index));
        }

        if !active_lease.is_leader(self.node_config.node_id) && !active_lease.is_expired(current_time_ms) {
            return Err(LeaseError::NotLeader { leader_node_id: active_lease.node_id });
        }

        Ok(None)
    }

    /// Fetch the current lease from S3.
    async fn get_active_lease(&self) -> Result<Option<CachedLease>, LeaseError> {
        let deadline = Some(Instant::now() + Duration::from_millis(self.node_config.margin_ms));
        
        match self.lease_ops.get_lease(deadline).await {
            Ok(Some(cached_lease)) => Ok(Some(cached_lease)),
            Ok(None) => Ok(None),
            Err(e) => {
                log::warn!("Failed to get lease from S3: {}", e);
                Err(LeaseError::ControlPlaneOffline)
            }
        }
    }

    /// Try to claim the lease for this node to become leader.
    async fn try_take_lease(&self, current_lease: Option<&CachedLease>) -> Result<CachedLease, LeaseError> {

        // How much total time we allocate to the sidecar to take the lease using s3
        let deadline = Some(Instant::now() + Duration::from_millis(self.node_config.margin_ms));
        
        let result = match current_lease {
            Some(cached) => {
                self.lease_ops
                    .try_update_lease(
                        cached,
                        self.node_config.node_id,
                        deadline,
                    )
                    .await
            }
            None => {
                self.lease_ops
                    .try_create_lease(
                        self.node_config.node_id,
                        deadline,
                    )
                    .await
            }
        };

        match result {
            Ok(cached_lease) => Ok(cached_lease),
            Err(e) => {
                if e.kind == crate::object_store::error::ErrorKind::PreconditionFailed {
                    // Another node took the lease, need to re-fetch
                    log::info!("Lease contention for cluster, another node won");
                    Err(LeaseError::CannotBeLeader)
                } else {
                    log::warn!("Failed to take lease: {}", e);
                    Err(LeaseError::ControlPlaneOffline)
                }
            }
        }
    }

    // /// Used for the write path - ensure this node is the leader for this aggregate.
    // /// Returns (lease_index, taking_lease_from_other_node).
    // /// If taking_lease_from_other_node is true, the caller needs to check for any missing batches it needs to catch up on.
    // pub async fn must_be_leader(&self) -> Result<(u64, bool), LeaseError> {
    //     if !self.node_config.s3_enabled {
    //         return Ok((0, false));
    //     }

    //     // Check cached lease first
    //     {
    //         let reader = self.lease_info.read().await?;
    //         if let Some(cached) = reader.as_ref() {
    //             if let Some(lease_index) = self.check_is_leader(&cached.lease_info)? {
    //                 return Ok((lease_index, false));
    //             }
    //         }
    //     }

    //     // Exclusive write lock to avoid multiple connections for same aggregate contention
    //     let mut writer = self.lease_info.write().await?;

    //     // Now we have write guard, check cache again!
    //     if let Some(cached) = writer.as_ref() {
    //         if let Some(lease_index) = self.check_is_leader(&cached.lease_info)? {
    //             return Ok((lease_index, false));
    //         }
    //     }

    //     // No cached lease or it's expired. Fetch from S3.
    //     let mut taking_lease_from_other_node = false;
    //     let fetched_lease = self.get_active_lease().await?;

    //     if let Some(ref cached) = fetched_lease {
    //         // Update cache
    //         *writer = Some(cached.clone());

    //         if let Some(lease_index) = self.check_is_leader(&cached.lease_info)? {
    //             return Ok((lease_index, false));
    //         }

    //         taking_lease_from_other_node = cached.lease_info.node_id != self.node_config.node_id;
    //     }

    //     // Either no lease or it's expired, and this node can become the leader.
    //     let cached_lease = self.try_take_lease(fetched_lease.as_ref()).await?;

    //     // Update cache with new lease
    //     let mut available_leaders = HashSet::new();
    //     available_leaders.insert(self.node_config.node_id);
        
    //     let lease_index = cached_lease.lease_info.lease_index;

    //     *writer = Some(cached_lease);

    //     Ok((lease_index, taking_lease_from_other_node))
    // }

    /// Used for the write path - ensure this node is the leader and return the full lease info.
    pub async fn must_be_leader_and_get_lease(&self) -> Result<LeaseInfo, LeaseError> {
        if !self.node_config.s3_enabled {
            return Ok(LeaseInfo::default());
        }

        // Check cached lease first
        {
            let reader = self.lease_info.read().await?;
            if let Some(cached) = reader.as_ref() {
                if let Some(_lease_index) = self.check_is_leader(&cached.lease_info)? {
                    return Ok(cached.lease_info.clone());
                }
            }
        }

        // Exclusive write lock to avoid contention
        let mut writer = self.lease_info.write().await?;

        // Double-check after acquiring write lock
        if let Some(cached) = writer.as_ref() {
            if let Some(_lease_index) = self.check_is_leader(&cached.lease_info)? {
                return Ok(cached.lease_info.clone());
            }
        }

        // Fetch from S3
        let fetched_lease = self.get_active_lease().await?;

        if let Some(ref cached) = fetched_lease {
            *writer = Some(cached.clone());

            if let Some(_lease_index) = self.check_is_leader(&cached.lease_info)? {
                return Ok(cached.lease_info.clone());
            }
        }

        // Try to take the lease
        let cached_lease = self.try_take_lease(fetched_lease.as_ref()).await?;
        let lease_info = cached_lease.lease_info.clone();
        *writer = Some(cached_lease);

        Ok(lease_info)
    }

    /// Try to renew the lease early (before expiration) if we're the current leader.
    pub async fn try_early_renew(&self) -> Result<LeaseInfo, LeaseError> {
        if !self.node_config.s3_enabled {
            return Ok(LeaseInfo::default());
        }

        let mut writer = self.lease_info.write().await?;

        let current_lease = match writer.as_ref() {
            Some(cached) => cached,
            None => return Err(LeaseError::CannotBeLeader),
        };

        let current_time_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Only renew if we're the leader and lease is expiring soon
        if !current_lease.lease_info.is_leader(self.node_config.node_id) {
            return Err(LeaseError::NotLeader {
                leader_node_id: current_lease.lease_info.node_id,
            });
        }

        if !current_lease.lease_info.is_expiring_soon(current_time_ms, self.node_config.margin_ms) {
            // Lease is still fresh, return current
            return Ok(current_lease.lease_info.clone());
        }

        // Renew the lease
        let cached_lease = self.try_take_lease(Some(current_lease)).await?;
        let lease_info = cached_lease.lease_info.clone();
        *writer = Some(cached_lease);

        Ok(lease_info)
    }
}