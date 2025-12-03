use std::{collections::HashSet, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use eventplanedb_structures::{aggregate_key::AggregateKey, lease_info::LeaseInfo};
use glommio::sync::RwLock;

use crate::{cache::lease_error::LeaseError, node_config::NodeConfig, object_store::{ObjectStoreGateway, LeaseOps}};

pub struct AggregateLease {
    aggregate_key: AggregateKey,
    node_config: NodeConfig,
    lease_info: RwLock<Option<CachedLease>>,
    lease_ops: Option<LeaseOps>,
}

/// Cached lease with its S3 ETag for conditional updates.
#[derive(Clone)]
struct CachedLease {
    info: LeaseInfo,
    etag: String,
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
            lease_ops: None,
        }
    }

    pub fn with_gateway(
        aggregate_key: AggregateKey,
        node_config: NodeConfig,
        gateway: ObjectStoreGateway,
        subfolder: Option<String>,
    ) -> Self {
        let lease_ops = LeaseOps::new(gateway, subfolder, node_config.lease_expiry_ms);
        Self {
            aggregate_key,
            lease_info: RwLock::new(None),
            node_config,
            lease_ops: Some(lease_ops),
        }
    }

    /// Check if the current node is the active leader for the aggregate.
    /// Will error if this node is not the current leader or it is ineligible to be a leader.
    fn check_is_leader(&self, active_lease: &LeaseInfo, can_be_leader_check: bool) -> Result<Option<u64>, LeaseError> {
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

        if can_be_leader_check && active_lease.is_expired(current_time_ms) && !active_lease.can_be_leader(self.node_config.node_id) {
            return Err(LeaseError::CannotBeLeader);
        }

        Ok(None)
    }

    /// Fetch the current lease from S3.
    async fn get_active_lease(&self) -> Result<Option<CachedLease>, LeaseError> {
        let lease_ops = match &self.lease_ops {
            Some(ops) => ops,
            None => {
                // S3 not enabled, return a mock lease that makes this node always the leader
                return Ok(Some(CachedLease {
                    info: LeaseInfo {
                        lease_index: 1,
                        node_id: self.node_config.node_id,
                        lease_expiry_ms: u64::MAX,
                        available_leaders: HashSet::new(),
                    },
                    etag: String::new(),
                }));
            }
        };

        let deadline = Some(Instant::now() + Duration::from_millis(self.node_config.margin_ms));
        
        match lease_ops.get_lease(&self.aggregate_key, deadline).await {
            Ok(Some((info, etag))) => Ok(Some(CachedLease { info, etag })),
            Ok(None) => Ok(None),
            Err(e) => {
                log::warn!("Failed to get lease from S3: {}", e);
                Err(LeaseError::ControlPlaneOffline)
            }
        }
    }

    /// Try to claim the lease for this node to become leader.
    async fn try_take_lease(&self, current_lease: Option<&CachedLease>) -> Result<(u64, String), LeaseError> {
        let lease_ops = match &self.lease_ops {
            Some(ops) => ops,
            None => return Ok((1, String::new())),
        };

        //TODO: What is this deadline for? Is it a s3 timeout? Why use margin_ms which is the leader renewal margin before expiry?
        let deadline = Some(Instant::now() + Duration::from_millis(self.node_config.margin_ms));
        
        let available_leaders = current_lease
            .map(|lease| lease.info.available_leaders.clone())
            .unwrap_or_else(|| {
                let mut set = HashSet::new();
                set.insert(self.node_config.node_id);
                set
            });

        let result = match current_lease {
            Some(cached) => {
                // Update existing lease
                let new_index = cached.info.lease_index + 1;
                lease_ops
                    .try_update_lease(
                        &self.aggregate_key,
                        self.node_config.node_id,
                        new_index,
                        available_leaders,
                        &cached.etag,
                        deadline,
                    )
                    .await
            }
            None => {
                // Create new lease
                lease_ops
                    .try_create_lease(
                        &self.aggregate_key,
                        self.node_config.node_id,
                        available_leaders,
                        deadline,
                    )
                    .await
            }
        };

        match result {
            Ok((index, etag)) => Ok((index, etag)),
            Err(e) => {
                if e.kind == crate::object_store::error::ErrorKind::PreconditionFailed {
                    // Another node took the lease, need to re-fetch
                    log::info!("Lease contention for {:?}, another node won", self.aggregate_key);
                    Err(LeaseError::CannotBeLeader)
                } else {
                    log::warn!("Failed to take lease: {}", e);
                    Err(LeaseError::ControlPlaneOffline)
                }
            }
        }
    }

    /// Used for the write path - ensure this node is the leader for this aggregate.
    /// Returns (lease_index, taking_lease_from_other_node).
    /// If taking_lease_from_other_node is true, the caller needs to check for any 
    /// batches on S3 (degraded mode replication).
    pub async fn must_be_leader(&self) -> Result<(u64, bool), LeaseError> {
        if !self.node_config.s3_enabled {
            return Ok((0, false));
        }

        // Check cached lease first
        {
            let reader = self.lease_info.read().await?;
            if let Some(cached) = reader.as_ref() {
                if let Some(lease_index) = self.check_is_leader(&cached.info, false)? {
                    return Ok((lease_index, false));
                }
            }
        }

        // Exclusive write lock to avoid multiple connections for same aggregate contention
        let mut writer = self.lease_info.write().await?;

        // Now we have write guard, check cache again!
        if let Some(cached) = writer.as_ref() {
            if let Some(lease_index) = self.check_is_leader(&cached.info, false)? {
                return Ok((lease_index, false));
            }
        }

        // No cached lease or it's expired. Fetch from S3.
        let mut taking_lease_from_other_node = false;
        let fetched_lease = self.get_active_lease().await?;

        if let Some(ref cached) = fetched_lease {
            // Update cache
            *writer = Some(cached.clone());

            if let Some(lease_index) = self.check_is_leader(&cached.info, true)? {
                return Ok((lease_index, false));
            }

            taking_lease_from_other_node = cached.info.node_id != self.node_config.node_id;
        }

        // Either no lease or it's expired, and this node can become the leader.
        let (lease_index, new_etag) = self.try_take_lease(fetched_lease.as_ref()).await?;

        // Update cache with new lease
        let mut available_leaders = HashSet::new();
        available_leaders.insert(self.node_config.node_id);
        
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        *writer = Some(CachedLease {
            info: LeaseInfo::new(
                lease_index,
                self.node_config.node_id,
                current_time_ms + self.node_config.lease_expiry_ms,
                available_leaders,
            ),
            etag: new_etag,
        });

        Ok((lease_index, taking_lease_from_other_node))
    }
}