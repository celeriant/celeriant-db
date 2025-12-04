use bytes::Bytes;
use eventplanedb_structures::lease_info::{CachedLease, LeaseInfo};
use std::time::Instant;

use super::error::ObjectStoreError;
use super::gateway::ObjectStoreGateway;
use super::ops::{ObjectStoreOp, ObjectStoreResult, ObjectStoreTarget, PutCondition};

/// Operations for managing aggregate leases in S3.
pub struct LeaseOps {
    gateway: ObjectStoreGateway,
    subfolder: Option<String>,
    lease_duration_ms: u64,
}

impl LeaseOps {
    pub fn new(
        gateway: ObjectStoreGateway,
        subfolder: Option<String>,
        lease_duration_ms: u64,
    ) -> Self {
        Self {
            gateway,
            subfolder,
            lease_duration_ms,
        }
    }

    fn lease_path(&self) -> String {
        let base = "lease.json";
        match &self.subfolder {
            Some(folder) => format!("{}/{}", folder, base),
            None => base.to_string(),
        }
    }

    /// Get the current lease, if one exists.
    pub async fn get_lease(
        &self,
        deadline: Option<Instant>,
    ) -> Result<Option<CachedLease>, ObjectStoreError> {
        let path = self.lease_path();

        let result = self
            .gateway
            .execute(
                ObjectStoreTarget::ControlPlaneLease,
                ObjectStoreOp::Get { path: path.clone() },
                deadline,
            )
            .await;

        match result {
            Ok(ObjectStoreResult::Get { data, e_tag, .. }) => {
                let lease_info = deserialize_lease(&data)?;
                let etag = e_tag.ok_or_else(|| {
                    ObjectStoreError::permanent("S3 returned no ETag for lease object")
                })?;
                Ok(Some(CachedLease {lease_info, etag}))
            }
            Err(e) if e.kind == super::error::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
            _ => Err(ObjectStoreError::permanent("Unexpected response type")),
        }
    }

    /// Try to acquire a new lease (create-only, fails if lease exists).
    pub async fn try_create_lease(
        &self,
        node_id: u128,
        deadline: Option<Instant>,
    ) -> Result<CachedLease, ObjectStoreError> {
        let path = self.lease_path();
        let lease_started_on = current_time_ms();
        let lease_expires_on = lease_started_on + self.lease_duration_ms;

        let lease_info = LeaseInfo::new(1, node_id, lease_expires_on, lease_started_on);
        let data = serialize_lease(&lease_info)?;

        let result = self
            .gateway
            .execute(
                ObjectStoreTarget::ControlPlaneLease,
                ObjectStoreOp::Put {
                    path,
                    data,
                    condition: PutCondition::CreateOnly,
                },
                deadline,
            )
            .await?;

        match result {
            ObjectStoreResult::Put { e_tag } => {
                let etag = e_tag
                    .ok_or_else(|| ObjectStoreError::permanent("S3 returned no ETag after PUT"))?;
                Ok(CachedLease {lease_info, etag })
            }
            _ => Err(ObjectStoreError::permanent("Unexpected response type")),
        }
    }

    /// Try to update an existing lease (conditional on ETag).
    pub async fn try_update_lease(
        &self,
        existing_lease: &CachedLease,
        node_id: u128,
        deadline: Option<Instant>,
    ) -> Result<CachedLease, ObjectStoreError> {
        let path = self.lease_path();
        let current_time = current_time_ms();
        let lease_started_on = if existing_lease.lease_info.node_id == node_id {
            existing_lease.lease_info.lease_started_on
        } else {
            current_time
        };
        let lease_expiry_ms = current_time + self.lease_duration_ms;
        let new_lease_index = existing_lease.lease_info.lease_index + 1;

        let lease_info =
            LeaseInfo::new(new_lease_index, node_id, lease_expiry_ms, lease_started_on);
        let data = serialize_lease(&lease_info)?;

        let result = self
            .gateway
            .execute(
                ObjectStoreTarget::ControlPlaneLease,
                ObjectStoreOp::Put {
                    path,
                    data,
                    condition: PutCondition::IfMatchETag(existing_lease.etag.to_string()),
                },
                deadline,
            )
            .await?;

        match result {
            ObjectStoreResult::Put { e_tag } => {
                let etag = e_tag
                    .ok_or_else(|| ObjectStoreError::permanent("S3 returned no ETag after PUT"))?;
                Ok(CachedLease { lease_info, etag })
            }
            _ => Err(ObjectStoreError::permanent("Unexpected response type")),
        }
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn serialize_lease(lease: &LeaseInfo) -> Result<Bytes, ObjectStoreError> {
    // Simple JSON serialization for leases
    let json = serde_json::json!({
        "version": 1,
        "lease_index": lease.lease_index,
        "node_id": lease.node_id.to_string(),
        "lease_started_on": lease.lease_started_on,
        "lease_expires_on": lease.lease_expires_on,
    });

    serde_json::to_vec(&json)
        .map(Bytes::from)
        .map_err(|e| ObjectStoreError::permanent(format!("Failed to serialize lease: {}", e)))
}

fn deserialize_lease(data: &Bytes) -> Result<LeaseInfo, ObjectStoreError> {
    let json: serde_json::Value = serde_json::from_slice(data)
        .map_err(|e| ObjectStoreError::permanent(format!("Failed to deserialize lease: {}", e)))?;

    let lease_index = json["lease_index"]
        .as_u64()
        .ok_or_else(|| ObjectStoreError::permanent("Missing lease_index"))?;

    let node_id_str = json["node_id"]
        .as_str()
        .ok_or_else(|| ObjectStoreError::permanent("Missing node_id"))?;
    let node_id: u128 = node_id_str
        .parse()
        .map_err(|_| ObjectStoreError::permanent("Invalid node_id"))?;

    let lease_expires_on = json["lease_expires_on"]
        .as_u64()
        .ok_or_else(|| ObjectStoreError::permanent("Missing lease_expires_on"))?;

    let lease_started_on = json["lease_started_on"]
        .as_u64()
        .ok_or_else(|| ObjectStoreError::permanent("Missing lease_started_on"))?;

    Ok(LeaseInfo::new(
        lease_index,
        node_id,
        lease_expires_on,
        lease_started_on,
    ))
}
