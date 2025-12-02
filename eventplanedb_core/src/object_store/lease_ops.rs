//! Lease operations over S3 using the object store gateway.

use std::collections::HashSet;
use std::time::Instant;

use bytes::Bytes;
use eventplanedb_structures::aggregate_key::AggregateKey;
use eventplanedb_structures::lease_info::LeaseInfo;

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
    pub fn new(gateway: ObjectStoreGateway, subfolder: Option<String>, lease_duration_ms: u64) -> Self {
        Self {
            gateway,
            subfolder,
            lease_duration_ms,
        }
    }

    fn lease_path(&self, aggregate_key: &AggregateKey) -> String {
        let base = format!(
            "leases/{}/{}/{}",
            aggregate_key.org_id, aggregate_key.aggregate_type_id, aggregate_key.aggregate_id
        );
        match &self.subfolder {
            Some(folder) => format!("{}/{}", folder, base),
            None => base,
        }
    }

    /// Get the current lease for an aggregate, if one exists.
    pub async fn get_lease(
        &self,
        aggregate_key: &AggregateKey,
        deadline: Option<Instant>,
    ) -> Result<Option<(LeaseInfo, String)>, ObjectStoreError> {
        let path = self.lease_path(aggregate_key);

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
                Ok(Some((lease_info, etag)))
            }
            Err(e) if e.kind == super::error::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
            _ => Err(ObjectStoreError::permanent("Unexpected response type")),
        }
    }

    /// Try to acquire a new lease (create-only, fails if lease exists).
    pub async fn try_create_lease(
        &self,
        aggregate_key: &AggregateKey,
        node_id: u128,
        available_leaders: HashSet<u128>,
        deadline: Option<Instant>,
    ) -> Result<(u64, String), ObjectStoreError> {
        let path = self.lease_path(aggregate_key);
        let lease_expiry_ms = current_time_ms() + self.lease_duration_ms;

        let lease_info = LeaseInfo::new(1, node_id, lease_expiry_ms, available_leaders);
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
                let etag = e_tag.ok_or_else(|| {
                    ObjectStoreError::permanent("S3 returned no ETag after PUT")
                })?;
                Ok((lease_info.lease_index, etag))
            }
            _ => Err(ObjectStoreError::permanent("Unexpected response type")),
        }
    }

    /// Try to update an existing lease (conditional on ETag).
    pub async fn try_update_lease(
        &self,
        aggregate_key: &AggregateKey,
        node_id: u128,
        new_lease_index: u64,
        available_leaders: HashSet<u128>,
        expected_etag: &str,
        deadline: Option<Instant>,
    ) -> Result<(u64, String), ObjectStoreError> {
        let path = self.lease_path(aggregate_key);
        let lease_expiry_ms = current_time_ms() + self.lease_duration_ms;

        let lease_info = LeaseInfo::new(new_lease_index, node_id, lease_expiry_ms, available_leaders);
        let data = serialize_lease(&lease_info)?;

        let result = self
            .gateway
            .execute(
                ObjectStoreTarget::ControlPlaneLease,
                ObjectStoreOp::Put {
                    path,
                    data,
                    condition: PutCondition::IfMatchETag(expected_etag.to_string()),
                },
                deadline,
            )
            .await?;

        match result {
            ObjectStoreResult::Put { e_tag } => {
                let etag = e_tag.ok_or_else(|| {
                    ObjectStoreError::permanent("S3 returned no ETag after PUT")
                })?;
                Ok((lease_info.lease_index, etag))
            }
            _ => Err(ObjectStoreError::permanent("Unexpected response type")),
        }
    }

    /// Release a lease by deleting it.
    pub async fn release_lease(
        &self,
        aggregate_key: &AggregateKey,
        deadline: Option<Instant>,
    ) -> Result<(), ObjectStoreError> {
        let path = self.lease_path(aggregate_key);

        self.gateway
            .execute(
                ObjectStoreTarget::ControlPlaneLease,
                ObjectStoreOp::Delete { path },
                deadline,
            )
            .await?;

        Ok(())
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
        "lease_index": lease.lease_index,
        "node_id": lease.node_id.to_string(),
        "lease_expiry_ms": lease.lease_expiry_ms,
        "available_leaders": lease.available_leaders.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
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

    let lease_expiry_ms = json["lease_expiry_ms"]
        .as_u64()
        .ok_or_else(|| ObjectStoreError::permanent("Missing lease_expiry_ms"))?;

    let available_leaders: HashSet<u128> = json["available_leaders"]
        .as_array()
        .ok_or_else(|| ObjectStoreError::permanent("Missing available_leaders"))?
        .iter()
        .filter_map(|v| v.as_str()?.parse().ok())
        .collect();

    Ok(LeaseInfo::new(lease_index, node_id, lease_expiry_ms, available_leaders))
}