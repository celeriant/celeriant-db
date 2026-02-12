use bytes::Bytes;
use celeriant_distributed::lease_store::{LeaseStore, LeaseStoreError, LeaseWithEtag, MembershipWithEtag};
use celeriant_distributed::paths::{LEASE_PATH, MEMBERSHIP_PATH};
use celeriant_wal::constants::{WIRE_VERSION_S3_LEASE, WIRE_VERSION_S3_MEMBERSHIP};
use celeriant_wal::s3::lease::Lease;
use celeriant_wal::s3::membership::Membership;
use crate::sidecar::sidecar_channels::SidecarSenders;
use crate::sidecar::sidecar_messages::SidecarTarget;
use crate::sidecar::error::ErrorKind;

pub struct SidecarLeaseStorage {
    senders: SidecarSenders,
}

impl SidecarLeaseStorage {
    pub fn new(senders: SidecarSenders) -> Self {
        Self { senders }
    }
}

impl LeaseStore for SidecarLeaseStorage {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError> {
        let request = celeriant_sidecar::request::Request::ObjectGet {
            path: LEASE_PATH.to_string(),
        };

        match self.senders.send_async(SidecarTarget::ControlPlaneLease, request).await {
            Ok(response) => match response {
                celeriant_sidecar::response::Response::ObjectGet { data, e_tag, .. } => {
                    let lease = celeriant_wire::disk::versioned_block::deserialise_lease(&data)
                        .map_err(|e| LeaseStoreError::Unavailable {
                            message: format!("Failed to deserialize lease: {:?}", e),
                        })?;

                    let etag = e_tag.ok_or_else(|| LeaseStoreError::Unavailable {
                        message: "S3 did not return ETag for lease".to_string(),
                    })?;

                    Ok(Some(LeaseWithEtag { lease, etag }))
                }
                _ => Err(LeaseStoreError::Unavailable {
                    message: "Unexpected response type for get_lease".to_string(),
                }),
            },
            Err(err) => match err.kind {
                ErrorKind::ChannelClosed | ErrorKind::TokioRuntimeFailure => {
                    Err(LeaseStoreError::Unavailable { message: err.message })
                }
                ErrorKind::StoreError(store_kind) => {
                    use celeriant_sidecar::error::ErrorKind as StoreErrorKind;
                    match store_kind {
                        StoreErrorKind::NotFound => Ok(None),
                        _ => Err(LeaseStoreError::Unavailable { message: err.message }),
                    }
                }
            }
        }
    }

    async fn put_lease_create_only(&self, lease: &Lease) -> Result<String, LeaseStoreError> {
        let data = celeriant_wire::disk::versioned_block::serialize_versioned_message_heap(lease, WIRE_VERSION_S3_LEASE)
            .map_err(|e| LeaseStoreError::Unavailable {
                message: format!("Failed to serialize lease: {}", e),
            })?;

        let request = celeriant_sidecar::request::Request::ObjectPut {
            path: LEASE_PATH.to_string(),
            data: Bytes::from(data),
            condition: celeriant_sidecar::request::PutCondition::CreateOnly,
        };

        match self.senders.send_async(SidecarTarget::ControlPlaneLease, request).await {
            Ok(response) => match response {
                celeriant_sidecar::response::Response::ObjectPut { e_tag } => {
                    e_tag.ok_or_else(|| LeaseStoreError::Unavailable {
                        message: "S3 did not return ETag for put_lease_create_only".to_string(),
                    })
                }
                _ => Err(LeaseStoreError::Unavailable {
                    message: "Unexpected response type for put_lease_create_only".to_string(),
                }),
            },
            Err(err) => match err.kind {
                ErrorKind::ChannelClosed | ErrorKind::TokioRuntimeFailure => {
                    Err(LeaseStoreError::Unavailable { message: err.message })
                }
                ErrorKind::StoreError(store_kind) => {
                    use celeriant_sidecar::error::ErrorKind as StoreErrorKind;
                    match store_kind {
                        StoreErrorKind::AlreadyExists => Err(LeaseStoreError::AlreadyExists),
                        _ => Err(LeaseStoreError::Unavailable { message: err.message }),
                    }
                }
            }
        }
    }

    async fn put_lease_conditional(
        &self,
        lease: &Lease,
        etag: &str,
    ) -> Result<String, LeaseStoreError> {
        let data = celeriant_wire::disk::versioned_block::serialize_versioned_message_heap(lease, WIRE_VERSION_S3_LEASE)
            .map_err(|e| LeaseStoreError::Unavailable {
                message: format!("Failed to serialize lease: {}", e),
            })?;

        let request = celeriant_sidecar::request::Request::ObjectPut {
            path: LEASE_PATH.to_string(),
            data: Bytes::from(data),
            condition: celeriant_sidecar::request::PutCondition::IfMatchETag(etag.to_string()),
        };

        match self.senders.send_async(SidecarTarget::ControlPlaneLease, request).await {
            Ok(response) => match response {
                celeriant_sidecar::response::Response::ObjectPut { e_tag } => {
                    e_tag.ok_or_else(|| LeaseStoreError::Unavailable {
                        message: "S3 did not return ETag for put_lease_conditional".to_string(),
                    })
                }
                _ => Err(LeaseStoreError::Unavailable {
                    message: "Unexpected response type for put_lease_conditional".to_string(),
                }),
            },
            Err(err) => match err.kind {
                ErrorKind::ChannelClosed | ErrorKind::TokioRuntimeFailure => {
                    Err(LeaseStoreError::Unavailable { message: err.message })
                }
                ErrorKind::StoreError(store_kind) => {
                    use celeriant_sidecar::error::ErrorKind as StoreErrorKind;
                    match store_kind {
                        StoreErrorKind::PreconditionFailed => Err(LeaseStoreError::PreconditionFailed),
                        _ => Err(LeaseStoreError::Unavailable { message: err.message }),
                    }
                }
            }
        }
    }

    async fn get_membership(&self) -> Result<Option<MembershipWithEtag>, LeaseStoreError> {
        let request = celeriant_sidecar::request::Request::ObjectGet {
            path: MEMBERSHIP_PATH.to_string(),
        };

        match self.senders.send_async(SidecarTarget::ControlPlaneMembership, request).await {
            Ok(response) => match response {
                celeriant_sidecar::response::Response::ObjectGet { data, e_tag, .. } => {
                    let membership = celeriant_wire::disk::versioned_block::deserialise_membership(&data)
                        .map_err(|e| LeaseStoreError::Unavailable {
                            message: format!("Failed to deserialize membership: {:?}", e),
                        })?;

                    let etag = e_tag.ok_or_else(|| LeaseStoreError::Unavailable {
                        message: "S3 did not return ETag for membership".to_string(),
                    })?;

                    Ok(Some(MembershipWithEtag { membership, etag }))
                }
                _ => Err(LeaseStoreError::Unavailable {
                    message: "Unexpected response type for get_membership".to_string(),
                }),
            },
            Err(err) => match err.kind {
                ErrorKind::ChannelClosed | ErrorKind::TokioRuntimeFailure => {
                    Err(LeaseStoreError::Unavailable { message: err.message })
                }
                ErrorKind::StoreError(store_kind) => {
                    use celeriant_sidecar::error::ErrorKind as StoreErrorKind;
                    match store_kind {
                        StoreErrorKind::NotFound => Ok(None),
                        _ => Err(LeaseStoreError::Unavailable { message: err.message }),
                    }
                }
            }
        }
    }

    async fn put_membership(
        &self,
        membership: &Membership,
        etag: Option<&str>,
    ) -> Result<(), LeaseStoreError> {
        let data = celeriant_wire::disk::versioned_block::serialize_versioned_message_heap(membership, WIRE_VERSION_S3_MEMBERSHIP)
            .map_err(|e| LeaseStoreError::Unavailable {
                message: format!("Failed to serialize membership: {}", e),
            })?;

        let condition = match etag {
            None => celeriant_sidecar::request::PutCondition::CreateOnly,
            Some(e) => celeriant_sidecar::request::PutCondition::IfMatchETag(e.to_string()),
        };

        let request = celeriant_sidecar::request::Request::ObjectPut {
            path: MEMBERSHIP_PATH.to_string(),
            data: Bytes::from(data),
            condition,
        };

        match self.senders.send_async(SidecarTarget::ControlPlaneMembership, request).await {
            Ok(response) => match response {
                celeriant_sidecar::response::Response::ObjectPut { .. } => Ok(()),
                _ => Err(LeaseStoreError::Unavailable {
                    message: "Unexpected response type for put_membership".to_string(),
                }),
            },
            Err(err) => match err.kind {
                ErrorKind::ChannelClosed | ErrorKind::TokioRuntimeFailure => {
                    Err(LeaseStoreError::Unavailable { message: err.message })
                }
                ErrorKind::StoreError(store_kind) => {
                    use celeriant_sidecar::error::ErrorKind as StoreErrorKind;
                    match store_kind {
                        StoreErrorKind::PreconditionFailed => Err(LeaseStoreError::PreconditionFailed),
                        StoreErrorKind::AlreadyExists => Err(LeaseStoreError::PreconditionFailed),
                        _ => Err(LeaseStoreError::Unavailable { message: err.message }),
                    }
                }
            }
        }
    }
}
