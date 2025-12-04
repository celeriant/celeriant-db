use glommio::GlommioError;

use crate::object_store::ObjectStoreError;

#[derive(Debug, Clone)]
pub enum LeaseError {
    GlommioError,
    ObjectStoreError(ObjectStoreError),
    ControlPlaneOffline,
    NotLeader {
        leader_node_id: u128
    },
    CannotBeLeader
}

impl From<GlommioError<()>> for LeaseError {
    fn from(_error: GlommioError<()>) -> Self {
        LeaseError::GlommioError
    }
}

impl From<ObjectStoreError> for LeaseError {
    fn from(error: ObjectStoreError) -> Self {
        LeaseError::ObjectStoreError(error)
    }
}