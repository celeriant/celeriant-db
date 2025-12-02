use glommio::GlommioError;

#[derive(Debug)]
pub enum LeaseError {
    GlommioError(GlommioError<()>),
    ControlPlaneOffline,
    NotLeader {
        leader_node_id: u128
    },
    CannotBeLeader
}

impl From<GlommioError<()>> for LeaseError {
    fn from(error: GlommioError<()>) -> Self {
        LeaseError::GlommioError(error)
    }
}