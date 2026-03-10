use celeriant_wal::s3::{lease::Lease, membership::Membership};


#[derive(Debug, Clone)]
pub struct LeaseWithEtag {
    pub lease: Lease,
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct MembershipWithEtag {
    pub membership: Membership,
    pub etag: String,
}

#[derive(Debug, Clone)]
pub enum LeaseStoreError {
    AlreadyExists,
    PreconditionFailed,
    Unavailable { message: String },
}

impl std::fmt::Display for LeaseStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists => write!(f, "already exists"),
            Self::PreconditionFailed => write!(f, "precondition failed"),
            Self::Unavailable { message } => write!(f, "unavailable: {message}"),
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait LeaseStore {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError>;

    async fn put_lease_create_only(&self, lease: &Lease) -> Result<String, LeaseStoreError>;

    async fn put_lease_conditional(
        &self,
        lease: &Lease,
        etag: &str,
    ) -> Result<String, LeaseStoreError>;

    async fn get_membership(&self) -> Result<Option<MembershipWithEtag>, LeaseStoreError>;

    /// Write membership with CAS protection.
    /// `etag: None` → CreateOnly (file must not exist).
    /// `etag: Some(e)` → IfMatchETag (file must match `e`).
    async fn put_membership(
        &self,
        membership: &Membership,
        etag: Option<&str>,
    ) -> Result<(), LeaseStoreError>;
}
