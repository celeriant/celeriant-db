use tracing::debug;

use celeriant_wal::s3::lease::Lease;
use celeriant_wal::s3::membership::{Membership, NodeInfo};

use crate::s3_lease_config::S3LeaseConfig;
use crate::lease_store::{LeaseStore, LeaseStoreError, MembershipWithEtag};
use crate::node_status::NodeStatus;
use crate::validated_node_status::{self, ValidatedNodeStatus};

const MEMBERSHIP_CAS_MAX_RETRIES: u32 = 5;

#[derive(Debug, Clone)]
pub struct ElectionOutcome {
    pub status: ValidatedNodeStatus,
    pub peer_info: Option<NodeInfo>,
}

pub struct S3LeaseManager<S: LeaseStore> {
    store: S,
    config: S3LeaseConfig,
}

impl<S: LeaseStore> S3LeaseManager<S> {
    pub fn new(store: S, config: S3LeaseConfig) -> Self {
        Self { store, config }
    }

    /// Add or update self on membership.bin in s3. Handles conflicts and retries.
    pub async fn register_self_on_membership_s3_object(&self) -> Result<(), LeaseStoreError> {

        for _ in 0..MEMBERSHIP_CAS_MAX_RETRIES {
            let existing = self.store.get_membership().await?;
            let (mut membership, etag) = match existing {
                Some(MembershipWithEtag { membership, etag }) => (membership, Some(etag)),
                None => (Membership::empty(), None),
            };

            membership.register(NodeInfo::new(
                self.config.node_id,
                self.config.advertised_client_address.clone(),
                self.config.advertised_replication_address.clone(),
            ));

            match self.store.put_membership(&membership, etag.as_deref()).await {
                Ok(()) => return Ok(()),
                Err(LeaseStoreError::PreconditionFailed) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(LeaseStoreError::Unavailable {
            message: format!(
                "membership CAS failed after {} retries",
                MEMBERSHIP_CAS_MAX_RETRIES
            ),
        })
    }

    /// Determine this node's role via S3 lease. No lease? Try create with self as leader.
    /// Own lease (any expiry)? Renew via CAS, keeping the same lease_epoch.
    /// Other node's expired lease? Promote via CAS, bumping lease_epoch.
    /// Other node still leader? Become follower, no lease.bin update.
    pub async fn run_election_to_acquire_s3_lease(&self) -> Result<ElectionOutcome, LeaseStoreError> {
        let now = validated_node_status::unix_epoch_now_ms();

        match self.store.get_lease().await? {
            None => {
                debug!("Election: no existing lease in S3 — racing to create");
                let lease = Lease::new_initial(
                    self.config.node_id,
                    now,
                    self.config.s3_lease_duration.as_millis() as u64,
                );
                match self.store.put_lease_create_only(&lease).await {
                    Ok(_) => self.become_leader(&lease).await,
                    Err(LeaseStoreError::AlreadyExists) => {
                        let lwe = self.store.get_lease().await?.ok_or_else(|| {
                            LeaseStoreError::Unavailable {
                                message: "lease disappeared after AlreadyExists".into(),
                            }
                        })?;
                        self.become_follower(&lwe.lease).await
                    }
                    Err(e) => Err(e),
                }
            }
            // Valid lease held by another node — follow unconditionally
            Some(lwe) if !lwe.lease.is_expired(now)
                && lwe.lease.leader_node_id != self.config.node_id =>
            {
                debug!(
                    observed_lease_epoch = lwe.lease.lease_epoch,
                    leader_node_id = lwe.lease.leader_node_id,
                    "Election: valid lease held by another node — becoming follower"
                );
                self.become_follower(&lwe.lease).await
            }
            Some(lwe) => {
                debug!(
                    observed_lease_epoch = lwe.lease.lease_epoch,
                    leader_node_id = lwe.lease.leader_node_id,
                    expired = lwe.lease.is_expired(now),
                    "Election: existing lease found — racing CAS"
                );
                let duration_ms = self.config.s3_lease_duration.as_millis() as u64;
                let next = if lwe.lease.leader_node_id == self.config.node_id {
                    lwe.lease.renew(now, duration_ms)
                } else {
                    lwe.lease.promote(self.config.node_id, now, duration_ms)
                };
                match self
                    .store
                    .put_lease_conditional(&next, &lwe.etag)
                    .await
                {
                    Ok(_) => self.become_leader(&next).await,
                    Err(LeaseStoreError::PreconditionFailed) => {
                        let new_lwe = self.store.get_lease().await?.ok_or_else(|| {
                            LeaseStoreError::Unavailable {
                                message: "lease disappeared after PreconditionFailed".into(),
                            }
                        })?;
                        self.become_follower(&new_lwe.lease).await
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    pub async fn discover_peer(&self) -> Result<Option<NodeInfo>, LeaseStoreError> {
        let membership = self.store.get_membership().await?;
        Ok(membership.and_then(|mwe| mwe.membership.peer_of(self.config.node_id).cloned()))
    }

    async fn become_follower(&self, lease: &Lease) -> Result<ElectionOutcome, LeaseStoreError> {
        let peer_info = self.discover_peer().await.ok().flatten();
        Ok(ElectionOutcome {
            status: ValidatedNodeStatus::create_custom_status(
                NodeStatus::Follower { leader_lease_epoch: lease.lease_epoch },
                self.config.max_clock_drift.as_millis() as u64,
                lease.expires_at_ms,
            ),
            peer_info,
        })
    }

    async fn become_leader(&self, lease: &Lease) -> Result<ElectionOutcome, LeaseStoreError> {
        let peer_info = self.discover_peer().await.ok().flatten();
        Ok(ElectionOutcome {
            status: ValidatedNodeStatus::create_custom_status(
                NodeStatus::Leader { lease_epoch: lease.lease_epoch },
                self.config.max_clock_drift.as_millis() as u64,
                lease.expires_at_ms,
            ),
            peer_info,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease_store::LeaseWithEtag;
    use futures::executor::block_on;
    use std::cell::RefCell;
    use std::time::Duration;

    struct MockLeaseStore {
        get_lease_responses: RefCell<Vec<Result<Option<LeaseWithEtag>, LeaseStoreError>>>,
        put_lease_create_only_responses: RefCell<Vec<Result<String, LeaseStoreError>>>,
        put_lease_conditional_responses: RefCell<Vec<Result<String, LeaseStoreError>>>,
        put_membership_responses: RefCell<Vec<Result<(), LeaseStoreError>>>,
        get_membership_responses:
            RefCell<Vec<Result<Option<MembershipWithEtag>, LeaseStoreError>>>,
        registered_memberships: RefCell<Vec<Membership>>,
    }

    impl MockLeaseStore {
        fn new() -> Self {
            Self {
                get_lease_responses: RefCell::new(vec![]),
                put_lease_create_only_responses: RefCell::new(vec![]),
                put_lease_conditional_responses: RefCell::new(vec![]),
                put_membership_responses: RefCell::new(vec![]),
                get_membership_responses: RefCell::new(vec![]),
                registered_memberships: RefCell::new(vec![]),
            }
        }

        fn push_get_lease(&self, response: Result<Option<LeaseWithEtag>, LeaseStoreError>) {
            self.get_lease_responses.borrow_mut().push(response);
        }

        fn push_put_lease_create_only(&self, response: Result<String, LeaseStoreError>) {
            self.put_lease_create_only_responses
                .borrow_mut()
                .push(response);
        }

        fn push_put_membership(&self, response: Result<(), LeaseStoreError>) {
            self.put_membership_responses
                .borrow_mut()
                .push(response);
        }

        fn push_get_membership(
            &self,
            response: Result<Option<MembershipWithEtag>, LeaseStoreError>,
        ) {
            self.get_membership_responses
                .borrow_mut()
                .push(response);
        }

        fn push_put_lease_conditional(&self, response: Result<String, LeaseStoreError>) {
            self.put_lease_conditional_responses
                .borrow_mut()
                .push(response);
        }

        fn get_registered_memberships(&self) -> Vec<Membership> {
            self.registered_memberships.borrow().clone()
        }
    }

    /// Helper: wrap a Membership in MembershipWithEtag for mock responses.
    fn membership_with_etag(m: Membership) -> MembershipWithEtag {
        MembershipWithEtag {
            membership: m,
            etag: "mock-etag".into(),
        }
    }

    impl LeaseStore for MockLeaseStore {
        async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError> {
            self.get_lease_responses.borrow_mut().remove(0)
        }

        async fn put_lease_create_only(&self, _lease: &Lease) -> Result<String, LeaseStoreError> {
            self.put_lease_create_only_responses
                .borrow_mut()
                .remove(0)
        }

        async fn put_lease_conditional(
            &self,
            _lease: &Lease,
            _etag: &str,
        ) -> Result<String, LeaseStoreError> {
            self.put_lease_conditional_responses
                .borrow_mut()
                .remove(0)
        }

        async fn get_membership(
            &self,
        ) -> Result<Option<MembershipWithEtag>, LeaseStoreError> {
            self.get_membership_responses.borrow_mut().remove(0)
        }

        async fn put_membership(
            &self,
            membership: &Membership,
            _etag: Option<&str>,
        ) -> Result<(), LeaseStoreError> {
            self.registered_memberships
                .borrow_mut()
                .push(membership.clone());
            self.put_membership_responses.borrow_mut().remove(0)
        }
    }

    fn test_config(node_id: u128) -> S3LeaseConfig {
        S3LeaseConfig {
            node_id,
            advertised_client_address: format!("127.0.0.1:{}", 10000 + node_id),
            advertised_replication_address: format!("127.0.0.1:{}", 11000 + node_id),
            s3_lease_duration: Duration::from_secs(5),
            max_clock_drift: Duration::from_millis(500)
        }
    }

    fn make_lease_with_etag(
        node_id: u128,
        lease_epoch: u64,
        now_ms: u64,
        duration_ms: u64,
    ) -> LeaseWithEtag {
        LeaseWithEtag {
            lease: Lease {
                leader_node_id: node_id,
                lease_epoch,
                acquired_at_ms: now_ms,
                expires_at_ms: now_ms + duration_ms,
            },
            etag: "etag1".into(),
        }
    }

    fn both_nodes_membership() -> Membership {
        Membership {
            nodes: [
                Some(NodeInfo::new(
                    1,
                    "127.0.0.1:10001".into(),
                    "127.0.0.1:11001".into(),
                )),
                Some(NodeInfo::new(
                    2,
                    "127.0.0.1:10002".into(),
                    "127.0.0.1:11002".into(),
                )),
            ],
        }
    }

    #[test]
    fn test_fresh_cluster_wins_race() {
        let store = MockLeaseStore::new();
        store.push_get_lease(Ok(None));
        store.push_put_lease_create_only(Ok("etag1".into()));
        store.push_get_membership(Ok(None));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Leader { lease_epoch: 1 }
        ));
        assert!(outcome.peer_info.is_none());
    }

    #[test]
    fn test_fresh_cluster_lost_race() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        store.push_get_lease(Ok(None));
        store.push_put_lease_create_only(Err(LeaseStoreError::AlreadyExists));
        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 1, now, 5000))));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Follower {
                leader_lease_epoch: 1
            }
        ));
        assert_eq!(outcome.peer_info.as_ref().unwrap().node_id, 2);
    }

    #[test]
    fn test_valid_lease_own_node_extends() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, 10000))));
        store.push_put_lease_conditional(Ok("etag2".into()));
        store.push_get_membership(Ok(None));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Leader { lease_epoch: 3 }
        ));
    }

    #[test]
    fn test_valid_lease_other_node_becomes_follower() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 5, now, 10000))));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Follower {
                leader_lease_epoch: 5
            }
        ));
        assert_eq!(outcome.peer_info.as_ref().unwrap().node_id, 2);
    }

    #[test]
    fn test_expired_lease_wins_cas() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 3, now - 10000, 5000))));
        store.push_put_lease_conditional(Ok("etag2".into()));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Leader { lease_epoch: 4 }
        ));
        assert_eq!(outcome.peer_info.as_ref().unwrap().node_id, 2);
    }

    #[test]
    fn test_expired_own_lease_wins_cas() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 2, now - 10000, 5000))));
        store.push_put_lease_conditional(Ok("etag2".into()));
        store.push_get_membership(Ok(None));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Leader { lease_epoch: 2 }
        ));
    }

    #[test]
    fn test_expired_lease_loses_cas() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 3, now - 10000, 5000))));
        store.push_put_lease_conditional(Err(LeaseStoreError::PreconditionFailed));
        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 4, now, 5000))));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Follower {
                leader_lease_epoch: 4
            }
        ));
        assert_eq!(outcome.peer_info.as_ref().unwrap().node_id, 2);
    }

    #[test]
    fn test_expired_cas_fails_lease_disappears() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 1, now - 10000, 5000))));
        store.push_put_lease_conditional(Err(LeaseStoreError::PreconditionFailed));
        store.push_get_lease(Ok(None));

        let manager = S3LeaseManager::new(store, test_config(1));
        let result = block_on(manager.run_election_to_acquire_s3_lease());

        assert!(matches!(result, Err(LeaseStoreError::Unavailable { .. })));
    }

    #[test]
    fn test_register_self() {
        let store = MockLeaseStore::new();
        store.push_get_membership(Ok(None));
        store.push_put_membership(Ok(()));

        let manager = S3LeaseManager::new(store, test_config(1));
        block_on(manager.register_self_on_membership_s3_object()).unwrap();

        let memberships = manager.store.get_registered_memberships();
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].node_count(), 1);
        assert_eq!(memberships[0].nodes[0].as_ref().unwrap().node_id, 1);
        assert_eq!(memberships[0].nodes[0].as_ref().unwrap().client_address, "127.0.0.1:10001");
        assert_eq!(memberships[0].nodes[0].as_ref().unwrap().replication_address, "127.0.0.1:11001");
    }

    #[test]
    fn test_membership_cas_retry_on_conflict() {
        let store = MockLeaseStore::new();
        store.push_get_membership(Ok(None));
        store.push_put_membership(Err(LeaseStoreError::PreconditionFailed));
        let other_membership = Membership {
            nodes: [
                Some(NodeInfo::new(
                    2,
                    "127.0.0.1:10002".into(),
                    "127.0.0.1:11002".into(),
                )),
                None,
            ],
        };
        store.push_get_membership(Ok(Some(membership_with_etag(other_membership))));
        store.push_put_membership(Ok(()));

        let manager = S3LeaseManager::new(store, test_config(1));
        block_on(manager.register_self_on_membership_s3_object()).unwrap();

        let memberships = manager.store.get_registered_memberships();
        assert_eq!(memberships.len(), 2);
        assert_eq!(memberships[1].node_count(), 2);
    }

    #[test]
    fn test_discover_peer() {
        let store = MockLeaseStore::new();
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = S3LeaseManager::new(store, test_config(1));
        let peer = block_on(manager.discover_peer()).unwrap();

        assert_eq!(peer.as_ref().unwrap().node_id, 2);
        assert_eq!(peer.as_ref().unwrap().client_address, "127.0.0.1:10002");
        assert_eq!(peer.as_ref().unwrap().replication_address, "127.0.0.1:11002");
    }

    #[test]
    fn test_discover_peer_reverse() {
        let store = MockLeaseStore::new();
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = S3LeaseManager::new(store, test_config(2));
        let peer = block_on(manager.discover_peer()).unwrap();

        assert_eq!(peer.as_ref().unwrap().node_id, 1);
        assert_eq!(peer.as_ref().unwrap().client_address, "127.0.0.1:10001");
        assert_eq!(peer.as_ref().unwrap().replication_address, "127.0.0.1:11001");
    }
}

