use celeriant_wal::s3::lease::Lease;
use celeriant_wal::s3::membership::{Membership, NodeInfo};

use crate::config::ReplicationConfig;
use crate::heartbeat::now_ms;
use crate::lease_store::{LeaseStore, LeaseStoreError, MembershipWithEtag};
use crate::node_status::NodeStatus;
use crate::validated_node_status::ValidatedNodeStatus;

/// Max CAS retries for membership updates. In a 2-node cluster, one retry
/// suffices — the second attempt merges the concurrent write. Extra retries
/// guard against transient S3 inconsistency.
const MEMBERSHIP_CAS_MAX_RETRIES: u32 = 5;

#[derive(Debug, Clone)]
pub struct ElectionOutcome {
    pub status: ValidatedNodeStatus,
    pub peer_info: Option<NodeInfo>,
}

pub struct LeaseManager<S: LeaseStore> {
    store: S,
    config: ReplicationConfig,
}

impl<S: LeaseStore> LeaseManager<S> {
    pub fn new(store: S, config: ReplicationConfig) -> Self {
        Self { store, config }
    }

    /// Read-modify-write membership with CAS to register this node.
    /// On conflict, re-reads and retries. The S3 round-trip (~100ms)
    /// provides natural backoff between retries.
    pub async fn register_self(&self) -> Result<(), LeaseStoreError> {

        for _ in 0..MEMBERSHIP_CAS_MAX_RETRIES {
            let existing = self.store.get_membership().await?;
            let (mut membership, etag) = match existing {
                Some(MembershipWithEtag { membership, etag }) => (membership, Some(etag)),
                None => (Membership::empty(), None),
            };

            membership.register(NodeInfo::new(
                self.config.node_id,
                self.config.client_address.clone(),
                self.config.replication_address.clone(),
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

    /// Determine this node's role via S3 lease.
    ///
    /// - No lease: fresh cluster, race with CreateOnly.
    /// - Valid lease: authoritative — resume if ours, follow if theirs.
    /// - Expired lease: race with CAS. Exactly one node wins.
    ///
    /// Used at boot and on heartbeat failure. The lease state determines
    /// the outcome — callers don't need different methods.
    pub async fn run_election(&self) -> Result<ElectionOutcome, LeaseStoreError> {
        let now = now_ms();

        match self.store.get_lease().await? {
            None => {
                let lease = Lease::new_initial(
                    self.config.node_id,
                    now,
                    self.config.initial_lease_duration.as_millis() as u64,
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
            Some(lwe) if !lwe.lease.is_expired(now) => {
                if lwe.lease.leader_node_id == self.config.node_id {
                    self.become_leader(&lwe.lease).await
                } else {
                    self.become_follower(&lwe.lease).await
                }
            }
            Some(lwe) => {
                let promoted = lwe.lease.promote(
                    self.config.node_id,
                    now,
                    self.config.initial_lease_duration.as_millis() as u64,
                );
                match self
                    .store
                    .put_lease_conditional(&promoted, &lwe.etag)
                    .await
                {
                    Ok(_) => self.become_leader(&promoted).await,
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
            status: ValidatedNodeStatus::new(
                NodeStatus::Follower { leader_lease_index: lease.lease_index },
                lease.expires_at_ms,
            ),
            peer_info,
        })
    }

    async fn become_leader(&self, lease: &Lease) -> Result<ElectionOutcome, LeaseStoreError> {
        let peer_info = self.discover_peer().await.ok().flatten();
        Ok(ElectionOutcome {
            status: ValidatedNodeStatus::new(
                NodeStatus::Leader { lease_index: lease.lease_index },
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

    fn test_config(node_id: u128) -> ReplicationConfig {
        ReplicationConfig {
            node_id,
            client_address: format!("127.0.0.1:{}", 10000 + node_id),
            replication_address: format!("127.0.0.1:{}", 11000 + node_id),
            initial_lease_duration: Duration::from_secs(5),
            ..Default::default()
        }
    }

    fn make_lease_with_etag(
        node_id: u128,
        lease_index: u64,
        now_ms: u64,
        duration_ms: u64,
    ) -> LeaseWithEtag {
        LeaseWithEtag {
            lease: Lease {
                leader_node_id: node_id,
                lease_index,
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

    // --- run_election: fresh cluster ---

    #[test]
    fn test_fresh_cluster_wins_race() {
        let store = MockLeaseStore::new();
        store.push_get_lease(Ok(None));
        store.push_put_lease_create_only(Ok("etag1".into()));
        store.push_get_membership(Ok(None));

        let manager = LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Leader { lease_index: 1 }
        ));
        assert!(outcome.peer_info.is_none());
    }

    #[test]
    fn test_fresh_cluster_lost_race() {
        let store = MockLeaseStore::new();
        let now = now_ms();

        store.push_get_lease(Ok(None));
        store.push_put_lease_create_only(Err(LeaseStoreError::AlreadyExists));
        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 1, now, 5000))));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Follower {
                leader_lease_index: 1
            }
        ));
        assert_eq!(outcome.peer_info.as_ref().unwrap().node_id, 2);
    }

    // --- run_election: valid lease ---

    #[test]
    fn test_valid_lease_own_node_resumes_leader() {
        let store = MockLeaseStore::new();
        let now = now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, 10000))));
        store.push_get_membership(Ok(None));

        let manager = LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Leader { lease_index: 3 }
        ));
    }

    #[test]
    fn test_valid_lease_other_node_becomes_follower() {
        let store = MockLeaseStore::new();
        let now = now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 5, now, 10000))));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Follower {
                leader_lease_index: 5
            }
        ));
        assert_eq!(outcome.peer_info.as_ref().unwrap().node_id, 2);
    }

    #[test]
    fn test_old_leader_restart_becomes_follower() {
        let store = MockLeaseStore::new();
        let now = now_ms();

        // Node 1 was the old leader. Lease now shows node 2 as leader (valid).
        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 2, now, 10000))));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Follower {
                leader_lease_index: 2
            }
        ));
        assert_eq!(outcome.peer_info.as_ref().unwrap().node_id, 2);
    }

    // --- run_election: expired lease (CAS race) ---

    #[test]
    fn test_expired_lease_wins_cas() {
        let store = MockLeaseStore::new();
        let now = now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 3, now - 10000, 5000))));
        store.push_put_lease_conditional(Ok("etag2".into()));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Leader { lease_index: 4 }
        ));
        assert_eq!(outcome.peer_info.as_ref().unwrap().node_id, 2);
    }

    #[test]
    fn test_expired_own_lease_wins_cas() {
        let store = MockLeaseStore::new();
        let now = now_ms();

        // Own expired lease — same CAS path, no special handling.
        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 2, now - 10000, 5000))));
        store.push_put_lease_conditional(Ok("etag2".into()));
        store.push_get_membership(Ok(None));

        let manager = LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Leader { lease_index: 3 }
        ));
    }

    #[test]
    fn test_expired_lease_loses_cas() {
        let store = MockLeaseStore::new();
        let now = now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 3, now - 10000, 5000))));
        store.push_put_lease_conditional(Err(LeaseStoreError::PreconditionFailed));
        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 4, now, 5000))));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election()).unwrap();

        assert!(matches!(
            outcome.status.raw(),
            NodeStatus::Follower {
                leader_lease_index: 4
            }
        ));
        assert_eq!(outcome.peer_info.as_ref().unwrap().node_id, 2);
    }

    #[test]
    fn test_expired_cas_fails_lease_disappears() {
        let store = MockLeaseStore::new();
        let now = now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 1, now - 10000, 5000))));
        store.push_put_lease_conditional(Err(LeaseStoreError::PreconditionFailed));
        store.push_get_lease(Ok(None));

        let manager = LeaseManager::new(store, test_config(1));
        let result = block_on(manager.run_election());

        assert!(matches!(result, Err(LeaseStoreError::Unavailable { .. })));
    }

    // --- register_self ---

    #[test]
    fn test_register_self() {
        let store = MockLeaseStore::new();
        store.push_get_membership(Ok(None));
        store.push_put_membership(Ok(()));

        let manager = LeaseManager::new(store, test_config(1));
        block_on(manager.register_self()).unwrap();

        let memberships = manager.store.get_registered_memberships();
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].node_count(), 1);
        assert_eq!(memberships[0].nodes[0].as_ref().unwrap().node_id, 1);
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

        let manager = LeaseManager::new(store, test_config(1));
        block_on(manager.register_self()).unwrap();

        let memberships = manager.store.get_registered_memberships();
        assert_eq!(memberships.len(), 2);
        assert_eq!(memberships[1].node_count(), 2);
    }

    // --- discover_peer ---

    #[test]
    fn test_discover_peer() {
        let store = MockLeaseStore::new();
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = LeaseManager::new(store, test_config(1));
        let peer = block_on(manager.discover_peer()).unwrap();

        assert_eq!(peer.as_ref().unwrap().node_id, 2);
    }

    #[test]
    fn test_discover_peer_reverse() {
        let store = MockLeaseStore::new();
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = LeaseManager::new(store, test_config(2));
        let peer = block_on(manager.discover_peer()).unwrap();

        assert_eq!(peer.as_ref().unwrap().node_id, 1);
    }
}

