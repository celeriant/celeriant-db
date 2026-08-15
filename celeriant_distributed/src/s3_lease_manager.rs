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
    /// True when this node reclaimed its OWN S3 lease (i.e. the lease already
    /// named this node as leader, so no leadership transfer occurred).
    /// Restart-proof: derived from the durable S3 lease holder, not in-memory epoch.
    /// False on genesis acquire, promote-over-peer, or any follower outcome.
    pub reacquired_own_lease: bool,
    /// True only when THIS election durably wrote `lease.json` (create or conditional put
    /// that S3 accepted). False when we merely re-read a lease and concluded we still hold it.
    pub cas_written: bool,
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
                    validated_node_status::unix_epoch_now_ms(),
                    self.config.s3_lease_duration.as_millis() as u64,
                );
                match self.store.put_lease_create_only(&lease).await {
                    Ok(_) => self.become_leader(&lease, false, true).await,
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
                let is_own_lease = lwe.lease.leader_node_id == self.config.node_id;
                let now_before_write = validated_node_status::unix_epoch_now_ms();
                let next = if is_own_lease {
                    lwe.lease.renew(now_before_write, duration_ms)
                } else {
                    lwe.lease.promote(self.config.node_id, now_before_write, duration_ms)
                };
                match self
                    .store
                    .put_lease_conditional(&next, &lwe.etag)
                    .await
                {
                    Ok(_) => self.become_leader(&next, is_own_lease, true).await,
                    Err(LeaseStoreError::PreconditionFailed) => {
                        let new_lwe = self.store.get_lease().await?.ok_or_else(|| {
                            LeaseStoreError::Unavailable {
                                message: "lease disappeared after PreconditionFailed".into(),
                            }
                        })?;
                        // A CAS etag-conflict on an own-lease renewal does NOT imply lost
                        // leadership. When the follower drops, two own-renewal hooks fire near
                        // simultaneously (the preemptive hook and the on-demand fallback-gate
                        // hook); one wins the CAS and bumps the etag while keeping us leader at
                        // the same epoch, the other lands here. Only demote when the lease
                        // genuinely changed hands or expired; otherwise the re-read lease is
                        // still ours and valid, so we remain the legitimate leader.
                        let still_ours = new_lwe.lease.leader_node_id == self.config.node_id;
                        let now_after_roundtrips = validated_node_status::unix_epoch_now_ms();
                        let expired = new_lwe.lease.is_expired(now_after_roundtrips);
                        let stay_leader = still_ours && !expired;
                        tracing::warn!(
                            attempted_own_lease = is_own_lease,
                            self_node_id = self.config.node_id,
                            reread_leader_node_id = new_lwe.lease.leader_node_id,
                            reread_lease_epoch = new_lwe.lease.lease_epoch,
                            reread_expired = expired,
                            election_elapsed_ms = now_after_roundtrips.saturating_sub(now),
                            still_ours,
                            stay_leader,
                            "Election CAS PreconditionFailed — re-read lease"
                        );
                        if stay_leader {
                            self.become_leader(&new_lwe.lease, true, false).await
                        } else {
                            self.become_follower(&new_lwe.lease).await
                        }
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

    /// Read-only peek of `cluster/lease.json` (no CAS). Used by the post-catchup
    /// boot-grace decision to distinguish a fresh cluster (no lease yet) from an
    /// expired lease held by another node.
    pub async fn peek_lease(&self) -> Result<Option<Lease>, LeaseStoreError> {
        Ok(self.store.get_lease().await?.map(|lwe| lwe.lease))
    }

    pub fn node_id(&self) -> u128 {
        self.config.node_id
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
            reacquired_own_lease: false,
            cas_written: false,
        })
    }

    async fn become_leader(&self, lease: &Lease, reacquired_own_lease: bool, cas_written: bool) -> Result<ElectionOutcome, LeaseStoreError> {
        let peer_info = self.discover_peer().await.ok().flatten();
        Ok(ElectionOutcome {
            status: ValidatedNodeStatus::create_custom_status(
                NodeStatus::Leader { lease_epoch: lease.lease_epoch },
                self.config.max_clock_drift.as_millis() as u64,
                lease.expires_at_ms,
            ),
            peer_info,
            reacquired_own_lease,
            cas_written,
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
        get_lease_delay_ms: std::cell::Cell<u64>,
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
                get_lease_delay_ms: std::cell::Cell::new(0),
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
            // Simulates a slow S3 round-trip so a test can let a lease lapse mid-election.
            let delay = self.get_lease_delay_ms.get();
            if delay > 0 {
                std::thread::sleep(Duration::from_millis(delay));
            }
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
        test_config_with_lease_ms(node_id, 5000)
    }

    fn test_config_with_lease_ms(node_id: u128, lease_ms: u64) -> S3LeaseConfig {
        S3LeaseConfig {
            node_id,
            advertised_client_address: format!("127.0.0.1:{}", 10000 + node_id),
            advertised_replication_address: format!("127.0.0.1:{}", 11000 + node_id),
            s3_lease_duration: Duration::from_millis(lease_ms),
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

    // --- cas_written: only a durable write may refresh the S3-confirmation signal ---
    //
    // Callers gate `s3_cas_confirmed_at_ms` on `cas_written`, and that cell is what opens the
    // S3-fallback durability gate. Reporting a write that did not happen lets a node ack
    // durability off a lease nobody re-confirmed.

    #[test]
    fn genesis_acquire_reports_a_cas_write() {
        let store = MockLeaseStore::new();
        store.push_get_lease(Ok(None));
        store.push_put_lease_create_only(Ok("etag1".into()));
        store.push_get_membership(Ok(None));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(outcome.cas_written, "a successful create-only put IS a durable write");
    }

    #[test]
    fn successful_conditional_put_reports_a_cas_write() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();
        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, 30_000))));
        store.push_put_lease_conditional(Ok("etag2".into()));
        store.push_get_membership(Ok(None));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(outcome.status.raw().is_leader());
        assert!(outcome.cas_written, "an accepted conditional put IS a durable write");
    }

    /// The split-brain-relevant case. Our CAS is rejected; we re-read and find the lease is
    /// still ours and live, so we stay leader — but we wrote nothing, so we must not claim a
    /// fresh S3 confirmation.
    #[test]
    fn precondition_failed_but_still_ours_reports_no_cas_write() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();
        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, 30_000))));
        store.push_put_lease_conditional(Err(LeaseStoreError::PreconditionFailed));
        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, 30_000))));
        store.push_get_membership(Ok(None));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(outcome.status.raw().is_leader(), "still our live lease — leadership is retained");
        assert!(
            !outcome.cas_written,
            "our conditional put was REJECTED; re-reading a lease is not a durable write"
        );
    }

    /// Expiry must be judged against the clock AFTER the election's S3 round-trips, not the
    /// reading taken at entry. Here the lease is live when the election starts and lapses while
    /// the (slow) re-read is in flight. Judging on the entry clock would keep this node leader
    /// on a dead lease — exactly the state that lets a batch be written on an expired lease.
    #[test]
    fn lease_that_lapses_during_a_slow_election_is_not_treated_as_live() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();
        // Live at entry (60ms of life), dead by the time the 200ms re-read returns.
        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, 60))));
        store.push_put_lease_conditional(Err(LeaseStoreError::PreconditionFailed));
        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, 60))));
        store.push_get_membership(Ok(None));
        store.get_lease_delay_ms.set(200);

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(
            !outcome.status.raw().is_leader(),
            "the lease expired mid-election; staying leader on it risks a stale-writer ack"
        );
        assert!(!outcome.cas_written);
    }

    /// D11 decision sweep — does the stale-clock fix demote in cases the old code kept as leader?
    ///
    /// Deliberately asserts almost nothing. It prints a decision table for the
    /// `PreconditionFailed -> still ours` branch across (lease life at entry) x (per-round-trip
    /// delay). Run it on this commit and on 2e27358 and diff the two tables: the difference IS
    /// the D11 answer.
    ///
    /// This exists because three separate cluster injections failed to reach this branch — the
    /// heartbeat kept the lease alive, then an infinite `retry_s3_operation` blocked the
    /// orchestrator inside the paused store instead of letting it fence. A branch this narrow is
    /// cheaper and more honestly measured here than chased around a live cluster.
    #[test]
    fn d11_demotion_decision_sweep() {
        println!("D11-SWEEP\tlease_life_ms\tdelay_ms\tdecision");
        let mut demotions = 0;
        for lease_life_ms in [30_000u64, 1_000, 300, 120, 60] {
            for delay_ms in [0u64, 50, 100, 200, 400] {
                let store = MockLeaseStore::new();
                let now = validated_node_status::unix_epoch_now_ms();
                store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, lease_life_ms))));
                store.push_put_lease_conditional(Err(LeaseStoreError::PreconditionFailed));
                store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, lease_life_ms))));
                store.push_get_membership(Ok(None));
                store.get_lease_delay_ms.set(delay_ms);

                let manager = S3LeaseManager::new(store, test_config(1));
                let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();
                let decision = if outcome.status.raw().is_leader() { "stay_leader" } else { "DEMOTE" };
                if decision == "DEMOTE" {
                    demotions += 1;
                }
                println!("D11-SWEEP\t{lease_life_ms}\t{delay_ms}\t{decision}");
            }
        }
        println!("D11-SWEEP-TOTAL\tdemotions={demotions}/25");
    }

    /// The write path has the same stale-clock hazard as the re-read path, and it is worse:
    /// here we DO report `cas_written`, so the caller stamps a fresh confirmation and opens the
    /// S3-fallback gate. If the lease we wrote was built from the pre-round-trip clock, it can
    /// already be expired by the time the put returns, leaving the gate open on a dead lease.
    #[test]
    fn slow_election_writes_a_lease_that_is_still_live_when_it_lands() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();
        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 3, now, 100))));
        store.push_put_lease_conditional(Ok("etag2".into()));
        store.push_get_membership(Ok(None));
        // The `get_lease` round-trip alone outlives the whole lease duration.
        store.get_lease_delay_ms.set(250);

        let manager = S3LeaseManager::new(store, test_config_with_lease_ms(1, 100));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(outcome.status.raw().is_leader());
        assert!(outcome.cas_written, "the conditional put was accepted");
        let landed_at = validated_node_status::unix_epoch_now_ms();
        assert!(
            outcome.status.lease_expires_at_ms() > landed_at,
            "the lease this election wrote expired {}ms before the put returned, yet cas_written \
             is true — the caller will stamp a fresh confirmation and open the durability gate \
             on a dead lease",
            landed_at.saturating_sub(outcome.status.lease_expires_at_ms())
        );
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

    /// Benign CAS conflict during our own-lease renewal: a concurrent own-renewal
    /// won the etag race but the re-read lease is STILL ours and unexpired. We must
    /// stay leader, not self-demote (the degraded-mode no-leader-stall bug).
    #[test]
    fn test_own_lease_benign_cas_conflict_stays_leader() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 1, now, 10000))));
        store.push_put_lease_conditional(Err(LeaseStoreError::PreconditionFailed));
        // Re-read: still ours (node 1), same epoch, still valid.
        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 1, now, 10000))));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(
            matches!(outcome.status.raw(), NodeStatus::Leader { lease_epoch: 1 }),
            "own valid lease after benign CAS conflict must stay Leader, got {:?}",
            outcome.status.raw()
        );
        assert!(outcome.reacquired_own_lease, "staying on own lease must set reacquired_own_lease=true");
    }

    /// CAS conflict during our own-lease renewal where a peer genuinely took over:
    /// the re-read lease names another node at a higher epoch. We must demote.
    #[test]
    fn test_own_lease_cas_conflict_peer_took_over_becomes_follower() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 1, now, 10000))));
        store.push_put_lease_conditional(Err(LeaseStoreError::PreconditionFailed));
        // Re-read: a peer (node 2) now holds a fresh, valid lease at epoch 2.
        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 2, now, 10000))));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(
            matches!(outcome.status.raw(), NodeStatus::Follower { leader_lease_epoch: 2 }),
            "genuine peer takeover must demote to Follower, got {:?}",
            outcome.status.raw()
        );
        assert!(!outcome.reacquired_own_lease);
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

    #[test]
    fn test_restart_self_reclaim_sets_reacquired_own_lease() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        // S3 lease is held by THIS node (node 1), not yet expired.
        store.push_get_lease(Ok(Some(make_lease_with_etag(1, 1, now, 10000))));
        store.push_put_lease_conditional(Ok("etag2".into()));
        store.push_get_membership(Ok(None));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(outcome.status.raw(), NodeStatus::Leader { lease_epoch: 1 }));
        assert!(outcome.reacquired_own_lease, "self-reclaim must set reacquired_own_lease=true");
    }

    #[test]
    fn test_promote_over_peer_clears_reacquired_own_lease() {
        let store = MockLeaseStore::new();
        let now = validated_node_status::unix_epoch_now_ms();

        // Peer (node 2) held the lease but it expired.
        store.push_get_lease(Ok(Some(make_lease_with_etag(2, 3, now - 10000, 5000))));
        store.push_put_lease_conditional(Ok("etag2".into()));
        store.push_get_membership(Ok(Some(membership_with_etag(both_nodes_membership()))));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(outcome.status.raw(), NodeStatus::Leader { lease_epoch: 4 }));
        assert!(!outcome.reacquired_own_lease, "promote-over-peer must set reacquired_own_lease=false");
    }

    /// Genesis acquire (fresh cluster, no prior lease): reacquired_own_lease=false.
    #[test]
    fn test_genesis_acquire_clears_reacquired_own_lease() {
        let store = MockLeaseStore::new();
        store.push_get_lease(Ok(None));
        store.push_put_lease_create_only(Ok("etag1".into()));
        store.push_get_membership(Ok(None));

        let manager = S3LeaseManager::new(store, test_config(1));
        let outcome = block_on(manager.run_election_to_acquire_s3_lease()).unwrap();

        assert!(matches!(outcome.status.raw(), NodeStatus::Leader { lease_epoch: 1 }));
        assert!(!outcome.reacquired_own_lease, "genesis acquire must set reacquired_own_lease=false");
    }
}

