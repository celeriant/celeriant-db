use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use eventplanedb_core::cache::lease_error::LeaseError;
use eventplanedb_core::process_request::LeasingChannelTrait;
use eventplanedb_core::replication::node_lease::NodeLease;
use eventplanedb_structures::lease_info::LeaseInfo;
use glommio::channels::channel_mesh::Senders;
use glommio::channels::local_channel::{new_bounded, LocalSender};
use glommio::sync::RwLock;
use log::{debug, error};

use crate::Msg;

pub const LEADER_SHARD_ID: usize = 0;

#[derive(Clone)]
pub enum LeaseRequestType {
    RequestOrGetLease,
    TryEarlyRenewLease,
}

pub struct LeaseRequestMsg {
    pub request_id: u64,
    pub from_shard: usize,
    pub request_type: LeaseRequestType,
}

#[derive(Clone)]
pub struct LeaseResponseMsg {
    pub request_id: u64,
    pub result: Result<LeaseInfo, LeaseError>,
}

pub struct LeasingChannel {
    shard_id: Cell<usize>,
    sender: RefCell<Option<Rc<RefCell<Senders<Msg>>>>>,
    node_lease: RwLock<Option<Rc<NodeLease>>>,
    pending_requests: RwLock<HashMap<u64, LocalSender<LeaseResponseMsg>>>,
    next_request_id: Cell<u64>,
}


impl LeasingChannel {
    pub fn new() -> Self {
        Self {
            shard_id: Cell::new(0),
            sender: RefCell::new(None),
            node_lease: RwLock::new(None),
            pending_requests: RwLock::new(HashMap::new()),
            next_request_id: Cell::new(0),
        }
    }

    /// Initialize for the leader shard (shard 0)
    pub async fn initialize_leader(
        &self,
        shard_id: usize,
        sender: Rc<RefCell<Senders<Msg>>>,
        node_lease: Rc<NodeLease>,
    ) {
        debug!("Initializing LeasingChannel as leader on shard {}", shard_id);
        self.shard_id.set(shard_id);
        *self.sender.borrow_mut() = Some(sender);
        *self.node_lease.write().await.unwrap() = Some(node_lease);
    }

    /// Initialize for follower shards
    pub async fn initialize_follower(
        &self,
        shard_id: usize,
        sender: Rc<RefCell<Senders<Msg>>>,
    ) {
        debug!("Initializing LeasingChannel as follower on shard {}", shard_id);
        self.shard_id.set(shard_id);
        *self.sender.borrow_mut() = Some(sender);
    }

    /// Deliver a response from the mesh to the waiting request
    pub async fn deliver_response(&self, response: LeaseResponseMsg) {
        let mut pending = self.pending_requests.write().await.unwrap();
        if let Some(sender) = pending.remove(&response.request_id) {
            if let Err(e) = sender.try_send(response) {
                error!("Failed to deliver lease response: {:?}", e);
            }
        } else {
            debug!("Received lease response for unknown request_id: {}", response.request_id);
        }
    }

    /// Handle a lease request (only called on leader shard)
    pub async fn handle_lease_request(&self, request: LeaseRequestMsg) -> LeaseResponseMsg {
        let result = match request.request_type {
            LeaseRequestType::RequestOrGetLease => self.local_request_or_get_lease().await,
            LeaseRequestType::TryEarlyRenewLease => self.local_try_early_renew_lease().await,
        };
        LeaseResponseMsg {
            request_id: request.request_id,
            result,
        }
    }

    fn is_leader_shard(&self) -> bool {
        self.shard_id.get() == LEADER_SHARD_ID
    }

    fn next_request_id(&self) -> u64 {
        let id = self.next_request_id.get();
        self.next_request_id.set(id.wrapping_add(1));
        id
    }

    async fn local_request_or_get_lease(&self) -> Result<LeaseInfo, LeaseError> {
        // let node_lease = {
        //     let guard = self.node_lease.read().await.map_err(|_| LeaseError::ControlPlaneOffline)?;
        //     guard.as_ref().ok_or(LeaseError::ControlPlaneOffline)?.clone()
        // };
        // node_lease.must_be_leader_and_get_lease().await
        let r1 = self.node_lease.read().await.unwrap();
        let r2 = r1.as_ref();
        let r3 = r2.unwrap();
        r3.must_be_leader_and_get_lease().await
    }

    async fn local_try_early_renew_lease(&self) -> Result<LeaseInfo, LeaseError> {
        let node_lease = {
            let guard = self.node_lease.read().await.map_err(|_| LeaseError::ControlPlaneOffline)?;
            guard.as_ref().ok_or(LeaseError::ControlPlaneOffline)?.clone()
        };
        node_lease.try_early_renew().await
    }

    async fn send_request_to_leader(
        &self,
        request_type: LeaseRequestType,
    ) -> Result<LeaseInfo, LeaseError> {
        let request_id = self.next_request_id();
        let from_shard = self.shard_id.get();

        // Create a oneshot-style channel for the response
        let (tx, rx) = new_bounded::<LeaseResponseMsg>(1);

        // Register the pending request
        self.pending_requests.write().await
            .map_err(|_| LeaseError::ControlPlaneOffline)?
            .insert(request_id, tx);

        // Build and send the message
        let msg = Msg {
            fd: -1,
            value: None,
            message_version: 0,
            require_shutdown: false,
            lease_request: Some(LeaseRequestMsg {
                request_id,
                from_shard,
                request_type,
            }),
            lease_response: None,
        };

        // Try to send - all borrows contained in this block
        let send_result: Result<(), LeaseError> = {
            let sender_opt = self.sender.borrow();
            match sender_opt.as_ref() {
                Some(sender) => {
                    match sender.borrow().try_send_to(LEADER_SHARD_ID, msg) {
                        Ok(_) => Ok(()),
                        Err(_) => Err(LeaseError::ControlPlaneOffline),
                    }
                }
                None => Err(LeaseError::ControlPlaneOffline),
            }
        };

        // Handle send failure - cleanup happens after borrows released
        if let Err(e) = send_result {
            let _ = self.pending_requests.write().await
                .map(|mut p| p.remove(&request_id));
            return Err(e);
        }

        debug!(
            "Shard {} sent lease request {} to leader",
            from_shard, request_id
        );

        // Await the response
        match rx.recv().await {
            Some(response) => {
                debug!(
                    "Shard {} received lease response for request {}",
                    from_shard, request_id
                );
                response.result
            }
            None => {
                error!(
                    "Shard {} lease response channel closed for request {}",
                    from_shard, request_id
                );
                Err(LeaseError::ControlPlaneOffline)
            }
        }
    }

}

impl LeasingChannelTrait for LeasingChannel {
    async fn try_early_renew_lease(&self) -> Result<LeaseInfo, LeaseError> {
        if self.is_leader_shard() {
            self.local_try_early_renew_lease().await
        } else {
            self.send_request_to_leader(LeaseRequestType::TryEarlyRenewLease).await
        }
    }

    async fn request_or_get_lease(&self) -> Result<LeaseInfo, LeaseError> {
        if self.is_leader_shard() {
            self.local_request_or_get_lease().await
        } else {
            self.send_request_to_leader(LeaseRequestType::RequestOrGetLease).await
        }
    }
}