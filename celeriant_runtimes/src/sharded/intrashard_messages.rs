use std::rc::Rc;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::{process_client_requests::ClientRequest, process_cluster_requests::ClusterRequest, request::requests::RegisterSchemaRequest, response::responses::AccessLevel};
use celeriant_shard::{error::{s3_catchup_error::S3CatchupError, shard_schema_error::ShardSchemaError}, shard_wal::TailReconciliation, shard_wal_s3_catchup::S3CatchupResult};
use glommio::channels::channel_mesh::Senders;
use glommio::channels::local_channel::LocalReceiver;
use glommio::net::AcceptedTcpStream;

#[derive(Clone, Debug)]
pub enum IntrashardMessages {
    Shutdown,
    ClientConnectionRedirect {
        accepted_tcp_stream: AcceptedTcpStream,
        request: ClientRequest,
        message_version: u32,
        verified_client_id: Option<u128>,
        access_level: Option<AccessLevel>,
    },
    ClusterConnectionRedirect {
        accepted_tcp_stream: AcceptedTcpStream,
        request: ClusterRequest,
        message_version: u32,
    },
    ExtensionConnectionRedirect {
        accepted_tcp_stream: AcceptedTcpStream,
        payload: Vec<u8>,
    },
    CullSpeculativeTail { mode: TailReconciliation },
    RenewS3LeaseNow { requesting_shard: usize },
    EnterS3Catchup {
        role: celeriant_shard::shard_wal_s3_catchup::CatchupRole,
        /// Catchup generation. Cycles can overlap (a kicked node's previous
        /// per-shard catchup may still be running when the next cycle starts)
        /// and the completion channel persists across cycles, so an untagged
        /// completion from an old cycle would satisfy the new cycle's
        /// accounting instantly and wrongly (kick/catchup livelock).
        attempt: u64,
    },
    S3CatchupComplete {
        shard_id: usize,
        attempt: u64,
        result: Result<S3CatchupResult, S3CatchupError>
    },
    StatusUpdate {
        status: ValidatedNodeStatus,
        cas_confirmed_at_ms: Option<u64>,
        leader_changed_hands: bool,
    },
    UpdatePeerNodeId { peer_node_id: Option<u128> },
    UpdateFollower { replication_address: Option<String>, peer_node_id: Option<u128> },
    FollowerReachable { reachable: bool, was_reachable: bool },
    PeriodicProbe,
    HeartbeatInFlightStarted { unix_ms: u64 },
    HeartbeatInFlightCleared,
    UpdateLeaderClientAddress { client_address: Option<String> },
    SchemaRegistration {
        request: RegisterSchemaRequest,
        request_id: u64,
    },
    SchemaRegistrationComplete {
        request_id: u64,
        result: Result<(), ShardSchemaError>,
    },
}

pub struct RedirectedConnection {
    pub accepted_tcp_stream: AcceptedTcpStream,
    pub payload: Vec<u8>,
}

pub struct ExtensionMesh {
    shard_id: usize,
    num_shards: usize,
    sender: Rc<Senders<IntrashardMessages>>,
    inbound: LocalReceiver<RedirectedConnection>,
}

impl ExtensionMesh {
    pub(crate) fn new(
        shard_id: usize,
        num_shards: usize,
        sender: Rc<Senders<IntrashardMessages>>,
        inbound: LocalReceiver<RedirectedConnection>,
    ) -> Self {
        Self { shard_id, num_shards, sender, inbound }
    }

    pub fn shard_id(&self) -> usize { self.shard_id }
    pub fn num_shards(&self) -> usize { self.num_shards }

    pub fn try_redirect(
        &self,
        target_shard: usize,
        stream: AcceptedTcpStream,
        payload: Vec<u8>,
    ) -> Result<(), Option<AcceptedTcpStream>> {
        if target_shard == self.shard_id {
            return Err(Some(stream));
        }
        let msg = IntrashardMessages::ExtensionConnectionRedirect {
            accepted_tcp_stream: stream,
            payload,
        };
        match self.sender.try_send_to(target_shard, msg) {
            Ok(()) => {
                metrics::counter!("celeriant_extension_redirects_total").increment(1);
                Ok(())
            }
            Err(e) => match e.into_inner() {
                Some(IntrashardMessages::ExtensionConnectionRedirect { accepted_tcp_stream, .. }) => {
                    Err(Some(accepted_tcp_stream))
                }
                _ => Err(None),
            },
        }
    }

    pub async fn recv(&self) -> Option<RedirectedConnection> {
        self.inbound.recv().await
    }
}
