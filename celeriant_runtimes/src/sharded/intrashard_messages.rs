use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::{process_client_requests::ClientRequest, process_cluster_requests::ClusterRequest, request::requests::RegisterSchemaRequest, response::responses::AccessLevel};
use celeriant_shard::{error::{s3_catchup_error::S3CatchupError, shard_schema_error::ShardSchemaError}, shard_wal_s3_catchup::S3CatchupResult};
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
    EnterS3Catchup,
    S3CatchupComplete {
        shard_id: usize,
        result: Result<S3CatchupResult, S3CatchupError>
    },
    StatusUpdate { status: ValidatedNodeStatus },
    UpdatePeerNodeId { peer_node_id: Option<u128> },
    UpdateFollower { replication_address: Option<String>, peer_node_id: Option<u128> },
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
