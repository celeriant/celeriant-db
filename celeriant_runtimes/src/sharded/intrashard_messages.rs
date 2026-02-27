use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::{process_requests::Request, response::responses::AccessLevel};
use celeriant_shard::{error::s3_catchup_error::S3CatchupError, shard_wal_s3_catchup::S3CatchupResult};
use glommio::net::AcceptedTcpStream;

use super::connection_handler::PortType;

#[derive(Clone, Debug)]
pub enum IntrashardMessages {
    Shutdown,
    ConnectionRedirect {
        accepted_tcp_stream: AcceptedTcpStream,
        request: Request,
        message_version: u32,
        port_type: PortType,
        verified_client_id: Option<u128>,
        access_level: Option<AccessLevel>,
    },
    EnterS3Catchup,
    S3CatchupComplete {
        shard_id: usize,
        result: Result<S3CatchupResult, S3CatchupError>
    },
    StatusUpdate { status: ValidatedNodeStatus },
    UpdateFollower { replication_address: Option<String> },
    UpdateLeaderClientAddress { client_address: Option<String> },
}