use std::time::Instant;

use celeriant_distributed::node_status::NodeStatus;
use celeriant_msg::process_requests::Request;
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
    },
    EnterS3Catchup,
    S3CatchupComplete { 
        shard_id: usize,
        result: Result<S3CatchupResult, S3CatchupError> 
    },
    StatusUpdate { status: NodeStatus, valid_until: Instant },
}