use celeriant_msg::process_requests::Request;
use glommio::net::AcceptedTcpStream;

#[derive(Clone, Debug)]
pub enum IntrashardMessages {
    Shutdown,
    ConnectionRedirect {
        accepted_tcp_stream: AcceptedTcpStream,
        request: Request,
        message_version: u32,
    }
}