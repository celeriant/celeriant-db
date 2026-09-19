//! Wire-format invariant: the protocol version is pinned by a connection's first
//! message. docs/invariants.md — "Protocol version is set on the first message
//! (Identify or first ClientRequest). All subsequent messages use that version.
//! No renegotiation."

use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

use crate::TestServer;
use crate::common::{R, port_for};

const MAX_REQUEST_SIZE: u64 = 16 * 1024 * 1024;
const WIRE_HEADER_SIZE: usize = 17;
const READ_DEADLINE: Duration = Duration::from_secs(10);

async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<u32>> {
    let mut header = [0u8; WIRE_HEADER_SIZE];
    match timeout(READ_DEADLINE, stream.read_exact(&mut header)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e))
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            return Ok(None);
        }
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(std::io::Error::other("no response and no close within deadline")),
    }
    let version = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let compressed_length =
        u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
    let mut body = vec![0u8; compressed_length];
    stream.read_exact(&mut body).await?;
    Ok(Some(version))
}

async fn serialize_read(version: u32, correlation_id: u128) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let req = ClientRequest::Read(ReadRequest {
        correlation_id: Some(correlation_id),
        aggregate_key: AggregateKey::new(1, 1, 1),
        filters: ReadFilters::new(1),
    });
    let mut sink = Vec::new();
    ClientRequest::write_request(&mut sink, &req, MAX_REQUEST_SIZE, version)
        .await
        .map_err(|e| format!("serialise v{version} request: {e:?}"))?;
    Ok(sink)
}

pub async fn pinned_per_connection() -> R {
    let config = crate::ServerConfig {
        num_shards: Some(1),
        standalone: true,
        ..Default::default()
    };
    let server =
        TestServer::start_with_config(port_for("invariant_protocol_version"), config).await?;

    // Control: V3 as a connection's FIRST message is served. Proves a later
    // rejection is about the mid-connection switch, not about V3 itself.
    let mut control = TcpStream::connect(server.address()).await?;
    control.write_all(&serialize_read(PROTOCOL_VERSION_V3, 1).await?).await?;
    match read_frame(&mut control).await? {
        Some(v) if v == PROTOCOL_VERSION_V3 => {}
        Some(v) => return Err(format!("control: V3 first frame answered at version {v}").into()),
        None => return Err("control: connection closed on a V3 first frame".into()),
    }

    // The invariant: a connection speaking V2 must not be allowed to switch to V3.
    let mut stream = TcpStream::connect(server.address()).await?;
    stream.write_all(&serialize_read(PROTOCOL_VERSION_V2, 2).await?).await?;
    match read_frame(&mut stream).await? {
        Some(v) if v == PROTOCOL_VERSION_V2 => {}
        Some(v) => return Err(format!("first V2 frame answered at version {v}").into()),
        None => return Err("connection closed on a valid V2 first frame".into()),
    }

    stream.write_all(&serialize_read(PROTOCOL_VERSION_V3, 3).await?).await?;
    match read_frame(&mut stream).await? {
        None => Ok(()),
        Some(v) => Err(format!(
            "server accepted a mid-connection V2 -> V3 version switch (answered at version {v}); \
             invariants.md requires the first message's version to be pinned"
        )
        .into()),
    }
}
