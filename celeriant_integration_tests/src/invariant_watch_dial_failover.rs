//! Watch-establishment invariant: one bad candidate node cannot wedge or
//! abort a watch when a healthy seed is configured.
//!
//! Two bad-candidate shapes, each a distinct historical failure:
//!   - a peer that speaks garbage: the handshake dies with a wire/read error,
//!     which must fail over to the next candidate like read_route does
//!     (watch-no-retry-on-wire-error);
//!   - a peer that accepts and never speaks: the handshake phases after the
//!     dial must be bounded by the watch timeout so the dial surfaces
//!     `ConnectionTimeout` and fails over, instead of hanging forever
//!     (watch-connect-no-handshake-timeout).
//!
//! Both scenarios end with the watch landed on the real server and a real
//! event delivered through it, so the assertion is system behaviour, not
//! which error variant a mock produced.

use std::collections::HashSet;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_client_tokio::watch_connection::WatchOptions;
use celeriant_msg::request::requests::WatchRequest;
use tokio::io::AsyncWriteExt;

use crate::TestServer;
use crate::common::{R, event, port_for};

/// The whole pool.watch call must resolve well inside this bound; pre-fix the
/// silent-peer scenario never resolves at all.
const ESTABLISH_DEADLINE: Duration = Duration::from_secs(20);

/// Accepts connections and writes garbage, so the client's handshake read
/// fails with a wire error.
async fn garbage_speaking_listener() -> Result<std::net::SocketAddr, std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _ = socket.write_all(&[0xFFu8; 64]).await;
                // Hold the socket open; the client has its garbage already.
                tokio::time::sleep(Duration::from_secs(60)).await;
            });
        }
    });
    Ok(addr)
}

/// Accepts connections and never writes a byte.
async fn silent_listener() -> Result<std::net::SocketAddr, std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    Ok(addr)
}

async fn watch_lands_despite_bad_primary(
    scenario: &str,
    bad_addr: std::net::SocketAddr,
    server: &TestServer,
    agg_id: u128,
) -> R {
    let pool = CeleriantPool::new(
        PoolOptions::new(bad_addr.to_string())
            .with_seed_addresses(vec![server.address().to_string()]),
    );

    let request = WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([agg_id])),
        operation_types: None,
    };
    let options = WatchOptions { timeout: Some(Duration::from_secs(2)), ..Default::default() };

    let mut watch = tokio::time::timeout(ESTABLISH_DEADLINE, pool.watch(request, options))
        .await
        .map_err(|_| {
            format!(
                "{scenario}: pool.watch did not resolve within {ESTABLISH_DEADLINE:?} — \
                 the handshake against the bad candidate is unbounded"
            )
        })?
        .map_err(|e| {
            format!("{scenario}: pool.watch failed instead of failing over to the healthy seed: {e:?}")
        })?;

    // Prove the watch is live on the real server: write and receive.
    let key = celeriant_wal::aggregate_key::AggregateKey::new(1, 1, agg_id);
    let mut writer = CeleriantClient::connect(server.address()).await?;
    writer
        .write_events_with(
            key,
            vec![event(1, 100, 1001, "{\"w\":1}")],
            0,
            WriteEventsOptions { allow_create: true, ..Default::default() },
        )
        .await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if let Some(resp) = watch.next_timeout(Duration::from_secs(1)).await? {
            if resp.events.iter().any(|e| e.aggregate_id == agg_id) {
                return Ok(());
            }
        }
    }
    Err(format!("{scenario}: watch established but never delivered the written event").into())
}

pub async fn survives_bad_candidates() -> R {
    let server =
        TestServer::start_with_port(port_for("invariant_watch_dial_failover")).await?;

    let garbage = garbage_speaking_listener().await?;
    watch_lands_despite_bad_primary("garbage-speaking primary", garbage, &server, 501_001).await?;

    let silent = silent_listener().await?;
    watch_lands_despite_bad_primary("silent primary", silent, &server, 501_002).await?;

    Ok(())
}
