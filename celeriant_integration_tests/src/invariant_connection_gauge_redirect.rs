//! Metrics invariant: `celeriant_client_connections_active` follows a stream
//! across a cross-shard redirect.
//!
//! A client socket lands on whichever shard SO_REUSEPORT picked. The first
//! request for an aggregate owned by another shard hands the whole stream over
//! via the intrashard mesh. The sending shard's connection guard drops on that
//! handover, so unless the receiving shard takes a guard of its own the gauge
//! decays towards zero while the sockets are still open. It read 7-28 on a
//! node holding 6000+ connections.
//!
//! The test opens N connections, forces redirects by writing to keys spread
//! over 4 shards, and pins the gauge at N while they are open and 0 after they
//! close.

use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;

use crate::common::{R, port_for, unique_key};
use crate::{ServerConfig, TestServer, scrape_counter, write_event};

const NUM_SHARDS: usize = 4;
const CONNECTIONS: u64 = 8;
/// Distinct keys per connection. With 4 shards each write has a 3-in-4 chance
/// of routing off the shard the socket landed on, so 4 keys makes a connection
/// that never redirects a 1-in-256 event, and all 8 missing impossible.
const KEYS_PER_CONNECTION: u64 = 4;

const GAUGE: &str = "celeriant_client_connections_active";
const REDIRECTS: &str = "celeriant_connection_redirects_total";

pub async fn client_connection_gauge_follows_cross_shard_redirects() -> R {
    let config = ServerConfig {
        num_shards: Some(NUM_SHARDS),
        standalone: true,
        log_level: "warn".to_string(),
        ..Default::default()
    };
    let server =
        TestServer::start_with_config(port_for("invariant_connection_gauge_redirect"), config)
            .await?;
    let metrics_port = server.config().metrics_port;

    // The readiness probe connects and disconnects; settle before measuring.
    let baseline = settle_to(metrics_port, 0, Duration::from_secs(10)).await?;
    if baseline != 0 {
        return Err(format!("{GAUGE}{{port_type=\"client\"}} = {baseline} before any client connected").into());
    }

    let mut clients = Vec::with_capacity(CONNECTIONS as usize);
    for connection in 0..CONNECTIONS {
        let mut client = CeleriantClient::connect(server.address()).await?;
        for key_num in 0..KEYS_PER_CONNECTION {
            let key = unique_key(&format!("conn_gauge_{connection}_{key_num}"));
            write_event(&mut client, &key, 1, true).await?;
        }
        clients.push(client);
    }

    let redirects = scrape_counter("127.0.0.1", metrics_port, REDIRECTS).await?;
    if redirects == 0 {
        return Err(format!(
            "premise unmet: {CONNECTIONS} connections x {KEYS_PER_CONNECTION} keys across \
             {NUM_SHARDS} shards produced no cross-shard redirect, so the handover this \
             test measures never happened"
        )
        .into());
    }

    let active = settle_to(metrics_port, CONNECTIONS, Duration::from_secs(10)).await?;
    if active != CONNECTIONS {
        return Err(format!(
            "{GAUGE}{{port_type=\"client\"}} = {active} with {CONNECTIONS} sockets open after \
             {redirects} redirects; the shard receiving a redirected stream is not taking a \
             connection guard"
        )
        .into());
    }

    drop(clients);

    let closed = settle_to(metrics_port, 0, Duration::from_secs(10)).await?;
    if closed != 0 {
        return Err(format!(
            "{GAUGE}{{port_type=\"client\"}} = {closed} after every client closed; a guard \
             is outliving its stream"
        )
        .into());
    }
    Ok(())
}

/// Poll the gauge until it reads `want` or the deadline passes, returning the
/// last value seen. Connection teardown is asynchronous on both sides, so an
/// instant assertion would be a race.
async fn settle_to(metrics_port: u16, want: u64, budget: Duration) -> Result<u64, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        let value = client_connections_active(metrics_port).await?;
        if value == want || std::time::Instant::now() >= deadline {
            return Ok(value);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// `celeriant_client_connections_active` summed over shards for the client
/// port only. `scrape_counter` cannot be reused: it sums every series of a
/// name, which would fold the replication port back in.
async fn client_connections_active(metrics_port: u16) -> Result<u64, Box<dyn std::error::Error>> {
    let url = format!("http://127.0.0.1:{metrics_port}/metrics");
    let body = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let mut total: u64 = 0;
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name_part, value_str)) = line.split_once(' ') else { continue };
        let name = name_part.split_once('{').map(|(n, _)| n).unwrap_or(name_part);
        if name == GAUGE
            && name_part.contains("port_type=\"client\"")
            && let Ok(value) = value_str.trim().parse::<f64>()
        {
            total = total.saturating_add(value.max(0.0) as u64);
        }
    }
    Ok(total)
}
