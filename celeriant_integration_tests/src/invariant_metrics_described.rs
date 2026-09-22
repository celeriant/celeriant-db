//! Metrics invariant: every metric family the live server exports carries a
//! `# HELP` description (metrics-undescribed-recorded). The prometheus
//! exporter emits HELP only for described metrics, so an undescribed-but-
//! recorded metric ships to operators as a bare series with no explanation.
//!
//! The premise assertions guard against this test going green vacuously: the
//! scrape must actually contain families from the once-undescribed set before
//! the HELP check means anything.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;

use crate::TestServer;
use crate::common::{R, event, port_for, read_all, unique_key};

/// Families from the once-undescribed set that a standalone server exports
/// after a write and a read. If none of these render, the premise is unmet and
/// the test fails rather than passing vacuously.
const PREMISE_FAMILIES: &[&str] = &[
    "celeriant_read_wal_seq",
    // Accept path: every connected client walks it, so these render on any
    // server that answered a request.
    "celeriant_client_accepts_total",
    "celeriant_tls_handshake_seconds",
    "celeriant_tls_handshakes_in_flight",
];

pub async fn exported_families_are_described() -> R {
    let server = TestServer::start_with_port(port_for("invariant_metrics_described")).await?;

    // Drive a write and a read so gauge/counter families actually register.
    let key = unique_key("invariant_metrics_described");
    let mut client = CeleriantClient::connect(server.address()).await?;
    client
        .write_events_with(
            key.clone(),
            vec![event(1, 100, 1001, "{\"m\":1}")],
            0,
            WriteEventsOptions { allow_create: true, ..Default::default() },
        )
        .await?;
    let _ = read_all(&mut client, &key).await?;
    // The metrics upkeep loop publishes on an interval; give it a beat.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let url = format!("http://127.0.0.1:{}/metrics", server.config().metrics_port);
    let body = reqwest::get(&url).await?.text().await?;

    let mut families: Vec<&str> = Vec::new();
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            if let Some(name) = rest.split_whitespace().next() {
                if name.starts_with("celeriant_") {
                    families.push(name);
                }
            }
        }
    }

    for premise in PREMISE_FAMILIES {
        if !families.contains(premise) {
            return Err(format!(
                "premise unmet: {premise} did not render after a write and a read, so the \
                 HELP check below would be vacuous; exported families: {families:?}"
            )
            .into());
        }
    }

    let undescribed: Vec<&str> = families
        .iter()
        .filter(|name| {
            let help = format!("# HELP {name} ");
            !body.lines().any(|l| {
                l.starts_with(&help) && l.len() > help.len()
            })
        })
        .copied()
        .collect();

    if undescribed.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} exported metric families have no # HELP description: {undescribed:?}",
            undescribed.len()
        )
        .into())
    }
}
