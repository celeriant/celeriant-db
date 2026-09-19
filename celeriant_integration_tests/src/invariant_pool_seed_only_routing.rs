//! Pool-routing invariant: a pool configured with seed addresses and no
//! primary routes leader operations to the first seed. Before the fix
//! (get-leader-connection-empty-address) `get_leader_connection` dialed the
//! empty primary address and failed, even though healthy seeds were listed.

use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;

use crate::TestServer;
use crate::common::{R, event, port_for, unique_key};

pub async fn seed_only_pool_routes_to_first_seed() -> R {
    let server = TestServer::start_with_port(port_for("invariant_pool_seed_only_routing")).await?;

    let options = PoolOptions::default()
        .with_seed_addresses(vec![server.address().to_string()]);
    let pool = CeleriantPool::new(options);

    // The direct observable: leader routing must yield a usable connection,
    // not a dial of the empty primary address.
    pool.get_leader_connection()
        .await
        .map_err(|e| format!("get_leader_connection with seeds configured must connect: {e:?}"))?;

    // And the end-to-end proof: a leader-routed write followed by a read.
    let key = unique_key("invariant_pool_seed_only_routing");
    pool.write_events_with(
        key.clone(),
        vec![event(1, 100, 1001, "{\"seed\":1}")],
        0,
        WriteEventsOptions { allow_create: true, ..Default::default() },
    )
    .await
    .map_err(|e| format!("leader-routed write through a seed-only pool failed: {e:?}"))?;

    let resp = pool
        .read(ReadRequest {
            correlation_id: None,
            aggregate_key: key,
            filters: ReadFilters::new(1),
        })
        .await
        .map_err(|e| format!("read through a seed-only pool failed: {e:?}"))?;
    let events: usize = resp.event_batches.iter().map(|b| b.events.len()).sum();
    if events != 1 {
        return Err(format!("expected the 1 written event back, got {events}").into());
    }
    Ok(())
}
