//! List invariant: every list iterator terminates and returns the complete set
//! when shard discovery runs open-ended (`max_shard_hint: None`), including
//! from the top of the shard range (`start_shard: u64::MAX`), where the shard
//! cursor used to overflow. Also pins the server-side contract discovery
//! depends on: an out-of-range shard errors instead of returning an empty page.
//! Findings: list-start-shard-overflow, pool-list-start-shard-overflow.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::list_operations::{
    ListAggregateTypesIterator, ListAggregatesIterator, ListOptions, ListOrgsIterator,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::requests::ListAggregatesRequest;
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;
use tokio::time::timeout;

use crate::common::{R, port_for};
use crate::{ServerConfig, TestServer, write_event};

const ORG: u128 = 1;
/// One aggregate type per shard so both shards hold data (AggregateTypeId routing).
const TYPES: [u128; 2] = [1, 2];
const AGGS_PER_TYPE: u64 = 3;

/// Generous bound: a non-terminating discovery loop hangs forever without it.
const DEADLINE: Duration = Duration::from_secs(30);

pub async fn terminate_at_shard_boundary() -> R {
    let config = ServerConfig {
        num_shards: Some(2),
        log_level: "warn".to_string(),
        standalone: true,
        routing_rule: RoutingRule::AggregateTypeId,
        ..Default::default()
    };
    let server =
        TestServer::start_with_config(port_for("invariant_list_termination"), config).await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    for t in TYPES {
        for a in 0..AGGS_PER_TYPE {
            write_event(&mut client, &AggregateKey::new(ORG, t, a as u128), 1, true).await?;
        }
    }

    // Server contract discovery relies on: an out-of-range shard errors, never
    // an empty OK page (an empty page would read as a real, empty shard and
    // discovery would walk forever).
    let out_of_range = client
        .send_request(&ClientRequest::ListAggregates(ListAggregatesRequest {
            correlation_id: Some(1),
            shard_id: 9999,
            org_id: None,
            aggregate_type_id: None,
            cursor: None,
        }))
        .await;
    match out_of_range {
        Err(ClientError::Server(
            celeriant_client_tokio::server_error::ServerError::ShardRouting { .. },
        )) => {}
        other => {
            return Err(format!(
                "out-of-range shard must be a ShardRouting error, got {other:?}"
            )
            .into());
        }
    }

    // Open-ended discovery from shard 0: complete sets, bounded time.
    let discover = ListOptions::default();
    let orgs = timeout(DEADLINE, ListOrgsIterator::new(&mut client, discover.clone()).collect())
        .await
        .map_err(|_| "list_orgs discovery did not terminate")??;
    if orgs.len() != 1 || orgs[0].org_id != ORG {
        return Err(format!("expected exactly org {ORG}, got {orgs:?}").into());
    }

    let types = timeout(
        DEADLINE,
        ListAggregateTypesIterator::new(&mut client, Some(ORG), discover.clone()).collect(),
    )
    .await
    .map_err(|_| "list_aggregate_types discovery did not terminate")??;
    let mut type_ids: Vec<u128> = types.iter().map(|t| t.aggregate_type_id).collect();
    type_ids.sort_unstable();
    if type_ids != TYPES {
        return Err(format!("expected types {TYPES:?} across both shards, got {type_ids:?}").into());
    }

    let aggs = timeout(
        DEADLINE,
        ListAggregatesIterator::new(&mut client, Some(ORG), None, discover).collect(),
    )
    .await
    .map_err(|_| "list_aggregates discovery did not terminate")??;
    if aggs.len() != (TYPES.len() as u64 * AGGS_PER_TYPE) as usize {
        return Err(format!(
            "expected {} aggregates across both shards, got {}",
            TYPES.len() as u64 * AGGS_PER_TYPE,
            aggs.len()
        )
        .into());
    }

    // From the top of the range the cursor must saturate: terminate with an
    // empty result, not wrap around to shard 0 or spin.
    let top = ListOptions { start_shard: u64::MAX, ..Default::default() };
    let orgs = timeout(DEADLINE, ListOrgsIterator::new(&mut client, top.clone()).collect())
        .await
        .map_err(|_| "list_orgs from u64::MAX did not terminate")??;
    let types = timeout(
        DEADLINE,
        ListAggregateTypesIterator::new(&mut client, Some(ORG), top.clone()).collect(),
    )
    .await
    .map_err(|_| "list_aggregate_types from u64::MAX did not terminate")??;
    let aggs = timeout(
        DEADLINE,
        ListAggregatesIterator::new(&mut client, Some(ORG), None, top).collect(),
    )
    .await
    .map_err(|_| "list_aggregates from u64::MAX did not terminate")??;
    if !orgs.is_empty() || !types.is_empty() || !aggs.is_empty() {
        return Err(format!(
            "listing from past the shard range must be empty, got {} orgs / {} types / {} aggs",
            orgs.len(),
            types.len(),
            aggs.len()
        )
        .into());
    }

    println!("PASS: list iterators terminate and return complete sets at the shard boundary");
    Ok(())
}
