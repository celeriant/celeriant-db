use std::time::Duration;

use celeriant_client_tokio::server_error::{ReadError, ServerError};
use celeriant_client_tokio::{CeleriantPool, ClientError};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;

use crate::constants::*;
use crate::types::AccountError;

pub const MAX_RETRIES: usize = 3;

/// How long a request's `event_id` stays resolvable to its original response.
/// Past this window a retried request writes a fresh event; the window is a
/// stated property of the API, not a safety boundary (the server's client_seq
/// check is what prevents double-writes).
pub const DEDUP_WINDOW_SECS: u64 = 90;
pub const DEDUP_WINDOW: Duration = Duration::from_secs(DEDUP_WINDOW_SECS);

pub enum SeqOwnership {
    /// The contested seq carries our event_id: the prior attempt landed.
    Ours,
    /// A sibling request consumed the seq; our event never landed.
    Sibling,
    /// No event holds this seq for our client_id. After a single-aggregate
    /// 2002 this is inconsistent; in a transfer it means the violation was
    /// on the other leg.
    Unwritten,
}

/// Who owns a contested client_seq? Answered from the stream itself, so it
/// works on any instance with no shared state. The seq filters match on batch
/// metadata, so every batch except the one holding the seq is skipped without
/// reading its events. This is the error path: a 2002 already told us the seq
/// is consumed and durable, we only need to know by whom.
pub async fn who_owns_seq(
    pool: &CeleriantPool,
    account_id: u128,
    client_seq: u64,
    our_event_id: u128,
) -> Result<SeqOwnership, AccountError> {
    let response = match pool.read(ReadRequest {
        correlation_id: None,
        aggregate_key: account_key(account_id),
        filters: ReadFilters::new(1)
            .client_seq_range(client_seq, client_seq)
            .include_client_id(*SERVICE_CLIENT_ID),
    }).await {
        Ok(r) => r,
        // Only reachable when a lagging read replica hides the aggregate;
        // lag can only hide events, never misattribute them, so the safe
        // verdict is "not visible yet" and the caller surfaces a retryable
        // error rather than guessing.
        Err(ClientError::Server(ServerError::Read {
            kind: ReadError::AggregateNotExists, ..
        })) => return Ok(SeqOwnership::Unwritten),
        Err(e) => return Err(e.into()),
    };

    let owner = response.event_batches.iter()
        .flat_map(|b| b.events.iter())
        .find(|e| e.client_seq == client_seq)
        .map(|e| e.event_id);

    Ok(match owner {
        Some(Some(eid)) if eid == our_event_id => SeqOwnership::Ours,
        Some(_) => SeqOwnership::Sibling,
        None => SeqOwnership::Unwritten,
    })
}

pub async fn backoff(attempt: usize) {
    let delay_ms = (100 * (1 << (attempt - 1))) + rand::random::<u64>() % 50;
    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
}
