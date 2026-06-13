use std::cell::Cell;
use std::rc::Rc;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::requests::ReplicationBatchItem;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wire::codec::compression::DictCodec;

use crate::error::fetch_catchup_entries_error::FetchCatchupEntriesError;
use crate::error::replication_error::ReplicationError;
use crate::error::replication_to_follower_error::ReplicateToFollowerError;
use crate::fetch_catchup_entries::fetch_catchup_entries;
use crate::replication_client::ReplicationClient;
use crate::shard_wal_replicate::current_leader_confirmed_wal_seq;

pub(crate) enum CatchupOutcome {
    /// Follower is now caught up to (but not including) `leader_wal_seq`.
    /// The caller can retry the original send.
    Caught,
    /// Catchup itself failed in a way that means TCP can't deliver. The caller
    /// should fall back to S3.
    FallbackToS3,
}

/// Bring a behind follower back up to the leader tip
pub(crate) async fn replicate_follower_catchup<R: ReplicationClient + 'static>(
    replication_client: &Rc<R>,
    log_segments_cache: &Rc<LogSegmentsCache>,
    node_status: &Rc<Cell<ValidatedNodeStatus>>,
    follower_wal_seq: u64,
    leader_wal_seq: u64,
    max_catchup_gap_bytes: Option<u64>,
    max_request_size: u64,
    read_max_chunk_size: u64,
    shard_id: u32,
    dict_codec: &DictCodec,
) -> Result<CatchupOutcome, ReplicationError> {
    let entries = match fetch_catchup_entries(
        log_segments_cache, follower_wal_seq, leader_wal_seq,
        max_catchup_gap_bytes, read_max_chunk_size, dict_codec,
    ).await {
        Ok(entries) => entries,
        Err(FetchCatchupEntriesError::FollowerTooFarBehind) => {
            tracing::debug!(shard_id, follower_wal_seq, leader_wal_seq, "Catchup gap exceeds byte budget; falling back to S3");
            metrics::counter!("celeriant_catchup_fallback_total", &[("reason", "too_far_behind".to_string())]).increment(1);
            return Ok(CatchupOutcome::FallbackToS3);
        }
        Err(e) => {
            tracing::warn!(shard_id, follower_wal_seq, leader_wal_seq, error = ?e, "Catchup fetch failed");
            metrics::counter!("celeriant_catchup_fetch_error_total").increment(1);
            return Err(ReplicationError::ExtendedCatchupFailure(e));
        }
    };

    if entries.is_empty() {
        // Leader no longer holds the gap range (compacted/trimmed). The follower's
        // position is implicitly current; let the caller retry the original send.
        // Loud on purpose: a nonzero gap resolving to nothing stalls convergence
        // silently if it's wrong.
        tracing::warn!(
            shard_id, follower_wal_seq, leader_wal_seq,
            gap = leader_wal_seq.saturating_sub(follower_wal_seq),
            "Catchup fetch returned empty for a nonzero gap; treating follower as caught up"
        );
        metrics::counter!("celeriant_catchup_empty_fetch_total").increment(1);
        return Ok(CatchupOutcome::Caught);
    }

    let items: Vec<ReplicationBatchItem> = entries
        .into_iter()
        .map(|e| ReplicationBatchItem { metablock: e.metablock, datablock: e.datablock })
        .collect();

    tracing::debug!(shard_id, follower_wal_seq, leader_wal_seq, count = items.len(), "Catchup entries fetched");

    let mut sent: usize = 0;
    while sent < items.len() {
        // The request carries our current epoch; the items' own authorship epochs
        // may be older (historical replay) and must not be used for fencing.
        let sender_lease_epoch = match node_status.get().effective_node_status().lease_epoch() {
            Some(e) => e,
            None => return Err(ReplicationError::LeaderFenced),
        };

        let end_idx = batch_end_index(&items[sent..], max_request_size);
        // Pre-flight lease check; bail if fenced or zero remaining before starting.
        match node_status.get().current_budget() {
            None => return Err(ReplicationError::LeaderFenced),
            Some(b) if b.is_zero() => {
                metrics::counter!("celeriant_lease_budget_exhausted_total", &[("op", "catchup".to_string()), ("shard_id", shard_id.to_string())]).increment(1);
                return Err(ReplicationError::BudgetExhausted);
            }
            Some(_) => {}
        }

        let chunk = items[sent..sent + end_idx].to_vec();
        let leader_confirmed_wal_seq = current_leader_confirmed_wal_seq(log_segments_cache);
        let send_result = replication_client.replicate_to_follower(chunk, leader_confirmed_wal_seq, sender_lease_epoch).await;

        match send_result {
            Ok(()) => {
                sent += end_idx;
            }
            Err(e @ (ReplicateToFollowerError::FollowerNetworkError(_) | ReplicateToFollowerError::LockTimeout)) => {
                replication_client.set_follower_reachable(false);
                tracing::debug!(shard_id, sent, chunk_len = end_idx, error = ?e, "Catchup TCP failed; falling back to S3");
                metrics::counter!("celeriant_catchup_fallback_total", &[("reason", "network".to_string())]).increment(1);
                return Ok(CatchupOutcome::FallbackToS3);
            }
            Err(e) => {
                tracing::debug!(shard_id, sent, chunk_len = end_idx, error = ?e, "Catchup TCP rejected; falling back to S3");
                metrics::counter!("celeriant_catchup_fallback_total", &[("reason", "rejected".to_string())]).increment(1);
                return Ok(CatchupOutcome::FallbackToS3);
            }
        }
    }

    Ok(CatchupOutcome::Caught)
}

/// Returns the number of items from the start that fit within `max_size_bytes`.
/// Always returns at least 1 to guarantee progress on oversize first items.
fn batch_end_index(items: &[ReplicationBatchItem], max_size_bytes: u64) -> usize {
    let mut cumulative = 0u64;
    for (i, item) in items.iter().enumerate() {
        let size = item.size_bytes();
        if cumulative + size > max_size_bytes && i > 0 {
            return i;
        }
        cumulative += size;
    }
    items.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::metablocks::metablock::Metablock;

    fn item() -> ReplicationBatchItem {
        ReplicationBatchItem {
            metablock: Metablock::default_inline_event_batch_metadata(AggregateKey::default()),
            datablock: None,
        }
    }

    #[test]
    fn batch_end_index_scenarios() {
        let sz = item().size_bytes();
        let items: Vec<_> = (0..5).map(|_| item()).collect();
        let cases: &[(&[ReplicationBatchItem], u64, usize)] = &[
            (&items[..1], sz * 2, 1),
            (&items[..5], sz * 10, 5),
            (&items[..3], 1, 1),               // oversized first item, progress guarantee
            (&items[..4], sz * 2, 2),
            (&items[..5], sz * 3, 3),
            (&items[..1], sz, 1),
            (&items[..5], sz * 5, 5),
        ];
        for (i, (slice, max_bytes, expected)) in cases.iter().enumerate() {
            assert_eq!(
                batch_end_index(slice, *max_bytes), *expected,
                "case {i}: items={}, max_bytes={max_bytes}", slice.len()
            );
        }
    }
}
