//! Account service with an in-memory projection, safe to run as many replicas
//! (e.g. k8s HPA) sharing one SERVICE_CLIENT_ID.
//!
//! Each replica folds the stream itself, so each replica's fold rebuilds its
//! own request-dedup index as a side effect of replay: the cursor and the
//! index live together in pod memory and move together under one lock. A
//! retry landing on any replica is caught because that replica either already
//! folded the original event (index hit) or folds it during this request's
//! own catch-up. No Postgres, no shared cache, no extra network hops: the
//! happy path is catch-up read, fold, validate, write.
//!
//! Bounds: dedup-index entries carry an expiry deadline and are evicted during
//! the fold. The account map is bounded by the demo's fixed account set; a
//! production service would cap it (LRU) and rebuild evicted accounts by
//! re-folding the stream, which is the same code path as a cold start.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use celeriant_client_tokio::server_error::{ReadError, ServerError, WriteError};
use celeriant_client_tokio::{CeleriantPool, ClientError, WriteEventsOptions, json_event};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use serde_json::Value;

use crate::constants::*;
use crate::events::*;
use crate::types::{AccountError, AccountProjection, TransferResult, WriteResult};
use crate::verify::{self, DEDUP_WINDOW, DEDUP_WINDOW_SECS, MAX_RETRIES, SeqOwnership, backoff};

struct RecentWrite {
    balance_cents: i64,
    aggregate_version: u64,
    /// When this entry stops being trusted. An entry created by our own write
    /// gets the full window from now; an entry created by replaying a batch
    /// gets the window minus the batch's age, where age is measured in SERVER
    /// time (batch vs tip-of-read) so clock skew cannot misjudge it. Only the
    /// remaining lifetime runs on the local monotonic clock.
    expires_at: Instant,
}

#[derive(Default)]
struct MemAccount {
    account_name: String,
    balance_cents: i64,
    last_version: u64,
    max_client_seq: u64,
    /// The request-dedup index: event_id -> the response that write produced.
    /// Maintained by the fold, evicted by expiry.
    recent: HashMap<u128, RecentWrite>,
}

pub struct MemAccountService {
    pool: Arc<CeleriantPool>,
    // Never held across an await; lock, mutate, unlock.
    accounts: Mutex<HashMap<u128, MemAccount>>,
}

impl MemAccountService {
    pub fn new(pool: Arc<CeleriantPool>) -> Self {
        // Pre-seed names; everything else is folded from the stream.
        let accounts = ACCOUNTS.iter().map(|a| {
            (a.id, MemAccount { account_name: a.name.to_string(), ..Default::default() })
        }).collect();
        Self { pool, accounts: Mutex::new(accounts) }
    }

    fn projection_of(account_id: u128, acc: &MemAccount) -> AccountProjection {
        AccountProjection {
            account_id,
            account_name: acc.account_name.clone(),
            balance_cents: acc.balance_cents,
            last_version: acc.last_version,
            max_client_seq: acc.max_client_seq,
        }
    }

    fn hit_of(acc: &MemAccount, event_id: Option<u128>) -> Option<WriteResult> {
        let r = acc.recent.get(&event_id?)?;
        if r.expires_at <= Instant::now() {
            return None;
        }
        Some(WriteResult { balance_cents: r.balance_cents, aggregate_version: r.aggregate_version })
    }

    // --- Catch-Up ---

    /// Lazy catch-up: read new events from Celeriant from this replica's own
    /// cursor, fold them into the projection and the dedup index under one
    /// lock. Returns fresh projection state, plus the original response if
    /// `event_id` already landed.
    pub async fn catch_up(
        &self,
        account_id: u128,
        min_version: Option<u64>,
        event_id: Option<u128>,
    ) -> Result<(AccountProjection, Option<WriteResult>), AccountError> {
        // Step 1: index and freshness checks against current state.
        let from_index = {
            let accounts = self.accounts.lock().unwrap();
            let acc = accounts.get(&account_id);
            if let Some(acc) = acc {
                if let Some(hit) = Self::hit_of(acc, event_id) {
                    return Ok((Self::projection_of(account_id, acc), Some(hit)));
                }
                if min_version.is_some_and(|min| acc.last_version >= min) {
                    return Ok((Self::projection_of(account_id, acc), None));
                }
            }
            acc.map(|a| a.last_version).unwrap_or(0) + 1
        };

        // Step 2: read new events from Celeriant, following pagination.
        // collect() buffers the whole backlog; a production fold over long
        // histories would stream batches instead, or start from a snapshot.
        let batches = match self.pool.read_all(
            account_key(account_id),
            Some(ReadFilters::new(from_index)),
        ).await?.collect().await {
            Ok(b) => b,
            Err(ClientError::Server(ServerError::Read {
                kind: ReadError::AggregateNotExists, ..
            })) => {
                let accounts = self.accounts.lock().unwrap();
                let acc = accounts.get(&account_id);
                return Ok((acc.map(|a| Self::projection_of(account_id, a))
                    .unwrap_or(AccountProjection {
                        account_id, account_name: String::new(),
                        balance_cents: 0, last_version: 0, max_client_seq: 0,
                    }), None));
            }
            Err(e) => return Err(e.into()),
        };

        // Step 3: fold under the lock. A sibling request may have folded some
        // of these batches while we were reading; the version guard makes the
        // overlap a no-op. The dedup index is maintained mid-fold: an event
        // inside the window is indexed in the same pass that applies it, with
        // its remaining lifetime (the window minus the batch's server-time
        // age). Age is batch-vs-tip in server time; using the local clock for
        // age would let skew silently disable indexing.
        let tip_ts = batches.last().map(|b| b.server_timestamp).unwrap_or(0);
        let window_ms = DEDUP_WINDOW_SECS * 1000;
        let now = Instant::now();

        let mut accounts = self.accounts.lock().unwrap();
        let acc = accounts.entry(account_id).or_default();

        for batch in &batches {
            if batch.aggregate_version <= acc.last_version {
                continue; // a sibling already folded this batch
            }
            acc.last_version = batch.aggregate_version;
            let track_client_seq = batch.client_id == *SERVICE_CLIENT_ID;
            let age_ms = tip_ts.saturating_sub(batch.server_timestamp);

            for evt in &batch.events {
                if track_client_seq && evt.client_seq > acc.max_client_seq {
                    acc.max_client_seq = evt.client_seq;
                }
                acc.balance_cents = replay_event(acc.balance_cents, evt);

                if age_ms < window_ms {
                    if let Some(eid) = evt.event_id {
                        acc.recent.insert(eid, RecentWrite {
                            balance_cents: acc.balance_cents,
                            aggregate_version: batch.aggregate_version,
                            expires_at: now + Duration::from_millis(window_ms - age_ms),
                        });
                    }
                }
            }
        }

        acc.recent.retain(|_, r| r.expires_at > now);

        let hit = Self::hit_of(acc, event_id);
        Ok((Self::projection_of(account_id, acc), hit))
    }

    /// The transfer write is all-or-nothing, so an index hit on EITHER leg
    /// proves the whole transfer landed. Reconstruct a missing leg (entry
    /// expired, or never folded on this replica) from current state.
    async fn resolve_transfer_hits(
        &self,
        event_id: u128,
        from_account_id: u128,
        to_account_id: u128,
        from_hit: Option<WriteResult>,
        to_hit: Option<WriteResult>,
    ) -> Result<Option<TransferResult>, AccountError> {
        match (from_hit, to_hit) {
            (Some(f), Some(t)) => Ok(Some(TransferResult { from: f, to: t })),
            (Some(f), None) => {
                let (tp, th) = self.catch_up(to_account_id, None, Some(event_id)).await?;
                Ok(Some(TransferResult {
                    from: f,
                    to: th.unwrap_or(WriteResult {
                        balance_cents: tp.balance_cents,
                        aggregate_version: tp.last_version,
                    }),
                }))
            }
            (None, Some(t)) => {
                let (fp, fh) = self.catch_up(from_account_id, None, Some(event_id)).await?;
                Ok(Some(TransferResult {
                    from: fh.unwrap_or(WriteResult {
                        balance_cents: fp.balance_cents,
                        aggregate_version: fp.last_version,
                    }),
                    to: t,
                }))
            }
            (None, None) => Ok(None),
        }
    }

    /// Record a successful write: index entry first, then the projection bump,
    /// under one lock acquisition. The bump is conditional so it never goes
    /// backwards if a sibling's fold got there first.
    fn record_write(
        &self,
        account_id: u128,
        event_id: u128,
        new_balance: i64,
        new_version: u64,
        expected_version: u64,
        client_seq: u64,
    ) {
        let mut accounts = self.accounts.lock().unwrap();
        let acc = accounts.entry(account_id).or_default();
        acc.recent.insert(event_id, RecentWrite {
            balance_cents: new_balance,
            aggregate_version: new_version,
            // The write just happened, so it gets the full window. If the bump
            // below wins, this replica never re-folds its own batch, so this
            // entry is the only record it will ever have. Its lifetime must
            // not depend on anything else; a fold-tip-relative stamp would let
            // an idle account's entry be born almost expired.
            expires_at: Instant::now() + DEDUP_WINDOW,
        });
        if acc.last_version == expected_version {
            acc.balance_cents = new_balance;
            acc.last_version = new_version;
            acc.max_client_seq = acc.max_client_seq.max(client_seq);
        }
    }

    // --- Deposit ---

    pub async fn deposit(
        &self,
        account_id: u128,
        amount_cents: i32,
        event_id: u128,
    ) -> Result<WriteResult, AccountError> {
        let (mut projection, hit) = self.catch_up(account_id, None, Some(event_id)).await?;
        if let Some(hit) = hit {
            return Ok(hit);
        }

        let mut client_seq = projection.max_client_seq + 1;
        let mut re_derive_cei = false;

        for attempt in 1..=MAX_RETRIES {
            if attempt > 1 {
                backoff(attempt).await;
                let (p, hit) = self.catch_up(account_id, None, Some(event_id)).await?;
                if let Some(hit) = hit {
                    return Ok(hit);
                }
                projection = p;
                if re_derive_cei {
                    client_seq = projection.max_client_seq + 1;
                    re_derive_cei = false;
                }
            }

            if amount_cents <= 0 {
                return Err(AccountError::Validation("Amount must be positive.".into()));
            }

            let new_balance = projection.balance_cents + amount_cents as i64;

            let mut evt = json_event(1, &Deposited { amount_cents }).unwrap();
            evt.client_seq = client_seq;
            evt.event_id = Some(event_id);

            match self.pool.write_events_with(
                account_key(account_id),
                vec![evt],
                *SERVICE_CLIENT_ID,
                WriteEventsOptions {
                    allow_create: true,
                    expected_version: Some(projection.last_version),
                    enforce_client_idempotency: true,
                },
            ).await {
                Ok(_) => {
                    let new_version = projection.last_version + 1;
                    self.record_write(account_id, event_id, new_balance, new_version,
                        projection.last_version, client_seq);
                    return Ok(WriteResult { balance_cents: new_balance, aggregate_version: new_version });
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
                })) => {
                    tracing::debug!("OCC conflict on deposit for {account_id:x}, attempt {attempt}");
                    re_derive_cei = true;
                    continue;
                }
                Err(ClientError::RequestTimeout) => {
                    // Timeout is ambiguous; hold clientSeq constant
                    tracing::warn!("Timeout on deposit for {account_id:x}, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::InflightDuplicateWrite { .. }, ..
                })) => {
                    // Prior attempt accepted but not yet confirmed durable; treating it
                    // as success would be a false ack if it later rolls back.
                    tracing::debug!("Inflight duplicate on deposit for {account_id:x}, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::ClientIdempotencyViolation { .. }, ..
                })) => {
                    // Someone landed this client_seq: our timed-out prior attempt, or a
                    // sibling request that derived the same number. The stream knows which.
                    match verify::who_owns_seq(&self.pool, account_id, client_seq, event_id).await? {
                        SeqOwnership::Ours => {
                            tracing::info!("Idempotency hit on deposit for {account_id:x}: prior attempt landed");
                            let (p, hit) = self.catch_up(account_id, None, Some(event_id)).await?;
                            return Ok(hit.unwrap_or(WriteResult {
                                balance_cents: p.balance_cents,
                                aggregate_version: p.last_version,
                            }));
                        }
                        SeqOwnership::Sibling => {
                            tracing::info!("client_seq {client_seq} on {account_id:x} taken by a sibling; re-deriving");
                            re_derive_cei = true;
                            continue;
                        }
                        SeqOwnership::Unwritten => {
                            return Err(AccountError::OccExhausted(
                                "Deposit state unverifiable after idempotency violation; retry the request.".into(),
                            ));
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(AccountError::OccExhausted(
            "Deposit did not complete after retries: concurrent updates or timeouts. Retry the request.".into(),
        ))
    }

    // --- Withdraw ---

    pub async fn withdraw(
        &self,
        account_id: u128,
        amount_cents: i32,
        event_id: u128,
    ) -> Result<WriteResult, AccountError> {
        let (mut projection, hit) = self.catch_up(account_id, None, Some(event_id)).await?;
        if let Some(hit) = hit {
            return Ok(hit);
        }

        let mut client_seq = projection.max_client_seq + 1;
        let mut re_derive_cei = false;

        for attempt in 1..=MAX_RETRIES {
            if attempt > 1 {
                backoff(attempt).await;
                let (p, hit) = self.catch_up(account_id, None, Some(event_id)).await?;
                if let Some(hit) = hit {
                    return Ok(hit);
                }
                projection = p;
                if re_derive_cei {
                    client_seq = projection.max_client_seq + 1;
                    re_derive_cei = false;
                }
            }

            if amount_cents <= 0 {
                return Err(AccountError::Validation("Amount must be positive.".into()));
            }

            if projection.balance_cents < amount_cents as i64 {
                return Err(AccountError::InsufficientFunds {
                    balance_cents: projection.balance_cents,
                    requested_cents: amount_cents,
                });
            }

            let new_balance = projection.balance_cents - amount_cents as i64;

            let mut evt = json_event(2, &Withdrawn { amount_cents }).unwrap();
            evt.client_seq = client_seq;
            evt.event_id = Some(event_id);

            match self.pool.write_events_with(
                account_key(account_id),
                vec![evt],
                *SERVICE_CLIENT_ID,
                WriteEventsOptions {
                    allow_create: true,
                    expected_version: Some(projection.last_version),
                    enforce_client_idempotency: true,
                },
            ).await {
                Ok(_) => {
                    let new_version = projection.last_version + 1;
                    self.record_write(account_id, event_id, new_balance, new_version,
                        projection.last_version, client_seq);
                    return Ok(WriteResult { balance_cents: new_balance, aggregate_version: new_version });
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
                })) => {
                    tracing::debug!("OCC conflict on withdraw for {account_id:x}, attempt {attempt}");
                    re_derive_cei = true;
                    continue;
                }
                Err(ClientError::RequestTimeout) => {
                    tracing::warn!("Timeout on withdraw for {account_id:x}, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::InflightDuplicateWrite { .. }, ..
                })) => {
                    tracing::debug!("Inflight duplicate on withdraw for {account_id:x}, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::ClientIdempotencyViolation { .. }, ..
                })) => {
                    // Same verification as deposit.
                    match verify::who_owns_seq(&self.pool, account_id, client_seq, event_id).await? {
                        SeqOwnership::Ours => {
                            tracing::info!("Idempotency hit on withdraw for {account_id:x}: prior attempt landed");
                            let (p, hit) = self.catch_up(account_id, None, Some(event_id)).await?;
                            return Ok(hit.unwrap_or(WriteResult {
                                balance_cents: p.balance_cents,
                                aggregate_version: p.last_version,
                            }));
                        }
                        SeqOwnership::Sibling => {
                            tracing::info!("client_seq {client_seq} on {account_id:x} taken by a sibling; re-deriving");
                            re_derive_cei = true;
                            continue;
                        }
                        SeqOwnership::Unwritten => {
                            return Err(AccountError::OccExhausted(
                                "Withdrawal state unverifiable after idempotency violation; retry the request.".into(),
                            ));
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(AccountError::OccExhausted(
            "Withdrawal did not complete after retries: concurrent updates or timeouts. Retry the request.".into(),
        ))
    }

    // --- Transfer ---

    pub async fn transfer(
        &self,
        from_account_id: u128,
        to_account_id: u128,
        amount_cents: i32,
        event_id: u128,
    ) -> Result<TransferResult, AccountError> {
        if from_account_id == to_account_id {
            return Err(AccountError::Validation("Cannot transfer to the same account.".into()));
        }

        let (mut from_proj, from_hit) = self.catch_up(from_account_id, None, Some(event_id)).await?;
        let (mut to_proj, to_hit) = self.catch_up(to_account_id, None, Some(event_id)).await?;
        if let Some(done) = self
            .resolve_transfer_hits(event_id, from_account_id, to_account_id, from_hit, to_hit)
            .await?
        {
            return Ok(done);
        }

        let mut from_cei = from_proj.max_client_seq + 1;
        let mut to_cei = to_proj.max_client_seq + 1;
        let mut re_derive_cei = false;

        for attempt in 1..=MAX_RETRIES {
            if attempt > 1 {
                backoff(attempt).await;
                let (fp, from_hit) = self.catch_up(from_account_id, None, Some(event_id)).await?;
                let (tp, to_hit) = self.catch_up(to_account_id, None, Some(event_id)).await?;
                if let Some(done) = self
                    .resolve_transfer_hits(event_id, from_account_id, to_account_id, from_hit, to_hit)
                    .await?
                {
                    return Ok(done);
                }
                from_proj = fp;
                to_proj = tp;
                if re_derive_cei {
                    from_cei = from_proj.max_client_seq + 1;
                    to_cei = to_proj.max_client_seq + 1;
                    re_derive_cei = false;
                }
            }

            if amount_cents <= 0 {
                return Err(AccountError::Validation("Amount must be positive.".into()));
            }
            if from_proj.balance_cents < amount_cents as i64 {
                return Err(AccountError::InsufficientFunds {
                    balance_cents: from_proj.balance_cents,
                    requested_cents: amount_cents,
                });
            }

            let from_key = account_key(from_account_id);
            let to_key = account_key(to_account_id);

            let mut transfer_out = json_event(3, &TransferredOut {
                amount_cents,
                to_account_id: u128_to_uuid(to_account_id),
            }).unwrap();
            transfer_out.client_seq = from_cei;
            transfer_out.event_id = Some(event_id);

            let mut transfer_in = json_event(4, &TransferredIn {
                amount_cents,
                from_account_id: u128_to_uuid(from_account_id),
            }).unwrap();
            transfer_in.client_seq = to_cei;
            transfer_in.event_id = Some(event_id);

            let write_request = WriteRequest {
                correlation_id: None,
                client_id: *SERVICE_CLIENT_ID,
                user_id: None,
                writes: HashMap::from([
                    (from_key, SingleAggregateWrite {
                        events: vec![transfer_out],
                        allow_create: true,
                        expected_version: Some(from_proj.last_version),
                        enforce_client_idempotency: true,
                    }),
                    (to_key, SingleAggregateWrite {
                        events: vec![transfer_in],
                        allow_create: true,
                        expected_version: Some(to_proj.last_version),
                        enforce_client_idempotency: true,
                    }),
                ]),
            };

            match self.pool.write(write_request).await {
                Ok(_) => {
                    let new_from_balance = from_proj.balance_cents - amount_cents as i64;
                    let new_to_balance = to_proj.balance_cents + amount_cents as i64;
                    let new_from_version = from_proj.last_version + 1;
                    let new_to_version = to_proj.last_version + 1;

                    self.record_write(from_account_id, event_id, new_from_balance,
                        new_from_version, from_proj.last_version, from_cei);
                    self.record_write(to_account_id, event_id, new_to_balance,
                        new_to_version, to_proj.last_version, to_cei);

                    return Ok(TransferResult {
                        from: WriteResult { balance_cents: new_from_balance, aggregate_version: new_from_version },
                        to: WriteResult { balance_cents: new_to_balance, aggregate_version: new_to_version },
                    });
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
                })) => {
                    tracing::debug!("OCC conflict on transfer, attempt {attempt}");
                    re_derive_cei = true;
                    continue;
                }
                Err(ClientError::RequestTimeout) => {
                    tracing::warn!("Timeout on transfer, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::InflightDuplicateWrite { .. }, ..
                })) => {
                    tracing::debug!("Inflight duplicate on transfer, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::ClientIdempotencyViolation { .. }, ..
                })) => {
                    // At least one leg's client_seq was consumed; the error does not say
                    // which. The write is all-or-nothing, so owning either leg proves the
                    // whole transfer landed; a sibling owning a leg proves it did not.
                    let from_owner = verify::who_owns_seq(&self.pool, from_account_id, from_cei, event_id).await?;
                    let verdict = match from_owner {
                        SeqOwnership::Unwritten => {
                            verify::who_owns_seq(&self.pool, to_account_id, to_cei, event_id).await?
                        }
                        v => v,
                    };
                    match verdict {
                        SeqOwnership::Ours => {
                            tracing::info!("Idempotency hit on transfer: prior attempt landed");
                            let (fp, fh) = self.catch_up(from_account_id, None, Some(event_id)).await?;
                            let (tp, th) = self.catch_up(to_account_id, None, Some(event_id)).await?;
                            if let Some(done) = self
                                .resolve_transfer_hits(event_id, from_account_id, to_account_id, fh, th)
                                .await?
                            {
                                return Ok(done);
                            }
                            return Ok(TransferResult {
                                from: WriteResult { balance_cents: fp.balance_cents, aggregate_version: fp.last_version },
                                to: WriteResult { balance_cents: tp.balance_cents, aggregate_version: tp.last_version },
                            });
                        }
                        SeqOwnership::Sibling => {
                            tracing::info!("transfer client_seq taken by a sibling; re-deriving");
                            re_derive_cei = true;
                            continue;
                        }
                        SeqOwnership::Unwritten => {
                            return Err(AccountError::OccExhausted(
                                "Transfer state unverifiable after idempotency violation; retry the request.".into(),
                            ));
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(AccountError::OccExhausted(
            "Transfer did not complete after retries: concurrent updates or timeouts. Retry the request.".into(),
        ))
    }

    // --- Event History ---

    pub async fn get_history(
        &self,
        account_id: u128,
        from_version: Option<u64>,
    ) -> Result<(Vec<Value>, u64, i64), AccountError> {
        let (projection, _) = self.catch_up(account_id, None, None).await?;
        let key = account_key(account_id);

        let batches = match self.pool.read_all(
            key,
            Some(ReadFilters::new(from_version.unwrap_or(1))),
        ).await?.collect().await {
            Ok(b) => b,
            Err(ClientError::Server(ServerError::Read {
                kind: ReadError::AggregateNotExists, ..
            })) => {
                return Ok((vec![], projection.last_version, projection.balance_cents));
            }
            Err(e) => return Err(e.into()),
        };

        let events: Vec<Value> = batches.iter().flat_map(|b| {
            b.events.iter().map(|e| format_event(b, e))
        }).collect();

        Ok((events, projection.last_version, projection.balance_cents))
    }
}
