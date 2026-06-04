use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::server_error::{ReadError, ServerError, WriteError};
use celeriant_client_tokio::{
    CeleriantPool, ClientError, WriteEventsOptions, from_json, json_event,
};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest};
use serde_json::{Value, json};
use tokio_postgres::Client as PgClient;
use crate::constants::*;
use crate::events::*;
use crate::idempotency::{IdempotencyCache, IdempotencyEntry, TTL as CACHE_TTL};

// --- Result types ---

pub struct WriteResult {
    pub balance_cents: i64,
    pub aggregate_version: u64,
}

pub struct TransferResult {
    pub from: WriteResult,
    pub to: WriteResult,
}

// --- Projection ---

#[allow(dead_code)]
pub struct AccountProjection {
    pub account_id: u128,
    pub account_name: String,
    pub balance_cents: i64,
    pub last_version: u64,
    pub max_client_seq: u64,
}

// --- Errors ---

#[derive(Debug)]
pub enum AccountError {
    Validation(String),
    InsufficientFunds { balance_cents: i64, requested_cents: i32 },
    OccExhausted(String),
    Client(ClientError),
    Postgres(tokio_postgres::Error),
}

impl std::fmt::Display for AccountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(msg) => write!(f, "{msg}"),
            Self::InsufficientFunds { balance_cents, requested_cents } => {
                write!(f, "Cannot process ${:.2} — balance is ${:.2}",
                    *requested_cents as f64 / 100.0, *balance_cents as f64 / 100.0)
            }
            Self::OccExhausted(msg) => write!(f, "{msg}"),
            Self::Client(e) => write!(f, "{e}"),
            Self::Postgres(e) => write!(f, "{e}"),
        }
    }
}

impl From<ClientError> for AccountError {
    fn from(e: ClientError) -> Self { Self::Client(e) }
}

impl From<tokio_postgres::Error> for AccountError {
    fn from(e: tokio_postgres::Error) -> Self { Self::Postgres(e) }
}

const MAX_RETRIES: usize = 3;

pub struct AccountService {
    pool: Arc<CeleriantPool>,
    db: Arc<PgClient>,
    idempotency: Arc<IdempotencyCache>,
}

impl AccountService {
    pub fn new(
        pool: Arc<CeleriantPool>,
        db: Arc<PgClient>,
        idempotency: Arc<IdempotencyCache>,
    ) -> Self {
        Self { pool, db, idempotency }
    }

    // --- Catch-Up ---

    /// Lazy catch-up: read projection from Postgres, read new events from Celeriant,
    /// replay, upsert. Returns fresh projection state.
    pub async fn catch_up(
        &self,
        account_id: u128,
        min_version: Option<u64>,
    ) -> Result<AccountProjection, AccountError> {
        let key = account_key(account_id);
        let account_uuid = u128_to_uuid(account_id);

        // Step 1: Read current projection from Postgres
        let row = self.db.query_opt(
            "SELECT account_name, balance_cents, last_version, last_client_seq \
             FROM account_balances WHERE account_id = $1",
            &[&account_uuid],
        ).await?;

        let (account_name, mut balance_cents, last_version, mut max_client_seq) = match row {
            Some(row) => {
                let name: String = row.get(0);
                let balance: i64 = row.get(1);
                let batch: i64 = row.get(2);
                let cei: i64 = row.get(3);
                (name, balance, batch as u64, cei as u64)
            }
            None => (String::new(), 0i64, 0u64, 0u64),
        };

        // If caller needs a minimum freshness and projection is already fresh enough, return early
        if let Some(min) = min_version {
            if last_version >= min {
                return Ok(AccountProjection {
                    account_id, account_name, balance_cents, last_version, max_client_seq,
                });
            }
        }

        // Step 2: Read new events from Celeriant
        let from_index = last_version + 1;
        let response = match self.pool.read(ReadRequest {
            correlation_id: None,
            aggregate_key: key,
            filters: ReadFilters::new(from_index),
        }).await {
            Ok(r) => r,
            Err(ClientError::Server(ServerError::Read {
                kind: ReadError::AggregateNotExists, ..
            })) => {
                return Ok(AccountProjection {
                    account_id, account_name, balance_cents, last_version, max_client_seq,
                });
            }
            Err(e) => return Err(e.into()),
        };

        if response.event_batches.is_empty() {
            return Ok(AccountProjection {
                account_id, account_name, balance_cents, last_version, max_client_seq,
            });
        }

        // Step 3: Replay new events. Warm-window aging is batch-vs-tip in server time;
        // mixing in the local clock would let skew silently disable warming.
        let tip_ts = response.event_batches.last().map(|b| b.server_timestamp).unwrap_or(0);
        let warm_window_ms = CACHE_TTL.as_millis() as u64;

        let mut new_version = last_version;
        for batch in &response.event_batches {
            new_version = batch.aggregate_version;
            let track_client_seq = batch.client_id == *SERVICE_CLIENT_ID;
            let warm_cache = tip_ts.saturating_sub(batch.server_timestamp) < warm_window_ms;

            for evt in &batch.events {
                if track_client_seq && evt.client_seq > max_client_seq {
                    max_client_seq = evt.client_seq;
                }
                balance_cents = replay_event(balance_cents, evt);

                if warm_cache {
                    if let Some(eid) = evt.event_id {
                        self.idempotency.set(eid, account_id, IdempotencyEntry {
                            balance_cents,
                            aggregate_version: batch.aggregate_version,
                        });
                        // Record the seq's owner so a CEI violation can be verified.
                        if track_client_seq {
                            self.idempotency.set_seq_owner(account_id, evt.client_seq, eid);
                        }
                    }
                }
            }
        }

        // Step 4: UPSERT into Postgres (conditional — won't go backwards)
        if new_version > last_version {
            self.db.execute(
                "INSERT INTO account_balances (account_id, account_name, balance_cents, \
                 last_version, last_client_seq, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, now()) \
                 ON CONFLICT (account_id) DO UPDATE \
                 SET balance_cents = $3, \
                     account_name = COALESCE(NULLIF($2, ''), account_balances.account_name), \
                     last_version = $4, last_client_seq = $5, updated_at = now() \
                 WHERE account_balances.last_version < $4",
                &[
                    &account_uuid,
                    &account_name,
                    &balance_cents,
                    &(new_version as i64),
                    &(max_client_seq as i64),
                ],
            ).await?;
        }

        Ok(AccountProjection {
            account_id, account_name, balance_cents, last_version: new_version, max_client_seq,
        })
    }

    // --- Deposit ---

    pub async fn deposit(
        &self,
        account_id: u128,
        amount_cents: i32,
        event_id: Option<u128>,
    ) -> Result<WriteResult, AccountError> {
        let mut projection = self.catch_up(account_id, None).await?;

        if let Some(eid) = event_id {
            if let Some(hit) = self.idempotency.try_get(eid, account_id) {
                return Ok(WriteResult {
                    balance_cents: hit.balance_cents,
                    aggregate_version: hit.aggregate_version,
                });
            }
        }

        let mut client_seq = projection.max_client_seq + 1;
        let mut re_derive_cei = false;

        for attempt in 1..=MAX_RETRIES {
            if attempt > 1 {
                backoff(attempt).await;
                projection = self.catch_up(account_id, None).await?;
                if let Some(eid) = event_id {
                    if let Some(hit) = self.idempotency.try_get(eid, account_id) {
                        return Ok(WriteResult {
                            balance_cents: hit.balance_cents,
                            aggregate_version: hit.aggregate_version,
                        });
                    }
                }
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
            evt.event_id = event_id;

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
                    // Caches before the projection bump: the bump kills the replay path
                    // for same-key siblings, so the cache must already answer by then.
                    if let Some(eid) = event_id {
                        self.idempotency.set(eid, account_id, IdempotencyEntry {
                            balance_cents: new_balance,
                            aggregate_version: new_version,
                        });
                        self.idempotency.set_seq_owner(account_id, client_seq, eid);
                    }
                    self.update_projection_optimistically(
                        account_id, new_balance,
                        new_version, projection.last_version, client_seq,
                    ).await;
                    return Ok(WriteResult { balance_cents: new_balance, aggregate_version: new_version });
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
                })) if attempt < MAX_RETRIES => {
                    tracing::debug!("OCC conflict on deposit for {account_id:x}, attempt {attempt}");
                    re_derive_cei = true;
                    continue;
                }
                Err(ClientError::RequestTimeout) if attempt < MAX_RETRIES => {
                    // Timeout is ambiguous — hold clientSeq constant
                    tracing::warn!("Timeout on deposit for {account_id:x}, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::InflightDuplicateWrite { .. }, ..
                })) if attempt < MAX_RETRIES => {
                    // Prior attempt fsynced but not yet durable; treating it as success
                    // would be a false ack if it later rolls back. Hold client_seq and retry.
                    tracing::debug!("Inflight duplicate on deposit for {account_id:x}, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::ClientIdempotencyViolation { .. }, ..
                })) => {
                    // Someone landed this client_seq: our timed-out prior attempt, or a
                    // sibling request that derived the same number. Verify before claiming
                    // success; a false "done" silently drops the deposit.
                    let p = self.catch_up(account_id, None).await?;
                    if let Some(eid) = event_id {
                        if let Some(hit) = self.idempotency.try_get(eid, account_id) {
                            return Ok(WriteResult {
                                balance_cents: hit.balance_cents,
                                aggregate_version: hit.aggregate_version,
                            });
                        }
                        match self.idempotency.seq_owner(account_id, client_seq) {
                            Some(owner) if owner == eid => {
                                tracing::info!("Idempotency hit on deposit for {account_id:x} — prior attempt landed");
                                return Ok(WriteResult { balance_cents: p.balance_cents, aggregate_version: p.last_version });
                            }
                            Some(_) => {
                                // A sibling took the seq; our event never landed.
                                tracing::info!("client_seq {client_seq} on {account_id:x} taken by a sibling — re-deriving");
                                re_derive_cei = true;
                                continue;
                            }
                            None => {} // unknown: refuse to guess
                        }
                    }
                    return Err(AccountError::OccExhausted(
                        "Deposit state unverifiable after idempotency violation — retry the request.".into(),
                    ));
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(AccountError::OccExhausted(
            "Deposit failed after retries — account was modified concurrently.".into(),
        ))
    }

    // --- Withdraw ---

    pub async fn withdraw(
        &self,
        account_id: u128,
        amount_cents: i32,
        event_id: Option<u128>,
    ) -> Result<WriteResult, AccountError> {
        let mut projection = self.catch_up(account_id, None).await?;

        if let Some(eid) = event_id {
            if let Some(hit) = self.idempotency.try_get(eid, account_id) {
                return Ok(WriteResult {
                    balance_cents: hit.balance_cents,
                    aggregate_version: hit.aggregate_version,
                });
            }
        }

        let mut client_seq = projection.max_client_seq + 1;
        let mut re_derive_cei = false;

        for attempt in 1..=MAX_RETRIES {
            if attempt > 1 {
                backoff(attempt).await;
                projection = self.catch_up(account_id, None).await?;
                if let Some(eid) = event_id {
                    if let Some(hit) = self.idempotency.try_get(eid, account_id) {
                        return Ok(WriteResult {
                            balance_cents: hit.balance_cents,
                            aggregate_version: hit.aggregate_version,
                        });
                    }
                }
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
            evt.event_id = event_id;

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
                    // Caches before the projection bump, as in deposit.
                    if let Some(eid) = event_id {
                        self.idempotency.set(eid, account_id, IdempotencyEntry {
                            balance_cents: new_balance,
                            aggregate_version: new_version,
                        });
                        self.idempotency.set_seq_owner(account_id, client_seq, eid);
                    }
                    self.update_projection_optimistically(
                        account_id, new_balance,
                        new_version, projection.last_version, client_seq,
                    ).await;
                    return Ok(WriteResult { balance_cents: new_balance, aggregate_version: new_version });
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
                })) if attempt < MAX_RETRIES => {
                    tracing::debug!("OCC conflict on withdraw for {account_id:x}, attempt {attempt}");
                    re_derive_cei = true;
                    continue;
                }
                Err(ClientError::RequestTimeout) if attempt < MAX_RETRIES => {
                    tracing::warn!("Timeout on withdraw for {account_id:x}, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::InflightDuplicateWrite { .. }, ..
                })) if attempt < MAX_RETRIES => {
                    tracing::debug!("Inflight duplicate on withdraw for {account_id:x}, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::ClientIdempotencyViolation { .. }, ..
                })) => {
                    // Same verification as deposit.
                    let p = self.catch_up(account_id, None).await?;
                    if let Some(eid) = event_id {
                        if let Some(hit) = self.idempotency.try_get(eid, account_id) {
                            return Ok(WriteResult {
                                balance_cents: hit.balance_cents,
                                aggregate_version: hit.aggregate_version,
                            });
                        }
                        match self.idempotency.seq_owner(account_id, client_seq) {
                            Some(owner) if owner == eid => {
                                tracing::info!("Idempotency hit on withdraw for {account_id:x} — prior attempt landed");
                                return Ok(WriteResult { balance_cents: p.balance_cents, aggregate_version: p.last_version });
                            }
                            Some(_) => {
                                tracing::info!("client_seq {client_seq} on {account_id:x} taken by a sibling — re-deriving");
                                re_derive_cei = true;
                                continue;
                            }
                            None => {}
                        }
                    }
                    return Err(AccountError::OccExhausted(
                        "Withdrawal state unverifiable after idempotency violation — retry the request.".into(),
                    ));
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(AccountError::OccExhausted(
            "Withdrawal failed after retries — account was modified concurrently.".into(),
        ))
    }

    // --- Transfer ---

    pub async fn transfer(
        &self,
        from_account_id: u128,
        to_account_id: u128,
        amount_cents: i32,
        event_id: Option<u128>,
    ) -> Result<TransferResult, AccountError> {
        if from_account_id == to_account_id {
            return Err(AccountError::Validation("Cannot transfer to the same account.".into()));
        }

        let mut from_proj = self.catch_up(from_account_id, None).await?;
        let mut to_proj = self.catch_up(to_account_id, None).await?;

        // After catching up both aggregates, both sides of the transfer should be in
        // the cache if a prior attempt landed. Reconstruct only when both hit.
        if let Some(hit) = self.cached_transfer(event_id, from_account_id, to_account_id) {
            return Ok(hit);
        }

        let mut from_cei = from_proj.max_client_seq + 1;
        let mut to_cei = to_proj.max_client_seq + 1;
        let mut re_derive_cei = false;

        for attempt in 1..=MAX_RETRIES {
            if attempt > 1 {
                backoff(attempt).await;
                from_proj = self.catch_up(from_account_id, None).await?;
                to_proj = self.catch_up(to_account_id, None).await?;
                if let Some(hit) = self.cached_transfer(event_id, from_account_id, to_account_id) {
                    return Ok(hit);
                }
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
            transfer_out.event_id = event_id;

            let mut transfer_in = json_event(4, &TransferredIn {
                amount_cents,
                from_account_id: u128_to_uuid(from_account_id),
            }).unwrap();
            transfer_in.client_seq = to_cei;
            transfer_in.event_id = event_id;

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
                    let new_from_batch = from_proj.last_version + 1;
                    let new_to_batch = to_proj.last_version + 1;

                    // Caches before the projection bumps, as in deposit.
                    if let Some(eid) = event_id {
                        self.idempotency.set(eid, from_account_id, IdempotencyEntry {
                            balance_cents: new_from_balance,
                            aggregate_version: new_from_batch,
                        });
                        self.idempotency.set(eid, to_account_id, IdempotencyEntry {
                            balance_cents: new_to_balance,
                            aggregate_version: new_to_batch,
                        });
                        self.idempotency.set_seq_owner(from_account_id, from_cei, eid);
                        self.idempotency.set_seq_owner(to_account_id, to_cei, eid);
                    }
                    self.update_projection_optimistically(
                        from_account_id, new_from_balance,
                        new_from_batch, from_proj.last_version, from_cei,
                    ).await;
                    self.update_projection_optimistically(
                        to_account_id, new_to_balance,
                        new_to_batch, to_proj.last_version, to_cei,
                    ).await;

                    return Ok(TransferResult {
                        from: WriteResult { balance_cents: new_from_balance, aggregate_version: new_from_batch },
                        to: WriteResult { balance_cents: new_to_balance, aggregate_version: new_to_batch },
                    });
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
                })) if attempt < MAX_RETRIES => {
                    tracing::debug!("OCC conflict on transfer, attempt {attempt}");
                    re_derive_cei = true;
                    continue;
                }
                Err(ClientError::RequestTimeout) if attempt < MAX_RETRIES => {
                    tracing::warn!("Timeout on transfer, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::InflightDuplicateWrite { .. }, ..
                })) if attempt < MAX_RETRIES => {
                    tracing::debug!("Inflight duplicate on transfer, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::ClientIdempotencyViolation { .. }, ..
                })) => {
                    // At least one leg's client_seq was consumed: our prior transfer, or a
                    // sibling's write on either account. Verify.
                    let fp = self.catch_up(from_account_id, None).await?;
                    let tp = self.catch_up(to_account_id, None).await?;
                    if let Some(hit) = self.cached_transfer(event_id, from_account_id, to_account_id) {
                        return Ok(hit);
                    }
                    if let Some(eid) = event_id {
                        let from_owner = self.idempotency.seq_owner(from_account_id, from_cei);
                        let to_owner = self.idempotency.seq_owner(to_account_id, to_cei);
                        let sibling_took_a_leg = matches!(from_owner, Some(o) if o != eid)
                            || matches!(to_owner, Some(o) if o != eid);
                        if sibling_took_a_leg {
                            tracing::info!("transfer client_seq taken by a sibling — re-deriving");
                            re_derive_cei = true;
                            continue;
                        }
                        // The write is all-or-nothing: owning either leg proves the whole
                        // transfer landed.
                        if from_owner == Some(eid) || to_owner == Some(eid) {
                            tracing::info!("Idempotency hit on transfer — prior attempt landed");
                            return Ok(TransferResult {
                                from: WriteResult { balance_cents: fp.balance_cents, aggregate_version: fp.last_version },
                                to: WriteResult { balance_cents: tp.balance_cents, aggregate_version: tp.last_version },
                            });
                        }
                    }
                    return Err(AccountError::OccExhausted(
                        "Transfer state unverifiable after idempotency violation — retry the request.".into(),
                    ));
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(AccountError::OccExhausted(
            "Transfer failed after retries — accounts were modified concurrently.".into(),
        ))
    }

    // --- Event History ---

    pub async fn get_history(
        &self,
        account_id: u128,
        from_version: Option<u64>,
    ) -> Result<(Vec<Value>, u64, i64), AccountError> {
        let projection = self.catch_up(account_id, None).await?;
        let key = account_key(account_id);

        let response = match self.pool.read(ReadRequest {
            correlation_id: None,
            aggregate_key: key,
            filters: ReadFilters::new(from_version.unwrap_or(1)),
        }).await {
            Ok(r) => r,
            Err(ClientError::Server(ServerError::Read {
                kind: ReadError::AggregateNotExists, ..
            })) => {
                return Ok((vec![], projection.last_version, projection.balance_cents));
            }
            Err(e) => return Err(e.into()),
        };

        let events: Vec<Value> = response.event_batches.iter().flat_map(|b| {
            b.events.iter().map(|e| format_event(b, e))
        }).collect();

        Ok((events, projection.last_version, projection.balance_cents))
    }

    // --- Helpers ---

    fn cached_transfer(
        &self,
        event_id: Option<u128>,
        from_account_id: u128,
        to_account_id: u128,
    ) -> Option<TransferResult> {
        let eid = event_id?;
        let from = self.idempotency.try_get(eid, from_account_id)?;
        let to = self.idempotency.try_get(eid, to_account_id)?;
        Some(TransferResult {
            from: WriteResult { balance_cents: from.balance_cents, aggregate_version: from.aggregate_version },
            to: WriteResult { balance_cents: to.balance_cents, aggregate_version: to.aggregate_version },
        })
    }

    async fn update_projection_optimistically(
        &self,
        account_id: u128,
        new_balance: i64,
        new_version: u64,
        expected_aggregate_version: u64,
        client_seq: u64,
    ) {
        let result = self.db.execute(
            "UPDATE account_balances \
             SET balance_cents = $1, last_version = $2, \
                 last_client_seq = $3, updated_at = now() \
             WHERE account_id = $4 AND last_version = $5",
            &[
                &new_balance,
                &(new_version as i64),
                &(client_seq as i64),
                &u128_to_uuid(account_id),
                &(expected_aggregate_version as i64),
            ],
        ).await;

        if let Err(e) = result {
            // Postgres failure after successful Celeriant write — log and continue.
            // The write DID succeed (Celeriant is source of truth). Projection self-heals on next catch-up.
            tracing::warn!("Failed to update projection for {account_id:x} — will self-heal on next catch-up: {e}");
        }
    }
}

fn replay_event(balance_cents: i64, evt: &celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent) -> i64 {
    match evt.event_type_major {
        1 => balance_cents + from_json::<Deposited>(evt).map(|d| d.amount_cents as i64).unwrap_or(0),
        2 => balance_cents - from_json::<Withdrawn>(evt).map(|w| w.amount_cents as i64).unwrap_or(0),
        3 => balance_cents - from_json::<TransferredOut>(evt).map(|t| t.amount_cents as i64).unwrap_or(0),
        4 => balance_cents + from_json::<TransferredIn>(evt).map(|t| t.amount_cents as i64).unwrap_or(0),
        _ => balance_cents,
    }
}

fn format_event(
    batch: &celeriant_msg::response::aggregate_event_batch::AggregateEventBatch,
    evt: &celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent,
) -> Value {
    let mut out = json!({
        "aggregateVersion": batch.aggregate_version,
        "timestamp": batch.server_timestamp,
    });
    let (type_name, amount_cents) = match evt.event_type_major {
        1 => ("Deposited", from_json::<Deposited>(evt).map(|d| d.amount_cents).unwrap_or(0)),
        2 => ("Withdrawn", from_json::<Withdrawn>(evt).map(|w| w.amount_cents).unwrap_or(0)),
        3 => {
            let t = from_json::<TransferredOut>(evt).ok();
            if let Some(t) = &t {
                out["toAccountId"] = json!(t.to_account_id);
            }
            ("TransferredOut", t.map(|t| t.amount_cents).unwrap_or(0))
        }
        4 => {
            let t = from_json::<TransferredIn>(evt).ok();
            if let Some(t) = &t {
                out["fromAccountId"] = json!(t.from_account_id);
            }
            ("TransferredIn", t.map(|t| t.amount_cents).unwrap_or(0))
        }
        _ => ("Unknown", 0),
    };
    out["type"] = json!(type_name);
    out["amountCents"] = json!(amount_cents);
    out
}

async fn backoff(attempt: usize) {
    let delay_ms = (100 * (1 << (attempt - 1))) + rand::random::<u64>() % 50;
    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
}
