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

// --- Result types ---

pub struct WriteResult {
    pub balance_cents: i64,
    pub batch_index: u64,
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
    pub last_batch_index: u64,
    pub max_client_event_index: u64,
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
}

impl AccountService {
    pub fn new(pool: Arc<CeleriantPool>, db: Arc<PgClient>) -> Self {
        Self { pool, db }
    }

    // --- Catch-Up ---

    /// Lazy catch-up: read projection from Postgres, read new events from Celeriant,
    /// replay, upsert. Returns fresh projection state.
    pub async fn catch_up(
        &self,
        account_id: u128,
        min_batch_index: Option<u64>,
    ) -> Result<AccountProjection, AccountError> {
        let key = account_key(account_id);
        let account_uuid = u128_to_uuid(account_id);

        // Step 1: Read current projection from Postgres
        let row = self.db.query_opt(
            "SELECT account_name, balance_cents, last_batch_index, last_client_event_index \
             FROM account_balances WHERE account_id = $1",
            &[&account_uuid],
        ).await?;

        let (account_name, mut balance_cents, last_batch_index, mut max_client_event_index) = match row {
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
        if let Some(min) = min_batch_index {
            if last_batch_index >= min {
                return Ok(AccountProjection {
                    account_id, account_name, balance_cents, last_batch_index, max_client_event_index,
                });
            }
        }

        // Step 2: Read new events from Celeriant
        let from_index = last_batch_index + 1;
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
                    account_id, account_name, balance_cents, last_batch_index, max_client_event_index,
                });
            }
            Err(e) => return Err(e.into()),
        };

        if response.event_batches.is_empty() {
            return Ok(AccountProjection {
                account_id, account_name, balance_cents, last_batch_index, max_client_event_index,
            });
        }

        // Step 3: Replay new events
        let mut new_batch_index = last_batch_index;
        for batch in &response.event_batches {
            new_batch_index = batch.event_batch_index;

            // Track max ClientEventIndex for our service ClientId
            if batch.client_id == *SERVICE_CLIENT_ID {
                for evt in &batch.events {
                    if evt.client_event_index > max_client_event_index {
                        max_client_event_index = evt.client_event_index;
                    }
                }
            }

            for evt in &batch.events {
                balance_cents = replay_event(balance_cents, evt);
            }
        }

        // Step 4: UPSERT into Postgres (conditional — won't go backwards)
        if new_batch_index > last_batch_index {
            self.db.execute(
                "INSERT INTO account_balances (account_id, account_name, balance_cents, \
                 last_batch_index, last_client_event_index, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, now()) \
                 ON CONFLICT (account_id) DO UPDATE \
                 SET balance_cents = $3, account_name = $2, \
                     last_batch_index = $4, last_client_event_index = $5, updated_at = now() \
                 WHERE account_balances.last_batch_index < $4",
                &[
                    &account_uuid,
                    &account_name,
                    &balance_cents,
                    &(new_batch_index as i64),
                    &(max_client_event_index as i64),
                ],
            ).await?;
        }

        Ok(AccountProjection {
            account_id, account_name, balance_cents, last_batch_index: new_batch_index, max_client_event_index,
        })
    }

    // --- Deposit ---

    pub async fn deposit(
        &self,
        account_id: u128,
        amount_cents: i32,
    ) -> Result<WriteResult, AccountError> {
        let mut projection = self.catch_up(account_id, None).await?;
        let mut client_event_index = projection.max_client_event_index + 1;
        let mut re_derive_cei = false;

        for attempt in 1..=MAX_RETRIES {
            if attempt > 1 {
                backoff(attempt).await;
                projection = self.catch_up(account_id, None).await?;
                if re_derive_cei {
                    client_event_index = projection.max_client_event_index + 1;
                    re_derive_cei = false;
                }
            }

            if amount_cents <= 0 {
                return Err(AccountError::Validation("Amount must be positive.".into()));
            }

            let new_balance = projection.balance_cents + amount_cents as i64;

            let mut evt = json_event(1, &Deposited { amount_cents }).unwrap();
            evt.client_event_index = client_event_index;

            match self.pool.write_events_with(
                account_key(account_id),
                vec![evt],
                WriteEventsOptions {
                    client_id: *SERVICE_CLIENT_ID,
                    allow_create: true,
                    expected_event_batch_index: Some(projection.last_batch_index),
                    enforce_client_idempotency: true,
                },
            ).await {
                Ok(_) => {
                    let new_batch_index = projection.last_batch_index + 1;
                    self.update_projection_optimistically(
                        account_id, new_balance,
                        new_batch_index, projection.last_batch_index, client_event_index,
                    ).await;
                    return Ok(WriteResult { balance_cents: new_balance, batch_index: new_batch_index });
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
                })) if attempt < MAX_RETRIES => {
                    tracing::debug!("OCC conflict on deposit for {account_id:x}, attempt {attempt}");
                    re_derive_cei = true;
                    continue;
                }
                Err(ClientError::RequestTimeout) if attempt < MAX_RETRIES => {
                    // Timeout is ambiguous — hold clientEventIndex constant
                    tracing::warn!("Timeout on deposit for {account_id:x}, attempt {attempt}");
                    continue;
                }
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::ClientIdempotencyViolation { .. }, ..
                })) => {
                    // Prior attempt already landed (K-FAIL recovery)
                    tracing::info!("Idempotency hit on deposit for {account_id:x} — prior attempt landed");
                    let p = self.catch_up(account_id, None).await?;
                    return Ok(WriteResult { balance_cents: p.balance_cents, batch_index: p.last_batch_index });
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
    ) -> Result<WriteResult, AccountError> {
        let mut projection = self.catch_up(account_id, None).await?;
        let mut client_event_index = projection.max_client_event_index + 1;
        let mut re_derive_cei = false;

        for attempt in 1..=MAX_RETRIES {
            if attempt > 1 {
                backoff(attempt).await;
                projection = self.catch_up(account_id, None).await?;
                if re_derive_cei {
                    client_event_index = projection.max_client_event_index + 1;
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
            evt.client_event_index = client_event_index;

            match self.pool.write_events_with(
                account_key(account_id),
                vec![evt],
                WriteEventsOptions {
                    client_id: *SERVICE_CLIENT_ID,
                    allow_create: true,
                    expected_event_batch_index: Some(projection.last_batch_index),
                    enforce_client_idempotency: true,
                },
            ).await {
                Ok(_) => {
                    let new_batch_index = projection.last_batch_index + 1;
                    self.update_projection_optimistically(
                        account_id, new_balance,
                        new_batch_index, projection.last_batch_index, client_event_index,
                    ).await;
                    return Ok(WriteResult { balance_cents: new_balance, batch_index: new_batch_index });
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
                    kind: WriteError::ClientIdempotencyViolation { .. }, ..
                })) => {
                    tracing::info!("Idempotency hit on withdraw for {account_id:x} — prior attempt landed");
                    let p = self.catch_up(account_id, None).await?;
                    return Ok(WriteResult { balance_cents: p.balance_cents, batch_index: p.last_batch_index });
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
    ) -> Result<TransferResult, AccountError> {
        if from_account_id == to_account_id {
            return Err(AccountError::Validation("Cannot transfer to the same account.".into()));
        }

        let mut from_proj = self.catch_up(from_account_id, None).await?;
        let mut to_proj = self.catch_up(to_account_id, None).await?;

        let mut from_cei = from_proj.max_client_event_index + 1;
        let mut to_cei = to_proj.max_client_event_index + 1;
        let mut re_derive_cei = false;

        for attempt in 1..=MAX_RETRIES {
            if attempt > 1 {
                backoff(attempt).await;
                from_proj = self.catch_up(from_account_id, None).await?;
                to_proj = self.catch_up(to_account_id, None).await?;
                if re_derive_cei {
                    from_cei = from_proj.max_client_event_index + 1;
                    to_cei = to_proj.max_client_event_index + 1;
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
            transfer_out.client_event_index = from_cei;

            let mut transfer_in = json_event(4, &TransferredIn {
                amount_cents,
                from_account_id: u128_to_uuid(from_account_id),
            }).unwrap();
            transfer_in.client_event_index = to_cei;

            let write_request = WriteRequest {
                correlation_id: None,
                client_id: *SERVICE_CLIENT_ID,
                user_id: None,
                writes: HashMap::from([
                    (from_key, SingleAggregateWrite {
                        events: vec![transfer_out],
                        allow_create: true,
                        expected_event_batch_index: Some(from_proj.last_batch_index),
                        enforce_client_idempotency: true,
                        compression_type_id: 0,
                        compression_level: None,
                    }),
                    (to_key, SingleAggregateWrite {
                        events: vec![transfer_in],
                        allow_create: true,
                        expected_event_batch_index: Some(to_proj.last_batch_index),
                        enforce_client_idempotency: true,
                        compression_type_id: 0,
                        compression_level: None,
                    }),
                ]),
            };

            match self.pool.write(write_request).await {
                Ok(_) => {
                    let new_from_balance = from_proj.balance_cents - amount_cents as i64;
                    let new_to_balance = to_proj.balance_cents + amount_cents as i64;
                    let new_from_batch = from_proj.last_batch_index + 1;
                    let new_to_batch = to_proj.last_batch_index + 1;

                    self.update_projection_optimistically(
                        from_account_id, new_from_balance,
                        new_from_batch, from_proj.last_batch_index, from_cei,
                    ).await;
                    self.update_projection_optimistically(
                        to_account_id, new_to_balance,
                        new_to_batch, to_proj.last_batch_index, to_cei,
                    ).await;

                    return Ok(TransferResult {
                        from: WriteResult { balance_cents: new_from_balance, batch_index: new_from_batch },
                        to: WriteResult { balance_cents: new_to_balance, batch_index: new_to_batch },
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
                    kind: WriteError::ClientIdempotencyViolation { .. }, ..
                })) => {
                    tracing::info!("Idempotency hit on transfer — prior attempt landed");
                    let fp = self.catch_up(from_account_id, None).await?;
                    let tp = self.catch_up(to_account_id, None).await?;
                    return Ok(TransferResult {
                        from: WriteResult { balance_cents: fp.balance_cents, batch_index: fp.last_batch_index },
                        to: WriteResult { balance_cents: tp.balance_cents, batch_index: tp.last_batch_index },
                    });
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
        from_batch_index: Option<u64>,
    ) -> Result<(Vec<Value>, u64, i64), AccountError> {
        let projection = self.catch_up(account_id, None).await?;
        let key = account_key(account_id);

        let response = match self.pool.read(ReadRequest {
            correlation_id: None,
            aggregate_key: key,
            filters: ReadFilters::new(from_batch_index.unwrap_or(1)),
        }).await {
            Ok(r) => r,
            Err(ClientError::Server(ServerError::Read {
                kind: ReadError::AggregateNotExists, ..
            })) => {
                return Ok((vec![], projection.last_batch_index, projection.balance_cents));
            }
            Err(e) => return Err(e.into()),
        };

        let events: Vec<Value> = response.event_batches.iter().flat_map(|b| {
            b.events.iter().map(|e| format_event(b, e))
        }).collect();

        Ok((events, projection.last_batch_index, projection.balance_cents))
    }

    // --- Helpers ---

    async fn update_projection_optimistically(
        &self,
        account_id: u128,
        new_balance: i64,
        new_batch_index: u64,
        expected_batch_index: u64,
        client_event_index: u64,
    ) {
        let result = self.db.execute(
            "UPDATE account_balances \
             SET balance_cents = $1, last_batch_index = $2, \
                 last_client_event_index = $3, updated_at = now() \
             WHERE account_id = $4 AND last_batch_index = $5",
            &[
                &new_balance,
                &(new_batch_index as i64),
                &(client_event_index as i64),
                &u128_to_uuid(account_id),
                &(expected_batch_index as i64),
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
    let (type_name, amount_cents) = match evt.event_type_major {
        1 => ("Deposited", from_json::<Deposited>(evt).map(|d| d.amount_cents).unwrap_or(0)),
        2 => ("Withdrawn", from_json::<Withdrawn>(evt).map(|w| w.amount_cents).unwrap_or(0)),
        3 => ("TransferredOut", from_json::<TransferredOut>(evt).map(|t| t.amount_cents).unwrap_or(0)),
        4 => ("TransferredIn", from_json::<TransferredIn>(evt).map(|t| t.amount_cents).unwrap_or(0)),
        _ => ("Unknown", 0),
    };

    json!({
        "batchIndex": batch.event_batch_index,
        "type": type_name,
        "amountCents": amount_cents,
        "timestamp": batch.server_timestamp,
    })
}

async fn backoff(attempt: usize) {
    let delay_ms = (100 * (1 << (attempt - 1))) + rand::random::<u64>() % 50;
    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
}
