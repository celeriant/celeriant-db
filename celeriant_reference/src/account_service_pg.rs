//! Account service with a Postgres-backed projection, safe to run as many
//! replicas (e.g. k8s HPA) sharing one SERVICE_CLIENT_ID.
//!
//! The projection cursor lives in Postgres, so the request-response cache
//! must live there too: once any replica bumps the shared `last_version`, no
//! other replica's catch-up will ever replay those events, and an in-memory
//! cache could never be warmed. Cursor and cache move together, atomically,
//! in the same statement. The happy path stays at one Postgres round trip
//! (projection row + response row in one query) plus the Celeriant catch-up
//! read and the write itself.
//!
//! Default READ COMMITTED is enough. Celeriant's expected_version guard is the
//! serialization point; Postgres holds no invariant that spans statements.
//! That stays true only while each persist remains a single statement.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use celeriant_client_tokio::server_error::{ReadError, ServerError, WriteError};
use celeriant_client_tokio::{CeleriantPool, ClientError, WriteEventsOptions, json_event};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use serde_json::Value;
use tokio_postgres::Client as PgClient;
use uuid::Uuid;

use crate::constants::*;
use crate::events::*;
use crate::types::{AccountError, AccountProjection, TransferResult, WriteResult};
use crate::verify::{self, DEDUP_WINDOW_SECS, MAX_RETRIES, SeqOwnership, backoff};

pub struct PgAccountService {
    pool: Arc<CeleriantPool>,
    db: Arc<PgClient>,
}

impl PgAccountService {
    pub fn new(pool: Arc<CeleriantPool>, db: Arc<PgClient>) -> Self {
        Self { pool, db }
    }

    pub async fn init_schema(db: &PgClient) -> Result<(), tokio_postgres::Error> {
        db.execute(
            "CREATE TABLE IF NOT EXISTS account_balances (
                account_id        UUID PRIMARY KEY,
                account_name      TEXT NOT NULL,
                balance_cents     BIGINT NOT NULL DEFAULT 0,
                last_version      BIGINT NOT NULL DEFAULT 0,
                last_client_seq   BIGINT NOT NULL DEFAULT 0,
                updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            &[],
        ).await?;

        // The response cache for retried requests. It lives next to the
        // projection cursor because the cursor lives here (see module docs).
        // Window-expired rows are cleaned opportunistically during catch-up
        // persists.
        db.execute(
            "CREATE TABLE IF NOT EXISTS request_responses (
                event_id           UUID NOT NULL,
                aggregate_id       UUID NOT NULL,
                balance_cents      BIGINT NOT NULL,
                aggregate_version  BIGINT NOT NULL,
                expires_at         TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (event_id, aggregate_id)
            )",
            &[],
        ).await?;
        db.execute(
            "CREATE INDEX IF NOT EXISTS request_responses_expires_at \
             ON request_responses (expires_at)",
            &[],
        ).await?;
        Ok(())
    }

    // --- Catch-Up ---

    /// Lazy catch-up: read the projection and response rows from Postgres in
    /// one query, read new events from Celeriant, fold, persist. Returns fresh
    /// projection state, plus the original response if `event_id` already
    /// landed.
    pub async fn catch_up(
        &self,
        account_id: u128,
        min_version: Option<u64>,
        event_id: Option<u128>,
    ) -> Result<(AccountProjection, Option<WriteResult>), AccountError> {
        let key = account_key(account_id);
        let account_uuid = u128_to_uuid(account_id);
        let event_uuid = event_id.map(Uuid::from_u128);

        // Step 1: projection row and response row, one round trip. The
        // response row answers "did this request already land?" for retries
        // arriving on a different replica than the one that served the
        // original.
        let row = self.db.query_opt(
            "SELECT b.account_name, b.balance_cents, b.last_version, b.last_client_seq, \
                    i.balance_cents, i.aggregate_version \
             FROM account_balances b \
             LEFT JOIN request_responses i \
               ON i.event_id = $2 AND i.aggregate_id = b.account_id AND i.expires_at > now() \
             WHERE b.account_id = $1",
            &[&account_uuid, &event_uuid],
        ).await?;

        let (account_name, mut balance_cents, last_version, mut max_client_seq, hit) = match row {
            Some(row) => {
                let name: String = row.get(0);
                let balance: i64 = row.get(1);
                let version: i64 = row.get(2);
                let seq: i64 = row.get(3);
                let hit = match (row.get::<_, Option<i64>>(4), row.get::<_, Option<i64>>(5)) {
                    (Some(b), Some(v)) => Some(WriteResult { balance_cents: b, aggregate_version: v as u64 }),
                    _ => None,
                };
                (name, balance, version as u64, seq as u64, hit)
            }
            None => (String::new(), 0i64, 0u64, 0u64, None),
        };

        if hit.is_some() {
            return Ok((AccountProjection {
                account_id, account_name, balance_cents, last_version, max_client_seq,
            }, hit));
        }

        // If caller needs a minimum freshness and projection is already fresh enough, return early
        if let Some(min) = min_version {
            if last_version >= min {
                return Ok((AccountProjection {
                    account_id, account_name, balance_cents, last_version, max_client_seq,
                }, None));
            }
        }

        // Step 2: read new events from Celeriant, following pagination.
        // collect() buffers the whole backlog; a production fold over long
        // histories would stream batches instead, or start from a snapshot.
        let from_index = last_version + 1;
        let batches = match self.pool.read_all(
            key,
            Some(ReadFilters::new(from_index)),
        ).await?.collect().await {
            Ok(b) => b,
            Err(ClientError::Server(ServerError::Read {
                kind: ReadError::AggregateNotExists, ..
            })) => {
                return Ok((AccountProjection {
                    account_id, account_name, balance_cents, last_version, max_client_seq,
                }, None));
            }
            Err(e) => return Err(e.into()),
        };

        if batches.is_empty() {
            return Ok((AccountProjection {
                account_id, account_name, balance_cents, last_version, max_client_seq,
            }, None));
        }

        // Step 3: replay new events, collecting response rows for the recent
        // window. A replayed event gets its remaining lifetime, the window
        // minus its server-time age (batch vs tip; the local clock would let
        // skew misjudge it), so a row can never outlive the stated window.
        // Keyed by event_id (sorted) so a reused key cannot produce two rows
        // for one upsert, and concurrent replicas upsert in the same order.
        let tip_ts = batches.last().map(|b| b.server_timestamp).unwrap_or(0);
        let warm_window_ms = DEDUP_WINDOW_SECS * 1000;

        let mut warm_rows: BTreeMap<u128, (i64, i64, i64)> = BTreeMap::new();
        let mut found: Option<WriteResult> = None;

        let mut new_version = last_version;
        for batch in &batches {
            new_version = batch.aggregate_version;
            let track_client_seq = batch.client_id == *SERVICE_CLIENT_ID;
            let age_ms = tip_ts.saturating_sub(batch.server_timestamp);

            for evt in &batch.events {
                if track_client_seq && evt.client_seq > max_client_seq {
                    max_client_seq = evt.client_seq;
                }
                balance_cents = replay_event(balance_cents, evt);

                if age_ms < warm_window_ms {
                    if let Some(eid) = evt.event_id {
                        warm_rows.insert(eid, (
                            balance_cents,
                            batch.aggregate_version as i64,
                            (warm_window_ms - age_ms) as i64,
                        ));
                        if event_id == Some(eid) {
                            found = Some(WriteResult {
                                balance_cents,
                                aggregate_version: batch.aggregate_version,
                            });
                        }
                    }
                }
            }
        }

        // Step 4: persist the cursor and the response rows in one atomic
        // statement. The bump kills the replay path for every replica, so the
        // rows must be visible no later than the bump; atomicity guarantees
        // it. The upsert refreshes any existing row rather than skipping it,
        // so a re-warmed event_id never loses its entry.
        if new_version > last_version {
            let mut warm_eids: Vec<Uuid> = Vec::with_capacity(warm_rows.len());
            let mut warm_balances: Vec<i64> = Vec::with_capacity(warm_rows.len());
            let mut warm_versions: Vec<i64> = Vec::with_capacity(warm_rows.len());
            let mut warm_remaining_ms: Vec<i64> = Vec::with_capacity(warm_rows.len());
            for (eid, (bal, ver, rem_ms)) in &warm_rows {
                warm_eids.push(Uuid::from_u128(*eid));
                warm_balances.push(*bal);
                warm_versions.push(*ver);
                warm_remaining_ms.push(*rem_ms);
            }

            self.db.execute(
                "WITH proj AS ( \
                     INSERT INTO account_balances (account_id, account_name, balance_cents, \
                         last_version, last_client_seq, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, now()) \
                     ON CONFLICT (account_id) DO UPDATE \
                     SET balance_cents = $3, \
                         account_name = COALESCE(NULLIF($2, ''), account_balances.account_name), \
                         last_version = $4, last_client_seq = $5, updated_at = now() \
                     WHERE account_balances.last_version < $4 \
                 ) \
                 INSERT INTO request_responses (event_id, aggregate_id, balance_cents, aggregate_version, expires_at) \
                 SELECT t.eid, $1, t.bal, t.ver, now() + t.rem_ms * interval '1 millisecond' \
                 FROM unnest($6::uuid[], $7::bigint[], $8::bigint[], $9::bigint[]) AS t(eid, bal, ver, rem_ms) \
                 ON CONFLICT (event_id, aggregate_id) DO UPDATE \
                 SET balance_cents = EXCLUDED.balance_cents, \
                     aggregate_version = EXCLUDED.aggregate_version, \
                     expires_at = GREATEST(request_responses.expires_at, EXCLUDED.expires_at)",
                &[
                    &account_uuid,
                    &account_name,
                    &balance_cents,
                    &(new_version as i64),
                    &(max_client_seq as i64),
                    &warm_eids,
                    &warm_balances,
                    &warm_versions,
                    &warm_remaining_ms,
                ],
            ).await?;

            // Housekeeping, deliberately outside the atomic statement: a
            // delete and an upsert touching the same row in one statement is
            // undefined in Postgres. This path only runs when the cursor was
            // behind; production would run it on a timer instead.
            self.db.execute(
                "DELETE FROM request_responses WHERE expires_at < now()",
                &[],
            ).await?;
        }

        Ok((AccountProjection {
            account_id, account_name, balance_cents, last_version: new_version, max_client_seq,
        }, found))
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
                    self.persist_write(account_id, event_id, new_balance, new_version,
                        projection.last_version, client_seq).await;
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
                    self.persist_write(account_id, event_id, new_balance, new_version,
                        projection.last_version, client_seq).await;
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

    /// The transfer write is all-or-nothing, so an index hit on EITHER leg
    /// proves the whole transfer landed. Reconstruct a missing leg (row
    /// expired) from current state.
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

                    self.persist_write(from_account_id, event_id, new_from_balance,
                        new_from_version, from_proj.last_version, from_cei).await;
                    self.persist_write(to_account_id, event_id, new_to_balance,
                        new_to_version, to_proj.last_version, to_cei).await;

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

    // --- Helpers ---

    /// Persist a successful write: response row and projection bump in one
    /// atomic statement, so no replica can ever observe the bump without the
    /// row.
    /// A Postgres failure here is logged and swallowed: the Celeriant write
    /// succeeded (Celeriant is the source of truth), and with neither the row
    /// nor the bump applied, the next catch-up replays the event and self-heals.
    async fn persist_write(
        &self,
        account_id: u128,
        event_id: u128,
        new_balance: i64,
        new_version: u64,
        expected_version: u64,
        client_seq: u64,
    ) {
        let result = self.db.execute(
            &format!(
                "WITH proj AS ( \
                     UPDATE account_balances \
                     SET balance_cents = $1, last_version = $2, \
                         last_client_seq = $3, updated_at = now() \
                     WHERE account_id = $4 AND last_version = $5 \
                 ) \
                 INSERT INTO request_responses (event_id, aggregate_id, balance_cents, aggregate_version, expires_at) \
                 VALUES ($6, $4, $1, $2, now() + interval '{DEDUP_WINDOW_SECS} seconds') \
                 ON CONFLICT (event_id, aggregate_id) DO UPDATE \
                 SET balance_cents = EXCLUDED.balance_cents, \
                     aggregate_version = EXCLUDED.aggregate_version, \
                     expires_at = GREATEST(request_responses.expires_at, EXCLUDED.expires_at)"
            ),
            &[
                &new_balance,
                &(new_version as i64),
                &(client_seq as i64),
                &u128_to_uuid(account_id),
                &(expected_version as i64),
                &Uuid::from_u128(event_id),
            ],
        ).await;

        if let Err(e) = result {
            tracing::warn!("Failed to persist write for {account_id:x}, will self-heal on next catch-up: {e}");
        }
    }
}
