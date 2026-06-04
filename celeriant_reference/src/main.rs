mod account_service;
mod constants;
mod events;
mod idempotency;

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use celeriant_client_tokio::{
    CeleriantPool, ClientError, PoolOptions, WatchConnection, WatchOptions, WriteEventsOptions,
    json_event,
};
use celeriant_msg::request::requests::{AggregateDetailsRequest, WatchRequest};
use futures::stream;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio_postgres::NoTls;
use uuid::Uuid;

use account_service::{AccountError, AccountService};
use constants::*;
use events::*;
use idempotency::IdempotencyCache;

struct AppState {
    account_service: AccountService,
    idempotency: Arc<IdempotencyCache>,
    watch_tx: broadcast::Sender<Value>,
}

type SharedState = Arc<AppState>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let celeriant_address =
        std::env::var("CELERIANT_ADDRESS").unwrap_or_else(|_| "localhost:10000".into());
    let postgres_url = std::env::var("POSTGRES_URL").unwrap_or_else(|_| {
        "host=localhost dbname=celeriant_reference user=demo password=demo".into()
    });

    let pool = Arc::new(CeleriantPool::new(PoolOptions::new(&celeriant_address)));

    let (pg_client, pg_connection) = tokio_postgres::connect(&postgres_url, NoTls)
        .await
        .expect("Failed to connect to Postgres");
    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            tracing::error!("Postgres connection error: {e}");
        }
    });
    let db = Arc::new(pg_client);

    init_database(&db).await;
    seed_accounts(&pool, &db).await;

    let (watch_tx, _) = broadcast::channel::<Value>(64);
    let idempotency = Arc::new(IdempotencyCache::new());
    let account_service = AccountService::new(pool.clone(), db.clone(), idempotency.clone());

    let state = Arc::new(AppState {
        account_service,
        idempotency,
        watch_tx: watch_tx.clone(),
    });

    let watch_address = celeriant_address.clone();
    let watch_tx_clone = watch_tx.clone();
    tokio::spawn(async move {
        watch_broadcaster(&watch_address, watch_tx_clone).await;
    });

    let app = Router::new()
        .route("/api/accounts", get(get_accounts))
        .route("/api/accounts/{account_id}/balance", get(get_balance))
        .route("/api/accounts/{account_id}/history", get(get_history))
        .route("/api/accounts/{account_id}/deposit", post(deposit))
        .route("/api/accounts/{account_id}/withdraw", post(withdraw))
        .route("/api/transfers", post(transfer))
        .route("/api/watch/stream", get(watch_sse))
        .fallback_service(tower_http::services::ServeDir::new("celeriant_reference/wwwroot"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:5001").await.unwrap();
    tracing::info!("Reference listening on http://localhost:5001");
    axum::serve(listener, app).await.unwrap();
}

// --- Routes ---

async fn get_accounts() -> Json<Value> {
    Json(json!({
        "accounts": ACCOUNTS.iter().map(|a| json!({
            "id": u128_to_uuid(a.id),
            "name": a.name,
        })).collect::<Vec<_>>(),
    }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BalanceQuery {
    min_version: Option<u64>,
}

async fn get_balance(
    Path(account_id): Path<Uuid>,
    Query(params): Query<BalanceQuery>,
    State(state): State<SharedState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let projection = state.account_service
        .catch_up(account_id.as_u128(), params.min_version)
        .await
        .map_err(map_account_error)?;
    Ok(Json(json!({
        "balanceCents": projection.balance_cents,
        "aggregateVersion": projection.last_version,
    })))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryQuery {
    from_version: Option<u64>,
}

async fn get_history(
    Path(account_id): Path<Uuid>,
    Query(params): Query<HistoryQuery>,
    State(state): State<SharedState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let (events, current_version, balance_cents) = state.account_service
        .get_history(account_id.as_u128(), params.from_version)
        .await
        .map_err(map_account_error)?;
    Ok(Json(json!({
        "events": events,
        "currentAggregateVersion": current_version,
        "balanceCents": balance_cents,
    })))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AmountRequest {
    amount_cents: i32,
}

async fn deposit(
    Path(account_id): Path<Uuid>,
    headers: HeaderMap,
    State(state): State<SharedState>,
    Json(req): Json<AmountRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let event_id = request_event_id(&headers);
    if let Some(eid) = event_id {
        if let Some(hit) = state.idempotency.try_get(eid, account_id.as_u128()) {
            return Ok(Json(json!({
                "balanceCents": hit.balance_cents,
                "aggregateVersion": hit.aggregate_version,
            })));
        }
    }

    let result = state.account_service
        .deposit(account_id.as_u128(), req.amount_cents, event_id)
        .await
        .map_err(map_account_error)?;

    Ok(Json(json!({
        "balanceCents": result.balance_cents,
        "aggregateVersion": result.aggregate_version,
    })))
}

async fn withdraw(
    Path(account_id): Path<Uuid>,
    headers: HeaderMap,
    State(state): State<SharedState>,
    Json(req): Json<AmountRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let event_id = request_event_id(&headers);
    if let Some(eid) = event_id {
        if let Some(hit) = state.idempotency.try_get(eid, account_id.as_u128()) {
            return Ok(Json(json!({
                "balanceCents": hit.balance_cents,
                "aggregateVersion": hit.aggregate_version,
            })));
        }
    }

    let result = state.account_service
        .withdraw(account_id.as_u128(), req.amount_cents, event_id)
        .await
        .map_err(map_account_error)?;

    Ok(Json(json!({
        "balanceCents": result.balance_cents,
        "aggregateVersion": result.aggregate_version,
    })))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferRequest {
    from_account_id: Uuid,
    to_account_id: Uuid,
    amount_cents: i32,
}

async fn transfer(
    headers: HeaderMap,
    State(state): State<SharedState>,
    Json(req): Json<TransferRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let event_id = request_event_id(&headers);
    if let Some(eid) = event_id {
        let from = state.idempotency.try_get(eid, req.from_account_id.as_u128());
        let to = state.idempotency.try_get(eid, req.to_account_id.as_u128());
        if let (Some(f), Some(t)) = (from, to) {
            return Ok(Json(json!({
                "from": { "balanceCents": f.balance_cents, "aggregateVersion": f.aggregate_version },
                "to": { "balanceCents": t.balance_cents, "aggregateVersion": t.aggregate_version },
            })));
        }
    }

    let result = state.account_service
        .transfer(
            req.from_account_id.as_u128(),
            req.to_account_id.as_u128(),
            req.amount_cents,
            event_id,
        )
        .await
        .map_err(map_account_error)?;

    Ok(Json(json!({
        "from": { "balanceCents": result.from.balance_cents, "aggregateVersion": result.from.aggregate_version },
        "to": { "balanceCents": result.to.balance_cents, "aggregateVersion": result.to.aggregate_version },
    })))
}

async fn watch_sse(
    State(state): State<SharedState>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.watch_tx.subscribe();
    let s = stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let data = serde_json::to_string(&event).unwrap_or_default();
                    return Some((Ok::<_, Infallible>(Event::default().data(data)), rx));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    Sse::new(s)
}

// --- Error mapping ---

fn map_account_error(e: AccountError) -> (StatusCode, Json<Value>) {
    match e {
        AccountError::Validation(msg) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "error": "VALIDATION_ERROR", "message": msg })),
        ),
        AccountError::InsufficientFunds { balance_cents, requested_cents: _ } => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "INSUFFICIENT_FUNDS",
                "balanceCents": balance_cents,
                "message": e.to_string(),
            })),
        ),
        AccountError::OccExhausted(_) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "CONFLICT", "message": "Account was modified concurrently. Please retry." })),
        ),
        AccountError::Client(ClientError::ConnectionFailed(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "SERVICE_UNAVAILABLE", "message": "Celeriant server unreachable." })),
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "INTERNAL_ERROR", "message": e.to_string() })),
        ),
    }
}

// --- Idempotency helpers ---

/// The `Idempotency-Key` header as a UUID, or a freshly minted one. It becomes the
/// `event_id` on the write. A caller-supplied key makes HTTP retries resolvable
/// without re-writing; any key, minted or not, is what lets a CEI violation be
/// verified as ours rather than a sibling's.
fn request_event_id(headers: &HeaderMap) -> Option<u128> {
    let from_header = headers
        .get("idempotency-key")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<Uuid>().ok())
        .map(|u| u.as_u128());
    Some(from_header.unwrap_or_else(|| Uuid::new_v4().as_u128()))
}

// --- Database init ---

async fn init_database(db: &tokio_postgres::Client) {
    db.execute(
        "CREATE TABLE IF NOT EXISTS account_balances (
            account_id                UUID PRIMARY KEY,
            account_name              TEXT NOT NULL,
            balance_cents             BIGINT NOT NULL DEFAULT 0,
            last_version          BIGINT NOT NULL DEFAULT 0,
            last_client_seq   BIGINT NOT NULL DEFAULT 0,
            updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
        &[],
    ).await.expect("Failed to create account_balances table");
}

// --- Seeding ---

async fn seed_accounts(pool: &CeleriantPool, db: &tokio_postgres::Client) {
    for account in ACCOUNTS.iter() {
        let account_uuid = u128_to_uuid(account.id);

        // Seed Postgres projection row
        db.execute(
            "INSERT INTO account_balances (account_id, account_name, balance_cents, \
             last_version, last_client_seq, updated_at) \
             VALUES ($1, $2, 0, 0, 0, now()) \
             ON CONFLICT (account_id) DO NOTHING",
            &[&account_uuid, &account.name],
        ).await.unwrap_or_else(|e| {
            tracing::warn!("Failed to seed Postgres row for {}: {e}", account.name);
            0
        });

        // Seed Celeriant aggregate (if not already present)
        let key = account_key(account.id);
        match pool.aggregate_details(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: key.clone(),
        }).await {
            Ok(details) if details.max_aggregate_version > 0 => continue,
            _ => {}
        }

        let evt = json_event(1, &Deposited { amount_cents: account.seed_cents }).unwrap();
        if let Err(e) = pool.write_events_with(key, vec![evt], *SERVICE_CLIENT_ID, WriteEventsOptions {
            allow_create: true,
            ..Default::default()
        }).await {
            tracing::warn!("Failed to seed {}: {e}", account.name);
            continue;
        }
        tracing::info!("Seeded {} with ${:.2}", account.name, account.seed_cents as f64 / 100.0);
    }
}

// --- Watch broadcaster ---

async fn watch_broadcaster(address: &str, tx: broadcast::Sender<Value>) {
    let account_ids: HashSet<u128> = ACCOUNTS.iter().map(|a| a.id).collect();

    loop {
        match watch_loop(address, &tx, &account_ids).await {
            Ok(()) => break,
            Err(e) => {
                tracing::warn!("Watch connection lost: {e}, reconnecting in 2s");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}

async fn watch_loop(
    address: &str,
    tx: &broadcast::Sender<Value>,
    account_ids: &HashSet<u128>,
) -> Result<(), ClientError> {
    let request = WatchRequest {
        correlation_id: None,
        requested_latency_ms: None,
        shard_id: None,
        orgs: Some(HashSet::from([*ORG_ID])),
        aggregate_types: Some(HashSet::from([*ACCOUNT_TYPE_ID])),
        aggregates: Some(account_ids.clone()),
        operation_types: Some(HashSet::from([1])), // Write
    };

    let mut connection = WatchConnection::connect(address, request, WatchOptions::default()).await?;
    tracing::info!("Watch connection established");

    loop {
        let response = connection.next().await?;
        for evt in &response.events {
            let watch_event = json!({
                "aggregateId": u128_to_uuid(evt.aggregate_id),
                "operation": "Write",
                "toAggregateVersion": evt.to_aggregate_version,
            });
            let _ = tx.send(watch_event);
        }
    }
}
