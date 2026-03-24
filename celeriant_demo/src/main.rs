mod constants;
mod events;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use celeriant_client_tokio::server_error::{ReadError, ServerError, WriteError};
use celeriant_client_tokio::{
    CeleriantPool, ClientError, PoolOptions, WatchConnection, WatchOptions, WriteEventsOptions,
    json_event, from_json,
};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{
    AggregateDetailsRequest, ReadRequest, SingleAggregateWrite, WatchRequest, WriteRequest,
};
use futures::stream;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::broadcast;
use uuid::Uuid;

use constants::*;
use events::*;

struct AppState {
    pool: CeleriantPool,
    watch_tx: broadcast::Sender<Value>,
}

type SharedState = Arc<AppState>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let address = std::env::var("CELERIANT_ADDRESS").unwrap_or_else(|_| "localhost:10000".into());
    let pool = CeleriantPool::new(PoolOptions::new(&address));
    let (watch_tx, _) = broadcast::channel::<Value>(64);

    let state = Arc::new(AppState { pool, watch_tx: watch_tx.clone() });

    seed_accounts(&state.pool).await;

    let watch_address = address.clone();
    let watch_tx_clone = watch_tx.clone();
    tokio::spawn(async move {
        watch_broadcaster(&watch_address, watch_tx_clone).await;
    });

    let app = Router::new()
        .route("/api/accounts", get(get_accounts))
        .route("/api/accounts/{account_id}/events", get(get_events))
        .route("/api/accounts/{account_id}/deposit", post(deposit))
        .route("/api/accounts/{account_id}/withdraw", post(withdraw))
        .route("/api/transfers", post(transfer))
        .route("/api/watch/stream", get(watch_sse))
        .fallback_service(tower_http::services::ServeDir::new("celeriant_demo/wwwroot"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:5000").await.unwrap();
    tracing::info!("Demo listening on http://localhost:5000");
    axum::serve(listener, app).await.unwrap();
}

// --- Routes ---

async fn get_accounts() -> Json<Value> {
    Json(json!({
        "accounts": ACCOUNTS.iter().map(|a| json!({
            "id": u128_to_uuid(a.id),
            "name": a.name,
        })).collect::<Vec<_>>(),
        "clients": CLIENTS.iter().map(|c| json!({
            "id": u128_to_uuid(c.id),
            "name": c.name,
        })).collect::<Vec<_>>(),
    }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventsQuery {
    from_batch_index: Option<u64>,
}

async fn get_events(
    Path(account_id): Path<Uuid>,
    Query(params): Query<EventsQuery>,
    State(state): State<SharedState>,
) -> Result<Json<Value>, StatusCode> {
    let key = account_key(account_id.as_u128());
    let from = params.from_batch_index.unwrap_or(1);

    match state.pool.read(ReadRequest {
        correlation_id: None,
        aggregate_key: key,
        filters: ReadFilters::new(from),
    }).await {
        Ok(response) => {
            let batches: Vec<Value> = response.event_batches.iter().map(|b| json!({
                "batchIndex": b.event_batch_index,
                "clientId": u128_to_uuid(b.client_id),
                "serverTimestamp": b.server_timestamp,
                "events": b.events.iter().map(deserialize_event).collect::<Vec<_>>(),
            })).collect();
            Ok(Json(json!({ "batches": batches })))
        }
        Err(ClientError::Server(ServerError::Read {
            kind: ReadError::AggregateNotExists, ..
        })) => Ok(Json(json!({ "batches": [] }))),
        Err(e) => {
            tracing::error!("Read error: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DepositRequest {
    client_id: Uuid,
    amount_cents: i32,
    expected_batch_index: u64,
}

async fn deposit(
    Path(account_id): Path<Uuid>,
    State(state): State<SharedState>,
    Json(req): Json<DepositRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let key = account_key(account_id.as_u128());
    let evt = json_event(1, &Deposited { amount_cents: req.amount_cents }).unwrap();

    match state.pool.write_events_with(key.clone(), vec![evt], WriteEventsOptions {
        client_id: req.client_id.as_u128(),
        allow_create: true,
        expected_event_batch_index: Some(req.expected_batch_index),
        ..Default::default()
    }).await {
        Ok(_) => {
            let details = state.pool.aggregate_details(AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: key,
            }).await.map_err(|e| internal_error(&e.to_string()))?;
            Ok(Json(json!({ "newBatchIndex": details.max_event_batch_index })))
        }
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { current_event_batch_index, .. }, ..
        })) => Err((StatusCode::CONFLICT, Json(json!({
            "error": "OCC_CONFLICT",
            "currentBatchIndex": current_event_batch_index,
            "message": "Account was modified. Please refresh and retry.",
        })))),
        Err(e) => Err(internal_error(&e.to_string())),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WithdrawRequest {
    client_id: Uuid,
    amount_cents: i32,
    expected_batch_index: u64,
}

async fn withdraw(
    Path(account_id): Path<Uuid>,
    State(state): State<SharedState>,
    Json(req): Json<WithdrawRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let key = account_key(account_id.as_u128());
    let evt = json_event(2, &Withdrawn { amount_cents: req.amount_cents }).unwrap();

    match state.pool.write_events_with(key.clone(), vec![evt], WriteEventsOptions {
        client_id: req.client_id.as_u128(),
        allow_create: true,
        expected_event_batch_index: Some(req.expected_batch_index),
        ..Default::default()
    }).await {
        Ok(_) => {
            let details = state.pool.aggregate_details(AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: key,
            }).await.map_err(|e| internal_error(&e.to_string()))?;
            Ok(Json(json!({ "newBatchIndex": details.max_event_batch_index })))
        }
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { current_event_batch_index, .. }, ..
        })) => Err((StatusCode::CONFLICT, Json(json!({
            "error": "OCC_CONFLICT",
            "currentBatchIndex": current_event_batch_index,
            "message": "Account was modified. Please refresh and retry.",
        })))),
        Err(e) => Err(internal_error(&e.to_string())),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferRequest {
    client_id: Uuid,
    from_account_id: Uuid,
    to_account_id: Uuid,
    amount_cents: i32,
    expected_from_batch_index: u64,
    expected_to_batch_index: u64,
}

async fn transfer(
    State(state): State<SharedState>,
    Json(req): Json<TransferRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let from_key = account_key(req.from_account_id.as_u128());
    let to_key = account_key(req.to_account_id.as_u128());

    let transfer_out = json_event(3, &TransferredOut {
        amount_cents: req.amount_cents,
        to_account_id: req.to_account_id,
    }).unwrap();
    let transfer_in = json_event(4, &TransferredIn {
        amount_cents: req.amount_cents,
        from_account_id: req.from_account_id,
    }).unwrap();

    let write_request = WriteRequest {
        correlation_id: None,
        client_id: req.client_id.as_u128(),
        user_id: None,
        writes: HashMap::from([
            (from_key.clone(), SingleAggregateWrite {
                events: vec![transfer_out],
                allow_create: true,
                expected_event_batch_index: Some(req.expected_from_batch_index),
                enforce_client_idempotency: false,
                compression_type_id: 0,
                compression_level: None,
            }),
            (to_key.clone(), SingleAggregateWrite {
                events: vec![transfer_in],
                allow_create: true,
                expected_event_batch_index: Some(req.expected_to_batch_index),
                enforce_client_idempotency: false,
                compression_type_id: 0,
                compression_level: None,
            }),
        ]),
    };

    match state.pool.write(write_request).await {
        Ok(_) => {
            let from_details = state.pool.aggregate_details(AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: from_key,
            }).await.map_err(|e| internal_error(&e.to_string()))?;
            let to_details = state.pool.aggregate_details(AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: to_key,
            }).await.map_err(|e| internal_error(&e.to_string()))?;

            Ok(Json(json!({
                "newFromBatchIndex": from_details.max_event_batch_index,
                "newToBatchIndex": to_details.max_event_batch_index,
            })))
        }
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
        })) => Err((StatusCode::CONFLICT, Json(json!({
            "error": "OCC_CONFLICT",
            "message": "One or more accounts were modified. Please refresh and retry.",
        })))),
        Err(e) => Err(internal_error(&e.to_string())),
    }
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

// --- Helpers ---

fn deserialize_event(e: &celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent) -> Value {
    match e.event_type_major {
        1 => {
            let d: Deposited = from_json(e).unwrap();
            json!({ "eventTypeMajor": 1, "amountCents": d.amount_cents })
        }
        2 => {
            let w: Withdrawn = from_json(e).unwrap();
            json!({ "eventTypeMajor": 2, "amountCents": w.amount_cents })
        }
        3 => {
            let t: TransferredOut = from_json(e).unwrap();
            json!({ "eventTypeMajor": 3, "amountCents": t.amount_cents, "toAccountId": t.to_account_id })
        }
        4 => {
            let t: TransferredIn = from_json(e).unwrap();
            json!({ "eventTypeMajor": 4, "amountCents": t.amount_cents, "fromAccountId": t.from_account_id })
        }
        _ => json!({ "eventTypeMajor": e.event_type_major }),
    }
}

fn internal_error(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": msg })))
}

// --- Seeding ---

async fn seed_accounts(pool: &CeleriantPool) {
    let first_client_id = CLIENTS[0].id;
    for account in ACCOUNTS.iter() {
        let key = account_key(account.id);
        match pool.aggregate_details(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: key.clone(),
        }).await {
            Ok(details) if details.max_event_batch_index > 0 => continue,
            _ => {}
        }

        let evt = json_event(1, &Deposited { amount_cents: account.seed_cents }).unwrap();
        if let Err(e) = pool.write_events_with(key, vec![evt], WriteEventsOptions {
            client_id: first_client_id,
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
                "toBatchIndex": evt.to_event_batch_index,
            });
            let _ = tx.send(watch_event);
        }
    }
}
