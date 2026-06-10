use celeriant_client_tokio::from_json;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Deposited {
    pub amount_cents: i32,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Withdrawn {
    pub amount_cents: i32,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TransferredOut {
    pub amount_cents: i32,
    pub to_account_id: Uuid,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TransferredIn {
    pub amount_cents: i32,
    pub from_account_id: Uuid,
}

/// Unknown event types and undecodable payloads fold as zero-amount: the demo
/// favours staying up over halting on schema drift. A production fold should
/// fail loudly instead; a silently skipped event is a silently wrong balance.
pub fn replay_event(balance_cents: i64, evt: &celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent) -> i64 {
    match evt.event_type_major {
        1 => balance_cents + from_json::<Deposited>(evt).map(|d| d.amount_cents as i64).unwrap_or(0),
        2 => balance_cents - from_json::<Withdrawn>(evt).map(|w| w.amount_cents as i64).unwrap_or(0),
        3 => balance_cents - from_json::<TransferredOut>(evt).map(|t| t.amount_cents as i64).unwrap_or(0),
        4 => balance_cents + from_json::<TransferredIn>(evt).map(|t| t.amount_cents as i64).unwrap_or(0),
        _ => balance_cents,
    }
}

pub fn format_event(
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
