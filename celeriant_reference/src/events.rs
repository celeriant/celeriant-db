use serde::{Deserialize, Serialize};
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
