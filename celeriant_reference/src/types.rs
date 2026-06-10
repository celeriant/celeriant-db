use celeriant_client_tokio::ClientError;

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
                write!(f, "Cannot process ${:.2}, balance is ${:.2}",
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
