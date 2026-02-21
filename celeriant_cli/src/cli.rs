use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "celeriant")]
#[command(author, version, about = "Celeriant Event Store CLI & TUI Client")]
#[command(propagate_version = true)]
pub struct Cli {
    /// Server address to connect to
    #[arg(short, long, default_value = "127.0.0.1:10000", env = "CELERIANT_SERVER")]
    pub server: String,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Clone)]
pub enum Commands {
    
    /// Check if an aggregate exists
    AggregateDetails(AggregateKeyArgs),

    /// Read events from an aggregate
    Read(ReadArgs),

    /// Write events to an aggregate
    Write(WriteArgs),

    /// Trim events from the start of an aggregate
    Trim(TrimArgs),

    /// Delete an aggregate
    Delete(DeleteArgs),
}

#[derive(Args, Clone)]
pub struct AggregateKeyArgs {
    /// Organisation ID
    #[arg(long)]
    pub org: u128,

    /// Aggregate type ID
    #[arg(long, long = "type")]
    pub aggregate_type: u128,

    /// Aggregate ID
    #[arg(long)]
    pub id: u128,

    /// Correlation ID for tracking
    #[arg(long)]
    pub correlation_id: Option<u128>,
}

#[derive(Args, Clone)]
pub struct ListOrgsArgs {
    /// Filter: created after timestamp (unix millis)
    #[arg(long)]
    pub created_after: Option<u64>,

    /// Filter: created before timestamp (unix millis)
    #[arg(long)]
    pub created_before: Option<u64>,

    /// Filter: modified after timestamp (unix millis)
    #[arg(long)]
    pub modified_after: Option<u64>,

    /// Filter: modified before timestamp (unix millis)
    #[arg(long)]
    pub modified_before: Option<u64>,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,
}

#[derive(Args, Clone)]
pub struct ListAggregatesArgs {
    /// Organisation ID
    #[arg(long)]
    pub org: u128,

    /// Aggregate type ID (optional filter)
    #[arg(long, name = "type")]
    pub aggregate_type: Option<u128>,

    /// Filter: created after timestamp
    #[arg(long)]
    pub created_after: Option<u64>,

    /// Filter: created before timestamp
    #[arg(long)]
    pub created_before: Option<u64>,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,
}

#[derive(Args, Clone)]
pub struct ReadArgs {
    #[command(flatten)]
    pub key: AggregateKeyArgs,

    /// Starting event batch index (1-based)
    #[arg(long, default_value = "1")]
    pub from: u64,

    /// Ending event batch index (inclusive)
    #[arg(long)]
    pub to: Option<u64>,

    /// Filter by event types (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub event_types: Option<Vec<u64>>,

    /// Exclude events from this client
    #[arg(long)]
    pub exclude_client: Option<u128>,

    /// Include only events from this client
    #[arg(long)]
    pub include_client: Option<u128>,

    /// Minimum server timestamp
    #[arg(long)]
    pub min_timestamp: Option<u64>,

    /// Maximum server timestamp
    #[arg(long)]
    pub max_timestamp: Option<u64>,

    /// Output format
    #[arg(long, value_enum, default_value = "json")]
    pub format: OutputFormat,
}

#[derive(Args, Clone)]
pub struct DeleteArgs {
    #[command(flatten)]
    pub key: AggregateKeyArgs,

    /// Client ID for the write
    #[arg(long)]
    pub client_id: u128,

    /// User ID (optional)
    #[arg(long)]
    pub user_id: Option<u128>,

    /// Allow re-creating the aggregate after delete
    #[arg(long)]
    pub allow_recreate: bool,

    /// On recreation after delete, continue from last batch and event indexes instead of resetting
    #[arg(long)]
    pub allow_index_continuation: bool,

    /// Expected event batch index (for optimistic concurrency)
    #[arg(long)]
    pub expected_index: Option<u64>,
}

#[derive(Args, Clone)]
pub struct WriteArgs {
    #[command(flatten)]
    pub key: AggregateKeyArgs,

    /// Client ID for the write
    #[arg(long)]
    pub client_id: u128,

    /// User ID (optional)
    #[arg(long)]
    pub user_id: Option<u128>,

    /// Event type ID
    #[arg(long)]
    pub event_type: u64,

    /// Event data as UTF-8 string
    #[arg(long, conflicts_with = "file")]
    pub data: Option<String>,

    /// Event data from file
    #[arg(long, conflicts_with = "data")]
    pub file: Option<PathBuf>,

    /// Allow creating the aggregate if it doesn't exist
    #[arg(long)]
    pub allow_create: bool,

    /// Expected event batch index (for optimistic concurrency)
    #[arg(long)]
    pub expected_index: Option<u64>,

    /// Enforce client idempotency
    #[arg(long)]
    pub enforce_idempotency: bool,

    /// Compression type
    #[arg(long, value_enum, default_value = "none")]
    pub compression: CompressionArg,
}

#[derive(Args, Clone)]
pub struct TrimArgs {
    #[command(flatten)]
    pub key: AggregateKeyArgs,

    /// Client ID for the write
    #[arg(long)]
    pub client_id: u128,

    /// User ID (optional)
    #[arg(long)]
    pub user_id: Option<u128>,

    /// Keep events from this batch index onwards
    #[arg(long)]
    pub keep_from: u64,
}

#[derive(Args, Clone)]
pub struct UpdateCacheArgs {
    /// Maximum data cache size in bytes
    #[arg(long)]
    pub max_size: u64,
}

#[derive(ValueEnum, Clone, Copy, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Compact,
}

#[derive(ValueEnum, Clone, Copy, Default)]
pub enum CompressionArg {
    #[default]
    None,
    Snappy,
    Zstd,
}

impl From<CompressionArg> for celeriant_wal::compression_type::CompressionType {
    fn from(val: CompressionArg) -> Self {
        match val {
            CompressionArg::None => celeriant_wal::compression_type::CompressionType::None,
            CompressionArg::Snappy => celeriant_wal::compression_type::CompressionType::Snappy,
            CompressionArg::Zstd => celeriant_wal::compression_type::CompressionType::Zstd { level: 6 },
        }
    }
}