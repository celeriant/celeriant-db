use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use crate::utils::parse_u128;

#[derive(Parser)]
#[command(name = "celeriant")]
#[command(author, version, about = "Celeriant Event Store CLI & TUI Client")]
#[command(propagate_version = true)]
pub struct Cli {
    /// Server address to connect to
    #[arg(short, long, default_value = "127.0.0.1:10000", env = "CELERIANT_SERVER")]
    pub server: String,

    /// Enable TLS
    #[arg(long, env = "CELERIANT_TLS")]
    pub tls: bool,

    /// CA certificate PEM file for server verification
    #[arg(long, env = "CELERIANT_CA_CERT")]
    pub ca_cert: Option<PathBuf>,

    /// Client certificate PEM file (enables mTLS)
    #[arg(long, env = "CELERIANT_CLIENT_CERT", requires = "client_key")]
    pub client_cert: Option<PathBuf>,

    /// Client private key PEM file (required with --client-cert)
    #[arg(long, env = "CELERIANT_CLIENT_KEY", requires = "client_cert")]
    pub client_key: Option<PathBuf>,

    /// TLS SNI server name (default: extracted from --server address)
    #[arg(long, env = "CELERIANT_SERVER_NAME")]
    pub server_name: Option<String>,

    /// API key for authentication (base64-encoded)
    #[arg(long, env = "CELERIANT_API_KEY")]
    pub api_key: Option<String>,

    /// RSA public key file (base64-encoded DER)
    #[arg(long, env = "CELERIANT_PUBLIC_KEY", requires = "private_key")]
    pub public_key: Option<PathBuf>,

    /// RSA private key file (base64-encoded DER)
    #[arg(long, env = "CELERIANT_PRIVATE_KEY", requires = "public_key")]
    pub private_key: Option<PathBuf>,

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

    /// List organisations
    ListOrgs(ListOrgsArgs),

    /// List aggregate types
    ListTypes(ListTypesArgs),

    /// List aggregates
    ListAggregates(ListAggregatesArgs),

    /// Register a schema for an aggregate type
    RegisterSchema(RegisterSchemaArgs),

    /// Dictionary training and management tools
    Dict(DictArgs),
}

#[derive(Args, Clone)]
pub struct DictArgs {
    #[command(subcommand)]
    pub command: DictCommands,
}

#[derive(Subcommand, Clone)]
pub enum DictCommands {
    /// Train a zstd dictionary from a JSONL corpus
    Train(DictTrainArgs),
}

#[derive(Args, Clone)]
pub struct DictTrainArgs {
    /// JSONL file or directory of JSONL files (one sample per line)
    #[arg(long)]
    pub corpus: PathBuf,

    /// Output path for the trained dictionary
    #[arg(long)]
    pub output: PathBuf,

    /// Maximum dictionary size in bytes
    #[arg(long, default_value = "65536")]
    pub max_dict_size: usize,
}

#[derive(Args, Clone)]
pub struct AggregateKeyArgs {
    /// Organisation ID (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub org: u128,

    /// Aggregate type ID (UUID, numeric, or base64)
    #[arg(long, long = "type", value_parser = parse_u128)]
    pub aggregate_type: u128,

    /// Aggregate ID (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub id: u128,

    /// Correlation ID for tracking (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub correlation_id: Option<u128>,
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

    /// Exclude events from this client (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub exclude_client: Option<u128>,

    /// Include only events from this client (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub include_client: Option<u128>,

    /// Minimum server timestamp (unix millis)
    #[arg(long)]
    pub min_timestamp: Option<u64>,

    /// Maximum server timestamp (unix millis)
    #[arg(long)]
    pub max_timestamp: Option<u64>,

    /// Only include batches from this user (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub include_user: Option<u128>,

    /// Exclude batches from this user (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub exclude_user: Option<u128>,

    /// Minimum client-side event timestamp (unix millis)
    #[arg(long)]
    pub min_event_timestamp: Option<u64>,

    /// Maximum client-side event timestamp (unix millis)
    #[arg(long)]
    pub max_event_timestamp: Option<u64>,

    /// Minimum event index
    #[arg(long)]
    pub min_event_index: Option<u64>,

    /// Maximum event index
    #[arg(long)]
    pub max_event_index: Option<u64>,

    /// Minimum client event index
    #[arg(long)]
    pub min_client_event_index: Option<u64>,

    /// Maximum client event index
    #[arg(long)]
    pub max_client_event_index: Option<u64>,

    /// Output format
    #[arg(long, value_enum, default_value = "json")]
    pub format: OutputFormat,
}

#[derive(Args, Clone)]
pub struct DeleteArgs {
    #[command(flatten)]
    pub key: AggregateKeyArgs,

    /// Client ID (UUID, numeric, or base64). Optional if using identity verification.
    #[arg(long, value_parser = parse_u128)]
    pub client_id: Option<u128>,

    /// User ID (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
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

    /// Client ID (UUID, numeric, or base64). Optional if using identity verification.
    #[arg(long, value_parser = parse_u128)]
    pub client_id: Option<u128>,

    /// User ID (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
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

}

#[derive(Args, Clone)]
pub struct TrimArgs {
    #[command(flatten)]
    pub key: AggregateKeyArgs,

    /// Client ID (UUID, numeric, or base64). Optional if using identity verification.
    #[arg(long, value_parser = parse_u128)]
    pub client_id: Option<u128>,

    /// User ID (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub user_id: Option<u128>,

    /// Keep events from this batch index onwards
    #[arg(long)]
    pub keep_from: u64,
}

#[derive(Args, Clone)]
pub struct ListOrgsArgs {
    /// Starting shard ID (default: 0, auto-discovers all shards)
    #[arg(long, default_value = "0")]
    pub shard: u64,

    /// Correlation ID for tracking (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub correlation_id: Option<u128>,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,
}

#[derive(Args, Clone)]
pub struct ListTypesArgs {
    /// Starting shard ID (default: 0, auto-discovers all shards)
    #[arg(long, default_value = "0")]
    pub shard: u64,

    /// Filter by organisation (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub org: Option<u128>,

    /// Correlation ID for tracking (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub correlation_id: Option<u128>,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,
}

#[derive(Args, Clone)]
pub struct ListAggregatesArgs {
    /// Starting shard ID (default: 0, auto-discovers all shards)
    #[arg(long, default_value = "0")]
    pub shard: u64,

    /// Filter by organisation (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub org: Option<u128>,

    /// Filter by aggregate type (UUID, numeric, or base64). Requires --org.
    #[arg(long, long = "type", value_parser = parse_u128, requires = "org")]
    pub aggregate_type: Option<u128>,

    /// Include deleted aggregates
    #[arg(long)]
    pub include_deleted: bool,

    /// Correlation ID for tracking (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub correlation_id: Option<u128>,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,
}

#[derive(Args, Clone)]
pub struct RegisterSchemaArgs {
    /// Organisation ID (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub org: u128,

    /// Aggregate type ID (UUID, numeric, or base64)
    #[arg(long, long = "type", value_parser = parse_u128)]
    pub aggregate_type: u128,

    /// Schema major version
    #[arg(long)]
    pub major: u64,

    /// Schema minor version
    #[arg(long, default_value = "0")]
    pub minor: u64,

    /// Schema type
    #[arg(long, value_enum)]
    pub schema_type: SchemaTypeArg,

    /// Schema definition as inline string (for json/avro)
    #[arg(long, conflicts_with = "file", conflicts_with = "proto_descriptor")]
    pub schema: Option<String>,

    /// Read schema definition from file (for json/avro)
    #[arg(long, conflicts_with = "schema", conflicts_with = "proto_descriptor")]
    pub file: Option<PathBuf>,

    /// Protobuf FileDescriptorSet binary file (from protoc --descriptor_set_out)
    #[arg(long, conflicts_with = "schema", conflicts_with = "file", requires = "message_name")]
    pub proto_descriptor: Option<PathBuf>,

    /// Fully qualified protobuf message name (e.g. mypackage.MyMessage)
    #[arg(long, requires = "proto_descriptor")]
    pub message_name: Option<String>,

    /// Client ID (UUID, numeric, or base64). Optional if using identity verification.
    #[arg(long, value_parser = parse_u128)]
    pub client_id: Option<u128>,

    /// User ID (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub user_id: Option<u128>,

    /// Correlation ID for tracking (UUID, numeric, or base64)
    #[arg(long, value_parser = parse_u128)]
    pub correlation_id: Option<u128>,
}

#[derive(ValueEnum, Clone, Copy)]
pub enum SchemaTypeArg {
    Json,
    Avro,
    Protobuf,
}

#[derive(ValueEnum, Clone, Copy, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Compact,
}

