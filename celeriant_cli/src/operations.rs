use anyhow::{Context, Result, bail};
use celeriant_client_tokio::{CeleriantClient, ClientIdentityConfig, ClientTlsConfig};
use celeriant_client_tokio::list_operations::*;
use celeriant_crypto::pki::PkiManager;
use humansize::{format_size, BINARY};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::{
        read_filters::ReadFilters,
        requests::*,
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use rustls::pki_types::ServerName;
use std::{collections::HashMap, fs};

use crate::cli::*;
use crate::utils::{format_response, format_timestamp, format_u128_uuid};

fn extract_host(address: &str) -> &str {
    address.split(':').next().unwrap_or(address)
}

fn build_tls_config(cli: &Cli) -> Result<Option<ClientTlsConfig>> {
    if !cli.tls {
        if cli.ca_cert.is_some() || cli.client_cert.is_some() || cli.client_key.is_some() {
            bail!("TLS certificate flags require --tls");
        }
        return Ok(None);
    }

    let ca_path = cli.ca_cert.as_ref()
        .ok_or_else(|| anyhow::anyhow!("--ca-cert is required when --tls is enabled"))?;
    let ca_bundle = PkiManager::load_ca_bundle(ca_path)
        .with_context(|| format!("Failed to load CA certificate: {}", ca_path.display()))?;

    let client_config = if let Some(cert_path) = &cli.client_cert {
        let key_path = cli.client_key.as_ref().unwrap(); // clap `requires` guarantees this
        let (chain, key) = PkiManager::load_identity(cert_path, key_path)
            .with_context(|| format!("Failed to load client identity: {}", cert_path.display()))?;
        PkiManager::build_client_config(&ca_bundle, chain, key)?
    } else {
        PkiManager::build_client_config_no_auth(&ca_bundle)?
    };

    let host = cli.server_name.as_deref().unwrap_or_else(|| extract_host(&cli.server));
    let server_name = ServerName::try_from(host.to_owned())
        .map_err(|_| anyhow::anyhow!("Invalid server name for TLS SNI: {host}"))?;

    Ok(Some(ClientTlsConfig::new(client_config, server_name)))
}

async fn identify_client(cli: &Cli, client: &mut CeleriantClient) -> Result<Option<u128>> {
    let has_keys = cli.public_key.is_some();
    let has_api_key = cli.api_key.is_some();
    if !has_keys && !has_api_key {
        return Ok(None);
    }

    let (public_key, private_key) = if has_keys {
        let pub_path = cli.public_key.as_ref().unwrap();
        let priv_path = cli.private_key.as_ref().unwrap();
        let pub_key = fs::read_to_string(pub_path)
            .with_context(|| format!("Failed to read public key: {}", pub_path.display()))?;
        let priv_key = fs::read_to_string(priv_path)
            .with_context(|| format!("Failed to read private key: {}", priv_path.display()))?;
        (Some(pub_key.trim().to_owned()), Some(priv_key.trim().to_owned()))
    } else {
        (None, None)
    };

    let identity_config = ClientIdentityConfig {
        public_key,
        private_key,
        api_key: cli.api_key.clone(),
    };

    let identity_client_id = client.identify(&identity_config).await
        .context("Identity verification failed")?;

    Ok(identity_client_id)
}

/// Resolve client_id for write/delete/trim operations.
/// Priority: identity-derived > explicit --client-id > error.
fn resolve_client_id(identity_client_id: Option<u128>, explicit_client_id: Option<u128>) -> Result<u128> {
    match (identity_client_id, explicit_client_id) {
        (Some(identity_id), Some(explicit_id)) => {
            if identity_id != explicit_id {
                bail!(
                    "--client-id {} does not match identity-derived client ID {}",
                    format_u128_uuid(explicit_id),
                    format_u128_uuid(identity_id),
                );
            }
            Ok(identity_id)
        }
        (Some(id), None) => Ok(id),
        (None, Some(id)) => Ok(id),
        (None, None) => bail!("No client ID available. Provide --client-id or use --public-key/--private-key for identity verification"),
    }
}

pub async fn execute_command(cli: &Cli, command: Commands) -> Result<()> {
    let tls_config = build_tls_config(cli)?;

    let mut client = CeleriantClient::connect_with_timeout(&cli.server, None, tls_config)
        .await
        .with_context(|| format!("Failed to connect to {}", cli.server))?;

    let identity_client_id = identify_client(cli, &mut client).await?;

    match command {
        Commands::AggregateDetails(args) => check_aggregatedetails(&mut client, args).await,
        Commands::Read(args) => read_events(&mut client, args).await,
        Commands::Write(args) => write_event(&mut client, args, identity_client_id).await,
        Commands::Trim(args) => trim_start(&mut client, args, identity_client_id).await,
        Commands::Delete(args) => delete_aggregate(&mut client, args, identity_client_id).await,
        Commands::ListOrgs(args) => list_orgs(&mut client, args).await,
        Commands::ListTypes(args) => list_types(&mut client, args).await,
        Commands::ListAggregates(args) => list_aggregates(&mut client, args).await,
    }
}

async fn check_aggregatedetails(client: &mut CeleriantClient, args: AggregateKeyArgs) -> Result<()> {
    let key = AggregateKey::new(args.org, args.aggregate_type, args.id);
    let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
        correlation_id: args.correlation_id,
        aggregate_key: key,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        ClientResponse::AggregateDetails(res) => {
            println!("Aggregate details:");
            println!("  Batch index range: {} - {}", res.min_event_batch_index, res.max_event_batch_index);
            println!("  Max event index: {}", res.max_event_index);
            println!("  Deleted: {}", res.is_deleted);
            if res.is_deleted {
                println!("  Allow recreate: {}", res.allow_recreate);
                println!("  Allow index continuation: {}", res.allow_index_continuation);
            }
            println!("  Last server timestamp: {}", format_timestamp(res.last_server_timestamp));
            println!("  Last client ID: {}", format_u128_uuid(res.last_client_id));
            if let Some(user_id) = res.last_user_id {
                println!("  Last user ID: {}", format_u128_uuid(user_id));
            }
        }
        ClientResponse::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

async fn read_events(client: &mut CeleriantClient, args: ReadArgs) -> Result<()> {
    let key = AggregateKey::new(args.key.org, args.key.aggregate_type, args.key.id);

    let mut filters = ReadFilters::new(args.from);

    if let Some(to) = args.to {
        filters = filters.to_event_batch_index(to);
    }
    if let Some(types) = args.event_types {
        filters = filters.include_event_types(types);
    }
    if let Some(client_id) = args.exclude_client {
        filters = filters.exclude_client_id(client_id);
    }
    if let Some(client_id) = args.include_client {
        filters = filters.include_client_id(client_id);
    }
    if let Some(ts) = args.min_timestamp {
        filters = filters.min_server_timestamp(ts);
    }
    if let Some(ts) = args.max_timestamp {
        filters = filters.max_server_timestamp(ts);
    }
    if let Some(user_id) = args.include_user {
        filters = filters.include_user_id(user_id);
    }
    if let Some(user_id) = args.exclude_user {
        filters = filters.exclude_user_id(user_id);
    }
    if let Some(ts) = args.min_event_timestamp {
        filters = filters.min_event_timestamp(ts);
    }
    if let Some(ts) = args.max_event_timestamp {
        filters = filters.max_event_timestamp(ts);
    }
    if let Some(idx) = args.min_event_index {
        filters = filters.min_event_index(idx);
    }
    if let Some(idx) = args.max_event_index {
        filters = filters.max_event_index(idx);
    }
    if let Some(idx) = args.min_client_event_index {
        filters = filters.min_client_event_index(idx);
    }
    if let Some(idx) = args.max_client_event_index {
        filters = filters.max_client_event_index(idx);
    }

    let request = ClientRequest::Read(ReadRequest {
        correlation_id: args.key.correlation_id,
        aggregate_key: key,
        filters,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        ClientResponse::Read(res) => {
            match args.format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&res)?);
                }
                OutputFormat::Table | OutputFormat::Compact => {
                    println!("Read {} event batches", res.event_batches.len());
                    if let Some(next) = res.next_event_batch_index {
                        println!("Next batch index: {}", next);
                    }
                    println!();
                    for batch in &res.event_batches {
                        println!("Batch {} ({})", batch.event_batch_index, format_timestamp(batch.server_timestamp));
                        println!("  Client: {}, Events: {}", format_u128_uuid(batch.client_id), batch.events.len());
                        for event in &batch.events {
                            let data_preview: String = String::from_utf8_lossy(&event.event_value)
                                .chars()
                                .take(50)
                                .collect();
                            println!("    Event type {}: {}...", event.event_type_major, data_preview);
                        }
                    }
                }
            }
        }
        ClientResponse::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

async fn write_event(client: &mut CeleriantClient, args: WriteArgs, identity_client_id: Option<u128>) -> Result<()> {
    let client_id = resolve_client_id(identity_client_id, args.client_id)?;
    let key = AggregateKey::new(args.key.org, args.key.aggregate_type, args.key.id);

    let data = if let Some(data_str) = args.data {
        data_str.into_bytes()
    } else if let Some(path) = args.file {
        fs::read(&path).with_context(|| format!("Failed to read file: {:?}", path))?
    } else {
        anyhow::bail!("Either --data or --file must be provided");
    };

    let event = DatablockAggregateEvent {
        event_type_major: args.event_type,
        client_event_index: 0,
        event_timestamp: chrono::Utc::now().timestamp_millis() as u64,
        event_value: std::sync::Arc::new(data),
        event_index: 0,
        event_id: None,
        event_type_minor: 0,
        iv: None,
    };

    let compression: CompressionType = args.compression.into();
    let (compression_type_id, compression_level) = compression.to_tuple();

    let mut writes = HashMap::new();
    writes.insert(key, SingleAggregateWrite {
        events: vec![event],
        allow_create: args.allow_create,
        expected_event_batch_index: args.expected_index,
        enforce_client_idempotency: args.enforce_idempotency,
        compression_type_id,
        compression_level,
    });

    let request = ClientRequest::Write(WriteRequest {
        correlation_id: args.key.correlation_id,
        client_id,
        user_id: args.user_id,
        writes,
    });

    let response = client.send_request(&request, compression).await?;

    match &response {
        ClientResponse::Write(_res) => {
            println!("Write successful.");
        }
        ClientResponse::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

async fn trim_start(client: &mut CeleriantClient, args: TrimArgs, identity_client_id: Option<u128>) -> Result<()> {
    let client_id = resolve_client_id(identity_client_id, args.client_id)?;
    let key = AggregateKey::new(args.key.org, args.key.aggregate_type, args.key.id);

    let request = ClientRequest::TrimStart(TrimStartRequest {
        correlation_id: args.key.correlation_id,
        aggregate_key: key,
        keep_from_event_batch_index: args.keep_from,
        client_id,
        user_id: args.user_id,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        ClientResponse::TrimStart(res) => {
            println!("Trim successful");
            if let Some(id) = res.correlation_id {
                println!("  Correlation ID: {}", format_u128_uuid(id));
            }
        }
        ClientResponse::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

async fn delete_aggregate(client: &mut CeleriantClient, args: DeleteArgs, identity_client_id: Option<u128>) -> Result<()> {
    let client_id = resolve_client_id(identity_client_id, args.client_id)?;
    let key = AggregateKey::new(args.key.org, args.key.aggregate_type, args.key.id);

    let mut deletes = HashMap::new();
    deletes.insert(key, SingleAggregateDelete {
        allow_recreate: args.allow_recreate,
        allow_index_continuation: args.allow_index_continuation,
        expected_event_batch_index: args.expected_index,
    });

    let request = ClientRequest::Delete(DeleteRequest {
        correlation_id: args.key.correlation_id,
        client_id,
        user_id: args.user_id,
        deletes,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        ClientResponse::Delete(res) => {
            println!("Delete successful");
            if let Some(id) = res.correlation_id {
                println!("  Correlation ID: {}", format_u128_uuid(id));
            }
        }
        ClientResponse::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

fn short_uuid(val: u128) -> String {
    let full = format_u128_uuid(val);
    format!("{}..{}", &full[..4], &full[full.len()-2..])
}

async fn list_orgs(client: &mut CeleriantClient, args: ListOrgsArgs) -> Result<()> {
    let options = ListOptions {
        start_shard: args.shard,
        ..Default::default()
    };
    let items = ListOrgsIterator::new(client, options).collect().await?;

    match args.format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(
            &items.iter().map(|i| format_u128_uuid(i.org_id)).collect::<Vec<_>>()
        )?),
        OutputFormat::Table | OutputFormat::Compact => {
            if items.is_empty() {
                println!("No organisations found.");
            } else {
                println!("{:<38}", "Org ID");
                println!("{}", "─".repeat(38));
                for item in &items {
                    println!("{}", format_u128_uuid(item.org_id));
                }
                println!("\n{} organisation(s)", items.len());
            }
        }
    }
    Ok(())
}

async fn list_types(client: &mut CeleriantClient, args: ListTypesArgs) -> Result<()> {
    let options = ListOptions {
        start_shard: args.shard,
        ..Default::default()
    };
    let items = ListAggregateTypesIterator::new(client, args.org, options).collect().await?;

    match args.format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(
            &items.iter().map(|i| serde_json::json!({
                "org_id": format_u128_uuid(i.org_id),
                "aggregate_type_id": format_u128_uuid(i.aggregate_type_id),
            })).collect::<Vec<_>>()
        )?),
        OutputFormat::Table | OutputFormat::Compact => {
            if items.is_empty() {
                println!("No aggregate types found.");
            } else {
                println!("{:<38} {:<38}", "Org ID", "Aggregate Type ID");
                println!("{} {}", "─".repeat(38), "─".repeat(38));
                for item in &items {
                    println!("{} {}", format_u128_uuid(item.org_id), format_u128_uuid(item.aggregate_type_id));
                }
                println!("\n{} aggregate type(s)", items.len());
            }
        }
    }
    Ok(())
}

async fn list_aggregates(client: &mut CeleriantClient, args: ListAggregatesArgs) -> Result<()> {
    let options = ListOptions {
        start_shard: args.shard,
        include_deleted: args.include_deleted,
        ..Default::default()
    };
    let items = ListAggregatesIterator::new(client, args.org, args.aggregate_type, options)
        .collect().await?;

    match args.format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(
            &items.iter().map(|i| serde_json::json!({
                "org_id": format_u128_uuid(i.org_id),
                "aggregate_type_id": format_u128_uuid(i.aggregate_type_id),
                "aggregate_id": format_u128_uuid(i.aggregate_id),
                "is_deleted": i.is_deleted,
                "event_batch_count": i.event_batch_count,
                "min_event_batch_index": i.min_event_batch_index,
                "max_event_batch_index": i.max_event_batch_index,
                "min_event_index": i.min_event_index,
                "max_event_index": i.max_event_index,
                "compressed_size": i.compressed_size,
                "uncompressed_size": i.uncompressed_size,
                "min_server_timestamp": i.min_server_timestamp,
                "max_server_timestamp": i.max_server_timestamp,
            })).collect::<Vec<_>>()
        )?),
        OutputFormat::Table | OutputFormat::Compact => {
            if items.is_empty() {
                println!("No aggregates found.");
            } else {
                println!("{:<8} {:<8} {:<10} {:<8} {:<12} {:<28} {:>10}",
                    "Org", "Type", "Aggregate", "Batches", "Events", "Server Time Range", "Size");
                println!("{} {} {} {} {} {} {}",
                    "─".repeat(8), "─".repeat(8), "─".repeat(10), "─".repeat(8),
                    "─".repeat(12), "─".repeat(28), "─".repeat(10));
                for item in &items {
                    let del = if item.is_deleted { "[DEL] " } else { "" };
                    let time_range = if item.min_server_timestamp > 0 {
                        format!("{} .. {}", format_timestamp(item.min_server_timestamp), format_timestamp(item.max_server_timestamp))
                    } else {
                        "-".to_string()
                    };
                    let events = if item.min_event_batch_index > 0 {
                        format!("{}-{}", item.min_event_batch_index, item.max_event_batch_index)
                    } else {
                        "-".to_string()
                    };
                    println!("{}{:<8} {:<8} {:<10} {:<8} {:<12} {:<28} {:>10}",
                        del,
                        short_uuid(item.org_id),
                        short_uuid(item.aggregate_type_id),
                        short_uuid(item.aggregate_id),
                        item.event_batch_count,
                        events,
                        time_range,
                        format_size(item.uncompressed_size, BINARY),
                    );
                }
                println!("\n{} aggregate(s)", items.len());
            }
        }
    }
    Ok(())
}