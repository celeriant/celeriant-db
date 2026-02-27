use anyhow::{Context, Result};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
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
use std::{collections::HashMap, fs};

use crate::cli::*;
use crate::utils::{format_response, format_timestamp};

pub async fn execute_command(server: &str, _api_key: Option<&str>, command: Commands) -> Result<()> {
    let mut client = CeleriantClient::connect(server)
        .await
        .with_context(|| format!("Failed to connect to {}", server))?;

    // Note: api_key is accepted but not yet used. It will be used when identity
    // verification is added to the CLI (requires --public-key and --private-key flags).

    match command {
        Commands::AggregateDetails(args) => check_aggregatedetails(&mut client, args).await,
        Commands::Read(args) => read_events(&mut client, args).await,
        Commands::Write(args) => write_event(&mut client, args).await,
        Commands::Trim(args) => trim_start(&mut client, args).await,
        Commands::Delete(args) => delete_aggregate(&mut client, args).await,
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
            println!("  Last client ID: {}", res.last_client_id);
            if let Some(user_id) = res.last_user_id {
                println!("  Last user ID: {}", user_id);
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
                        println!("  Client: {}, Events: {}", batch.client_id, batch.events.len());
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

async fn write_event(client: &mut CeleriantClient, args: WriteArgs) -> Result<()> {
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

    let mut writes = HashMap::new();
    writes.insert(key, SingleAggregateWrite {
        events: vec![event],
        allow_create: args.allow_create,
        expected_event_batch_index: args.expected_index,
        enforce_client_idempotency: args.enforce_idempotency,
        compression_type: compression,
    });
    
    let request = ClientRequest::Write(WriteRequest {
        correlation_id: args.key.correlation_id,
        client_id: args.client_id,
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

async fn trim_start(client: &mut CeleriantClient, args: TrimArgs) -> Result<()> {
    let key = AggregateKey::new(args.key.org, args.key.aggregate_type, args.key.id);

    let request = ClientRequest::TrimStart(TrimStartRequest {
        correlation_id: args.key.correlation_id,
        aggregate_key: key,
        keep_from_event_batch_index: args.keep_from,
        client_id: args.client_id,
        user_id: args.user_id,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        ClientResponse::TrimStart(res) => {
            println!("Trim successful");
            if let Some(id) = res.correlation_id {
                println!("  Correlation ID: {}", id);
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

async fn delete_aggregate(client: &mut CeleriantClient, args: DeleteArgs) -> Result<()> {
    let key = AggregateKey::new(args.key.org, args.key.aggregate_type, args.key.id);

    let mut deletes = HashMap::new();
    deletes.insert(key, SingleAggregateDelete {
        allow_recreate: args.allow_recreate,
        allow_index_continuation: args.allow_index_continuation,
        expected_event_batch_index: args.expected_index,
    });
    
    let request = ClientRequest::Delete(DeleteRequest {
        correlation_id: args.key.correlation_id,
        client_id: args.client_id,
        user_id: args.user_id,
        deletes,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        ClientResponse::Delete(res) => {
            println!("Delete successful");
            if let Some(id) = res.correlation_id {
                println!("  Correlation ID: {}", id);
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