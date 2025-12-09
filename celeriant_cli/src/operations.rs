use anyhow::{Context, Result};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::{
    process_requests::Request,
    process_responses::Response,
    request::{
        directory_filters::DirectoryFilters,
        read_filters::ReadFilters,
        requests::*,
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    compression_type::CompressionType,
    wal::event_item::EventItem,
};
use std::fs;

use crate::cli::*;
use crate::utils::{format_response, format_timestamp};

pub async fn execute_command(server: &str, command: Commands) -> Result<()> {
    let mut client = CeleriantClient::connect(server)
        .await
        .with_context(|| format!("Failed to connect to {}", server))?;

    match command {
        Commands::ListOrgs(args) => list_organisations(&mut client, args).await,
        Commands::ListAggregates(args) => list_aggregates(&mut client, args).await,
        Commands::Exists(args) => check_exists(&mut client, args).await,
        Commands::Read(args) => read_events(&mut client, args).await,
        Commands::Write(args) => write_event(&mut client, args).await,
        Commands::Prepend(args) => prepend_batches(&mut client, args).await,
        Commands::Trim(args) => trim_start(&mut client, args).await,
        Commands::Delete(args) => delete_aggregate(&mut client, args).await,
        Commands::UpdateCache(args) => update_cache(&mut client, args).await,
    }
}

async fn list_organisations(client: &mut CeleriantClient, args: ListOrgsArgs) -> Result<()> {
    let filters = DirectoryFilters {
        created_after_or_on: args.created_after,
        created_before_or_on: args.created_before,
        modified_after_or_on: args.modified_after,
        modified_before_or_on: args.modified_before,
        ..Default::default()
    };

    let request = Request::ListOrganisations(ListOrganisationsRequest {
        correlation_id: None,
        filters,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        Response::ListOrganisations(res) => {
            match args.format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&res.organisations)?);
                }
                OutputFormat::Table | OutputFormat::Compact => {
                    println!("{:<40} {:<24} {:<24} {:>12}", "ORG ID", "CREATED", "MODIFIED", "DISK USAGE");
                    println!("{}", "-".repeat(104));
                    for org in &res.organisations {
                        println!(
                            "{:<40} {:<24} {:<24} {:>12}",
                            org.org_id,
                            format_timestamp(org.created_at),
                            format_timestamp(org.modified_at),
                            humansize::format_size(org.disk_usage, humansize::BINARY)
                        );
                    }
                    println!("\nTotal: {} organisations", res.organisations.len());
                }
            }
        }
        Response::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

async fn list_aggregates(client: &mut CeleriantClient, args: ListAggregatesArgs) -> Result<()> {
    let filters = DirectoryFilters {
        created_after_or_on: args.created_after,
        created_before_or_on: args.created_before,
        ..Default::default()
    };

    let request = Request::ListAggregates(ListAggregatesRequest {
        correlation_id: None,
        org_id: args.org,
        aggregate_type_id: args.aggregate_type,
        filters,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        Response::ListAggregates(res) => {
            match args.format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&res.aggregates)?);
                }
                OutputFormat::Table | OutputFormat::Compact => {
                    println!("{:<40} {:<20} {:<20} {:<24} {:>12}", "AGGREGATE ID", "ORG", "TYPE", "MODIFIED", "DISK USAGE");
                    println!("{}", "-".repeat(120));
                    for agg in &res.aggregates {
                        println!(
                            "{:<40} {:<20} {:<20} {:<24} {:>12}",
                            agg.key.aggregate_id,
                            agg.key.org_id,
                            agg.key.aggregate_type_id,
                            format_timestamp(agg.modified_at),
                            humansize::format_size(agg.disk_usage, humansize::BINARY)
                        );
                    }
                    println!("\nTotal: {} aggregates", res.aggregates.len());
                }
            }
        }
        Response::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

async fn check_exists(client: &mut CeleriantClient, args: AggregateKeyArgs) -> Result<()> {
    let key = AggregateKey::new(args.org, args.aggregate_type, args.id);
    let request = Request::Exists(ExistsRequest {
        correlation_id: args.correlation_id,
        aggregate_key: key,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        Response::Exists(res) => {
            println!("Aggregate exists:");
            println!("  Min batch index: {}", res.min_event_batch_index);
            println!("  Max batch index: {}", res.max_event_batch_index);
        }
        Response::GenericError(err) => {
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

    let request = Request::Read(ReadRequest {
        correlation_id: args.key.correlation_id,
        aggregate_key: key,
        filters,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        Response::Read(res) => {
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
        Response::GenericError(err) => {
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

    let event = EventItem {
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

    let request = Request::Write(WriteRequest {
        correlation_id: args.key.correlation_id,
        aggregate_key: key,
        client_id: args.client_id,
        user_id: args.user_id,
        events: vec![event],
        allow_create: args.allow_create,
        expected_event_batch_index: args.expected_index,
        enforce_client_idempotency: args.enforce_idempotency,
        durable_write_with_delay_us: None,
        compression_type: compression,
    });

    let response = client.send_request(&request, compression).await?;

    match &response {
        Response::Write(res) => {
            println!("Write successful:");
            println!("  Batch index: {}", res.event_batch_index);
            println!("  Start event index: {}", res.start_event_index);
            println!("  Server timestamp: {}", format_timestamp(res.server_timestamp));
            println!("  Compressed size: {}", humansize::format_size(res.compressed_size, humansize::BINARY));
            println!("  Node ID: {}", res.node_id);
            println!("  CRC: 0x{:08X}", res.events_crc);
        }
        Response::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

async fn prepend_batches(client: &mut CeleriantClient, args: PrependArgs) -> Result<()> {
    let key = AggregateKey::new(args.key.org, args.key.aggregate_type, args.key.id);

    let file_content = fs::read_to_string(&args.file)
        .with_context(|| format!("Failed to read file: {:?}", args.file))?;

    let batches = serde_json::from_str(&file_content)
        .context("Failed to parse batches JSON")?;

    let compression: CompressionType = args.compression.into();

    let request = Request::PrependBatches(PrependBatchesRequest {
        correlation_id: args.key.correlation_id,
        aggregate_key: key,
        allow_create: args.allow_create,
        durable_write_with_delay_us: None,
        compression_type: compression,
        batches,
    });

    let response = client.send_request(&request, compression).await?;

    match &response {
        Response::PrependBatches(res) => {
            println!("Prepend successful");
            if let Some(id) = res.correlation_id {
                println!("  Correlation ID: {}", id);
            }
        }
        Response::GenericError(err) => {
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

    let request = Request::TrimStart(TrimStartRequest {
        correlation_id: args.key.correlation_id,
        aggregate_key: key,
        keep_from_event_batch_index: args.keep_from,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        Response::TrimStart(res) => {
            println!("Trim successful");
            if let Some(id) = res.correlation_id {
                println!("  Correlation ID: {}", id);
            }
        }
        Response::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

async fn delete_aggregate(client: &mut CeleriantClient, args: AggregateKeyArgs) -> Result<()> {
    let key = AggregateKey::new(args.org, args.aggregate_type, args.id);

    let request = Request::Delete(DeleteRequest {
        correlation_id: args.correlation_id,
        aggregate_key: key,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        Response::Delete(res) => {
            println!("Delete successful");
            if let Some(id) = res.correlation_id {
                println!("  Correlation ID: {}", id);
            }
        }
        Response::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}

async fn update_cache(client: &mut CeleriantClient, args: UpdateCacheArgs) -> Result<()> {
    let request = Request::UpdateCacheLimits(UpdateCacheLimitsRequest {
        correlation_id: None,
        aggregate_write_max_data_cache_size_bytes: args.max_size,
    });

    let response = client.send_request(&request, CompressionType::None).await?;

    match &response {
        Response::UpdateCacheLimits(res) => {
            println!("Cache limits updated");
            if let Some(id) = res.correlation_id {
                println!("  Correlation ID: {}", id);
            }
        }
        Response::GenericError(err) => {
            anyhow::bail!("Error {}: {}", err.error_code, err.error_message);
        }
        other => {
            println!("{}", format_response(other));
        }
    }

    Ok(())
}