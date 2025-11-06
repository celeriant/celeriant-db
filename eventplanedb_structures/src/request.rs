use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use std::io::Cursor;
use crate::{batch_metadata_item_pair::BatchMetadataItemPair, compression_type::CompressionType, constants::BINCODE_CONFIG_VARIABLE, directory_filters::DirectoryFilters, event_item::EventItem, read_filters::ReadFilters, wire_format::{MAX_MESSAGE_SIZE, PROTOCOL_VERSION_V1, PROTOCOL_VERSION_V2, STACK_BUFFER_SIZE, WireError}};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum Request {

    /// All organisations (128 bit ids) that are present on this server
    /// It's possible to apply a filter for created or modified times or disk usage
    ListOrganisations {
        correlation_id: Option<u128>,
        filters: DirectoryFilters,
    },

    /// List of aggregates under an organisation and optionally an aggregate type
    /// It's possible to apply a filter for created or modified times or disk usage
    ListAggregates {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: Option<u128>,
        filters: DirectoryFilters,
    },

    /// Confirm if a specific aggregate still exists on this server
    Exists {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },

    /// Attempt to gain exclusive write access to an aggregate for a specific client
    /// All other clients will fail to write, trim or delete the aggregate
    /// Reads are allowed unless allow_read is false
    Lock {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        timeout_ms: u64,
        allow_read: bool,
    },

    /// Unlock an aggregate early before the timeout was to expire
    Unlock {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },

    /// Get event batches for a specific aggregate
    /// A common use is to catch up the stream from a specific event batch index
    /// Filters can be applied to skip batches or filter events within a returned batch
    /// Does not return metadata entries, use ReadAll instead for this purpose.
    Read {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: ReadFilters,
    },

    /// Get event batches for a specific aggregate
    /// Use ReadAll to get both event batches and associated metadata
    /// Commonly used for backup or data tiering purposes, data can be returned to the aggregate via WriteBatches
    /// Be careful with filters as it may exclude batches or events within a batch
    ReadAll {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: ReadFilters,
    },

    /// Appends all events provided as a single batch to the end of the aggregate event stream
    /// If the aggregate does not exist, it will be created if allow_create is true
    /// If idempotent_client is true, events will be filtered using client_event_index and client_id to prevent duplicate events
    /// Set a durable_write_with_delay_us (eg. 200 micro-seconds) to force durable writes (fsync) to disk after the delay
    /// Turning off durable writes will reduce write latency and throughput but will result in data loss if the server crashes
    /// Set allow_repair_corruption to true to automatically trim away any corrupted events at the end of the aggregate (eg. from power loss or bitrot)
    Write {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        allow_create: bool,
        allow_repair_corruption: bool,
        expected_event_batch_index: Option<u64>,
        enforce_client_idempotency: bool,
        durable_write_with_delay_us: Option<u64>,
        compression_type: CompressionType,
    },

    /// Adds a batch and metadata without any additional processing
    /// Will either add to front or back of the aggregate, and even create the aggregate if allow_create is true
    /// Will error if adding would result in a gap in the event batch indexing
    /// (it is critical that event batch index is always ordered and continguous)
    WriteBatches {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        allow_create: bool,
        allow_repair_corruption: bool,
        durable_write_with_delay_us: Option<u64>,
        batches: Vec<BatchMetadataItemPair>,
    },

    /// Removes all event batches before keep_from_event_batch_index
    /// Can be used to free up space on disk
    /// If clients try to read from the trimmed batches, they will get an error
    /// It's recommended to use ReadAll to backup the data elsewhere before triming
    TrimStart {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
    },

    /// Throws away recent event batches in an aggregate
    /// Can be used to repair corruption manually or remove invalid events from buggy clients
    TrimEnd {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        trim_from_event_batch_index: u64,
    },

    /// Completely removes the aggregate from the server and clears any in-memory caches for that aggregate
    Delete {
        correlation_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },
}

impl Request {
    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate (they go to shard 0).
    pub fn routing_id(&self) -> u128 {
        match self {
            Request::ListOrganisations { .. } => 0,
            Request::ListAggregates { .. } => 0,
            Request::Exists { aggregate_id, .. }
            | Request::Lock { aggregate_id, .. }
            | Request::Unlock { aggregate_id, .. }
            | Request::Read { aggregate_id, .. }
            | Request::ReadAll { aggregate_id, .. }
            | Request::Write { aggregate_id, .. }
            | Request::WriteBatches { aggregate_id, .. }
            | Request::TrimStart { aggregate_id, .. }
            | Request::TrimEnd { aggregate_id, .. }
            | Request::Delete { aggregate_id, .. } => *aggregate_id,
        }
    }
}

/// Read a request from the wire protocol
pub async fn read_request<R>(reader: &mut R) -> Result<(Request, u32), WireError>
where
    R: AsyncReadExt + Unpin,
{
    // Read version (4 bytes)
    let mut version_buffer = [0u8; 4];
    reader.read_exact(&mut version_buffer).await?;
    let version = u32::from_be_bytes(version_buffer);

    // Read length (4 bytes)
    let mut length_buffer = [0u8; 4];
    reader.read_exact(&mut length_buffer).await?;
    let message_length = u32::from_be_bytes(length_buffer);

    if message_length > MAX_MESSAGE_SIZE {
        return Err(WireError::MessageTooLarge(message_length));
    }

    let message = match version {
        PROTOCOL_VERSION_V1 => read_request_v1(reader, message_length as usize).await,
        PROTOCOL_VERSION_V2 => read_request_v2(reader, message_length as usize).await,
        _ => Err(WireError::UnsupportedVersion(version)),
    }?;

    Ok((message, version))
}

pub async fn write_request<W>(
    writer: &mut W,
    message: &Request,
    use_v2: bool,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    let protocol_version = if use_v2 {
        PROTOCOL_VERSION_V2
    } else {
        PROTOCOL_VERSION_V1
    };

    if use_v2 {
        write_request_v2(writer, message, protocol_version).await
    } else {
        write_request_v1(writer, message, protocol_version).await
    }
}

async fn read_request_v1<R>(reader: &mut R, message_length: usize) -> Result<Request, WireError>
where
    R: AsyncReadExt + Unpin,
{
    // Read payload - use stack for small messages, heap for large ones
    let message = if message_length <= STACK_BUFFER_SIZE as usize {
        // Use stack allocation for small messages
        let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];
        let payload_slice = &mut stack_buffer[..message_length];
        reader.read_exact(payload_slice).await?;
        rmp_serde::from_slice(payload_slice)?
    } else {
        // Use heap allocation for large messages
        let mut payload = vec![0u8; message_length];
        reader.read_exact(&mut payload).await?;
        rmp_serde::from_slice(&payload)?
    };

    Ok(message)
}

async fn read_request_v2<R>(reader: &mut R, message_length: usize) -> Result<Request, WireError>
where
    R: AsyncReadExt + Unpin,
{
    let message = if message_length <= STACK_BUFFER_SIZE as usize {
        let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];
        reader
            .read_exact(&mut stack_buffer[..message_length])
            .await?;
        bincode::decode_from_slice(&stack_buffer[..message_length], BINCODE_CONFIG_VARIABLE)
            .map_err(|_| WireError::InvalidFormatWithVersion(PROTOCOL_VERSION_V2))?
            .0
    } else {
        let mut heap_buffer = vec![0u8; message_length];
        reader.read_exact(&mut heap_buffer).await?;
        bincode::decode_from_slice(&heap_buffer, BINCODE_CONFIG_VARIABLE)
            .map_err(|_| WireError::InvalidFormatWithVersion(PROTOCOL_VERSION_V2))?
            .0
    };

    Ok(message)
}

async fn write_request_v1<W>(
    writer: &mut W,
    message: &Request,
    protocol_version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    match message {
        Request::Write { events, .. } if !events.is_empty() => {
            // Write requests with events can be large, use heap allocation
            let encoded = rmp_serde::to_vec(message)?;

            if encoded.len() > MAX_MESSAGE_SIZE as usize {
                return Err(WireError::MessageTooLarge(encoded.len() as u32));
            }

            let header_size = 8;
            let mut combined = Vec::with_capacity(header_size + encoded.len());
            combined.extend_from_slice(&protocol_version.to_be_bytes());
            combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            combined.extend_from_slice(&encoded);

            writer.write_all(&combined).await?;
        }
        Request::WriteBatches { batches, .. } if !batches.is_empty() => {
            // WriteBatches with batches can be large, use heap allocation
            let encoded = rmp_serde::to_vec(message)?;

            if encoded.len() > MAX_MESSAGE_SIZE as usize {
                return Err(WireError::MessageTooLarge(encoded.len() as u32));
            }

            let header_size = 8;
            let mut combined = Vec::with_capacity(header_size + encoded.len());
            combined.extend_from_slice(&protocol_version.to_be_bytes());
            combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            combined.extend_from_slice(&encoded);

            writer.write_all(&combined).await?;
        }
        _ => {
            // Other requests are typically small, try stack buffer
            let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];
            let mut cursor = Cursor::new(&mut stack_buffer[8..]);

            match rmp_serde::encode::write(&mut cursor, message) {
                Ok(()) => {
                    let encoded_len = cursor.position() as usize;

                    if encoded_len > MAX_MESSAGE_SIZE as usize {
                        return Err(WireError::MessageTooLarge(encoded_len as u32));
                    }

                    stack_buffer[0..4].copy_from_slice(&protocol_version.to_be_bytes());
                    stack_buffer[4..8].copy_from_slice(&(encoded_len as u32).to_be_bytes());

                    writer.write_all(&stack_buffer[..8 + encoded_len]).await?;
                }
                Err(_) => {
                    // Stack buffer too small, fall back to heap
                    let encoded = rmp_serde::to_vec(message)?;
                    let encoded_len = encoded.len();

                    if encoded_len > MAX_MESSAGE_SIZE as usize {
                        return Err(WireError::MessageTooLarge(encoded_len as u32));
                    }

                    let header_size = 8;
                    let mut combined = Vec::with_capacity(header_size + encoded.len());
                    combined.extend_from_slice(&protocol_version.to_be_bytes());
                    combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
                    combined.extend_from_slice(&encoded);

                    writer.write_all(&combined).await?;
                }
            }
        }
    }

    Ok(())
}

async fn write_request_v2<W>(
    writer: &mut W,
    message: &Request,
    protocol_version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    match message {
        Request::Write { events, .. } if !events.is_empty() => {
            // Write requests with events can be large, use heap allocation
            let encoded = bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE)?;

            if encoded.len() > MAX_MESSAGE_SIZE as usize {
                return Err(WireError::MessageTooLarge(encoded.len() as u32));
            }

            let header_size = 8;
            let mut combined = Vec::with_capacity(header_size + encoded.len());
            combined.extend_from_slice(&protocol_version.to_be_bytes());
            combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            combined.extend_from_slice(&encoded);

            writer.write_all(&combined).await?;
        }
        Request::WriteBatches { batches, .. } if !batches.is_empty() => {
            // WriteBatches with batches can be large, use heap allocation
            let encoded = bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE)?;

            if encoded.len() > MAX_MESSAGE_SIZE as usize {
                return Err(WireError::MessageTooLarge(encoded.len() as u32));
            }

            let header_size = 8;
            let mut combined = Vec::with_capacity(header_size + encoded.len());
            combined.extend_from_slice(&protocol_version.to_be_bytes());
            combined.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            combined.extend_from_slice(&encoded);

            writer.write_all(&combined).await?;
        }
        _ => {
            // Other requests are typically small, try stack buffer
            let mut stack_buffer = [0u8; STACK_BUFFER_SIZE as usize];

            match bincode::encode_into_slice(
                message,
                &mut stack_buffer[8..],
                BINCODE_CONFIG_VARIABLE,
            ) {
                Ok(encoded_len) => {
                    if encoded_len > MAX_MESSAGE_SIZE as usize {
                        return Err(WireError::MessageTooLarge(encoded_len as u32));
                    }

                    stack_buffer[0..4].copy_from_slice(&protocol_version.to_be_bytes());
                    stack_buffer[4..8].copy_from_slice(&(encoded_len as u32).to_be_bytes());

                    writer.write_all(&stack_buffer[..8 + encoded_len]).await?;
                }
                Err(_) => {
                    // Stack buffer too small, fall back to heap
                    let encoded = bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE)?;
                    let encoded_len = encoded.len();
                    if encoded_len > MAX_MESSAGE_SIZE as usize {
                        return Err(WireError::MessageTooLarge(encoded.len() as u32));
                    }

                    let header_size = 8;
                    let mut combined = Vec::with_capacity(header_size + encoded.len());
                    combined.extend_from_slice(&protocol_version.to_be_bytes());
                    combined.extend_from_slice(&(encoded_len as u32).to_be_bytes());
                    combined.extend_from_slice(&encoded);

                    writer.write_all(&combined).await?;
                }
            }
        }
    }

    Ok(())
}