use eventplanedb_storage_structures::{event_batch_metadata::EventBatchMetadata, event_item::EventItem, read_filters::ReadFilters};
use serde::{Deserialize, Serialize};
use rmp_serde::{encode::to_vec_named, decode::from_slice};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Serialization error")]
    SerializationError,
    #[error("Deserialization error")]
    DeserializationError,
    #[error("Invalid message format")]
    InvalidFormat,
    #[error("Message too large")]
    MessageTooLarge,
    #[error("Connection closed unexpectedly")]
    ConnectionClosed,
}

const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;
const PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    AppendEvents {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
    },
    ReadFiltered {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: ReadFilters,
    },
    Exists {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },
    TrimStart {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
    },
    Delete {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    AppendEventsResult(Result<EventBatchMetadata, String>),
}

pub async fn write_message<T, W>(writer: &mut W, message: &T) -> Result<(), ProtocolError>
where
    T: Serialize,
    W: tokio::io::AsyncWriteExt + Unpin,
{
    let encoded = to_vec_named(message).map_err(|_| ProtocolError::SerializationError)?;
    if encoded.len() > MAX_MESSAGE_SIZE as usize {
        return Err(ProtocolError::MessageTooLarge);
    }

    // Write version
    writer.write_all(&[PROTOCOL_VERSION]).await.map_err(|_| ProtocolError::SerializationError)?;

    // Write length
    let length = encoded.len() as u32;
    writer.write_all(&length.to_le_bytes()).await.map_err(|_| ProtocolError::SerializationError)?;

    // Write payload
    writer.write_all(&encoded).await.map_err(|_| ProtocolError::SerializationError)?;

    Ok(())
}

pub async fn read_message<T, R>(reader: &mut R) -> Result<T, ProtocolError>
where
    T: for<'de> Deserialize<'de>,
    R: tokio::io::AsyncReadExt + Unpin,
{
    // Read version
    let mut version_buf = [0u8; 1];
    reader.read_exact(&mut version_buf).await.map_err(|_| ProtocolError::DeserializationError)?;
    if version_buf[0] != PROTOCOL_VERSION {
        return Err(ProtocolError::InvalidFormat);
    }

    // Read length
    let mut length_buf = [0u8; 4];
    reader.read_exact(&mut length_buf).await.map_err(|_| ProtocolError::DeserializationError)?;
    let length = u32::from_le_bytes(length_buf);
    if length > MAX_MESSAGE_SIZE {
        return Err(ProtocolError::MessageTooLarge);
    }

    // Read payload
    let mut payload = vec![0u8; length as usize];
    reader.read_exact(&mut payload).await.map_err(|_| ProtocolError::DeserializationError)?;

    let message = from_slice(&payload).map_err(|_| ProtocolError::DeserializationError)?;
    Ok(message)
}